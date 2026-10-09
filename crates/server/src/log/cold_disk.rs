use crate::durable;
use crate::error::ServerError;
use crate::fail_point;
use crate::log::io_probe::{self, IoEvent};
use crate::log::warm_disk::{decode_segment, RecoveredLogData};
use std::io::Write;
use std::path::Path;
use zemdb_core::id::SequenceNumber;
use zemdb_core::protocol::messages::SequencedOperation;

/// Tier 3: Compressed delta log on disk using Zstandard for high-density long-term retention.
#[derive(Debug, Default)]
pub struct ColdDiskLog;

/// A warm segment whose cold copy is durably in place: written, synced, renamed over its
/// final path with the directory synced, and verified readable.
#[derive(Debug)]
pub struct CompressedSegment {
    /// Size of the cold segment.
    pub bytes: u64,
    /// Set when removing the warm original, or making its removal durable, failed. The cold
    /// segment replaces the warm one all the same. A warm file that is still there (or comes
    /// back after a crash) is found next to its cold copy the next time the log opens, which
    /// keeps the warm one and compresses it again.
    pub cleanup_error: Option<ServerError>,
}

impl ColdDiskLog {
    /// Compresses an uncompressed Warm Disk segment (.wal) into a Cold Disk segment (.wal.zst)
    /// synchronously.
    ///
    /// Writes to a `.tmp` file first, flushes, syncs, atomically renames over the destination
    /// and syncs the directory, verifies readability, and only then deletes the uncompressed
    /// `.wal` file (syncing the directory again). A power loss can therefore never persist the
    /// deletion of the warm segment without the cold segment that replaces it. Opening the log
    /// removes a `.tmp` file left by an interrupted compression.
    ///
    /// An `Err` means the cold segment is not in place and the warm one is untouched. Once the
    /// cold segment is in place the call returns `Ok`, reporting a failed removal of the warm
    /// segment in the outcome.
    pub fn compress_warm_segment_sync(
        warm_path: &Path,
        cold_path: &Path,
    ) -> Result<CompressedSegment, ServerError> {
        if !warm_path.exists() {
            return Err(ServerError::Wal(format!(
                "Cannot compress non-existent warm segment: {:?}",
                warm_path
            )));
        }

        let uncompressed_data = std::fs::read(warm_path)?;
        let compressed_data = zstd::stream::encode_all(&uncompressed_data[..], 3)
            .map_err(|e| ServerError::Wal(format!("Zstd compression failed: {}", e)))?;

        let tmp_path = durable::tmp_path_for(cold_path);
        {
            let mut tmp_file = std::fs::File::create(&tmp_path)?;
            tmp_file.write_all(&compressed_data)?;
            tmp_file.flush()?;
            tmp_file.sync_all()?;
        }

        std::fs::rename(&tmp_path, cold_path)?;
        let dir = cold_path.parent().unwrap_or_else(|| Path::new(""));
        durable::sync_dir(dir)?;

        // Verify decompressed roundtrip before purging the uncompressed file
        let verified_ops = Self::read_range(cold_path, SequenceNumber::new(0), usize::MAX)?;
        if verified_ops.is_empty() && !uncompressed_data.is_empty() {
            return Err(ServerError::WalCorruption(
                "Cold segment verification failed: produced zero operations from non-empty WAL"
                    .to_string(),
            ));
        }

        let cleanup = std::fs::remove_file(warm_path)
            .map_err(ServerError::from)
            .and_then(|()| fail_point::check("compress_after_warm_removed", warm_path))
            .and_then(|()| {
                durable::sync_dir(warm_path.parent().unwrap_or_else(|| Path::new("")))
                    .map_err(ServerError::from)
            });
        Ok(CompressedSegment {
            bytes: compressed_data.len() as u64,
            cleanup_error: cleanup.err(),
        })
    }

    /// Asynchronously compresses a warm segment into a cold segment, delegating CPU-heavy
    /// Zstandard encoding and disk operations to `tokio::task::spawn_blocking` to avoid stalling
    /// Tokio worker threads.
    pub async fn compress_warm_segment(
        warm_path: impl AsRef<Path>,
        cold_path: impl AsRef<Path>,
    ) -> Result<CompressedSegment, ServerError> {
        let warm = warm_path.as_ref().to_path_buf();
        let cold = cold_path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || Self::compress_warm_segment_sync(&warm, &cold))
            .await
            .map_err(|e| ServerError::Internal(format!("Spawn blocking task failed: {}", e)))?
    }

    /// Reads operations within `(from_seq .. ]` up to `limit` from a compressed `.wal.zst` file.
    pub fn read_range(
        cold_path: &Path,
        from_seq: SequenceNumber,
        limit: usize,
    ) -> Result<Vec<SequencedOperation>, ServerError> {
        Self::read_range_with_mutations(cold_path, from_seq, limit).map(|(ops, _)| ops)
    }

    /// Reads operations and associated mutation IDs within `(from_seq .. ]` up to `limit` from
    /// a compressed `.wal.zst` file. A missing file yields nothing; the caller detects the gap
    /// it leaves.
    pub fn read_range_with_mutations(
        cold_path: &Path,
        from_seq: SequenceNumber,
        limit: usize,
    ) -> Result<RecoveredLogData, ServerError> {
        if limit == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        io_probe::record(
            IoEvent::SegmentRead,
            cold_path.parent().unwrap_or_else(|| Path::new("")),
        );
        let compressed_data = match std::fs::read(cold_path) {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok((Vec::new(), Vec::new()))
            }
            Err(e) => return Err(e.into()),
        };
        let decompressed_data = zstd::stream::decode_all(&compressed_data[..])
            .map_err(|e| ServerError::WalCorruption(format!("Zstd decompression failed: {}", e)))?;

        let decoded = decode_segment(&decompressed_data, from_seq, limit)?;
        if decoded.torn {
            tracing::warn!(
                path = ?cold_path,
                valid_bytes = decoded.valid_len,
                "Torn write detected in decompressed cold segment"
            );
        }
        Ok((decoded.ops, decoded.mutations))
    }
}

#[cfg(test)]
#[path = "tests/cold_disk.rs"]
mod tests;
