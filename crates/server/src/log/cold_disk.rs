use crate::error::ServerError;
use crate::log::warm_disk::{parse_segment_filename, RecoveredLogData, SealedSegmentMeta};
use rimdb_core::id::SequenceNumber;
use rimdb_core::protocol::messages::SequencedOperation;
use rimdb_core::protocol::wal_frame::{decode_wal_batch_from_slice, WalBatchDecodeResult};
use std::io::Write;
use std::path::Path;

/// Tier 3: Compressed delta log on disk using Zstandard for high-density long-term retention.
#[derive(Debug, Default)]
pub struct ColdDiskLog;

impl ColdDiskLog {
    /// Compresses an uncompressed Warm Disk segment (.wal) into a Cold Disk segment (.wal.zst) synchronously.
    ///
    /// Writes to a `.tmp` file first, flushes, syncs, atomically renames over the destination,
    /// verifies readability, and deletes the uncompressed `.wal` file.
    pub fn compress_warm_segment_sync(
        warm_path: &Path,
        cold_path: &Path,
    ) -> Result<(), ServerError> {
        if !warm_path.exists() {
            return Err(ServerError::Wal(format!(
                "Cannot compress non-existent warm segment: {:?}",
                warm_path
            )));
        }

        let uncompressed_data = std::fs::read(warm_path)?;
        let compressed_data = zstd::stream::encode_all(&uncompressed_data[..], 3)
            .map_err(|e| ServerError::Wal(format!("Zstd compression failed: {}", e)))?;

        let tmp_path = cold_path.with_extension("tmp");
        {
            let mut tmp_file = std::fs::File::create(&tmp_path)?;
            tmp_file.write_all(&compressed_data)?;
            tmp_file.flush()?;
            tmp_file.sync_all()?;
        }

        std::fs::rename(&tmp_path, cold_path)?;

        // Verify decompressed roundtrip before purging the uncompressed file
        let verified_ops = Self::read_range(cold_path, SequenceNumber::new(0), usize::MAX)?;
        if verified_ops.is_empty() && !uncompressed_data.is_empty() {
            return Err(ServerError::WalCorruption(
                "Cold segment verification failed: produced zero operations from non-empty WAL"
                    .to_string(),
            ));
        }

        std::fs::remove_file(warm_path)?;
        Ok(())
    }

    /// Asynchronously compresses a warm segment into a cold segment, delegating CPU-heavy
    /// Zstandard encoding and disk operations to `tokio::task::spawn_blocking` to avoid stalling Tokio worker threads.
    pub async fn compress_warm_segment(
        warm_path: impl AsRef<Path>,
        cold_path: impl AsRef<Path>,
    ) -> Result<(), ServerError> {
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

    /// Reads operations and associated mutation IDs within `(from_seq .. ]` up to `limit` from a compressed `.wal.zst` file.
    pub fn read_range_with_mutations(
        cold_path: &Path,
        from_seq: SequenceNumber,
        limit: usize,
    ) -> Result<RecoveredLogData, ServerError> {
        if limit == 0 || !cold_path.exists() {
            return Ok((Vec::new(), Vec::new()));
        }

        let compressed_data = std::fs::read(cold_path)?;
        let decompressed_data = zstd::stream::decode_all(&compressed_data[..])
            .map_err(|e| ServerError::Wal(format!("Zstd decompression failed: {}", e)))?;

        let mut offset = 0;
        let mut collected = Vec::new();
        let mut mutations = Vec::new();

        while offset < decompressed_data.len() && collected.len() < limit {
            match decode_wal_batch_from_slice(&decompressed_data[offset..]) {
                Ok(WalBatchDecodeResult::Ok {
                    ops,
                    mutation_id,
                    bytes_consumed,
                }) => {
                    for op in ops {
                        if op.seq.get() > from_seq.get() {
                            if let Some(m_id) = mutation_id {
                                mutations.push((m_id, op.seq));
                            }
                            collected.push(op);
                            if collected.len() >= limit {
                                break;
                            }
                        }
                    }
                    offset += bytes_consumed;
                }
                Ok(WalBatchDecodeResult::CleanEof) => break,
                Ok(WalBatchDecodeResult::TornWrite {
                    valid_bytes_offset, ..
                }) => {
                    tracing::warn!(
                        path = ?cold_path,
                        valid_bytes_offset = offset + valid_bytes_offset,
                        "Torn write detected in decompressed cold segment"
                    );
                    break;
                }
                Err(e) => return Err(ServerError::WalCorruption(e.to_string())),
            }
        }

        Ok((collected, mutations))
    }

    /// Lists all `.wal.zst` segments in ascending sequence order.
    pub fn list_cold_segments(dir: &Path) -> Result<Vec<SealedSegmentMeta>, ServerError> {
        let mut segments = Vec::new();
        if !dir.exists() {
            return Ok(segments);
        }

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("segment_") && filename.ends_with(".wal.zst") {
                        if let Some((start, end)) = parse_segment_filename(filename, ".wal.zst") {
                            segments.push(SealedSegmentMeta {
                                start_seq: SequenceNumber::new(start),
                                end_seq: SequenceNumber::new(end),
                                path,
                            });
                        }
                    }
                }
            }
        }

        segments.sort_by_key(|s| s.start_seq.get());
        Ok(segments)
    }
}
