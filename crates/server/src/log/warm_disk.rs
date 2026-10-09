use crate::durable;
use crate::error::ServerError;
use crate::fail_point;
use crate::log::io_probe::{self, IoEvent};
use crate::log::segment_index::warm_segment_path;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use zemdb_core::id::{MutationId, SequenceNumber};
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::protocol::wal_frame::{
    decode_wal_batch_from_slice, encode_wal_record, WalBatchDecodeResult,
};

/// Operations and mutation assignment pairs recovered from disk segments.
pub type RecoveredLogData = (Vec<SequencedOperation>, Vec<(MutationId, SequenceNumber)>);

/// Name of the active segment inside the segments directory.
const ACTIVE_SEGMENT: &str = "active.wal";

/// Tier 2: Uncompressed append-only log on disk ensuring crash durability and fast sequential reads.
///
/// `active.wal` is only ever accessed through `active_file`, the handle that holds its
/// exclusive lock: on Windows the lock is mandatory, so a second handle could not read it.
#[derive(Debug)]
pub struct WarmDiskLog {
    segments_dir: PathBuf,
    active_file: Option<File>,
    active_start_seq: Option<SequenceNumber>,
    active_end_seq: Option<SequenceNumber>,
    /// Length of the valid records in `active.wal`, where the next record is written.
    active_len: u64,
}

impl WarmDiskLog {
    /// Opens or creates the Warm Disk directory.
    pub fn open_or_create(segments_dir: impl AsRef<Path>) -> Result<Self, ServerError> {
        Self::open_recovering(segments_dir).map(|(log, _)| log)
    }

    /// Opens or creates the Warm Disk directory, and returns the operations and mutation IDs
    /// recovered from `active.wal` in order. A torn write at its end is truncated.
    pub(crate) fn open_recovering(
        segments_dir: impl AsRef<Path>,
    ) -> Result<(Self, RecoveredLogData), ServerError> {
        let segments_dir = segments_dir.as_ref().to_path_buf();
        durable::create_dir_all_synced(&segments_dir)?;

        let mut log = Self {
            segments_dir,
            active_file: None,
            active_start_seq: None,
            active_end_seq: None,
            active_len: 0,
        };

        let recovered = log.inspect_active_segment()?;
        Ok((log, recovered))
    }

    /// Appends a sequenced operation with an optional mutation ID to the active `.wal` segment and ensures physical durability on disk.
    pub fn append_record(
        &mut self,
        op: &SequencedOperation,
        mutation_id: Option<MutationId>,
    ) -> Result<(), ServerError> {
        let active_path = self.segments_dir.join(ACTIVE_SEGMENT);

        if self.active_file.is_none() {
            let created = !active_path.exists();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&active_path)?;

            file.try_lock_exclusive().map_err(|e| {
                ServerError::RoomLocked(format!("active.wal locked by another process: {}", e))
            })?;
            // A file left behind by a failed rotation still holds its records; append after them.
            self.active_len = file.metadata()?.len();
            self.active_file = Some(file);

            // A new file's directory entry must be durable before any record in it is
            // acknowledged; `sync_data` on the file alone does not persist it.
            if created {
                durable::sync_dir(&self.segments_dir)?;
            }
        }

        let file = self.active_file.as_mut().unwrap();
        let encoded =
            encode_wal_record(op, mutation_id).map_err(|e| ServerError::Wal(e.to_string()))?;

        // Reads of the active segment share this handle and move its position.
        file.seek(SeekFrom::Start(self.active_len))?;
        fail_point::check("warm_append_write", &active_path)?;
        file.write_all(&encoded)?;
        file.flush()?;
        fail_point::check("warm_append_sync", &active_path)?;
        file.sync_data()?;
        self.active_len += encoded.len() as u64;

        // The segment start is tracked independently of when the file handle was opened:
        // an `active.wal` recovered empty already has an open handle but no operations yet.
        if self.active_start_seq.is_none() {
            self.active_start_seq = Some(op.seq);
        }
        self.active_end_seq = Some(op.seq);
        Ok(())
    }

    /// Number of operations currently accumulated in the unsealed active WAL segment.
    pub fn active_ops_count(&self) -> usize {
        match (self.active_start_seq, self.active_end_seq) {
            (Some(start), Some(end)) if end.get() >= start.get() => {
                (end.get() - start.get() + 1) as usize
            }
            _ => 0,
        }
    }

    /// Lowest sequence number present in the currently active WAL segment.
    pub fn active_start_seq(&self) -> Option<SequenceNumber> {
        self.active_start_seq
    }

    /// Highest sequence number present in the currently active WAL segment.
    pub fn active_end_seq(&self) -> Option<SequenceNumber> {
        self.active_end_seq
    }

    /// Size in bytes of the records in the active WAL segment.
    pub fn active_len(&self) -> u64 {
        self.active_len
    }

    /// Rotates and seals `active.wal` into an immutable `segment_{start}_{end}.wal` file.
    pub fn rotate_active_segment(&mut self) -> Result<Option<PathBuf>, ServerError> {
        if let (Some(start), Some(end)) = (self.active_start_seq, self.active_end_seq) {
            // The handle is closed before the rename: renaming a file this process has open
            // would leave it writing to the sealed segment, and closing it releases the lock.
            if let Some(mut file) = self.active_file.take() {
                file.flush()?;
                file.sync_all()?;
            }

            let active_path = self.segments_dir.join(ACTIVE_SEGMENT);
            let sealed_path = warm_segment_path(&self.segments_dir, start, end);

            let renamed = active_path.exists();
            if renamed {
                std::fs::rename(&active_path, &sealed_path)?;
            }

            self.active_start_seq = None;
            self.active_end_seq = None;
            self.active_len = 0;

            // Make the rename durable before the sealed segment can be compressed or pruned.
            if renamed {
                durable::sync_dir(&self.segments_dir)?;
            }
            Ok(Some(sealed_path))
        } else {
            Ok(None)
        }
    }

    /// Reads operations within `(from_seq .. ]` up to `limit` from `active.wal`, through the
    /// handle that holds its lock.
    pub(crate) fn read_active_range(
        &self,
        from_seq: SequenceNumber,
        limit: usize,
    ) -> Result<Vec<SequencedOperation>, ServerError> {
        let Some(file) = self.active_file.as_ref() else {
            return Ok(Vec::new());
        };
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut reader = file;
        reader.seek(SeekFrom::Start(0))?;
        let mut data = vec![0u8; self.active_len as usize];
        reader.read_exact(&mut data)?;
        let decoded = decode_segment(&data, from_seq, limit)?;
        if decoded.torn {
            // Appends are synced and recovery truncates a torn tail, so this means the file
            // was damaged while the room was open.
            tracing::warn!(
                path = ?self.segments_dir.join(ACTIVE_SEGMENT),
                valid_bytes = decoded.valid_len,
                "Torn write detected while reading the active WAL segment"
            );
        }
        Ok(decoded.ops)
    }

    /// Reads operations within `(from_seq .. ]` up to `limit` from a specific sealed `.wal` file.
    pub fn read_range(
        file_path: &Path,
        from_seq: SequenceNumber,
        limit: usize,
    ) -> Result<Vec<SequencedOperation>, ServerError> {
        Self::read_range_with_mutations(file_path, from_seq, limit).map(|(ops, _)| ops)
    }

    /// Reads operations and associated mutation IDs within `(from_seq .. ]` up to `limit` from
    /// a specific sealed `.wal` file. A missing file yields nothing; the caller detects the
    /// gap it leaves.
    pub fn read_range_with_mutations(
        file_path: &Path,
        from_seq: SequenceNumber,
        limit: usize,
    ) -> Result<RecoveredLogData, ServerError> {
        if limit == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        let dir = file_path.parent().unwrap_or_else(|| Path::new(""));
        if file_path
            .file_name()
            .is_some_and(|name| name == ACTIVE_SEGMENT)
        {
            io_probe::record(IoEvent::ActiveWalPathRead, dir);
        } else {
            io_probe::record(IoEvent::SegmentRead, dir);
        }
        let data = match std::fs::read(file_path) {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok((Vec::new(), Vec::new()))
            }
            Err(e) => return Err(e.into()),
        };
        let decoded = decode_segment(&data, from_seq, limit)?;
        if decoded.torn {
            tracing::warn!(
                path = ?file_path,
                valid_bytes = decoded.valid_len,
                "Torn write detected while reading WAL segment"
            );
        }
        Ok((decoded.ops, decoded.mutations))
    }

    /// Inspects and repairs `active.wal` upon opening, reading it through the locked handle
    /// that later appends use, and returns its operations and mutation IDs.
    fn inspect_active_segment(&mut self) -> Result<RecoveredLogData, ServerError> {
        let active_path = self.segments_dir.join(ACTIVE_SEGMENT);
        if !active_path.exists() {
            return Ok((Vec::new(), Vec::new()));
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)?;

        file.try_lock_exclusive().map_err(|e| {
            ServerError::RoomLocked(format!("active.wal locked by another process: {}", e))
        })?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        let decoded = decode_segment(&data, SequenceNumber::new(0), usize::MAX)?;

        if decoded.torn {
            tracing::warn!(
                path = ?active_path,
                valid_len = decoded.valid_len,
                "Truncating torn write in active.wal"
            );
        }
        if decoded.valid_len < data.len() {
            file.set_len(decoded.valid_len as u64)?;
            file.sync_all()?;
        }

        self.active_file = Some(file);
        self.active_start_seq = decoded.ops.first().map(|op| op.seq);
        self.active_end_seq = decoded.ops.last().map(|op| op.seq);
        self.active_len = decoded.valid_len as u64;

        Ok((decoded.ops, decoded.mutations))
    }
}

/// Operations decoded from the framed batches of one segment.
#[derive(Debug, Default)]
pub(crate) struct DecodedSegment {
    pub(crate) ops: Vec<SequencedOperation>,
    pub(crate) mutations: Vec<(MutationId, SequenceNumber)>,
    /// Length of the prefix made of complete batches that were decoded.
    pub(crate) valid_len: usize,
    /// Whether decoding stopped at a torn write: an incomplete or damaged batch at the end,
    /// or a zero-filled tail.
    pub(crate) torn: bool,
}

/// Decodes the operations after `from_seq` in the framed batches of `data`, with the mutation
/// IDs of their batches, up to `limit` operations. Decoding stops at the first torn write;
/// corruption anywhere else is an error.
pub(crate) fn decode_segment(
    data: &[u8],
    from_seq: SequenceNumber,
    limit: usize,
) -> Result<DecodedSegment, ServerError> {
    let mut decoded = DecodedSegment::default();
    let mut offset = 0;

    while offset < data.len() && decoded.ops.len() < limit {
        match decode_wal_batch_from_slice(&data[offset..]) {
            Ok(WalBatchDecodeResult::Ok {
                ops,
                mutation_id,
                bytes_consumed,
            }) => {
                for op in ops {
                    if op.seq.get() > from_seq.get() {
                        if let Some(m_id) = mutation_id {
                            decoded.mutations.push((m_id, op.seq));
                        }
                        decoded.ops.push(op);
                        if decoded.ops.len() >= limit {
                            break;
                        }
                    }
                }
                offset += bytes_consumed;
                decoded.valid_len = offset;
            }
            Ok(WalBatchDecodeResult::CleanEof) => break,
            Ok(WalBatchDecodeResult::TornWrite {
                valid_bytes_offset, ..
            }) => {
                decoded.valid_len = offset + valid_bytes_offset;
                decoded.torn = true;
                break;
            }
            Err(e) => return Err(ServerError::WalCorruption(e.to_string())),
        }
    }

    Ok(decoded)
}

/// Helper function to parse `segment_{start}_{end}{extension}` filenames into (start_seq, end_seq).
pub fn parse_segment_filename(name: &str, extension: &str) -> Option<(u64, u64)> {
    let prefix = "segment_";
    if !name.starts_with(prefix) || !name.ends_with(extension) {
        return None;
    }
    let core = &name[prefix.len()..name.len() - extension.len()];
    let parts: Vec<&str> = core.split('_').collect();
    if parts.len() != 2 {
        return None;
    }
    let start = parts[0].parse::<u64>().ok()?;
    let end = parts[1].parse::<u64>().ok()?;
    Some((start, end))
}

#[cfg(test)]
#[path = "tests/warm_disk.rs"]
mod tests;
