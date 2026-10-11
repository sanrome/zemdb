use crate::durable;
use crate::error::ServerError;
use crate::fail_point;
use crate::log::budget::ReadBudget;
use crate::log::io_probe::{self, IoEvent};
use crate::log::segment_index::warm_segment_path;
use crate::log::segment_reader::{classify_in, read_frames, FrameRead};
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use zemdb_core::id::{MutationId, SequenceNumber};
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::protocol::wal_frame::encode_wal_record;

/// Operations and mutation assignment pairs recovered from disk segments.
pub type RecoveredLogData = (Vec<SequencedOperation>, Vec<(MutationId, SequenceNumber)>);

/// Name of the active segment inside the segments directory.
const ACTIVE_SEGMENT: &str = "active.wal";

/// Read buffer of a segment file.
const READ_BUFFER: usize = 64 * 1024;

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

    /// The handle that holds the lock on `active.wal`, so that tests can modify the file on
    /// Windows, where a second handle could not.
    #[cfg(test)]
    pub(crate) fn active_file(&self) -> Option<&File> {
        self.active_file.as_ref()
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

    /// Reads the operations after `from_seq` that fit in `budget` from `active.wal`, through
    /// the handle that holds its lock.
    pub(crate) fn read_active_range(
        &self,
        from_seq: SequenceNumber,
        budget: &mut ReadBudget,
    ) -> Result<Vec<SequencedOperation>, ServerError> {
        let Some(file) = self.active_file.as_ref() else {
            return Ok(Vec::new());
        };
        if budget.is_exhausted() {
            return Ok(Vec::new());
        }
        let mut reader = BufReader::with_capacity(READ_BUFFER, file);
        reader.seek(SeekFrom::Start(0))?;
        let read = read_frames(
            &mut reader,
            Some(self.active_len),
            self.active_start_seq,
            from_seq,
            budget,
        )?;
        let path = self.segments_dir.join(ACTIVE_SEGMENT);
        // Appends are synced and recovery truncates a torn tail, so a torn write here means
        // the file was damaged while the room was open.
        Ok(finish_file_read(read, reader.get_mut(), self.active_len, &path)?.0)
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
        let start_seq = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| parse_segment_filename(name, ".wal"))
            .map(|(start, _)| SequenceNumber::new(start));
        read_segment_file(file_path, start_seq, from_seq, &mut ReadBudget::ops(limit))
    }

    /// Inspects and repairs `active.wal` upon opening, reading it through the locked handle
    /// that later appends use, and returns its operations and mutation IDs.
    fn inspect_active_segment(&mut self) -> Result<RecoveredLogData, ServerError> {
        let active_path = self.segments_dir.join(ACTIVE_SEGMENT);
        if !active_path.exists() {
            return Ok((Vec::new(), Vec::new()));
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)?;

        file.try_lock_exclusive().map_err(|e| {
            ServerError::RoomLocked(format!("active.wal locked by another process: {}", e))
        })?;

        let len = file.metadata()?.len();
        let mut reader = BufReader::with_capacity(READ_BUFFER, &file);
        let read = read_frames(
            &mut reader,
            Some(len),
            None,
            SequenceNumber::new(0),
            &mut ReadBudget::ops(usize::MAX),
        )?;
        drop(reader);

        let valid_len = match read.anomaly {
            Some((at, anomaly)) => {
                let reason = classify_in(&mut &file, at, anomaly, len)?;
                tracing::warn!(
                    path = ?active_path,
                    valid_len = at,
                    %reason,
                    "Truncating torn write in active.wal"
                );
                at
            }
            None => len,
        };
        if valid_len < len {
            file.set_len(valid_len)?;
            file.sync_all()?;
        }

        self.active_file = Some(file);
        self.active_start_seq = read.ops.first().map(|op| op.seq);
        self.active_end_seq = read.ops.last().map(|op| op.seq);
        self.active_len = valid_len;

        Ok((read.ops, read.mutations))
    }
}

/// Reads the operations after `from_seq` that fit in `budget` from the sealed `.wal` file at
/// `path`, whose first operation is `start_seq`, streaming it. A missing file yields nothing;
/// the caller detects the gap it leaves.
pub(crate) fn read_segment_file(
    path: &Path,
    start_seq: Option<SequenceNumber>,
    from_seq: SequenceNumber,
    budget: &mut ReadBudget,
) -> Result<RecoveredLogData, ServerError> {
    if budget.is_exhausted() {
        return Ok((Vec::new(), Vec::new()));
    }
    let dir = path.parent().unwrap_or_else(|| Path::new(""));
    if path.file_name().is_some_and(|name| name == ACTIVE_SEGMENT) {
        io_probe::record(IoEvent::ActiveWalPathRead, dir);
    } else {
        io_probe::record(IoEvent::SegmentRead, dir);
    }
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), Vec::new())),
        Err(e) => return Err(e.into()),
    };
    let len = file.metadata()?.len();
    let mut reader = BufReader::with_capacity(READ_BUFFER, file);
    let read = read_frames(&mut reader, Some(len), start_seq, from_seq, budget)?;
    finish_file_read(read, reader.get_mut(), len, path)
}

/// Ends a read of the segment file `file`, of length `len`: a frame that could not be read
/// is a torn write, logged, after which the operations before it are returned; or corruption.
fn finish_file_read(
    read: FrameRead,
    file: &mut (impl Read + Seek),
    len: u64,
    path: &Path,
) -> Result<RecoveredLogData, ServerError> {
    if let Some((at, anomaly)) = read.anomaly {
        let reason = classify_in(file, at, anomaly, len)?;
        tracing::warn!(path = ?path, valid_bytes = at, %reason, "Torn write detected while reading WAL segment");
    }
    Ok((read.ops, read.mutations))
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
