use crate::error::ServerError;
use fs2::FileExt;
use rimdb_core::id::{MutationId, SequenceNumber};
use rimdb_core::protocol::messages::SequencedOperation;
use rimdb_core::protocol::wal_frame::{
    decode_wal_batch_from_slice, encode_wal_record, WalBatchDecodeResult,
};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Metadata describing a sealed uncompressed Warm Disk segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedSegmentMeta {
    pub start_seq: SequenceNumber,
    pub end_seq: SequenceNumber,
    pub path: PathBuf,
}

/// Operations and mutation assignment pairs recovered from disk segments.
pub type RecoveredLogData = (Vec<SequencedOperation>, Vec<(MutationId, SequenceNumber)>);

/// Tier 2: Uncompressed append-only log on disk ensuring crash durability and fast sequential reads.
#[derive(Debug)]
pub struct WarmDiskLog {
    segments_dir: PathBuf,
    active_file: Option<File>,
    active_start_seq: Option<SequenceNumber>,
    active_end_seq: Option<SequenceNumber>,
}

impl WarmDiskLog {
    /// Opens or creates the Warm Disk directory.
    pub fn open_or_create(segments_dir: impl AsRef<Path>) -> Result<Self, ServerError> {
        let segments_dir = segments_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&segments_dir)?;

        let mut log = Self {
            segments_dir,
            active_file: None,
            active_start_seq: None,
            active_end_seq: None,
        };

        log.inspect_active_segment()?;
        Ok(log)
    }

    /// Appends a sequenced operation with an optional mutation ID to the active `.wal` segment and ensures physical durability on disk.
    pub fn append_record(
        &mut self,
        op: &SequencedOperation,
        mutation_id: Option<MutationId>,
    ) -> Result<(), ServerError> {
        let active_path = self.segments_dir.join("active.wal");

        if self.active_file.is_none() {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&active_path)?;

            file.try_lock_exclusive().map_err(|e| {
                ServerError::RoomLocked(format!("active.wal locked by another process: {}", e))
            })?;

            self.active_file = Some(file);
            if self.active_start_seq.is_none() {
                self.active_start_seq = Some(op.seq);
            }
        }

        let file = self.active_file.as_mut().unwrap();
        let encoded =
            encode_wal_record(op, mutation_id).map_err(|e| ServerError::Wal(e.to_string()))?;

        file.write_all(&encoded)?;
        file.flush()?;
        file.sync_data()?;

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

    /// Rotates and seals `active.wal` into an immutable `segment_{start}_{end}.wal` file.
    pub fn rotate_active_segment(&mut self) -> Result<Option<PathBuf>, ServerError> {
        if let (Some(start), Some(end)) = (self.active_start_seq, self.active_end_seq) {
            // Drop file handle to allow renaming on all platforms
            if let Some(mut file) = self.active_file.take() {
                file.flush()?;
                file.sync_all()?;
            }

            let active_path = self.segments_dir.join("active.wal");
            let sealed_name = format!("segment_{:016}_{:016}.wal", start.get(), end.get());
            let sealed_path = self.segments_dir.join(sealed_name);

            if active_path.exists() {
                std::fs::rename(&active_path, &sealed_path)?;
            }

            self.active_start_seq = None;
            self.active_end_seq = None;
            Ok(Some(sealed_path))
        } else {
            Ok(None)
        }
    }

    /// Lists all sealed `.wal` segments in ascending sequence order.
    pub fn list_sealed_segments(&self) -> Result<Vec<SealedSegmentMeta>, ServerError> {
        let mut segments = Vec::new();

        if !self.segments_dir.exists() {
            return Ok(segments);
        }

        for entry in std::fs::read_dir(&self.segments_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("segment_") && filename.ends_with(".wal") {
                        if let Some((start, end)) = parse_segment_filename(filename, ".wal") {
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

    /// Reads operations within `(from_seq .. ]` up to `limit` from a specific `.wal` file.
    pub fn read_range(
        file_path: &Path,
        from_seq: SequenceNumber,
        limit: usize,
    ) -> Result<Vec<SequencedOperation>, ServerError> {
        Self::read_range_with_mutations(file_path, from_seq, limit).map(|(ops, _)| ops)
    }

    /// Reads operations and associated mutation IDs within `(from_seq .. ]` up to `limit` from a specific `.wal` file.
    pub fn read_range_with_mutations(
        file_path: &Path,
        from_seq: SequenceNumber,
        limit: usize,
    ) -> Result<RecoveredLogData, ServerError> {
        if limit == 0 || !file_path.exists() {
            return Ok((Vec::new(), Vec::new()));
        }

        let data = std::fs::read(file_path)?;
        let mut offset = 0;
        let mut collected = Vec::new();
        let mut mutations = Vec::new();

        while offset < data.len() && collected.len() < limit {
            match decode_wal_batch_from_slice(&data[offset..]) {
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
                        path = ?file_path,
                        valid_bytes_offset = offset + valid_bytes_offset,
                        "Torn write detected while reading WAL segment"
                    );
                    break;
                }
                Err(e) => return Err(ServerError::WalCorruption(e.to_string())),
            }
        }

        Ok((collected, mutations))
    }

    /// Recovers all operations and mutation IDs present in sealed segments and active.wal in chronological order.
    pub fn recover_all(&mut self) -> Result<RecoveredLogData, ServerError> {
        let mut all_ops = Vec::new();
        let mut all_mutations = Vec::new();

        for sealed in self.list_sealed_segments()? {
            let (ops, muts) =
                Self::read_range_with_mutations(&sealed.path, SequenceNumber::new(0), usize::MAX)?;
            all_ops.extend(ops);
            all_mutations.extend(muts);
        }

        let active_path = self.segments_dir.join("active.wal");
        if active_path.exists() {
            let (active_ops, active_muts) =
                Self::read_range_with_mutations(&active_path, SequenceNumber::new(0), usize::MAX)?;
            all_ops.extend(active_ops);
            all_mutations.extend(active_muts);
        }

        Ok((all_ops, all_mutations))
    }

    /// Inspects and repairs `active.wal` upon opening, seeking to the end for subsequent appends.
    fn inspect_active_segment(&mut self) -> Result<(), ServerError> {
        let active_path = self.segments_dir.join("active.wal");
        if !active_path.exists() {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)?;

        file.try_lock_exclusive().map_err(|e| {
            ServerError::RoomLocked(format!("active.wal locked by another process: {}", e))
        })?;

        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut file, &mut data)?;

        let mut offset = 0;
        let mut start_seq = None;
        let mut end_seq = None;
        let mut valid_len = 0;

        while offset < data.len() {
            match decode_wal_batch_from_slice(&data[offset..]) {
                Ok(WalBatchDecodeResult::Ok {
                    ops,
                    bytes_consumed,
                    ..
                }) => {
                    for op in ops {
                        if start_seq.is_none() {
                            start_seq = Some(op.seq);
                        }
                        end_seq = Some(op.seq);
                    }
                    offset += bytes_consumed;
                    valid_len = offset;
                }
                Ok(WalBatchDecodeResult::CleanEof) => break,
                Ok(WalBatchDecodeResult::TornWrite {
                    valid_bytes_offset, ..
                }) => {
                    valid_len = offset + valid_bytes_offset;
                    tracing::warn!(
                        path = ?active_path,
                        valid_len = valid_len,
                        "Truncating torn write in active.wal"
                    );
                    break;
                }
                Err(e) => return Err(ServerError::WalCorruption(e.to_string())),
            }
        }

        if valid_len < data.len() {
            file.set_len(valid_len as u64)?;
            file.sync_all()?;
        }

        file.seek(SeekFrom::End(0))?;
        self.active_file = Some(file);
        self.active_start_seq = start_seq;
        self.active_end_seq = end_seq;

        Ok(())
    }
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
