use crate::error::ServerError;
use crate::log::cold_disk::ColdDiskLog;
use crate::log::hot_buffer::HotBuffer;
use crate::log::policy::RoomLifecyclePolicy;
use crate::log::warm_disk::WarmDiskLog;
use rimdb_core::id::{MutationId, SequenceNumber};
use rimdb_core::protocol::messages::SequencedOperation;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Summary of maintenance operations performed across storage tiers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaintenanceReport {
    pub warm_compressed_count: usize,
    pub cold_pruned_count: usize,
    pub new_tail_seq: SequenceNumber,
}

/// Summary of a proactive cursor-driven log pruning operation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PruneReport {
    pub warm_deleted_count: usize,
    pub cold_deleted_count: usize,
    pub new_tail_seq: SequenceNumber,
}

/// Unified 4-tier immutable log coordinator and delta query engine.
///
/// Manages the full lifecycle of sequenced operations across RAM, Warm Disk, Cold Disk,
/// and compaction eviction, enforcing Write-Through durability on every append.
#[derive(Debug)]
pub struct TieredLog {
    dir: PathBuf,
    policy: RoomLifecyclePolicy,
    hot_buffer: HotBuffer,
    warm_disk: WarmDiskLog,
    head_seq: SequenceNumber,
    tail_seq: SequenceNumber,
}

impl TieredLog {
    /// Opens an existing room delta log or creates a new one, running recovery and cache rehydration.
    pub fn open_or_create(
        dir: impl AsRef<Path>,
        policy: RoomLifecyclePolicy,
    ) -> Result<(Self, Vec<(MutationId, SequenceNumber)>), ServerError> {
        let dir = dir.as_ref().to_path_buf();
        let segments_dir = dir.join("segments");
        std::fs::create_dir_all(&segments_dir)?;

        let mut warm_disk = WarmDiskLog::open_or_create(&segments_dir)?;
        let mut recovered_mutations = Vec::new();

        let mut head_seq = SequenceNumber::new(0);
        let mut tail_seq = SequenceNumber::new(0);

        // Check Cold Disk segments first
        let cold_segments = ColdDiskLog::list_cold_segments(&segments_dir)?;
        if let Some(first_cold) = cold_segments.first() {
            tail_seq = first_cold.start_seq;
            head_seq = cold_segments.last().unwrap().end_seq;
            for cold_seg in &cold_segments {
                let (_, muts) = ColdDiskLog::read_range_with_mutations(
                    &cold_seg.path,
                    SequenceNumber::new(0),
                    usize::MAX,
                )?;
                recovered_mutations.extend(muts);
            }
        }

        // Incorporate recovered Warm operations
        let (recovered_ops, warm_muts) = warm_disk.recover_all()?;
        recovered_mutations.extend(warm_muts);

        if let Some(first_op) = recovered_ops.first() {
            if tail_seq.get() == 0 {
                tail_seq = first_op.seq;
            }
            if let Some(last_op) = recovered_ops.last() {
                if last_op.seq.get() > head_seq.get() {
                    head_seq = last_op.seq;
                }
            }
        }

        // Rehydrate HotBuffer in RAM with the most recent operations up to ram_max_ops
        let mut hot_buffer = HotBuffer::new();
        let rehydrate_start = if recovered_ops.len() > policy.ram_max_ops {
            recovered_ops.len() - policy.ram_max_ops
        } else {
            0
        };
        hot_buffer.rehydrate(recovered_ops.into_iter().skip(rehydrate_start));

        Ok((
            Self {
                dir,
                policy,
                hot_buffer,
                warm_disk,
                head_seq,
                tail_seq,
            },
            recovered_mutations,
        ))
    }

    /// Appends a new sequenced operation using the Write-Through durability model.
    ///
    /// 1. Persists synchronously on Warm Disk (`active.wal`) with batch framing and `sync_data()`.
    /// 2. Stores in `HotBuffer` in RAM to resolve immediate `/sync` queries in sub-millisecond.
    /// 3. Rotates and seals segment if size or age threshold is exceeded.
    pub fn append(
        &mut self,
        op: SequencedOperation,
        mutation_id: Option<MutationId>,
    ) -> Result<(), ServerError> {
        if self.head_seq.get() != 0 && op.seq.get() != self.head_seq.get() + 1 {
            return Err(ServerError::Wal(format!(
                "Non-contiguous sequence: expected {}, got {}",
                self.head_seq.get() + 1,
                op.seq.get()
            )));
        }

        // 1. Write-Through to Warm Disk
        self.warm_disk.append_record(&op, mutation_id)?;

        // 2. Add to RAM HotBuffer
        self.hot_buffer.append(op)?;

        if self.tail_seq.get() == 0 {
            self.tail_seq = self.head_seq.next();
        }
        self.head_seq = self.head_seq.next();

        // 3. Segment rotation check
        if self.hot_buffer.should_rotate(&self.policy) {
            if let Some(sealed_path) = self.warm_disk.rotate_active_segment()? {
                if let Some(filename) = sealed_path.file_name().and_then(|n| n.to_str()) {
                    if let Some((_start, end)) = crate::log::warm_disk::parse_segment_filename(filename, ".wal") {
                        self.hot_buffer.evict_older_than(SequenceNumber::new(end + 1));
                    }
                }
            }
        }

        Ok(())
    }

    /// Unified multi-tier delta query engine.
    ///
    /// Transparently streams deltas across Cold Disk, Warm Disk, and RAM HotBuffer,
    /// enforcing strict sequence contiguity and emitting `BehindCompaction` if the cursor is expired.
    pub fn fetch_deltas(
        &self,
        from_seq: SequenceNumber,
        max_batch_size: u32,
    ) -> Result<(Vec<SequencedOperation>, bool), ServerError> {
        // Eviction check (Tier 4 boundary)
        if self.tail_seq.get() > 1 && from_seq.get() < self.tail_seq.get().saturating_sub(1) {
            return Err(ServerError::BehindCompaction);
        }

        if from_seq.get() >= self.head_seq.get() || max_batch_size == 0 {
            return Ok((Vec::new(), false));
        }

        let limit = max_batch_size as usize;
        let mut collected: Vec<SequencedOperation> = Vec::with_capacity(limit.min(1024));
        let mut current_from = from_seq;
        let segments_dir = self.dir.join("segments");

        // 1. Query Tier 3: Cold Disk (.wal.zst)
        let cold_segments = ColdDiskLog::list_cold_segments(&segments_dir)?;
        for cold in cold_segments {
            if cold.end_seq.get() > current_from.get() {
                let ops = ColdDiskLog::read_range(&cold.path, current_from, limit - collected.len())?;
                for op in ops {
                    current_from = op.seq;
                    collected.push(op);
                    if collected.len() >= limit {
                        break;
                    }
                }
            }
            if collected.len() >= limit {
                break;
            }
        }

        // 2. Query Tier 2: Sealed Warm Disk (.wal)
        if collected.len() < limit {
            let sealed_segments = self.warm_disk.list_sealed_segments()?;
            for sealed in sealed_segments {
                if sealed.end_seq.get() > current_from.get() {
                    let ops = WarmDiskLog::read_range(&sealed.path, current_from, limit - collected.len())?;
                    for op in ops {
                        current_from = op.seq;
                        collected.push(op);
                        if collected.len() >= limit {
                            break;
                        }
                    }
                }
                if collected.len() >= limit {
                    break;
                }
            }
        }

        // 3. Query Tier 1: HotBuffer in RAM (Fast path)
        if collected.len() < limit {
            let ram_ops = self.hot_buffer.get_range(current_from, limit - collected.len());
            for op in ram_ops {
                current_from = op.seq;
                collected.push(op);
                if collected.len() >= limit {
                    break;
                }
            }
        }

        // Fallback: if HotBuffer evicted the range but active.wal contains it
        if collected.len() < limit && current_from.get() < self.head_seq.get() {
            let active_path = segments_dir.join("active.wal");
            if active_path.exists() {
                let active_ops = WarmDiskLog::read_range(&active_path, current_from, limit - collected.len())?;
                for op in active_ops {
                    collected.push(op);
                    if collected.len() >= limit {
                        break;
                    }
                }
            }
        }

        let has_more = match collected.last() {
            Some(last) => last.seq.get() < self.head_seq.get(),
            None => false,
        };

        Ok((collected, has_more))
    }

    /// Background maintenance task: compresses aged warm segments to cold Zstd segments,
    /// and prunes expired cold segments according to retention time and disk space quota.
    pub fn run_maintenance(&mut self) -> Result<MaintenanceReport, ServerError> {
        let mut report = MaintenanceReport::default();
        let segments_dir = self.dir.join("segments");

        // 1. Compress aged sealed Warm segments to Cold Disk (.wal.zst)
        let sealed_warm = self.warm_disk.list_sealed_segments()?;
        let now = SystemTime::now();

        for sealed in sealed_warm {
            let metadata = std::fs::metadata(&sealed.path)?;
            let modified = metadata.modified().unwrap_or(now);
            let age = now.duration_since(modified).unwrap_or_default();

            if age >= self.policy.warm_disk_ttl {
                let cold_name = format!(
                    "segment_{:016}_{:016}.wal.zst",
                    sealed.start_seq.get(),
                    sealed.end_seq.get()
                );
                let cold_path = segments_dir.join(cold_name);

                ColdDiskLog::compress_warm_segment(&sealed.path, &cold_path)?;
                report.warm_compressed_count += 1;
            }
        }

        // 2. Prune expired or over-quota Cold Disk segments
        let mut cold_segments = ColdDiskLog::list_cold_segments(&segments_dir)?;
        let mut total_disk_bytes: u64 = 0;

        for entry in std::fs::read_dir(&segments_dir)? {
            let entry = entry?;
            if let Ok(meta) = entry.metadata() {
                total_disk_bytes += meta.len();
            }
        }

        let mut to_delete = Vec::new();

        for cold in &cold_segments {
            let metadata = std::fs::metadata(&cold.path)?;
            let modified = metadata.modified().unwrap_or(now);
            let age = now.duration_since(modified).unwrap_or_default();

            // Check TTL expiration or quota overflow
            if age >= self.policy.cold_disk_ttl || total_disk_bytes > self.policy.max_room_disk_bytes {
                to_delete.push(cold.path.clone());
                total_disk_bytes = total_disk_bytes.saturating_sub(metadata.len());
                report.cold_pruned_count += 1;
            }
        }

        for path in to_delete {
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }

        // 3. Update tail_seq to the oldest retained sequence
        cold_segments = ColdDiskLog::list_cold_segments(&segments_dir)?;
        if let Some(first_cold) = cold_segments.first() {
            self.tail_seq = first_cold.start_seq;
        } else {
            let remaining_warm = self.warm_disk.list_sealed_segments()?;
            if let Some(first_warm) = remaining_warm.first() {
                self.tail_seq = first_warm.start_seq;
            } else if let Some(min_ram) = self.hot_buffer.min_seq() {
                self.tail_seq = min_ram;
            } else {
                self.tail_seq = self.head_seq;
            }
        }

        report.new_tail_seq = self.tail_seq;
        Ok(report)
    }

    /// Highest sequence number committed to the log.
    pub fn head_seq(&self) -> SequenceNumber {
        self.head_seq
    }

    /// Oldest sequence number retained across all tiers (compaction boundary).
    pub fn tail_seq(&self) -> SequenceNumber {
        self.tail_seq
    }

    /// Force rotates the active Warm segment immediately.
    pub fn force_rotate_warm(&mut self) -> Result<Option<PathBuf>, ServerError> {
        let sealed = self.warm_disk.rotate_active_segment()?;
        if let Some(ref path) = sealed {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some((_start, end)) = crate::log::warm_disk::parse_segment_filename(filename, ".wal") {
                    self.hot_buffer.evict_older_than(SequenceNumber::new(end + 1));
                }
            }
        }
        Ok(sealed)
    }

    /// Proactively prunes all sealed Warm and Cold segments where `end_seq < target_seq`.
    /// Also evicts matching deltas from RAM HotBuffer and advances `tail_seq`.
    ///
    /// Any client requesting deltas with a cursor older than the new `tail_seq - 1`
    /// will immediately receive `BehindCompaction`.
    pub fn prune_older_than(&mut self, target_seq: SequenceNumber) -> Result<PruneReport, ServerError> {
        let mut report = PruneReport::default();
        let segments_dir = self.dir.join("segments");

        // 1. Delete sealed Warm segments strictly older than target_seq
        let sealed_warm = self.warm_disk.list_sealed_segments()?;
        for sealed in sealed_warm {
            if sealed.end_seq.get() < target_seq.get() && sealed.path.exists() {
                std::fs::remove_file(&sealed.path)?;
                report.warm_deleted_count += 1;
            }
        }

        // 2. Delete Cold segments strictly older than target_seq
        let cold_segments = ColdDiskLog::list_cold_segments(&segments_dir)?;
        for cold in cold_segments {
            if cold.end_seq.get() < target_seq.get() && cold.path.exists() {
                std::fs::remove_file(&cold.path)?;
                report.cold_deleted_count += 1;
            }
        }

        // 3. Evict from RAM HotBuffer
        self.hot_buffer.evict_older_than(target_seq);

        // 4. Recalculate tail_seq
        let remaining_cold = ColdDiskLog::list_cold_segments(&segments_dir)?;
        if let Some(first_cold) = remaining_cold.first() {
            self.tail_seq = first_cold.start_seq;
        } else {
            let remaining_warm = self.warm_disk.list_sealed_segments()?;
            if let Some(first_warm) = remaining_warm.first() {
                self.tail_seq = first_warm.start_seq;
            } else if let Some(min_ram) = self.hot_buffer.min_seq() {
                self.tail_seq = min_ram;
            } else {
                self.tail_seq = target_seq.min(self.head_seq);
            }
        }

        if self.tail_seq.get() < target_seq.get() && target_seq.get() <= self.head_seq.get() {
            self.tail_seq = target_seq;
        }

        report.new_tail_seq = self.tail_seq;
        Ok(report)
    }
}
