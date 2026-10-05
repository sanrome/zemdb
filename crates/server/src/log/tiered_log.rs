use crate::durable;
use crate::error::ServerError;
use crate::log::cold_disk::ColdDiskLog;
use crate::log::hot_buffer::HotBuffer;
use crate::log::policy::RoomLifecyclePolicy;
use crate::log::warm_disk::WarmDiskLog;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use zemdb_core::id::{MutationId, SequenceNumber};
use zemdb_core::protocol::messages::SequencedOperation;

/// File, inside the room directory, recording the highest sequence ever pruned from disk.
const LOG_META_FILE: &str = "log_meta.json";

/// Durable log metadata that must outlive the segments it describes.
#[derive(Debug, Serialize, Deserialize)]
struct LogMeta {
    /// Highest sequence number whose segment has been deleted. Recovery derives `head_seq`
    /// from the segments on disk; once all of them are pruned this is the only record of
    /// how far the sequence advanced, preventing sequence numbers from being reused.
    pruned_through_seq: u64,
}

/// Summary of maintenance operations performed across storage tiers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaintenanceReport {
    pub warm_compressed_count: usize,
    pub cold_pruned_count: usize,
    pub new_tail_seq: SequenceNumber,
}

/// Result of a successful `TieredLog::append`: the record is durable and visible.
#[derive(Debug, Default)]
pub struct AppendOutcome {
    /// Set when sealing the active segment after the append failed. The appended record is
    /// still durable, but the segment files are in an uncertain state and the log should be
    /// reopened from disk.
    pub rotation_error: Option<ServerError>,
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
    pruned_through_seq: SequenceNumber,
}

impl TieredLog {
    /// Opens an existing room delta log or creates a new one, running recovery and cache rehydration.
    pub fn open_or_create(
        dir: impl AsRef<Path>,
        policy: RoomLifecyclePolicy,
    ) -> Result<(Self, Vec<(MutationId, SequenceNumber)>), ServerError> {
        let dir = dir.as_ref().to_path_buf();
        let segments_dir = dir.join("segments");
        durable::create_dir_all_synced(&segments_dir)?;

        let mut warm_disk = WarmDiskLog::open_or_create(&segments_dir)?;
        let mut recovered_mutations = Vec::new();

        let pruned_through_seq = read_pruned_through_seq(&dir)?;
        let mut head_seq = pruned_through_seq;

        // Check Cold Disk segments first
        let cold_segments = ColdDiskLog::list_cold_segments(&segments_dir)?;
        if let Some(last_cold) = cold_segments.last() {
            head_seq = head_seq.max(last_cold.end_seq);
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

        if let Some(last_op) = recovered_ops.last() {
            if last_op.seq.get() > head_seq.get() {
                head_seq = last_op.seq;
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

        let mut log = Self {
            dir,
            policy,
            hot_buffer,
            warm_disk,
            head_seq,
            tail_seq: head_seq.next(),
            pruned_through_seq,
        };
        log.tail_seq = log.compute_tail_seq()?;

        Ok((log, recovered_mutations))
    }

    /// Appends a new sequenced operation using the Write-Through durability model.
    ///
    /// 1. Persists synchronously on Warm Disk (`active.wal`) with batch framing and `sync_data()`.
    /// 2. Stores in `HotBuffer` in RAM to resolve immediate `/sync` queries in sub-millisecond.
    /// 3. Rotates and seals segment if size or age threshold is exceeded.
    ///
    /// An `Err` means the record may not be durable. Once the record is durable the call
    /// returns `Ok`, and a failure to seal the segment afterwards is reported in the outcome.
    pub fn append(
        &mut self,
        op: SequencedOperation,
        mutation_id: Option<MutationId>,
    ) -> Result<AppendOutcome, ServerError> {
        if self.head_seq.get() != 0 && op.seq.get() != self.head_seq.get() + 1 {
            return Err(ServerError::Wal(format!(
                "Non-contiguous sequence: expected {}, got {}",
                self.head_seq.get() + 1,
                op.seq.get()
            )));
        }

        // 1. Write-Through to Warm Disk
        self.warm_disk.append_record(&op, mutation_id)?;

        // 2. Add to RAM HotBuffer and maintain sliding window
        self.hot_buffer.append(op)?;
        self.hot_buffer
            .apply_sliding_window(self.policy.ram_max_ops, self.policy.ram_ttl);

        // `tail_seq` needs no update here: when the log was empty it already pointed at
        // `head_seq + 1`, which is exactly the sequence of the operation just appended.
        self.head_seq = self.head_seq.next();

        // 3. Segment rotation check: rotate active segment on disk when size threshold is reached,
        // retaining recent operations in the RAM HotBuffer sliding window without destructive eviction to zero.
        let mut outcome = AppendOutcome::default();
        if self.warm_disk.active_ops_count() >= self.policy.ram_max_ops {
            if let Err(err) = self.warm_disk.rotate_active_segment() {
                outcome.rotation_error = Some(err);
            }
        }

        Ok(outcome)
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
        if self.is_behind_retention(from_seq) {
            return Err(ServerError::BehindCompaction);
        }

        if from_seq.get() >= self.head_seq.get() || max_batch_size == 0 {
            return Ok((Vec::new(), false));
        }

        let limit = max_batch_size as usize;

        // 1. Fast Path (Tier 1: HotBuffer in RAM)
        // If the requested cursor falls within the current RAM buffer window, serve directly
        // from memory in sub-microsecond time without touching disk or executing system calls.
        if let Some(min_ram) = self.hot_buffer.min_seq() {
            if from_seq.get() + 1 >= min_ram.get() {
                let ops = self.hot_buffer.get_range(from_seq, limit);
                let has_more = ops
                    .last()
                    .is_some_and(|last| last.seq.get() < self.head_seq.get());
                return Ok((ops, has_more));
            }
        }

        let mut collected: Vec<SequencedOperation> = Vec::with_capacity(limit.min(1024));
        let mut current_from = from_seq;
        let segments_dir = self.dir.join("segments");

        // 1. Query Tier 3: Cold Disk (.wal.zst)
        let cold_segments = ColdDiskLog::list_cold_segments(&segments_dir)?;
        for cold in cold_segments {
            if cold.end_seq.get() > current_from.get() {
                let ops =
                    ColdDiskLog::read_range(&cold.path, current_from, limit - collected.len())?;
                for op in ops {
                    if op.seq.get() != current_from.get() + 1 {
                        return Err(sequence_gap("cold", current_from, op.seq));
                    }
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
                    let ops = WarmDiskLog::read_range(
                        &sealed.path,
                        current_from,
                        limit - collected.len(),
                    )?;
                    for op in ops {
                        if op.seq.get() != current_from.get() + 1 {
                            return Err(sequence_gap("warm sealed", current_from, op.seq));
                        }
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

        // 3. Query Tier 2 Active: Active Warm Disk (active.wal)
        // Queried before RAM HotBuffer to bridge the gap between sealed segments
        // and the in-memory sliding window, maintaining strict chronological contiguity.
        if collected.len() < limit && current_from.get() < self.head_seq.get() {
            let active_path = segments_dir.join("active.wal");
            if active_path.exists() {
                let active_ops =
                    WarmDiskLog::read_range(&active_path, current_from, limit - collected.len())?;
                for op in active_ops {
                    if op.seq.get() != current_from.get() + 1 {
                        return Err(sequence_gap("active wal", current_from, op.seq));
                    }
                    current_from = op.seq;
                    collected.push(op);
                    if collected.len() >= limit {
                        break;
                    }
                }
            }
        }

        // 4. Query Tier 1: HotBuffer in RAM
        // If remaining limit exists and cursor has reached the RAM buffer's window,
        // satisfy the remainder directly from memory.
        if collected.len() < limit && current_from.get() < self.head_seq.get() {
            let ram_ops = self
                .hot_buffer
                .get_range(current_from, limit - collected.len());
            for op in ram_ops {
                if op.seq.get() != current_from.get() + 1 {
                    return Err(sequence_gap("hot buffer", current_from, op.seq));
                }
                current_from = op.seq;
                collected.push(op);
                if collected.len() >= limit {
                    break;
                }
            }
        }

        let has_more = match collected.last() {
            Some(last) => last.seq.get() < self.head_seq.get(),
            None => false,
        };

        Ok((collected, has_more))
    }

    /// Background maintenance task: compresses aged warm segments to cold Zstd segments
    /// delegating heavy CPU encoding to `spawn_blocking`, and prunes expired cold segments.
    pub async fn run_maintenance(&mut self) -> Result<MaintenanceReport, ServerError> {
        let mut report = MaintenanceReport::default();
        let segments_dir = self.dir.join("segments");

        // 1. Compress aged sealed Warm segments to Cold Disk (.wal.zst) asynchronously
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

                ColdDiskLog::compress_warm_segment(&sealed.path, &cold_path).await?;
                report.warm_compressed_count += 1;
            }
        }

        self.prune_and_update_tail(&mut report)?;
        Ok(report)
    }

    /// Synchronous variant of `run_maintenance` for synchronous callers and tests.
    pub fn run_maintenance_sync(&mut self) -> Result<MaintenanceReport, ServerError> {
        let mut report = MaintenanceReport::default();
        let segments_dir = self.dir.join("segments");

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

                ColdDiskLog::compress_warm_segment_sync(&sealed.path, &cold_path)?;
                report.warm_compressed_count += 1;
            }
        }

        self.prune_and_update_tail(&mut report)?;
        Ok(report)
    }

    fn prune_and_update_tail(&mut self, report: &mut MaintenanceReport) -> Result<(), ServerError> {
        let segments_dir = self.dir.join("segments");
        let now = SystemTime::now();

        // 2. Prune expired or over-quota Cold Disk segments
        let cold_segments = ColdDiskLog::list_cold_segments(&segments_dir)?;
        let mut total_disk_bytes: u64 = 0;

        for entry in std::fs::read_dir(&segments_dir)? {
            let entry = entry?;
            if let Ok(meta) = entry.metadata() {
                total_disk_bytes += meta.len();
            }
        }

        let mut to_delete = Vec::new();
        let mut max_pruned_seq: Option<SequenceNumber> = None;

        for cold in &cold_segments {
            let metadata = std::fs::metadata(&cold.path)?;
            let modified = metadata.modified().unwrap_or(now);
            let age = now.duration_since(modified).unwrap_or_default();

            // Check TTL expiration or quota overflow
            if age >= self.policy.cold_disk_ttl
                || total_disk_bytes > self.policy.max_room_disk_bytes
            {
                to_delete.push(cold.path.clone());
                total_disk_bytes = total_disk_bytes.saturating_sub(metadata.len());
                report.cold_pruned_count += 1;
                max_pruned_seq = Some(match max_pruned_seq {
                    Some(cur) => cur.max(cold.end_seq),
                    None => cold.end_seq,
                });
            }
        }

        if let Some(pruned_end) = max_pruned_seq {
            self.record_pruned_through(pruned_end)?;
        }
        for path in to_delete {
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }

        if let Some(pruned_end) = max_pruned_seq {
            self.hot_buffer
                .evict_older_than(SequenceNumber::new(pruned_end.get() + 1));
        }

        // 3. Update tail_seq to the oldest retained sequence
        self.tail_seq = self.compute_tail_seq()?;

        report.new_tail_seq = self.tail_seq;
        Ok(())
    }

    /// Durably records that every sequence up to `seq` is about to be deleted from disk.
    ///
    /// Must complete before the segments are removed, so that a crash in between can never
    /// leave the log without any record of its highest sequence.
    fn record_pruned_through(&mut self, seq: SequenceNumber) -> Result<(), ServerError> {
        if seq.get() <= self.pruned_through_seq.get() {
            return Ok(());
        }
        let meta = LogMeta {
            pruned_through_seq: seq.get(),
        };
        let bytes = serde_json::to_vec(&meta).map_err(|e| {
            ServerError::Serialization(format!("Failed to encode log metadata: {}", e))
        })?;
        durable::write_atomic(&self.dir.join(LOG_META_FILE), &bytes)?;
        self.pruned_through_seq = seq;
        Ok(())
    }

    /// Computes the oldest sequence number still physically retained on disk.
    ///
    /// Every operation is written to disk before entering the RAM buffer, so disk tiers alone
    /// define retention: the first cold segment, else the first sealed warm segment, else the
    /// start of `active.wal`. When nothing is retained the tail is `head_seq + 1`, so that a
    /// cursor at `head_seq - 1` is rejected instead of silently missing operation `head_seq`.
    fn compute_tail_seq(&self) -> Result<SequenceNumber, ServerError> {
        let segments_dir = self.dir.join("segments");

        if let Some(first_cold) = ColdDiskLog::list_cold_segments(&segments_dir)?.first() {
            return Ok(first_cold.start_seq);
        }
        if let Some(first_sealed) = self.warm_disk.list_sealed_segments()?.first() {
            return Ok(first_sealed.start_seq);
        }
        if let Some(active_start) = self.warm_disk.active_start_seq() {
            return Ok(active_start);
        }
        Ok(self.head_seq.next())
    }

    /// Returns true if a client whose cursor is `cursor` can no longer be caught up from the log,
    /// because the operation right after its cursor has already been pruned.
    pub fn is_behind_retention(&self, cursor: SequenceNumber) -> bool {
        cursor.get().saturating_add(1) < self.tail_seq.get()
    }

    /// Highest sequence number committed to the log.
    pub fn head_seq(&self) -> SequenceNumber {
        self.head_seq
    }

    /// Oldest sequence number retained across all tiers (compaction boundary).
    pub fn tail_seq(&self) -> SequenceNumber {
        self.tail_seq
    }

    /// Force rotates the active Warm segment immediately without evicting the RAM HotBuffer.
    pub fn force_rotate_warm(&mut self) -> Result<Option<PathBuf>, ServerError> {
        let sealed = self.warm_disk.rotate_active_segment()?;
        Ok(sealed)
    }

    /// Proactively prunes all sealed Warm and Cold segments where `end_seq < target_seq`.
    /// Also evicts matching deltas from RAM HotBuffer and advances `tail_seq`.
    ///
    /// Any client requesting deltas with a cursor older than the new `tail_seq - 1`
    /// will immediately receive `BehindCompaction`.
    pub fn prune_older_than(
        &mut self,
        target_seq: SequenceNumber,
    ) -> Result<PruneReport, ServerError> {
        let mut report = PruneReport::default();
        let segments_dir = self.dir.join("segments");

        let sealed_to_delete: Vec<_> = self
            .warm_disk
            .list_sealed_segments()?
            .into_iter()
            .filter(|sealed| sealed.end_seq.get() < target_seq.get())
            .collect();
        let cold_to_delete: Vec<_> = ColdDiskLog::list_cold_segments(&segments_dir)?
            .into_iter()
            .filter(|cold| cold.end_seq.get() < target_seq.get())
            .collect();

        // 1. Record how far the log advanced before any segment disappears from disk
        let max_pruned_seq = sealed_to_delete
            .iter()
            .map(|sealed| sealed.end_seq)
            .chain(cold_to_delete.iter().map(|cold| cold.end_seq))
            .max();
        if let Some(pruned_end) = max_pruned_seq {
            self.record_pruned_through(pruned_end)?;
        }

        // 2. Delete sealed Warm and Cold segments strictly older than target_seq
        for sealed in sealed_to_delete {
            if sealed.path.exists() {
                std::fs::remove_file(&sealed.path)?;
                report.warm_deleted_count += 1;
            }
        }
        for cold in cold_to_delete {
            if cold.path.exists() {
                std::fs::remove_file(&cold.path)?;
                report.cold_deleted_count += 1;
            }
        }

        // 3. Evict from RAM HotBuffer
        self.hot_buffer.evict_older_than(target_seq);

        // 4. Recalculate tail_seq from the oldest physically retained segment
        self.tail_seq = self.compute_tail_seq()?;

        report.new_tail_seq = self.tail_seq;
        Ok(report)
    }
}

/// Handles a gap found while reading a tier: the log claims to retain the range but an
/// operation is missing. The client cannot be caught up from the log, so it is reported as
/// `BehindCompaction` to send it to a snapshot, and logged as a server-side fault.
fn sequence_gap(tier: &str, cursor: SequenceNumber, found: SequenceNumber) -> ServerError {
    tracing::error!(
        tier,
        expected = cursor.get() + 1,
        found = found.get(),
        "Sequence gap in retained log range"
    );
    ServerError::BehindCompaction
}

/// Reads the highest pruned sequence recorded for the log in `dir`, or zero if nothing was ever pruned.
///
/// An unreadable file is an error rather than zero: silently restarting the sequence would reuse
/// sequence numbers already delivered to clients.
fn read_pruned_through_seq(dir: &Path) -> Result<SequenceNumber, ServerError> {
    let path = dir.join(LOG_META_FILE);
    match std::fs::remove_file(durable::tmp_path_for(&path)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(SequenceNumber::new(0)),
        Err(e) => return Err(e.into()),
    };
    let meta: LogMeta = serde_json::from_slice(&bytes).map_err(|e| {
        ServerError::WalCorruption(format!("Unreadable log metadata {:?}: {}", path, e))
    })?;
    Ok(SequenceNumber::new(meta.pruned_through_seq))
}

#[cfg(test)]
mod tests;
