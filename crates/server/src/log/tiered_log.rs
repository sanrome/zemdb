use crate::blocking::blocking_io;
use crate::durable;
use crate::error::ServerError;
use crate::log::budget::{op_bytes, ReadBudget};
use crate::log::cold_disk::{read_cold_file, ColdDiskLog, CompressedSegment};
use crate::log::hot_buffer::HotBuffer;
use crate::log::policy::RoomLifecyclePolicy;
use crate::log::retention;
use crate::log::segment_index::{cold_segment_path, SegmentEntry, SegmentIndex, Tier};
use crate::log::warm_disk::{read_segment_file, RecoveredLogData, WarmDiskLog};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};
use zemdb_core::id::{MutationId, SequenceNumber};
use zemdb_core::protocol::limits::MAX_RESPONSE_OPS_BYTES;
use zemdb_core::protocol::messages::SequencedOperation;

/// Warm segments one maintenance pass tries to compress. The room actor waits for the pass,
/// so this bounds how long it stops serving requests (about 0.1–0.3 s per segment of
/// `segment_max_bytes`), whether the compressions succeed or fail late; the next pass
/// compresses the rest. Segments whose last compression failed are tried after the others,
/// so they do not hold back the newer ones.
const MAX_COMPRESSIONS_PER_PASS: usize = 4;

/// File, inside the room directory, recording the highest sequence ever pruned from disk.
const LOG_META_FILE: &str = "log_meta.json";

/// File, inside the room directory, whose exclusive lock a log holds while it is open. It holds
/// no data and is never read, and nothing depends on it surviving a crash, so its creation is
/// not synced.
const LOG_LOCK_FILE: &str = "log.lock";

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

/// The current time as maintenance sees it. Segment ages are wall-clock durations, because
/// after a reopen they come from file modification times; the ages of operations in RAM are
/// monotonic.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaintenanceClock {
    pub(crate) wall: SystemTime,
    pub(crate) mono: Instant,
}

impl MaintenanceClock {
    pub(crate) fn now() -> Self {
        Self {
            wall: SystemTime::now(),
            mono: Instant::now(),
        }
    }

    /// The time `elapsed` from now, so that tests can age the log without sleeping.
    #[cfg(test)]
    pub(crate) fn after(elapsed: std::time::Duration) -> Self {
        let now = Self::now();
        Self {
            wall: now.wall + elapsed,
            mono: now.mono + elapsed,
        }
    }
}

/// Unified 4-tier immutable log coordinator and delta query engine.
///
/// Manages the full lifecycle of sequenced operations across RAM, Warm Disk, Cold Disk,
/// and compaction eviction, enforcing Write-Through durability on every append.
///
/// The sealed segments on disk are tracked in an in-memory [`SegmentIndex`], built with one
/// listing of the segments directory on open and updated on every rotation, compression and
/// prune; only opening the log lists the directory.
#[derive(Debug)]
pub struct TieredLog {
    dir: PathBuf,
    segments_dir: PathBuf,
    policy: RoomLifecyclePolicy,
    hot_buffer: HotBuffer,
    warm_disk: WarmDiskLog,
    segments: SegmentIndex,
    /// The warm segments whose last compression failed, by first sequence, with the pass in
    /// which it failed.
    failed_compressions: HashMap<SequenceNumber, u64>,
    /// Maintenance passes run since the log was opened.
    maintenance_passes: u64,
    head_seq: SequenceNumber,
    tail_seq: SequenceNumber,
    pruned_through_seq: SequenceNumber,
    /// Exclusive lock on the room's log, taken before opening touches any file and released
    /// when the log is dropped. `active.wal` has its own lock, but only while it exists, and
    /// opening reads and cleans up other files before it gets to it.
    _lock: File,
}

impl TieredLog {
    /// Opens an existing room delta log or creates a new one, running recovery and cache
    /// rehydration, and returns every mutation ID still in the log, oldest first.
    pub fn open_or_create(
        dir: impl AsRef<Path>,
        policy: RoomLifecyclePolicy,
    ) -> Result<(Self, Vec<(MutationId, SequenceNumber)>), ServerError> {
        Self::open_or_create_with_dedup_window(dir, policy, usize::MAX)
    }

    /// Opens an existing room delta log or creates a new one, and returns the last
    /// `dedup_window` mutation IDs in the log, oldest first.
    ///
    /// The log is read backwards from its newest segment only until it has the last
    /// operations that fit in RAM (`ram_max_ops` and `ram_max_bytes`) and the last
    /// `dedup_window` mutation IDs, keeping only those operations,
    /// so the cost of opening a room does not grow with its retained history. `head_seq`
    /// comes from the segment names, `active.wal` and `log_meta.json`.
    pub fn open_or_create_with_dedup_window(
        dir: impl AsRef<Path>,
        policy: RoomLifecyclePolicy,
        dedup_window: usize,
    ) -> Result<(Self, Vec<(MutationId, SequenceNumber)>), ServerError> {
        let dir = dir.as_ref().to_path_buf();
        let segments_dir = dir.join("segments");
        durable::create_dir_all_synced(&segments_dir)?;
        let lock = lock_log(&dir)?;

        let pruned_through_seq = read_pruned_through_seq(&dir)?;
        let segments = SegmentIndex::scan(&segments_dir)?;
        let (warm_disk, active) = WarmDiskLog::open_recovering(&segments_dir)?;

        let head_seq = [
            Some(pruned_through_seq),
            segments.last_end(),
            warm_disk.active_end_seq(),
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(pruned_through_seq);

        let (ops, mutations) = read_newest(&segments, active, &policy, dedup_window)?;
        let mutations = newest_distinct(mutations, dedup_window);

        let mut hot_buffer = HotBuffer::new();
        hot_buffer.rehydrate(contiguous_suffix(ops, policy.ram_max_ops, head_seq));
        hot_buffer.apply_sliding_window(&policy);

        let mut log = Self {
            dir,
            segments_dir,
            policy,
            hot_buffer,
            warm_disk,
            segments,
            failed_compressions: HashMap::new(),
            maintenance_passes: 0,
            head_seq,
            tail_seq: head_seq.next(),
            pruned_through_seq,
            _lock: lock,
        };
        log.tail_seq = log.compute_tail_seq();

        Ok((log, mutations))
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
        blocking_io(|| self.warm_disk.append_record(&op, mutation_id))?;

        // 2. Add to RAM HotBuffer and maintain sliding window
        self.hot_buffer.append(op)?;
        self.hot_buffer.apply_sliding_window(&self.policy);

        // `tail_seq` needs no update here: when the log was empty it already pointed at
        // `head_seq + 1`, which is exactly the sequence of the operation just appended.
        self.head_seq = self.head_seq.next();

        // 3. Segment rotation check: rotate active segment on disk when it reaches `ram_max_ops`
        // operations or `segment_max_bytes` bytes, retaining recent operations in the RAM
        // HotBuffer sliding window without destructive eviction to zero.
        let mut outcome = AppendOutcome::default();
        if self.warm_disk.active_ops_count() >= self.policy.ram_max_ops
            || self.warm_disk.active_len() >= self.policy.segment_max_bytes
        {
            if let Err(err) = blocking_io(|| self.seal_active_segment()) {
                outcome.rotation_error = Some(err);
            }
        }

        Ok(outcome)
    }

    /// Seals `active.wal` into a warm segment and indexes it.
    fn seal_active_segment(&mut self) -> Result<Option<PathBuf>, ServerError> {
        let (Some(start_seq), Some(end_seq)) = (
            self.warm_disk.active_start_seq(),
            self.warm_disk.active_end_seq(),
        ) else {
            return Ok(None);
        };
        let bytes = self.warm_disk.active_len();
        let sealed = self.warm_disk.rotate_active_segment()?;
        if let Some(path) = &sealed {
            self.segments.insert(SegmentEntry {
                start_seq,
                end_seq,
                tier: Tier::Warm,
                path: path.clone(),
                bytes,
                since: SystemTime::now(),
            });
        }
        Ok(sealed)
    }

    /// Unified multi-tier delta query engine.
    ///
    /// Transparently streams deltas across Cold Disk, Warm Disk, and RAM HotBuffer,
    /// enforcing strict sequence contiguity and emitting `BehindCompaction` if the cursor is expired.
    ///
    /// Returns at most `max_batch_size` operations that fit in one response
    /// ([`MAX_RESPONSE_OPS_BYTES`]), and at least one: operations beyond the budget are neither
    /// decoded into the batch nor cloned.
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

        let budget = ReadBudget::new(max_batch_size as usize, MAX_RESPONSE_OPS_BYTES);

        // 1. Fast Path (Tier 1: HotBuffer in RAM)
        // If the requested cursor falls within the current RAM buffer window, serve directly
        // from memory in sub-microsecond time without touching disk or executing system calls.
        if let Some(min_ram) = self.hot_buffer.min_seq() {
            if from_seq.get() + 1 >= min_ram.get() {
                let ops = self.hot_buffer.get_range(from_seq, &mut budget.clone());
                if let Some(last) = ops.last() {
                    let has_more = last.seq.get() < self.head_seq.get();
                    return Ok((ops, has_more));
                }
            }
        }

        // 2. Disk tiers, oldest first: the indexed sealed segments (cold, then warm), then
        // `active.wal`, which bridges the sealed segments and the RAM window.
        let mut batch = DeltaBatch::new(from_seq, budget);
        blocking_io(|| self.fetch_from_disk(&mut batch))?;

        // 3. Tier 1: if the batch reached the RAM buffer's window, the rest comes from memory.
        if !batch.is_full() && batch.cursor.get() < self.head_seq.get() {
            let ram_ops = self.hot_buffer.get_range(batch.cursor, &mut batch.budget);
            batch.extend("hot buffer", ram_ops)?;
        }

        // The range is retained, yet no tier holds the operation after the cursor (a missing
        // segment at the end of the disk tiers, or names that claim more than the files hold).
        // An empty answer would tell the client it is up to date, so this is a gap.
        if batch.ops.is_empty() {
            tracing::error!(
                cursor = from_seq.get(),
                head_seq = self.head_seq.get(),
                "No operation after the cursor in the retained log range"
            );
            return Err(ServerError::BehindCompaction);
        }

        let has_more = batch.cursor.get() < self.head_seq.get();
        Ok((batch.ops, has_more))
    }

    /// Fills `batch` from the sealed segments and `active.wal`.
    fn fetch_from_disk(&self, batch: &mut DeltaBatch) -> Result<(), ServerError> {
        for segment in self.segments.iter() {
            if batch.is_full() {
                return Ok(());
            }
            if segment.end_seq.get() <= batch.cursor.get() {
                continue;
            }
            let (ops, _) = read_segment(segment, batch.cursor, &mut batch.budget)
                .map_err(|err| unreadable_range(&segment.path, err))?;
            batch.extend(segment.tier.name(), ops)?;
        }
        if !batch.is_full() && batch.cursor.get() < self.head_seq.get() {
            let ops = self
                .warm_disk
                .read_active_range(batch.cursor, &mut batch.budget)
                .map_err(|err| unreadable_range(&self.segments_dir.join("active.wal"), err))?;
            batch.extend("active wal", ops)?;
        }
        Ok(())
    }

    /// Disk maintenance: compresses aged warm segments into cold Zstd segments (at most
    /// [`MAX_COMPRESSIONS_PER_PASS`]), delegating the compression to `spawn_blocking`, applies
    /// the RAM TTL window, and prunes cold segments past their TTL or over the room's disk
    /// quota.
    ///
    /// A failed compression does not stop the other passes: the remaining segments are still
    /// compressed and the TTL and quota still apply. The first error is returned at the end.
    pub async fn run_maintenance(&mut self) -> Result<MaintenanceReport, ServerError> {
        let clock = MaintenanceClock::now();
        let mut report = MaintenanceReport::default();
        let mut first_error = None;

        for segment in self.warm_segments_due(clock.wall) {
            let cold_path =
                cold_segment_path(&self.segments_dir, segment.start_seq, segment.end_seq);
            let result = ColdDiskLog::compress_warm_segment(&segment.path, &cold_path).await;
            if let Err(err) =
                self.record_compression(segment, cold_path, result, clock, &mut report)
            {
                first_error.get_or_insert(err);
            }
        }

        let expired = blocking_io(|| self.expire(clock, &mut report));
        first_error.map_or(expired, Err)?;
        Ok(report)
    }

    /// Synchronous variant of `run_maintenance` for synchronous callers and tests.
    pub fn run_maintenance_sync(&mut self) -> Result<MaintenanceReport, ServerError> {
        self.run_maintenance_sync_at(MaintenanceClock::now())
    }

    /// [`run_maintenance_sync`](Self::run_maintenance_sync) at the time `clock`.
    pub(crate) fn run_maintenance_sync_at(
        &mut self,
        clock: MaintenanceClock,
    ) -> Result<MaintenanceReport, ServerError> {
        blocking_io(|| {
            let mut report = MaintenanceReport::default();
            let mut first_error = None;
            for segment in self.warm_segments_due(clock.wall) {
                let cold_path =
                    cold_segment_path(&self.segments_dir, segment.start_seq, segment.end_seq);
                let result = ColdDiskLog::compress_warm_segment_sync(&segment.path, &cold_path);
                if let Err(err) =
                    self.record_compression(segment, cold_path, result, clock, &mut report)
                {
                    first_error.get_or_insert(err);
                }
            }
            let expired = self.expire(clock, &mut report);
            first_error.map_or(expired, Err)?;
            Ok(report)
        })
    }

    /// Updates the index after compressing the warm `segment` into `cold_path`. Whenever the
    /// cold copy is in place the index points at it, even if removing the warm file failed:
    /// the warm path may no longer exist.
    fn record_compression(
        &mut self,
        segment: SegmentEntry,
        cold_path: PathBuf,
        result: Result<CompressedSegment, ServerError>,
        clock: MaintenanceClock,
        report: &mut MaintenanceReport,
    ) -> Result<(), ServerError> {
        let compressed = match result {
            Ok(compressed) => compressed,
            Err(err) => {
                self.failed_compressions
                    .insert(segment.start_seq, self.maintenance_passes);
                return Err(err);
            }
        };
        self.failed_compressions.remove(&segment.start_seq);
        if let Some(err) = &compressed.cleanup_error {
            tracing::warn!(
                path = ?segment.path,
                error = %err,
                "Warm segment compressed, but removing it failed; the next open removes the duplicate"
            );
        }
        self.mark_compressed(segment, cold_path, compressed.bytes, clock.wall);
        report.warm_compressed_count += 1;
        Ok(())
    }

    /// The warm segments to compress in this pass: those sealed for at least `warm_disk_ttl`
    /// at `now`, at most [`MAX_COMPRESSIONS_PER_PASS`]. Those that never failed go first,
    /// oldest first; then those whose last failure is oldest, so that retries rotate through
    /// every failed segment instead of always retrying the same ones.
    fn warm_segments_due(&mut self, now: SystemTime) -> Vec<SegmentEntry> {
        self.maintenance_passes += 1;
        let mut due: Vec<SegmentEntry> = self
            .segments
            .iter()
            .filter(|segment| segment.tier == Tier::Warm)
            .filter(|segment| age(segment, now) >= self.policy.warm_disk_ttl)
            .cloned()
            .collect();
        self.failed_compressions
            .retain(|start, _| due.iter().any(|segment| segment.start_seq == *start));
        due.sort_by_key(|segment| self.failed_compressions.get(&segment.start_seq).copied());
        due.truncate(MAX_COMPRESSIONS_PER_PASS);
        due
    }

    /// Replaces the warm `segment` in the index with its cold copy.
    fn mark_compressed(
        &mut self,
        segment: SegmentEntry,
        cold_path: PathBuf,
        bytes: u64,
        now: SystemTime,
    ) {
        self.segments.insert(SegmentEntry {
            tier: Tier::Cold,
            path: cold_path,
            bytes,
            since: now,
            ..segment
        });
    }

    /// Applies the RAM TTL window and prunes the oldest cold segments while they are past
    /// `cold_disk_ttl` or the log is over `max_room_disk_bytes`.
    ///
    /// Every operation in RAM is also on disk, and retention is defined by the disk tiers
    /// alone, so evicting expired operations from RAM never moves `tail_seq`.
    fn expire(
        &mut self,
        clock: MaintenanceClock,
        report: &mut MaintenanceReport,
    ) -> Result<(), ServerError> {
        self.hot_buffer
            .apply_sliding_window_at(&self.policy, clock.mono);

        // Only a prefix of the log is ever pruned, so the retained range stays contiguous.
        let mut disk_bytes = self.disk_bytes();
        let mut doomed = Vec::new();
        for segment in self.segments.iter() {
            let expired = segment.tier == Tier::Cold
                && (age(segment, clock.wall) >= self.policy.cold_disk_ttl
                    || disk_bytes > self.policy.max_room_disk_bytes);
            if !expired {
                break;
            }
            disk_bytes = disk_bytes.saturating_sub(segment.bytes);
            doomed.push(segment.clone());
        }
        report.cold_pruned_count += doomed.len();

        self.prune_segments(&doomed)?;
        report.new_tail_seq = self.tail_seq;
        Ok(())
    }

    /// Deletes `doomed`, a prefix of the indexed segments, after durably recording how far
    /// the log advanced, then evicts the pruned operations from RAM and recomputes `tail_seq`.
    /// Returns how many warm and cold segments were deleted.
    fn prune_segments(&mut self, doomed: &[SegmentEntry]) -> Result<(usize, usize), ServerError> {
        let Some(pruned_end) = doomed.iter().map(|segment| segment.end_seq).max() else {
            return Ok((0, 0));
        };
        self.record_pruned_through(pruned_end)?;

        let mut deleted = (0, 0);
        let result = doomed.iter().try_for_each(|segment| {
            match std::fs::remove_file(&segment.path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(ServerError::from(e)),
            }
            self.segments.remove(segment.start_seq);
            match segment.tier {
                Tier::Warm => deleted.0 += 1,
                Tier::Cold => deleted.1 += 1,
            }
            Ok(())
        });
        let result =
            result.and_then(|()| durable::sync_dir(&self.segments_dir).map_err(ServerError::from));

        // The index only lost the segments actually deleted, so the tail is right either way.
        self.hot_buffer
            .evict_older_than(SequenceNumber::new(pruned_end.get() + 1));
        self.tail_seq = self.compute_tail_seq();
        result.map(|()| deleted)
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
    /// define retention: the first sealed segment (cold, else warm), else the start of
    /// `active.wal`. When nothing is retained the tail is `head_seq + 1`, so that a cursor at
    /// `head_seq - 1` is rejected instead of silently missing operation `head_seq`.
    fn compute_tail_seq(&self) -> SequenceNumber {
        self.segments
            .first_start()
            .or(self.warm_disk.active_start_seq())
            .unwrap_or(self.head_seq.next())
    }

    /// Returns true if a client whose cursor is `cursor` can no longer be caught up from the log,
    /// because the operation right after its cursor has already been pruned.
    pub fn is_behind_retention(&self, cursor: SequenceNumber) -> bool {
        retention::is_behind_tail(cursor, self.tail_seq)
    }

    /// Highest sequence number committed to the log.
    pub fn head_seq(&self) -> SequenceNumber {
        self.head_seq
    }

    /// Oldest sequence number retained across all tiers (compaction boundary).
    pub fn tail_seq(&self) -> SequenceNumber {
        self.tail_seq
    }

    /// Bytes the log occupies on disk: its sealed segments and `active.wal`. Kept up to date
    /// incrementally; checked against `max_room_disk_bytes`.
    pub fn disk_bytes(&self) -> u64 {
        self.segments.total_bytes() + self.warm_disk.active_len()
    }

    /// Force rotates the active Warm segment immediately without evicting the RAM HotBuffer.
    pub fn force_rotate_warm(&mut self) -> Result<Option<PathBuf>, ServerError> {
        blocking_io(|| self.seal_active_segment())
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
        let doomed: Vec<SegmentEntry> = self
            .segments
            .iter()
            .take_while(|segment| segment.end_seq.get() < target_seq.get())
            .cloned()
            .collect();
        let (warm_deleted_count, cold_deleted_count) =
            blocking_io(|| self.prune_segments(&doomed))?;
        self.hot_buffer.evict_older_than(target_seq);

        Ok(PruneReport {
            warm_deleted_count,
            cold_deleted_count,
            new_tail_seq: self.tail_seq,
        })
    }
}

/// A contiguous run of operations being collected after a cursor.
struct DeltaBatch {
    ops: Vec<SequencedOperation>,
    /// Sequence of the last operation collected, or the starting cursor.
    cursor: SequenceNumber,
    /// What the tiers may still return; they take each operation from it as they read it.
    budget: ReadBudget,
}

impl DeltaBatch {
    fn new(cursor: SequenceNumber, budget: ReadBudget) -> Self {
        Self {
            ops: Vec::new(),
            cursor,
            budget,
        }
    }

    fn is_full(&self) -> bool {
        self.budget.is_exhausted()
    }

    /// Appends `ops`, read from `tier` within the batch's budget. Each must continue the batch
    /// exactly: a gap means the log lost operations it claims to retain.
    fn extend(&mut self, tier: &str, ops: Vec<SequencedOperation>) -> Result<(), ServerError> {
        for op in ops {
            if op.seq.get() != self.cursor.get() + 1 {
                return Err(sequence_gap(tier, self.cursor, op.seq));
            }
            self.cursor = op.seq;
            self.ops.push(op);
        }
        Ok(())
    }
}

/// Reads the operations after `from_seq` in a sealed segment that fit in `budget`.
fn read_segment(
    segment: &SegmentEntry,
    from_seq: SequenceNumber,
    budget: &mut ReadBudget,
) -> Result<RecoveredLogData, ServerError> {
    let start_seq = Some(segment.start_seq);
    match segment.tier {
        Tier::Warm => read_segment_file(&segment.path, start_seq, from_seq, budget),
        Tier::Cold => read_cold_file(&segment.path, start_seq, from_seq, budget),
    }
}

/// How long `segment` has been in its tier at `now`.
fn age(segment: &SegmentEntry, now: SystemTime) -> std::time::Duration {
    now.duration_since(segment.since).unwrap_or_default()
}

/// Reads the log backwards, starting with `active` (the contents of `active.wal`) and then the
/// sealed segments from the newest, until it holds the newest operations that fit in RAM
/// (`ram_max_ops` and `ram_max_bytes` of `policy`) and `mutations_wanted` distinct mutation
/// IDs, or the log is exhausted. Returns them oldest first.
///
/// Segments read only for their mutation IDs keep none of their operations, so memory is
/// bounded by what RAM holds plus one segment.
///
/// A corrupt sealed segment stops the reading there instead of failing the open: what is
/// older is not loaded, and a read of the damaged range later reports a gap.
fn read_newest(
    segments: &SegmentIndex,
    active: RecoveredLogData,
    policy: &RoomLifecyclePolicy,
    mutations_wanted: usize,
) -> Result<RecoveredLogData, ServerError> {
    let mut ram = RamFill::new(policy);
    let mut distinct: HashSet<MutationId> = active.1.iter().map(|(id, _)| *id).collect();
    let (active_ops, active_mutations) = active;
    let mut chunks = vec![(ram.keep_newest(active_ops), active_mutations)];
    for segment in segments.iter().rev() {
        if ram.is_full() && distinct.len() >= mutations_wanted {
            break;
        }
        let chunk = match read_segment(
            segment,
            SequenceNumber::new(0),
            &mut ReadBudget::ops(usize::MAX),
        ) {
            Ok(chunk) => chunk,
            Err(ServerError::WalCorruption(reason)) => {
                tracing::error!(
                    path = ?segment.path,
                    %reason,
                    "Corrupt log segment; older operations are not loaded"
                );
                break;
            }
            Err(err) => return Err(err),
        };
        distinct.extend(chunk.1.iter().map(|(id, _)| *id));
        chunks.push((ram.keep_newest(chunk.0), chunk.1));
    }

    let mut ops = Vec::new();
    let mut mutations = Vec::new();
    for (chunk_ops, chunk_mutations) in chunks.into_iter().rev() {
        ops.extend(chunk_ops);
        mutations.extend(chunk_mutations);
    }
    Ok((ops, mutations))
}

/// The operations kept for RAM while the log is read backwards on open.
struct RamFill {
    max_ops: usize,
    max_bytes: u64,
    ops: usize,
    bytes: u64,
}

impl RamFill {
    fn new(policy: &RoomLifecyclePolicy) -> Self {
        Self {
            max_ops: policy.ram_max_ops,
            max_bytes: policy.ram_max_bytes,
            ops: 0,
            bytes: 0,
        }
    }

    fn is_full(&self) -> bool {
        self.ops >= self.max_ops || self.bytes >= self.max_bytes
    }

    /// Keeps the newest operations of `chunk`, which is older than everything kept so far,
    /// while RAM is not full. One operation beyond the byte limit is kept, so that RAM is
    /// filled up to it; the sliding window trims it afterwards.
    fn keep_newest(&mut self, mut chunk: Vec<SequencedOperation>) -> Vec<SequencedOperation> {
        let mut first = chunk.len();
        while first > 0 && !self.is_full() {
            first -= 1;
            self.ops += 1;
            self.bytes = self.bytes.saturating_add(op_bytes(&chunk[first]));
        }
        chunk.split_off(first)
    }
}

/// The last `window` distinct mutation IDs of `mutations` (oldest first), each with its
/// newest sequence: an ID appended again once it left the deduplication window counts once.
fn newest_distinct(
    mutations: Vec<(MutationId, SequenceNumber)>,
    window: usize,
) -> Vec<(MutationId, SequenceNumber)> {
    let mut seen = HashSet::new();
    let mut newest: Vec<_> = mutations
        .into_iter()
        .rev()
        .filter(|(id, _)| seen.insert(*id))
        .take(window)
        .collect();
    newest.reverse();
    newest
}

/// The last operations of `ops`, at most `max`, that form a contiguous run ending exactly at
/// `head_seq`: the RAM buffer only ever holds contiguous sequences up to the head. When the
/// files hold less than their names claim, the run does not reach the head and RAM stays
/// empty, so that reads go to disk and report the gap.
fn contiguous_suffix(
    mut ops: Vec<SequencedOperation>,
    max: usize,
    head_seq: SequenceNumber,
) -> Vec<SequencedOperation> {
    if ops.last().map(|op| op.seq) != Some(head_seq) {
        return Vec::new();
    }
    let floor = ops.len().saturating_sub(max);
    let mut first = ops.len();
    while first > floor
        && (first == ops.len() || ops[first - 1].seq.get() + 1 == ops[first].seq.get())
    {
        first -= 1;
    }
    ops.split_off(first)
}

/// Handles an error reading a file of the retained range. Corruption means the log cannot
/// serve that range: like a gap, it is reported as `BehindCompaction`, which sends the client
/// to a snapshot, and logged as a server-side fault. Other errors (I/O) pass through.
fn unreadable_range(path: &Path, err: ServerError) -> ServerError {
    match err {
        ServerError::WalCorruption(reason) => {
            tracing::error!(path = ?path, %reason, "Corrupt segment in retained log range");
            ServerError::BehindCompaction
        }
        other => other,
    }
}

/// Takes the exclusive lock of the log in `dir`, or fails with `RoomLocked` if another log (in
/// this or another process) holds it. The lock lives on the returned handle; the file is never
/// read, so the lock being mandatory on Windows does not matter.
fn lock_log(dir: &Path) -> Result<File, ServerError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join(LOG_LOCK_FILE))?;
    file.try_lock_exclusive().map_err(|e| {
        ServerError::RoomLocked(format!(
            "room log {:?} locked by another process: {}",
            dir, e
        ))
    })?;
    Ok(file)
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
#[path = "tests/tiered_log.rs"]
mod tests;
