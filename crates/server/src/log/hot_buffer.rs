use crate::error::ServerError;
use crate::log::policy::RoomLifecyclePolicy;
use std::collections::VecDeque;
use std::time::Instant;
use zemdb_core::id::SequenceNumber;
use zemdb_core::protocol::messages::SequencedOperation;

/// Tier 1: In-memory contiguous read cache of recent sequenced operations.
///
/// Ensures zero-squashing on sequenced deltas, guaranteeing absolute sequence contiguity
/// (`head_seq + 1`) and providing sub-millisecond response times for connected clients.
#[derive(Debug, Default)]
pub struct HotBuffer {
    entries: VecDeque<(SequencedOperation, Instant)>,
    min_seq: Option<SequenceNumber>,
    max_seq: Option<SequenceNumber>,
}

impl HotBuffer {
    /// Creates an empty hot buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a new sequenced operation to the in-memory buffer.
    ///
    /// Validates strictly contiguous sequence numbering (`op.seq == last_seq + 1`).
    pub fn append(&mut self, op: SequencedOperation) -> Result<(), ServerError> {
        if let Some(last) = self.max_seq {
            if op.seq.get() != last.get() + 1 {
                return Err(ServerError::Wal(format!(
                    "HotBuffer sequence gap: expected sequence {}, got {}",
                    last.get() + 1,
                    op.seq.get()
                )));
            }
        } else {
            self.min_seq = Some(op.seq);
        }

        self.max_seq = Some(op.seq);
        self.entries.push_back((op, Instant::now()));
        Ok(())
    }

    /// Retrieves a contiguous slice of operations strictly after `from_seq` up to `limit` in O(1) time.
    ///
    /// Exploits the strictly contiguous and monotonic sequence invariant of the hot buffer
    /// (`seq == min_seq + offset`) to compute the initial offset in constant time without linear filtering.
    pub fn get_range(&self, from_seq: SequenceNumber, limit: usize) -> Vec<SequencedOperation> {
        if limit == 0 || self.entries.is_empty() {
            return Vec::new();
        }

        let min = match self.min_seq {
            Some(m) => m.get(),
            None => return Vec::new(),
        };

        // If the requested cursor falls before the minimum sequence present in the buffer,
        // RAM cannot satisfy the beginning of this contiguous range without introducing a gap.
        // Returning empty forces the caller to fetch the preceding sequence prefix from disk.
        if from_seq.get() + 1 < min {
            return Vec::new();
        }

        let start_idx = (from_seq.get() + 1 - min) as usize;
        if start_idx >= self.entries.len() {
            return Vec::new();
        }

        let take_count = limit.min(self.entries.len() - start_idx);
        self.entries
            .range(start_idx..start_idx + take_count)
            .map(|(op, _)| op.clone())
            .collect()
    }

    /// Enforces a continuous sliding window policy based on maximum capacity and retention TTL.
    ///
    /// Evicts aged or overflow deltas gradually from the front of the queue, ensuring
    /// recent operations remain buffered in RAM without dropping capacity to zero upon rotation.
    pub fn apply_sliding_window(&mut self, max_ops: usize, ttl: std::time::Duration) {
        self.apply_sliding_window_at(max_ops, ttl, Instant::now());
    }

    /// [`apply_sliding_window`](Self::apply_sliding_window) with the current time given as `now`.
    pub fn apply_sliding_window_at(
        &mut self,
        max_ops: usize,
        ttl: std::time::Duration,
        now: Instant,
    ) {
        while self.entries.len() > max_ops {
            self.entries.pop_front();
        }

        while let Some((_, time)) = self.entries.front() {
            if now.saturating_duration_since(*time) >= ttl {
                self.entries.pop_front();
            } else {
                break;
            }
        }

        self.min_seq = self.entries.front().map(|(op, _)| op.seq);
        if self.entries.is_empty() {
            self.max_seq = None;
        }
    }

    /// Checks if the buffer has exceeded capacity or TTL thresholds according to the lifecycle policy.
    pub fn should_rotate(&self, policy: &RoomLifecyclePolicy) -> bool {
        if self.entries.is_empty() {
            return false;
        }

        if self.entries.len() >= policy.ram_max_ops {
            return true;
        }

        if let Some((_, first_time)) = self.entries.front() {
            if first_time.elapsed() >= policy.ram_ttl {
                return true;
            }
        }

        false
    }

    /// Evicts operations older than `threshold_seq` from RAM cache.
    ///
    /// This is safe because all operations in the HotBuffer are already persisted to Warm Disk.
    pub fn evict_older_than(&mut self, threshold_seq: SequenceNumber) {
        while let Some((op, _)) = self.entries.front() {
            if op.seq.get() < threshold_seq.get() {
                self.entries.pop_front();
            } else {
                break;
            }
        }

        self.min_seq = self.entries.front().map(|(op, _)| op.seq);
        if self.entries.is_empty() {
            self.max_seq = None;
        }
    }

    /// Clears and rehydrates the buffer from recovered operations (e.g. on server startup).
    pub fn rehydrate(&mut self, ops: impl IntoIterator<Item = SequencedOperation>) {
        self.entries.clear();
        self.min_seq = None;
        self.max_seq = None;

        for op in ops {
            if self.min_seq.is_none() {
                self.min_seq = Some(op.seq);
            }
            self.max_seq = Some(op.seq);
            self.entries.push_back((op, Instant::now()));
        }
    }

    /// Number of operations currently resident in the buffer.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the buffer has no operations.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Lowest sequence number currently present in the buffer.
    pub fn min_seq(&self) -> Option<SequenceNumber> {
        self.min_seq
    }

    /// Highest sequence number currently present in the buffer.
    pub fn max_seq(&self) -> Option<SequenceNumber> {
        self.max_seq
    }
}
