use crate::error::ServerError;
use crate::log::budget::{op_bytes, ReadBudget};
use crate::log::policy::RoomLifecyclePolicy;
use std::collections::VecDeque;
use std::time::Instant;
use zemdb_core::id::SequenceNumber;
use zemdb_core::protocol::messages::SequencedOperation;

/// RAM cache of the newest operations of the log, as a contiguous run of sequences.
#[derive(Debug, Default)]
pub struct HotBuffer {
    entries: VecDeque<Entry>,
    /// Encoded size of the operations in `entries`.
    bytes: u64,
    min_seq: Option<SequenceNumber>,
    max_seq: Option<SequenceNumber>,
}

/// An operation in the buffer, with when it arrived and its encoded size.
#[derive(Debug)]
struct Entry {
    op: SequencedOperation,
    at: Instant,
    bytes: u64,
}

impl Entry {
    fn new(op: SequencedOperation) -> Self {
        Self {
            bytes: op_bytes(&op),
            op,
            at: Instant::now(),
        }
    }
}

impl HotBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends `op`, which must follow the last operation in the buffer.
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
        self.push(Entry::new(op));
        Ok(())
    }

    fn push(&mut self, entry: Entry) {
        self.bytes = self.bytes.saturating_add(entry.bytes);
        self.entries.push_back(entry);
    }

    fn pop_front(&mut self) {
        if let Some(entry) = self.entries.pop_front() {
            self.bytes = self.bytes.saturating_sub(entry.bytes);
        }
    }

    /// Updates the sequence bounds after evicting from the front.
    fn after_eviction(&mut self) {
        self.min_seq = self.entries.front().map(|entry| entry.op.seq);
        if self.entries.is_empty() {
            self.max_seq = None;
        }
    }

    /// Retrieves the contiguous operations strictly after `from_seq` that fit in `budget`,
    /// cloning only those.
    ///
    /// The start index is `from_seq + 1 - min_seq`: the buffer is contiguous.
    pub fn get_range(
        &self,
        from_seq: SequenceNumber,
        budget: &mut ReadBudget,
    ) -> Vec<SequencedOperation> {
        if budget.is_exhausted() || self.entries.is_empty() {
            return Vec::new();
        }

        let min = match self.min_seq {
            Some(m) => m.get(),
            None => return Vec::new(),
        };

        // The range starts before the buffer: the caller reads that prefix from disk first.
        if from_seq.get() + 1 < min {
            return Vec::new();
        }

        let start_idx = (from_seq.get() + 1 - min) as usize;
        if start_idx >= self.entries.len() {
            return Vec::new();
        }

        self.entries
            .range(start_idx..)
            .take_while(|entry| budget.admit(entry.bytes))
            .map(|entry| entry.op.clone())
            .collect()
    }

    /// Enforces the sliding window of `policy`: at most `ram_max_ops` operations and
    /// `ram_max_bytes` bytes, none older than `ram_ttl`.
    ///
    /// Evicts from the front of the queue, so the most recent operations stay buffered. An
    /// operation larger than `ram_max_bytes` on its own leaves the buffer empty; reads then go
    /// to disk.
    pub fn apply_sliding_window(&mut self, policy: &RoomLifecyclePolicy) {
        self.apply_sliding_window_at(policy, Instant::now());
    }

    /// [`apply_sliding_window`](Self::apply_sliding_window) with the current time given as `now`.
    pub fn apply_sliding_window_at(&mut self, policy: &RoomLifecyclePolicy, now: Instant) {
        while self.entries.len() > policy.ram_max_ops || self.bytes > policy.ram_max_bytes {
            self.pop_front();
        }

        while let Some(entry) = self.entries.front() {
            if now.saturating_duration_since(entry.at) >= policy.ram_ttl {
                self.pop_front();
            } else {
                break;
            }
        }

        self.after_eviction();
    }

    /// Evicts the operations before `threshold_seq`. Every operation in the buffer is already
    /// on disk.
    pub fn evict_older_than(&mut self, threshold_seq: SequenceNumber) {
        while let Some(entry) = self.entries.front() {
            if entry.op.seq.get() < threshold_seq.get() {
                self.pop_front();
            } else {
                break;
            }
        }

        self.after_eviction();
    }

    /// Replaces the contents with `ops`, recovered from disk.
    pub fn rehydrate(&mut self, ops: impl IntoIterator<Item = SequencedOperation>) {
        self.entries.clear();
        self.bytes = 0;
        self.min_seq = None;
        self.max_seq = None;

        for op in ops {
            if self.min_seq.is_none() {
                self.min_seq = Some(op.seq);
            }
            self.max_seq = Some(op.seq);
            self.push(Entry::new(op));
        }
    }

    /// Number of operations currently resident in the buffer.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Encoded size of the operations currently resident in the buffer.
    #[cfg(test)]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Returns true if the buffer has no operations.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Lowest sequence number currently present in the buffer.
    pub fn min_seq(&self) -> Option<SequenceNumber> {
        self.min_seq
    }

    /// Highest sequence number currently present in the buffer.
    #[cfg(test)]
    pub fn max_seq(&self) -> Option<SequenceNumber> {
        self.max_seq
    }
}

#[cfg(test)]
#[path = "tests/hot_buffer.rs"]
mod tests;
