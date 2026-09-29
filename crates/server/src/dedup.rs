use lru::LruCache;
use rimdb_core::id::{MutationId, SequenceNumber};
use std::num::NonZeroUsize;

/// Fixed-capacity in-memory LRU cache for mutation deduplication and Exactly-Once semantics.
#[derive(Debug)]
pub struct DedupLruCache {
    cache: LruCache<MutationId, SequenceNumber>,
    capacity: usize,
}

impl DedupLruCache {
    /// Creates a new deduplication cache with a maximum capacity.
    pub fn new(capacity: usize) -> Self {
        let non_zero_cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            cache: LruCache::new(non_zero_cap),
            capacity,
        }
    }

    /// Checks if a mutation ID has already been assigned a sequence number.
    ///
    /// If present, returns the assigned `SequenceNumber` and refreshes its LRU recency.
    pub fn is_duplicate(&mut self, mutation_id: &MutationId) -> Option<SequenceNumber> {
        self.cache.get(mutation_id).copied()
    }

    /// Records a new mutation and its assigned sequence number into the cache.
    pub fn record(&mut self, mutation_id: MutationId, seq: SequenceNumber) {
        self.cache.put(mutation_id, seq);
    }

    /// Hydrates the deduplication cache from recovered log entries.
    pub fn hydrate(&mut self, entries: impl IntoIterator<Item = (MutationId, SequenceNumber)>) {
        for (mutation_id, seq) in entries {
            self.record(mutation_id, seq);
        }
    }

    /// Returns the number of mutations currently cached.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns true if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Returns the configured capacity limit.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
