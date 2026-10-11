use lru::LruCache;
use zemdb_core::id::{MutationId, SequenceNumber};

/// Bounded in-memory LRU cache for mutation deduplication and Exactly-Once semantics.
///
/// Its memory grows with the entries it holds, not with its capacity: an idle room keeps an
/// almost empty cache however large the capacity is.
#[derive(Debug)]
pub struct DedupLruCache {
    cache: LruCache<MutationId, SequenceNumber>,
    capacity: usize,
}

impl DedupLruCache {
    /// Creates a new deduplication cache with a maximum capacity. A capacity of zero is
    /// raised to one.
    pub fn new(capacity: usize) -> Self {
        // `LruCache::new` would allocate the map for the whole capacity up front.
        Self {
            cache: LruCache::unbounded(),
            capacity: capacity.max(1),
        }
    }

    /// Checks if a mutation ID has already been assigned a sequence number.
    ///
    /// If present, returns the assigned `SequenceNumber` and refreshes its LRU recency.
    pub fn is_duplicate(&mut self, mutation_id: &MutationId) -> Option<SequenceNumber> {
        self.cache.get(mutation_id).copied()
    }

    /// Records a new mutation and its assigned sequence number into the cache, evicting the
    /// least recently used entry when it is full.
    pub fn record(&mut self, mutation_id: MutationId, seq: SequenceNumber) {
        self.cache.put(mutation_id, seq);
        if self.cache.len() > self.capacity {
            self.cache.pop_lru();
        }
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

    /// Returns the number of mutations the cache holds at most (at least one).
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
