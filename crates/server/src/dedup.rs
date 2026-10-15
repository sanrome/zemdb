use lru::LruCache;
use zemdb_core::id::{ClientId, MutationId, SequenceNumber};

/// The key under which the mutation `mutation_id` of `client_id` is deduplicated and logged:
/// 16 bytes of a BLAKE3 hash of both (the client id prefixed by its length), so that the same
/// id sent by two clients is two different mutations.
pub(crate) fn dedup_key(client_id: &ClientId, mutation_id: &MutationId) -> MutationId {
    let hash = blake3::Hasher::new()
        .update(&(client_id.as_str().len() as u64).to_le_bytes())
        .update(client_id.as_str().as_bytes())
        .update(mutation_id.as_bytes())
        .finalize();
    MutationId::new(std::array::from_fn(|i| hash.as_bytes()[i]))
}

/// Bounded in-memory LRU cache for mutation deduplication and Exactly-Once semantics, keyed
/// by [`dedup_key`].
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

    /// Checks if the mutation of `key` has already been assigned a sequence number.
    ///
    /// If present, returns the assigned `SequenceNumber` and refreshes its LRU recency.
    pub fn is_duplicate(&mut self, key: &MutationId) -> Option<SequenceNumber> {
        self.cache.get(key).copied()
    }

    /// Records the key of a new mutation and its assigned sequence number into the cache,
    /// evicting the least recently used entry when it is full.
    pub fn record(&mut self, key: MutationId, seq: SequenceNumber) {
        self.cache.put(key, seq);
        if self.cache.len() > self.capacity {
            self.cache.pop_lru();
        }
    }

    /// Hydrates the deduplication cache from recovered log entries.
    pub fn hydrate(&mut self, entries: impl IntoIterator<Item = (MutationId, SequenceNumber)>) {
        for (key, seq) in entries {
            self.record(key, seq);
        }
    }

    /// Returns the number of mutations currently cached.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns the number of mutations the cache holds at most (at least one).
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
#[path = "tests/dedup.rs"]
mod tests;
