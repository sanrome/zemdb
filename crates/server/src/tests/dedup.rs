use super::*;
use zemdb_core::id::{MutationId, SequenceNumber};

fn id(n: u128) -> MutationId {
    MutationId::from_u128(n)
}

#[test]
fn new_cache_does_not_reserve_its_capacity() {
    // Reserving a map for this capacity overflows, so the cache can only exist if it
    // allocates as entries arrive.
    let mut cache = DedupLruCache::new(usize::MAX);
    cache.record(id(1), SequenceNumber::new(1));

    assert_eq!(cache.capacity(), usize::MAX);
    assert_eq!(cache.is_duplicate(&id(1)), Some(SequenceNumber::new(1)));
}

#[test]
fn full_cache_evicts_the_oldest_recorded_entry_even_if_it_was_just_found() {
    let mut cache = DedupLruCache::new(2);
    cache.record(id(1), SequenceNumber::new(1));
    cache.record(id(2), SequenceNumber::new(2));
    // Finding 1 does not make it more recent than 2.
    assert_eq!(cache.is_duplicate(&id(1)), Some(SequenceNumber::new(1)));

    cache.record(id(3), SequenceNumber::new(3));

    assert_eq!(cache.len(), 2);
    assert_eq!(cache.is_duplicate(&id(1)), None);
    assert_eq!(cache.is_duplicate(&id(2)), Some(SequenceNumber::new(2)));
    assert_eq!(cache.is_duplicate(&id(3)), Some(SequenceNumber::new(3)));
}

#[test]
fn zero_capacity_keeps_one_entry() {
    let mut cache = DedupLruCache::new(0);
    cache.record(id(1), SequenceNumber::new(1));
    cache.record(id(2), SequenceNumber::new(2));

    assert_eq!(cache.capacity(), 1);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.is_duplicate(&id(2)), Some(SequenceNumber::new(2)));
}
