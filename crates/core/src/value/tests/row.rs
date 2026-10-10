use super::*;
use serde::de::value::{Error as ValueError, SeqDeserializer};

/// An iterator that claims `claimed` elements but yields only `items`.
struct Overclaiming {
    items: std::vec::IntoIter<u64>,
    claimed: usize,
}

impl Iterator for Overclaiming {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.items.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.claimed, Some(self.claimed))
    }
}

fn columns_from(items: Vec<u64>, claimed: usize) -> Result<Vec<u64>, ValueError> {
    let deserializer = SeqDeserializer::<_, ValueError>::new(Overclaiming {
        items: items.into_iter(),
        claimed,
    });
    deserialize_columns(deserializer)
}

#[test]
fn the_declared_length_does_not_drive_the_preallocation() {
    let decoded = columns_from(vec![1, 2], MAX_COLUMNS).unwrap();
    assert_eq!(decoded, vec![1, 2]);
    assert!(
        decoded.capacity() * std::mem::size_of::<u64>() <= MAX_COLUMNS_PREALLOC_BYTES,
        "reserved {} elements for a declared length of {MAX_COLUMNS}",
        decoded.capacity()
    );
}

#[test]
fn a_declared_length_over_the_limit_is_rejected_before_decoding() {
    let err = columns_from(vec![1], MAX_COLUMNS + 1).unwrap_err();
    assert!(err.to_string().contains("65536"), "{err}");
}

#[test]
fn elements_are_counted_whatever_the_declared_length() {
    // A format that understates the length cannot slip more elements than the limit.
    let items: Vec<u64> = (0..=MAX_COLUMNS as u64).collect();
    assert!(columns_from(items.clone(), 0).is_err());
    let mut within = items;
    within.pop();
    assert_eq!(columns_from(within, 0).unwrap().len(), MAX_COLUMNS);
}

#[test]
fn small_primary_keys_stay_inline() {
    let key = PrimaryKey::single(7i64);
    let bytes = bincode::serialize(&key).unwrap();
    let decoded: PrimaryKey = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded, key);
    assert!(!decoded.into_values().spilled());
}

#[test]
fn elements_beyond_the_declared_length_are_charged_one_by_one() {
    // A format that declares fewer elements than it holds still charges every one.
    let budget = crate::value::ValueBudget::start(3);
    assert!(columns_from(vec![1, 2, 3], 0).is_ok());
    assert!(!budget.exhausted());
    drop(budget);

    let budget = crate::value::ValueBudget::start(3);
    assert!(columns_from(vec![1, 2, 3, 4], 2).is_err());
    assert!(budget.exhausted());
}
