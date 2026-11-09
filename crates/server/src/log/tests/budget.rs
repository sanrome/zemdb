use super::*;
use std::sync::Arc;
use zemdb_core::id::{ClientId, RoomId};

use crate::relay::Uploader;
use crate::response_budget::InflightBudget;

/// An operation charged `bytes`, with as many encoded.
fn size(bytes: u64) -> OpSize {
    OpSize { bytes, wire: bytes }
}

/// A budget of response bytes in flight of `total` bytes, with a reservation of nothing in it.
fn reservation(total: u64) -> (Arc<InflightBudget>, Reservation) {
    let budget = Arc::new(InflightBudget::new(total, total));
    let holder = (
        RoomId::new("room").unwrap(),
        Uploader::Client(ClientId::new("c").unwrap()),
    );
    let reservation = budget.reserve(holder, 0).unwrap();
    (budget, reservation)
}

#[test]
fn admits_up_to_the_operation_count() {
    let mut budget = ReadBudget::ops(2);
    assert!(budget.admit(size(10)));
    assert!(!budget.is_exhausted());
    assert!(budget.admit(size(10)));
    assert!(budget.is_exhausted());
    assert!(!budget.admit(size(1)));
}

#[test]
fn refuses_what_exceeds_the_bytes_and_stays_exhausted() {
    let mut budget = ReadBudget::new(10, 100);
    assert!(budget.admit(size(60)));
    assert!(!budget.admit(size(50)));
    assert!(budget.is_exhausted());
    // A smaller operation later would leave a hole where the refused one was.
    assert!(!budget.admit(size(1)));
}

#[test]
fn always_admits_the_first_operation() {
    let mut budget = ReadBudget::new(10, 100);
    assert!(budget.admit(size(1_000)));
    assert!(!budget.admit(size(1)));
}

#[test]
fn zero_operations_admit_nothing() {
    let mut budget = ReadBudget::ops(0);
    assert!(budget.is_exhausted());
    assert!(!budget.admit(size(1)));
}

#[test]
fn the_owner_of_a_delete_is_charged_as_a_value_in_memory() {
    use zemdb_core::mutation::Operation;
    use zemdb_core::value::PrimaryKey;
    // A small delete is charged what its values take in memory: the key, and the owner it
    // declares if any.
    let value = std::mem::size_of::<Value>() as u64;
    let delete = SequencedOperation::new(1u64, Operation::delete(0, PrimaryKey::single(1i64), 0));
    assert_eq!(op_bytes(&delete), value);
    let owned = Operation::delete_with_owner(0, PrimaryKey::single(1i64), "a", 0);
    assert_eq!(op_bytes(&SequencedOperation::new(1u64, owned)), 2 * value);
}

#[test]
fn a_reserving_read_grows_the_reservation_by_what_each_operation_takes_in_the_response() {
    let (budget, mut reserved) = reservation(1_000);
    let mut read = ReadBudget::ops(10).reserving(&mut reserved);
    // Once read, in memory; and once encoded.
    assert!(read.admit(OpSize {
        bytes: 100,
        wire: 40
    }));
    assert!(read.admit(OpSize {
        bytes: 50,
        wire: 50
    }));
    assert!(!read.is_out_of_room());
    assert_eq!(budget.reserved().0, 240);
    drop(read);
    drop(reserved);
    assert_eq!(budget.reserved(), (0, 1_000));
}

#[test]
fn an_operation_without_room_in_the_reservation_ends_the_read() {
    let (budget, mut reserved) = reservation(300);
    let mut read = ReadBudget::ops(10).reserving(&mut reserved);
    assert!(read.admit(size(100)));
    assert!(!read.admit(size(100)));
    assert!(read.is_out_of_room() && read.is_exhausted());
    // A smaller operation later would leave a hole where the refused one was.
    assert!(!read.admit(size(1)));
    // The refused operation took nothing.
    assert_eq!(budget.reserved().0, 200);
}

#[test]
fn a_reserving_read_refuses_even_the_first_operation_without_room() {
    let (_budget, mut reserved) = reservation(100);
    let mut read = ReadBudget::ops(10).reserving(&mut reserved);
    assert!(!read.admit(size(60)));
    assert!(read.is_out_of_room() && read.is_exhausted());
}

#[test]
fn a_read_without_a_reservation_is_never_out_of_room() {
    let mut read = ReadBudget::new(1, 10);
    assert!(read.admit(size(1_000)));
    assert!(!read.admit(size(1)));
    assert!(read.is_exhausted() && !read.is_out_of_room());
}
