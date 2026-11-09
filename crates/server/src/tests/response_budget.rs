use super::*;
use zemdb_core::id::ClientId;

fn holder(client: &str) -> BudgetHolder {
    (
        RoomId::new("room").unwrap(),
        Uploader::Client(ClientId::new(client).unwrap()),
    )
}

#[test]
fn a_reservation_grows_while_the_part_and_the_whole_budget_have_room() {
    let budget = Arc::new(InflightBudget::new(1_000, 600));
    let mut a = budget.reserve(holder("a"), 100).unwrap();
    assert!(a.grow(400));
    assert_eq!(budget.reserved(), (500, 500));
    // Over the client's part: refused, and nothing taken.
    assert!(!a.grow(200));
    assert_eq!(budget.reserved(), (500, 500));
    // Another client has its own part, until the whole budget runs out.
    let mut b = budget.reserve(holder("b"), 0).unwrap();
    assert!(b.grow(500));
    assert!(!b.grow(1));
    assert!(!a.grow(1));
    assert_eq!(budget.reserved(), (1_000, 0));
}

#[test]
fn a_grown_reservation_returns_everything_once_shrunk_and_dropped() {
    let budget = Arc::new(InflightBudget::new(1_000, 1_000));
    let mut reservation = budget.reserve(holder("a"), 10).unwrap();
    assert!(reservation.grow(90));
    reservation.shrink_to(30);
    assert_eq!(budget.reserved(), (30, 970));
    drop(reservation);
    assert_eq!(budget.reserved(), (0, 1_000));
}

/// A response never reserves more than a frame, so that one fits in the smallest part and
/// budget accepted, whatever its operations take.
#[test]
fn a_reservation_grows_for_free_past_a_frame() {
    let frame = MAX_FRAME_SIZE as u64;
    let budget = Arc::new(InflightBudget::new(frame, frame));
    let mut reservation = budget.reserve(holder("a"), RESPONSE_OVERHEAD).unwrap();
    assert!(reservation.grow(frame));
    assert!(reservation.grow(frame));
    assert_eq!(budget.reserved(), (frame, 0));
    drop(reservation);
    assert_eq!(budget.reserved(), (0, MAX_FRAME_SIZE));
}

#[test]
fn a_first_reservation_larger_than_the_part_is_refused_and_takes_nothing() {
    let budget = Arc::new(InflightBudget::new(1_000, 100));
    assert!(budget.reserve(holder("a"), 101).is_none());
    assert_eq!(budget.reserved(), (0, 1_000));
    let reservation = budget.reserve(holder("a"), 100).unwrap();
    assert_eq!(budget.reserved(), (100, 900));
    drop(reservation);
}

/// A read grows the reservation of its response by a chunk when what it reserved ahead runs
/// out, or by what its operation lacks if that is more, so that it takes from the budget once
/// while its operations fit in a chunk; and it gives back what no operation took when it ends.
#[test]
fn a_read_grows_its_reservation_by_chunks_and_gives_back_what_it_did_not_use() {
    let chunk = READ_RESERVATION_CHUNK;
    let budget = Arc::new(InflightBudget::new(8 * chunk, 8 * chunk));
    let mut reservation = budget.reserve(holder("a"), 100).unwrap();
    let takes = budget.takes();
    let mut read = reservation.for_read();

    for _ in 0..10 {
        assert!(read.take(300));
    }
    assert_eq!(budget.takes() - takes, 1);
    assert_eq!(budget.reserved().0, 100 + chunk);
    // Past what it reserved ahead: another chunk.
    assert!(read.take(chunk));
    assert_eq!(budget.reserved().0, 100 + 2 * chunk);
    // An operation larger than a chunk grows it by what it lacks.
    assert!(read.take(2 * chunk));
    assert_eq!(budget.reserved().0, 100 + 3 * chunk + 3_000);
    assert!(read.take(300));
    assert_eq!(budget.takes() - takes, 4);

    drop(read);
    assert_eq!(budget.reserved().0, 100 + 3_000 + 3 * chunk + 300);
    drop(reservation);
    assert_eq!(budget.reserved(), (0, 8 * chunk as usize));
}

/// A chunk without room in the client's part or in the whole budget does not refuse the
/// operation: the reservation grows by what it lacks, as with no chunks, and the rest of the
/// read grows by what each operation lacks, even once a chunk would fit again.
#[test]
fn a_chunk_without_room_grows_by_what_the_operation_lacks_for_the_rest_of_the_read() {
    let chunk = READ_RESERVATION_CHUNK;

    // The whole budget.
    let budget = Arc::new(InflightBudget::new(2 * chunk, 2 * chunk));
    let other = budget.reserve(holder("b"), chunk + 500).unwrap();
    let mut reservation = budget.reserve(holder("a"), 0).unwrap();
    let mut read = reservation.for_read();
    assert!(read.take(300));
    assert_eq!(budget.reserved().0, chunk + 500 + 300);
    drop(other);
    assert!(read.take(300));
    assert_eq!(budget.reserved().0, 600);
    // And without room for what the operation lacks, it is refused.
    assert!(!read.take(2 * chunk));
    drop(read);
    assert_eq!(budget.reserved().0, 600);
    drop(reservation);

    // The client's part.
    let budget = Arc::new(InflightBudget::new(4 * chunk, chunk));
    let mut reservation = budget.reserve(holder("a"), chunk - 1_000).unwrap();
    let mut read = reservation.for_read();
    assert!(read.take(300));
    assert!(read.take(700));
    assert!(!read.take(1));
    drop(read);
    assert_eq!(budget.reserved().0, chunk);
}

/// Past a frame a reservation grows for free and by less than asked: a read counts as reserved
/// ahead only what the reservation did grow, so that it never gives back what its operations
/// took, and its response ends with the reservation it has growing once per operation.
#[test]
fn a_read_near_a_frame_reserves_ahead_only_what_the_reservation_grew() {
    let frame = MAX_FRAME_SIZE as u64;
    let budget = Arc::new(InflightBudget::new(4 * frame, 4 * frame));

    // 100 KiB below the frame: the chunk grows it to the frame, and the operations after it,
    // past the frame, are free.
    let mut reservation = budget.reserve(holder("a"), frame - 100 * 1024).unwrap();
    let mut read = reservation.for_read();
    assert!(read.take(1_000));
    assert!(read.take(READ_RESERVATION_CHUNK));
    assert!(read.take(1_000));
    drop(read);
    assert_eq!(budget.reserved().0, frame);
    drop(reservation);

    // Below the frame at the end, it gives back what no operation took, and no more.
    let mut reservation = budget.reserve(holder("a"), frame - 100 * 1024).unwrap();
    let mut read = reservation.for_read();
    assert!(read.take(1_000));
    drop(read);
    assert_eq!(budget.reserved().0, frame - 100 * 1024 + 1_000);
}

/// A chunk without room after one that had it grows the reservation by what the operation lacks
/// beyond what the read reserved ahead, not by the whole operation, so that the read ends at the
/// same operation as growing once per operation.
#[test]
fn a_chunk_without_room_after_one_with_room_grows_by_what_the_operation_lacks() {
    let chunk = READ_RESERVATION_CHUNK;
    let budget = Arc::new(InflightBudget::new(chunk + 1_000, chunk + 1_000));
    let mut reservation = budget.reserve(holder("a"), 0).unwrap();
    let mut read = reservation.for_read();
    assert!(read.take(1_000));
    // Growing once per operation, 1_000 and then a chunk fit exactly.
    assert!(read.take(chunk));
    assert!(!read.take(1));
    drop(read);
    assert_eq!(budget.reserved(), (chunk + 1_000, 0));
}
