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
