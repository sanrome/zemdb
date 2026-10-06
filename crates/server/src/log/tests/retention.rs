use super::*;

fn seq(n: u64) -> SequenceNumber {
    SequenceNumber::new(n)
}

#[test]
fn new_room_lets_every_cursor_catch_up() {
    // tail = 1, head = 0: nothing was committed or pruned.
    assert!(!is_behind_tail(seq(0), seq(1)));
    // The only usable snapshot is the empty one at sequence 0.
    assert!(is_usable_snapshot(seq(0), seq(1), seq(0)));
    assert!(!is_usable_snapshot(seq(1), seq(1), seq(0)));
}

#[test]
fn cursor_can_catch_up_iff_it_is_at_least_tail_minus_one() {
    // Log retains 5..=9.
    assert!(is_behind_tail(seq(3), seq(5)));
    assert!(!is_behind_tail(seq(4), seq(5)));
    assert!(!is_behind_tail(seq(9), seq(5)));
}

#[test]
fn fully_pruned_log_has_tail_one_past_head() {
    // head = 9, everything pruned: tail = 10. Only a client at the head, or a snapshot of it,
    // can continue.
    assert!(is_behind_tail(seq(8), seq(10)));
    assert!(!is_behind_tail(seq(9), seq(10)));
    assert!(is_usable_snapshot(seq(9), seq(10), seq(9)));
    assert!(!is_usable_snapshot(seq(8), seq(10), seq(9)));
}

#[test]
fn snapshot_is_usable_between_tail_minus_one_and_head() {
    // Log retains 5..=9.
    assert!(!is_usable_snapshot(seq(3), seq(5), seq(9)));
    assert!(is_usable_snapshot(seq(4), seq(5), seq(9)));
    assert!(is_usable_snapshot(seq(9), seq(5), seq(9)));
    assert!(!is_usable_snapshot(seq(10), seq(5), seq(9)));
}
