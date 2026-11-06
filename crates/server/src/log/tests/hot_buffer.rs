use super::*;
use crate::log::test_support::{make_op, seqs};

#[test]
fn range_queries_return_only_contiguous_operations_within_the_budget() {
    let mut buffer = HotBuffer::new();
    for i in 10..=20 {
        buffer.append(Arc::new(make_op(i))).unwrap();
    }
    assert_eq!(buffer.min_seq().unwrap().get(), 10);
    assert_eq!(buffer.max_seq().unwrap().get(), 20);

    // (cursor, operation budget, expected sequences)
    let cases: [(u64, usize, Vec<u64>); 7] = [
        // The cursor precedes the buffer start (5 + 1 < 10): any answer would leave a gap.
        (5, 5, vec![]),
        // The cursor immediately precedes the buffer start (9 + 1 = 10).
        (9, 5, (10..=14).collect()),
        (10, 3, (11..=13).collect()),
        (15, 10, (16..=20).collect()),
        (20, 5, vec![]),
        (25, 5, vec![]),
        (10, 0, vec![]),
    ];
    for (from, budget, expected) in cases {
        let got = buffer.get_range(SequenceNumber::new(from), &mut ReadBudget::ops(budget));
        assert_eq!(seqs(&got), expected, "from {from}, budget {budget}");
    }
}

/// A read returns the operations of the buffer themselves, not copies of them: two reads of a
/// range share each operation with each other and with what was appended (DS6).
#[test]
fn reads_share_the_operations_of_the_buffer() {
    let mut buffer = HotBuffer::new();
    let appended: Vec<_> = (1..=3).map(|seq| Arc::new(make_op(seq))).collect();
    for op in &appended {
        buffer.append(Arc::clone(op)).unwrap();
    }

    let all = buffer.get_range(SequenceNumber::new(0), &mut ReadBudget::ops(10));
    let last_two = buffer.get_range(SequenceNumber::new(1), &mut ReadBudget::ops(10));
    assert_eq!(seqs(&all), [1, 2, 3]);
    assert_eq!(seqs(&last_two), [2, 3]);
    for (read, op) in all
        .iter()
        .chain(&last_two)
        .zip(appended.iter().chain(&appended[1..]))
    {
        assert!(Arc::ptr_eq(read, op), "operation {} is a copy", op.seq);
    }
}

/// The window holds and drops the operations a read holds like any other: by count, bytes and
/// age, as each takes in memory, whoever else holds it. A read keeps what it returned after the
/// buffer drops it, and is then its only holder.
#[test]
fn the_window_drops_the_operations_a_read_holds_like_any_other() {
    let bytes = |seqs: std::ops::RangeInclusive<u64>| -> u64 {
        seqs.map(|seq| crate::log::budget::op_bytes(&make_op(seq)))
            .sum()
    };
    let window = |ram_max_ops, ram_max_bytes| RoomLifecyclePolicy {
        ram_max_ops,
        ram_max_bytes,
        ram_ttl: std::time::Duration::from_secs(60),
        ..RoomLifecyclePolicy::default()
    };
    let mut buffer = HotBuffer::new();
    for seq in 1..=4 {
        buffer.append(Arc::new(make_op(seq))).unwrap();
    }
    let held = buffer.get_range(SequenceNumber::new(0), &mut ReadBudget::ops(10));
    assert_eq!(buffer.bytes(), bytes(1..=4));

    // By count.
    buffer.apply_sliding_window(&window(3, u64::MAX));
    assert_eq!(
        (buffer.min_seq(), buffer.len()),
        (Some(SequenceNumber::new(2)), 3)
    );
    assert_eq!(buffer.bytes(), bytes(2..=4));
    // By bytes.
    buffer.apply_sliding_window(&window(10, bytes(3..=4)));
    assert_eq!(
        (buffer.min_seq(), buffer.len()),
        (Some(SequenceNumber::new(3)), 2)
    );
    assert_eq!(buffer.bytes(), bytes(3..=4));
    // By age.
    let later = Instant::now() + std::time::Duration::from_secs(60);
    buffer.apply_sliding_window_at(&window(10, u64::MAX), later);
    assert!(buffer.is_empty());
    assert_eq!(buffer.bytes(), 0);

    assert_eq!(seqs(&held), [1, 2, 3, 4]);
    assert!(held.iter().all(|op| Arc::strong_count(op) == 1));
}
