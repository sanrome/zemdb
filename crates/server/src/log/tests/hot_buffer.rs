use super::*;
use crate::log::test_support::{make_op, seqs};

#[test]
fn range_queries_return_only_contiguous_operations_within_the_budget() {
    let mut buffer = HotBuffer::new();
    for i in 10..=20 {
        buffer.append(make_op(i)).unwrap();
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
