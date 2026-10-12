use super::*;
use zemdb_core::id::SequenceNumber;
use zemdb_core::mutation::Operation;
use zemdb_core::value::{PrimaryKey, Value};

fn make_op(seq: u64) -> SequencedOperation {
    let pk = PrimaryKey::single(Value::Int(seq as i64));
    SequencedOperation::new(
        SequenceNumber::new(seq),
        Operation::delete(1, pk, seq * 1000),
    )
}

#[test]
fn test_tiered_log_hot_buffer_o1_range_query() {
    let mut buffer = HotBuffer::new();

    // Appending contiguous operations 10..=20
    for i in 10..=20 {
        buffer.append(make_op(i)).unwrap();
    }

    assert_eq!(buffer.min_seq().unwrap().get(), 10);
    assert_eq!(buffer.max_seq().unwrap().get(), 20);

    // Query 1: from_seq older than min_seq (5 + 1 < 10, cannot satisfy contiguity)
    let res = buffer.get_range(SequenceNumber::new(5), &mut ReadBudget::ops(5));
    assert!(
        res.is_empty(),
        "Must return empty when requested cursor precedes buffer start to prevent sequence gap"
    );

    // Query 1b: from_seq immediately preceding min_seq (9 + 1 = 10, valid contiguous start)
    let res = buffer.get_range(SequenceNumber::new(9), &mut ReadBudget::ops(5));
    assert_eq!(res.len(), 5);
    assert_eq!(res[0].seq.get(), 10);
    assert_eq!(res[4].seq.get(), 14);

    // Query 2: from_seq exactly at min_seq
    let res = buffer.get_range(SequenceNumber::new(10), &mut ReadBudget::ops(3));
    assert_eq!(res.len(), 3);
    assert_eq!(res[0].seq.get(), 11);
    assert_eq!(res[2].seq.get(), 13);

    // Query 3: from_seq in the middle
    let res = buffer.get_range(SequenceNumber::new(15), &mut ReadBudget::ops(10));
    assert_eq!(res.len(), 5);
    assert_eq!(res[0].seq.get(), 16);
    assert_eq!(res[4].seq.get(), 20);

    // Query 4: from_seq equal to max_seq
    let res = buffer.get_range(SequenceNumber::new(20), &mut ReadBudget::ops(5));
    assert!(res.is_empty());

    // Query 5: from_seq greater than max_seq
    let res = buffer.get_range(SequenceNumber::new(25), &mut ReadBudget::ops(5));
    assert!(res.is_empty());

    // Query 6: limit 0
    let res = buffer.get_range(SequenceNumber::new(10), &mut ReadBudget::ops(0));
    assert!(res.is_empty());
}
