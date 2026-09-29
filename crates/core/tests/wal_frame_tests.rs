use rimdb_core::id::MutationId;
use rimdb_core::mutation::Operation;
use rimdb_core::protocol::messages::SequencedOperation;
use rimdb_core::protocol::wal_frame::{
    decode_wal_batch_from_slice, encode_wal_batch, WalBatchDecodeResult, WalFrameError,
    BATCH_MAGIC,
};
use rimdb_core::value::{CompactRow, PrimaryKey, Value};

#[test]
fn test_wal_frame_roundtrip() {
    let row = CompactRow::new(vec![Value::Int(1), Value::String("Alice".into())]);
    let op = SequencedOperation::new(1, Operation::insert(0, PrimaryKey::single(1i64), row, 100));
    let batch = vec![op.clone()];

    let encoded = encode_wal_batch(&batch, None).expect("encode ok");
    assert_eq!(&encoded[0..2], &BATCH_MAGIC);

    let decoded = decode_wal_batch_from_slice(&encoded).expect("decode ok");
    match decoded {
        WalBatchDecodeResult::Ok {
            ops,
            mutation_id,
            bytes_consumed,
        } => {
            assert_eq!(ops, batch);
            assert_eq!(mutation_id, None);
            assert_eq!(bytes_consumed, encoded.len());
        }
        other => panic!("Expected Ok, got {:?}", other),
    }
}

#[test]
fn test_wal_frame_with_mutation_id_roundtrip() {
    let row = CompactRow::new(vec![Value::Int(42)]);
    let op = SequencedOperation::new(1, Operation::insert(0, PrimaryKey::single(1i64), row, 100));
    let mutation_id = MutationId::new([7u8; 16]);
    let batch = vec![op.clone()];

    let encoded = encode_wal_batch(&batch, Some(mutation_id)).expect("encode ok");
    let decoded = decode_wal_batch_from_slice(&encoded).expect("decode ok");

    match decoded {
        WalBatchDecodeResult::Ok {
            ops,
            mutation_id: recovered_mut,
            bytes_consumed,
        } => {
            assert_eq!(ops, batch);
            assert_eq!(recovered_mut, Some(mutation_id));
            assert_eq!(bytes_consumed, encoded.len());
        }
        other => panic!("Expected Ok, got {:?}", other),
    }
}

#[test]
fn test_wal_frame_detects_crc_tampering() {
    let op1 = SequencedOperation::new(1, Operation::delete(0, PrimaryKey::single(1i64), 100));
    let op2 = SequencedOperation::new(2, Operation::delete(0, PrimaryKey::single(2i64), 100));
    let mut encoded1 = encode_wal_batch(&[op1], None).expect("encode ok");
    let encoded2 = encode_wal_batch(&[op2], None).expect("encode ok");

    // Tamper with payload byte of first batch
    let last_idx = encoded1.len() - 1;
    encoded1[last_idx] ^= 0xFF;

    let mut combined = encoded1;
    combined.extend_from_slice(&encoded2);

    // Since a valid batch follows, this is true corruption in the middle of the log
    let err = decode_wal_batch_from_slice(&combined).unwrap_err();
    assert!(matches!(err, WalFrameError::Corruption(_)));
}

#[test]
fn test_wal_frame_crc_mismatch_at_eof_treated_as_torn_write() {
    let op = SequencedOperation::new(1, Operation::delete(0, PrimaryKey::single(1i64), 100));
    let mut encoded = encode_wal_batch(&[op], None).expect("encode ok");

    // Tamper with payload byte at terminal EOF
    let last_idx = encoded.len() - 1;
    encoded[last_idx] ^= 0xFF;

    let res = decode_wal_batch_from_slice(&encoded).expect("returns TornWrite at EOF");
    assert!(matches!(res, WalBatchDecodeResult::TornWrite { .. }));
}

#[test]
fn test_wal_frame_torn_write_detection() {
    let op = SequencedOperation::new(1, Operation::delete(0, PrimaryKey::single(1i64), 100));
    let encoded = encode_wal_batch(&[op], None).expect("encode ok");

    // Truncate to just partial header
    let partial = &encoded[0..5];
    let res = decode_wal_batch_from_slice(partial).expect("returns TornWrite");
    assert!(matches!(res, WalBatchDecodeResult::TornWrite { .. }));
}

#[test]
fn test_wal_frame_zero_filled_eof_torn_write_detection() {
    // Zero-filled tail at EOF (e.g., 64 zero bytes from power outage fallocate)
    let zeros = vec![0u8; 64];
    let res = decode_wal_batch_from_slice(&zeros).expect("returns TornWrite for zero tail");
    assert!(matches!(res, WalBatchDecodeResult::TornWrite { valid_bytes_offset: 0, .. }));
}
