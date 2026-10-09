use zemdb_core::id::MutationId;
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::protocol::wal_frame::{
    classify_checksum_mismatch, classify_zeroed_header, decode_batch_payload,
    decode_wal_batch_from_slice, encode_wal_batch, parse_batch_header, BatchPayloadDecode,
    WalBatchDecodeResult, WalFrameError, BATCH_HEADER_SIZE, BATCH_MAGIC,
};
use zemdb_core::value::{CompactRow, PrimaryKey, Value};

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
    assert!(matches!(
        res,
        WalBatchDecodeResult::TornWrite {
            valid_bytes_offset: 0,
            ..
        }
    ));
}

fn delete_frame(seq: u64) -> Vec<u8> {
    let op = SequencedOperation::new(seq, Operation::delete(0, PrimaryKey::single(1i64), 100));
    encode_wal_batch(&[op], None).expect("encode ok")
}

fn header_of(frame: &[u8]) -> &[u8; BATCH_HEADER_SIZE] {
    frame.first_chunk().unwrap()
}

#[test]
fn header_and_payload_decode_a_frame() {
    let frame = delete_frame(1);

    let header = parse_batch_header(header_of(&frame)).unwrap().unwrap();
    assert_eq!(header.frame_len(), frame.len());
    let decoded = decode_batch_payload(&header, &frame[BATCH_HEADER_SIZE..]).unwrap();

    assert!(matches!(decoded, BatchPayloadDecode::Batch { ops, .. } if ops.len() == 1));
}

#[test]
fn zeroed_header_is_not_a_frame() {
    assert_eq!(parse_batch_header(&[0u8; BATCH_HEADER_SIZE]).unwrap(), None);
    assert!(classify_zeroed_header(true, 64).is_ok());
    assert!(matches!(
        classify_zeroed_header(false, 64),
        Err(WalFrameError::Corruption(_))
    ));
}

#[test]
fn header_with_invalid_magic_is_corruption() {
    let mut frame = delete_frame(1);
    frame[0] = 0;
    assert!(matches!(
        parse_batch_header(header_of(&frame)),
        Err(WalFrameError::Corruption(_))
    ));
}

#[test]
fn payload_with_wrong_checksum_is_reported_not_decoded() {
    let mut frame = delete_frame(1);
    *frame.last_mut().unwrap() ^= 0xFF;

    let header = parse_batch_header(header_of(&frame)).unwrap().unwrap();
    let decoded = decode_batch_payload(&header, &frame[BATCH_HEADER_SIZE..]).unwrap();

    assert!(matches!(
        decoded,
        BatchPayloadDecode::ChecksumMismatch { .. }
    ));
}

#[test]
fn checksum_mismatch_is_corruption_only_before_a_complete_header() {
    let next = delete_frame(2);

    assert!(classify_checksum_mismatch(1, 2, &[]).is_ok());
    assert!(classify_checksum_mismatch(1, 2, &next[..BATCH_HEADER_SIZE - 1]).is_ok());
    assert!(classify_checksum_mismatch(1, 2, &[0u8; BATCH_HEADER_SIZE]).is_ok());
    assert!(matches!(
        classify_checksum_mismatch(1, 2, &next[..BATCH_HEADER_SIZE]),
        Err(WalFrameError::Corruption(_))
    ));
}

#[test]
fn checksum_mismatch_before_a_partial_header_is_a_torn_write() {
    let mut wal = delete_frame(1);
    *wal.last_mut().unwrap() ^= 0xFF;
    wal.extend_from_slice(&delete_frame(2)[..5]);

    let res = decode_wal_batch_from_slice(&wal).expect("returns TornWrite");

    assert!(matches!(res, WalBatchDecodeResult::TornWrite { .. }));
}
