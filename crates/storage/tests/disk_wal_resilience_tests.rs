use rimdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value, MAX_MESSAGE_SIZE,
};
use rimdb_storage::format::{
    decode_wal_batch_from_slice, decode_wal_record_from_slice, encode_wal_batch,
    encode_wal_record, replay_wal_records, FileHeader, WalBatchDecodeResult, WalDecodeResult,
    BATCH_HEADER_SIZE, BATCH_MAGIC, HEADER_SIZE, MAGIC_BYTES,
};
use rimdb_storage::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};

const USERS_TABLE: u16 = 0;

fn test_schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS_TABLE)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .column("score", DataType::Int)
        .column("active", DataType::Bool)
        .build()
        .expect("valid users table");

    Schema::builder().table(users).build()
}

#[test]
fn test_file_header_encode_decode_roundtrip() {
    let header = FileHeader::new(100, 250, 4096);
    assert_eq!(header.magic, MAGIC_BYTES);
    assert_eq!(header.version, 1);
    assert_eq!(header.snapshot_seq, 100);
    assert_eq!(header.head_seq, 250);
    assert_eq!(header.snapshot_compressed_len, 4096);

    let encoded = header.encode();
    assert_eq!(encoded.len(), HEADER_SIZE);

    let decoded = FileHeader::decode(&encoded).expect("valid header");
    assert_eq!(header, decoded);
}

#[test]
fn test_file_header_rejects_invalid_magic() {
    let mut header = FileHeader::new(0, 0, 0).encode();
    header[0] = b'X';
    let err = FileHeader::decode(&header).unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[test]
fn test_file_header_rejects_corrupted_crc() {
    let mut header = FileHeader::new(10, 20, 100).encode();
    // Tamper with head_seq byte
    header[16] ^= 0xFF;
    let err = FileHeader::decode(&header).unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[test]
fn test_wal_record_encode_decode_clean() {
    let row = CompactRow::new(vec![Value::Int(1), Value::String("Alice".into())]);
    let op = SequencedOperation::with_default_origin(
        42u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row, 1000),
    );

    let encoded = encode_wal_record(&op).expect("encoding ok");
    assert!(encoded.len() > 8);

    let res = decode_wal_record_from_slice(&encoded).expect("decode ok");
    match res {
        WalDecodeResult::Ok {
            op: decoded_op,
            bytes_consumed,
        } => {
            assert_eq!(op, decoded_op);
            assert_eq!(bytes_consumed, encoded.len());
        }
        other => panic!("Expected Ok, got {:?}", other),
    }
}

#[test]
fn test_wal_record_detects_crc_corruption() {
    let row = CompactRow::new(vec![Value::Int(1)]);
    let op = SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row, 100),
    );

    let mut encoded = encode_wal_record(&op).expect("encoding ok");
    // Tamper with payload byte
    let last_idx = encoded.len() - 1;
    encoded[last_idx] ^= 0xFF;

    let err = decode_wal_record_from_slice(&encoded).unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[test]
fn test_wal_replay_detects_torn_write_and_recovers_valid_prefix() {
    let row1 = CompactRow::new(vec![Value::Int(1)]);
    let op1 = SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row1, 100),
    );

    let row2 = CompactRow::new(vec![Value::Int(2)]);
    let op2 = SequencedOperation::with_default_origin(
        2u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(2i64), row2, 200),
    );

    let enc1 = encode_wal_record(&op1).unwrap();
    let enc2 = encode_wal_record(&op2).unwrap();

    let mut wal_buffer = Vec::new();
    wal_buffer.extend_from_slice(&enc1);
    wal_buffer.extend_from_slice(&enc2);

    // Append a torn write (only 5 bytes of the next record header)
    wal_buffer.extend_from_slice(&[0x10, 0x00, 0x00, 0x00, 0xAA]);

    let (ops, valid_bytes, torn_write) = replay_wal_records(&wal_buffer).unwrap();
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[0], op1);
    assert_eq!(ops[1], op2);
    assert_eq!(valid_bytes, enc1.len() + enc2.len());
    assert!(torn_write.is_some());
}

#[tokio::test]
async fn test_disk_wal_crc32_corruption_detection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("room-corrupt");
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    let row = CompactRow::new(vec![
        Value::Int(10),
        Value::String("Test".into()),
        Value::Int(50),
        Value::Bool(true),
    ]);
    let ops = vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(10i64), row, 10),
    )];
    engine.apply_batch(&room_id, ops).await.unwrap();
    engine.close_room(&room_id).await.unwrap();

    // Corrupt the WAL payload byte in the file
    let file_path = temp_dir.path().join("room_room-corrupt.rimdb");
    let mut file_bytes = tokio::fs::read(&file_path).await.unwrap();
    let last_idx = file_bytes.len() - 1;
    file_bytes[last_idx] ^= 0xFF;
    tokio::fs::write(&file_path, &file_bytes).await.unwrap();

    // Reopening should detect WAL corruption
    let engine2 = DiskStorageEngine::new(options);
    let err = engine2.open_room(&room_id, schema).await.unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[tokio::test]
async fn test_disk_torn_write_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("room-torn");
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    let row = CompactRow::new(vec![
        Value::Int(1),
        Value::String("Valid Row".into()),
        Value::Int(99),
        Value::Bool(true),
    ]);
    let ops = vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row.clone(), 10),
    )];
    engine.apply_batch(&room_id, ops).await.unwrap();
    engine.close_room(&room_id).await.unwrap();

    // Append 5 garbage bytes simulating an interrupted write / power loss
    let file_path = temp_dir.path().join("room_room-torn.rimdb");
    let mut file_bytes = tokio::fs::read(&file_path).await.unwrap();
    let clean_len = file_bytes.len();
    file_bytes.extend_from_slice(&[0x20, 0x00, 0x00, 0x00, 0xAA]); // truncated 5 bytes
    tokio::fs::write(&file_path, &file_bytes).await.unwrap();

    // Reopening should safely recover valid operations and truncate the damaged tail
    let engine2 = DiskStorageEngine::new(options);
    engine2.open_room(&room_id, schema).await.unwrap();

    let head = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head, SequenceNumber::from(1u64));

    let retrieved = engine2.get(&room_id, "users", &PrimaryKey::single(1i64)).await.unwrap();
    assert_eq!(retrieved, Some(row));

    // Check file was truncated back to clean_len
    let repaired_bytes = tokio::fs::read(&file_path).await.unwrap();
    assert_eq!(repaired_bytes.len(), clean_len);
}

#[tokio::test]
async fn test_disk_room_lifecycle_and_persistence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("room-disk-1");
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    let row1 = CompactRow::new(vec![
        Value::Int(1),
        Value::String("Alice".into()),
        Value::Int(100),
        Value::Bool(true),
    ]);
    let row2 = CompactRow::new(vec![
        Value::Int(2),
        Value::String("Bob".into()),
        Value::Int(80),
        Value::Bool(false),
    ]);

    let ops = vec![
        SequencedOperation::with_default_origin(
            1u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row1.clone(), 10),
        ),
        SequencedOperation::with_default_origin(
            2u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(2i64), row2.clone(), 20),
        ),
    ];

    let head = engine.apply_batch(&room_id, ops).await.unwrap();
    assert_eq!(head, SequenceNumber::from(2u64));

    // Point lookups
    let res1 = engine.get(&room_id, "users", &PrimaryKey::single(1i64)).await.unwrap();
    assert_eq!(res1, Some(row1.clone()));

    // Close room
    engine.close_room(&room_id).await.unwrap();

    // Instantiate brand-new engine on same directory
    let engine2 = DiskStorageEngine::new(options);
    engine2.open_room(&room_id, schema).await.unwrap();

    let head2 = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head2, SequenceNumber::from(2u64));

    let res1_replayed = engine2.get(&room_id, "users", &PrimaryKey::single(1i64)).await.unwrap();
    assert_eq!(res1_replayed, Some(row1));

    let res2_replayed = engine2.get(&room_id, "users", &PrimaryKey::single(2i64)).await.unwrap();
    assert_eq!(res2_replayed, Some(row2));
}

#[test]
fn test_wal_decode_rejects_size_exceeding_max_message_size() {
    let mut bad_header = [0u8; BATCH_HEADER_SIZE];
    bad_header[0..2].copy_from_slice(&BATCH_MAGIC);
    let oversized_len = (MAX_MESSAGE_SIZE + 1) as u32;
    bad_header[2..6].copy_from_slice(&oversized_len.to_le_bytes());
    bad_header[6..10].copy_from_slice(&12345u32.to_le_bytes());
    bad_header[10..14].copy_from_slice(&1u32.to_le_bytes());

    let err = decode_wal_batch_from_slice(&bad_header).unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[test]
fn test_wal_decode_rejects_overflowing_record_len() {
    let mut bad_header = [0u8; BATCH_HEADER_SIZE];
    bad_header[0..2].copy_from_slice(&BATCH_MAGIC);
    let overflow_len = u32::MAX;
    bad_header[2..6].copy_from_slice(&overflow_len.to_le_bytes());
    bad_header[6..10].copy_from_slice(&12345u32.to_le_bytes());
    bad_header[10..14].copy_from_slice(&1u32.to_le_bytes());

    let err = decode_wal_batch_from_slice(&bad_header).unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[test]
fn test_wal_batch_framing_roundtrip() {
    let row1 = CompactRow::new(vec![Value::Int(10), Value::String("Op1".into())]);
    let row2 = CompactRow::new(vec![Value::Int(20), Value::String("Op2".into())]);
    let ops = vec![
        SequencedOperation::with_default_origin(
            1u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(10i64), row1, 100),
        ),
        SequencedOperation::with_default_origin(
            2u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(20i64), row2, 200),
        ),
    ];

    let encoded_batch = encode_wal_batch(&ops).expect("encode batch ok");
    assert_eq!(&encoded_batch[0..2], &BATCH_MAGIC);

    let res = decode_wal_batch_from_slice(&encoded_batch).expect("decode batch ok");
    match res {
        WalBatchDecodeResult::Ok {
            ops: decoded_ops,
            bytes_consumed,
        } => {
            assert_eq!(decoded_ops.len(), 2);
            assert_eq!(decoded_ops, ops);
            assert_eq!(bytes_consumed, encoded_batch.len());
        }
        other => panic!("Expected Ok batch, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_disk_flock_collision() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine1 = DiskStorageEngine::new(options.clone());
    let engine2 = DiskStorageEngine::new(options);

    let room_id = RoomId::new("locked-room");
    let schema = test_schema();

    // engine1 opens room and acquires exclusive OS file lock
    engine1.open_room(&room_id, schema.clone()).await.unwrap();

    // engine2 tries to open the same room concurrently -> must return RoomLocked
    let err = engine2.open_room(&room_id, schema).await.unwrap_err();
    match err {
        StorageError::RoomLocked(locked_id) => {
            assert_eq!(locked_id, room_id);
        }
        other => panic!("Expected RoomLocked, got: {other:?}"),
    }

    // After engine1 closes the room, engine2 can open it cleanly
    engine1.close_room(&room_id).await.unwrap();
    let schema2 = test_schema();
    engine2.open_room(&room_id, schema2).await.unwrap();
    engine2.close_room(&room_id).await.unwrap();
}

#[tokio::test]
async fn test_disk_apply_batch_sequence_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options);
    let room_id = RoomId::new("seq-mismatch");
    let schema = test_schema();

    engine.open_room(&room_id, schema).await.unwrap();

    let row = CompactRow::new(vec![Value::Int(1)]);
    let bad_ops = vec![SequencedOperation::with_default_origin(
        42u64, // room starts at 0, expected 1
        Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row, 10),
    )];

    let err = engine.apply_batch(&room_id, bad_ops).await.unwrap_err();
    match err {
        StorageError::SequenceMismatch { expected, actual } => {
            assert_eq!(expected, SequenceNumber::from(1u64));
            assert_eq!(actual, SequenceNumber::from(42u64));
        }
        other => panic!("Expected SequenceMismatch, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_disk_blind_update_ignored() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options);
    let room_id = RoomId::new("blind-update");
    let schema = test_schema();

    engine.open_room(&room_id, schema).await.unwrap();

    // Send update for non-existent row
    let updates = vec![rimdb_core::ColumnUpdate {
        column_idx: 1,
        value: Value::String("NonExistent".into()),
    }];
    let update_op = vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::update(USERS_TABLE, PrimaryKey::single(999i64), updates, 100),
    )];

    let head = engine.apply_batch(&room_id, update_op).await.unwrap();
    assert_eq!(head, SequenceNumber::from(1u64));

    // Must NOT have created an invalid row with nulls
    let retrieved = engine.get(&room_id, "users", &PrimaryKey::single(999i64)).await.unwrap();
    assert_eq!(retrieved, None);
}
