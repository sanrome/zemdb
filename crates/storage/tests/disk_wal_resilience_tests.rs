use rimdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value, MAX_MESSAGE_SIZE,
};
use rimdb_storage::format::{
    decode_wal_batch_from_slice, decode_wal_record_from_slice, encode_wal_batch, encode_wal_record,
    replay_wal_records, FileHeader, WalBatchDecodeResult, WalDecodeResult, BATCH_HEADER_SIZE,
    BATCH_MAGIC, HEADER_SIZE, MAGIC_BYTES,
};
use rimdb_storage::{
    DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError, WalReader,
};

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
    let header = FileHeader::new(100, 250, 4096, 0x12345678);
    assert_eq!(header.magic, MAGIC_BYTES);
    assert_eq!(header.version, 1);
    assert_eq!(header.snapshot_seq, 100);
    assert_eq!(header.head_seq, 250);
    assert_eq!(header.snapshot_compressed_len, 4096);
    assert_eq!(header.snapshot_payload_crc32, 0x12345678);

    let encoded = header.encode();
    assert_eq!(encoded.len(), HEADER_SIZE);

    let decoded = FileHeader::decode(&encoded).expect("valid header");
    assert_eq!(header, decoded);
}

#[test]
fn test_file_header_rejects_invalid_magic() {
    let mut header = FileHeader::new(0, 0, 0, 0).encode();
    header[0] = b'X';
    let err = FileHeader::decode(&header).unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[test]
fn test_file_header_rejects_corrupted_crc() {
    let mut header = FileHeader::new(10, 20, 100, 0).encode();
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

    let encoded = encode_wal_record(&op, None).expect("encoding ok");
    assert!(encoded.len() > 8);

    let res = decode_wal_record_from_slice(&encoded).expect("decode ok");
    match res {
        WalDecodeResult::Ok {
            op: decoded_op,
            bytes_consumed,
            ..
        } => {
            assert_eq!(op, decoded_op);
            assert_eq!(bytes_consumed, encoded.len());
        }
        other => panic!("Expected Ok, got {:?}", other),
    }
}

#[test]
fn test_wal_record_detects_crc_corruption() {
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

    let mut enc1 = encode_wal_record(&op1, None).expect("encoding ok");
    let enc2 = encode_wal_record(&op2, None).expect("encoding ok");

    // Tamper with payload byte of first record
    let last_idx = enc1.len() - 1;
    enc1[last_idx] ^= 0xFF;

    let mut combined = enc1;
    combined.extend_from_slice(&enc2);

    let err = decode_wal_record_from_slice(&combined).unwrap_err();
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

    let enc1 = encode_wal_record(&op1, None).unwrap();
    let enc2 = encode_wal_record(&op2, None).unwrap();

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

    let row1 = CompactRow::new(vec![
        Value::Int(10),
        Value::String("First".into()),
        Value::Int(50),
        Value::Bool(true),
    ]);
    let row2 = CompactRow::new(vec![
        Value::Int(20),
        Value::String("Second".into()),
        Value::Int(60),
        Value::Bool(false),
    ]);
    let ops1 = vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(10i64), row1, 10),
    )];
    let ops2 = vec![SequencedOperation::with_default_origin(
        2u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(20i64), row2, 20),
    )];
    engine.apply_batch(&room_id, ops1).await.unwrap();
    engine.apply_batch(&room_id, ops2).await.unwrap();
    engine.close_room(&room_id).await.unwrap();

    // Corrupt an intermediate WAL record in the file (op 1 followed by valid op 2)
    let file_path = temp_dir.path().join("room_room-corrupt.wal");
    let mut file_bytes = tokio::fs::read(&file_path).await.unwrap();
    // Tamper with payload byte inside first record
    file_bytes[20] ^= 0xFF;
    tokio::fs::write(&file_path, &file_bytes).await.unwrap();

    // Reopening should detect WAL corruption because valid data follows the corrupt record
    let engine2 = DiskStorageEngine::new(options);
    let err = engine2.open_room(&room_id, schema).await.unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[tokio::test]
async fn test_disk_wal_crc32_torn_write_at_eof_recovers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("room-torn-crc");
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    let row = CompactRow::new(vec![
        Value::Int(10),
        Value::String("Valid".into()),
        Value::Int(50),
        Value::Bool(true),
    ]);
    let ops = vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(10i64), row.clone(), 10),
    )];
    engine.apply_batch(&room_id, ops).await.unwrap();
    engine.close_room(&room_id).await.unwrap();

    // Tamper with the last byte at EOF simulating an interrupted final batch write
    let file_path = temp_dir.path().join("room_room-torn-crc.wal");
    let mut file_bytes = tokio::fs::read(&file_path).await.unwrap();
    let last_idx = file_bytes.len() - 1;
    file_bytes[last_idx] ^= 0xFF;
    tokio::fs::write(&file_path, &file_bytes).await.unwrap();

    // Reopening should cleanly recover and treat the terminal corrupted batch as a torn write
    let engine2 = DiskStorageEngine::new(options);
    engine2.open_room(&room_id, schema).await.unwrap();
    let head = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head, SequenceNumber::from(0u64));
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
    let file_path = temp_dir.path().join("room_room-torn.wal");
    let mut file_bytes = tokio::fs::read(&file_path).await.unwrap();
    let clean_len = file_bytes.len();
    file_bytes.extend_from_slice(&[0x20, 0x00, 0x00, 0x00, 0xAA]); // truncated 5 bytes
    tokio::fs::write(&file_path, &file_bytes).await.unwrap();

    // Reopening should safely recover valid operations and truncate the damaged tail
    let engine2 = DiskStorageEngine::new(options);
    engine2.open_room(&room_id, schema).await.unwrap();

    let head = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head, SequenceNumber::from(1u64));

    let retrieved = engine2
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap();
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
    let res1 = engine
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap();
    assert_eq!(res1, Some(row1.clone()));

    // Close room
    engine.close_room(&room_id).await.unwrap();

    // Instantiate brand-new engine on same directory
    let engine2 = DiskStorageEngine::new(options);
    engine2.open_room(&room_id, schema).await.unwrap();

    let head2 = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head2, SequenceNumber::from(2u64));

    let res1_replayed = engine2
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap();
    assert_eq!(res1_replayed, Some(row1));

    let res2_replayed = engine2
        .get(&room_id, "users", &PrimaryKey::single(2i64))
        .await
        .unwrap();
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

    let encoded_batch = encode_wal_batch(&ops, None).expect("encode batch ok");
    assert_eq!(&encoded_batch[0..2], &BATCH_MAGIC);

    let res = decode_wal_batch_from_slice(&encoded_batch).expect("decode batch ok");
    match res {
        WalBatchDecodeResult::Ok {
            ops: decoded_ops,
            bytes_consumed,
            ..
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
    let retrieved = engine
        .get(&room_id, "users", &PrimaryKey::single(999i64))
        .await
        .unwrap();
    assert_eq!(retrieved, None);
}

#[tokio::test]
async fn test_dual_file_storage_layout_and_compaction_truncation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("dual-layout");
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    let snap_path = temp_dir.path().join("room_dual-layout.snap");
    let wal_path = temp_dir.path().join("room_dual-layout.wal");

    // Both files must exist after opening room
    assert!(snap_path.exists());
    assert!(wal_path.exists());
    assert_eq!(
        tokio::fs::metadata(&snap_path).await.unwrap().len(),
        HEADER_SIZE as u64
    );
    assert_eq!(tokio::fs::metadata(&wal_path).await.unwrap().len(), 0);

    // Apply mutation batch
    let row = CompactRow::new(vec![
        Value::Int(1),
        Value::String("Dual File".into()),
        Value::Int(42),
        Value::Bool(true),
    ]);
    let ops = vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row, 100),
    )];
    engine.apply_batch(&room_id, ops).await.unwrap();

    // WAL file must now have content (> 0 bytes)
    let wal_len_before = tokio::fs::metadata(&wal_path).await.unwrap().len();
    assert!(wal_len_before > 0);

    // Explicitly compact room
    engine.compact_room(&room_id).await.unwrap();

    // After compaction: snapshot file must have grown (> HEADER_SIZE), and WAL must be truncated to 0
    let snap_len_after = tokio::fs::metadata(&snap_path).await.unwrap().len();
    assert!(snap_len_after > HEADER_SIZE as u64);

    let wal_len_after = tokio::fs::metadata(&wal_path).await.unwrap().len();
    assert_eq!(wal_len_after, 0);

    // Close and reopen to ensure state persists from .snap
    engine.close_room(&room_id).await.unwrap();

    let engine2 = DiskStorageEngine::new(options);
    engine2.open_room(&room_id, schema).await.unwrap();

    let head = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head, SequenceNumber::from(1u64));

    let row_retrieved = engine2
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row_retrieved[1], Value::String("Dual File".into()));
}

#[tokio::test]
async fn test_dynamic_column_update_resizing_disk_and_wal_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("dynamic-disk");

    let users_table = TableSchema::builder("users")
        .table_id(USERS_TABLE)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .nullable_column("score", DataType::Int)
        .nullable_column("note", DataType::String)
        .build()
        .unwrap();

    let schema = Schema::builder().table(users_table).build();
    engine.open_room(&room_id, schema.clone()).await.unwrap();

    // 1. Insert 2-column row (from older client/payload)
    let row = CompactRow::new(vec![Value::Int(1), Value::String("Initial".into())]);
    engine
        .apply_batch(
            &room_id,
            vec![SequencedOperation::with_default_origin(
                1u64,
                Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row, 100),
            )],
        )
        .await
        .unwrap();

    // 2. Apply Update for column 3
    engine
        .apply_batch(
            &room_id,
            vec![SequencedOperation::with_default_origin(
                2u64,
                Operation::update(
                    USERS_TABLE,
                    PrimaryKey::single(1i64),
                    vec![rimdb_core::ColumnUpdate::new(
                        3,
                        Value::String("Disk Note".into()),
                    )],
                    110,
                ),
            )],
        )
        .await
        .unwrap();

    // Verify row has 4 columns and column 3 has "Disk Note"
    let updated = engine
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.len(), 4);
    assert_eq!(updated[0], Value::Int(1));
    assert_eq!(updated[1], Value::String("Initial".into()));
    assert_eq!(updated[2], Value::Null);
    assert_eq!(updated[3], Value::String("Disk Note".into()));

    // 4. Close and recover from WAL with the evolved schema
    engine.close_room(&room_id).await.unwrap();

    let engine_recovered = DiskStorageEngine::new(options);
    engine_recovered.open_room(&room_id, schema).await.unwrap();

    let recovered_row = engine_recovered
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered_row.len(), 4);
    assert_eq!(recovered_row[0], Value::Int(1));
    assert_eq!(recovered_row[1], Value::String("Initial".into()));
    assert_eq!(recovered_row[2], Value::Null);
    assert_eq!(recovered_row[3], Value::String("Disk Note".into()));
}

#[tokio::test]
async fn test_wal_recovery_truncates_zero_filled_tail_at_eof() {
    let tmp = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(tmp.path());

    let schema = test_schema();
    let room_id = RoomId::new("zero_tail_room");

    // 1. Open room and write a valid operation
    let engine = DiskStorageEngine::new(options.clone());
    engine.open_room(&room_id, schema.clone()).await.unwrap();

    let op1 = SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(
            USERS_TABLE,
            PrimaryKey::single(10i64),
            CompactRow::new(vec![
                Value::Int(10),
                Value::String("TailTest".into()),
                Value::Int(500),
                Value::Bool(true),
            ]),
            1000,
        ),
    );
    engine.apply_batch(&room_id, vec![op1]).await.unwrap();

    // 2. Close room
    engine.close_room(&room_id).await.unwrap();

    let wal_path = tmp.path().join("room_zero_tail_room.wal");
    let valid_len = std::fs::metadata(&wal_path).unwrap().len();
    assert!(valid_len > 0);

    // 3. Append 1024 zero-bytes to simulate crash on thin-provisioned/preallocated disk
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        f.write_all(&vec![0u8; 1024]).unwrap();
        f.sync_all().unwrap();
    }
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        valid_len + 1024
    );

    // 4. Recover room: should detect zero-filled EOF tail, truncate cleanly, and load row
    let engine_rec = DiskStorageEngine::new(options.clone());
    engine_rec
        .open_room(&room_id, schema.clone())
        .await
        .unwrap();

    let row = engine_rec
        .get(&room_id, "users", &PrimaryKey::single(10i64))
        .await
        .unwrap()
        .expect("row recovered");
    assert_eq!(row[0], Value::Int(10));
    assert_eq!(row[1], Value::String("TailTest".into()));

    // Verify WAL length on disk is restored to valid_len
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), valid_len);

    // 5. Subsequent write after truncation must succeed and append properly
    let op2 = SequencedOperation::with_default_origin(
        2u64,
        Operation::insert(
            USERS_TABLE,
            PrimaryKey::single(20i64),
            CompactRow::new(vec![
                Value::Int(20),
                Value::String("PostRecovery".into()),
                Value::Int(600),
                Value::Bool(false),
            ]),
            1010,
        ),
    );
    engine_rec.apply_batch(&room_id, vec![op2]).await.unwrap();
    engine_rec.close_room(&room_id).await.unwrap();

    // Reopen and ensure both operations persist
    let engine_final = DiskStorageEngine::new(options);
    engine_final.open_room(&room_id, schema).await.unwrap();

    let row1 = engine_final
        .get(&room_id, "users", &PrimaryKey::single(10i64))
        .await
        .unwrap()
        .unwrap();
    let row2 = engine_final
        .get(&room_id, "users", &PrimaryKey::single(20i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row1[0], Value::Int(10));
    assert_eq!(row2[0], Value::Int(20));
}

#[test]
fn test_wal_reader_multi_op_batch_iteration_buffered() {
    let row1 = CompactRow::new(vec![Value::Int(1), Value::String("Alice".into())]);
    let op1 = SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row1, 100),
    );
    let row2 = CompactRow::new(vec![Value::Int(2), Value::String("Bob".into())]);
    let op2 = SequencedOperation::with_default_origin(
        2u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(2i64), row2, 101),
    );
    let row3 = CompactRow::new(vec![Value::Int(3), Value::String("Charlie".into())]);
    let op3 = SequencedOperation::with_default_origin(
        3u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(3i64), row3, 102),
    );

    let batch = vec![op1.clone(), op2.clone(), op3.clone()];
    let encoded = encode_wal_batch(&batch, None).expect("encode batch");

    let mut reader = WalReader::new(&encoded);

    match reader.next_record().expect("read record 1") {
        WalDecodeResult::Ok { op, .. } => assert_eq!(op, op1),
        other => panic!("expected record 1, got {:?}", other),
    }

    match reader.next_record().expect("read record 2") {
        WalDecodeResult::Ok { op, .. } => assert_eq!(op, op2),
        other => panic!("expected record 2, got {:?}", other),
    }

    match reader.next_record().expect("read record 3") {
        WalDecodeResult::Ok { op, .. } => assert_eq!(op, op3),
        other => panic!("expected record 3, got {:?}", other),
    }

    match reader.next_record().expect("clean eof") {
        WalDecodeResult::CleanEof => {}
        other => panic!("expected CleanEof, got {:?}", other),
    }
}

#[tokio::test]
async fn test_wal_replay_skips_operations_before_snapshot_seq() {
    use rimdb_core::ColumnUpdate;
    use std::io::Write;

    let tmp = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(tmp.path());
    let schema = test_schema();
    let room_id = RoomId::new("room_skip_snapshot_seq");

    // 1. Initialize room and insert row at seq 1
    let engine = DiskStorageEngine::new(options.clone());
    engine.open_room(&room_id, schema.clone()).await.unwrap();

    let op1 = SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(
            USERS_TABLE,
            PrimaryKey::single(1i64),
            CompactRow::new(vec![
                Value::Int(1),
                Value::String("Initial".into()),
                Value::Int(10),
                Value::Bool(true),
            ]),
            100,
        ),
    );
    let op2 = SequencedOperation::with_default_origin(
        2u64,
        Operation::update(
            USERS_TABLE,
            PrimaryKey::single(1i64),
            vec![ColumnUpdate::new(2, Value::Int(20))],
            101,
        ),
    );
    engine.apply_batch(&room_id, vec![op1, op2]).await.unwrap();

    // 2. Compact room creating snapshot at seq 2 and truncating WAL
    engine.compact_room(&room_id).await.unwrap();
    assert_eq!(
        engine.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(2u64)
    );
    engine.close_room(&room_id).await.unwrap();

    // 3. Manually append to WAL: an obsolete op with seq 1 (or 2) that tries to revert score to 999,
    //    followed by a new op with seq 3 setting score to 30.
    let wal_path = tmp.path().join("room_room_skip_snapshot_seq.wal");
    let stale_op = SequencedOperation::with_default_origin(
        1u64,
        Operation::update(
            USERS_TABLE,
            PrimaryKey::single(1i64),
            vec![ColumnUpdate::new(2, Value::Int(999))],
            102,
        ),
    );
    let new_op = SequencedOperation::with_default_origin(
        3u64,
        Operation::update(
            USERS_TABLE,
            PrimaryKey::single(1i64),
            vec![ColumnUpdate::new(2, Value::Int(30))],
            103,
        ),
    );

    let stale_bytes = encode_wal_batch(&[stale_op], None).unwrap();
    let new_bytes = encode_wal_batch(&[new_op], None).unwrap();

    let mut wal_file = std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap();
    wal_file.write_all(&stale_bytes).unwrap();
    wal_file.write_all(&new_bytes).unwrap();
    wal_file.sync_all().unwrap();

    // 4. Reopen room: recovery must skip stale_op (seq 1 <= snapshot_seq 2) and only apply new_op (seq 3)
    let engine_rec = DiskStorageEngine::new(options);
    engine_rec.open_room(&room_id, schema).await.unwrap();

    assert_eq!(
        engine_rec.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(3u64)
    );
    let row = engine_rec
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .expect("row exists");
    // If stale_op had been replayed, score might have been 999 or caused sequence violation.
    assert_eq!(row[2], Value::Int(30));
}

#[tokio::test]
async fn test_snapshot_payload_crc_corruption_detected() {
    let tmp = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(tmp.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("room-crc-corruption");
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    // Insert 5 rows
    for i in 1..=5i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        engine
            .apply_batch(
                &room_id,
                vec![SequencedOperation::with_default_origin(
                    i as u64,
                    Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
                )],
            )
            .await
            .unwrap();
    }

    engine.compact_room(&room_id).await.unwrap();
    engine.close_room(&room_id).await.unwrap();

    // Tamper with a payload byte in the .snap file
    let snap_path = tmp.path().join("room_room-crc-corruption.snap");
    let mut snap_bytes = std::fs::read(&snap_path).unwrap();
    assert!(snap_bytes.len() > HEADER_SIZE);
    snap_bytes[HEADER_SIZE + 2] ^= 0xFF;
    std::fs::write(&snap_path, snap_bytes).unwrap();

    // Reopening the room must fail with SnapshotCorruption due to CRC mismatch
    let engine_tampered = DiskStorageEngine::new(options);
    let err = engine_tampered
        .open_room(&room_id, schema)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::SnapshotCorruption(msg) if msg.contains("CRC32 mismatch")));
}

#[tokio::test]
async fn test_crash_recovery_with_wal_compacting() {
    let tmp = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(tmp.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("room-compacting-crash");
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    // 1. Initial 10 rows
    for i in 1..=10i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        engine
            .apply_batch(
                &room_id,
                vec![SequencedOperation::with_default_origin(
                    i as u64,
                    Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
                )],
            )
            .await
            .unwrap();
    }

    engine.compact_room(&room_id).await.unwrap();
    engine.close_room(&room_id).await.unwrap();

    // 2. Simulate pre-crash state:
    // Snapshot is at seq 10.
    // An unfinished compaction left `wal.compacting` containing rows 11..15.
    // The active `wal` file contains rows 16..20.
    let mut compacting_ops = Vec::new();
    for i in 11..=15i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        compacting_ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 200),
        ));
    }
    let compacting_bytes = encode_wal_batch(&compacting_ops, None).unwrap();
    let compacting_path = tmp.path().join("room_room-compacting-crash.wal.compacting");
    std::fs::write(&compacting_path, compacting_bytes).unwrap();

    let mut wal_ops = Vec::new();
    for i in 16..=20i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        wal_ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 300),
        ));
    }
    let wal_bytes = encode_wal_batch(&wal_ops, None).unwrap();
    let wal_path = tmp.path().join("room_room-compacting-crash.wal");
    std::fs::write(&wal_path, wal_bytes).unwrap();

    // 3. Open room to trigger recovery
    let engine_rec = DiskStorageEngine::new(options);
    engine_rec.open_room(&room_id, schema).await.unwrap();

    // Verify all rows from snapshot (1..10), compacting WAL (11..15), and active WAL (16..20) are restored
    assert_eq!(
        engine_rec.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(20u64)
    );

    let row1 = engine_rec
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row1[1], Value::String("User 1".into()));

    let row12 = engine_rec
        .get(&room_id, "users", &PrimaryKey::single(12i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row12[1], Value::String("User 12".into()));

    let row20 = engine_rec
        .get(&room_id, "users", &PrimaryKey::single(20i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row20[1], Value::String("User 20".into()));

    // Verify wal.compacting was cleaned up
    assert!(!compacting_path.exists());
}
