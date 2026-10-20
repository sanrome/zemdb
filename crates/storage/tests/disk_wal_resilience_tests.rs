mod common;

use common::*;
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value, MAX_MESSAGE_SIZE,
};
use zemdb_storage::format::{
    decode_wal_batch_from_slice, encode_wal_batch, encode_wal_record, replay_wal_records,
    FileHeader, WalBatchDecodeResult, BATCH_HEADER_SIZE, BATCH_MAGIC, HEADER_SIZE, MAGIC_BYTES,
};
use zemdb_storage::{
    DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError, WalReader, WalWriter,
};

/// A row of one value: the WAL format tests encode rows without checking them against a schema.
fn key_row(id: i64) -> CompactRow {
    CompactRow::new(vec![Value::Int(id)])
}

/// A row of two values, with no schema to check it against.
fn named_row(id: i64, name: &str) -> CompactRow {
    CompactRow::new(vec![Value::Int(id), Value::String(name.into())])
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
    let op = insert(42, 1, named_row(1, "Alice"), 1000);

    let encoded = encode_wal_record(&op, None).expect("encoding ok");
    assert!(encoded.len() > 8);

    let res = decode_wal_batch_from_slice(&encoded).expect("decode ok");
    let WalBatchDecodeResult::Ok {
        ops,
        bytes_consumed,
        ..
    } = res
    else {
        panic!("Expected Ok, got {res:?}");
    };
    assert_eq!(ops, vec![op]);
    assert_eq!(bytes_consumed, encoded.len());
}

#[test]
fn test_wal_record_detects_crc_corruption() {
    let op1 = insert(1, 1, key_row(1), 100);
    let op2 = insert(2, 2, key_row(2), 200);

    let mut enc1 = encode_wal_record(&op1, None).expect("encoding ok");
    let enc2 = encode_wal_record(&op2, None).expect("encoding ok");

    // Tamper with payload byte of first record
    *enc1.last_mut().unwrap() ^= 0xFF;

    let mut combined = enc1;
    combined.extend_from_slice(&enc2);

    let err = decode_wal_batch_from_slice(&combined).unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[test]
fn test_wal_replay_detects_torn_write_and_recovers_valid_prefix() {
    let op1 = insert(1, 1, key_row(1), 100);
    let op2 = insert(2, 2, key_row(2), 200);

    let enc1 = encode_wal_record(&op1, None).unwrap();
    let enc2 = encode_wal_record(&op2, None).unwrap();

    let mut wal_buffer = Vec::new();
    wal_buffer.extend_from_slice(&enc1);
    wal_buffer.extend_from_slice(&enc2);

    // Append a torn write (only 5 bytes of the next record header)
    wal_buffer.extend_from_slice(&[0x10, 0x00, 0x00, 0x00, 0xAA]);

    let (ops, valid_bytes, torn_write) = replay_wal_records(&wal_buffer).unwrap();
    assert_eq!(ops, vec![op1, op2]);
    assert_eq!(valid_bytes, enc1.len() + enc2.len());
    assert!(torn_write.is_some());
}

#[tokio::test]
async fn test_disk_wal_crc32_corruption_detection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let room_id = RoomId::new("room-corrupt").unwrap();
    let ops = vec![
        insert(1, 10, row(10, "First", 50, true), 10),
        insert(2, 20, row(20, "Second", 60, false), 20),
    ];
    let engine = open_with_ops(&options, &room_id, ops).await;
    engine.close_room(&room_id).await.unwrap();

    // A payload byte of the first record, which the valid second record follows.
    damage_file(&engine.wal_file_path(&room_id), |bytes| {
        bytes[BATCH_HEADER_SIZE + 2] ^= 0xFF
    });

    // Valid data follows the corrupt record, so it is corruption and not a torn write.
    let engine2 = DiskStorageEngine::new(options).unwrap();
    let err = engine2
        .open_room(&room_id, users_schema())
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::WalCorruption(_)));
}

#[tokio::test]
async fn test_disk_wal_crc32_torn_write_at_eof_recovers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let room_id = RoomId::new("room-torn-crc").unwrap();
    let ops = vec![insert(1, 10, row(10, "Valid", 50, true), 10)];
    let engine = open_with_ops(&options, &room_id, ops).await;
    engine.close_room(&room_id).await.unwrap();

    // Tamper with the last byte at EOF simulating an interrupted final batch write
    damage_file(&engine.wal_file_path(&room_id), |bytes| {
        *bytes.last_mut().unwrap() ^= 0xFF
    });

    // Reopening should cleanly recover and treat the terminal corrupted batch as a torn write
    let engine2 = DiskStorageEngine::new(options).unwrap();
    engine2.open_room(&room_id, users_schema()).await.unwrap();
    let head = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head, SequenceNumber::from(0u64));
}

#[tokio::test]
async fn test_disk_torn_write_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let room_id = RoomId::new("room-torn").unwrap();
    let valid_row = row(1, "Valid Row", 99, true);
    let ops = vec![insert(1, 1, valid_row.clone(), 10)];
    let engine = open_with_ops(&options, &room_id, ops).await;
    engine.close_room(&room_id).await.unwrap();

    // 5 garbage bytes, as an interrupted write or a power loss leaves them.
    let file_path = engine.wal_file_path(&room_id);
    let mut clean_len = 0;
    damage_file(&file_path, |bytes| {
        clean_len = bytes.len();
        bytes.extend_from_slice(&[0x20, 0x00, 0x00, 0x00, 0xAA]);
    });

    // Reopening should safely recover valid operations and truncate the damaged tail
    let engine2 = DiskStorageEngine::new(options).unwrap();
    engine2.open_room(&room_id, users_schema()).await.unwrap();

    let head = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head, SequenceNumber::from(1u64));
    assert_eq!(get_user(&engine2, &room_id, 1).await, Some(valid_row));

    // Check file was truncated back to clean_len. The room is closed first: while it is open
    // its handle holds the WAL's lock, which is mandatory on Windows and refuses other readers.
    engine2.close_room(&room_id).await.unwrap();
    let repaired_bytes = std::fs::read(&file_path).unwrap();
    assert_eq!(repaired_bytes.len(), clean_len);
}

#[tokio::test]
async fn test_disk_room_lifecycle_and_persistence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone()).unwrap();
    let room_id = RoomId::new("room-disk-1").unwrap();

    engine.open_room(&room_id, users_schema()).await.unwrap();

    let alice = row(1, "Alice", 100, true);
    let bob = row(2, "Bob", 80, false);
    let ops = vec![
        insert(1, 1, alice.clone(), 10),
        insert(2, 2, bob.clone(), 20),
    ];

    let report = engine.apply_batch(&room_id, ops).await.unwrap();
    assert_eq!(report.head_seq(), SequenceNumber::from(2u64));
    assert_eq!(get_user(&engine, &room_id, 1).await, Some(alice.clone()));

    engine.close_room(&room_id).await.unwrap();

    let engine2 = DiskStorageEngine::new(options).unwrap();
    engine2.open_room(&room_id, users_schema()).await.unwrap();

    let head2 = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head2, SequenceNumber::from(2u64));
    assert_eq!(get_user(&engine2, &room_id, 1).await, Some(alice));
    assert_eq!(get_user(&engine2, &room_id, 2).await, Some(bob));
}

#[test]
fn test_wal_decode_never_trusts_an_oversized_record_len() {
    let next = encode_wal_batch(&[delete(1, 1, 100)], None).unwrap();

    for len in [(MAX_MESSAGE_SIZE + 1) as u32, u32::MAX] {
        // Even with a matching header checksum, the length is never allocated or followed.
        let mut bad_header = [0u8; BATCH_HEADER_SIZE];
        bad_header[0..2].copy_from_slice(&BATCH_MAGIC);
        bad_header[2..6].copy_from_slice(&len.to_le_bytes());
        bad_header[6..10].copy_from_slice(&12345u32.to_le_bytes());
        bad_header[10..14].copy_from_slice(&1u32.to_le_bytes());
        let crc = crc32fast::hash(&bad_header[..14]);
        bad_header[14..18].copy_from_slice(&crc.to_le_bytes());

        let res = decode_wal_batch_from_slice(&bad_header).unwrap();
        assert!(matches!(res, WalBatchDecodeResult::TornWrite { .. }));

        let mut log = bad_header.to_vec();
        log.extend_from_slice(&next);
        let err = decode_wal_batch_from_slice(&log).unwrap_err();
        assert!(matches!(err, StorageError::WalCorruption(_)));
    }
}

#[test]
fn test_wal_batch_framing_roundtrip() {
    let ops = vec![
        insert(1, 10, named_row(10, "Op1"), 100),
        insert(2, 20, named_row(20, "Op2"), 200),
    ];

    let encoded_batch = encode_wal_batch(&ops, None).expect("encode batch ok");
    assert_eq!(&encoded_batch[0..2], &BATCH_MAGIC);

    let res = decode_wal_batch_from_slice(&encoded_batch).expect("decode batch ok");
    let WalBatchDecodeResult::Ok {
        ops: decoded_ops,
        bytes_consumed,
        ..
    } = res
    else {
        panic!("Expected Ok batch, got: {res:?}");
    };
    assert_eq!(decoded_ops, ops);
    assert_eq!(bytes_consumed, encoded_batch.len());
}

#[tokio::test]
async fn test_disk_flock_collision() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine1 = DiskStorageEngine::new(options.clone()).unwrap();
    let engine2 = DiskStorageEngine::new(options).unwrap();
    let room_id = RoomId::new("locked-room").unwrap();

    // engine1 holds the room's exclusive OS file lock, so engine2 cannot open it.
    engine1.open_room(&room_id, users_schema()).await.unwrap();
    let err = engine2
        .open_room(&room_id, users_schema())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, StorageError::RoomLocked(locked_id) if *locked_id == room_id),
        "{err:?}"
    );

    // After engine1 closes the room, engine2 can open it cleanly
    engine1.close_room(&room_id).await.unwrap();
    engine2.open_room(&room_id, users_schema()).await.unwrap();
    engine2.close_room(&room_id).await.unwrap();
}

#[tokio::test]
async fn test_disk_apply_batch_sequence_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    sequence_gap_is_rejected(
        &DiskStorageEngine::new(DiskStorageOptions::new(temp_dir.path())).unwrap(),
    )
    .await;
}

#[tokio::test]
async fn test_disk_blind_update_ignored() {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = DiskStorageEngine::new(DiskStorageOptions::new(temp_dir.path())).unwrap();
    let room_id = RoomId::new("blind-update").unwrap();

    engine.open_room(&room_id, users_schema()).await.unwrap();

    // An update of a row that does not exist.
    let update_op = update(1, 999, 1, Value::String("NonExistent".into()), 100);
    let report = engine.apply_batch(&room_id, vec![update_op]).await.unwrap();
    assert_eq!(report.head_seq(), SequenceNumber::from(1u64));

    // Must NOT have created an invalid row with nulls
    assert_eq!(get_user(&engine, &room_id, 999).await, None);
}

#[tokio::test]
async fn test_dual_file_storage_layout_and_compaction_truncation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone()).unwrap();
    let room_id = RoomId::new("dual-layout").unwrap();

    engine.open_room(&room_id, users_schema()).await.unwrap();

    let snap_path = temp_dir.path().join("room_dual-layout.snap");
    let wal_path = temp_dir.path().join("room_dual-layout.wal");

    // Both files must exist after opening room
    assert!(snap_path.exists());
    assert!(wal_path.exists());
    assert_eq!(
        std::fs::metadata(&snap_path).unwrap().len(),
        HEADER_SIZE as u64
    );
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);

    let ops = vec![insert(1, 1, row(1, "Dual File", 42, true), 100)];
    engine.apply_batch(&room_id, ops).await.unwrap();
    let wal_len_before = std::fs::metadata(&wal_path).unwrap().len();
    assert!(wal_len_before > 0);

    engine.compact_room(&room_id).await.unwrap();

    // After compaction: snapshot file must have grown (> HEADER_SIZE), and WAL must be truncated to 0
    let snap_len_after = std::fs::metadata(&snap_path).unwrap().len();
    assert!(snap_len_after > HEADER_SIZE as u64);
    let wal_len_after = std::fs::metadata(&wal_path).unwrap().len();
    assert_eq!(wal_len_after, 0);

    // Close and reopen to ensure state persists from .snap
    engine.close_room(&room_id).await.unwrap();

    let engine2 = DiskStorageEngine::new(options).unwrap();
    engine2.open_room(&room_id, users_schema()).await.unwrap();

    let head = engine2.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head, SequenceNumber::from(1u64));
    let row_retrieved = get_user(&engine2, &room_id, 1).await.unwrap();
    assert_eq!(row_retrieved[1], Value::String("Dual File".into()));
}

#[tokio::test]
async fn test_dynamic_column_update_resizing_disk_and_wal_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone()).unwrap();
    let room_id = RoomId::new("dynamic-disk").unwrap();

    let updated = column_update_grows_row(&engine, &room_id).await;

    // The row is recovered from the WAL with the evolved schema.
    engine.close_room(&room_id).await.unwrap();
    let engine_recovered = DiskStorageEngine::new(options).unwrap();
    engine_recovered
        .open_room(&room_id, evolved_users_schema())
        .await
        .unwrap();
    assert_eq!(
        get_user(&engine_recovered, &room_id, 1).await,
        Some(updated)
    );
}

#[tokio::test]
async fn test_wal_recovery_truncates_zero_filled_tail_at_eof() {
    let tmp = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(tmp.path());
    let room_id = RoomId::new("zero_tail_room").unwrap();
    let tail_row = row(10, "TailTest", 500, true);
    let engine = open_with_ops(
        &options,
        &room_id,
        vec![insert(1, 10, tail_row.clone(), 1000)],
    )
    .await;
    engine.close_room(&room_id).await.unwrap();

    let wal_path = engine.wal_file_path(&room_id);
    let valid_len = std::fs::metadata(&wal_path).unwrap().len();
    assert!(valid_len > 0);

    // 1024 zero bytes, as a crash on a thin-provisioned or preallocated disk leaves them.
    damage_file(&wal_path, |bytes| bytes.extend_from_slice(&[0u8; 1024]));
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        valid_len + 1024
    );

    // Recovery detects the zero-filled tail, truncates it and loads the row.
    let engine_rec = DiskStorageEngine::new(options.clone()).unwrap();
    engine_rec
        .open_room(&room_id, users_schema())
        .await
        .unwrap();
    assert_eq!(
        get_user(&engine_rec, &room_id, 10).await,
        Some(tail_row.clone())
    );
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), valid_len);

    // A write after the truncation is appended properly, and both operations persist.
    let post_row = row(20, "PostRecovery", 600, false);
    engine_rec
        .apply_batch(&room_id, vec![insert(2, 20, post_row.clone(), 1010)])
        .await
        .unwrap();
    engine_rec.close_room(&room_id).await.unwrap();

    let engine_final = DiskStorageEngine::new(options).unwrap();
    engine_final
        .open_room(&room_id, users_schema())
        .await
        .unwrap();
    assert_eq!(get_user(&engine_final, &room_id, 10).await, Some(tail_row));
    assert_eq!(get_user(&engine_final, &room_id, 20).await, Some(post_row));
}

#[test]
fn test_wal_reader_returns_multi_op_batch_whole() {
    let op1 = insert(1, 1, named_row(1, "Alice"), 100);
    let op2 = insert(2, 2, named_row(2, "Bob"), 101);
    let op3 = insert(3, 3, named_row(3, "Charlie"), 102);

    let batch = vec![op1.clone(), op2.clone(), op3.clone()];
    let encoded = encode_wal_batch(&batch, None).expect("encode batch");

    let mut reader = WalReader::new(&encoded);

    let res = reader.next_batch().expect("read batch");
    let WalBatchDecodeResult::Ok { ops, .. } = res else {
        panic!("expected the whole batch, got {res:?}");
    };
    assert_eq!(ops, vec![op1, op2, op3]);
    assert_eq!(
        reader.next_batch().expect("clean eof"),
        WalBatchDecodeResult::CleanEof
    );
}

#[tokio::test]
async fn test_wal_replay_skips_operations_before_snapshot_seq() {
    use std::io::Write;

    let tmp = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(tmp.path());
    let room_id = RoomId::new("room_skip_snapshot_seq").unwrap();

    let engine = DiskStorageEngine::new(options.clone()).unwrap();
    engine.open_room(&room_id, users_schema()).await.unwrap();
    let op1 = insert(1, 1, row(1, "Initial", 10, true), 100);
    let op2 = update(2, 1, 2, Value::Int(20), 101);
    engine.apply_batch(&room_id, vec![op1, op2]).await.unwrap();

    // The snapshot is now at seq 2 and the WAL is empty.
    engine.compact_room(&room_id).await.unwrap();
    assert_eq!(
        engine.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(2u64)
    );
    engine.close_room(&room_id).await.unwrap();

    // Manually append to the WAL an obsolete op with seq 1 that tries to revert the score to
    // 999, followed by a new op with seq 3 setting it to 30.
    let wal_path = engine.wal_file_path(&room_id);
    let stale_op = update(1, 1, 2, Value::Int(999), 102);
    let new_op = update(3, 1, 2, Value::Int(30), 103);
    let stale_bytes = encode_wal_batch(&[stale_op], None).unwrap();
    let new_bytes = encode_wal_batch(&[new_op], None).unwrap();

    let mut wal_file = std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap();
    wal_file.write_all(&stale_bytes).unwrap();
    wal_file.write_all(&new_bytes).unwrap();
    wal_file.sync_all().unwrap();

    // Recovery must skip stale_op (seq 1 <= snapshot_seq 2) and only apply new_op (seq 3).
    let engine_rec = DiskStorageEngine::new(options).unwrap();
    engine_rec
        .open_room(&room_id, users_schema())
        .await
        .unwrap();

    assert_eq!(
        engine_rec.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(3u64)
    );
    let row = get_user(&engine_rec, &room_id, 1)
        .await
        .expect("row exists");
    // Replaying stale_op would set 999 or break the sequence.
    assert_eq!(row[2], Value::Int(30));
}

#[tokio::test]
async fn test_snapshot_payload_crc_corruption_detected() {
    let tmp = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(tmp.path());
    let room_id = RoomId::new("room-crc-corruption").unwrap();
    let engine = open_with_ops(&options, &room_id, inserts(1..=5, 100)).await;
    engine.compact_room(&room_id).await.unwrap();
    engine.close_room(&room_id).await.unwrap();

    damage_file(&engine.snap_file_path(&room_id), |bytes| {
        assert!(bytes.len() > HEADER_SIZE);
        bytes[HEADER_SIZE + 2] ^= 0xFF;
    });

    // Reopening the room must fail with SnapshotCorruption due to CRC mismatch
    let engine_tampered = DiskStorageEngine::new(options).unwrap();
    let err = engine_tampered
        .open_room(&room_id, users_schema())
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::SnapshotCorruption(msg) if msg.contains("CRC32 mismatch")));
}

#[tokio::test]
async fn test_crash_recovery_with_wal_compacting() {
    let tmp = tempfile::tempdir().unwrap();
    let room_id = RoomId::new("room-compacting-crash").unwrap();
    // The snapshot is at seq 10, an unfinished compaction left `.wal.compacting` holding rows
    // 11..=15, and the active WAL holds rows 16..=20.
    let engine = seed_interrupted_compaction(tmp.path(), &room_id, 1..=10, 11..=15, 16..=20).await;
    let compacting_path = compacting_wal_path(&engine, &room_id);

    let engine_rec = DiskStorageEngine::new(DiskStorageOptions::new(tmp.path())).unwrap();
    engine_rec
        .open_room(&room_id, users_schema())
        .await
        .unwrap();

    // Rows from the snapshot (1..=10), the compacting WAL (11..=15) and the active WAL (16..=20).
    assert_users_up_to(&engine_rec, &room_id, 20).await;
    assert!(!compacting_path.exists());
}

#[tokio::test]
async fn test_multi_thread_flock_concurrency_stress() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let room_id = RoomId::new("stress-flock-room").unwrap();

    const NUM_WORKERS: usize = 16;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(NUM_WORKERS));
    let mut handles = Vec::new();

    for _ in 0..NUM_WORKERS {
        let b = std::sync::Arc::clone(&barrier);
        let opts = options.clone();
        let r_id = room_id.clone();

        handles.push(tokio::spawn(async move {
            let engine = DiskStorageEngine::new(opts).unwrap();
            b.wait().await;
            let res = engine.open_room(&r_id, users_schema()).await;
            (engine, res)
        }));
    }

    let mut winner_engine = None;
    let mut locked_failures = 0;

    for handle in handles {
        let (engine, res) = handle.await.unwrap();
        match res {
            Ok(()) => {
                assert!(
                    winner_engine.is_none(),
                    "Only one engine must win the flock"
                );
                winner_engine = Some(engine);
            }
            Err(StorageError::RoomLocked(locked_id)) => {
                assert_eq!(locked_id, room_id);
                locked_failures += 1;
            }
            Err(other) => panic!("Expected RoomLocked, got: {other:?}"),
        }
    }

    assert_eq!(locked_failures, NUM_WORKERS - 1);
    let winner = winner_engine.expect("One engine must have acquired the lock");
    winner.close_room(&room_id).await.unwrap();

    // Now another engine can successfully acquire the lock and write data
    let next_engine = DiskStorageEngine::new(options).unwrap();
    next_engine
        .open_room(&room_id, users_schema())
        .await
        .unwrap();
    let ops = vec![insert(1, 1, row(1, "Stress Winner", 100, true), 1000)];
    next_engine.apply_batch(&room_id, ops).await.unwrap();
    assert_eq!(
        next_engine.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(1u64)
    );
    next_engine.close_room(&room_id).await.unwrap();
}

#[tokio::test]
async fn test_empty_batch_writes_nothing_to_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = DiskStorageEngine::new(DiskStorageOptions::new(tmp.path())).unwrap();
    let room_id = RoomId::new("empty-batch").unwrap();
    engine.open_room(&room_id, users_schema()).await.unwrap();
    let wal_path = engine.wal_file_path(&room_id);
    let len_before = std::fs::metadata(&wal_path).unwrap().len();

    let report = engine.apply_batch(&room_id, Vec::new()).await.unwrap();

    assert_eq!(report.head_seq(), SequenceNumber::from(0u64));
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), len_before);
}

#[test]
fn wal_reader_skips_empty_frames() {
    let op = insert(1, 1, named_row(1, "Alice"), 100);
    let mut encoded = encode_wal_batch(&[], None).expect("encode empty batch");
    encoded.extend_from_slice(&encode_wal_batch(std::slice::from_ref(&op), None).unwrap());
    encoded.extend_from_slice(&encode_wal_batch(&[], None).unwrap());

    let mut reader = WalReader::new(&encoded);

    let res = reader.next_batch().expect("read batch");
    let WalBatchDecodeResult::Ok { ops, .. } = res else {
        panic!("expected the non-empty batch, got {res:?}");
    };
    assert_eq!(ops, vec![op]);
    assert_eq!(
        reader.next_batch().expect("clean eof"),
        WalBatchDecodeResult::CleanEof
    );
    assert_eq!(reader.offset(), encoded.len());
}

#[tokio::test]
async fn test_wal_writer_reports_a_write_that_fails_in_the_background() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read-only.wal");
    std::fs::write(&path, b"").unwrap();
    // Writes through a read-only handle are accepted by `write_all` and fail on the blocking pool.
    let mut file = tokio::fs::File::open(&path).await.unwrap();
    let op = delete(1, 1, 1);

    assert!(WalWriter::write_record(&mut file, &op).await.is_err());
    assert!(WalWriter::write_batch(&mut file, &[op]).await.is_err());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
}

/// A schema with a table `t{id}`, id `id`, for each id in `ids`: an integer key and value.
fn tables_schema(ids: &[u16]) -> Schema {
    ids.iter()
        .fold(Schema::builder(), |schema, &id| {
            schema.table(
                TableSchema::builder(format!("t{id}"))
                    .table_id(id)
                    .primary_key("id", DataType::Int)
                    .column("v", DataType::Int),
            )
        })
        .build()
}

/// Inserts the row `(key, key)` into table `table` at sequence `seq`.
fn table_insert(seq: u64, table: u16, key: i64) -> SequencedOperation {
    let row = CompactRow::new(vec![Value::Int(key), Value::Int(key)]);
    sequenced(
        seq,
        Operation::insert(table, PrimaryKey::single(key), row, 0),
    )
}

/// The bytes of the snapshot and WAL of `room`.
fn room_files(engine: &DiskStorageEngine, room: &RoomId) -> (Vec<u8>, Vec<u8>) {
    let read = |path| std::fs::read(path).unwrap();
    (
        read(engine.snap_file_path(room)),
        read(engine.wal_file_path(room)),
    )
}

/// Writes one row to each of the tables 0 and 1 of a room, closes it, and checks that it does
/// not reopen with a schema that lacks table 1, without changing its files, and then that it
/// reopens with both tables and all its rows. With `compact`, the rows are in the snapshot
/// and the WAL is empty; otherwise they are only in the WAL.
async fn schema_without_a_table_of_the_room_is_rejected(compact: bool) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("schema-cover").unwrap();
    let engine = disk_engine(dir.path());
    engine
        .open_room(&room, tables_schema(&[0, 1]))
        .await
        .unwrap();
    let batch = vec![table_insert(1, 0, 1), table_insert(2, 1, 7)];
    engine.apply_batch(&room, batch).await.unwrap();
    if compact {
        engine.compact_room(&room).await.unwrap();
    }
    engine.close_room(&room).await.unwrap();
    let files = room_files(&engine, &room);

    let err = engine
        .open_room(&room, tables_schema(&[0]))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::SchemaMismatch { table_id: 1, .. }),
        "{err:?}"
    );
    assert_eq!(room_files(&engine, &room), files);

    engine
        .open_room(&room, tables_schema(&[0, 1]))
        .await
        .unwrap();
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 2);
    let key = PrimaryKey::single(7i64);
    assert!(engine.get_by_id(&room, 1, &key).await.unwrap().is_some());
}

#[tokio::test]
async fn reopening_with_a_table_missing_from_the_wal_is_rejected() {
    schema_without_a_table_of_the_room_is_rejected(false).await;
}

#[tokio::test]
async fn reopening_with_a_table_missing_from_the_snapshot_is_rejected() {
    schema_without_a_table_of_the_room_is_rejected(true).await;
}

#[tokio::test]
async fn reopening_with_a_new_table_keeps_the_data() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("schema-wider").unwrap();
    let engine = disk_engine(dir.path());
    engine.open_room(&room, tables_schema(&[0])).await.unwrap();
    engine
        .apply_batch(&room, vec![table_insert(1, 0, 1)])
        .await
        .unwrap();
    engine.compact_room(&room).await.unwrap();
    engine
        .apply_batch(&room, vec![table_insert(2, 0, 2)])
        .await
        .unwrap();
    engine.close_room(&room).await.unwrap();

    engine
        .open_room(&room, tables_schema(&[0, 1]))
        .await
        .unwrap();
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 2);
    for key in [1i64, 2] {
        let key = PrimaryKey::single(key);
        assert!(engine.get_by_id(&room, 0, &key).await.unwrap().is_some());
    }
    engine
        .apply_batch(&room, vec![table_insert(3, 1, 9)])
        .await
        .unwrap();
}

#[tokio::test]
async fn reopening_with_a_table_missing_from_the_rotated_wal_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("schema-rotated").unwrap();
    // The snapshot holds no rows (an empty range).
    let no_rows = std::ops::RangeInclusive::new(1, 0);
    let engine = seed_interrupted_compaction(dir.path(), &room, no_rows, 1..=5, 6..=10).await;
    // Only the rotated WAL holds data: the error must come from replaying it.
    std::fs::write(engine.wal_file_path(&room), []).unwrap();
    let rotated = compacting_wal_path(&engine, &room);
    let files = (room_files(&engine, &room), std::fs::read(&rotated).unwrap());

    let err = engine
        .open_room(&room, tables_schema(&[5]))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::SchemaMismatch { table_id: 0, .. }),
        "{err:?}"
    );
    // Not folded: every file is as it was.
    let after = (room_files(&engine, &room), std::fs::read(&rotated).unwrap());
    assert!(after == files);
}

#[tokio::test]
async fn an_empty_table_of_the_snapshot_does_not_have_to_be_in_the_schema() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("schema-empty-table").unwrap();
    let engine = disk_engine(dir.path());
    engine
        .open_room(&room, tables_schema(&[0, 1]))
        .await
        .unwrap();
    engine
        .apply_batch(&room, vec![table_insert(1, 0, 1)])
        .await
        .unwrap();
    engine.compact_room(&room).await.unwrap();
    engine.close_room(&room).await.unwrap();

    engine.open_room(&room, tables_schema(&[0])).await.unwrap();
    let key = PrimaryKey::single(1i64);
    assert!(engine.get_by_id(&room, 0, &key).await.unwrap().is_some());
}

/// `staff`: an integer key, a nullable name and the nullable key of another member, its boss;
/// with `constrained`, also `UNIQUE (name)` and a foreign key from `boss` to `staff`.
fn staff_schema(constrained: bool) -> Schema {
    let mut staff = TableSchema::builder("staff")
        .table_id(0)
        .primary_key("id", DataType::Int)
        .nullable_column("name", DataType::String)
        .nullable_column("boss", DataType::Int);
    if constrained {
        staff = staff
            .unique("staff_name", ["name"])
            .foreign_key("staff_boss", ["boss"], "staff");
    }
    Schema::builder().table(staff).build()
}

/// An insert into `staff` at sequence `seq`.
fn member(seq: u64, id: i64, name: &str, boss: Option<i64>) -> SequencedOperation {
    let boss = boss.map_or(Value::Null, Value::Int);
    let row = CompactRow::new(vec![Value::Int(id), Value::String(name.into()), boss]);
    sequenced(seq, Operation::insert(0, PrimaryKey::single(id), row, 0))
}

/// Writes `rows`, which break `constraint`, into a disk room without constraints and compacts
/// them into its snapshot, which the engine seals with a valid checksum. A `.wal.compacting`
/// and an active WAL with a torn write at its end follow. Opening the room with the constraints
/// fails naming the constraint before anything is truncated, folded or rewritten: every file
/// keeps its bytes, and the room still opens without the constraints, with all its data.
async fn local_snapshot_that_breaks_a_constraint_is_not_opened(
    constraint: &str,
    rows: Vec<SequencedOperation>,
) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("local-snapshot-breaks").unwrap();
    let engine = disk_engine(dir.path());
    engine.open_room(&room, staff_schema(false)).await.unwrap();
    let head = rows.len() as u64;
    let report = engine.apply_batch(&room, rows).await.unwrap();
    assert!(report.rejected().is_empty());
    engine.compact_room(&room).await.unwrap();
    engine.close_room(&room).await.unwrap();

    let compacting = compacting_wal_path(&engine, &room);
    let rotated = vec![member(head + 1, 10, "rotated", None)];
    std::fs::write(&compacting, encode_wal_batch(&rotated, None).unwrap()).unwrap();
    let active = vec![member(head + 2, 11, "active", None)];
    let mut wal = encode_wal_batch(&active, None).unwrap();
    wal.extend_from_slice(&[0x20, 0x00, 0x00, 0x00, 0xAA]);
    std::fs::write(engine.wal_file_path(&room), wal).unwrap();
    let files = || {
        (
            room_files(&engine, &room),
            std::fs::read(&compacting).unwrap(),
        )
    };
    let before = files();

    let err = engine
        .open_room(&room, staff_schema(true))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, StorageError::SnapshotCorruption(message) if message.contains(constraint)),
        "{err:?}"
    );
    assert!(files() == before, "a file changed");

    engine.open_room(&room, staff_schema(false)).await.unwrap();
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), head + 2);
    for id in [10i64, 11] {
        let key = PrimaryKey::single(id);
        assert!(engine.get_by_id(&room, 0, &key).await.unwrap().is_some());
    }
}

#[tokio::test]
async fn opening_a_snapshot_with_a_repeated_unique_key_is_refused() {
    let rows = vec![member(1, 1, "dup", None), member(2, 2, "dup", None)];
    local_snapshot_that_breaks_a_constraint_is_not_opened("staff_name", rows).await;
}

#[tokio::test]
async fn opening_a_snapshot_with_a_dangling_foreign_key_is_refused() {
    let rows = vec![
        member(1, 1, "boss", Some(1)),
        member(2, 2, "staff", Some(3)),
    ];
    local_snapshot_that_breaks_a_constraint_is_not_opened("staff_boss", rows).await;
}
