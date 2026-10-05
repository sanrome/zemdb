use std::sync::Arc;
use zemdb_core::{
    ColumnUpdate, CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value,
};
use zemdb_storage::{DiskStorageEngine, DiskStorageOptions, StorageEngine};

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

#[tokio::test]
async fn test_disk_zstd_snapshot_and_restore() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options);
    let room_a = RoomId::new("room-snap-a").unwrap();
    let room_b = RoomId::new("room-snap-b").unwrap();
    let schema = test_schema();

    engine.open_room(&room_a, schema.clone()).await.unwrap();

    let mut ops = Vec::new();
    for i in 1..=50i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User Name {i}").into()),
            Value::Int(i * 100),
            Value::Bool(i % 2 == 0),
        ]);
        ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    engine.apply_batch(&room_a, ops).await.unwrap();

    // Create compressed snapshot
    let snapshot_bytes = engine.create_snapshot(&room_a).await.unwrap();
    assert!(!snapshot_bytes.is_empty());

    // Restore into room_b
    engine.open_room(&room_b, schema.clone()).await.unwrap();
    let restored_head = engine
        .apply_snapshot(&room_b, schema, &snapshot_bytes)
        .await
        .unwrap();
    assert_eq!(restored_head, SequenceNumber::from(50u64));

    let row10 = engine
        .get(&room_b, "users", &PrimaryKey::single(10i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row10[1], Value::String("User Name 10".into()));
}

#[tokio::test]
async fn test_disk_compaction_and_wal_truncation() {
    let temp_dir = tempfile::tempdir().unwrap();
    // Configure small threshold to trigger compaction quickly
    let options = DiskStorageOptions::new(temp_dir.path())
        .min_compaction_bytes(200)
        .compaction_ratio(1.5);
    let engine = DiskStorageEngine::new(options);
    let room_id = RoomId::new("room-compact").unwrap();
    let schema = test_schema();

    engine.open_room(&room_id, schema).await.unwrap();

    // Insert initial row
    let row = CompactRow::new(vec![
        Value::Int(1),
        Value::String("Initial".into()),
        Value::Int(10),
        Value::Bool(true),
    ]);
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

    // Create an initial snapshot via explicit compact
    engine.compact_room(&room_id).await.unwrap();

    // Generate repeated updates on row 1 to grow WAL past compaction threshold
    for i in 2..=30u64 {
        let update_op = vec![SequencedOperation::with_default_origin(
            i,
            Operation::update(
                USERS_TABLE,
                PrimaryKey::single(1i64),
                vec![ColumnUpdate::new(2, Value::Int((i * 10) as i64))],
                100 + i,
            ),
        )];
        engine.apply_batch(&room_id, update_op).await.unwrap();
    }

    // Verify state after auto compaction
    let current_row = engine
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current_row[2], Value::Int(300));
    assert_eq!(
        engine.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(30u64)
    );
}

#[tokio::test]
async fn test_disk_concurrent_readers_and_writers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = Arc::new(DiskStorageEngine::new(options));
    let room_id = RoomId::new("room-concurrent-disk").unwrap();
    let schema = test_schema();

    engine.open_room(&room_id, schema).await.unwrap();

    // Writer task
    let engine_writer = Arc::clone(&engine);
    let r_writer = room_id.clone();
    let writer_handle = tokio::spawn(async move {
        for i in 1..=15i64 {
            let row = CompactRow::new(vec![
                Value::Int(i),
                Value::String(format!("Concurrent User {i}").into()),
                Value::Int(i * 10),
                Value::Bool(true),
            ]);
            let ops = vec![SequencedOperation::with_default_origin(
                i as u64,
                Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
            )];
            engine_writer.apply_batch(&r_writer, ops).await.unwrap();
            tokio::task::yield_now().await;
        }
    });

    // Reader task
    let engine_reader = Arc::clone(&engine);
    let r_reader = room_id.clone();
    let reader_handle = tokio::spawn(async move {
        let mut successful_reads = 0;
        for _ in 0..15 {
            if let Ok(Some(_)) = engine_reader
                .get(&r_reader, "users", &PrimaryKey::single(1i64))
                .await
            {
                successful_reads += 1;
            }
            tokio::task::yield_now().await;
        }
        successful_reads
    });

    writer_handle.await.unwrap();
    let _ = reader_handle.await.unwrap();

    assert_eq!(
        engine.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(15u64)
    );
    let user15 = engine
        .get(&room_id, "users", &PrimaryKey::single(15i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user15[1], Value::String("Concurrent User 15".into()));
}

#[tokio::test]
async fn test_cow_compaction_concurrent_writes_unblocked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path()).zstd_level(1);
    let engine = Arc::new(DiskStorageEngine::new(options.clone()));
    let room_id = RoomId::new("room-cow-concurrent").unwrap();
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    // 1. Insert initial batch of 100 rows
    let mut initial_ops = Vec::new();
    for i in 1..=100i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        initial_ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    engine.apply_batch(&room_id, initial_ops).await.unwrap();

    // 2. Spawn concurrent writer task that applies 50 more updates while compaction is triggered
    let engine_compactor = Arc::clone(&engine);
    let room_compactor = room_id.clone();
    let compactor_handle =
        tokio::spawn(async move { engine_compactor.compact_room(&room_compactor).await });

    let engine_writer = Arc::clone(&engine);
    let room_writer = room_id.clone();
    let writer_handle = tokio::spawn(async move {
        let mut ops = Vec::new();
        for i in 101..=150i64 {
            let row = CompactRow::new(vec![
                Value::Int(i),
                Value::String(format!("User {i}").into()),
                Value::Int(i * 10),
                Value::Bool(true),
            ]);
            ops.push(SequencedOperation::with_default_origin(
                i as u64,
                Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 200),
            ));
        }
        engine_writer.apply_batch(&room_writer, ops).await
    });

    let (comp_res, write_res) = tokio::join!(compactor_handle, writer_handle);
    comp_res.unwrap().unwrap();
    write_res.unwrap().unwrap();

    // Verify head sequence advanced to 150
    assert_eq!(
        engine.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(150u64)
    );

    // Verify both compacted rows and concurrent rows exist
    let row1 = engine
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row1[1], Value::String("User 1".into()));

    let row150 = engine
        .get(&room_id, "users", &PrimaryKey::single(150i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row150[1], Value::String("User 150".into()));

    // Close and reopen to ensure WAL and snapshot recovery works seamlessly
    engine.close_room(&room_id).await.unwrap();

    let engine2 = DiskStorageEngine::new(options);
    engine2.open_room(&room_id, schema).await.unwrap();
    assert_eq!(
        engine2.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(150u64)
    );
    let row150_reopen = engine2
        .get(&room_id, "users", &PrimaryKey::single(150i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row150_reopen[1], Value::String("User 150".into()));
}

#[tokio::test]
async fn test_compaction_unique_tmp_paths() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options);
    let room_id = RoomId::new("room-unique-tmp").unwrap();
    let schema = test_schema();

    engine.open_room(&room_id, schema).await.unwrap();

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

    // Trigger multiple sequential compactions
    for _ in 0..5 {
        engine.compact_room(&room_id).await.unwrap();
    }

    // Verify directory contains only clean .snap and .wal without leaked .snap.tmp files
    let mut entries = tokio::fs::read_dir(temp_dir.path()).await.unwrap();
    let mut found_tmp = false;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let file_name = entry.file_name().to_string_lossy().to_string();
        if file_name.contains(".tmp.") {
            found_tmp = true;
        }
    }
    assert!(!found_tmp, "Temporary snapshot files must be cleaned up");
}

#[tokio::test]
async fn test_crash_recovery_during_compaction_window_after_snapshot_rename() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("room-crash-after-snap-rename").unwrap();
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    // 1. Initial 10 rows
    let mut initial_ops = Vec::new();
    for i in 1..=10i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        initial_ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    engine.apply_batch(&room_id, initial_ops).await.unwrap();

    // Compact room: snapshot advances to seq 10
    engine.compact_room(&room_id).await.unwrap();
    engine.close_room(&room_id).await.unwrap();

    // 2. Simulate pre-crash state where snapshot has already been updated/renamed up to seq 10,
    // but wal.compacting (containing ops 1..=10) was not yet deleted before crash,
    // and active wal has new concurrent writes (11..=15).
    let mut compacting_ops = Vec::new();
    for i in 1..=10i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        compacting_ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    let compacting_bytes = zemdb_storage::format::encode_wal_batch(&compacting_ops, None).unwrap();
    let compacting_path = temp_dir
        .path()
        .join(format!("room_{}.wal.compacting", room_id.as_str()));
    tokio::fs::write(&compacting_path, compacting_bytes)
        .await
        .unwrap();

    let mut wal_ops = Vec::new();
    for i in 11..=15i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        wal_ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 200),
        ));
    }
    let wal_bytes = zemdb_storage::format::encode_wal_batch(&wal_ops, None).unwrap();
    let wal_path = temp_dir
        .path()
        .join(format!("room_{}.wal", room_id.as_str()));
    tokio::fs::write(&wal_path, wal_bytes).await.unwrap();

    // 3. Re-open room to trigger crash recovery
    let engine_rec = DiskStorageEngine::new(options);
    engine_rec.open_room(&room_id, schema).await.unwrap();

    // Verify wal.compacting was detected as already folded into snapshot and cleaned up
    assert!(!compacting_path.exists(), "wal.compacting must be deleted");

    // Verify head sequence advanced to 15
    assert_eq!(
        engine_rec.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(15u64)
    );

    // Verify all rows 1..=15 exist without duplication or corruption
    for i in 1..=15i64 {
        let row = engine_rec
            .get(&room_id, "users", &PrimaryKey::single(i))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row[1], Value::String(format!("User {i}").into()));
    }
}

#[tokio::test]
async fn test_crash_recovery_during_compaction_window_before_snapshot_rename() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options.clone());
    let room_id = RoomId::new("room-crash-before-snap-rename").unwrap();
    let schema = test_schema();

    engine.open_room(&room_id, schema.clone()).await.unwrap();

    // 1. Initial 5 rows
    let mut initial_ops = Vec::new();
    for i in 1..=5i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        initial_ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    engine.apply_batch(&room_id, initial_ops).await.unwrap();

    // Compact room: snapshot advances to seq 5
    engine.compact_room(&room_id).await.unwrap();
    engine.close_room(&room_id).await.unwrap();

    // 2. Simulate pre-crash state during Phase 2:
    // Base snapshot is at seq 5.
    // Compaction rotated WAL: wal.compacting contains rows 6..=10.
    // An unfinished temporary snapshot file snap.tmp.uuid exists with partial/temp data.
    // Active wal contains subsequent writes 11..=15.
    let mut compacting_ops = Vec::new();
    for i in 6..=10i64 {
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
    let compacting_bytes = zemdb_storage::format::encode_wal_batch(&compacting_ops, None).unwrap();
    let compacting_path = temp_dir
        .path()
        .join(format!("room_{}.wal.compacting", room_id.as_str()));
    tokio::fs::write(&compacting_path, compacting_bytes)
        .await
        .unwrap();

    // Unfinished snapshot temporary file
    let tmp_snap_path = temp_dir
        .path()
        .join(format!("room_{}.snap.tmp.deadbeef-1234", room_id.as_str()));
    tokio::fs::write(&tmp_snap_path, b"incomplete-snapshot-bytes")
        .await
        .unwrap();

    // Active wal with rows 11..=15
    let mut wal_ops = Vec::new();
    for i in 11..=15i64 {
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
    let wal_bytes = zemdb_storage::format::encode_wal_batch(&wal_ops, None).unwrap();
    let wal_path = temp_dir
        .path()
        .join(format!("room_{}.wal", room_id.as_str()));
    tokio::fs::write(&wal_path, wal_bytes).await.unwrap();

    // 3. Re-open room to trigger crash recovery
    let engine_rec = DiskStorageEngine::new(options);
    engine_rec.open_room(&room_id, schema).await.unwrap();

    // Verify orphan tmp file was purged
    assert!(
        !tmp_snap_path.exists(),
        "Orphan tmp snapshot file must be removed"
    );

    // Verify wal.compacting was merged into active wal and deleted
    assert!(!compacting_path.exists(), "wal.compacting must be deleted");

    // Verify head sequence advanced to 15
    assert_eq!(
        engine_rec.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(15u64)
    );

    // Verify all rows 1..=15 are intact
    for i in 1..=15i64 {
        let row = engine_rec
            .get(&room_id, "users", &PrimaryKey::single(i))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row[1], Value::String(format!("User {i}").into()));
    }
}
