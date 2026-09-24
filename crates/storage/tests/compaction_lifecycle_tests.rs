use std::sync::Arc;
use rimdb_core::{
    ColumnUpdate, CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value,
};
use rimdb_storage::{DiskStorageEngine, DiskStorageOptions, StorageEngine};

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
    let room_a = RoomId::new("room-snap-a");
    let room_b = RoomId::new("room-snap-b");
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
    let restored_head = engine.apply_snapshot(&room_b, schema, &snapshot_bytes).await.unwrap();
    assert_eq!(restored_head, SequenceNumber::from(50u64));

    let row10 = engine.get(&room_b, "users", &PrimaryKey::single(10i64)).await.unwrap().unwrap();
    assert_eq!(row10.values[1], Value::String("User Name 10".into()));
}

#[tokio::test]
async fn test_disk_compaction_and_wal_truncation() {
    let temp_dir = tempfile::tempdir().unwrap();
    // Configure small threshold to trigger compaction quickly
    let options = DiskStorageOptions::new(temp_dir.path())
        .min_compaction_bytes(200)
        .compaction_ratio(1.5);
    let engine = DiskStorageEngine::new(options);
    let room_id = RoomId::new("room-compact");
    let schema = test_schema();

    engine.open_room(&room_id, schema).await.unwrap();

    // Insert initial row
    let row = CompactRow::new(vec![
        Value::Int(1),
        Value::String("Initial".into()),
        Value::Int(10),
        Value::Bool(true),
    ]);
    engine.apply_batch(&room_id, vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row, 100),
    )]).await.unwrap();

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
    let current_row = engine.get(&room_id, "users", &PrimaryKey::single(1i64)).await.unwrap().unwrap();
    assert_eq!(current_row.values[2], Value::Int(300));
    assert_eq!(engine.get_head_seq(&room_id).await.unwrap(), SequenceNumber::from(30u64));
}

#[tokio::test]
async fn test_disk_concurrent_readers_and_writers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = Arc::new(DiskStorageEngine::new(options));
    let room_id = RoomId::new("room-concurrent-disk");
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
            if let Ok(Some(_)) = engine_reader.get(&r_reader, "users", &PrimaryKey::single(1i64)).await {
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
    let user15 = engine.get(&room_id, "users", &PrimaryKey::single(15i64)).await.unwrap().unwrap();
    assert_eq!(user15.values[1], Value::String("Concurrent User 15".into()));
}
