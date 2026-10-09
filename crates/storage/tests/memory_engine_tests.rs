use std::sync::Arc;
use zemdb_core::{
    ColumnUpdate, CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value,
};
use zemdb_storage::{MemoryStorageEngine, StorageEngine, StorageError};

const USERS_TABLE: u16 = 0;

fn test_schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS_TABLE)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .column("score", DataType::Int)
        .column("active", DataType::Bool);

    Schema::builder().table(users).build()
}

#[tokio::test]
async fn test_room_lifecycle() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-1").unwrap();
    let schema = test_schema();

    // 1. Open room
    assert!(engine.open_room(&room_id, schema.clone()).await.is_ok());

    // 2. Open again should fail with RoomAlreadyOpen
    let err = engine.open_room(&room_id, schema).await.unwrap_err();
    assert!(matches!(err, StorageError::RoomAlreadyOpen(_)));

    // 3. Head seq should initially be 0
    let head = engine.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head, SequenceNumber::from(0u64));

    // 4. Close room
    assert!(engine.close_room(&room_id).await.is_ok());

    // 5. Querying closed room should fail with RoomNotFound
    let err = engine
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::RoomNotFound(_)));
}

#[tokio::test]
async fn test_point_lookup_and_batch_mutation() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-point").unwrap();
    engine.open_room(&room_id, test_schema()).await.unwrap();

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
    assert_eq!(res1, Some(row1));

    let res2 = engine
        .get(&room_id, "users", &PrimaryKey::single(2i64))
        .await
        .unwrap();
    assert_eq!(res2, Some(row2));

    let res3 = engine
        .get(&room_id, "users", &PrimaryKey::single(999i64))
        .await
        .unwrap();
    assert_eq!(res3, None);

    // Update Alice's score to 120 (column_idx 2)
    let update_op = vec![SequencedOperation::with_default_origin(
        3u64,
        Operation::update(
            USERS_TABLE,
            PrimaryKey::single(1i64),
            vec![ColumnUpdate::new(2, Value::Int(120))],
            30,
        ),
    )];
    engine.apply_batch(&room_id, update_op).await.unwrap();

    let updated_alice = engine
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated_alice[2], Value::Int(120));

    // Delete Bob
    let delete_op = vec![SequencedOperation::with_default_origin(
        4u64,
        Operation::delete(USERS_TABLE, PrimaryKey::single(2i64), 40),
    )];
    engine.apply_batch(&room_id, delete_op).await.unwrap();

    let bob_after_del = engine
        .get(&room_id, "users", &PrimaryKey::single(2i64))
        .await
        .unwrap();
    assert_eq!(bob_after_del, None);
}

#[tokio::test]
async fn test_concurrent_rooms_isolation() {
    let engine = Arc::new(MemoryStorageEngine::new());
    let room_a = RoomId::new("room-a").unwrap();
    let room_b = RoomId::new("room-b").unwrap();
    let schema = test_schema();

    engine.open_room(&room_a, schema.clone()).await.unwrap();
    engine.open_room(&room_b, schema).await.unwrap();

    // Spawn parallel writers to room A and room B
    let engine_a = Arc::clone(&engine);
    let ra = room_a.clone();
    let handle_a = tokio::spawn(async move {
        for i in 1..=20i64 {
            let row = CompactRow::new(vec![
                Value::Int(i),
                Value::String(format!("User A {i}").into()),
                Value::Int(i * 10),
                Value::Bool(true),
            ]);
            let ops = vec![SequencedOperation::with_default_origin(
                i as u64,
                Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
            )];
            engine_a.apply_batch(&ra, ops).await.unwrap();
        }
    });

    let engine_b = Arc::clone(&engine);
    let rb = room_b.clone();
    let handle_b = tokio::spawn(async move {
        for i in 1..=20i64 {
            let row = CompactRow::new(vec![
                Value::Int(i),
                Value::String(format!("User B {i}").into()),
                Value::Int(i * 20),
                Value::Bool(false),
            ]);
            let ops = vec![SequencedOperation::with_default_origin(
                i as u64,
                Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
            )];
            engine_b.apply_batch(&rb, ops).await.unwrap();
        }
    });

    // Concurrently read while writing
    let engine_reader = Arc::clone(&engine);
    let ra_reader = room_a.clone();
    let handle_reader = tokio::spawn(async move {
        let mut reads = 0;
        for _ in 0..10 {
            let _ = engine_reader
                .get(&ra_reader, "users", &PrimaryKey::single(1i64))
                .await;
            reads += 1;
            tokio::task::yield_now().await;
        }
        reads
    });

    handle_a.await.unwrap();
    handle_b.await.unwrap();
    let reads = handle_reader.await.unwrap();
    assert_eq!(reads, 10);

    // Verify independent state
    assert_eq!(
        engine.get_head_seq(&room_a).await.unwrap(),
        SequenceNumber::from(20u64)
    );
    assert_eq!(
        engine.get_head_seq(&room_b).await.unwrap(),
        SequenceNumber::from(20u64)
    );

    let user_a1 = engine
        .get(&room_a, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .unwrap();
    let user_b1 = engine
        .get(&room_b, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user_a1[1], Value::String("User A 1".into()));
    assert_eq!(user_b1[1], Value::String("User B 1".into()));
}

#[tokio::test]
async fn test_snapshot_create_and_restore() {
    let engine_a = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-snap").unwrap();
    let schema = test_schema();
    engine_a.open_room(&room_id, schema.clone()).await.unwrap();

    let row = CompactRow::new(vec![
        Value::Int(42),
        Value::String("Snapshot User".into()),
        Value::Int(999),
        Value::Bool(true),
    ]);

    let ops = vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(42i64), row.clone(), 100),
    )];
    engine_a.apply_batch(&room_id, ops).await.unwrap();

    // Create snapshot
    let snapshot_bytes = engine_a.create_snapshot(&room_id).await.unwrap();
    assert!(!snapshot_bytes.is_empty());

    // Restore snapshot into fresh engine_b
    let engine_b = MemoryStorageEngine::new();
    engine_b.open_room(&room_id, schema.clone()).await.unwrap();
    let restored_head = engine_b
        .apply_snapshot(&room_id, schema, &snapshot_bytes)
        .await
        .unwrap();
    assert_eq!(restored_head, SequenceNumber::from(1u64));

    let restored_row = engine_b
        .get(&room_id, "users", &PrimaryKey::single(42i64))
        .await
        .unwrap();
    assert_eq!(restored_row, Some(row));

    let head_b = engine_b.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head_b, SequenceNumber::from(1u64));
}

#[tokio::test]
async fn test_sequence_mismatch_rejected() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("seq_test").unwrap();
    let schema = test_schema();
    engine.open_room(&room_id, schema).await.unwrap();

    let row = CompactRow::new(vec![
        Value::Int(1),
        Value::String("User".into()),
        Value::Int(100),
        Value::Bool(true),
    ]);

    // Expected 1, but provided 5
    let bad_ops = vec![SequencedOperation::with_default_origin(
        5u64,
        Operation::insert(USERS_TABLE, PrimaryKey::single(1i64), row, 100),
    )];

    let err = engine.apply_batch(&room_id, bad_ops).await.unwrap_err();
    match err {
        StorageError::SequenceMismatch { expected, actual } => {
            assert_eq!(expected, SequenceNumber::from(1u64));
            assert_eq!(actual, SequenceNumber::from(5u64));
        }
        other => panic!("Expected SequenceMismatch error, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_dynamic_column_update_resizing_memory() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-dynamic-col").unwrap();

    // Table schema with 4 columns (2 initial + 2 evolved nullable columns)
    let users_table = TableSchema::builder("users")
        .table_id(USERS_TABLE)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .nullable_column("score", DataType::Int)
        .nullable_column("note", DataType::String);

    let schema = Schema::builder().table(users_table).build();
    engine.open_room(&room_id, schema).await.unwrap();

    // 1. Insert a 2-column row (from a client prior to adding score and note)
    let row = CompactRow::new(vec![Value::Int(1), Value::String("Alice".into())]);
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

    // 2. Apply an Update targeting column 3 (note)
    engine
        .apply_batch(
            &room_id,
            vec![SequencedOperation::with_default_origin(
                2u64,
                Operation::update(
                    USERS_TABLE,
                    PrimaryKey::single(1i64),
                    vec![ColumnUpdate::new(3, Value::String("Updated Note".into()))],
                    110,
                ),
            )],
        )
        .await
        .unwrap();

    // 3. Verify the row was resized to 4 columns, column 2 is Null, and column 3 has "Updated Note"
    let updated_row = engine
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .expect("row must exist");

    assert_eq!(updated_row.len(), 4);
    assert_eq!(updated_row[0], Value::Int(1));
    assert_eq!(updated_row[1], Value::String("Alice".into()));
    assert_eq!(updated_row[2], Value::Null);
    assert_eq!(updated_row[3], Value::String("Updated Note".into()));
}
