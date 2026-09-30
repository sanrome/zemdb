use futures::StreamExt;
use rimdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation, TableSchema,
    Value,
};
use rimdb_storage::{
    DiskStorageEngine, DiskStorageOptions, KeyRange, MemoryStorageEngine, ScanOptions,
    StorageEngine,
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

#[tokio::test]
async fn test_range_scans_and_reverse_ordering() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-scan");
    engine.open_room(&room_id, test_schema()).await.unwrap();

    let mut ops = Vec::new();
    for i in 1..=5i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User {i}").into()),
            Value::Int(i * 10),
            Value::Bool(true),
        ]);
        ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    engine.apply_batch(&room_id, ops).await.unwrap();

    // 1. Forward full scan
    let mut stream = engine
        .scan(&room_id, "users", ScanOptions::new())
        .await
        .unwrap();
    let mut pks = Vec::new();
    while let Some(item) = stream.next().await {
        let (pk, _) = item.unwrap();
        pks.push(pk);
    }
    assert_eq!(
        pks,
        vec![
            PrimaryKey::single(1i64),
            PrimaryKey::single(2i64),
            PrimaryKey::single(3i64),
            PrimaryKey::single(4i64),
            PrimaryKey::single(5i64),
        ]
    );

    // 2. Backward scan (ORDER BY pk DESC)
    let mut desc_stream = engine
        .scan(&room_id, "users", ScanOptions::new().backward())
        .await
        .unwrap();
    let mut desc_pks = Vec::new();
    while let Some(item) = desc_stream.next().await {
        let (pk, _) = item.unwrap();
        desc_pks.push(pk);
    }
    assert_eq!(
        desc_pks,
        vec![
            PrimaryKey::single(5i64),
            PrimaryKey::single(4i64),
            PrimaryKey::single(3i64),
            PrimaryKey::single(2i64),
            PrimaryKey::single(1i64),
        ]
    );

    // 3. Sub-range scan: [2, 4]
    let range = KeyRange::from(PrimaryKey::single(2i64)..=PrimaryKey::single(4i64));
    let mut range_stream = engine
        .scan(&room_id, "users", ScanOptions::new().range(range))
        .await
        .unwrap();
    let mut range_pks = Vec::new();
    while let Some(item) = range_stream.next().await {
        let (pk, _) = item.unwrap();
        range_pks.push(pk);
    }
    assert_eq!(
        range_pks,
        vec![
            PrimaryKey::single(2i64),
            PrimaryKey::single(3i64),
            PrimaryKey::single(4i64),
        ]
    );
}

#[tokio::test]
async fn test_limit_and_projection_pushdowns() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-pushdowns");
    engine.open_room(&room_id, test_schema()).await.unwrap();

    let mut ops = Vec::new();
    for i in 1..=10i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("Name_{i}").into()),
            Value::Int(i * 100),
            Value::Bool(i % 2 == 0),
        ]);
        ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    engine.apply_batch(&room_id, ops).await.unwrap();

    // 1. Limit pushdown: limit 3
    let mut limit_stream = engine
        .scan(&room_id, "users", ScanOptions::new().limit(3))
        .await
        .unwrap();
    let mut count = 0;
    while let Some(item) = limit_stream.next().await {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 3);

    // 2. Limit pushdown + Backward: top 2 descending
    let mut desc_limit_stream = engine
        .scan(&room_id, "users", ScanOptions::new().backward().limit(2))
        .await
        .unwrap();
    let mut desc_limit_pks = Vec::new();
    while let Some(item) = desc_limit_stream.next().await {
        let (pk, _) = item.unwrap();
        desc_limit_pks.push(pk);
    }
    assert_eq!(
        desc_limit_pks,
        vec![PrimaryKey::single(10i64), PrimaryKey::single(9i64)]
    );

    // 3. Projection pushdown: only project column 1 (name) and column 3 (active)
    let mut proj_stream = engine
        .scan(
            &room_id,
            "users",
            ScanOptions::new().limit(1).projection(vec![1, 3]),
        )
        .await
        .unwrap();
    let (pk, proj_row) = proj_stream.next().await.unwrap().unwrap();
    assert_eq!(pk, PrimaryKey::single(1i64));
    assert_eq!(proj_row.len(), 2);
    assert_eq!(proj_row.values[0], Value::String("Name_1".into()));
    assert_eq!(proj_row.values[1], Value::Bool(false));
}

#[tokio::test]
async fn test_disk_query_pushdowns_parity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options);
    let room_id = RoomId::new("room-disk-pushdowns");
    let schema = test_schema();

    engine.open_room(&room_id, schema).await.unwrap();

    let mut ops = Vec::new();
    for i in 1..=10i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User_{i}").into()),
            Value::Int(i * 10),
            Value::Bool(i % 2 == 0),
        ]);
        ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    engine.apply_batch(&room_id, ops).await.unwrap();

    // Backward scan with limit 3 and projection [1]
    let mut stream = engine
        .scan(
            &room_id,
            "users",
            ScanOptions::new().backward().limit(3).projection(vec![1]),
        )
        .await
        .unwrap();

    let mut names = Vec::new();
    while let Some(item) = stream.next().await {
        let (pk, row) = item.unwrap();
        names.push((pk, row.values[0].clone()));
    }

    assert_eq!(names.len(), 3);
    assert_eq!(names[0], (PrimaryKey::single(10i64), Value::String("User_10".into())));
    assert_eq!(names[1], (PrimaryKey::single(9i64), Value::String("User_9".into())));
    assert_eq!(names[2], (PrimaryKey::single(8i64), Value::String("User_8".into())));
}

#[tokio::test]
async fn test_multi_batch_lazy_streaming_memory() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-multi-batch-mem");
    engine.open_room(&room_id, test_schema()).await.unwrap();

    let total = 150;
    let mut ops = Vec::with_capacity(total);
    for i in 1..=total as i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User_{i}").into()),
            Value::Int(i * 10),
            Value::Bool(i % 2 == 0),
        ]);
        ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    engine.apply_batch(&room_id, ops).await.unwrap();

    // 1. Forward scan across all 150 items (more than two 64-item batches)
    let mut forward_stream = engine
        .scan(&room_id, "users", ScanOptions::new())
        .await
        .unwrap();
    let mut forward_pks = Vec::new();
    while let Some(item) = forward_stream.next().await {
        let (pk, _) = item.unwrap();
        forward_pks.push(pk);
    }
    assert_eq!(forward_pks.len(), total);
    assert_eq!(forward_pks[0], PrimaryKey::single(1i64));
    assert_eq!(forward_pks[total - 1], PrimaryKey::single(total as i64));

    // 2. Backward scan across all 150 items
    let mut backward_stream = engine
        .scan(&room_id, "users", ScanOptions::new().backward())
        .await
        .unwrap();
    let mut backward_pks = Vec::new();
    while let Some(item) = backward_stream.next().await {
        let (pk, _) = item.unwrap();
        backward_pks.push(pk);
    }
    assert_eq!(backward_pks.len(), total);
    assert_eq!(backward_pks[0], PrimaryKey::single(total as i64));
    assert_eq!(backward_pks[total - 1], PrimaryKey::single(1i64));

    // 3. Multi-batch with limit (e.g. limit 75 spans 2 batches)
    let mut limit_stream = engine
        .scan(&room_id, "users", ScanOptions::new().limit(75))
        .await
        .unwrap();
    let mut count = 0;
    while let Some(item) = limit_stream.next().await {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 75);
}

#[tokio::test]
async fn test_multi_batch_lazy_streaming_disk() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = DiskStorageEngine::new(options);
    let room_id = RoomId::new("room-multi-batch-disk");
    engine.open_room(&room_id, test_schema()).await.unwrap();

    let total = 150;
    let mut ops = Vec::with_capacity(total);
    for i in 1..=total as i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("User_{i}").into()),
            Value::Int(i * 10),
            Value::Bool(i % 2 == 0),
        ]);
        ops.push(SequencedOperation::with_default_origin(
            i as u64,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100),
        ));
    }
    engine.apply_batch(&room_id, ops).await.unwrap();

    // 1. Forward scan across all 150 items
    let mut forward_stream = engine
        .scan(&room_id, "users", ScanOptions::new())
        .await
        .unwrap();
    let mut forward_pks = Vec::new();
    while let Some(item) = forward_stream.next().await {
        let (pk, _) = item.unwrap();
        forward_pks.push(pk);
    }
    assert_eq!(forward_pks.len(), total);
    assert_eq!(forward_pks[0], PrimaryKey::single(1i64));
    assert_eq!(forward_pks[total - 1], PrimaryKey::single(total as i64));

    // 2. Backward scan across all 150 items
    let mut backward_stream = engine
        .scan(&room_id, "users", ScanOptions::new().backward())
        .await
        .unwrap();
    let mut backward_pks = Vec::new();
    while let Some(item) = backward_stream.next().await {
        let (pk, _) = item.unwrap();
        backward_pks.push(pk);
    }
    assert_eq!(backward_pks.len(), total);
    assert_eq!(backward_pks[0], PrimaryKey::single(total as i64));
    assert_eq!(backward_pks[total - 1], PrimaryKey::single(1i64));

    // 3. Multi-batch with limit (e.g. limit 75 spans across channel buffers)
    let mut limit_stream = engine
        .scan(&room_id, "users", ScanOptions::new().limit(75))
        .await
        .unwrap();
    let mut count = 0;
    while let Some(item) = limit_stream.next().await {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 75);
}

#[tokio::test]
async fn test_scan_snapshot_isolation_memory() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-iso-mem");
    engine.open_room(&room_id, test_schema()).await.unwrap();

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

    // 2. Open scan stream and read first 2 rows
    let mut stream = engine
        .scan(&room_id, "users", ScanOptions::new())
        .await
        .unwrap();

    let (pk1, row1) = stream.next().await.unwrap().unwrap();
    assert_eq!(pk1, PrimaryKey::single(1i64));
    assert_eq!(row1.values[2], Value::Int(10));

    let (pk2, row2) = stream.next().await.unwrap().unwrap();
    assert_eq!(pk2, PrimaryKey::single(2i64));
    assert_eq!(row2.values[2], Value::Int(20));

    // 3. Mutate table concurrently: insert 11..15, update 3..10 to 9999, delete 5
    let mut mutate_ops = Vec::new();
    let mut seq = 11u64;
    for i in 11..=15i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("Phantom {i}").into()),
            Value::Int(i * 100),
            Value::Bool(false),
        ]);
        mutate_ops.push(SequencedOperation::with_default_origin(
            seq,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 200),
        ));
        seq += 1;
    }
    for i in 3..=10i64 {
        mutate_ops.push(SequencedOperation::with_default_origin(
            seq,
            Operation::update(
                USERS_TABLE,
                PrimaryKey::single(i),
                vec![rimdb_core::ColumnUpdate::new(2, Value::Int(9999))],
                201,
            ),
        ));
        seq += 1;
    }
    mutate_ops.push(SequencedOperation::with_default_origin(
        seq,
        Operation::delete(USERS_TABLE, PrimaryKey::single(5i64), 202),
    ));

    engine.apply_batch(&room_id, mutate_ops).await.unwrap();

    // 4. Continue reading from the original scan stream
    let mut remaining_pks = Vec::new();
    let mut remaining_scores = Vec::new();
    while let Some(item) = stream.next().await {
        let (pk, row) = item.unwrap();
        remaining_pks.push(pk);
        remaining_scores.push(row.values[2].clone());
    }

    // Verify snapshot isolation:
    // Exactly rows 3..=10 must be returned in sequence
    assert_eq!(
        remaining_pks,
        (3..=10i64).map(PrimaryKey::single).collect::<Vec<_>>()
    );
    // Scores must be the original ones (30, 40, ..., 100), NOT 9999
    assert_eq!(
        remaining_scores,
        (3..=10i64).map(|i| Value::Int(i * 10)).collect::<Vec<_>>()
    );
    // Deleted row 5 was still returned in the snapshot
    assert!(remaining_pks.contains(&PrimaryKey::single(5i64)));
    // Phantoms 11..15 were NOT returned
    assert!(!remaining_pks.contains(&PrimaryKey::single(11i64)));
}

#[tokio::test]
async fn test_scan_snapshot_isolation_disk() {
    let tmp = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(tmp.path());
    let engine = DiskStorageEngine::new(options);
    let room_id = RoomId::new("room-iso-disk");
    engine.open_room(&room_id, test_schema()).await.unwrap();

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

    // 2. Open scan stream and read first 2 rows
    let mut stream = engine
        .scan(&room_id, "users", ScanOptions::new())
        .await
        .unwrap();

    let (pk1, row1) = stream.next().await.unwrap().unwrap();
    assert_eq!(pk1, PrimaryKey::single(1i64));
    assert_eq!(row1.values[2], Value::Int(10));

    let (pk2, row2) = stream.next().await.unwrap().unwrap();
    assert_eq!(pk2, PrimaryKey::single(2i64));
    assert_eq!(row2.values[2], Value::Int(20));

    // 3. Mutate table concurrently
    let mut mutate_ops = Vec::new();
    let mut seq = 11u64;
    for i in 11..=15i64 {
        let row = CompactRow::new(vec![
            Value::Int(i),
            Value::String(format!("Phantom {i}").into()),
            Value::Int(i * 100),
            Value::Bool(false),
        ]);
        mutate_ops.push(SequencedOperation::with_default_origin(
            seq,
            Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 200),
        ));
        seq += 1;
    }
    for i in 3..=10i64 {
        mutate_ops.push(SequencedOperation::with_default_origin(
            seq,
            Operation::update(
                USERS_TABLE,
                PrimaryKey::single(i),
                vec![rimdb_core::ColumnUpdate::new(2, Value::Int(9999))],
                201,
            ),
        ));
        seq += 1;
    }
    mutate_ops.push(SequencedOperation::with_default_origin(
        seq,
        Operation::delete(USERS_TABLE, PrimaryKey::single(5i64), 202),
    ));

    engine.apply_batch(&room_id, mutate_ops).await.unwrap();

    // 4. Continue reading from the original scan stream
    let mut remaining_pks = Vec::new();
    let mut remaining_scores = Vec::new();
    while let Some(item) = stream.next().await {
        let (pk, row) = item.unwrap();
        remaining_pks.push(pk);
        remaining_scores.push(row.values[2].clone());
    }

    // Verify snapshot isolation on disk engine
    assert_eq!(
        remaining_pks,
        (3..=10i64).map(PrimaryKey::single).collect::<Vec<_>>()
    );
    assert_eq!(
        remaining_scores,
        (3..=10i64).map(|i| Value::Int(i * 10)).collect::<Vec<_>>()
    );
    assert!(remaining_pks.contains(&PrimaryKey::single(5i64)));
    assert!(!remaining_pks.contains(&PrimaryKey::single(11i64)));
}

