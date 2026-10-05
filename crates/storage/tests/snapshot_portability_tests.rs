use futures::StreamExt;
use zemdb_core::*;
use zemdb_storage::{
    DiskStorageEngine, DiskStorageOptions, MemoryStorageEngine, ScanOptions, StorageEngine,
    StorageError,
};

fn sample_schema() -> Schema {
    let table = TableSchema::builder("items")
        .table_id(1)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .nullable_column("price", DataType::Int)
        .build()
        .expect("valid table schema");
    Schema::builder().table(table).build()
}

#[tokio::test]
async fn test_cross_engine_snapshot_portability() {
    let schema = sample_schema();
    let room_id = RoomId::new("portability-room").unwrap();

    // 1. Memory engine populates state and creates snapshot
    let mem_engine = MemoryStorageEngine::new();
    mem_engine
        .open_room(&room_id, schema.clone())
        .await
        .unwrap();

    let row1 = CompactRow::new(vec![
        Value::Int(1),
        Value::String("Widget A".into()),
        Value::Int(100),
    ]);
    let row2 = CompactRow::new(vec![
        Value::Int(2),
        Value::String("Widget B".into()),
        Value::Int(200),
    ]);

    let ops = vec![
        SequencedOperation::with_default_origin(
            1u64,
            Operation::insert(1, PrimaryKey::single(1i64), row1, 10),
        ),
        SequencedOperation::with_default_origin(
            2u64,
            Operation::insert(1, PrimaryKey::single(2i64), row2, 20),
        ),
    ];
    let head = mem_engine.apply_batch(&room_id, ops).await.unwrap();
    assert_eq!(head, SequenceNumber::from(2u64));

    // Create snapshot from memory engine (Raw uncompressed envelope)
    let mem_snapshot = mem_engine.create_snapshot(&room_id).await.unwrap();
    assert_eq!(&mem_snapshot[0..4], b"ZMSN");
    assert_eq!(mem_snapshot[4], 1); // version 1
    assert_eq!(mem_snapshot[5], 0); // compression flag = 0 (Raw)

    // 2. Disk engine restores snapshot directly from memory engine
    let temp_dir = tempfile::tempdir().unwrap();
    let disk_options = DiskStorageOptions::new(temp_dir.path());
    let disk_engine = DiskStorageEngine::new(disk_options.clone());

    disk_engine
        .open_room(&room_id, schema.clone())
        .await
        .unwrap();
    let disk_head = disk_engine
        .apply_snapshot(&room_id, schema.clone(), &mem_snapshot)
        .await
        .unwrap();
    assert_eq!(disk_head, SequenceNumber::from(2u64));

    // Verify row lookups on disk engine via both table name and table_id
    let disk_row1_name = disk_engine
        .get(&room_id, "items", &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .expect("row 1 must exist");
    assert_eq!(disk_row1_name[0], Value::Int(1));
    assert_eq!(disk_row1_name[1], Value::String("Widget A".into()));

    let disk_row1_id = disk_engine
        .get_by_id(&room_id, 1, &PrimaryKey::single(1i64))
        .await
        .unwrap()
        .expect("row 1 must exist");
    assert_eq!(disk_row1_id, disk_row1_name);

    // 3. Disk engine creates snapshot (Zstandard compressed envelope)
    let disk_snapshot = disk_engine.create_snapshot(&room_id).await.unwrap();
    assert_eq!(&disk_snapshot[0..4], b"ZMSN");
    assert_eq!(disk_snapshot[4], 1); // version 1
    assert_eq!(disk_snapshot[5], 1); // compression flag = 1 (Zstd)

    // 4. Second Memory engine restores snapshot directly from disk engine
    let mem_engine2 = MemoryStorageEngine::new();
    mem_engine2
        .open_room(&room_id, schema.clone())
        .await
        .unwrap();
    let mem2_head = mem_engine2
        .apply_snapshot(&room_id, schema.clone(), &disk_snapshot)
        .await
        .unwrap();
    assert_eq!(mem2_head, SequenceNumber::from(2u64));

    let mem2_row2_id = mem_engine2
        .get_by_id(&room_id, 1, &PrimaryKey::single(2i64))
        .await
        .unwrap()
        .expect("row 2 must exist");
    assert_eq!(mem2_row2_id[0], Value::Int(2));
    assert_eq!(mem2_row2_id[1], Value::String("Widget B".into()));
    assert_eq!(mem2_row2_id[2], Value::Int(200));

    // 5. Verify scan_by_id on both engines
    let mut mem_scan = mem_engine2
        .scan_by_id(&room_id, 1, ScanOptions::new())
        .await
        .unwrap();
    let (pk1, r1) = mem_scan.next().await.unwrap().unwrap();
    assert_eq!(pk1, PrimaryKey::single(1i64));
    assert_eq!(r1[1], Value::String("Widget A".into()));

    let mut disk_scan = disk_engine
        .scan_by_id(&room_id, 1, ScanOptions::new())
        .await
        .unwrap();
    let (dpk1, dr1) = disk_scan.next().await.unwrap().unwrap();
    assert_eq!(dpk1, PrimaryKey::single(1i64));
    assert_eq!(dr1[1], Value::String("Widget A".into()));
}

#[tokio::test]
async fn test_snapshot_envelope_corruption_detection() {
    let schema = sample_schema();
    let room_id = RoomId::new("corrupt-snap-room").unwrap();

    let mem_engine = MemoryStorageEngine::new();
    mem_engine
        .open_room(&room_id, schema.clone())
        .await
        .unwrap();
    let valid_snapshot = mem_engine.create_snapshot(&room_id).await.unwrap();

    // 1. Invalid magic
    let mut bad_magic = valid_snapshot.clone();
    bad_magic[0] = b'B';
    bad_magic[1] = b'A';
    bad_magic[2] = b'D';
    bad_magic[3] = b'!';
    let res = mem_engine
        .apply_snapshot(&room_id, schema.clone(), &bad_magic)
        .await;
    assert!(matches!(res, Err(StorageError::SnapshotCorruption(_))));

    // 2. Invalid version
    let mut bad_version = valid_snapshot.clone();
    bad_version[4] = 99;
    let res = mem_engine
        .apply_snapshot(&room_id, schema.clone(), &bad_version)
        .await;
    assert!(matches!(res, Err(StorageError::SnapshotCorruption(_))));

    // 3. CRC32 tampering on payload
    let mut bad_crc = valid_snapshot.clone();
    let last_idx = bad_crc.len() - 1;
    bad_crc[last_idx] ^= 0xFF;
    let res = mem_engine
        .apply_snapshot(&room_id, schema.clone(), &bad_crc)
        .await;
    assert!(matches!(res, Err(StorageError::SnapshotCorruption(_))));

    // 4. Truncated snapshot
    let truncated = &valid_snapshot[..10];
    let res = mem_engine
        .apply_snapshot(&room_id, schema.clone(), truncated)
        .await;
    assert!(matches!(res, Err(StorageError::SnapshotCorruption(_))));
}

#[tokio::test]
async fn test_storage_engine_apply_batch_schema_validation() {
    let schema = sample_schema();
    let room_id = RoomId::new("validation-room").unwrap();

    let mem_engine = MemoryStorageEngine::new();
    mem_engine
        .open_room(&room_id, schema.clone())
        .await
        .unwrap();

    // Bad operation: row arity mismatch (missing non-null column or wrong PK)
    let bad_row = CompactRow::new(vec![Value::Int(1)]);
    let bad_op = vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(1, PrimaryKey::single(1i64), bad_row, 10),
    )];

    let res = mem_engine.apply_batch(&room_id, bad_op).await;
    assert!(matches!(res, Err(StorageError::SchemaValidation(_))));

    // Same check on disk engine
    let temp_dir = tempfile::tempdir().unwrap();
    let disk_engine = DiskStorageEngine::new(DiskStorageOptions::new(temp_dir.path()));
    disk_engine
        .open_room(&room_id, schema.clone())
        .await
        .unwrap();

    let bad_row2 = CompactRow::new(vec![Value::Int(2)]);
    let bad_op2 = vec![SequencedOperation::with_default_origin(
        1u64,
        Operation::insert(1, PrimaryKey::single(2i64), bad_row2, 10),
    )];

    let res_disk = disk_engine.apply_batch(&room_id, bad_op2).await;
    assert!(matches!(res_disk, Err(StorageError::SchemaValidation(_))));
}
