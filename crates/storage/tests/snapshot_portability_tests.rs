mod common;

use common::{disk_engine, sequenced};
use futures::StreamExt;
use zemdb_core::*;
use zemdb_storage::{MemoryStorageEngine, ScanOptions, StorageEngine, StorageError};

fn sample_schema() -> Schema {
    let table = TableSchema::builder("items")
        .table_id(1)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .nullable_column("price", DataType::Int);
    Schema::builder().table(table).build()
}

fn insert_item(seq: u64, id: i64, row: CompactRow, timestamp: u64) -> SequencedOperation {
    sequenced(
        seq,
        Operation::insert(1, PrimaryKey::single(id), row, timestamp),
    )
}

#[tokio::test]
async fn test_cross_engine_snapshot_portability() {
    let schema = sample_schema();
    let room_id = RoomId::new("portability-room").unwrap();

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
    let ops = vec![insert_item(1, 1, row1, 10), insert_item(2, 2, row2, 20)];
    let head = mem_engine.apply_batch(&room_id, ops).await.unwrap();
    assert_eq!(head, SequenceNumber::from(2u64));

    // The memory engine writes a raw, uncompressed envelope.
    let mem_snapshot = mem_engine.create_snapshot(&room_id).await.unwrap();
    assert_eq!(&mem_snapshot[0..4], b"ZMSN");
    assert_eq!(mem_snapshot[4], 1); // version 1
    assert_eq!(mem_snapshot[5], 0); // compression flag = 0 (Raw)

    let temp_dir = tempfile::tempdir().unwrap();
    let disk_engine = disk_engine(temp_dir.path());
    disk_engine
        .open_room(&room_id, schema.clone())
        .await
        .unwrap();
    let disk_head = disk_engine
        .apply_snapshot(&room_id, schema.clone(), mem_snapshot.into(), 2u64.into())
        .await
        .unwrap();
    assert_eq!(disk_head, SequenceNumber::from(2u64));

    // Lookups on the disk engine by table name and by table_id.
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

    // The disk engine writes a Zstandard-compressed envelope.
    let disk_snapshot = disk_engine.create_snapshot(&room_id).await.unwrap();
    assert_eq!(&disk_snapshot[0..4], b"ZMSN");
    assert_eq!(disk_snapshot[4], 1); // version 1
    assert_eq!(disk_snapshot[5], 1); // compression flag = 1 (Zstd)

    let mem_engine2 = MemoryStorageEngine::new();
    mem_engine2
        .open_room(&room_id, schema.clone())
        .await
        .unwrap();
    let mem2_head = mem_engine2
        .apply_snapshot(&room_id, schema.clone(), disk_snapshot.into(), 2u64.into())
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

    let damaged = |damage: fn(&mut Vec<u8>)| {
        let mut bytes = valid_snapshot.clone();
        damage(&mut bytes);
        bytes
    };
    let cases = [
        (
            "invalid magic",
            damaged(|b| b[..4].copy_from_slice(b"BAD!")),
        ),
        ("invalid version", damaged(|b| b[4] = 99)),
        (
            "tampered payload CRC32",
            damaged(|b| *b.last_mut().unwrap() ^= 0xFF),
        ),
        ("truncated", valid_snapshot[..10].to_vec()),
    ];
    for (fault, bytes) in cases {
        let res = mem_engine
            .apply_snapshot(&room_id, schema.clone(), bytes.into(), 0u64.into())
            .await;
        assert!(
            matches!(res, Err(StorageError::SnapshotCorruption(_))),
            "{fault}: {res:?}"
        );
    }
}

/// Rejects a row with fewer values than the table has non-null columns.
async fn row_missing_columns_is_rejected<E: StorageEngine>(engine: &E) {
    let room_id = RoomId::new("validation-room").unwrap();
    engine.open_room(&room_id, sample_schema()).await.unwrap();

    let bad_row = CompactRow::new(vec![Value::Int(1)]);
    let res = engine
        .apply_batch(&room_id, vec![insert_item(1, 1, bad_row, 10)])
        .await;
    assert!(
        matches!(res, Err(StorageError::SchemaValidation(_))),
        "{res:?}"
    );
}

#[tokio::test]
async fn memory_rejects_a_row_missing_columns() {
    row_missing_columns_is_rejected(&MemoryStorageEngine::new()).await;
}

#[tokio::test]
async fn disk_rejects_a_row_missing_columns() {
    let temp_dir = tempfile::tempdir().unwrap();
    row_missing_columns_is_rejected(&disk_engine(temp_dir.path())).await;
}
