use crate::fail_point;
use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation, TableSchema,
    Value,
};

const USERS: u16 = 0;

fn schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .build()
        .unwrap();
    Schema::from_tables(vec![users])
}

fn insert(seq: u64) -> SequencedOperation {
    let row = CompactRow::new(vec![
        Value::Int(seq as i64),
        Value::String(format!("user {seq}").into()),
    ]);
    SequencedOperation::with_default_origin(
        seq,
        Operation::insert(USERS, PrimaryKey::single(seq as i64), row, 100),
    )
}

#[tokio::test]
async fn failed_wal_sync_marks_room_failed_until_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("sync-failure").unwrap();
    let options = DiskStorageOptions::new(dir.path()).auto_compact(false);
    let engine = DiskStorageEngine::new(options.clone());
    engine.open_room(&room, schema()).await.unwrap();
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    fail_point::arm("apply_batch.sync", &engine.wal_file_path(&room));
    assert!(engine.apply_batch(&room, vec![insert(2)]).await.is_err());

    // The durability of batch 2 is unknown, so nothing may be written after it.
    assert!(matches!(
        engine.apply_batch(&room, vec![insert(2)]).await,
        Err(StorageError::RoomFailed { .. })
    ));

    engine.close_room(&room).await.unwrap();
    let reopened = DiskStorageEngine::new(options);
    reopened.open_room(&room, schema()).await.unwrap();
    let head = reopened.get_head_seq(&room).await.unwrap().get();
    reopened
        .apply_batch(&room, vec![insert(head + 1)])
        .await
        .unwrap();
}
