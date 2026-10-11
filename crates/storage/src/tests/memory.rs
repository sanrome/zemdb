use crate::fail_point;
use crate::{MemoryStorageEngine, StorageEngine};
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation, TableSchema,
    Value,
};

const USERS: u16 = 0;

fn schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String);
    Schema::builder().table(users).build()
}

fn insert(seq: u64, key: i64) -> SequencedOperation {
    let row = CompactRow::new(vec![
        Value::Int(key),
        Value::String(format!("user {key}").into()),
    ]);
    SequencedOperation::with_default_origin(
        seq,
        Operation::insert(USERS, PrimaryKey::single(key), row, 100),
    )
}

#[tokio::test]
async fn write_during_scan_shares_untouched_rows() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("shared-table").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    let ops = (0..1000).map(|key| insert(key as u64 + 1, key)).collect();
    engine.apply_batch(&room, ops).await.unwrap();

    // A scan keeps its own reference to the table while writes continue.
    let room_handle = engine.get_room(&room).unwrap();
    let held = room_handle.read().unwrap().tables[&USERS].clone();
    engine
        .apply_batch(&room, vec![insert(1001, 5000)])
        .await
        .unwrap();
    let current = room_handle.read().unwrap().tables[&USERS].clone();

    // A row far from the written key is not copied: both versions share it.
    let key = PrimaryKey::single(0i64);
    assert!(std::ptr::eq(
        held.get(&key).unwrap(),
        current.get(&key).unwrap()
    ));
    assert!(held.get(&PrimaryKey::single(5000i64)).is_none());
}

#[tokio::test]
async fn snapshot_is_visible_through_room_handle_taken_before() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("held-handle").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    engine.apply_batch(&room, vec![insert(1, 1)]).await.unwrap();
    // A writer that looked up the room before the snapshot keeps this handle.
    let handle = engine.get_room(&room).unwrap();

    let source = MemoryStorageEngine::new();
    let source_room = RoomId::new("source").unwrap();
    source.open_room(&source_room, schema()).await.unwrap();
    let ops = [10, 11, 12]
        .iter()
        .enumerate()
        .map(|(i, key)| insert(i as u64 + 1, *key))
        .collect();
    source.apply_batch(&source_room, ops).await.unwrap();
    let snapshot = source.create_snapshot(&source_room).await.unwrap();
    engine
        .apply_snapshot(&room, schema(), snapshot.into(), 3u64.into())
        .await
        .unwrap();

    let state = handle.read().unwrap();
    assert_eq!(state.head_seq, zemdb_core::SequenceNumber::from(3u64));
    assert!(state.tables[&USERS]
        .get(&PrimaryKey::single(12i64))
        .is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_is_decoded_without_blocking_the_runtime() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("decoded-off-the-runtime").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    let source = MemoryStorageEngine::new();
    source.open_room(&room, schema()).await.unwrap();
    let ops = (1..=3).map(|seq| insert(seq, seq as i64)).collect();
    source.apply_batch(&room, ops).await.unwrap();
    let snapshot = source.create_snapshot(&room).await.unwrap();

    let mut pause = fail_point::arm_blocking_pause(
        "apply_snapshot.decode",
        std::path::Path::new("decoded-off-the-runtime"),
    );
    // The runtime has a single thread: this task only runs if decoding leaves it free.
    let releaser = tokio::spawn(async move {
        pause.reached().await;
        pause.release();
    });
    let head = engine
        .apply_snapshot(&room, schema(), snapshot.into(), 3u64.into())
        .await
        .unwrap();
    assert_eq!(head.get(), 3);
    tokio::time::timeout(std::time::Duration::from_secs(10), releaser)
        .await
        .expect("decoding never reached its pause point")
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_goes_to_the_room_open_when_decoding_ends() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("reopened-while-decoding").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    let source = MemoryStorageEngine::new();
    source.open_room(&room, schema()).await.unwrap();
    let ops = (1..=3).map(|seq| insert(seq, seq as i64)).collect();
    source.apply_batch(&room, ops).await.unwrap();
    let snapshot = source.create_snapshot(&room).await.unwrap();

    let mut pause = fail_point::arm_blocking_pause(
        "apply_snapshot.decode",
        std::path::Path::new("reopened-while-decoding"),
    );
    let apply = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move {
            engine
                .apply_snapshot(&room, schema(), snapshot.into(), 3u64.into())
                .await
        }
    });
    // The room is closed and reopened, empty, while the snapshot is decoded.
    pause.reached().await;
    engine.close_room(&room).await.unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    pause.release();

    assert_eq!(apply.await.unwrap().unwrap().get(), 3);
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 3);
}
