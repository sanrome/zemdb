use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value,
};
use zemdb_storage::{
    DiskStorageEngine, DiskStorageOptions, MemoryStorageEngine, StorageEngine, StorageError,
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

/// Builds a snapshot whose rows have the given keys, one batch per key, so that its head
/// sequence is the number of keys.
async fn snapshot_with_keys(keys: &[i64]) -> Vec<u8> {
    let source = MemoryStorageEngine::new();
    let room = RoomId::new("snapshot-source").unwrap();
    source.open_room(&room, schema()).await.unwrap();
    for (seq, key) in (1u64..).zip(keys) {
        source
            .apply_batch(&room, vec![insert(seq, *key)])
            .await
            .unwrap();
    }
    source.create_snapshot(&room).await.unwrap()
}

/// Opens `room` and writes one batch per key.
async fn open_with_keys<E: StorageEngine>(engine: &E, room: &RoomId, keys: &[i64]) {
    engine.open_room(room, schema()).await.unwrap();
    for (seq, key) in (1u64..).zip(keys) {
        engine
            .apply_batch(room, vec![insert(seq, *key)])
            .await
            .unwrap();
    }
}

async fn has_key<E: StorageEngine>(engine: &E, room: &RoomId, key: i64) -> bool {
    engine
        .get(room, "users", &PrimaryKey::single(key))
        .await
        .unwrap()
        .is_some()
}

async fn snapshot_behind_is_rejected<E: StorageEngine>(engine: &E) {
    let room = RoomId::new("behind").unwrap();
    open_with_keys(engine, &room, &[1, 2, 3]).await;
    let snapshot = snapshot_with_keys(&[10, 11]).await;

    let result = engine.apply_snapshot(&room, schema(), &snapshot).await;

    assert!(
        matches!(
            result,
            Err(StorageError::SnapshotBehind { current, snapshot })
                if current.get() == 3 && snapshot.get() == 2
        ),
        "unexpected result: {result:?}"
    );
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 3);
    assert!(has_key(engine, &room, 3).await);
    assert!(!has_key(engine, &room, 10).await);
}

async fn snapshot_at_head_changes_nothing<E: StorageEngine>(engine: &E) {
    let room = RoomId::new("same-head").unwrap();
    open_with_keys(engine, &room, &[1, 2]).await;
    // Different rows at the same sequence number: a retry must not replace the room's state.
    let snapshot = snapshot_with_keys(&[10, 11]).await;

    let head = engine
        .apply_snapshot(&room, schema(), &snapshot)
        .await
        .unwrap();

    assert_eq!(head.get(), 2);
    assert!(has_key(engine, &room, 1).await);
    assert!(!has_key(engine, &room, 10).await);
}

async fn snapshot_ahead_replaces_contents<E: StorageEngine>(engine: &E) {
    let room = RoomId::new("ahead").unwrap();
    open_with_keys(engine, &room, &[1]).await;
    let snapshot = snapshot_with_keys(&[10, 11, 12]).await;

    let head = engine
        .apply_snapshot(&room, schema(), &snapshot)
        .await
        .unwrap();

    assert_eq!(head.get(), 3);
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 3);
    assert!(!has_key(engine, &room, 1).await);
    assert!(has_key(engine, &room, 12).await);
    engine
        .apply_batch(&room, vec![insert(4, 13)])
        .await
        .unwrap();
}

async fn snapshot_requires_open_room<E: StorageEngine>(engine: &E) {
    let room = RoomId::new("never-opened").unwrap();
    let snapshot = snapshot_with_keys(&[1]).await;

    let result = engine.apply_snapshot(&room, schema(), &snapshot).await;

    assert!(
        matches!(result, Err(StorageError::RoomNotFound(_))),
        "unexpected result: {result:?}"
    );
}

fn disk_engine(dir: &std::path::Path) -> DiskStorageEngine {
    DiskStorageEngine::new(DiskStorageOptions::new(dir).auto_compact(false))
}

#[tokio::test]
async fn memory_rejects_snapshot_behind_room_head() {
    snapshot_behind_is_rejected(&MemoryStorageEngine::new()).await;
}

#[tokio::test]
async fn disk_rejects_snapshot_behind_room_head() {
    let dir = tempfile::tempdir().unwrap();
    snapshot_behind_is_rejected(&disk_engine(dir.path())).await;
}

#[tokio::test]
async fn memory_snapshot_at_room_head_changes_nothing() {
    snapshot_at_head_changes_nothing(&MemoryStorageEngine::new()).await;
}

#[tokio::test]
async fn disk_snapshot_at_room_head_rewrites_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let engine = disk_engine(dir.path());
    let room = RoomId::new("same-head").unwrap();
    let snap_path = engine.snap_file_path(&room);
    let wal_path = engine.wal_file_path(&room);

    // Fails if the snapshot were applied: the room would hold keys 10 and 11 instead.
    snapshot_at_head_changes_nothing(&engine).await;

    let snap_before = std::fs::read(&snap_path).unwrap();
    let wal_len_before = std::fs::metadata(&wal_path).unwrap().len();
    let snapshot = snapshot_with_keys(&[20, 21]).await;
    engine
        .apply_snapshot(&room, schema(), &snapshot)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&snap_path).unwrap(), snap_before);
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), wal_len_before);
}

#[tokio::test]
async fn memory_snapshot_ahead_replaces_room_contents() {
    snapshot_ahead_replaces_contents(&MemoryStorageEngine::new()).await;
}

#[tokio::test]
async fn disk_snapshot_ahead_replaces_room_contents_durably() {
    let dir = tempfile::tempdir().unwrap();
    let engine = disk_engine(dir.path());
    snapshot_ahead_replaces_contents(&engine).await;

    let room = RoomId::new("ahead").unwrap();
    engine.close_room(&room).await.unwrap();
    let reopened = disk_engine(dir.path());
    reopened.open_room(&room, schema()).await.unwrap();
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 4);
    assert!(!has_key(&reopened, &room, 1).await);
    assert!(has_key(&reopened, &room, 10).await);
    assert!(has_key(&reopened, &room, 13).await);
}

#[tokio::test]
async fn memory_snapshot_requires_open_room() {
    snapshot_requires_open_room(&MemoryStorageEngine::new()).await;
}

#[tokio::test]
async fn disk_snapshot_requires_open_room() {
    let dir = tempfile::tempdir().unwrap();
    snapshot_requires_open_room(&disk_engine(dir.path())).await;
}

#[tokio::test]
async fn memory_snapshot_is_visible_through_room_handle_taken_before() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("held-handle").unwrap();
    open_with_keys(&engine, &room, &[1]).await;
    // A writer that looked up the room before the snapshot keeps this handle.
    let handle = engine.get_room(&room).unwrap();

    let snapshot = snapshot_with_keys(&[10, 11, 12]).await;
    engine
        .apply_snapshot(&room, schema(), &snapshot)
        .await
        .unwrap();

    let state = handle.read().unwrap();
    assert_eq!(state.head_seq, SequenceNumber::from(3u64));
    assert!(state.tables[&USERS]
        .get(&PrimaryKey::single(12i64))
        .is_some());
}
