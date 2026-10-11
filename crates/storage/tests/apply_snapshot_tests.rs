use bytes::Bytes;
use serde::Serialize;
use std::collections::BTreeMap;
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value,
};
use zemdb_storage::snapshot::encode_snapshot_envelope;
use zemdb_storage::{
    DiskStorageEngine, DiskStorageOptions, MemoryStorageEngine, StorageEngine, StorageError,
};

const USERS: u16 = 0;

fn schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String);
    Schema::builder().table(users).build()
}

fn user_row(key: i64) -> CompactRow {
    CompactRow::new(vec![
        Value::Int(key),
        Value::String(format!("user {key}").into()),
    ])
}

fn insert(seq: u64, key: i64) -> SequencedOperation {
    SequencedOperation::with_default_origin(
        seq,
        Operation::insert(USERS, PrimaryKey::single(key), user_row(key), 100),
    )
}

/// Builds a snapshot whose rows have the given keys, one batch per key, so that its head
/// sequence is the number of keys.
async fn snapshot_with_keys(keys: &[i64]) -> Bytes {
    let source = MemoryStorageEngine::new();
    let room = RoomId::new("snapshot-source").unwrap();
    source.open_room(&room, schema()).await.unwrap();
    for (seq, key) in (1u64..).zip(keys) {
        source
            .apply_batch(&room, vec![insert(seq, *key)])
            .await
            .unwrap();
    }
    source.create_snapshot(&room).await.unwrap().into()
}

/// The payload of a snapshot, laid out as the engines write it, with any rows: the engines
/// never write invalid ones.
#[derive(Serialize)]
struct Payload {
    head_seq: SequenceNumber,
    tables: BTreeMap<u16, BTreeMap<PrimaryKey, CompactRow>>,
}

/// A snapshot at `head_seq` holding exactly `tables`, valid or not.
fn crafted_snapshot(head_seq: u64, tables: Vec<(u16, Vec<(PrimaryKey, CompactRow)>)>) -> Bytes {
    let payload = Payload {
        head_seq: head_seq.into(),
        tables: tables
            .into_iter()
            .map(|(table_id, rows)| (table_id, rows.into_iter().collect()))
            .collect(),
    };
    let raw = bincode::serialize(&payload).unwrap();
    encode_snapshot_envelope(&raw, false, 0).unwrap().into()
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

    let result = engine
        .apply_snapshot(&room, schema(), snapshot, 2u64.into())
        .await;

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
        .apply_snapshot(&room, schema(), snapshot, 2u64.into())
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
        .apply_snapshot(&room, schema(), snapshot, 3u64.into())
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

    let result = engine
        .apply_snapshot(&room, schema(), snapshot, 1u64.into())
        .await;

    assert!(
        matches!(result, Err(StorageError::RoomNotFound(_))),
        "unexpected result: {result:?}"
    );
}

/// Applies snapshots ahead of the room that are invalid in one way each, and checks that every
/// one is rejected and leaves the room as it was. Then applies the same snapshot without the
/// fault, which succeeds: the others were rejected for their fault alone.
async fn invalid_snapshots_are_rejected<E: StorageEngine>(engine: &E, room: &RoomId) {
    open_with_keys(engine, room, &[1]).await;
    let valid_rows = || {
        vec![
            (PrimaryKey::single(10), user_row(10)),
            (PrimaryKey::single(11), user_row(11)),
        ]
    };
    let with_row = |pk: PrimaryKey, values: Vec<Value>| {
        let mut rows = valid_rows();
        rows.push((pk, CompactRow::new(values)));
        crafted_snapshot(3, vec![(USERS, rows)])
    };
    let name = || Value::String("user 12".into());
    let rejected = [
        (
            "a value of the wrong type",
            with_row(PrimaryKey::single(12), vec![Value::Int(12), Value::Int(12)]),
        ),
        (
            "a row with more values than columns",
            with_row(PrimaryKey::single(12), vec![Value::Int(12), name(), name()]),
        ),
        (
            "a key with more values than key columns",
            with_row(
                PrimaryKey::composite([12, 13]),
                vec![Value::Int(12), name()],
            ),
        ),
        (
            "a row whose key column differs from its key",
            with_row(PrimaryKey::single(12), vec![Value::Int(13), name()]),
        ),
        (
            "a table that is not in the schema",
            crafted_snapshot(
                3,
                vec![
                    (USERS, valid_rows()),
                    (7, vec![(PrimaryKey::single(12), user_row(12))]),
                ],
            ),
        ),
    ];

    for (fault, snapshot) in rejected {
        let result = engine
            .apply_snapshot(room, schema(), snapshot, 3u64.into())
            .await;
        assert!(
            matches!(result, Err(StorageError::SnapshotCorruption(_))),
            "{fault}: {result:?}"
        );
        assert_room_unchanged(engine, room).await;
    }

    let result = engine
        .apply_snapshot(
            room,
            schema(),
            crafted_snapshot(3, vec![(USERS, valid_rows())]),
            5u64.into(),
        )
        .await;
    assert!(
        matches!(
            result,
            Err(StorageError::SnapshotHeadMismatch { expected, actual })
                if expected.get() == 5 && actual.get() == 3
        ),
        "{result:?}"
    );
    assert_room_unchanged(engine, room).await;

    let head = engine
        .apply_snapshot(
            room,
            schema(),
            crafted_snapshot(3, vec![(USERS, valid_rows())]),
            3u64.into(),
        )
        .await
        .unwrap();
    assert_eq!(head.get(), 3);
    assert!(!has_key(engine, room, 1).await);
    assert!(has_key(engine, room, 11).await);
}

/// The room opened by `invalid_snapshots_are_rejected`, as it was before any snapshot.
async fn assert_room_unchanged<E: StorageEngine>(engine: &E, room: &RoomId) {
    assert_eq!(engine.get_head_seq(room).await.unwrap().get(), 1);
    assert!(has_key(engine, room, 1).await);
    assert!(!has_key(engine, room, 10).await);
}

/// A snapshot at head 1 whose payload declares `table_count` tables, the first one with id 0
/// and `row_count` rows, and holds nothing else.
fn snapshot_declaring(table_count: u64, row_count: u64) -> Bytes {
    let mut raw = Vec::new();
    raw.extend_from_slice(&1u64.to_le_bytes());
    raw.extend_from_slice(&table_count.to_le_bytes());
    raw.extend_from_slice(&USERS.to_le_bytes());
    raw.extend_from_slice(&row_count.to_le_bytes());
    encode_snapshot_envelope(&raw, false, 0).unwrap().into()
}

/// Applies snapshots that declare far more tables or rows than they hold. Reserving room for
/// them would abort the process; they must fail as corrupt and leave the room as it was.
async fn snapshots_declaring_huge_counts_are_rejected<E: StorageEngine>(engine: &E) {
    let room = RoomId::new("huge-counts").unwrap();
    open_with_keys(engine, &room, &[1]).await;

    for (tables, rows) in [(1, 1 << 40), (1, 1 << 58), (1 << 58, 1)] {
        let result = engine
            .apply_snapshot(
                &room,
                schema(),
                snapshot_declaring(tables, rows),
                1u64.into(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageError::SnapshotCorruption(_))),
            "{tables} tables, {rows} rows: {result:?}"
        );
        assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 1);
        assert!(has_key(engine, &room, 1).await);
    }
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
        .apply_snapshot(&room, schema(), snapshot, 2u64.into())
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
async fn memory_rejects_invalid_snapshots_and_keeps_the_room() {
    let room = RoomId::new("invalid-snapshots").unwrap();
    invalid_snapshots_are_rejected(&MemoryStorageEngine::new(), &room).await;
}

#[tokio::test]
async fn disk_rejects_invalid_snapshots_and_keeps_the_room() {
    let dir = tempfile::tempdir().unwrap();
    let engine = disk_engine(dir.path());
    let room = RoomId::new("invalid-snapshots").unwrap();
    invalid_snapshots_are_rejected(&engine, &room).await;

    // The applied snapshot is the one on disk.
    engine.close_room(&room).await.unwrap();
    let reopened = disk_engine(dir.path());
    reopened.open_room(&room, schema()).await.unwrap();
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 3);
    assert!(has_key(&reopened, &room, 10).await);
}

#[tokio::test]
async fn memory_rejects_snapshots_declaring_huge_counts() {
    snapshots_declaring_huge_counts_are_rejected(&MemoryStorageEngine::new()).await;
}

#[tokio::test]
async fn disk_rejects_snapshots_declaring_huge_counts() {
    let dir = tempfile::tempdir().unwrap();
    snapshots_declaring_huge_counts_are_rejected(&disk_engine(dir.path())).await;
}
