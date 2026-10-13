//! Helpers shared by the integration tests of the storage engines.

// Each file in `tests/` is a crate of its own and uses only part of these helpers.
#![allow(dead_code)]

use futures::StreamExt;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use zemdb_core::{
    ColumnUpdate, CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation,
    TableSchema, Value,
};
use zemdb_storage::format::encode_wal_batch;
use zemdb_storage::{
    DiskStorageEngine, DiskStorageOptions, ScanOptions, StorageEngine, StorageError,
};

pub const USERS_TABLE: u16 = 0;

/// `users`: an integer key, a name, a score and an active flag.
pub fn users_schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS_TABLE)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .column("score", DataType::Int)
        .column("active", DataType::Bool);
    Schema::builder().table(users).build()
}

/// `users` with a key and a name, and two nullable columns added later (score and note), so
/// that rows written before them hold two values.
pub fn evolved_users_schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS_TABLE)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String)
        .nullable_column("score", DataType::Int)
        .nullable_column("note", DataType::String);
    Schema::builder().table(users).build()
}

/// `op` at sequence `seq`.
pub fn sequenced(seq: u64, op: Operation) -> SequencedOperation {
    SequencedOperation::with_default_origin(seq, op)
}

pub fn row(id: i64, name: &str, score: i64, active: bool) -> CompactRow {
    CompactRow::new(vec![
        Value::Int(id),
        Value::String(name.into()),
        Value::Int(score),
        Value::Bool(active),
    ])
}

/// The usual row of the tests: "User {id}", score `id * 10`, active if `id` is even.
pub fn user_row(id: i64) -> CompactRow {
    row(id, &format!("User {id}"), id * 10, id % 2 == 0)
}

/// Inserts `row` into `users` under the key `id`.
pub fn insert(seq: u64, id: i64, row: CompactRow, timestamp: u64) -> SequencedOperation {
    sequenced(
        seq,
        Operation::insert(USERS_TABLE, PrimaryKey::single(id), row, timestamp),
    )
}

/// One insert of `user_row(id)` per id, at sequence `id`.
pub fn inserts(ids: RangeInclusive<i64>, timestamp: u64) -> Vec<SequencedOperation> {
    ids.map(|id| insert(id as u64, id, user_row(id), timestamp))
        .collect()
}

/// Sets column `column` of the `users` row `id` to `value`.
pub fn update(seq: u64, id: i64, column: u16, value: Value, timestamp: u64) -> SequencedOperation {
    let updates = vec![ColumnUpdate::new(column, value)];
    sequenced(
        seq,
        Operation::update(USERS_TABLE, PrimaryKey::single(id), updates, timestamp),
    )
}

pub fn delete(seq: u64, id: i64, timestamp: u64) -> SequencedOperation {
    sequenced(
        seq,
        Operation::delete(USERS_TABLE, PrimaryKey::single(id), timestamp),
    )
}

pub async fn get_user<E: StorageEngine>(engine: &E, room: &RoomId, id: i64) -> Option<CompactRow> {
    engine
        .get(room, "users", &PrimaryKey::single(id))
        .await
        .unwrap()
}

/// Checks that the head of `room` is `last` and that it holds `user_row(id)` for every id in
/// `1..=last`.
pub async fn assert_users_up_to<E: StorageEngine>(engine: &E, room: &RoomId, last: i64) {
    assert_eq!(engine.get_head_seq(room).await.unwrap().get(), last as u64);
    for id in 1..=last {
        assert_eq!(
            get_user(engine, room, id).await,
            Some(user_row(id)),
            "row {id}"
        );
    }
}

/// Every row of `users` that a scan with `options` returns.
pub async fn scan_rows<E: StorageEngine>(
    engine: &E,
    room: &RoomId,
    options: ScanOptions,
) -> Vec<(PrimaryKey, CompactRow)> {
    let stream = engine.scan(room, "users", options).await.unwrap();
    stream.map(Result::unwrap).collect().await
}

/// The keys of the rows that a scan of `users` with `options` returns.
pub async fn scan_keys<E: StorageEngine>(
    engine: &E,
    room: &RoomId,
    options: ScanOptions,
) -> Vec<PrimaryKey> {
    let rows = scan_rows(engine, room, options).await;
    rows.into_iter().map(|(pk, _)| pk).collect()
}

pub fn pks(ids: impl IntoIterator<Item = i64>) -> Vec<PrimaryKey> {
    ids.into_iter().map(PrimaryKey::single).collect()
}

/// A disk engine in `dir` that compacts only when asked to.
pub fn disk_engine(dir: &Path) -> DiskStorageEngine {
    DiskStorageEngine::new(DiskStorageOptions::new(dir).auto_compact(false))
}

/// Opens `room` with `users_schema` and applies each operation in a batch of its own.
pub async fn open_with_ops(
    options: &DiskStorageOptions,
    room: &RoomId,
    ops: Vec<SequencedOperation>,
) -> DiskStorageEngine {
    let engine = DiskStorageEngine::new(options.clone());
    engine.open_room(room, users_schema()).await.unwrap();
    for op in ops {
        engine.apply_batch(room, vec![op]).await.unwrap();
    }
    engine
}

/// Rewrites the file at `path` with `damage` applied to its bytes.
pub fn damage_file(path: &Path, damage: impl FnOnce(&mut Vec<u8>)) {
    let mut bytes = std::fs::read(path).unwrap();
    damage(&mut bytes);
    std::fs::write(path, bytes).unwrap();
}

/// The `.wal.compacting` file of `room`, where a compaction moves the WAL it folds.
pub fn compacting_wal_path(engine: &DiskStorageEngine, room: &RoomId) -> PathBuf {
    engine.wal_file_path(room).with_extension("wal.compacting")
}

/// Leaves `room` in `dir` closed, as a crash in the middle of a compaction would: its snapshot
/// holds the rows `snapshot`, `.wal.compacting` holds the rows `compacting` and the active WAL
/// holds the rows `wal`, each WAL as a single batch of `user_row` inserts at sequence `id`.
/// Returns the engine that wrote it, for its file paths.
pub async fn seed_interrupted_compaction(
    dir: &Path,
    room: &RoomId,
    snapshot: RangeInclusive<i64>,
    compacting: RangeInclusive<i64>,
    wal: RangeInclusive<i64>,
) -> DiskStorageEngine {
    let engine = DiskStorageEngine::new(DiskStorageOptions::new(dir));
    engine.open_room(room, users_schema()).await.unwrap();
    engine
        .apply_batch(room, inserts(snapshot, 100))
        .await
        .unwrap();
    engine.compact_room(room).await.unwrap();
    engine.close_room(room).await.unwrap();

    let compacting_bytes = encode_wal_batch(&inserts(compacting, 200), None).unwrap();
    std::fs::write(compacting_wal_path(&engine, room), compacting_bytes).unwrap();
    let wal_bytes = encode_wal_batch(&inserts(wal, 300), None).unwrap();
    std::fs::write(engine.wal_file_path(room), wal_bytes).unwrap();
    engine
}

/// Rejects a batch whose sequence does not follow the room's head.
pub async fn sequence_gap_is_rejected<E: StorageEngine>(engine: &E) {
    let room = RoomId::new("sequence-gap").unwrap();
    engine.open_room(&room, users_schema()).await.unwrap();

    let err = engine
        .apply_batch(&room, vec![insert(42, 1, user_row(1), 10)])
        .await
        .unwrap_err();

    assert!(
        matches!(
            err,
            StorageError::SequenceMismatch { expected, actual }
                if expected.get() == 1 && actual.get() == 42
        ),
        "{err:?}"
    );
}

/// Inserts a row with two values into `room`, opened with `evolved_users_schema`, and updates
/// its last column, which must grow the row to four values with `Null` in between. Returns the
/// row it ends up as.
pub async fn column_update_grows_row<E: StorageEngine>(engine: &E, room: &RoomId) -> CompactRow {
    engine
        .open_room(room, evolved_users_schema())
        .await
        .unwrap();
    let short_row = CompactRow::new(vec![Value::Int(1), Value::String("Initial".into())]);
    engine
        .apply_batch(room, vec![insert(1, 1, short_row, 100)])
        .await
        .unwrap();
    let note = Value::String("Note".into());
    engine
        .apply_batch(room, vec![update(2, 1, 3, note.clone(), 110)])
        .await
        .unwrap();

    let expected = CompactRow::new(vec![
        Value::Int(1),
        Value::String("Initial".into()),
        Value::Null,
        note,
    ]);
    assert_eq!(get_user(engine, room, 1).await, Some(expected.clone()));
    expected
}
