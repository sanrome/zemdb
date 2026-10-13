//! Helpers shared by the unit tests of the disk engine.

use crate::fail_point;
use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
use std::ops::RangeInclusive;
use std::path::Path;
use tokio::task::JoinHandle;
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value,
};

pub(crate) const USERS: u16 = 0;

pub(crate) fn schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String);
    Schema::builder().table(users).build()
}

/// `op` at sequence `seq`.
pub(crate) fn sequenced(seq: u64, op: Operation) -> SequencedOperation {
    SequencedOperation::new(seq, op)
}

pub(crate) fn insert_named(seq: u64, key: i64, name: &str) -> SequencedOperation {
    let row = CompactRow::new(vec![Value::Int(key), Value::String(name.into())]);
    sequenced(
        seq,
        Operation::insert(USERS, PrimaryKey::single(key), row, 100),
    )
}

/// Inserts the row "user {key}" under `key`.
pub(crate) fn insert_key(seq: u64, key: i64) -> SequencedOperation {
    insert_named(seq, key, &format!("user {key}"))
}

/// Inserts the row "user {seq}" under the key `seq`.
pub(crate) fn insert(seq: u64) -> SequencedOperation {
    insert_key(seq, seq as i64)
}

/// A disk engine in `dir` that compacts only when asked to.
pub(crate) fn engine_at(dir: &Path) -> DiskStorageEngine {
    DiskStorageEngine::new(DiskStorageOptions::new(dir).auto_compact(false))
}

/// A disk engine in `dir` with `room` open.
pub(crate) async fn open_engine(dir: &Path, room: &RoomId) -> DiskStorageEngine {
    let engine = engine_at(dir);
    engine.open_room(room, schema()).await.unwrap();
    engine
}

/// Closes `room` and opens it again in a new engine, as a restart would.
pub(crate) async fn reopen(
    engine: DiskStorageEngine,
    dir: &Path,
    room: &RoomId,
) -> DiskStorageEngine {
    engine.close_room(room).await.unwrap();
    open_engine(dir, room).await
}

/// Applies `insert(seq)` for every `seq`, one batch each.
pub(crate) async fn apply_range(
    engine: &DiskStorageEngine,
    room: &RoomId,
    seqs: RangeInclusive<u64>,
) {
    for seq in seqs {
        engine.apply_batch(room, vec![insert(seq)]).await.unwrap();
    }
}

pub(crate) async fn has_key(engine: &DiskStorageEngine, room: &RoomId, key: i64) -> bool {
    engine
        .get(room, "users", &PrimaryKey::single(key))
        .await
        .unwrap()
        .is_some()
}

/// Checks that the head of `room` is `last` and that every key in `1..=last` has its row.
pub(crate) async fn assert_rows(engine: &DiskStorageEngine, room: &RoomId, last: u64) {
    assert_eq!(engine.get_head_seq(room).await.unwrap().get(), last);
    for key in 1..=last as i64 {
        assert!(has_key(engine, room, key).await, "row {key} lost");
    }
}

/// Checks that `room` refuses the batch `insert(seq)` because it is marked as failed.
pub(crate) async fn assert_room_failed(engine: &DiskStorageEngine, room: &RoomId, seq: u64) {
    let result = engine.apply_batch(room, vec![insert(seq)]).await;
    assert!(
        matches!(result, Err(StorageError::RoomFailed { .. })),
        "{result:?}"
    );
}

/// Fails a compaction of `room` before its snapshot is staged, which leaves the rotated WAL
/// behind as `.wal.compacting`.
pub(crate) async fn fail_compaction_leaving_compacting_wal(
    engine: &DiskStorageEngine,
    room: &RoomId,
) {
    fail_point::arm("compaction.phase2", &engine.snap_file_path(room));
    assert!(engine.compact_room(room).await.is_err());
}

/// Names of the staged snapshot files left in `dir`.
pub(crate) fn staged_snapshot_files(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".snap.tmp."))
        .collect()
}

/// Applies `ops` to `room` in a task of its own.
pub(crate) fn spawn_batch(
    engine: &DiskStorageEngine,
    room: &RoomId,
    ops: Vec<SequencedOperation>,
) -> JoinHandle<Result<SequenceNumber, StorageError>> {
    let (engine, room) = (engine.clone(), room.clone());
    tokio::spawn(async move { engine.apply_batch(&room, ops).await })
}

/// Compacts `room` in a task of its own.
pub(crate) fn spawn_compaction(
    engine: &DiskStorageEngine,
    room: &RoomId,
) -> JoinHandle<Result<(), StorageError>> {
    let (engine, room) = (engine.clone(), room.clone());
    tokio::spawn(async move { engine.compact_room(&room).await })
}
