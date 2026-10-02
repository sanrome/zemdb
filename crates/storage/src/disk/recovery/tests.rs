use crate::disk::fail;
use crate::disk::format::encode_wal_batch;
use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
use std::io::Write;
use std::path::Path;
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

fn insert_named(seq: u64, key: i64, name: &str) -> SequencedOperation {
    let row = CompactRow::new(vec![Value::Int(key), Value::String(name.into())]);
    SequencedOperation::with_default_origin(
        seq,
        Operation::insert(USERS, PrimaryKey::single(key), row, 100),
    )
}

fn insert(seq: u64) -> SequencedOperation {
    insert_named(seq, seq as i64, &format!("user {seq}"))
}

fn engine_at(dir: &Path) -> DiskStorageEngine {
    DiskStorageEngine::new(DiskStorageOptions::new(dir).auto_compact(false))
}

async fn assert_rows(engine: &DiskStorageEngine, room: &RoomId, last: u64) {
    assert_eq!(engine.get_head_seq(room).await.unwrap().get(), last);
    for seq in 1..=last {
        let row = engine
            .get(room, "users", &PrimaryKey::single(seq as i64))
            .await
            .unwrap();
        assert!(row.is_some(), "row {seq} lost");
    }
}

async fn name_of(engine: &DiskStorageEngine, room: &RoomId, key: i64) -> Value {
    let row = engine
        .get(room, "users", &PrimaryKey::single(key))
        .await
        .unwrap()
        .unwrap();
    row[1].clone()
}

/// Leaves the room as a crash after a failed compaction would: snapshot without the latest
/// writes, an orphan `.wal.compacting` holding 1..=5 and the active `.wal` holding 6..=8.
async fn leave_orphan_compacting(dir: &Path, room: &RoomId) {
    let engine = engine_at(dir);
    engine.open_room(room, schema()).await.unwrap();
    for seq in 1..=5 {
        engine.apply_batch(room, vec![insert(seq)]).await.unwrap();
    }
    fail::arm("compaction.phase2", &engine.snap_file_path(room));
    assert!(engine.compact_room(room).await.is_err());
    for seq in 6..=8 {
        engine.apply_batch(room, vec![insert(seq)]).await.unwrap();
    }
    // Dropped without close_room, as in a crash.
}

fn append_batch(path: &Path, ops: &[SequencedOperation]) {
    let bytes = encode_wal_batch(ops, None).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    file.write_all(&bytes).unwrap();
}

#[tokio::test]
async fn crash_while_folding_compacting_wal_recovers_all_data() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("fold-crash");
    leave_orphan_compacting(dir.path(), &room).await;

    let engine = engine_at(dir.path());
    fail::arm("recovery.fold", &engine.wal_file_path(&room));
    assert!(engine.open_room(&room, schema()).await.is_err());

    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 8).await;
}

#[tokio::test]
async fn crash_before_compacting_wal_removal_recovers_all_data() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("cleanup-crash");
    leave_orphan_compacting(dir.path(), &room).await;

    let engine = engine_at(dir.path());
    fail::arm("recovery.fold_cleanup", &engine.wal_file_path(&room));
    assert!(engine.open_room(&room, schema()).await.is_err());

    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 8).await;
}

#[tokio::test]
async fn recovered_compacting_wal_is_folded_and_removed() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("fold-ok");
    leave_orphan_compacting(dir.path(), &room).await;

    let engine = engine_at(dir.path());
    engine.open_room(&room, schema()).await.unwrap();

    let compacting = engine.wal_file_path(&room).with_extension("wal.compacting");
    assert!(!compacting.exists());
    let state = engine.get_room(&room).await.unwrap();
    assert_eq!(state.read().await.snapshot_seq.get(), 8);
    drop(state);
    engine.close_room(&room).await.unwrap();

    let reopened = engine_at(dir.path());
    reopened.open_room(&room, schema()).await.unwrap();
    assert_rows(&reopened, &room, 8).await;
}

#[tokio::test]
async fn replay_skips_records_already_applied() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("duplicate-replay");
    let engine = engine_at(dir.path());
    let wal_path = engine.wal_file_path(&room);

    append_batch(
        &wal_path,
        &[
            insert_named(1, 1, "first"),
            insert_named(2, 2, "first"),
            insert_named(3, 1, "second"),
        ],
    );
    // A stale copy of operation 1 appended again, as a crash between copying and truncating
    // a WAL can leave behind. Re-applying it would revert key 1 to its first value.
    append_batch(&wal_path, &[insert_named(1, 1, "first")]);

    engine.open_room(&room, schema()).await.unwrap();

    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 3);
    assert_eq!(
        name_of(&engine, &room, 1).await,
        Value::String("second".into())
    );
}

#[tokio::test]
async fn replay_rejects_sequence_gap() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("gap-replay");
    let engine = engine_at(dir.path());
    let wal_path = engine.wal_file_path(&room);

    append_batch(&wal_path, &[insert(1), insert(2), insert(3)]);
    append_batch(&wal_path, &[insert(5)]);

    let result = engine.open_room(&room, schema()).await;

    assert!(matches!(result, Err(StorageError::WalCorruption(_))));
}
