use crate::fail_point;
use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
use std::path::Path;
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value,
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

async fn open_engine(dir: &Path, room: &RoomId) -> DiskStorageEngine {
    let engine = DiskStorageEngine::new(DiskStorageOptions::new(dir).auto_compact(false));
    engine.open_room(room, schema()).await.unwrap();
    engine
}

async fn apply_range(
    engine: &DiskStorageEngine,
    room: &RoomId,
    seqs: std::ops::RangeInclusive<u64>,
) {
    for seq in seqs {
        engine.apply_batch(room, vec![insert(seq)]).await.unwrap();
    }
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

async fn snapshot_seq(engine: &DiskStorageEngine, room: &RoomId) -> SequenceNumber {
    engine
        .get_room(room)
        .await
        .unwrap()
        .read()
        .await
        .snapshot_seq
}

#[tokio::test]
async fn two_failed_compactions_in_a_row_lose_no_data() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("double-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let snap_path = engine.snap_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.phase2", &snap_path);
    assert!(engine.compact_room(&room).await.is_err());

    apply_range(&engine, &room, 6..=10).await;
    fail_point::arm("compaction.phase2", &snap_path);
    assert!(engine.compact_room(&room).await.is_err());

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
}

#[tokio::test]
async fn compaction_after_failed_wal_rotation_still_runs() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("stuck-flag").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.rotate_open", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());

    engine.compact_room(&room).await.unwrap();

    assert_eq!(snapshot_seq(&engine, &room).await.get(), 5);
}

#[tokio::test]
async fn writes_after_failed_wal_rotation_stay_durable() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("rotation-rollback").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.rotate_open", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());
    apply_range(&engine, &room, 6..=10).await;

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
}

#[tokio::test]
async fn compaction_absorbs_orphan_from_failed_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("absorb-orphan").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let snap_path = engine.snap_file_path(&room);
    let compacting_path = engine.wal_file_path(&room).with_extension("wal.compacting");

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.phase2", &snap_path);
    assert!(engine.compact_room(&room).await.is_err());
    assert!(compacting_path.exists());
    apply_range(&engine, &room, 6..=10).await;

    engine.compact_room(&room).await.unwrap();

    assert_eq!(snapshot_seq(&engine, &room).await.get(), 10);
    assert!(!compacting_path.exists());
    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
}

#[tokio::test]
async fn crash_between_absorbing_and_truncating_wal_recovers_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("absorb-crash").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let snap_path = engine.snap_file_path(&room);
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.phase2", &snap_path);
    assert!(engine.compact_room(&room).await.is_err());
    apply_range(&engine, &room, 6..=10).await;

    // The active WAL is copied into `.wal.compacting` but never truncated, so records
    // 6..=10 now exist in both files.
    fail_point::arm("compaction.absorb", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
    apply_range(&reopened, &room, 11..=11).await;
}

#[tokio::test]
async fn failed_absorb_leaves_no_partial_batch_in_compacting_wal() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("partial-absorb").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let snap_path = engine.snap_file_path(&room);
    let wal_path = engine.wal_file_path(&room);
    let compacting_path = wal_path.with_extension("wal.compacting");

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.phase2", &snap_path);
    assert!(engine.compact_room(&room).await.is_err());
    let compacting_len = std::fs::metadata(&compacting_path).unwrap().len();
    apply_range(&engine, &room, 6..=10).await;

    // The append to `.wal.compacting` stops halfway through, as with a full disk.
    fail_point::arm("compaction.absorb_write", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());
    assert_eq!(
        std::fs::metadata(&compacting_path).unwrap().len(),
        compacting_len
    );

    // A later compaction appends after it and also fails; recovery must still read everything.
    apply_range(&engine, &room, 11..=12).await;
    fail_point::arm("compaction.phase2", &snap_path);
    assert!(engine.compact_room(&room).await.is_err());

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 12).await;
}

#[tokio::test]
async fn failed_rotation_rollback_marks_room_failed_until_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("rollback-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.rotate_open", &wal_path);
    fail_point::arm("compaction.rotate_rollback", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());

    assert!(matches!(
        engine.apply_batch(&room, vec![insert(6)]).await,
        Err(StorageError::RoomFailed { .. })
    ));
    assert!(matches!(
        engine.compact_room(&room).await,
        Err(StorageError::RoomFailed { .. })
    ));

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 5).await;
    apply_range(&reopened, &room, 6..=6).await;
}
