use super::{sync_file, SyncKind};
use crate::fail_point;
use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
use std::path::Path;
use tokio::io::AsyncWriteExt;
use zemdb_core::{
    ColumnUpdate, CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
    SequencedOperation, TableSchema, Value,
};

const USERS: u16 = 0;

fn schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String);
    Schema::builder().table(users).build()
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
        .state
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

#[tokio::test]
async fn failed_wal_sync_before_compaction_marks_room_failed_until_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("compaction-sync-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.sync", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());

    assert!(matches!(
        engine.apply_batch(&room, vec![insert(6)]).await,
        Err(StorageError::RoomFailed { .. })
    ));

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 5).await;
    apply_range(&reopened, &room, 6..=6).await;
}

#[tokio::test]
async fn failed_directory_sync_after_wal_rotation_marks_room_failed_until_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("rotation-dir-sync").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.rotate_sync_dir", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());

    // The new WAL's directory entry may not be durable: nothing may be acknowledged into it.
    assert!(matches!(
        engine.apply_batch(&room, vec![insert(6)]).await,
        Err(StorageError::RoomFailed { .. })
    ));

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 5).await;
    apply_range(&reopened, &room, 6..=6).await;
}

#[tokio::test]
async fn sync_file_reports_a_write_that_failed_in_the_background() {
    for kind in [SyncKind::Data, SyncKind::All] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("read-only");
        std::fs::write(&path, b"").unwrap();
        let mut file = tokio::fs::File::open(&path).await.unwrap();

        // `write_all` returns before the write runs; the write then fails on the blocking pool.
        // On Windows the sync of a read-only handle fails by itself, flush or not; the failed
        // write is only what reaches the sync on the other platforms.
        let result = async {
            file.write_all(b"hello").await?;
            sync_file(&mut file, kind).await
        }
        .await;

        assert!(
            result.is_err(),
            "{kind:?} sync reported a failed write as durable"
        );
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }
}

#[tokio::test]
async fn failed_background_append_to_compacting_wal_keeps_the_active_wal() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("absorb-background-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let snap_path = engine.snap_file_path(&room);
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.phase2", &snap_path);
    assert!(engine.compact_room(&room).await.is_err());
    apply_range(&engine, &room, 6..=10).await;
    let wal_len = std::fs::metadata(&wal_path).unwrap().len();

    // The last write of the append to `.wal.compacting` fails after `write_all` returned.
    fail_point::arm("compaction.absorb_append", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());

    // Records 6..=10 never reached `.wal.compacting`: emptying the active WAL would lose them.
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), wal_len);
    drop(engine);
    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
    apply_range(&reopened, &room, 11..=11).await;
}

/// Name of every row a room holds after `failed_phase3_step_loses_no_batch`, by key: batches
/// 1..=5 and 7..=10 insert their own key, batch 6 deletes row 2 and batch 11 renames row 3.
/// Replaying an earlier batch after them (out of order, or from a stale snapshot) would bring
/// back row 2 or the old name of row 3. Skipping records already applied is covered by
/// `replay_skips_records_already_applied`.
async fn assert_phase3_contents(engine: &DiskStorageEngine, room: &RoomId) {
    assert_eq!(engine.get_head_seq(room).await.unwrap().get(), 11);
    for key in 1..=11i64 {
        let expected = match key {
            2 | 6 | 11 => None,
            3 => Some("renamed".to_string()),
            _ => Some(format!("user {key}")),
        };
        let name = engine
            .get(room, "users", &PrimaryKey::single(key))
            .await
            .unwrap()
            .map(|row| match &row[1] {
                Value::String(name) => name.to_string(),
                other => panic!("unexpected name {other:?}"),
            });
        assert_eq!(name, expected, "row {key}");
    }
}

/// Fails the compaction step `fail_point_name` of phase 3, after the new snapshot is staged,
/// and checks that the room stays usable and that every batch, written before or after the
/// failure, survives a reopen, with later deletes and updates still in effect. With `reopen_first`, the room is reopened from the
/// files the failure left before compacting again; otherwise it is compacted again in place.
async fn failed_phase3_step_loses_no_batch(fail_point_name: &'static str, reopen_first: bool) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("phase3-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm(fail_point_name, &engine.wal_file_path(&room));
    assert!(engine.compact_room(&room).await.is_err());

    // No acknowledged batch depends on this step, so the room is not marked as failed.
    let delete = Operation::delete(USERS, PrimaryKey::single(2i64), 100);
    engine
        .apply_batch(
            &room,
            vec![SequencedOperation::with_default_origin(6, delete)],
        )
        .await
        .unwrap();
    apply_range(&engine, &room, 7..=10).await;
    let engine = if reopen_first {
        engine.close_room(&room).await.unwrap();
        let reopened = open_engine(dir.path(), &room).await;
        assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 10);
        assert!(reopened
            .get(&room, "users", &PrimaryKey::single(2i64))
            .await
            .unwrap()
            .is_none());
        reopened
    } else {
        engine
    };

    engine.compact_room(&room).await.unwrap();
    assert_eq!(snapshot_seq(&engine, &room).await.get(), 10);
    let rename = Operation::update(
        USERS,
        PrimaryKey::single(3i64),
        vec![ColumnUpdate::new(1, Value::String("renamed".into()))],
        100,
    );
    engine
        .apply_batch(
            &room,
            vec![SequencedOperation::with_default_origin(11, rename)],
        )
        .await
        .unwrap();
    assert_phase3_contents(&engine, &room).await;

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_phase3_contents(&reopened, &room).await;
}

#[tokio::test]
async fn failed_snapshot_rename_in_compaction_loses_no_batch() {
    failed_phase3_step_loses_no_batch("compaction.phase3_rename", true).await;
    failed_phase3_step_loses_no_batch("compaction.phase3_rename", false).await;
}

#[tokio::test]
async fn failed_directory_sync_after_snapshot_rename_loses_no_batch() {
    failed_phase3_step_loses_no_batch("compaction.phase3_sync", true).await;
    failed_phase3_step_loses_no_batch("compaction.phase3_sync", false).await;
}

#[tokio::test]
async fn failed_compacting_wal_cleanup_loses_no_batch() {
    failed_phase3_step_loses_no_batch("compaction.cleanup", true).await;
    failed_phase3_step_loses_no_batch("compaction.cleanup", false).await;
}
