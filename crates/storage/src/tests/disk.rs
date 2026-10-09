use crate::disk::compactor::compact_room_cow;
use crate::fail_point;
use crate::{
    DiskStorageEngine, DiskStorageOptions, MemoryStorageEngine, StorageEngine, StorageError,
};
use futures::FutureExt;
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

fn insert_key(seq: u64, key: i64) -> SequencedOperation {
    let row = CompactRow::new(vec![
        Value::Int(key),
        Value::String(format!("user {key}").into()),
    ]);
    SequencedOperation::with_default_origin(
        seq,
        Operation::insert(USERS, PrimaryKey::single(key), row, 100),
    )
}

fn insert(seq: u64) -> SequencedOperation {
    insert_key(seq, seq as i64)
}

async fn open_engine(dir: &std::path::Path, room: &RoomId) -> DiskStorageEngine {
    let engine = DiskStorageEngine::new(DiskStorageOptions::new(dir).auto_compact(false));
    engine.open_room(room, schema()).await.unwrap();
    engine
}

/// A snapshot whose rows have keys `first_key..first_key + count`, one batch per row, so that its
/// head sequence is `count`.
async fn snapshot_with_rows(room: &RoomId, first_key: i64, count: u64) -> Vec<u8> {
    let source = MemoryStorageEngine::new();
    source.open_room(room, schema()).await.unwrap();
    for seq in 1..=count {
        source
            .apply_batch(room, vec![insert_key(seq, first_key + seq as i64 - 1)])
            .await
            .unwrap();
    }
    source.create_snapshot(room).await.unwrap()
}

/// Names of the staged snapshot files left in `dir`.
fn staged_snapshot_files(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".snap.tmp."))
        .collect()
}

/// Applies batches until the room refuses one, and returns the last acknowledged sequence.
async fn write_until_refused(engine: &DiskStorageEngine, room: &RoomId, count: u64) -> u64 {
    let mut head = engine.get_head_seq(room).await.unwrap().get();
    for _ in 0..count {
        if engine
            .apply_batch(room, vec![insert(head + 1)])
            .await
            .is_err()
        {
            break;
        }
        head += 1;
    }
    head
}

async fn has_key(engine: &DiskStorageEngine, room: &RoomId, key: i64) -> bool {
    engine
        .get(room, "users", &PrimaryKey::single(key))
        .await
        .unwrap()
        .is_some()
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

#[tokio::test]
async fn reads_are_not_blocked_while_a_batch_syncs() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("read-during-sync").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    let mut pause = fail_point::arm_pause("apply_batch.sync", &engine.wal_file_path(&room));
    let writer = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.apply_batch(&room, vec![insert(2)]).await }
    });
    pause.reached().await;

    // The writer is inside its WAL sync. A read completes without waiting for it, and sees
    // the state before the batch: nothing that is not yet durable.
    let head = engine
        .get_head_seq(&room)
        .now_or_never()
        .expect("read blocked by a WAL sync")
        .unwrap();
    assert_eq!(head.get(), 1);
    let row = engine
        .get(&room, "users", &PrimaryKey::single(2i64))
        .now_or_never()
        .expect("read blocked by a WAL sync")
        .unwrap();
    assert!(row.is_none());

    pause.release();
    assert_eq!(writer.await.unwrap().unwrap().get(), 2);
    assert!(has_key(&engine, &room, 2).await);
}

#[tokio::test]
async fn concurrent_writers_apply_in_sequence_order() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("writer-order").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    let mut pause = fail_point::arm_pause("apply_batch.sync", &engine.wal_file_path(&room));
    let first = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.apply_batch(&room, vec![insert(2)]).await }
    });
    pause.reached().await;
    let second = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.apply_batch(&room, vec![insert(3)]).await }
    });
    // Let the second writer run until it waits for the first. Validated against a head that
    // the first writer has not applied yet, it would be rejected as out of sequence.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(!second.is_finished());

    pause.release();
    assert_eq!(first.await.unwrap().unwrap().get(), 2);
    assert_eq!(second.await.unwrap().unwrap().get(), 3);

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 3);
    for key in 1..=3 {
        assert!(has_key(&reopened, &room, key).await, "row {key} lost");
    }
}

#[tokio::test]
async fn wal_rotation_holds_the_wal_mutex() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("rotation-mutex").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    for seq in 1..=3 {
        engine.apply_batch(&room, vec![insert(seq)]).await.unwrap();
    }

    let mut pause = fail_point::arm_pause("compaction.rotate", &engine.wal_file_path(&room));
    let compaction = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.compact_room(&room).await }
    });
    pause.reached().await;

    // While the WAL moves, no batch may be between its write and its application in memory:
    // its record would land in the segment that a snapshot taken without it replaces.
    let room_handle = engine.get_room(&room).await.unwrap();
    assert!(room_handle.wal.try_lock().is_err());
    drop(room_handle);
    let writer = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.apply_batch(&room, vec![insert(4)]).await }
    });

    pause.release();
    compaction.await.unwrap().unwrap();
    assert_eq!(writer.await.unwrap().unwrap().get(), 4);

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 4);
    for key in 1..=4 {
        assert!(has_key(&reopened, &room, key).await, "row {key} lost");
    }
}

#[tokio::test]
async fn write_during_scan_shares_untouched_rows() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("shared-table").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let ops = (0..1000)
        .map(|key| insert_key(key as u64 + 1, key))
        .collect();
    engine.apply_batch(&room, ops).await.unwrap();

    // A scan or a compaction keeps its own reference to the table while writes continue.
    let room_handle = engine.get_room(&room).await.unwrap();
    let held = room_handle.state.read().await.tables[&USERS].clone();
    engine
        .apply_batch(&room, vec![insert_key(1001, 5000)])
        .await
        .unwrap();
    let current = room_handle.state.read().await.tables[&USERS].clone();

    // A row far from the written key is not copied: both versions share it.
    let key = PrimaryKey::single(0i64);
    assert!(std::ptr::eq(
        held.get(&key).unwrap(),
        current.get(&key).unwrap()
    ));
    assert!(held.get(&PrimaryKey::single(5000i64)).is_none());
}

#[tokio::test]
async fn crash_between_snapshot_rename_and_wal_truncation_recovers_the_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("snapshot-crash").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    for seq in 1..=3 {
        engine.apply_batch(&room, vec![insert(seq)]).await.unwrap();
    }

    let source = MemoryStorageEngine::new();
    source.open_room(&room, schema()).await.unwrap();
    for seq in 1..=5 {
        source
            .apply_batch(&room, vec![insert_key(seq, 100 + seq as i64)])
            .await
            .unwrap();
    }
    let snapshot = source.create_snapshot(&room).await.unwrap();

    // The new snapshot is in place, but the WAL still holds records 1..=3, all of them at or
    // below the snapshot's sequence.
    fail_point::arm("apply_snapshot.truncate", &engine.wal_file_path(&room));
    assert!(engine
        .apply_snapshot(&room, schema(), &snapshot)
        .await
        .is_err());
    engine.close_room(&room).await.unwrap();

    let reopened = open_engine(dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 5);
    assert!(!has_key(&reopened, &room, 1).await);
    assert!(has_key(&reopened, &room, 105).await);
    reopened.apply_batch(&room, vec![insert(6)]).await.unwrap();
}

#[tokio::test]
async fn write_abandoned_during_its_sync_marks_room_failed_until_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("abandoned-write").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    let mut pause = fail_point::arm_pause("apply_batch.sync", &engine.wal_file_path(&room));
    let writer = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.apply_batch(&room, vec![insert(2)]).await }
    });
    pause.reached().await;
    // The caller gives up on the write: batch 2 is in the WAL but not applied in memory, so a
    // new batch 2 would be accepted and written after it.
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    drop(pause);

    assert!(matches!(
        engine.apply_batch(&room, vec![insert(2)]).await,
        Err(StorageError::RoomFailed { .. })
    ));

    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    let head = reopened.get_head_seq(&room).await.unwrap().get();
    reopened
        .apply_batch(&room, vec![insert(head + 1)])
        .await
        .unwrap();
}

#[tokio::test]
async fn snapshot_waiting_across_a_reopen_never_runs_alongside_a_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("stale-compaction-lock").unwrap();
    let wal_path = DiskStorageEngine::new(DiskStorageOptions::new(dir.path())).wal_file_path(&room);
    let engine = open_engine(dir.path(), &room).await;
    for seq in 1..=3 {
        engine.apply_batch(&room, vec![insert(seq)]).await.unwrap();
    }
    let snapshot = snapshot_with_rows(&room, 100, 5).await;

    // The snapshot is about to wait for the room's compaction lock...
    let mut snapshot_pause = fail_point::arm_pause("apply_snapshot.lock", &wal_path);
    let apply = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.apply_snapshot(&room, schema(), &snapshot).await }
    });
    snapshot_pause.reached().await;

    // ...while the room is closed and reopened, and a compaction of the reopened room is
    // between its WAL rotation and its snapshot.
    engine.close_room(&room).await.unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    let mut compaction_pause = fail_point::arm_pause("compaction.staging", &wal_path);
    let compaction = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.compact_room(&room).await }
    });
    compaction_pause.reached().await;

    snapshot_pause.release();
    let _ = apply.await.unwrap();
    compaction_pause.release();
    compaction.await.unwrap().unwrap();
    let acknowledged = write_until_refused(&engine, &room, 1).await;
    engine.close_room(&room).await.unwrap();

    let reopened = open_engine(dir.path(), &room).await;
    assert_eq!(
        reopened.get_head_seq(&room).await.unwrap().get(),
        acknowledged
    );
}

/// Pauses `apply_snapshot` once its snapshot is staged, and checks that writers can still
/// take the WAL mutex meanwhile. Returns the paused snapshot task.
async fn stage_snapshot_and_pause(
    engine: &DiskStorageEngine,
    room: &RoomId,
    snapshot: Vec<u8>,
) -> (
    tokio::task::JoinHandle<Result<SequenceNumber, StorageError>>,
    fail_point::Pause,
) {
    let mut pause = fail_point::arm_pause("apply_snapshot.staged", &engine.wal_file_path(room));
    let apply = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.apply_snapshot(&room, schema(), &snapshot).await }
    });
    pause.reached().await;
    let room_handle = engine.get_room(room).await.unwrap();
    assert!(
        room_handle.wal.try_lock().is_ok(),
        "writers wait while a snapshot is staged"
    );
    (apply, pause)
}

#[tokio::test]
async fn snapshot_reached_by_writers_while_staged_is_discarded() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("staged-equal").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    for seq in 1..=3 {
        engine.apply_batch(&room, vec![insert(seq)]).await.unwrap();
    }
    let snapshot = snapshot_with_rows(&room, 100, 5).await;

    let (apply, pause) = stage_snapshot_and_pause(&engine, &room, snapshot).await;
    for seq in 4..=5 {
        engine.apply_batch(&room, vec![insert(seq)]).await.unwrap();
    }
    pause.release();

    assert_eq!(apply.await.unwrap().unwrap().get(), 5);
    assert!(has_key(&engine, &room, 5).await);
    assert!(!has_key(&engine, &room, 100).await);
    assert!(staged_snapshot_files(dir.path()).is_empty());
}

#[tokio::test]
async fn snapshot_overtaken_by_writers_while_staged_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("staged-behind").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    for seq in 1..=3 {
        engine.apply_batch(&room, vec![insert(seq)]).await.unwrap();
    }
    let snapshot = snapshot_with_rows(&room, 100, 5).await;

    let (apply, pause) = stage_snapshot_and_pause(&engine, &room, snapshot).await;
    for seq in 4..=6 {
        engine.apply_batch(&room, vec![insert(seq)]).await.unwrap();
    }
    pause.release();

    assert!(matches!(
        apply.await.unwrap(),
        Err(StorageError::SnapshotBehind { .. })
    ));
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 6);
    assert!(staged_snapshot_files(dir.path()).is_empty());
}

async fn failed_snapshot_install_step_leaves_room_usable(fail_point_name: &'static str) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("snapshot-step-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();
    let snapshot = snapshot_with_rows(&room, 100, 5).await;

    let path = if fail_point_name == "snapshot.stage_write" {
        engine.snap_file_path(&room)
    } else {
        engine.wal_file_path(&room)
    };
    fail_point::arm(fail_point_name, &path);
    assert!(engine
        .apply_snapshot(&room, schema(), &snapshot)
        .await
        .is_err());

    // Nothing on disk changed: the room keeps working and no staged file is left behind.
    assert!(staged_snapshot_files(dir.path()).is_empty());
    engine.apply_batch(&room, vec![insert(2)]).await.unwrap();
    engine.close_room(&room).await.unwrap();
    let reopened = open_engine(dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 2);
}

#[tokio::test]
async fn failed_snapshot_staging_leaves_room_usable() {
    failed_snapshot_install_step_leaves_room_usable("snapshot.stage_write").await;
}

#[tokio::test]
async fn failed_snapshot_rename_leaves_room_usable() {
    failed_snapshot_install_step_leaves_room_usable("apply_snapshot.rename").await;
}

#[tokio::test]
async fn panic_while_applying_a_batch_exposes_none_of_it() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("panicking-batch").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    // The panic comes after the batch's first operation is applied.
    fail_point::arm("apply_batch.apply", &engine.wal_file_path(&room));
    let writer = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.apply_batch(&room, vec![insert(2), insert(3)]).await }
    });
    assert!(writer.await.unwrap_err().is_panic());

    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 1);
    assert!(!has_key(&engine, &room, 2).await);
    assert!(matches!(
        engine.apply_batch(&room, vec![insert(2)]).await,
        Err(StorageError::RoomFailed { .. })
    ));
}

/// How a test goes on after aborting a compaction.
#[derive(Clone, Copy)]
enum AfterAbort {
    /// Write, then reopen.
    Write,
    /// Write, compact, write again, then reopen.
    WriteAndCompact,
}

/// Runs a compaction in its own task, aborts it at the pause point `pause_name`, goes on as
/// `after` says, and checks that every acknowledged write survives a reopen. With `orphan`, an
/// earlier failed compaction leaves `.wal.compacting`, so the WAL is absorbed (and then emptied)
/// instead of renamed. With `direct`, the compaction is called below `compact_room`.
async fn compaction_aborted_at(
    pause_name: &'static str,
    orphan: bool,
    after: AfterAbort,
    direct: bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("aborted-compaction").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    for seq in 1..=3 {
        engine.apply_batch(&room, vec![insert(seq)]).await.unwrap();
    }
    if orphan {
        fail_point::arm("compaction.phase2", &engine.snap_file_path(&room));
        assert!(engine.compact_room(&room).await.is_err());
        engine.apply_batch(&room, vec![insert(4)]).await.unwrap();
    }

    let mut pause = fail_point::arm_pause(pause_name, &engine.wal_file_path(&room));
    let compaction = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move {
            if direct {
                let room_handle = engine.get_room(&room).await.unwrap();
                compact_room_cow(room_handle, &engine.options).await
            } else {
                engine.compact_room(&room).await
            }
        }
    });
    pause.reached().await;
    compaction.abort();
    let _ = compaction.await;
    pause.release();

    let mut acknowledged = write_until_refused(&engine, &room, 2).await;
    if let AfterAbort::WriteAndCompact = after {
        let _ = engine.compact_room(&room).await;
        acknowledged = write_until_refused(&engine, &room, 1).await;
    }
    engine.close_room(&room).await.unwrap();

    let reopened = open_engine(dir.path(), &room).await;
    assert_eq!(
        reopened.get_head_seq(&room).await.unwrap().get(),
        acknowledged
    );
}

#[tokio::test]
async fn compaction_abandoned_after_renaming_the_wal_loses_no_acknowledged_write() {
    // Later writes would go through the old handle into `.wal.compacting`, which the next
    // compaction absorbs into itself and deletes.
    compaction_aborted_at(
        "compaction.rotate_renamed",
        false,
        AfterAbort::WriteAndCompact,
        true,
    )
    .await;
}

#[tokio::test]
async fn compaction_abandoned_while_emptying_the_wal_loses_no_acknowledged_write() {
    // The WAL is empty but its handle still points past the end: the next write would leave a
    // run of zeros in front of itself.
    compaction_aborted_at("wal.truncate", true, AfterAbort::Write, true).await;
}

#[tokio::test]
async fn compact_room_dropped_after_renaming_the_wal_loses_no_acknowledged_write() {
    compaction_aborted_at(
        "compaction.rotate_renamed",
        false,
        AfterAbort::WriteAndCompact,
        false,
    )
    .await;
}

#[tokio::test]
async fn compact_room_dropped_while_emptying_the_wal_loses_no_acknowledged_write() {
    compaction_aborted_at("wal.truncate", true, AfterAbort::Write, false).await;
}
