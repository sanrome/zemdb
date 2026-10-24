use crate::disk::compactor::{compact_room_cow, compacting_wal_path};
use crate::engine::RejectReason;
use crate::fail_point;
use crate::memory::index::tests::{
    apply_checked, apply_random_batches, assert_equivalent_contents, assert_same_contents,
    checked_contents, child_with_alt, constrained_schema, evolved_schema, insert_parent, oracle,
    owned_schema, refused_batches_change_nothing, reloaded_schema,
    reloads_at_random_points_decide_as_the_new_schema, reloads_race_batches_and_readers,
    reloads_that_do_not_evolve_change_nothing, reloads_that_evolve_decide_as_the_new_schema,
    snapshot_indexes_are_built_off_the_runtime, snapshots_replace_the_indexes,
    snapshots_that_break_a_constraint_change_nothing,
    snapshots_that_meet_the_constraints_are_installed, tagged_parent, RandomLog, Rng, RoomContents,
    BUILD_POINT, CHILDREN, PARENTS,
};
use crate::memory::state::RoomData;
use crate::memory::{RoomSnapshotRef, Table};
use crate::snapshot::encode_snapshot_envelope;
use crate::{DiskStorageEngine, MemoryStorageEngine, StorageEngine, StorageError};
use futures::FutureExt;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::path::Path;
use std::time::Duration;
use support::*;
use zemdb_core::{ColumnUpdate, Operation, PrimaryKey, RoomId, Schema, SequenceNumber, Value};

/// Helpers shared with the tests of the disk engine's submodules.
#[path = "../disk/tests/support.rs"]
pub(super) mod support;

/// Randomized simulations of the disk engine against the memory engine.
#[path = "../disk/tests/simulation.rs"]
mod simulation;

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

/// The data directory, and its entry in the parent, are synced on every open: an earlier open
/// may have created them and failed to sync.
#[tokio::test]
async fn open_room_syncs_the_data_directory_and_its_parent() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let engine = engine_at(&data);
    let room = RoomId::new("new-dir").unwrap();
    for synced in [dir.path(), data.as_path(), dir.path()] {
        fail_point::arm("sync_dir", synced);
        assert!(engine.open_room(&room, schema()).await.is_err());
    }
}

#[tokio::test]
async fn failed_wal_sync_marks_room_failed_until_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("sync-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    fail_point::arm("apply_batch.sync", &engine.wal_file_path(&room));
    assert!(engine.apply_batch(&room, vec![insert(2)]).await.is_err());

    // The durability of batch 2 is unknown, so nothing may be written after it.
    assert_room_failed(&engine, &room, 2).await;

    // Batch 2 may be only in the page cache: the room opens once this process synced it.
    engine.close_room(&room).await.unwrap();
    let reopened = engine_at(dir.path());
    fail_point::arm("recovery.wal_sync", &reopened.wal_file_path(&room));
    assert!(reopened.open_room(&room, schema()).await.is_err());
    reopened.open_room(&room, schema()).await.unwrap();
    let head = reopened.get_head_seq(&room).await.unwrap().get();
    reopened
        .apply_batch(&room, vec![insert(head + 1)])
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_wal_write_marks_room_failed_until_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("background-write-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    // The WAL write fails.
    fail_point::arm("apply_batch.write", &engine.wal_file_path(&room));
    assert!(engine.apply_batch(&room, vec![insert(2)]).await.is_err());

    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 1);
    assert!(!has_key(&engine, &room, 2).await);
    assert_room_failed(&engine, &room, 2).await;

    drop(engine);
    let reopened = open_engine(dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 1);
    assert!(!has_key(&reopened, &room, 2).await);
    reopened.apply_batch(&room, vec![insert(2)]).await.unwrap();
}

#[tokio::test]
async fn reads_are_not_blocked_while_a_batch_syncs() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("read-during-sync").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    let mut pause = fail_point::arm_pause("apply_batch.sync", &engine.wal_file_path(&room));
    let writer = spawn_batch(&engine, &room, vec![insert(2)]);
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
    assert_eq!(writer.await.unwrap().unwrap().head_seq().get(), 2);
    assert!(has_key(&engine, &room, 2).await);
}

#[tokio::test]
async fn concurrent_writers_apply_in_sequence_order() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("writer-order").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    let mut pause = fail_point::arm_pause("apply_batch.sync", &engine.wal_file_path(&room));
    let first = spawn_batch(&engine, &room, vec![insert(2)]);
    pause.reached().await;
    let second = spawn_batch(&engine, &room, vec![insert(3)]);
    // Let the second writer run until it waits for the first. Validated against a head that
    // the first writer has not applied yet, it would be rejected as out of sequence.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(!second.is_finished());

    pause.release();
    assert_eq!(first.await.unwrap().unwrap().head_seq().get(), 2);
    assert_eq!(second.await.unwrap().unwrap().head_seq().get(), 3);

    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 3).await;
}

#[tokio::test]
async fn wal_rotation_holds_the_wal_mutex() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("rotation-mutex").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=3).await;

    let mut pause = fail_point::arm_pause("compaction.rotate", &engine.wal_file_path(&room));
    let compaction = spawn_compaction(&engine, &room);
    pause.reached().await;

    // While the WAL moves, no batch may be between its write and its application in memory:
    // its record would land in the segment that a snapshot taken without it replaces.
    let room_handle = engine.get_room(&room).await.unwrap();
    assert!(room_handle.wal.try_lock().is_err());
    drop(room_handle);
    let writer = spawn_batch(&engine, &room, vec![insert(4)]);

    pause.release();
    compaction.await.unwrap().unwrap();
    assert_eq!(writer.await.unwrap().unwrap().head_seq().get(), 4);

    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 4).await;
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
    let held = room_handle.state.read().await.data.tables()[&USERS].clone();
    engine
        .apply_batch(&room, vec![insert_key(1001, 5000)])
        .await
        .unwrap();
    let current = room_handle.state.read().await.data.tables()[&USERS].clone();

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
    apply_range(&engine, &room, 1..=3).await;

    let snapshot = snapshot_with_rows(&room, 101, 5).await;

    // The new snapshot is in place, but the WAL still holds records 1..=3, all of them at or
    // below the snapshot's sequence.
    fail_point::arm("apply_snapshot.truncate", &engine.wal_file_path(&room));
    assert!(engine
        .apply_snapshot(&room, schema(), snapshot.into(), SequenceNumber::from(5u64))
        .await
        .is_err());
    let reopened = reopen(engine, dir.path(), &room).await;
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
    let writer = spawn_batch(&engine, &room, vec![insert(2)]);
    pause.reached().await;
    // The caller gives up on the write: batch 2 is in the WAL but not applied in memory, so a
    // new batch 2 would be accepted and written after it.
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    drop(pause);

    assert_room_failed(&engine, &room, 2).await;

    let reopened = reopen(engine, dir.path(), &room).await;
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
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);
    apply_range(&engine, &room, 1..=3).await;
    let snapshot = snapshot_with_rows(&room, 100, 5).await;

    // The snapshot is about to wait for the room's compaction lock...
    let mut snapshot_pause = fail_point::arm_pause("apply_snapshot.lock", &wal_path);
    let apply = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move {
            engine
                .apply_snapshot(&room, schema(), snapshot.into(), SequenceNumber::from(5u64))
                .await
        }
    });
    snapshot_pause.reached().await;

    // ...while the room is closed and reopened, and a compaction of the reopened room is
    // between its WAL rotation and its snapshot.
    engine.close_room(&room).await.unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    let mut compaction_pause = fail_point::arm_pause("compaction.staging", &wal_path);
    let compaction = spawn_compaction(&engine, &room);
    compaction_pause.reached().await;

    snapshot_pause.release();
    let _ = apply.await.unwrap();
    compaction_pause.release();
    compaction.await.unwrap().unwrap();
    let acknowledged = write_until_refused(&engine, &room, 1).await;
    let reopened = reopen(engine, dir.path(), &room).await;
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
    snapshot_head: u64,
) -> (
    tokio::task::JoinHandle<Result<SequenceNumber, StorageError>>,
    fail_point::Pause,
) {
    let mut pause = fail_point::arm_pause("apply_snapshot.staged", &engine.wal_file_path(room));
    let apply = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move {
            let head = SequenceNumber::from(snapshot_head);
            engine
                .apply_snapshot(&room, schema(), snapshot.into(), head)
                .await
        }
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
    apply_range(&engine, &room, 1..=3).await;
    let snapshot = snapshot_with_rows(&room, 100, 5).await;

    let (apply, pause) = stage_snapshot_and_pause(&engine, &room, snapshot, 5).await;
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
    apply_range(&engine, &room, 1..=3).await;
    let snapshot = snapshot_with_rows(&room, 100, 5).await;

    let (apply, pause) = stage_snapshot_and_pause(&engine, &room, snapshot, 5).await;
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
        .apply_snapshot(&room, schema(), snapshot.into(), SequenceNumber::from(5u64))
        .await
        .is_err());

    // Nothing on disk changed: the room keeps working and no staged file is left behind.
    assert!(staged_snapshot_files(dir.path()).is_empty());
    engine.apply_batch(&room, vec![insert(2)]).await.unwrap();
    let reopened = reopen(engine, dir.path(), &room).await;
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

/// An `apply_snapshot` whose caller gives up while its rename is in flight completes before the
/// room can be reopened. A reopen that loaded the previous snapshot first would have the rename
/// land under it, and lose the batches it acknowledges at the next open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_dropped_during_its_rename_is_installed_before_the_room_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("late-rename").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);
    apply_range(&engine, &room, 1..=3).await;
    let snapshot = snapshot_with_rows(&room, 101, 5).await;

    let mut rename = fail_point::arm_blocking_pause("apply_snapshot.rename", &wal_path);
    tokio::select! {
        _ = engine.apply_snapshot(&room, schema(), snapshot.into(), SequenceNumber::from(5u64)) => {
            panic!("apply_snapshot finished during its rename")
        }
        _ = rename.reached() => {}
    }

    // The room is closed and reopened. If the reopen loads a snapshot before the rename lands,
    // the rename lands right then.
    let mut loaded = fail_point::arm_pause("recovery.snapshot_loaded", &wal_path);
    let reopening = tokio::spawn({
        let (dir, room) = (dir.path().to_path_buf(), room.clone());
        async move { reopen(engine, &dir, &room).await }
    });
    let loaded_first = tokio::time::timeout(Duration::from_millis(200), loaded.reached())
        .await
        .is_ok();
    rename.release();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !staged_snapshot_files(dir.path()).is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the rename never landed");
    if !loaded_first {
        tokio::time::timeout(Duration::from_secs(10), loaded.reached())
            .await
            .expect("the reopen never loaded the snapshot");
    }
    loaded.release();
    let reopened = reopening.await.unwrap();
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 5);

    apply_range(&reopened, &room, 6..=7).await;
    let reopened = reopen(reopened, dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 7);
    for key in (101..=105).chain(6..=7) {
        assert!(has_key(&reopened, &room, key).await, "row {key} lost");
    }
}

/// An `open_room` whose caller gives up while its recovery removes a folded `.wal.compacting`
/// keeps the room until the removal lands. Another open that took the room first could rotate
/// batches it acknowledges into a new `.wal.compacting`, and the late removal would delete it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_dropped_during_its_recovery_keeps_the_room_until_it_completes() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("late-removal").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let compacting_path = compacting_wal_path(&engine.wal_file_path(&room));
    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=8).await;
    engine.close_room(&room).await.unwrap();

    let mut removal = fail_point::arm_blocking_pause("remove", &compacting_path);
    let first = engine_at(dir.path());
    tokio::select! {
        _ = first.open_room(&room, schema()) => panic!("open_room finished during its recovery"),
        _ = removal.reached() => {}
    }
    let second = engine_at(dir.path());
    let early = second.open_room(&room, schema()).await;
    assert!(
        matches!(early, Err(StorageError::RoomLocked(_))),
        "{early:?}"
    );
    removal.release();

    // The dropped open completes; once its room is closed, the other open takes it.
    first.close_room(&room).await.unwrap();
    second.open_room(&room, schema()).await.unwrap();
    apply_range(&second, &room, 9..=10).await;
    fail_compaction_leaving_compacting_wal(&second, &room).await;
    let reopened = reopen(second, dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
}

/// An `apply_snapshot` dropped by its caller while its task is staged (before it takes the WAL
/// mutex) still completes: `close_room` waits for it, another engine cannot open the room
/// meanwhile, and the reopened room holds the snapshot and keeps the writes made after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_waits_for_the_task_of_a_dropped_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("close-during-snapshot").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);
    apply_range(&engine, &room, 1..=3).await;
    let snapshot = snapshot_with_rows(&room, 101, 5).await;

    let mut staged = fail_point::arm_pause("apply_snapshot.staged", &wal_path);
    tokio::select! {
        _ = engine.apply_snapshot(&room, schema(), snapshot.into(), SequenceNumber::from(5u64)) => {
            panic!("apply_snapshot finished while staged")
        }
        _ = staged.reached() => {}
    }
    // The caller is gone; the task holds the compaction lock.
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 3);
    let closing = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.close_room(&room).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !closing.is_finished(),
        "close did not wait for the snapshot"
    );
    let other = engine_at(dir.path());
    let early = other.open_room(&room, schema()).await;
    assert!(
        matches!(early, Err(StorageError::RoomLocked(_))),
        "{early:?}"
    );

    staged.release();
    tokio::time::timeout(Duration::from_secs(10), closing)
        .await
        .expect("close never finished")
        .unwrap()
        .unwrap();
    other.open_room(&room, schema()).await.unwrap();
    assert_eq!(other.get_head_seq(&room).await.unwrap().get(), 5);
    apply_range(&other, &room, 6..=7).await;
    let reopened = reopen(other, dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 7);
    for key in (101..=105).chain(6..=7) {
        assert!(has_key(&reopened, &room, key).await, "row {key} lost");
    }
}

/// An engine dropped (without closing the room) while the task of a dropped `apply_snapshot`
/// waits to install it keeps the room locked until the task is done: another engine opens the
/// room only afterwards, with the snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_dropped_during_a_snapshot_task_keeps_the_room_until_it_completes() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("drop-during-snapshot").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);
    apply_range(&engine, &room, 1..=3).await;
    let snapshot = snapshot_with_rows(&room, 101, 5).await;

    let mut rename = fail_point::arm_blocking_pause("apply_snapshot.rename", &wal_path);
    tokio::select! {
        _ = engine.apply_snapshot(&room, schema(), snapshot.into(), SequenceNumber::from(5u64)) => {
            panic!("apply_snapshot finished during its rename")
        }
        _ = rename.reached() => {}
    }
    drop(engine);
    let other = engine_at(dir.path());
    let early = other.open_room(&room, schema()).await;
    assert!(
        matches!(early, Err(StorageError::RoomLocked(_))),
        "{early:?}"
    );

    rename.release();
    open_when_unlocked(&other, &room, schema()).await.unwrap();
    assert_eq!(other.get_head_seq(&room).await.unwrap().get(), 5);
    apply_range(&other, &room, 6..=7).await;
    let reopened = reopen(other, dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 7);
}

/// Two opens of the same room at once: in one engine, one opens it and the other gets
/// `RoomAlreadyOpen`; in two engines, one gets `RoomLocked`. A dropped open that completes in
/// its task leaves the room registered, and it can be closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_and_dropped_opens_of_a_room_open_it_once() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("concurrent-open").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=8).await;
    engine.close_room(&room).await.unwrap();

    let (first, second) = tokio::join!(
        engine.open_room(&room, schema()),
        engine.open_room(&room, schema())
    );
    assert!(
        matches!(
            (&first, &second),
            (Ok(()), Err(StorageError::RoomAlreadyOpen(_)))
                | (Err(StorageError::RoomAlreadyOpen(_)), Ok(()))
        ),
        "{first:?} {second:?}"
    );
    assert_rows(&engine, &room, 8).await;
    engine.close_room(&room).await.unwrap();

    let (one, two) = (engine_at(dir.path()), engine_at(dir.path()));
    let (first, second) = tokio::join!(
        one.open_room(&room, schema()),
        two.open_room(&room, schema())
    );
    assert!(
        matches!(
            (&first, &second),
            (Ok(()), Err(StorageError::RoomLocked(_))) | (Err(StorageError::RoomLocked(_)), Ok(()))
        ),
        "{first:?} {second:?}"
    );
    let opened = if first.is_ok() { one } else { two };
    assert_rows(&opened, &room, 8).await;
    opened.close_room(&room).await.unwrap();

    // Dropped right after its first poll: the task opens the room all the same.
    let dropped = engine_at(dir.path());
    let completed = poll_then_drop(dropped.open_room(&room, schema()), 0).await;
    assert!(completed.is_none(), "the open completed at its first poll");
    let again = dropped.open_room(&room, schema()).await;
    assert!(
        matches!(again, Err(StorageError::RoomAlreadyOpen(_))),
        "{again:?}"
    );
    assert_rows(&dropped, &room, 8).await;
    let reopened = reopen(dropped, dir.path(), &room).await;
    assert_rows(&reopened, &room, 8).await;
}

#[tokio::test]
async fn panic_while_applying_a_batch_exposes_none_of_it() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("panicking-batch").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    // The panic comes after the batch's first operation is applied.
    fail_point::arm("apply_batch.apply", &engine.wal_file_path(&room));
    let writer = spawn_batch(&engine, &room, vec![insert(2), insert(3)]);
    assert!(writer.await.unwrap_err().is_panic());

    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 1);
    assert!(!has_key(&engine, &room, 2).await);
    assert_room_failed(&engine, &room, 2).await;
}

/// Whether `result` is the error a task that panicked is reported as.
fn is_panicked_task<T: std::fmt::Debug>(result: &Result<T, StorageError>) -> bool {
    matches!(result, Err(StorageError::Other(message)) if message.contains("panicked"))
}

/// A panic in the snapshot task after the snapshot is installed on disk, but before memory is
/// replaced, marks the room as failed and releases its compaction lock: closing works, and the
/// reopened room holds the snapshot and keeps the writes made after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_in_the_snapshot_task_after_installing_marks_the_room_failed() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("panic-after-snapshot-install").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=3).await;
    let snapshot = snapshot_with_rows(&room, 101, 5).await;

    fail_point::arm("apply_snapshot.installed", &engine.wal_file_path(&room));
    let applied = engine
        .apply_snapshot(&room, schema(), snapshot.into(), SequenceNumber::from(5u64))
        .await;
    assert!(is_panicked_task(&applied), "{applied:?}");
    // Memory still holds the previous state, and the room refuses writes until reopened.
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 3);
    assert_room_failed(&engine, &room, 4).await;

    let reopened = tokio::time::timeout(Duration::from_secs(10), reopen(engine, dir.path(), &room))
        .await
        .expect("the panic left the compaction lock held, and closing waited for it");
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 5);
    apply_range(&reopened, &room, 6..=7).await;
    let reopened = reopen(reopened, dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 7);
    for key in (101..=105).chain(6..=7) {
        assert!(has_key(&reopened, &room, key).await, "row {key} lost");
    }
}

/// A panic in the open task, after the WALs are replayed and before `.wal.compacting` is
/// folded, registers nothing and releases the engine's rooms and the room's locks: the next open
/// of the same engine recovers every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_in_the_open_task_releases_the_room_and_its_files() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("panic-during-open").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=8).await;
    engine.close_room(&room).await.unwrap();

    let engine = engine_at(dir.path());
    fail_point::arm("recovery.replayed", &engine.wal_file_path(&room));
    let opened = tokio::time::timeout(Duration::from_secs(10), engine.open_room(&room, schema()))
        .await
        .expect("the open never returned");
    assert!(is_panicked_task(&opened), "{opened:?}");
    assert!(matches!(
        engine.get_head_seq(&room).await,
        Err(StorageError::RoomNotFound(_))
    ));

    // Another engine can take the room's files, and so can this one.
    let other = open_engine(dir.path(), &room).await;
    assert_rows(&other, &room, 8).await;
    other.close_room(&room).await.unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 8).await;
    apply_range(&engine, &room, 9..=10).await;
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
}

/// A panic in the compaction task while it rotates the WAL marks the room as failed (the WAL
/// handle points at the renamed file); one after the snapshot rename, outside the WAL mutex,
/// leaves the room usable. Either way no row is lost and the room compacts again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_in_the_compaction_task_loses_no_row() {
    for (point, marks_failed) in [("compaction.rotated", true), ("compaction.renamed", false)] {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("panic-during-compaction").unwrap();
        let engine = open_engine(dir.path(), &room).await;
        apply_range(&engine, &room, 1..=3).await;

        fail_point::arm(point, &engine.wal_file_path(&room));
        let compacted = engine.compact_room(&room).await;
        assert!(is_panicked_task(&compacted), "{point}: {compacted:?}");
        let written = engine.apply_batch(&room, vec![insert(4)]).await;
        let last = if marks_failed {
            assert!(
                matches!(written, Err(StorageError::RoomFailed { .. })),
                "{point}: {written:?}"
            );
            3
        } else {
            assert_eq!(written.unwrap().head_seq().get(), 4, "{point}");
            engine.compact_room(&room).await.unwrap();
            4
        };
        let reopened = reopen(engine, dir.path(), &room).await;
        assert_rows(&reopened, &room, last).await;
        reopened
            .apply_batch(&room, vec![insert(last + 1)])
            .await
            .unwrap();
        reopened.compact_room(&room).await.unwrap();
        let reopened = reopen(reopened, dir.path(), &room).await;
        assert_rows(&reopened, &room, last + 1).await;
    }
}

/// How a test goes on after aborting a compaction.
#[derive(Clone, Copy)]
enum AfterAbort {
    /// Write, then reopen.
    Write,
    /// Write, compact, write again, then reopen.
    WriteAndCompact,
}

/// What a test runs as the compaction it aborts.
#[derive(Clone, Copy)]
enum Caller {
    /// The compaction below `compact_room`.
    Direct,
    /// `compact_room`.
    CompactRoom,
}

/// Runs a compaction in its own task, called as `caller` says, aborts it at the pause point
/// `pause_name`, goes on as `after` says, and checks that every acknowledged write survives a
/// reopen. With `orphan`, an earlier failed compaction leaves `.wal.compacting`, so the WAL is
/// absorbed (and then emptied) instead of renamed.
async fn compaction_aborted_at(
    pause_name: &'static str,
    orphan: bool,
    after: AfterAbort,
    caller: Caller,
) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("aborted-compaction").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=3).await;
    if orphan {
        fail_compaction_leaving_compacting_wal(&engine, &room).await;
        engine.apply_batch(&room, vec![insert(4)]).await.unwrap();
    }

    let mut pause = fail_point::arm_pause(pause_name, &engine.wal_file_path(&room));
    let compaction = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move {
            match caller {
                Caller::Direct => {
                    let room_handle = engine.get_room(&room).await.unwrap();
                    compact_room_cow(room_handle, &engine.options).await
                }
                Caller::CompactRoom => engine.compact_room(&room).await,
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
    let reopened = reopen(engine, dir.path(), &room).await;
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
        Caller::Direct,
    )
    .await;
}

#[tokio::test]
async fn compaction_abandoned_while_emptying_the_wal_loses_no_acknowledged_write() {
    // The WAL is empty but its handle still points past the end: the next write would leave a
    // run of zeros in front of itself.
    compaction_aborted_at("wal.truncate", true, AfterAbort::Write, Caller::Direct).await;
}

#[tokio::test]
async fn compact_room_dropped_after_renaming_the_wal_loses_no_acknowledged_write() {
    compaction_aborted_at(
        "compaction.rotate_renamed",
        false,
        AfterAbort::WriteAndCompact,
        Caller::CompactRoom,
    )
    .await;
}

#[tokio::test]
async fn compact_room_dropped_while_emptying_the_wal_loses_no_acknowledged_write() {
    compaction_aborted_at("wal.truncate", true, AfterAbort::Write, Caller::CompactRoom).await;
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_is_decoded_without_blocking_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("decoded-off-the-runtime").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let snapshot = snapshot_with_rows(&room, 100, 5).await;

    let mut pause =
        fail_point::arm_blocking_pause("apply_snapshot.decode", &engine.wal_file_path(&room));
    // The runtime has a single thread: this task only runs if decoding leaves it free.
    let releaser = tokio::spawn(async move {
        pause.reached().await;
        pause.release();
    });
    let head = engine
        .apply_snapshot(&room, schema(), snapshot.into(), SequenceNumber::from(5u64))
        .await
        .unwrap();
    assert_eq!(head.get(), 5);
    tokio::time::timeout(Duration::from_secs(10), releaser)
        .await
        .expect("decoding never reached its pause point")
        .unwrap();
}

#[tokio::test]
async fn room_stays_locked_while_a_compaction_has_moved_its_wal_aside() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("locked-during-rotation").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    let wal_path = engine.wal_file_path(&room);
    let mut pause = fail_point::arm_pause("compaction.rotate_renamed", &wal_path);
    let compaction = spawn_compaction(&engine, &room);
    pause.reached().await;
    // The WAL is now `.wal.compacting`, and its replacement does not exist yet.
    assert!(!wal_path.exists());

    let other = engine_at(dir.path());
    let result = other.open_room(&room, schema()).await;
    assert!(
        matches!(result, Err(StorageError::RoomLocked(_))),
        "{result:?}"
    );
    assert!(!wal_path.exists());

    pause.release();
    compaction.await.unwrap().unwrap();
    engine.apply_batch(&room, vec![insert(2)]).await.unwrap();
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 2);
}

#[tokio::test]
async fn write_completes_while_a_compaction_stages_its_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("write-during-staging").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=3).await;

    let mut pause = fail_point::arm_pause("compaction.staging", &engine.wal_file_path(&room));
    let compaction = spawn_compaction(&engine, &room);
    pause.reached().await;

    // The compaction has rotated the WAL and holds its snapshot back: a write that waited for
    // it would never finish.
    let write = tokio::time::timeout(
        Duration::from_secs(5),
        engine.apply_batch(&room, vec![insert(4)]),
    )
    .await;
    assert!(
        matches!(&write, Ok(Ok(report)) if report.head_seq().get() == 4),
        "the write waited for the compaction: {write:?}"
    );
    assert!(has_key(&engine, &room, 4).await);

    pause.release();
    compaction.await.unwrap().unwrap();
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 4).await;
}

/// Drops an `apply_batch` of `insert(4)` on a room at head 3 once its WAL write is held on the
/// blocking pool, and returns the handle that holds it.
async fn drop_batch_during_its_write(
    engine: &DiskStorageEngine,
    room: &RoomId,
) -> fail_point::BlockingPause {
    let mut write = fail_point::arm_blocking_pause("file.write_at", &engine.wal_file_path(room));
    tokio::select! {
        _ = engine.apply_batch(room, vec![insert(4)]) => panic!("apply_batch finished during its write"),
        _ = write.reached() => {}
    }
    write
}

/// An engine dropped (without closing the room) while the write of a dropped `apply_batch` is
/// still on the blocking pool keeps the room locked until the write lands: another engine opens
/// the room only afterwards, and its recovery reads the write instead of having it land on top of
/// the batches it acknowledges.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_dropped_during_a_write_keeps_the_room_until_the_write_lands() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("drop-during-write").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=3).await;

    let write = drop_batch_during_its_write(&engine, &room).await;
    drop(engine);
    let other = engine_at(dir.path());
    let early = other.open_room(&room, schema()).await;
    assert!(
        matches!(early, Err(StorageError::RoomLocked(_))),
        "{early:?}"
    );

    write.release();
    open_when_unlocked(&other, &room, schema()).await.unwrap();
    assert_rows(&other, &room, 4).await;
    apply_range(&other, &room, 5..=6).await;
    let reopened = reopen(other, dir.path(), &room).await;
    assert_rows(&reopened, &room, 6).await;
}

/// `close_room` waits for the write of a dropped `apply_batch` still on the blocking pool, and
/// the room can be opened again only once it is closed, with the write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_waits_for_the_write_of_a_dropped_batch() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("close-during-write").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=3).await;

    let write = drop_batch_during_its_write(&engine, &room).await;
    let closing = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.close_room(&room).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!closing.is_finished(), "close did not wait for the write");
    let other = engine_at(dir.path());
    let early = other.open_room(&room, schema()).await;
    assert!(
        matches!(early, Err(StorageError::RoomLocked(_))),
        "{early:?}"
    );

    write.release();
    tokio::time::timeout(Duration::from_secs(10), closing)
        .await
        .expect("close never finished")
        .unwrap()
        .unwrap();
    other.open_room(&room, schema()).await.unwrap();
    assert_rows(&other, &room, 4).await;
}

/// `close_room` returns the error of a failed write of a dropped `apply_batch`, which nobody saw,
/// and the room opens again without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_reports_the_failed_write_of_a_dropped_batch() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("close-after-failed-write").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=3).await;

    fail_point::arm("apply_batch.write", &engine.wal_file_path(&room));
    let write = drop_batch_during_its_write(&engine, &room).await;
    write.release();
    let closed = engine.close_room(&room).await;
    assert!(matches!(closed, Err(StorageError::Io(_))), "{closed:?}");

    let reopened = open_engine(dir.path(), &room).await;
    assert_rows(&reopened, &room, 3).await;
    reopened.apply_batch(&room, vec![insert(4)]).await.unwrap();
}

/// A batch at the last sequence number is acknowledged, nothing can follow it, and it survives a
/// reopen, with or without a compaction before it.
#[tokio::test]
async fn batch_at_the_last_sequence_number_survives_reopen_and_compaction() {
    let last = u64::MAX;
    let tables = HashMap::from([(USERS, Table::new())]);
    let raw = bincode::serialize(&RoomSnapshotRef {
        head_seq: SequenceNumber::from(last - 1),
        tables: &tables,
    })
    .unwrap();
    let snapshot = encode_snapshot_envelope(&raw, false, 0).unwrap();
    for compact in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("last-sequence").unwrap();
        let engine = open_engine(dir.path(), &room).await;
        let head = SequenceNumber::from(last - 1);
        engine
            .apply_snapshot(&room, schema(), snapshot.clone().into(), head)
            .await
            .unwrap();
        let report = engine.apply_batch(&room, vec![insert(last)]).await.unwrap();
        assert_eq!(report.head_seq().get(), last);
        let after_last = engine.apply_batch(&room, vec![insert(last)]).await;
        assert!(
            matches!(after_last, Err(StorageError::SequenceMismatch { .. })),
            "{after_last:?}"
        );
        if compact {
            engine.compact_room(&room).await.unwrap();
        }
        let reopened = reopen(engine, dir.path(), &room).await;
        assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), last);
        assert!(has_key(&reopened, &room, last as i64).await);
    }
}

/// Checks `room` after an operation that would have taken it from the head and rows of `old`
/// to those of `new` was dropped (`k` names the run): in memory and after a reopen, it holds
/// one of the two. If it still takes writes, memory and disk agree and the write survives the
/// reopen; otherwise it is marked failed. Returns whether it still took writes.
async fn check_after_drop(
    engine: DiskStorageEngine,
    dir: &Path,
    room: &RoomId,
    k: usize,
    old: (u64, RangeInclusive<i64>),
    new: (u64, RangeInclusive<i64>),
) -> bool {
    // File operations handed to the blocking pool before the drop complete meanwhile, and so do
    // the tasks that a dropped snapshot or compaction left running with the compaction lock.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let compaction = engine.compaction_lock(room).await.unwrap();
    drop(compaction.lock().await);
    let memory_head = engine.get_head_seq(room).await.unwrap().get();
    let extra = 9_000 + k as i64;
    let usable = engine
        .apply_batch(room, vec![insert_key(memory_head + 1, extra)])
        .await
        .is_ok();
    let reopened = reopen(engine, dir, room).await;
    let head = reopened.get_head_seq(room).await.unwrap().get();
    let base = if usable {
        assert_eq!(head, memory_head + 1, "k={k}: memory and disk disagree");
        assert!(has_key(&reopened, room, extra).await, "k={k}: write lost");
        head - 1
    } else {
        head
    };
    let keys = if base == old.0 {
        old.1
    } else {
        assert_eq!(
            base, new.0,
            "k={k}: head {base} is neither the old nor the new one"
        );
        new.1
    };
    for key in keys {
        assert!(
            has_key(&reopened, room, key).await,
            "k={k}: row {key} missing"
        );
    }
    usable
}

/// `apply_batch` dropped by its caller after any number of suspensions leaves the room before or
/// after the batch, or marked failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_batch_dropped_at_any_suspension_point_keeps_the_room_consistent() {
    for k in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("dropped-batch").unwrap();
        let engine = open_engine(dir.path(), &room).await;
        apply_range(&engine, &room, 1..=3).await;
        let batch = engine.apply_batch(&room, vec![insert(4), insert(5)]);
        let completed = poll_then_drop(batch, k).await.is_some();
        check_after_drop(engine, dir.path(), &room, k, (3, 1..=3), (5, 1..=5)).await;
        if completed {
            break;
        }
    }
}

/// `apply_snapshot` dropped by its caller after any number of suspensions leaves the room before
/// or after the snapshot, and never marked failed: once it has the compaction lock it completes
/// in its own task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_snapshot_dropped_at_any_suspension_point_keeps_the_room_consistent() {
    for k in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("dropped-snapshot").unwrap();
        let engine = open_engine(dir.path(), &room).await;
        apply_range(&engine, &room, 1..=3).await;
        let snapshot = snapshot_with_rows(&room, 101, 5).await;
        let head = SequenceNumber::from(5u64);
        let apply = engine.apply_snapshot(&room, schema(), snapshot.into(), head);
        let completed = poll_then_drop(apply, k).await.is_some();
        let usable =
            check_after_drop(engine, dir.path(), &room, k, (3, 1..=3), (5, 101..=105)).await;
        assert!(usable, "k={k}: the dropped snapshot marked the room failed");
        if completed {
            break;
        }
    }
}

/// `open_room` dropped by its caller after any number of suspensions still opens the room, with
/// every row: once it has the engine's rooms, it completes in its own task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_room_dropped_at_any_suspension_point_still_opens_the_room() {
    for k in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("dropped-open").unwrap();
        let engine = open_engine(dir.path(), &room).await;
        apply_range(&engine, &room, 1..=5).await;
        fail_compaction_leaving_compacting_wal(&engine, &room).await;
        apply_range(&engine, &room, 6..=8).await;
        drop(engine);
        let engine = engine_at(dir.path());
        let completed = poll_then_drop(engine.open_room(&room, schema()), k)
            .await
            .is_some();
        let reopened = reopen(engine, dir.path(), &room).await;
        assert_rows(&reopened, &room, 8).await;
        if completed {
            break;
        }
    }
}

/// `compact_room` dropped by its caller after any number of suspensions loses no row, and leaves
/// the room usable or marked failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_room_dropped_at_any_suspension_point_keeps_the_room_consistent() {
    for k in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("dropped-compaction").unwrap();
        let engine = open_engine(dir.path(), &room).await;
        apply_range(&engine, &room, 1..=3).await;
        let completed = poll_then_drop(engine.compact_room(&room), k)
            .await
            .is_some();
        check_after_drop(engine, dir.path(), &room, k, (3, 1..=3), (3, 1..=3)).await;
        if completed {
            break;
        }
    }
}

/// Closes `room` and opens it with `schema` in a new engine, as a restart would.
async fn reopen_with(
    engine: DiskStorageEngine,
    dir: &Path,
    room: &RoomId,
    schema: Schema,
) -> DiskStorageEngine {
    engine.close_room(room).await.unwrap();
    let engine = engine_at(dir);
    engine.open_room(room, schema).await.unwrap();
    engine
}

/// Applies the next `batches` batches of `log` to `room` in the disk engine and in a memory
/// replica, checking each one, and checks that both end with the same rows and indexes.
async fn apply_to_both(
    disk: &DiskStorageEngine,
    memory: &MemoryStorageEngine,
    room: &RoomId,
    log: &mut RandomLog,
    batches: usize,
) {
    for _ in 0..batches {
        let (ops, expected) = log.next_batch();
        apply_checked(disk, room, ops.clone(), &expected).await;
        apply_checked(memory, room, ops, &expected).await;
    }
    assert_same_as_memory(disk, memory, room).await;
}

/// Checks that the indexes of `room` follow its rows in the disk engine, and that it holds the
/// same rows and indexes as in the memory replica.
async fn assert_same_as_memory(
    disk: &DiskStorageEngine,
    memory: &MemoryStorageEngine,
    room: &RoomId,
) {
    let (_, on_disk) = checked_contents(disk, room).await;
    let (_, in_memory) = memory.contents(room).await;
    assert_same_contents(&on_disk, &in_memory);
}

/// The indexes follow the rows after random batches; after reopening, built from the replayed
/// WAL alone, from a compacted snapshot and the WAL after it, and under a later version of the
/// schema, whose new column the earlier rows lack; and after replaying the WAL that a failed
/// compaction left rotated. They are always those of a memory replica of the same log.
#[tokio::test]
async fn indexes_follow_the_rows_through_reopens_and_compactions() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("indexes-reopened").unwrap();
    let mut engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    let mut memory = MemoryStorageEngine::new();
    memory.open_room(&room, constrained_schema()).await.unwrap();
    let mut log = RandomLog::new(3, constrained_schema());

    apply_to_both(&engine, &memory, &room, &mut log, 15).await;
    engine = reopen_with(engine, dir.path(), &room, constrained_schema()).await;
    assert_same_as_memory(&engine, &memory, &room).await;

    apply_to_both(&engine, &memory, &room, &mut log, 10).await;
    engine.compact_room(&room).await.unwrap();
    apply_to_both(&engine, &memory, &room, &mut log, 5).await;
    engine = reopen_with(engine, dir.path(), &room, constrained_schema()).await;
    assert_same_as_memory(&engine, &memory, &room).await;

    // The memory replica moves to the later schema through a snapshot of the disk room.
    engine = reopen_with(engine, dir.path(), &room, evolved_schema()).await;
    let head = engine.get_head_seq(&room).await.unwrap();
    let snapshot = engine.create_snapshot(&room).await.unwrap();
    memory = MemoryStorageEngine::new();
    memory.open_room(&room, evolved_schema()).await.unwrap();
    memory
        .apply_snapshot(&room, evolved_schema(), snapshot.into(), head)
        .await
        .unwrap();
    assert_same_as_memory(&engine, &memory, &room).await;
    log.evolve(evolved_schema());
    apply_to_both(&engine, &memory, &room, &mut log, 10).await;

    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_to_both(&engine, &memory, &room, &mut log, 5).await;
    assert!(compacting_wal_path(&engine.wal_file_path(&room)).exists());
    engine = reopen_with(engine, dir.path(), &room, evolved_schema()).await;
    assert_same_as_memory(&engine, &memory, &room).await;
    apply_to_both(&engine, &memory, &room, &mut log, 5).await;
}

#[tokio::test]
async fn refused_batches_change_neither_rows_nor_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("refused-batches").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    let before = refused_batches_change_nothing(&engine, &room).await;

    let engine = reopen_with(engine, dir.path(), &room, constrained_schema()).await;
    let (_, after) = checked_contents(&engine, &room).await;
    assert_same_contents(&before, &after);
}

/// Applied snapshots replace rows and indexes together, and the installed snapshot is what a
/// reopen builds them from.
#[tokio::test]
async fn snapshots_replace_rows_and_indexes_together() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("snapshot-indexes").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    snapshots_replace_the_indexes(&engine, &room).await;
    let (_, before) = engine.contents(&room).await;

    let engine = reopen_with(engine, dir.path(), &room, evolved_schema()).await;
    let (_, after) = checked_contents(&engine, &room).await;
    assert_same_contents(&before, &after);
}

/// A panic while applying a batch publishes neither its rows nor its indexes; the reopened room
/// replays it from the WAL, with indexes that follow its rows.
#[tokio::test]
async fn panic_while_applying_a_batch_publishes_neither_rows_nor_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("panic-indexes").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    let memory = MemoryStorageEngine::new();
    memory.open_room(&room, constrained_schema()).await.unwrap();
    let mut log = RandomLog::new(5, constrained_schema());
    apply_to_both(&engine, &memory, &room, &mut log, 10).await;
    let (_, before) = engine.contents(&room).await;

    // The panic comes after the batch's first operation is applied.
    let (ops, expected) = log.next_batch();
    fail_point::arm("apply_batch.apply", &engine.wal_file_path(&room));
    let writer = spawn_batch(&engine, &room, ops.clone());
    assert!(writer.await.unwrap_err().is_panic());
    let (_, after) = checked_contents(&engine, &room).await;
    assert_same_contents(&before, &after);

    apply_checked(&memory, &room, ops, &expected).await;
    let engine = reopen_with(engine, dir.path(), &room, constrained_schema()).await;
    assert_same_as_memory(&engine, &memory, &room).await;
}

/// Opening a room builds the indexes of its snapshot's rows off the runtime, and so does
/// applying a snapshot: on a current-thread runtime, the test's thread never builds them.
#[tokio::test(flavor = "current_thread")]
async fn indexes_are_built_off_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("indexes-off-the-runtime").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    let mut log = RandomLog::new(9, constrained_schema());
    apply_random_batches(&engine, &room, &mut log, 5).await;
    engine.compact_room(&room).await.unwrap();

    let engine = reopen_with(engine, dir.path(), &room, constrained_schema()).await;
    let (_, data) = checked_contents(&engine, &room).await;
    assert!(data.tables().values().any(|table| !table.is_empty()));
    assert!(
        !fail_point::reached_on_this_thread(BUILD_POINT),
        "the indexes were built on the runtime"
    );

    let room = RoomId::new("snapshot-indexes-off-the-runtime").unwrap();
    engine.open_room(&room, constrained_schema()).await.unwrap();
    snapshot_indexes_are_built_off_the_runtime(&engine, &room).await;
}

/// A snapshot that breaks a constraint changes nothing, in memory or on disk: the reopened room
/// holds what it held before.
#[tokio::test]
async fn a_snapshot_that_breaks_a_constraint_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("snapshot-breaks").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    let (schema, before) = snapshots_that_break_a_constraint_change_nothing(&engine, &room).await;
    let head = engine.get_head_seq(&room).await.unwrap();

    let engine = reopen_with(engine, dir.path(), &room, schema).await;
    assert_eq!(engine.get_head_seq(&room).await.unwrap(), head);
    let (_, after) = checked_contents(&engine, &room).await;
    assert_same_contents(&before, &after);
}

/// A snapshot that meets the constraints is installed, and the room reopens with it.
#[tokio::test]
async fn a_snapshot_that_meets_the_constraints_is_installed() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("snapshot-meets").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    snapshots_that_meet_the_constraints_are_installed(&engine, &room).await;
    let (schema, before) = engine.contents(&room).await;

    let engine = reopen_with(engine, dir.path(), &room, schema).await;
    let (_, after) = checked_contents(&engine, &room).await;
    assert_same_contents(&before, &after);
}

/// A `.wal.compacting` whose only live record is a rejected update is folded too: the head moves
/// past it and survives a second reopen, and the next batch continues it.
#[tokio::test]
async fn compacting_wal_with_only_rejected_records_keeps_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("rejected-compacting").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=2).await;
    engine.compact_room(&room).await.unwrap();
    let name = vec![ColumnUpdate::new(1, Value::String("missing".into()))];
    let update = Operation::update(USERS, PrimaryKey::single(99i64), name, 100);
    let report = engine
        .apply_batch(&room, vec![sequenced(3, update)])
        .await
        .unwrap();
    assert_eq!(report.rejected().len(), 1);
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    assert!(compacting_wal_path(&engine.wal_file_path(&room)).exists());

    let engine = reopen(engine, dir.path(), &room).await;
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 3);
    assert!(!compacting_wal_path(&engine.wal_file_path(&room)).exists());
    let engine = reopen(engine, dir.path(), &room).await;
    assert_eq!(
        engine.get_head_seq(&room).await.unwrap().get(),
        3,
        "the head moved back after the rotated WAL was removed"
    );
    engine.apply_batch(&room, vec![insert(4)]).await.unwrap();
    let engine = reopen(engine, dir.path(), &room).await;
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 4);
    assert!(has_key(&engine, &room, 4).await);
}

/// `apply_batch` dropped by its caller after any number of suspensions leaves indexes that
/// follow the rows, in memory and after a reopen, and equal those of a memory replica of
/// whatever reached the WAL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_batch_dropped_at_any_suspension_point_keeps_the_indexes_on_the_rows() {
    for k in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("dropped-batch-indexes").unwrap();
        let engine = engine_at(dir.path());
        engine.open_room(&room, constrained_schema()).await.unwrap();
        let memory = MemoryStorageEngine::new();
        memory.open_room(&room, constrained_schema()).await.unwrap();
        let mut log = RandomLog::new(21, constrained_schema());
        apply_to_both(&engine, &memory, &room, &mut log, 6).await;
        let before = log.head();
        let (ops, expected) = log.next_batch();
        let completed = poll_then_drop(engine.apply_batch(&room, ops.clone()), k)
            .await
            .is_some();
        // File operations handed to the blocking pool before the drop complete meanwhile.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (_, in_memory_now) = checked_contents(&engine, &room).await;
        let head_now = engine.get_head_seq(&room).await.unwrap().get();
        assert!(head_now == before || head_now == log.head(), "k={k}");

        let engine = reopen_with(engine, dir.path(), &room, constrained_schema()).await;
        let (_, reopened) = checked_contents(&engine, &room).await;
        let head = engine.get_head_seq(&room).await.unwrap().get();
        if head == log.head() {
            apply_checked(&memory, &room, ops, &expected).await;
        } else {
            assert_eq!(head, before, "k={k}");
        }
        let (_, replica) = memory.contents(&room).await;
        assert_same_contents(&reopened, &replica);
        if head_now == head {
            assert_same_contents(&in_memory_now, &reopened);
        }
        if completed {
            break;
        }
    }
}

/// `apply_snapshot` dropped by its caller after any number of suspensions leaves the room with
/// the old schema, rows and indexes or with the snapshot's, never a mix, in memory and after a
/// reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_snapshot_dropped_at_any_suspension_point_keeps_the_indexes_on_the_rows() {
    for k in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("dropped-snapshot-indexes").unwrap();
        let engine = engine_at(dir.path());
        engine.open_room(&room, constrained_schema()).await.unwrap();
        let mut log = RandomLog::new(23, constrained_schema());
        apply_random_batches(&engine, &room, &mut log, 4).await;
        let (_, old) = checked_contents(&engine, &room).await;

        let source = MemoryStorageEngine::new();
        source.open_room(&room, evolved_schema()).await.unwrap();
        let mut source_log = RandomLog::new(29, evolved_schema());
        while source_log.head() <= log.head() {
            apply_random_batches(&source, &room, &mut source_log, 1).await;
        }
        let ahead = SequenceNumber::from(source_log.head());
        let snapshot = source.create_snapshot(&room).await.unwrap();
        let (_, new) = checked_contents(&source, &room).await;

        let apply = engine.apply_snapshot(&room, evolved_schema(), snapshot.into(), ahead);
        let completed = poll_then_drop(apply, k).await.is_some();
        // File operations handed to the blocking pool before the drop complete meanwhile, and so
        // does the task that a dropped snapshot left running with the compaction lock.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let compaction = engine.compaction_lock(&room).await.unwrap();
        drop(compaction.lock().await);
        let (schema, now) = checked_contents(&engine, &room).await;
        if schema == evolved_schema() {
            assert_same_contents(&now, &new);
        } else {
            assert_same_contents(&now, &old);
        }
        let engine = reopen_with(engine, dir.path(), &room, evolved_schema()).await;
        let (_, reopened) = checked_contents(&engine, &room).await;
        if engine.get_head_seq(&room).await.unwrap() == ahead {
            assert_same_contents(&reopened, &new);
        } else {
            assert_eq!(reopened.tables(), old.tables(), "k={k}");
        }
        if completed {
            break;
        }
    }
}

/// The long version of the tests above: many random logs, each reopened and compacted at
/// random points. Run it with `cargo test -p zemdb-storage --release -- --ignored`.
#[tokio::test]
#[ignore]
async fn indexes_follow_the_rows_of_many_logs() {
    for seed in 0..200u64 {
        let outcome = std::panic::AssertUnwindSafe(indexes_follow_the_rows_of_log(seed))
            .catch_unwind()
            .await;
        if let Err(panic) = outcome {
            eprintln!("the log of seed {seed} failed");
            std::panic::resume_unwind(panic);
        }
    }
}

/// Applies the random log of `seed` to a disk room and a memory replica, reopening or
/// compacting the disk room at random points.
async fn indexes_follow_the_rows_of_log(seed: u64) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("many-logs").unwrap();
    let mut engine = engine_at(dir.path());
    engine.open_room(&room, evolved_schema()).await.unwrap();
    let memory = MemoryStorageEngine::new();
    memory.open_room(&room, evolved_schema()).await.unwrap();
    let mut log = RandomLog::new(seed, evolved_schema());
    let mut rng = Rng::new(seed);
    for _ in 0..40 {
        apply_to_both(&engine, &memory, &room, &mut log, 1).await;
        match rng.below(8) {
            0 => engine = reopen_with(engine, dir.path(), &room, evolved_schema()).await,
            1 => engine.compact_room(&room).await.unwrap(),
            _ => {}
        }
    }
    let engine = reopen_with(engine, dir.path(), &room, evolved_schema()).await;
    assert_same_as_memory(&engine, &memory, &room).await;
}

// ---------------------------------------------------------------------------------------------
// Reloading the schema

#[tokio::test]
async fn reloads_to_a_schema_that_does_not_evolve_change_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("reload-refused").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, owned_schema()).await.unwrap();
    reloads_that_do_not_evolve_change_nothing(&engine, &room).await;
    let (_, before) = engine.contents(&room).await;

    let engine = reopen_with(engine, dir.path(), &room, owned_schema()).await;
    let (_, after) = checked_contents(&engine, &room).await;
    assert_same_contents(&before, &after);
}

#[tokio::test]
async fn reload_of_an_unopened_room_is_room_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    let room = RoomId::new("reload-unopened").unwrap();
    let err = engine
        .reload_schema(&room, evolved_schema())
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::RoomNotFound(_)), "{err:?}");
}

/// A reload decides from then on as the new schema, builds the indexes of its new constraints
/// off the runtime (on a current-thread runtime, the test's thread never builds them), and
/// reopening the room with the new schema gives the same rows and indexes.
#[tokio::test(flavor = "current_thread")]
async fn reloads_decide_as_the_new_schema() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("reload-evolves").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    reloads_that_evolve_decide_as_the_new_schema(&engine, &room).await;
    assert!(
        !fail_point::reached_on_this_thread(BUILD_POINT),
        "the indexes were built on the runtime"
    );
    let (_, before) = engine.contents(&room).await;

    let engine = reopen_with(engine, dir.path(), &room, reloaded_schema()).await;
    let (_, after) = checked_contents(&engine, &room).await;
    assert_equivalent_contents(&before, &after);
}

/// Reloads at random points of random logs decide as a room open with the new schema, and
/// reopening with the new schema, from the WAL or from a compacted snapshot, gives the same
/// rows and indexes.
#[tokio::test]
async fn reloads_at_random_points_decide_as_a_room_open_with_the_new_schema() {
    for seed in 0..6 {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("reload-random").unwrap();
        let engine = engine_at(dir.path());
        engine.open_room(&room, constrained_schema()).await.unwrap();
        reloads_at_random_points_decide_as_the_new_schema(&engine, &room, seed).await;
        if seed % 2 == 1 {
            engine.compact_room(&room).await.unwrap();
        }
        let (_, before) = engine.contents(&room).await;
        let engine = reopen_with(engine, dir.path(), &room, reloaded_schema()).await;
        let (_, after) = checked_contents(&engine, &room).await;
        assert_equivalent_contents(&before, &after);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reloads_racing_batches_and_readers_apply_each_batch_under_one_schema() {
    for seed in 0..10 {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("reload-race").unwrap();
        let engine = engine_at(dir.path());
        engine.open_room(&room, constrained_schema()).await.unwrap();
        reloads_race_batches_and_readers(&engine, &room, seed).await;
        let (_, before) = engine.contents(&room).await;
        let engine = reopen_with(engine, dir.path(), &room, evolved_schema()).await;
        let (_, after) = checked_contents(&engine, &room).await;
        assert_equivalent_contents(&before, &after);
    }
}

/// A disk room reopened with a schema narrower than the one its rows were written under keeps
/// their values in the columns it lacks, and a reload indexes them: the index of a new
/// `UNIQUE` constraint holds the row written before the reopen, and a row that repeats its key
/// is rejected.
#[tokio::test]
async fn reload_after_a_narrower_reopen_indexes_the_rows_wider_than_the_schema() {
    for compact in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("reload-wider-rows").unwrap();
        let engine = engine_at(dir.path());
        engine.open_room(&room, constrained_schema()).await.unwrap();
        engine.reload_schema(&room, evolved_schema()).await.unwrap();
        let report = engine
            .apply_batch(&room, vec![tagged_parent(1, 1, "c1", "1")])
            .await
            .unwrap();
        assert!(report.rejected().is_empty(), "{report:?}");
        if compact {
            engine.compact_room(&room).await.unwrap();
        }
        let engine = reopen_with(engine, dir.path(), &room, constrained_schema()).await;
        let (_, narrower) = checked_contents(&engine, &room).await;
        let row = &narrower.tables()[&PARENTS][&PrimaryKey::single(1i64)];
        assert_eq!(row.len(), 4, "the tag was dropped: {row:?}");

        engine.reload_schema(&room, evolved_schema()).await.unwrap();
        let (schema, data) = checked_contents(&engine, &room).await;
        assert_eq!(schema, evolved_schema());
        assert_eq!(data.tables(), narrower.tables());
        let report = engine
            .apply_batch(&room, vec![tagged_parent(2, 2, "c2", "1")])
            .await
            .unwrap();
        let unique = RejectReason::Unique {
            constraint: "parents_tag".into(),
        };
        assert_eq!(report.rejected(), [(SequenceNumber::from(2u64), unique)]);
    }
}

/// A reload is refused (`SnapshotConstraints`, naming the table and the constraint) when the
/// rows of a room reopened with a narrower schema break a new constraint: they were written
/// under another schema that extends the narrower one, without that constraint. The room keeps
/// its schema, rows and indexes, and keeps applying batches.
#[tokio::test]
async fn reload_refuses_rows_that_break_its_new_constraints() {
    let mut unique_free = constrained_schema();
    let tag = zemdb_core::ColumnDef::new("tag", zemdb_core::DataType::String).nullable(true);
    unique_free.add_column("parents", tag).unwrap();
    let mut foreign_free = evolved_schema();
    let alt = zemdb_core::ColumnDef::new("alt", zemdb_core::DataType::Int).nullable(true);
    foreign_free.add_column("children", alt).unwrap();
    let cases = [
        (
            unique_free,
            vec![
                tagged_parent(1, 1, "c1", "1"),
                tagged_parent(2, 2, "c2", "1"),
            ],
            constrained_schema(),
            evolved_schema(),
            (PARENTS, "parents_tag"),
        ),
        (
            foreign_free,
            vec![child_with_alt(1, (1, 1), 999)],
            evolved_schema(),
            reloaded_schema(),
            (CHILDREN, "children_alt"),
        ),
    ];
    for (written_under, ops, narrower, reloaded, (table, constraint)) in cases {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("reload-breaks").unwrap();
        let engine = engine_at(dir.path());
        engine.open_room(&room, written_under).await.unwrap();
        let head = ops.len() as u64;
        let report = engine.apply_batch(&room, ops).await.unwrap();
        assert!(report.rejected().is_empty(), "{report:?}");
        let engine = reopen_with(engine, dir.path(), &room, narrower.clone()).await;
        let (_, before) = checked_contents(&engine, &room).await;

        let err = engine.reload_schema(&room, reloaded).await.unwrap_err();
        assert!(
            matches!(
                &err,
                StorageError::SnapshotConstraints { table_id, constraint: named }
                    if *table_id == table && named == constraint
            ),
            "{err:?}"
        );
        let (schema, after) = checked_contents(&engine, &room).await;
        assert_eq!(schema, narrower);
        assert_same_contents(&before, &after);
        engine
            .apply_batch(&room, vec![insert_parent(head + 1, "after")])
            .await
            .unwrap();
    }
}

/// A reload of a room marked as failed is `RoomFailed`, and changes nothing.
#[tokio::test]
async fn reload_of_a_failed_room_is_room_failed() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("reload-failed").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    let mut log = RandomLog::new(43, constrained_schema());
    apply_random_batches(&engine, &room, &mut log, 5).await;
    let (ops, _) = log.next_batch();
    fail_point::arm("apply_batch.sync", &engine.wal_file_path(&room));
    assert!(engine.apply_batch(&room, ops).await.is_err());
    let (_, before) = checked_contents(&engine, &room).await;

    let err = engine
        .reload_schema(&room, evolved_schema())
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::RoomFailed { .. }), "{err:?}");
    let (schema, after) = checked_contents(&engine, &room).await;
    assert_eq!(schema, constrained_schema());
    assert_same_contents(&before, &after);
}

/// A compaction that captured the tables before a reload and records its snapshot while the
/// reload builds keeps what it records: the reload publishes only the schema and the rows, not
/// the snapshot's sequence and size. Its snapshot, written with the tables of before the reload,
/// reopens with the new schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_keeps_what_a_compaction_records_meanwhile() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("reload-compaction").unwrap();
    let engine = engine_at(dir.path());
    engine.open_room(&room, constrained_schema()).await.unwrap();
    let mut log = RandomLog::new(47, constrained_schema());
    apply_random_batches(&engine, &room, &mut log, 10).await;
    let wal_path = engine.wal_file_path(&room);

    let mut staging = fail_point::arm_pause("compaction.staging", &wal_path);
    let compaction = spawn_compaction(&engine, &room);
    staging.reached().await;
    let mut build = fail_point::arm_blocking_pause("reload_schema.build", &wal_path);
    let reload = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.reload_schema(&room, evolved_schema()).await }
    });
    tokio::time::timeout(Duration::from_secs(10), build.reached())
        .await
        .expect("the reload never built its indexes");
    staging.release();
    compaction.await.unwrap().unwrap();
    build.release();
    reload.await.unwrap().unwrap();

    {
        let room = engine.get_room(&room).await.unwrap();
        let state = room.state.read().await;
        assert_eq!(state.snapshot_seq.get(), log.head());
        assert!(state.snapshot_len > 0);
        assert_eq!(state.schema, evolved_schema());
    }
    log.evolve(evolved_schema());
    apply_random_batches(&engine, &room, &mut log, 5).await;
    let (_, before) = checked_contents(&engine, &room).await;
    let engine = reopen_with(engine, dir.path(), &room, evolved_schema()).await;
    let (_, after) = checked_contents(&engine, &room).await;
    assert_equivalent_contents(&before, &after);
}

/// A reload and an `apply_snapshot` in progress take effect in the order of the WAL mutex: a
/// reload while the snapshot is staged, or a snapshot that waits for a reload's build, ends
/// with the snapshot's schema, rows and indexes, which reopen the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_and_snapshot_in_progress_take_effect_in_order() {
    for reload_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("reload-snapshot").unwrap();
        let engine = engine_at(dir.path());
        engine.open_room(&room, constrained_schema()).await.unwrap();
        let mut log = RandomLog::new(53, constrained_schema());
        apply_random_batches(&engine, &room, &mut log, 3).await;
        let wal_path = engine.wal_file_path(&room);

        let source = MemoryStorageEngine::new();
        source.open_room(&room, constrained_schema()).await.unwrap();
        let mut source_log = RandomLog::new(59, constrained_schema());
        while source_log.head() <= log.head() {
            apply_random_batches(&source, &room, &mut source_log, 1).await;
        }
        let ahead = SequenceNumber::from(source_log.head());
        let snapshot = source.create_snapshot(&room).await.unwrap();
        let (_, expected) = checked_contents(&source, &room).await;
        let apply_snapshot = || {
            let (engine, room, snapshot) = (engine.clone(), room.clone(), snapshot.clone());
            tokio::spawn(async move {
                let schema = constrained_schema();
                engine
                    .apply_snapshot(&room, schema, snapshot.into(), ahead)
                    .await
            })
        };

        if reload_first {
            let mut build = fail_point::arm_blocking_pause("reload_schema.build", &wal_path);
            let reload = tokio::spawn({
                let (engine, room) = (engine.clone(), room.clone());
                async move { engine.reload_schema(&room, evolved_schema()).await }
            });
            tokio::time::timeout(Duration::from_secs(10), build.reached())
                .await
                .expect("the reload never built its indexes");
            let applied = apply_snapshot();
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!applied.is_finished());
            build.release();
            reload.await.unwrap().unwrap();
            assert_eq!(applied.await.unwrap().unwrap(), ahead);
        } else {
            let mut staged = fail_point::arm_pause("apply_snapshot.staged", &wal_path);
            let applied = apply_snapshot();
            staged.reached().await;
            engine.reload_schema(&room, evolved_schema()).await.unwrap();
            staged.release();
            assert_eq!(applied.await.unwrap().unwrap(), ahead);
        }
        let (schema, now) = checked_contents(&engine, &room).await;
        assert_eq!(schema, constrained_schema(), "reload first: {reload_first}");
        assert_same_contents(&now, &expected);
        let engine = reopen_with(engine, dir.path(), &room, constrained_schema()).await;
        let (_, after) = checked_contents(&engine, &room).await;
        assert_same_contents(&after, &expected);
    }
}

/// `reload_schema` dropped by its caller after any number of suspensions leaves the room with
/// the old schema or the new one, with indexes that follow its rows, and usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_dropped_at_any_suspension_point_keeps_the_room_consistent() {
    for k in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("dropped-reload").unwrap();
        let engine = engine_at(dir.path());
        engine.open_room(&room, constrained_schema()).await.unwrap();
        let mut log = RandomLog::new(61, constrained_schema());
        apply_random_batches(&engine, &room, &mut log, 6).await;
        let (_, before) = engine.contents(&room).await;

        let reload = engine.reload_schema(&room, evolved_schema());
        let completed = poll_then_drop(reload, k).await.is_some();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (schema, now) = checked_contents(&engine, &room).await;
        assert!(
            schema == constrained_schema() || schema == evolved_schema(),
            "k={k}"
        );
        assert_eq!(now.tables(), before.tables(), "k={k}");
        if completed {
            assert_eq!(schema, evolved_schema(), "k={k}");
        }
        log.evolve(schema.clone());
        apply_random_batches(&engine, &room, &mut log, 2).await;
        if completed {
            break;
        }
    }
}

/// The reloads of a random log reopened now and then with a narrower schema: see
/// [`narrower_reopens_then_reloads`].
#[tokio::test]
async fn reloads_after_narrower_reopens_meet_the_constraints() {
    let mut reloaded = 0;
    for seed in 0..8 {
        reloaded += narrower_reopens_then_reloads(seed).await.0;
    }
    assert!(reloaded > 0);
}

/// The long version of the test above. Run it with
/// `cargo test -p zemdb-storage --release -- --ignored`.
#[tokio::test]
#[ignore]
async fn reloads_after_narrower_reopens_of_many_logs_meet_the_constraints() {
    let (mut reloaded, mut refused) = (0, 0);
    for seed in 0..200u64 {
        let outcome = std::panic::AssertUnwindSafe(narrower_reopens_then_reloads(1000 + seed))
            .catch_unwind()
            .await;
        match outcome {
            Ok((ok, err)) => {
                reloaded += ok;
                refused += err;
            }
            Err(panic) => {
                eprintln!("the log of seed {} failed", 1000 + seed);
                std::panic::resume_unwind(panic);
            }
        }
    }
    assert!(reloaded > 0 && refused > 0, "{reloaded} {refused}");
}

/// Applies a random log of `seed` to a disk room under versions of [`constrained_schema`] with
/// random columns added (and the constraints that come with them), reopening it now and then
/// with an earlier version than the one it is under, narrower than its rows (half of the times
/// after a compaction, so that the wide rows also come from the snapshot), and reloading it to a
/// later version at random points. A reload that succeeds leaves rows that meet the constraints
/// of the new schema, and indexes that follow them; one that is refused
/// (`SnapshotConstraints`) changes nothing. A reopen with a version whose constraints the
/// snapshot's rows break, compacted under a narrower one, is refused too, and the room reopens
/// with an earlier version. Returns how many reloads succeeded and how many were refused.
async fn narrower_reopens_then_reloads(seed: u64) -> (usize, usize) {
    let case = format!("seed {seed}");
    let mut rng = Rng::new(seed);
    let mut versions = vec![constrained_schema()];
    for n in 0..3 {
        let next = oracle::add_random_column(&mut rng, versions.last().unwrap(), n);
        versions.push(next);
    }
    let last = versions.len() - 1;
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("narrower-reopens").unwrap();
    let mut engine = engine_at(dir.path());
    let mut current = last;
    engine
        .open_room(&room, versions[current].clone())
        .await
        .unwrap();
    let (mut reloaded, mut refused, mut head) = (0, 0, 0);
    for _ in 0..60 {
        let len = 1 + rng.below(6);
        let ops = oracle::random_log(&mut rng, &versions[current], head + 1, len);
        engine.apply_batch(&room, ops).await.unwrap();
        head += len;
        match rng.below(10) {
            0 | 1 => {
                if rng.chance(50) {
                    engine.compact_room(&room).await.unwrap();
                }
                current = rng.below(current as u64 + 1) as usize;
                engine.close_room(&room).await.unwrap();
                engine = engine_at(dir.path());
                // A snapshot compacted while the rows broke a constraint of this version (one
                // that a reload refused, or was never asked for) does not open with it: the room
                // opens with an earlier one.
                while let Err(err) = engine.open_room(&room, versions[current].clone()).await {
                    match err {
                        StorageError::SnapshotConstraints { .. } if current > 0 => current -= 1,
                        err => panic!("{case}: {err:?}"),
                    }
                }
                checked_contents(&engine, &room).await;
            }
            2..=4 if current < last => {
                let target = current + 1 + rng.below((last - current) as u64) as usize;
                let (_, before) = checked_contents(&engine, &room).await;
                match engine.reload_schema(&room, versions[target].clone()).await {
                    Ok(()) => {
                        current = target;
                        reloaded += 1;
                        let (schema, data) = checked_contents(&engine, &room).await;
                        assert_eq!(schema, versions[target], "{case}");
                        assert_eq!(data.tables(), before.tables(), "{case}");
                        RoomData::build_checked(&schema, data.tables().clone())
                            .unwrap_or_else(|err| panic!("{case}: {err:?}"));
                    }
                    Err(StorageError::SnapshotConstraints { .. }) => {
                        refused += 1;
                        let (schema, after) = checked_contents(&engine, &room).await;
                        assert_eq!(schema, versions[current], "{case}");
                        assert_same_contents(&before, &after);
                    }
                    Err(err) => panic!("{case}: {err:?}"),
                }
            }
            _ => {}
        }
    }
    (reloaded, refused)
}
