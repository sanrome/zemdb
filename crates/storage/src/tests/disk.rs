use crate::disk::compactor::compact_room_cow;
use crate::fail_point;
use crate::memory::{RoomSnapshotRef, Table};
use crate::snapshot::encode_snapshot_envelope;
use crate::{DiskStorageEngine, MemoryStorageEngine, StorageEngine, StorageError};
use futures::FutureExt;
use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;
use std::path::Path;
use std::task::Poll;
use std::time::Duration;
use support::*;
use zemdb_core::{PrimaryKey, RoomId, SequenceNumber};

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

#[tokio::test]
async fn open_room_syncs_the_data_directory_it_creates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(&dir.path().join("data"));
    fail_point::arm("sync_dir", dir.path());
    let room = RoomId::new("new-dir").unwrap();
    assert!(engine.open_room(&room, schema()).await.is_err());
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

    let reopened = reopen(engine, dir.path(), &room).await;
    let head = reopened.get_head_seq(&room).await.unwrap().get();
    reopened
        .apply_batch(&room, vec![insert(head + 1)])
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_background_wal_write_marks_room_failed_until_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("background-write-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    // The WAL write fails on the blocking pool, after `write_all` has returned.
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
    assert_eq!(first.await.unwrap().unwrap().get(), 2);
    assert_eq!(second.await.unwrap().unwrap().get(), 3);

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
    assert_eq!(writer.await.unwrap().unwrap().get(), 4);

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
        matches!(write, Ok(Ok(head)) if head.get() == 4),
        "the write waited for the compaction: {write:?}"
    );
    assert!(has_key(&engine, &room, 4).await);

    pause.release();
    compaction.await.unwrap().unwrap();
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 4).await;
}

// On Unix a duplicated handle shares the lock of the handle it was duplicated from.
#[cfg(unix)]
#[tokio::test]
async fn room_stays_locked_while_a_handle_of_its_wal_is_still_open() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("wal-still-open").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    // A write of `tokio::fs` still in flight on the blocking pool holds a duplicate of the WAL's
    // handle, like this one, after the engine is dropped without closing the room.
    let in_flight = engine
        .get_room(&room)
        .await
        .unwrap()
        .wal
        .lock()
        .await
        .file
        .try_clone()
        .await
        .unwrap();
    drop(engine);

    let other = engine_at(dir.path());
    let result = other.open_room(&room, schema()).await;
    assert!(
        matches!(result, Err(StorageError::RoomLocked(_))),
        "{result:?}"
    );

    drop(in_flight);
    other.open_room(&room, schema()).await.unwrap();
    assert_eq!(other.get_head_seq(&room).await.unwrap().get(), 1);
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
        let head = engine.apply_batch(&room, vec![insert(last)]).await.unwrap();
        assert_eq!(head.get(), last);
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

/// Polls `operation`, letting the runtime and its blocking pool run between polls, and drops it
/// after it has returned `Pending` `suspensions` times. Returns whether it completed first.
async fn poll_then_drop(operation: impl Future, suspensions: usize) -> bool {
    let mut operation = std::pin::pin!(operation);
    let mut seen = 0;
    loop {
        let ready =
            std::future::poll_fn(|cx| Poll::Ready(operation.as_mut().poll(cx).is_ready())).await;
        if ready {
            return true;
        }
        if seen == suspensions {
            return false;
        }
        seen += 1;
        tokio::time::sleep(Duration::from_millis(2)).await;
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
        let completed = poll_then_drop(batch, k).await;
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
#[ignore = "long-running test; run with --ignored in release"]
async fn apply_snapshot_dropped_at_any_suspension_point_keeps_the_room_consistent() {
    for k in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("dropped-snapshot").unwrap();
        let engine = open_engine(dir.path(), &room).await;
        apply_range(&engine, &room, 1..=3).await;
        let snapshot = snapshot_with_rows(&room, 101, 5).await;
        let head = SequenceNumber::from(5u64);
        let apply = engine.apply_snapshot(&room, schema(), snapshot.into(), head);
        let completed = poll_then_drop(apply, k).await;
        let usable =
            check_after_drop(engine, dir.path(), &room, k, (3, 1..=3), (5, 101..=105)).await;
        assert!(usable, "k={k}: the dropped snapshot marked the room failed");
        if completed {
            break;
        }
    }
}

/// `compact_room` dropped by its caller after any number of suspensions loses no row, and leaves
/// the room usable or marked failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "long-running test; run with --ignored in release"]
async fn compact_room_dropped_at_any_suspension_point_keeps_the_room_consistent() {
    for k in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("dropped-compaction").unwrap();
        let engine = open_engine(dir.path(), &room).await;
        apply_range(&engine, &room, 1..=3).await;
        let completed = poll_then_drop(engine.compact_room(&room), k).await;
        check_after_drop(engine, dir.path(), &room, k, (3, 1..=3), (3, 1..=3)).await;
        if completed {
            break;
        }
    }
}
