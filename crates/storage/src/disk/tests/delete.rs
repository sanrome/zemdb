//! `delete_room` on disk: what it removes and what it refuses, and the room that a crash, an I/O
//! error or a dropped call in the middle of it leaves.

use super::drop_batch_during_its_write;
use super::support::*;
use crate::disk::compactor::{absorbing_wal_path, compacting_wal_path};
use crate::fail_point;
use crate::files::{NativeFiles, RoomFiles};
use crate::{DiskStorageEngine, ScanOptions, StorageEngine, StorageError};
use futures::StreamExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use zemdb_core::RoomId;

/// The staged snapshot that [`closed_room_with_every_file`] leaves next to `snap_path`.
fn staged_path(snap_path: &Path) -> PathBuf {
    snap_path.with_extension("snap.tmp.crashed")
}

/// Leaves `room` closed in `dir` with every kind of file `delete_room` removes: a snapshot at
/// sequence 5, the rotated WAL of an interrupted compaction with 6..=10, the active WAL with
/// 11..=15, a staged snapshot and the copy of an interrupted absorption.
async fn closed_room_with_every_file(dir: &Path, room: &RoomId) -> DiskStorageEngine {
    let engine = open_engine(dir, room).await;
    apply_range(&engine, room, 1..=5).await;
    engine.compact_room(room).await.unwrap();
    apply_range(&engine, room, 6..=10).await;
    fail_compaction_leaving_compacting_wal(&engine, room).await;
    apply_range(&engine, room, 11..=15).await;
    engine.close_room(room).await.unwrap();
    std::fs::write(staged_path(&engine.snap_file_path(room)), b"partial").unwrap();
    std::fs::write(absorbing_wal_path(&engine.wal_file_path(room)), b"partial").unwrap();
    engine
}

/// The names of the files of `room` in `dir`, sorted.
fn room_files(dir: &Path, room: &RoomId) -> Vec<String> {
    let prefix = format!("room_{room}.");
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(&prefix))
        .collect();
    names.sort();
    names
}

/// Opens `room` in a new engine on `dir`, checks that it holds exactly the rows of the sequences
/// up to its head, closes it and returns the head.
async fn open_head(dir: &Path, room: &RoomId) -> u64 {
    let engine = open_engine(dir, room).await;
    let head = engine.get_head_seq(room).await.unwrap().get();
    assert_rows(&engine, room, head).await;
    let rows = engine
        .scan(room, "users", ScanOptions::new())
        .await
        .unwrap();
    assert_eq!(rows.count().await as u64, head);
    engine.close_room(room).await.unwrap();
    head
}

/// `delete_room` removes every file of a room but its lock file, those of an interrupted
/// compaction too, and leaves another room as it was; the room then opens empty and takes writes.
#[tokio::test]
async fn delete_room_leaves_only_the_lock_file_and_the_room_opens_empty() {
    let dir = tempfile::tempdir().unwrap();
    let (room, kept) = (
        RoomId::new("deleted").unwrap(),
        RoomId::new("kept").unwrap(),
    );
    let engine = closed_room_with_every_file(dir.path(), &room).await;
    engine.open_room(&kept, schema()).await.unwrap();
    apply_range(&engine, &kept, 1..=3).await;
    let kept_files = room_files(dir.path(), &kept);

    engine.delete_room(&room).await.unwrap();

    assert_eq!(room_files(dir.path(), &room), ["room_deleted.lock"]);
    assert_eq!(room_files(dir.path(), &kept), kept_files);
    engine.close_room(&kept).await.unwrap();
    assert_eq!(open_head(dir.path(), &kept).await, 3);
    assert_eq!(open_head(dir.path(), &room).await, 0);
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=1).await;
}

/// `delete_room` of a room never opened, in a data directory that does not exist yet, creates the
/// directory and leaves only the room's lock file in it, as on the first open of a new install.
#[tokio::test]
async fn delete_room_of_a_room_never_opened_leaves_only_its_lock_file() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("missing");
    let room = RoomId::new("never-opened").unwrap();

    engine_at(&data).delete_room(&room).await.unwrap();

    assert_eq!(room_files(&data, &room), ["room_never-opened.lock"]);
    assert_eq!(open_head(&data, &room).await, 0);
}

/// `delete_room` removes nothing from a room open in its engine (`RoomAlreadyOpen`) or in another
/// one (`RoomLocked`).
#[tokio::test]
async fn delete_room_refuses_an_open_room() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("open").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=3).await;
    let files = room_files(dir.path(), &room);

    let result = engine.delete_room(&room).await;
    assert!(
        matches!(result, Err(StorageError::RoomAlreadyOpen(_))),
        "{result:?}"
    );
    let result = engine_at(dir.path()).delete_room(&room).await;
    assert!(
        matches!(result, Err(StorageError::RoomLocked(_))),
        "{result:?}"
    );

    assert_eq!(room_files(dir.path(), &room), files);
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 3).await;
}

/// A write of a dropped engine still on the blocking pool holds the room's WAL, though not its
/// lock file: `delete_room` is `RoomLocked` and removes nothing until the write lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_room_refuses_a_room_whose_wal_a_dropped_write_holds() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("landing").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    apply_range(&engine, &room, 1..=3).await;
    let lock_path = engine.lock_file_path(&room);

    let write = drop_batch_during_its_write(&engine, &room).await;
    drop(engine);
    let files = room_files(dir.path(), &room);
    assert!(NativeFiles.open_locked(&lock_path).await.unwrap().is_some());
    let other = engine_at(dir.path());
    let result = other.delete_room(&room).await;
    assert!(
        matches!(result, Err(StorageError::RoomLocked(_))),
        "{result:?}"
    );
    assert_eq!(room_files(dir.path(), &room), files);

    write.release();
    let mut deleted = other.delete_room(&room).await;
    for _ in 0..500 {
        if !matches!(deleted, Err(StorageError::RoomLocked(_))) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        deleted = other.delete_room(&room).await;
    }
    deleted.unwrap();
    assert_eq!(open_head(dir.path(), &room).await, 0);
}

/// A crash at any point of `delete_room` leaves a room that opens at a prefix of its log: all of
/// it, its snapshot with the rotated WAL, its snapshot alone, or empty, as it is once it returns.
#[cfg(unix)]
#[tokio::test]
async fn crash_at_any_point_of_delete_room_leaves_a_prefix_of_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let (data, images) = (dir.path().join("data"), dir.path().join("images"));
    let room = RoomId::new("crashed").unwrap();
    let engine = closed_room_with_every_file(&data, &room).await;

    fail_point::arm_crash_plan(
        &data,
        fail_point::CrashAction::CopyAtEveryPoint(images.clone()),
    );
    engine.delete_room(&room).await.unwrap();
    let points = fail_point::disarm_crash_plan(&data);
    let mut heads = Vec::new();
    for n in 1..=points.len() {
        heads.push(open_head(&images.join(n.to_string()), &room).await);
    }
    heads.dedup();
    assert_eq!(heads, [15, 10, 5, 0], "{points:?}");
    assert_eq!(open_head(&data, &room).await, 0);
}

/// Each of the five files of the room is removed and its removal synced. An I/O error removing
/// one, or syncing its removal, is returned by `delete_room` and leaves a room that opens at a
/// prefix of its log; deleting it again removes it.
#[tokio::test]
async fn failed_removal_of_delete_room_leaves_a_prefix_of_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("failed-removal").unwrap();
    let counted = dir.path().join("counted");
    let engine = closed_room_with_every_file(&counted, &room).await;
    // Points are counted from 1, so this plan only counts them.
    fail_point::arm_crash_plan(&counted, fail_point::CrashAction::FailAt(0));
    engine.delete_room(&room).await.unwrap();
    let points = fail_point::disarm_crash_plan(&counted);
    assert_eq!(
        points,
        ["delete_room.remove", "remove", "sync_dir"].repeat(5)
    );
    // The head the room opens at once its first `n` files are removed.
    let heads = [15, 10, 5, 5, 5, 0];

    // The native layer's "remove" is a pause point, which cannot fail.
    for (n, point) in points.iter().enumerate().filter(|(_, p)| **p != "remove") {
        let data = dir.path().join(n.to_string());
        let engine = closed_room_with_every_file(&data, &room).await;
        fail_point::arm_crash_plan(&data, fail_point::CrashAction::FailAt(n + 1));
        let result = engine.delete_room(&room).await;
        fail_point::disarm_crash_plan(&data);
        assert!(
            matches!(result, Err(StorageError::Io(_))),
            "{point} {n}: {result:?}"
        );
        let removed = n / 3 + usize::from(*point == "sync_dir");
        assert_eq!(open_head(&data, &room).await, heads[removed], "{point} {n}");
        engine.delete_room(&room).await.unwrap();
        assert_eq!(open_head(&data, &room).await, 0, "{point} {n}");
    }
}

/// A `delete_room` whose caller gives up during a removal still removes the room before it can be
/// opened again: its task keeps the lock file, so another engine cannot open it meanwhile, and
/// another `delete_room` or an open in its engine waits for it, the open finding the room empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_room_dropped_during_a_removal_completes_before_the_room_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("dropped-delete").unwrap();
    let engine = closed_room_with_every_file(dir.path(), &room).await;
    let compacting_path = compacting_wal_path(&engine.wal_file_path(&room));

    let mut removal = fail_point::arm_blocking_pause("remove", &compacting_path);
    tokio::select! {
        _ = engine.delete_room(&room) => panic!("delete_room finished during a removal"),
        _ = removal.reached() => {}
    }
    let early = engine_at(dir.path()).open_room(&room, schema()).await;
    assert!(
        matches!(early, Err(StorageError::RoomLocked(_))),
        "{early:?}"
    );
    let waited = tokio::time::timeout(Duration::from_millis(100), engine.delete_room(&room)).await;
    assert!(waited.is_err(), "{waited:?}");
    let reopening = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.open_room(&room, schema()).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !reopening.is_finished(),
        "the room reopened during a removal"
    );

    removal.release();
    reopening.await.unwrap().unwrap();
    engine.close_room(&room).await.unwrap();
    assert_eq!(open_head(dir.path(), &room).await, 0);
}
