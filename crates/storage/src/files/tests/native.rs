use super::*;
use crate::fail_point::{self, BlockingPause};
use std::future::Future;
use std::time::Duration;

/// Starts `op` and drops it once it is held at its pause point `name`, armed for `path`: the
/// operation is then on the blocking pool, and nobody awaits it. Returns the pause, still held.
async fn start_and_drop(name: &'static str, path: &Path, op: impl Future) -> BlockingPause {
    let mut hold = fail_point::arm_blocking_pause(name, path);
    tokio::select! {
        _ = op => panic!("{name} completed while held"),
        () = hold.reached() => {}
    }
    hold
}

/// Waits for `path` to be unlocked, and returns a handle that holds its lock.
async fn wait_unlocked(path: &Path) -> NativeFile {
    for _ in 0..1000 {
        if let Some(file) = NativeFiles.open_locked(path).await.unwrap() {
            return file;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{path:?} stayed locked");
}

#[tokio::test]
async fn reads_and_writes_go_at_their_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("file");
    let file = NativeFiles.open_locked(&path).await.unwrap().unwrap();

    // Written out of order: there is no position.
    file.write_at(4, b"world".to_vec()).await.unwrap();
    file.write_at(0, b"helo".to_vec()).await.unwrap();
    assert_eq!(file.size().await.unwrap(), 9);
    assert_eq!(file.read_at(0, 9).await.unwrap(), b"heloworld");
    assert_eq!(file.read_at(4, 5).await.unwrap(), b"world");
    let past_the_end = file.read_at(5, 5).await.unwrap_err();
    assert_eq!(past_the_end.kind(), io::ErrorKind::UnexpectedEof);

    file.set_len(4).await.unwrap();
    assert_eq!(file.size().await.unwrap(), 4);
    file.set_len(6).await.unwrap();
    assert_eq!(file.read_at(0, 6).await.unwrap(), b"helo\0\0");
    file.sync_data().await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);
    assert_eq!(std::fs::read(&path).unwrap(), b"helo\0\0");
}

/// A write through a read-only handle fails in the call that makes it, which `tokio::fs` only
/// reported on a later flush.
#[tokio::test]
async fn a_failed_operation_reports_its_error_in_the_same_call() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read-only");
    std::fs::write(&path, b"data").unwrap();
    let file = NativeFiles.open_read(&path).await.unwrap();

    assert!(file.write_at(4, b"more".to_vec()).await.is_err());
    assert!(file.set_len(0).await.is_err());
    // An error its caller saw is not reported again.
    assert_eq!(file.read_at(0, 4).await.unwrap(), b"data");
    drop(file);
    assert_eq!(std::fs::read(&path).unwrap(), b"data");
}

#[tokio::test]
async fn an_operation_nobody_awaits_keeps_the_file_locked_until_it_ends() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let file = NativeFiles.open_locked(&path).await.unwrap().unwrap();

    let hold = start_and_drop("file.write_at", &path, file.write_at(0, b"late".to_vec())).await;
    drop(file);
    // The handle is gone, but the write is still on the blocking pool: the file stays locked.
    assert!(NativeFiles.open_locked(&path).await.unwrap().is_none());

    hold.release();
    // Locked again only once the write has landed.
    let reopened = wait_unlocked(&path).await;
    assert_eq!(reopened.read_at(0, 4).await.unwrap(), b"late");
}

/// The lock is kept while the operation itself runs, not only until it starts.
#[tokio::test]
async fn a_running_operation_nobody_awaits_keeps_the_file_locked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let file = NativeFiles.open_locked(&path).await.unwrap().unwrap();

    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (go, go_rx) = std::sync::mpsc::channel::<()>();
    let mut write = Box::pin(file.run("file.write_late", move |file, _| {
        entered.send(()).unwrap();
        go_rx.recv().unwrap();
        write_all_at(file, b"late", 0)
    }));
    assert!(futures::poll!(&mut write).is_pending());
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();
    drop(write);
    drop(file);
    assert!(NativeFiles.open_locked(&path).await.unwrap().is_none());

    go.send(()).unwrap();
    let reopened = wait_unlocked(&path).await;
    assert_eq!(reopened.read_at(0, 4).await.unwrap(), b"late");
}

#[tokio::test]
async fn a_later_operation_waits_for_one_nobody_awaits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let file = NativeFiles.open_locked(&path).await.unwrap().unwrap();

    let hold = start_and_drop("file.write_at", &path, file.write_at(0, b"late".to_vec())).await;
    let early = tokio::time::timeout(Duration::from_millis(100), file.sync_all()).await;
    assert!(early.is_err(), "the sync did not wait for the write");

    hold.release();
    file.sync_all().await.unwrap();
    assert_eq!(file.read_at(0, 4).await.unwrap(), b"late");
}

#[tokio::test]
async fn the_error_of_an_operation_nobody_awaits_is_reported_by_the_next_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read-only");
    std::fs::write(&path, b"data").unwrap();
    let file = NativeFiles.open_read(&path).await.unwrap();

    let hold = start_and_drop("file.write_at", &path, file.write_at(0, b"late".to_vec())).await;
    hold.release();
    assert!(
        file.size().await.is_err(),
        "the failed write was not reported"
    );
    // Once.
    assert_eq!(file.size().await.unwrap(), 4);
    assert_eq!(std::fs::read(&path).unwrap(), b"data");
}

/// The operation that reports an earlier error does not run: a write that fails writes nothing.
#[tokio::test]
async fn the_operation_that_reports_an_earlier_error_does_not_run() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let file = NativeFiles.open_locked(&path).await.unwrap().unwrap();

    // Reading an empty file fails, with nobody awaiting the read.
    start_and_drop("file.read_at", &path, file.read_at(0, 4))
        .await
        .release();
    assert!(file.write_at(0, b"late".to_vec()).await.is_err());
    assert_eq!(file.size().await.unwrap(), 0, "the write that failed ran");
}

/// The operation may end, and send its error, before its caller is dropped.
#[tokio::test]
async fn the_error_of_an_operation_dropped_once_it_ended_is_reported_by_the_next_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read-only");
    std::fs::write(&path, b"data").unwrap();
    let file = NativeFiles.open_read(&path).await.unwrap();

    let mut hold = fail_point::arm_blocking_pause("file.write_at", &path);
    let mut write = Box::pin(file.write_at(0, b"late".to_vec()));
    assert!(futures::poll!(&mut write).is_pending());
    hold.reached().await;
    hold.release();
    // The write ends, and sends its error, without its caller polling it.
    for _ in 0..10_000 {
        if file.permit.try_lock().is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    drop(write);

    assert!(
        file.size().await.is_err(),
        "the failed write was not reported"
    );
    assert_eq!(file.size().await.unwrap(), 4);
}

/// An operation that panics, with nobody awaiting it, reports that it did not complete.
#[tokio::test]
async fn an_operation_that_never_ends_nobody_awaits_is_reported_by_the_next_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let file = NativeFiles.open_locked(&path).await.unwrap().unwrap();

    let panics = file.run("file.panics", |_, _| -> io::Result<()> {
        panic!("injected panic in a file operation")
    });
    start_and_drop("file.panics", &path, panics).await.release();
    let err = file.sync_all().await.unwrap_err();
    assert!(err.to_string().contains("did not complete"), "{err}");
    file.sync_all().await.unwrap();
}

/// An operation that panics while its caller waits reports that it did not complete to that
/// caller only.
#[tokio::test]
async fn an_operation_that_never_ends_reports_it_to_its_caller() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let file = NativeFiles.open_locked(&path).await.unwrap().unwrap();

    let err = file
        .run("file.panics", |_, _| -> io::Result<()> {
            panic!("injected panic in a file operation")
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("did not complete"), "{err}");
    file.sync_all().await.unwrap();
}

#[tokio::test]
async fn a_locked_file_cannot_be_locked_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("room.lock");
    let first = NativeFiles.open_locked(&path).await.unwrap().unwrap();
    assert!(NativeFiles.open_locked(&path).await.unwrap().is_none());

    // A file to create must not exist, locked or not.
    let fresh = dir.path().join("room.wal");
    let created = NativeFiles.create_locked(&fresh).await.unwrap().unwrap();
    assert!(NativeFiles.open_locked(&fresh).await.unwrap().is_none());
    for existing in [&path, &fresh] {
        let err = NativeFiles.create_locked(existing).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    drop((first, created));
    assert!(NativeFiles.open_locked(&path).await.unwrap().is_some());
    assert!(NativeFiles.open_locked(&fresh).await.unwrap().is_some());
}

/// Once an operation returned, it no longer holds the file: dropping the handle closes it and
/// releases its lock, so the file can be locked again right away, as a room closed and reopened
/// is. An operation that panics releases it before its error too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handle_dropped_once_its_operations_returned_releases_its_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("room.wal");
    for _ in 0..1000 {
        let file = NativeFiles.open_locked(&path).await.unwrap().unwrap();
        file.sync_data().await.unwrap();
        assert_eq!(
            Arc::strong_count(&file.file),
            1,
            "the operation holds the file"
        );
        drop(file);
        assert!(NativeFiles.open_locked(&path).await.unwrap().is_some());
    }

    let file = NativeFiles.open_locked(&path).await.unwrap().unwrap();
    for _ in 0..100 {
        let panicked = file.run("test.panic", |_, _| -> io::Result<()> {
            panic!("injected")
        });
        assert!(panicked.await.is_err());
        assert_eq!(
            Arc::strong_count(&file.file),
            1,
            "a panicked operation holds the file"
        );
    }
}

#[tokio::test]
async fn a_renamed_open_file_keeps_its_handle_and_its_lock() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("room.wal");
    let moved = dir.path().join("room.wal.compacting");
    std::fs::write(&moved, b"replaced").unwrap();
    let mut file = NativeFiles.open_locked(&wal).await.unwrap().unwrap();
    file.write_at(0, b"before".to_vec()).await.unwrap();

    file.rename(&moved).await.unwrap();
    assert!(!wal.exists());
    file.write_at(6, b" after".to_vec()).await.unwrap();
    assert_eq!(file.read_at(0, 12).await.unwrap(), b"before after");
    assert!(NativeFiles.open_locked(&moved).await.unwrap().is_none());

    // The handle knows where its file is now.
    file.rename(&wal).await.unwrap();
    assert!(!moved.exists());
    drop(file);
    assert_eq!(std::fs::read(&wal).unwrap(), b"before after");
}

/// A rename that fails leaves the file where it was, and its handle on it.
#[tokio::test]
async fn a_failed_rename_of_an_open_file_keeps_its_path() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("room.wal");
    let moved = dir.path().join("room.wal.compacting");
    let mut file = NativeFiles.open_locked(&wal).await.unwrap().unwrap();

    let nowhere = dir.path().join("missing").join("room.wal");
    assert!(file.rename(&nowhere).await.is_err());
    assert!(wal.exists());
    file.rename(&moved).await.unwrap();
    assert!(moved.exists() && !wal.exists());
}

/// An error checking for a file is not a missing file: a missing snapshot is created empty.
#[cfg(unix)]
#[tokio::test]
async fn exists_reports_an_error_that_is_not_a_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"").unwrap();
    // A path under a file is not a directory entry at all.
    assert!(NativeFiles.exists(&file.join("child")).await.is_err());
}

#[tokio::test]
async fn directory_operations_act_on_paths() {
    let dir = tempfile::tempdir().unwrap();
    let (snap, tmp) = (
        dir.path().join("room.snap"),
        dir.path().join("room.snap.tmp.1"),
    );
    let other = dir.path().join("other.snap.tmp.1");
    for (path, contents) in [(&snap, "snap"), (&tmp, "tmp"), (&other, "other")] {
        std::fs::write(path, contents).unwrap();
    }
    let missing = dir.path().join("missing");

    assert!(NativeFiles.exists(&snap).await.unwrap());
    assert!(!NativeFiles.exists(&missing).await.unwrap());
    let listed = NativeFiles
        .list(dir.path(), "room.snap.tmp.")
        .await
        .unwrap();
    assert_eq!(listed, vec![tmp.clone()]);
    assert!(NativeFiles.list(&missing, "room").await.is_err());
    let err = NativeFiles.open_read(&missing).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    // Created empty, even over an existing (longer) file.
    let created = NativeFiles.create(&other).await.unwrap();
    created.write_at(0, b"new".to_vec()).await.unwrap();
    drop(created);
    assert_eq!(std::fs::read(&other).unwrap(), b"new");

    NativeFiles.rename(&tmp, &snap).await.unwrap();
    assert!(!tmp.exists());
    assert_eq!(std::fs::read(&snap).unwrap(), b"tmp");
    NativeFiles.sync_dir(dir.path()).await.unwrap();

    NativeFiles.remove(&snap).await.unwrap();
    let err = NativeFiles.remove(&snap).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[tokio::test]
async fn create_dir_all_creates_missing_components() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("a").join("b");
    NativeFiles.create_dir_all(&target).await.unwrap();
    NativeFiles.create_dir_all(&target).await.unwrap();
    assert!(target.is_dir());
}

#[tokio::test]
async fn create_dir_all_syncs_the_parent_of_every_new_directory() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a");
    let target = a.join("b");
    // The parent of each new directory: failing its sync fails the call.
    for parent in [dir.path(), a.as_path()] {
        fail_point::arm("sync_dir", parent);
        assert!(NativeFiles.create_dir_all(&target).await.is_err());
        std::fs::remove_dir_all(&a).unwrap();
    }
}
