//! The unit tests of the layer over the OPFS, in wasm32: in a dedicated Worker of a browser, with
//! `wasm-bindgen-test`. They reach what a step of the browser harness does not: the errors as
//! the layer makes them, and a point in the middle of an operation, where a test acts on its
//! file (a reopening that fails, a move that fails, a file taken between a listing and its
//! opening).

use super::*;
use futures::FutureExt;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A directory of its own for a test, under the root of the OPFS.
async fn fresh(test: &str) -> PathBuf {
    let run = (js_sys::Math::random() * 1e12) as u64;
    let dir = PathBuf::from(format!("zemdb-storage/{test}-{run}"));
    OpfsFiles.create_dir_all(&dir).await.unwrap();
    dir
}

async fn handle_of(path: &Path) -> FileSystemDirectoryHandle {
    let names = opfs_names::dir_names(path).unwrap();
    directory(&names, false).await.unwrap()
}

/// The entries of the directory `path`, in order, through the browser.
async fn entries_of(path: &Path) -> Vec<String> {
    let mut found = entries(&handle_of(path).await).await.unwrap();
    found.sort();
    found
}

/// What the entry `entry` of the directory `path` holds, through the browser.
async fn holds(path: &Path, entry: &str) -> Vec<u8> {
    let file = file_handle(&handle_of(path).await, entry, false)
        .await
        .unwrap();
    let access = access(&file, entry, &[]).await.unwrap();
    let mut bytes = vec![0; access.get_size().unwrap() as usize];
    let read = access.read_with_u8_array_and_options(&mut bytes, &at(0));
    access.close();
    assert_eq!(read.unwrap() as usize, bytes.len());
    bytes
}

/// Makes the entry `entry` of the directory `path` with `bytes`, through the browser.
async fn put(path: &Path, entry: &str, bytes: &[u8]) {
    let file = file_handle(&handle_of(path).await, entry, true)
        .await
        .unwrap();
    let access = access(&file, entry, &[]).await.unwrap();
    let written = access.write_with_u8_array_and_options(bytes, &at(0));
    access.close();
    assert_eq!(written.unwrap() as usize, bytes.len());
}

/// A sync access handle that a test holds on an entry, from an action.
type Held = Rc<RefCell<Option<FileSystemSyncAccessHandle>>>;

/// An action that opens a sync access handle on the entry `entry` of the directory `path`, which
/// another one cannot take until [`let_go`] closes it.
fn hold(path: &Path, entry: &str, held: &Held) -> impl Future<Output = ()> + 'static {
    let (path, entry, held) = (path.to_path_buf(), entry.to_owned(), Rc::clone(held));
    async move {
        let file = file_handle(&handle_of(&path).await, &entry, false)
            .await
            .unwrap();
        *held.borrow_mut() = Some(access(&file, &entry, &[]).await.unwrap());
    }
}

fn let_go(held: &Held) {
    if let Some(access) = held.borrow_mut().take() {
        access.close();
    }
}

/// Each error of the browser is an `io::Error` of the kind of its name, which its message
/// gives, also one that is no `DOMException` (a `TypeError`) or no error at all; and the error
/// of a taken file only where the call says that the browser gives that name for it.
#[wasm_bindgen_test]
fn each_error_of_the_browser_is_an_io_error_of_its_kind_with_its_name() {
    let dom =
        |name| JsValue::from(DomException::new_with_message_and_name("it failed", name).unwrap());

    let err = error("getFileHandle(a)", &dom("NotFoundError"), &[]);
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    assert_eq!(
        err.to_string(),
        "getFileHandle(a): NotFoundError: it failed"
    );
    let err = error("write", &dom("QuotaExceededError"), &[]);
    assert_eq!(err.kind(), io::ErrorKind::StorageFull);
    assert!(!FileTaken::is(&err));

    let type_error = JsValue::from(js_sys::TypeError::new("not a valid name"));
    let err = error("getFileHandle(a)", &type_error, &[]);
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        err.to_string(),
        "getFileHandle(a): TypeError: not a valid name"
    );

    let err = error("keys", &JsValue::from_str("no error"), TAKEN_BY_OPEN);
    assert_eq!(err.kind(), io::ErrorKind::Other);
    let message = err.to_string();
    assert!(
        message.starts_with("keys: : ") && message.contains("no error"),
        "{message}"
    );

    let taken = error(
        "createSyncAccessHandle(a)",
        &dom("InvalidStateError"),
        TAKEN_BY_OPEN,
    );
    assert!(FileTaken::is(&taken));
    assert_eq!(taken.kind(), io::ErrorKind::ResourceBusy);
    assert_eq!(
        taken.to_string(),
        "createSyncAccessHandle(a): InvalidStateError: it failed"
    );
    let err = error("removeEntry(a)", &dom("InvalidStateError"), TAKEN_BY_REMOVE);
    assert!(!FileTaken::is(&err));
    assert_eq!(err.kind(), io::ErrorKind::Other);
    let err = error("read", &dom("NoModificationAllowedError"), &[]);
    assert!(!FileTaken::is(&err));
}

/// Fails unless every operation of `file` fails, as the operations of a dead file do, and the
/// first one with `first` in its error.
async fn expect_dead(file: &mut OpfsFile, to: &Path, first: &str) {
    let mut first = Some(first);
    let mut check = |what: &str, result: io::Result<()>| {
        let message = result.expect_err(what).to_string();
        let expected = first
            .take()
            .unwrap_or("is closed: a rename could not reopen it");
        assert!(message.contains(expected), "{what} failed with {message}");
    };
    check("read_at", file.read_at(0, 1).await.map(drop));
    check("write_at", file.write_at(0, b"written".to_vec()).await);
    check("size", file.size().await.map(drop));
    check("set_len", file.set_len(100).await);
    check("sync_data", file.sync_data().await);
    check("sync_all", file.sync_all().await);
    check("rename", file.rename(to).await);
}

/// A rename that moves the file and cannot reopen it fails, and leaves the file closed: every
/// later operation fails, and none writes in any file.
#[wasm_bindgen_test]
async fn a_rename_that_cannot_reopen_the_file_leaves_it_dead_and_writes_nowhere() {
    let dir = fresh("dead").await;
    let mut file = OpfsFiles
        .open_locked(&dir.join("a"))
        .await
        .unwrap()
        .unwrap();
    file.write_at(0, b"a0".to_vec()).await.unwrap();

    let held = Held::default();
    arm("rename.moved", "b", hold(&dir, "b", &held));
    let err = file.rename(&dir.join("b")).await.unwrap_err();
    assert!(
        err.to_string().contains("createSyncAccessHandle(b)"),
        "{err}"
    );
    assert!(!FileTaken::is(&err));
    let_go(&held);

    expect_dead(&mut file, &dir.join("c"), "is closed").await;
    // The dead file holds no handle: the moved file is free, and holds what it held.
    assert_eq!(entries_of(&dir).await, ["b"]);
    assert_eq!(holds(&dir, "b").await, b"a0");
    drop(file);
    assert_eq!(holds(&dir, "b").await, b"a0");
}

/// The rename of an open file that its caller dropped, and that cannot reopen the file: the next
/// operation gives its error, and does not run, and the ones after it fail as on a dead file.
#[wasm_bindgen_test]
async fn a_dropped_rename_that_cannot_reopen_gives_its_error_to_the_next_operation() {
    let dir = fresh("dropped-dead").await;
    let mut file = OpfsFiles
        .open_locked(&dir.join("e"))
        .await
        .unwrap()
        .unwrap();
    file.write_at(0, b"e0".to_vec()).await.unwrap();

    let held = Held::default();
    arm("rename.moved", "f", hold(&dir, "f", &held));
    assert!(file.rename(&dir.join("f")).now_or_never().is_none());
    // The next rename gives that error before it refuses its path, and does not run.
    let err = file.rename(Path::new("../x")).await.unwrap_err();
    assert!(
        err.to_string().contains("createSyncAccessHandle(f)"),
        "{err}"
    );
    expect_dead(&mut file, &dir.join("g"), "is closed").await;
    let_go(&held);
    assert_eq!(entries_of(&dir).await, ["f"]);
    assert_eq!(holds(&dir, "f").await, b"e0");
}

/// A move that fails reopens the file where it was: the rename fails with the error of the
/// move, and the file goes on, open, under its name and its generation, which a later rename
/// moves, leaving the old generation of the destination of the one that failed.
#[wasm_bindgen_test]
async fn a_move_that_fails_reopens_the_file_where_it_was() {
    let dir = fresh("move-fails").await;
    let mut file = OpfsFiles
        .open_locked(&dir.join("h"))
        .await
        .unwrap()
        .unwrap();
    file.write_at(0, b"h0".to_vec()).await.unwrap();
    put(&dir, "k", b"k0").await;

    // Taken once the rename closed it, and let go once its move failed.
    let held = Held::default();
    arm("rename.closed", "h", hold(&dir, "h", &held));
    let release = Rc::clone(&held);
    arm("rename.moved", "k~1", async move { let_go(&release) });
    let err = file.rename(&dir.join("k")).await.unwrap_err();
    assert!(FileTaken::is(&err), "{err}");
    assert!(held.borrow().is_none());

    file.write_at(2, b"x".to_vec()).await.unwrap();
    assert_eq!(file.read_at(0, 3).await.unwrap(), b"h0x");
    assert_eq!(entries_of(&dir).await, ["h", "k"]);
    file.rename(&dir.join("m")).await.unwrap();
    assert_eq!(file.read_at(0, 3).await.unwrap(), b"h0x");
    assert_eq!(entries_of(&dir).await, ["k", "m"]);
    assert_eq!(holds(&dir, "k").await, b"k0");
}

/// A move to another directory that fails reopens the file where it was, which it finds in the
/// listing of the directory of the file, not of the destination.
#[wasm_bindgen_test]
async fn a_move_to_another_directory_that_fails_reopens_the_file_where_it_was() {
    let dir = fresh("move-fails-elsewhere").await;
    OpfsFiles.create_dir_all(&dir.join("sub")).await.unwrap();
    let mut file = OpfsFiles
        .open_locked(&dir.join("i"))
        .await
        .unwrap()
        .unwrap();
    file.write_at(0, b"i0".to_vec()).await.unwrap();
    let held = Held::default();
    arm("rename.closed", "i", hold(&dir, "i", &held));
    let release = Rc::clone(&held);
    arm("rename.moved", "j", async move { let_go(&release) });
    let err = file.rename(&dir.join("sub").join("j")).await.unwrap_err();
    assert!(FileTaken::is(&err), "{err}");
    file.write_at(2, b"!".to_vec()).await.unwrap();
    assert_eq!(file.read_at(0, 3).await.unwrap(), b"i0!");
    assert_eq!(entries_of(&dir).await, ["i", "sub"]);
    assert!(entries_of(&dir.join("sub")).await.is_empty());
}

/// A move and a reopening that both fail, the file taken by another handle all along: the
/// rename fails with the error of the reopening, which is not the error of a taken file (the
/// file did not stay as it was: it is closed), and the file is dead.
#[wasm_bindgen_test]
async fn a_move_and_a_reopening_that_fail_leave_the_file_dead_and_not_taken() {
    let dir = fresh("both-fail").await;
    let mut file = OpfsFiles
        .open_locked(&dir.join("q"))
        .await
        .unwrap()
        .unwrap();
    file.write_at(0, b"q0".to_vec()).await.unwrap();
    let held = Held::default();
    arm("rename.closed", "q", hold(&dir, "q", &held));
    let err = file.rename(&dir.join("r")).await.unwrap_err();
    let_go(&held);
    assert!(!FileTaken::is(&err), "{err}");
    assert!(
        err.to_string().contains("createSyncAccessHandle(q)"),
        "{err}"
    );
    expect_dead(&mut file, &dir.join("s"), "is closed").await;
    assert_eq!(entries_of(&dir).await, ["q"]);
    assert_eq!(holds(&dir, "q").await, b"q0");
}

/// A move that fails, but that moved the file all the same (here another handle moves it in
/// its place): the rename does not reopen the file at its source, where Safari would create it
/// again, empty, and the file is dead.
#[wasm_bindgen_test]
async fn a_move_that_fails_and_moved_the_file_leaves_it_dead_without_reopening_its_source() {
    let dir = fresh("move-moved").await;
    let mut file = OpfsFiles
        .open_locked(&dir.join("u"))
        .await
        .unwrap()
        .unwrap();
    file.write_at(0, b"u0".to_vec()).await.unwrap();
    let held = Held::default();
    arm("rename.closed", "u", hold(&dir, "u", &held));
    let (release, path) = (Rc::clone(&held), dir.clone());
    arm("rename.moved", "w", async move {
        let_go(&release);
        let dir = handle_of(&path).await;
        let entry = file_handle(&dir, "u", false).await.unwrap();
        let entry: &MovableFile = entry.unchecked_ref();
        JsFuture::from(entry.move_to(&dir, "w").unwrap())
            .await
            .unwrap();
    });
    let err = file.rename(&dir.join("w")).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("a move that failed left it no entry"),
        "{err}"
    );
    expect_dead(&mut file, &dir.join("x"), "is closed").await;
    assert_eq!(entries_of(&dir).await, ["w"]);
    assert_eq!(holds(&dir, "w").await, b"u0");
}

/// `create_locked` checks that the name has no generation and then creates it: a file that
/// another handle takes between the two is `None`, and is left created. With the file there, it
/// is `AlreadyExists`.
#[wasm_bindgen_test]
async fn create_locked_is_none_for_a_file_taken_once_it_found_the_name_free() {
    let dir = fresh("create-locked").await;
    let path = dir.join("n");
    let held = Held::default();
    arm("open", "n", hold(&dir, "n", &held));
    assert!(OpfsFiles.create_locked(&path).await.unwrap().is_none());
    assert!(held.borrow().is_some());
    let_go(&held);
    assert_eq!(entries_of(&dir).await, ["n"]);
    let err = OpfsFiles.create_locked(&path).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
}

/// The disk engine on the layer, with a rotation whose fresh WAL `create_locked` leaves created
/// and cannot open (another handle takes it as it creates it): the rotation is rolled back, which
/// moves the rotated WAL to the next generation of the fresh one, and cannot remove the one that
/// is taken, so it stays until the next replacement of the name. The compaction is `RoomLocked`,
/// the room stays usable, with its writes in the WAL it rolled back, which the recovery reads
/// when the room reopens, and the next compaction removes the old generation.
#[wasm_bindgen_test]
async fn a_rotation_rolled_back_onto_a_wal_left_created_writes_to_its_next_generation() {
    use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
    use zemdb_core::{
        CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation,
        TableSchema, Value,
    };
    let schema = || {
        let users = TableSchema::builder("users")
            .table_id(0)
            .primary_key("id", DataType::Int)
            .column("name", DataType::String);
        Schema::builder().table(users).build()
    };
    let insert = |seq: u64| {
        let row = CompactRow::new(vec![Value::Int(seq as i64), Value::String("u".into())]);
        let op = Operation::insert(0, PrimaryKey::single(seq as i64), row, 100);
        vec![SequencedOperation::new(seq, op)]
    };
    let dir = fresh("engine-rollback").await;
    let options = DiskStorageOptions::new(&dir).auto_compact(false);
    let room = RoomId::new("back").unwrap();
    let engine = DiskStorageEngine::new(options.clone()).unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    for seq in 1..=3 {
        engine.apply_batch(&room, insert(seq)).await.unwrap();
    }

    let held = Held::default();
    arm("open", "room_back.wal", hold(&dir, "room_back.wal", &held));
    let compacted = engine.compact_room(&room).await;
    assert!(
        matches!(&compacted, Err(StorageError::RoomLocked(_))),
        "{compacted:?}"
    );
    let_go(&held);
    assert_eq!(
        entries_of(&dir).await,
        [
            "room_back.lock",
            "room_back.snap",
            "room_back.wal",
            "room_back.wal~1"
        ]
    );
    assert!(holds(&dir, "room_back.wal").await.is_empty());
    engine.apply_batch(&room, insert(4)).await.unwrap();
    engine.close_room(&room).await.unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 4);
    engine.compact_room(&room).await.unwrap();
    assert_eq!(
        entries_of(&dir).await,
        ["room_back.lock", "room_back.snap~1", "room_back.wal"]
    );
    engine.close_room(&room).await.unwrap();
    let engine = DiskStorageEngine::new(options).unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 4);
    engine.apply_batch(&room, insert(5)).await.unwrap();
}

/// The disk engine on the layer, with a rotation of its WAL whose reopening fails: the WAL is
/// dead. The compaction fails and readers still see every row; the next write fails on the dead
/// WAL and marks the room as failed; closing it gives the error of its sync; and the room reopens
/// with every row, from `.wal.compacting`, and writes and compacts.
#[wasm_bindgen_test]
async fn a_rotation_of_the_engine_that_cannot_reopen_the_wal_loses_nothing() {
    use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
    use zemdb_core::{
        CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation,
        TableSchema, Value,
    };
    let schema = || {
        let users = TableSchema::builder("users")
            .table_id(0)
            .primary_key("id", DataType::Int)
            .column("name", DataType::String);
        Schema::builder().table(users).build()
    };
    let insert = |seq: u64| {
        let row = CompactRow::new(vec![Value::Int(seq as i64), Value::String("u".into())]);
        let op = Operation::insert(0, PrimaryKey::single(seq as i64), row, 100);
        vec![SequencedOperation::new(seq, op)]
    };
    let dir = fresh("engine-dead").await;
    let options = DiskStorageOptions::new(&dir).auto_compact(false);
    let room = RoomId::new("dead").unwrap();
    let engine = DiskStorageEngine::new(options.clone()).unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    for seq in 1..=5 {
        engine.apply_batch(&room, insert(seq)).await.unwrap();
    }

    let held = Held::default();
    let compacting = "room_dead.wal.compacting";
    arm("rename.moved", compacting, hold(&dir, compacting, &held));
    let compacted = engine.compact_room(&room).await;
    assert!(
        matches!(&compacted, Err(StorageError::Io(err)) if !FileTaken::is(err)),
        "{compacted:?}"
    );
    let_go(&held);
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 5);
    let written = engine.apply_batch(&room, insert(6)).await;
    assert!(matches!(written, Err(StorageError::Io(_))), "{written:?}");
    let refused = engine.apply_batch(&room, insert(6)).await;
    assert!(
        matches!(refused, Err(StorageError::RoomFailed { .. })),
        "{refused:?}"
    );
    let closed = engine.close_room(&room).await;
    assert!(matches!(closed, Err(StorageError::Io(_))), "{closed:?}");
    drop(engine);
    // The dead WAL holds no handle and wrote nowhere: the files are the lock, the snapshot of a
    // new room and the rotated WAL.
    assert_eq!(
        entries_of(&dir).await,
        ["room_dead.lock", "room_dead.snap", compacting]
    );

    let engine = DiskStorageEngine::new(options.clone()).unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 5);
    for seq in 6..=7 {
        engine.apply_batch(&room, insert(seq)).await.unwrap();
    }
    engine.compact_room(&room).await.unwrap();
    engine.close_room(&room).await.unwrap();
    let engine = DiskStorageEngine::new(options).unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 7);
    for key in 1..=7 {
        let row = engine.get(&room, "users", &PrimaryKey::single(key)).await;
        assert!(row.unwrap().is_some(), "row {key} lost");
    }
}
