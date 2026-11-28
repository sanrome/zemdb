//! The durable metadata of the client: in files, through the file layer, and in memory.
#![allow(clippy::disallowed_methods)]

mod common;

use common::*;
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;
use zemdb_client::{FileMetadata, MemoryMetadata, MetadataStore, RoomMetadata};
use zemdb_core::{RoomId, SchemaId, SequencedOperation};
use zemdb_storage::{DiskStorageEngine, DiskStorageOptions, StorageEngine};

fn room() -> RoomId {
    RoomId::new("room-a").unwrap()
}

fn metadata(room_incarnation: u128) -> RoomMetadata {
    RoomMetadata {
        room_incarnation,
        schema_id: SchemaId::new("tasks").unwrap(),
        schema: tasks_schema(),
    }
}

/// The file of `room` in `dir`, as the store names it.
fn room_file(dir: &Path) -> std::path::PathBuf {
    dir.join("room_room-a.meta")
}

/// Every copy of `bytes` with one bit flipped, and every prefix of it.
fn damaged(bytes: &[u8]) -> impl Iterator<Item = Vec<u8>> + '_ {
    let flipped = (0..bytes.len()).map(|i| {
        let mut copy = bytes.to_vec();
        copy[i] ^= 1 << (i % 8);
        copy
    });
    flipped.chain((0..bytes.len()).map(|len| bytes[..len].to_vec()))
}

#[tokio::test]
async fn room_metadata_survive_reopening_and_each_save_replaces_them() {
    let dir = tempdir().unwrap();
    let store = FileMetadata::new(dir.path());
    store.open().await.unwrap();
    assert_eq!(store.room(&room()).await.unwrap(), None);
    store
        .save_room(&room(), &metadata(u128::MAX - 3))
        .await
        .unwrap();
    assert_eq!(
        store.room(&room()).await.unwrap(),
        Some(metadata(u128::MAX - 3))
    );
    store.save_room(&room(), &metadata(7)).await.unwrap();
    store.close().await;

    let reopened = FileMetadata::new(dir.path());
    reopened.open().await.unwrap();
    assert_eq!(reopened.room(&room()).await.unwrap(), Some(metadata(7)));
    let other = RoomId::new("room-b").unwrap();
    assert_eq!(reopened.room(&other).await.unwrap(), None);
}

#[tokio::test]
async fn a_file_is_replaced_by_a_synced_temporary_file_renamed_with_no_handle_open() {
    let dir = tempdir().unwrap();
    let files = TestFiles::default();
    let store = FileMetadata::with_files(dir.path(), files.clone());
    store.open().await.unwrap();
    store.save_room(&room(), &metadata(5)).await.unwrap();
    store.close().await;

    let parent_name = dir_name(dir.path().parent().unwrap());
    let dir_name = dir_name(dir.path());
    let replace = |name: &str| {
        [
            format!("create {name}.tmp"),
            format!("write_at {name}.tmp"),
            format!("sync_all {name}.tmp"),
            format!("close {name}.tmp"),
            format!("rename {name}.tmp {name}"),
            format!("sync_dir {dir_name}"),
        ]
    };
    let mut expected = vec![
        format!("create_dir_all {dir_name}"),
        "open_locked client.lock".to_string(),
        format!("sync_dir {dir_name}"),
        format!("sync_dir {parent_name}"),
        "open_read client.meta".to_string(),
    ];
    expected.extend(replace("client.meta"));
    expected.extend(replace("room_room-a.meta"));
    expected.push("close client.lock".to_string());
    assert_eq!(files.log(), expected);
}

#[tokio::test]
async fn memory_metadata_keep_the_rooms_and_have_a_device_id_of_their_own() {
    let store = MemoryMetadata::default();
    let device_id = store.open().await.unwrap();
    assert_eq!(store.room(&room()).await.unwrap(), None);
    store.save_room(&room(), &metadata(3)).await.unwrap();
    assert_eq!(store.room(&room()).await.unwrap(), Some(metadata(3)));
    store.remove_room(&room()).await.unwrap();
    assert_eq!(store.room(&room()).await.unwrap(), None);
    store.remove_room(&room()).await.unwrap();
    assert_ne!(MemoryMetadata::default().open().await.unwrap(), device_id);
}

#[tokio::test]
async fn damaged_room_metadata_count_as_missing() {
    let dir = tempdir().unwrap();
    let store = FileMetadata::new(dir.path());
    store.save_room(&room(), &metadata(5)).await.unwrap();
    let bytes = std::fs::read(room_file(dir.path())).unwrap();
    for damaged in damaged(&bytes) {
        std::fs::write(room_file(dir.path()), &damaged).unwrap();
        let read = store.room(&room()).await.unwrap();
        assert_eq!(read, None, "{}", String::from_utf8_lossy(&damaged));
    }
}

#[tokio::test]
async fn a_damaged_device_id_counts_as_missing_and_is_replaced() {
    let dir = tempdir().unwrap();
    let first = FileMetadata::new(dir.path()).open().await.unwrap();
    let path = dir.path().join("client.meta");
    let bytes = std::fs::read(&path).unwrap();
    let mut last = first;
    for damaged in damaged(&bytes) {
        std::fs::write(&path, &damaged).unwrap();
        last = FileMetadata::new(dir.path()).open().await.unwrap();
        assert_ne!(last, first, "{}", String::from_utf8_lossy(&damaged));
    }
    assert_eq!(FileMetadata::new(dir.path()).open().await.unwrap(), last);
}

#[tokio::test]
async fn a_temporary_file_left_by_a_cut_is_not_read() {
    let (dir, other) = (tempdir().unwrap(), tempdir().unwrap());
    // What a save cut right before its rename leaves: the new content in the temporary file.
    let cut_room_save = || {
        let tmp = room_file(dir.path()).with_extension("meta.tmp");
        std::fs::copy(room_file(other.path()), tmp).unwrap();
    };
    FileMetadata::new(other.path())
        .save_room(&room(), &metadata(9))
        .await
        .unwrap();
    let store = FileMetadata::new(dir.path());
    cut_room_save();
    assert_eq!(store.room(&room()).await.unwrap(), None);
    store.save_room(&room(), &metadata(5)).await.unwrap();
    cut_room_save();
    assert_eq!(store.room(&room()).await.unwrap(), Some(metadata(5)));

    // The same for the first device id.
    let device_id = FileMetadata::new(other.path()).open().await.unwrap();
    std::fs::copy(
        other.path().join("client.meta"),
        dir.path().join("client.meta.tmp"),
    )
    .unwrap();
    assert_ne!(store.open().await.unwrap(), device_id);
}

#[tokio::test]
async fn metadata_of_another_kind_count_as_missing() {
    let dir = tempdir().unwrap();
    let store = FileMetadata::new(dir.path());
    let device_id = store.open().await.unwrap();
    store.save_room(&room(), &metadata(5)).await.unwrap();
    store.close().await;
    let device_file = dir.path().join("client.meta");
    let device_bytes = std::fs::read(&device_file).unwrap();
    let room_bytes = std::fs::read(room_file(dir.path())).unwrap();

    // Each file with the content of the other, which verifies.
    std::fs::write(room_file(dir.path()), &device_bytes).unwrap();
    assert_eq!(store.room(&room()).await.unwrap(), None);
    std::fs::write(&device_file, &room_bytes).unwrap();
    assert_ne!(store.open().await.unwrap(), device_id);

    // A schema id that does not validate, under a CRC32 that does.
    let checked = |json: &str| format!("{json}\ncrc32:{:08x}\n", crc32fast::hash(json.as_bytes()));
    let text = String::from_utf8(room_bytes).unwrap();
    let json = text.split_once("\ncrc32:").unwrap().0;
    std::fs::write(room_file(dir.path()), checked(json)).unwrap();
    assert_eq!(store.room(&room()).await.unwrap(), Some(metadata(5)));
    let invalid = json.replace(r#""schema_id":"tasks""#, r#""schema_id":"Tasks""#);
    assert_ne!(invalid, json);
    std::fs::write(room_file(dir.path()), checked(&invalid)).unwrap();
    assert_eq!(store.room(&room()).await.unwrap(), None);
}

#[tokio::test]
async fn an_error_in_any_step_of_reading_or_saving_room_metadata_is_returned() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    let store = FileMetadata::new(&data);
    store.open().await.unwrap();
    store.save_room(&room(), &metadata(5)).await.unwrap();
    for operation in ["open_read", "size", "read_at"] {
        let flaky = TestFiles::failing(operation, "room_room-a.meta");
        let flaky = FileMetadata::with_files(&data, flaky);
        assert!(flaky.room(&room()).await.is_err(), "{operation}");
    }
    // Until the rename the previous metadata stay; after it, the new ones are in place.
    let steps = [
        ("create", "room_room-a.meta.tmp", 5),
        ("write_at", "room_room-a.meta.tmp", 5),
        ("sync_all", "room_room-a.meta.tmp", 5),
        ("rename", "room_room-a.meta", 5),
        ("sync_dir", "data", 6),
    ];
    for (operation, name, kept) in steps {
        store.save_room(&room(), &metadata(5)).await.unwrap();
        let flaky = FileMetadata::with_files(&data, TestFiles::failing(operation, name));
        assert!(
            flaky.save_room(&room(), &metadata(6)).await.is_err(),
            "{operation}"
        );
        let read = store.room(&room()).await.unwrap();
        assert_eq!(read, Some(metadata(kept)), "{operation}");
    }
}

#[tokio::test]
async fn removing_room_metadata_removes_them_and_a_temporary_file_durably() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    let files = TestFiles::default();
    let store = FileMetadata::with_files(&data, files.clone());
    store.open().await.unwrap();
    store.save_room(&room(), &metadata(5)).await.unwrap();
    let tmp = data.join("room_room-a.meta.tmp");
    std::fs::write(&tmp, "cut").unwrap();
    let before = files.log().len();
    store.remove_room(&room()).await.unwrap();
    let removal = [
        "remove room_room-a.meta",
        "remove room_room-a.meta.tmp",
        "sync_dir data",
    ];
    assert_eq!(files.log()[before..], removal);
    assert_eq!(store.room(&room()).await.unwrap(), None);
    assert!(!tmp.exists());
    // Removing missing metadata succeeds, and syncs the directory all the same.
    store.remove_room(&room()).await.unwrap();
    assert_eq!(
        files.log().last().map(String::as_str),
        Some("sync_dir data")
    );

    // An error in any step is returned.
    for (operation, name) in removal.map(|entry| entry.split_once(' ').unwrap()) {
        store.save_room(&room(), &metadata(5)).await.unwrap();
        let flaky = FileMetadata::with_files(&data, TestFiles::failing(operation, name));
        assert!(
            flaky.remove_room(&room()).await.is_err(),
            "{operation} {name}"
        );
    }
}

#[tokio::test]
async fn an_error_in_any_step_of_an_open_is_returned_and_keeps_the_device_id() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    let device_id = FileMetadata::new(&data).open().await.unwrap();
    let reading = [
        ("create_dir_all", "data"),
        ("open_locked", "client.lock"),
        ("open_read", "client.meta"),
        ("size", "client.meta"),
        ("read_at", "client.meta"),
    ];
    for (operation, name) in reading {
        let flaky = FileMetadata::with_files(&data, TestFiles::failing(operation, name));
        assert!(flaky.open().await.is_err(), "{operation}");
        let reopened = FileMetadata::new(&data).open().await.unwrap();
        assert_eq!(reopened, device_id, "{operation}");
    }
    // The first open, which saves a new device id.
    let saving = [
        ("create", "client.meta.tmp"),
        ("write_at", "client.meta.tmp"),
        ("sync_all", "client.meta.tmp"),
        ("rename", "client.meta"),
        ("sync_dir", "data"),
    ];
    for (operation, name) in saving {
        let dir = tempdir().unwrap();
        let flaky = TestFiles::failing(operation, name);
        let flaky = FileMetadata::with_files(dir.path().join("data"), flaky);
        assert!(flaky.open().await.is_err(), "{operation}");
    }
}

#[tokio::test]
async fn an_open_cut_by_its_caller_holds_the_directory_until_its_device_id_is_saved() {
    let dir = tempdir().unwrap();
    let files = TestFiles::default();
    files.slow(Duration::from_millis(500));
    let slow = FileMetadata::with_files(dir.path(), files.clone());
    tokio::select! {
        _ = slow.open() => panic!("the write is slow"),
        () = files.logged("write_at client.meta.tmp") => {}
    }
    drop(slow);
    assert!(FileMetadata::new(dir.path()).open().await.is_err());
    let reopened = async {
        loop {
            if let Ok(device_id) = FileMetadata::new(dir.path()).open().await {
                break device_id;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let device_id = timeout(Duration::from_secs(10), reopened).await.unwrap();
    assert_eq!(
        FileMetadata::new(dir.path()).open().await.unwrap(),
        device_id
    );
}

#[tokio::test]
async fn a_save_cut_by_its_caller_ends_before_the_next_operation() {
    let dir = tempdir().unwrap();
    let files = TestFiles::default();
    files.slow(Duration::from_millis(300));
    let store = FileMetadata::with_files(dir.path(), files.clone());
    let (room_id, cut) = (room(), metadata(5));
    tokio::select! {
        _ = store.save_room(&room_id, &cut) => panic!("the write is slow"),
        () = files.logged("write_at room_room-a.meta.tmp") => {}
    }
    store.save_room(&room(), &metadata(6)).await.unwrap();
    // Where the cut write would have landed.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(store.room(&room()).await.unwrap(), Some(metadata(6)));
}

#[tokio::test]
async fn a_close_waits_for_a_cut_save_and_then_releases_the_directory() {
    let dir = tempdir().unwrap();
    // With the device id saved, the first write of the store is that of the save.
    FileMetadata::new(dir.path()).open().await.unwrap();
    let files = TestFiles::default();
    files.slow(Duration::from_millis(300));
    let store = FileMetadata::with_files(dir.path(), files.clone());
    store.open().await.unwrap();
    let (room_id, cut) = (room(), metadata(5));
    tokio::select! {
        _ = store.save_room(&room_id, &cut) => panic!("the write is slow"),
        () = files.logged("write_at room_room-a.meta.tmp") => {}
    }
    store.close().await;
    let other = FileMetadata::new(dir.path());
    other.open().await.unwrap();
    assert_eq!(other.room(&room()).await.unwrap(), Some(metadata(5)));
}

#[tokio::test]
async fn a_second_store_over_the_same_directory_fails_until_the_first_closes() {
    let dir = tempdir().unwrap();
    let first = FileMetadata::new(dir.path());
    let device_id = first.open().await.unwrap();
    assert!(FileMetadata::new(dir.path()).open().await.is_err());
    first.close().await;
    assert_eq!(
        FileMetadata::new(dir.path()).open().await.unwrap(),
        device_id
    );
}

#[tokio::test]
async fn metadata_share_the_directory_of_a_disk_engine_without_touching_its_files() {
    let dir = tempdir().unwrap();
    let store = FileMetadata::new(dir.path());
    store.open().await.unwrap();
    store.save_room(&room(), &metadata(5)).await.unwrap();

    let engine = DiskStorageEngine::new(DiskStorageOptions::new(dir.path())).unwrap();
    engine.open_room(&room(), tasks_schema()).await.unwrap();
    let op = SequencedOperation::new(1, insert_task(1));
    engine.apply_batch(&room(), vec![op]).await.unwrap();
    engine.close_room(&room()).await.unwrap();
    engine.delete_room(&room()).await.unwrap();
    engine.open_room(&room(), tasks_schema()).await.unwrap();
    assert_eq!(engine.get_head_seq(&room()).await.unwrap().get(), 0);
    assert_eq!(store.room(&room()).await.unwrap(), Some(metadata(5)));
}

// ---------------------------------------------------------------------------------------------
// Durable before they are served

/// The steps of a save of the metadata of `room` in the directory `data`, by the operation of
/// the file layer and the file it fails on.
const SAVE_STEPS: [(&str, &str); 5] = [
    ("create", "room_room-a.meta.tmp"),
    ("write_at", "room_room-a.meta.tmp"),
    ("sync_all", "room_room-a.meta.tmp"),
    ("rename", "room_room-a.meta"),
    ("sync_dir", "data"),
];

/// The steps of a removal of the metadata of `room` in the directory `data`, as [`SAVE_STEPS`].
const REMOVAL_STEPS: [(&str, &str); 3] = [
    ("remove", "room_room-a.meta"),
    ("remove", "room_room-a.meta.tmp"),
    ("sync_dir", "data"),
];

/// Each step of a save or of a removal, with whether it is of a removal.
fn failing_steps() -> impl Iterator<Item = (bool, (&'static str, &'static str))> {
    let saves = SAVE_STEPS.into_iter().map(|step| (false, step));
    saves.chain(REMOVAL_STEPS.into_iter().map(|step| (true, step)))
}

/// What a read of the metadata of `room` logs: their file opened, and closed if it is there.
fn read_log(found: bool) -> Vec<String> {
    let mut log = vec!["open_read room_room-a.meta".to_string()];
    if found {
        log.push("close room_room-a.meta".to_string());
    }
    log
}

/// What a save of the metadata of `room` in the directory `data` logs.
fn save_log() -> Vec<String> {
    let log = [
        "create room_room-a.meta.tmp",
        "write_at room_room-a.meta.tmp",
        "sync_all room_room-a.meta.tmp",
        "close room_room-a.meta.tmp",
        "rename room_room-a.meta.tmp room_room-a.meta",
        "sync_dir data",
    ];
    log.map(String::from).to_vec()
}

/// What a removal of the metadata of `room` in the directory `data` logs.
fn removal_log() -> Vec<String> {
    let log = REMOVAL_STEPS.map(|(operation, name)| format!("{operation} {name}"));
    log.to_vec()
}

/// A store over the directory `data` of `dir` through `files`, open, with the metadata of `room`
/// saved (the incarnation 5), and the temporary file of a cut save next to them.
async fn saved_store(dir: &Path, files: &TestFiles) -> FileMetadata<TestFiles> {
    let store = FileMetadata::with_files(dir.join("data"), files.clone());
    store.open().await.unwrap();
    store.save_room(&room(), &metadata(5)).await.unwrap();
    std::fs::write(dir.join("data/room_room-a.meta.tmp"), "cut").unwrap();
    store
}

/// Makes the step `step` of a save of the metadata of `room` (the incarnation 6), or of their
/// removal, fail once.
async fn fail(
    store: &FileMetadata<TestFiles>,
    files: &TestFiles,
    removal: bool,
    step: (&'static str, &'static str),
) {
    files.fail_next(step.0, step.1);
    let failed = match removal {
        false => store.save_room(&room(), &metadata(6)).await,
        true => store.remove_room(&room()).await,
    };
    assert!(failed.is_err(), "{removal} {step:?}");
}

/// What `store` serves of `room`, and what it logs to serve it.
async fn served(
    store: &FileMetadata<TestFiles>,
    files: &TestFiles,
) -> (Option<RoomMetadata>, Vec<String>) {
    let before = files.log().len();
    let read = store.room(&room()).await.unwrap();
    (read, files.log()[before..].to_vec())
}

/// What the metadata of `room` hold after the step `step` of a save of the incarnation 6 over
/// the incarnation 5, or of their removal, failed: until the rename the previous metadata stay,
/// and after it the new ones are in place; a removal removed them from its second step on.
fn after_failing(removal: bool, step: (&str, &str)) -> Option<RoomMetadata> {
    match (removal, step) {
        (false, ("sync_dir", _)) => Some(metadata(6)),
        (false, _) | (true, ("remove", "room_room-a.meta")) => Some(metadata(5)),
        (true, _) => None,
    }
}

#[tokio::test]
async fn after_a_save_or_a_removal_that_failed_at_any_step_the_directory_is_synced_first() {
    for (removal, step) in failing_steps() {
        let dir = tempdir().unwrap();
        let files = TestFiles::default();
        let store = saved_store(dir.path(), &files).await;
        // Without a failure, nothing is synced: the open synced the directory.
        let (read, log) = served(&store, &files).await;
        assert_eq!((read, log), (Some(metadata(5)), read_log(true)), "{step:?}");

        fail(&store, &files, removal, step).await;
        let kept = after_failing(removal, step);
        let (read, log) = served(&store, &files).await;
        let mut synced = vec!["sync_dir data".to_string()];
        synced.extend(read_log(kept.is_some()));
        assert_eq!((&read, log), (&kept, synced), "{removal} {step:?}");
        // Once served, they are durable: the next read syncs nothing.
        let (read, log) = served(&store, &files).await;
        let expected = (kept.clone(), read_log(kept.is_some()));
        assert_eq!((read, log), expected, "{removal} {step:?}");
    }
}

#[tokio::test]
async fn a_save_or_a_removal_that_succeeds_after_one_that_failed_needs_nothing_more() {
    let covering = [
        ("native", TestFiles::default()),
        ("opfs", TestFiles::with_the_sync_dir_of_the_opfs()),
    ];
    for (layer, files) in covering {
        for removal in [false, true] {
            let dir = tempdir().unwrap();
            let store = saved_store(dir.path(), &files).await;
            fail(&store, &files, removal, ("sync_dir", "data")).await;
            // The operation that fails, done again, succeeds: what it left is durable.
            match removal {
                false => store.save_room(&room(), &metadata(6)).await.unwrap(),
                true => store.remove_room(&room()).await.unwrap(),
            }
            let kept = after_failing(removal, ("sync_dir", "data"));
            let (read, log) = served(&store, &files).await;
            let expected = (kept.clone(), read_log(kept.is_some()));
            assert_eq!((read, log), expected, "{layer} {removal}");
        }
    }
}

#[tokio::test]
async fn a_sync_that_fails_before_the_room_is_served_fails_the_read_until_one_succeeds() {
    for removal in [false, true] {
        let dir = tempdir().unwrap();
        let files = TestFiles::default();
        let store = saved_store(dir.path(), &files).await;
        fail(&store, &files, removal, ("sync_dir", "data")).await;
        files.fail_next("sync_dir", "data");
        assert!(store.room(&room()).await.is_err(), "{removal}");
        let kept = after_failing(removal, ("sync_dir", "data"));
        let (read, log) = served(&store, &files).await;
        let mut synced = vec!["sync_dir data".to_string()];
        synced.extend(read_log(kept.is_some()));
        assert_eq!((read, log), (kept, synced), "{removal}");
    }
}

#[tokio::test]
async fn a_store_opened_again_syncs_the_directory_before_it_serves_anything() {
    for removal in [false, true] {
        let dir = tempdir().unwrap();
        let files = TestFiles::default();
        let store = saved_store(dir.path(), &files).await;
        fail(&store, &files, removal, ("sync_dir", "data")).await;
        store.close().await;
        let before = files.log().len();
        store.open().await.unwrap();
        let opened = [
            "create_dir_all data",
            "open_locked client.lock",
            "sync_dir data",
            &format!("sync_dir {}", dir_name(dir.path())),
            "open_read client.meta",
            "close client.meta",
        ];
        assert_eq!(files.log()[before..], opened, "{removal}");
        // That sync made the failed change durable, so the store forgot it.
        let kept = after_failing(removal, ("sync_dir", "data"));
        let (read, log) = served(&store, &files).await;
        assert_eq!((read, log), (kept.clone(), read_log(kept.is_some())));
    }
}

#[tokio::test]
async fn an_open_whose_sync_fails_serves_nothing_and_releases_the_directory() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    let device_id = FileMetadata::new(&data).open().await.unwrap();
    let files = TestFiles::failing("sync_dir", "data");
    let flaky = FileMetadata::with_files(&data, files.clone());
    assert!(flaky.open().await.is_err());
    let opened = [
        "create_dir_all data",
        "open_locked client.lock",
        "sync_dir data",
    ];
    assert_eq!(files.log()[..3], opened);
    assert_eq!(files.log()[3..], ["close client.lock"]);
    assert_eq!(FileMetadata::new(&data).open().await.unwrap(), device_id);
}

#[tokio::test]
async fn a_save_cut_by_its_caller_that_fails_in_its_task_is_synced_before_it_is_served() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    // With the device id saved, the first write of the store is that of the save.
    FileMetadata::new(&data).open().await.unwrap();
    let files = TestFiles::default();
    let store = FileMetadata::with_files(&data, files.clone());
    store.open().await.unwrap();
    files.slow(Duration::from_millis(300));
    files.fail_next("sync_dir", "data");
    let (room_id, cut) = (room(), metadata(5));
    tokio::select! {
        _ = store.save_room(&room_id, &cut) => panic!("the write is slow"),
        () = files.logged("write_at room_room-a.meta.tmp") => {}
    }
    // The save goes on in its task, and fails there after its rename.
    let (read, log) = served(&store, &files).await;
    assert_eq!(read, Some(metadata(5)));
    let mut expected = [
        "rename room_room-a.meta.tmp room_room-a.meta",
        "sync_dir data",
    ]
    .map(String::from)
    .to_vec();
    expected.push("sync_dir data".to_string());
    expected.extend(read_log(true));
    assert!(log.ends_with(&expected), "{log:#?}");
}

#[tokio::test]
async fn an_open_syncs_the_directory_and_then_its_parent_or_fails() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    let parent = format!("sync_dir {}", dir_name(dir.path()));
    let files = TestFiles::default();
    let store = FileMetadata::with_files(&data, files.clone());
    store.open().await.unwrap();
    let opened = [
        "create_dir_all data",
        "open_locked client.lock",
        "sync_dir data",
        &parent,
        "open_read client.meta",
    ];
    assert_eq!(files.log()[..5], opened);
    store.close().await;
    // Once the directory exists, its creation is not synced again: the open syncs its entry.
    let before = files.log().len();
    store.open().await.unwrap();
    assert_eq!(files.log()[before..][..4], opened[..4]);
    store.close().await;

    // A sync of the parent that fails fails the open, which serves nothing.
    let files = TestFiles::failing("sync_dir", dir_name(dir.path()));
    let flaky = FileMetadata::with_files(&data, files.clone());
    assert!(flaky.open().await.is_err());
    assert_eq!(files.log(), [&opened[..4], &["close client.lock"]].concat());
}

#[tokio::test]
async fn a_save_or_a_removal_whose_task_panics_after_its_rename_leaves_the_room_marked() {
    let layers = [
        ("native", TestFiles::default()),
        ("opfs", TestFiles::with_the_sync_dir_of_the_opfs()),
    ];
    for (layer, files) in layers {
        for removal in [false, true] {
            let dir = tempdir().unwrap();
            let store = saved_store(dir.path(), &files).await;
            // The last step panics: the save renamed its file, or the removal removed it.
            files.panic_next("sync_dir", "data");
            let failed = match removal {
                false => store.save_room(&room(), &metadata(6)).await,
                true => store.remove_room(&room()).await,
            };
            let panicked = failed.expect_err("the task panicked").to_string();
            assert!(panicked.contains("panicked"), "{panicked}");
            let kept = after_failing(removal, ("sync_dir", "data"));
            let expected = match (layer, removal) {
                ("native", _) => {
                    let mut synced = vec!["sync_dir data".to_string()];
                    synced.extend(read_log(kept.is_some()));
                    synced
                }
                (_, false) => [read_log(true), save_log()].concat(),
                (_, true) => removal_log(),
            };
            let (read, log) = served(&store, &files).await;
            assert_eq!((read, log), (kept, expected), "{layer} {removal}");
        }
    }
}

#[tokio::test]
async fn without_a_sync_that_covers_earlier_changes_the_first_read_of_a_room_saves_it_again() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    let earlier = FileMetadata::new(&data);
    earlier.open().await.unwrap();
    earlier.save_room(&room(), &metadata(5)).await.unwrap();
    earlier.close().await;

    let files = TestFiles::with_the_sync_dir_of_the_opfs();
    let store = FileMetadata::with_files(&data, files.clone());
    store.open().await.unwrap();
    let mut saved_again = read_log(true);
    saved_again.extend(save_log());
    let (read, log) = served(&store, &files).await;
    assert_eq!((read, log), (Some(metadata(5)), saved_again.clone()));
    // The second time, and after a save that succeeded, nothing is saved again.
    let (read, log) = served(&store, &files).await;
    assert_eq!((read, log), (Some(metadata(5)), read_log(true)));
    store.save_room(&room(), &metadata(6)).await.unwrap();
    let (read, log) = served(&store, &files).await;
    assert_eq!((read, log), (Some(metadata(6)), read_log(true)));
    // A room without metadata has nothing to save again.
    let before = files.log().len();
    let other = RoomId::new("room-b").unwrap();
    assert_eq!(store.room(&other).await.unwrap(), None);
    assert_eq!(files.log()[before..], ["open_read room_room-b.meta"]);
    // Once the store opened again, each room is read for the first time again.
    store.close().await;
    store.open().await.unwrap();
    let (read, log) = served(&store, &files).await;
    assert_eq!((read, log), (Some(metadata(6)), saved_again));
}

#[tokio::test]
async fn without_a_sync_that_covers_earlier_changes_metadata_that_do_not_verify_are_not_saved() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    let earlier = FileMetadata::new(&data);
    earlier.open().await.unwrap();
    earlier.save_room(&room(), &metadata(5)).await.unwrap();
    earlier.close().await;
    let bytes = std::fs::read(room_file(&data)).unwrap();
    std::fs::write(room_file(&data), &bytes[..bytes.len() - 1]).unwrap();

    let files = TestFiles::with_the_sync_dir_of_the_opfs();
    let store = FileMetadata::with_files(&data, files.clone());
    store.open().await.unwrap();
    for read in ["first", "second"] {
        let (served_now, log) = served(&store, &files).await;
        assert_eq!((served_now, log), (None, read_log(true)), "{read}");
    }
    assert_eq!(
        std::fs::read(room_file(&data)).unwrap(),
        bytes[..bytes.len() - 1]
    );
}

#[tokio::test]
async fn without_a_sync_that_covers_earlier_changes_a_failed_save_is_saved_again_first() {
    for step in SAVE_STEPS {
        let dir = tempdir().unwrap();
        let files = TestFiles::with_the_sync_dir_of_the_opfs();
        let store = saved_store(dir.path(), &files).await;
        // Saved by the store, they are durable.
        let (read, log) = served(&store, &files).await;
        assert_eq!((read, log), (Some(metadata(5)), read_log(true)), "{step:?}");

        fail(&store, &files, false, step).await;
        let kept = after_failing(false, step);
        let mut saved_again = read_log(true);
        saved_again.extend(save_log());
        let (read, log) = served(&store, &files).await;
        assert_eq!((&read, log), (&kept, saved_again), "{step:?}");
        let (read, log) = served(&store, &files).await;
        assert_eq!((read, log), (kept, read_log(true)), "{step:?}");
    }
}

#[tokio::test]
async fn without_a_sync_that_covers_earlier_changes_a_failed_removal_is_repeated_first() {
    for step in REMOVAL_STEPS {
        let dir = tempdir().unwrap();
        let files = TestFiles::with_the_sync_dir_of_the_opfs();
        let store = saved_store(dir.path(), &files).await;
        fail(&store, &files, true, step).await;
        let (read, log) = served(&store, &files).await;
        assert_eq!((read, log), (None, removal_log()), "{step:?}");
        let data = dir.path().join("data");
        assert!(!room_file(&data).exists(), "{step:?}");
        assert!(!data.join("room_room-a.meta.tmp").exists(), "{step:?}");
        let (read, log) = served(&store, &files).await;
        assert_eq!((read, log), (None, read_log(false)), "{step:?}");

        // With the store closed and opened again, the removal is not repeated: the open forgot
        // it, and what it left is saved again and served the first time.
        let dir = tempdir().unwrap();
        let store = saved_store(dir.path(), &files).await;
        fail(&store, &files, true, step).await;
        store.close().await;
        store.open().await.unwrap();
        let kept = after_failing(true, step);
        let mut first_read = read_log(kept.is_some());
        if kept.is_some() {
            first_read.extend(save_log());
        }
        let (read, log) = served(&store, &files).await;
        assert_eq!((&read, log), (&kept, first_read), "{step:?}");
        let (read, log) = served(&store, &files).await;
        assert_eq!(
            (read, log),
            (kept.clone(), read_log(kept.is_some())),
            "{step:?}"
        );
    }
}

#[tokio::test]
async fn without_a_sync_that_covers_earlier_changes_a_failed_save_again_fails_the_read() {
    // The first time a room is served.
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    let earlier = FileMetadata::new(&data);
    earlier.open().await.unwrap();
    earlier.save_room(&room(), &metadata(5)).await.unwrap();
    earlier.close().await;
    let files = TestFiles::with_the_sync_dir_of_the_opfs();
    let store = FileMetadata::with_files(&data, files.clone());
    store.open().await.unwrap();
    files.fail_next("create", "room_room-a.meta.tmp");
    assert!(store.room(&room()).await.is_err());
    let mut saved_again = read_log(true);
    saved_again.extend(save_log());
    let (read, log) = served(&store, &files).await;
    assert_eq!((read, log), (Some(metadata(5)), saved_again.clone()));

    // After a save that failed.
    fail(&store, &files, false, ("sync_dir", "data")).await;
    files.fail_next("rename", "room_room-a.meta");
    assert!(store.room(&room()).await.is_err());
    let (read, log) = served(&store, &files).await;
    assert_eq!((read, log), (Some(metadata(6)), saved_again));

    // After a removal that failed.
    fail(&store, &files, true, ("remove", "room_room-a.meta")).await;
    files.fail_next("remove", "room_room-a.meta");
    assert!(store.room(&room()).await.is_err());
    assert!(room_file(&data).exists());
    let (read, log) = served(&store, &files).await;
    assert_eq!((read, log), (None, removal_log()));
}
