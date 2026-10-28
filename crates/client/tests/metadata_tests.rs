//! The durable metadata of the client: in files, through the file layer, and in memory.

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

    let dir_name = dir.path().file_name().unwrap().to_str().unwrap();
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
    let files = TestFiles::slow(Duration::from_millis(500));
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
    let files = TestFiles::slow(Duration::from_millis(300));
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
    let files = TestFiles::slow(Duration::from_millis(300));
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
