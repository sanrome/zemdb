mod common;

use common::*;
use std::sync::Arc;
use zemdb_core::{RoomId, SequenceNumber, Value};
use zemdb_storage::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};

#[tokio::test]
async fn test_disk_zstd_snapshot_and_restore() {
    let temp_dir = tempfile::tempdir().unwrap();
    let engine = DiskStorageEngine::new(DiskStorageOptions::new(temp_dir.path())).unwrap();
    let room_a = RoomId::new("room-snap-a").unwrap();
    let room_b = RoomId::new("room-snap-b").unwrap();

    engine.open_room(&room_a, users_schema()).await.unwrap();
    engine
        .apply_batch(&room_a, inserts(1..=50, 100))
        .await
        .unwrap();

    let snapshot_bytes = engine.create_snapshot(&room_a).await.unwrap();
    assert!(!snapshot_bytes.is_empty());

    engine.open_room(&room_b, users_schema()).await.unwrap();
    let restored_head = engine
        .apply_snapshot(&room_b, users_schema(), snapshot_bytes.into(), 50u64.into())
        .await
        .unwrap();
    assert_eq!(restored_head, SequenceNumber::from(50u64));
    assert_eq!(get_user(&engine, &room_b, 10).await, Some(user_row(10)));
}

#[tokio::test]
async fn test_disk_compaction_and_wal_truncation() {
    let temp_dir = tempfile::tempdir().unwrap();
    // A small threshold, so that the updates below trigger an automatic compaction.
    let options = DiskStorageOptions::new(temp_dir.path())
        .min_compaction_bytes(200)
        .compaction_ratio(1.5);
    let engine = DiskStorageEngine::new(options).unwrap();
    let room_id = RoomId::new("room-compact").unwrap();

    engine.open_room(&room_id, users_schema()).await.unwrap();
    let initial = row(1, "Initial", 10, true);
    engine
        .apply_batch(&room_id, vec![insert(1, 1, initial, 100)])
        .await
        .unwrap();
    engine.compact_room(&room_id).await.unwrap();

    // Repeated updates of row 1 grow the WAL past the compaction threshold.
    for i in 2..=30u64 {
        let update_op = update(i, 1, 2, Value::Int((i * 10) as i64), 100 + i);
        engine.apply_batch(&room_id, vec![update_op]).await.unwrap();
    }

    let current_row = get_user(&engine, &room_id, 1).await.unwrap();
    assert_eq!(current_row[2], Value::Int(300));
    assert_eq!(
        engine.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(30u64)
    );
}

#[tokio::test]
async fn test_disk_concurrent_readers_and_writers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let engine = Arc::new(DiskStorageEngine::new(options).unwrap());
    let room_id = RoomId::new("room-concurrent-disk").unwrap();

    engine.open_room(&room_id, users_schema()).await.unwrap();
    // Row 1 exists before the readers start, so every read must find it.
    engine
        .apply_batch(&room_id, inserts(1..=1, 100))
        .await
        .unwrap();

    let engine_writer = Arc::clone(&engine);
    let r_writer = room_id.clone();
    let writer_handle = tokio::spawn(async move {
        for i in 2..=15i64 {
            engine_writer
                .apply_batch(&r_writer, inserts(i..=i, 100))
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
    });

    let engine_reader = Arc::clone(&engine);
    let r_reader = room_id.clone();
    let reader_handle = tokio::spawn(async move {
        for _ in 0..15 {
            let row = get_user(engine_reader.as_ref(), &r_reader, 1)
                .await
                .expect("row 1 is committed before the readers start");
            assert_eq!(row, user_row(1));
            tokio::task::yield_now().await;
        }
    });

    writer_handle.await.unwrap();
    reader_handle.await.unwrap();

    assert_eq!(
        engine.get_head_seq(&room_id).await.unwrap(),
        SequenceNumber::from(15u64)
    );
    assert_eq!(
        get_user(engine.as_ref(), &room_id, 15).await,
        Some(user_row(15))
    );
}

#[tokio::test]
async fn test_compaction_unique_tmp_paths() {
    let temp_dir = tempfile::tempdir().unwrap();
    let options = DiskStorageOptions::new(temp_dir.path());
    let room_id = RoomId::new("room-unique-tmp").unwrap();
    let engine = open_with_ops(&options, &room_id, inserts(1..=5, 100)).await;

    for _ in 0..5 {
        engine.compact_room(&room_id).await.unwrap();
    }

    // No temporary file of any kind is left behind.
    let leaked_tmp = std::fs::read_dir(temp_dir.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp.")
    });
    assert!(!leaked_tmp, "Temporary snapshot files must be cleaned up");
}

#[tokio::test]
async fn test_crash_recovery_during_compaction_window_after_snapshot_rename() {
    let temp_dir = tempfile::tempdir().unwrap();
    let room_id = RoomId::new("room-crash-after-snap-rename").unwrap();
    // The snapshot has already been renamed into place at seq 10, but `.wal.compacting`
    // (holding ops 1..=10) was not yet deleted before the crash, and the active WAL holds new
    // concurrent writes (11..=15).
    let engine =
        seed_interrupted_compaction(temp_dir.path(), &room_id, 1..=10, 1..=10, 11..=15).await;
    let compacting_path = compacting_wal_path(&engine, &room_id);

    let engine_rec = DiskStorageEngine::new(DiskStorageOptions::new(temp_dir.path())).unwrap();
    engine_rec
        .open_room(&room_id, users_schema())
        .await
        .unwrap();

    // `.wal.compacting` was detected as already folded into the snapshot and cleaned up.
    assert!(!compacting_path.exists(), "wal.compacting must be deleted");
    // Every row is there, without duplication or corruption.
    assert_users_up_to(&engine_rec, &room_id, 15).await;
}

#[tokio::test]
async fn test_crash_recovery_during_compaction_window_before_snapshot_rename() {
    let temp_dir = tempfile::tempdir().unwrap();
    let room_id = RoomId::new("room-crash-before-snap-rename").unwrap();
    // A crash during phase 2: the base snapshot is at seq 5, the compaction rotated the WAL into
    // `.wal.compacting` (rows 6..=10) and left an unfinished temporary snapshot file, and the
    // active WAL holds the later writes 11..=15.
    let engine =
        seed_interrupted_compaction(temp_dir.path(), &room_id, 1..=5, 6..=10, 11..=15).await;
    let compacting_path = compacting_wal_path(&engine, &room_id);
    let tmp_snap_path = engine
        .snap_file_path(&room_id)
        .with_extension("snap.tmp.deadbeef-1234");
    std::fs::write(&tmp_snap_path, b"incomplete-snapshot-bytes").unwrap();

    let engine_rec = DiskStorageEngine::new(DiskStorageOptions::new(temp_dir.path())).unwrap();
    engine_rec
        .open_room(&room_id, users_schema())
        .await
        .unwrap();

    assert!(
        !tmp_snap_path.exists(),
        "Orphan tmp snapshot file must be removed"
    );
    // `.wal.compacting` was merged into the active WAL and deleted.
    assert!(!compacting_path.exists(), "wal.compacting must be deleted");
    assert_users_up_to(&engine_rec, &room_id, 15).await;
}

#[test]
fn engine_options_that_cannot_work_are_rejected() {
    // The engine does no I/O until a room is opened.
    let options = || DiskStorageOptions::new("unused");
    for ratio in [f64::NAN, 0.0, -1.0, f64::INFINITY] {
        let result = DiskStorageEngine::new(options().compaction_ratio(ratio));
        assert!(
            matches!(result, Err(StorageError::InvalidOptions(_))),
            "ratio {ratio}"
        );
    }
    let levels = zstd::compression_level_range();
    for level in [levels.start() - 1, levels.end() + 1] {
        let result = DiskStorageEngine::new(options().zstd_level(level));
        assert!(
            matches!(result, Err(StorageError::InvalidOptions(_))),
            "level {level}"
        );
    }
    assert!(DiskStorageEngine::new(options()).is_ok());
    assert!(DiskStorageEngine::new(options().compaction_ratio(0.5).zstd_level(19)).is_ok());
}
