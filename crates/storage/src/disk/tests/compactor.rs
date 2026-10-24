use super::{absorbing_wal_path, compacting_wal_path};
use crate::disk::format::HEADER_SIZE;
use crate::disk::tests::support::*;
use crate::fail_point;
use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
use zemdb_core::{ColumnUpdate, Operation, PrimaryKey, RoomId, SequenceNumber, Value};

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

    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=10).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;

    let reopened = reopen(engine, dir.path(), &room).await;
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

    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
}

/// A file found where the rotation creates the fresh WAL is never adopted: the rotation fails and
/// is rolled back, and every write before and after it survives.
#[tokio::test]
async fn rotation_never_adopts_a_file_in_place_of_the_fresh_wal() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("rotation-finds-a-file").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);
    apply_range(&engine, &room, 1..=5).await;

    let mut renamed = fail_point::arm_pause("compaction.rotate_renamed", &wal_path);
    let compaction = spawn_compaction(&engine, &room);
    renamed.reached().await;
    std::fs::write(&wal_path, vec![0xFF; 4096]).unwrap();
    renamed.release();
    let result = compaction.await.unwrap();
    assert!(
        matches!(&result, Err(StorageError::Io(err)) if err.kind() == std::io::ErrorKind::AlreadyExists),
        "{result:?}"
    );

    apply_range(&engine, &room, 6..=10).await;
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
}

#[tokio::test]
async fn compaction_absorbs_orphan_from_failed_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("absorb-orphan").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let compacting_path = compacting_wal_path(&engine.wal_file_path(&room));

    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    assert!(compacting_path.exists());
    apply_range(&engine, &room, 6..=10).await;

    engine.compact_room(&room).await.unwrap();

    assert_eq!(snapshot_seq(&engine, &room).await.get(), 10);
    assert!(!compacting_path.exists());
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
}

/// A crash at any point of a compaction that absorbs the active WAL leaves `.wal.compacting`
/// whole, as it was or with the active WAL appended, and the room reopens with every row.
#[cfg(unix)]
#[tokio::test]
async fn crash_at_any_point_of_an_absorption_leaves_the_rotated_wal_whole() {
    let dir = tempfile::tempdir().unwrap();
    let (data, images) = (dir.path().join("data"), dir.path().join("images"));
    let room = RoomId::new("absorb-images").unwrap();
    let engine = open_engine(&data, &room).await;
    let wal_path = engine.wal_file_path(&room);
    let compacting_path = compacting_wal_path(&wal_path);
    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=10).await;
    let before = std::fs::read(&compacting_path).unwrap();
    let absorbed = [before.clone(), std::fs::read(&wal_path).unwrap()].concat();
    // Opened before the compaction: it keeps reading the same file, which is never written in
    // place, however few fail points the writes cross.
    let mut held = std::fs::File::open(&compacting_path).unwrap();

    fail_point::arm_crash_plan(
        &data,
        fail_point::CrashAction::CopyAtEveryPoint(images.clone()),
    );
    engine.compact_room(&room).await.unwrap();
    let mut read = Vec::new();
    std::io::Read::read_to_end(&mut held, &mut read).unwrap();
    assert!(read == before, "`.wal.compacting` was written in place");
    for (n, point) in fail_point::disarm_crash_plan(&data).iter().enumerate() {
        let image = images.join((n + 1).to_string());
        let compacting = std::fs::read(image.join(compacting_path.file_name().unwrap()));
        if let Ok(compacting) = compacting {
            assert!(
                compacting == before || compacting == absorbed,
                "{point}: partly written"
            );
        }
        let engine = open_engine(&image, &room).await;
        assert_rows(&engine, &room, 10).await;
    }
}

#[tokio::test]
async fn crash_between_absorbing_and_truncating_wal_recovers_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("absorb-crash").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=10).await;

    // The active WAL is copied into `.wal.compacting` but never truncated, so records
    // 6..=10 now exist in both files.
    fail_point::arm("compaction.absorb", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());

    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
    apply_range(&reopened, &room, 11..=11).await;
}

/// The snapshot of a compaction is compressed at the engine's `zstd_level`, and the room records
/// its compressed length.
#[tokio::test]
async fn compaction_compresses_the_snapshot_at_its_zstd_level() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("zstd-level").unwrap();
    let options = DiskStorageOptions::new(dir.path())
        .auto_compact(false)
        .zstd_level(19);
    let engine = DiskStorageEngine::new(options).unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    apply_range(&engine, &room, 1..=200).await;
    engine.compact_room(&room).await.unwrap();

    let snap = std::fs::read(engine.snap_file_path(&room)).unwrap();
    let body = &snap[HEADER_SIZE..];
    let payload = zstd::decode_all(body).unwrap();
    assert_eq!(zstd::encode_all(&payload[..], 19).unwrap(), body);
    assert_ne!(zstd::encode_all(&payload[..], 1).unwrap(), body);
    let room = engine.get_room(&room).await.unwrap();
    assert_eq!(room.state.read().await.snapshot_len, body.len() as u64);
}

/// An absorption copies `.wal.compacting` a chunk at a time: a copy of several chunks keeps every
/// record of it, in order.
#[tokio::test]
async fn absorbing_a_rotated_wal_of_several_copy_chunks_keeps_every_record() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("absorb-chunks").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let compacting_path = compacting_wal_path(&engine.wal_file_path(&room));
    let name = "x".repeat(300 * 1024);
    for seq in 1..=8 {
        let row = insert_named(seq, seq as i64, &name);
        engine.apply_batch(&room, vec![row]).await.unwrap();
    }
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    let len = std::fs::metadata(&compacting_path).unwrap().len();
    assert!(len > 2 * super::COPY_CHUNK as u64, "{len}");

    apply_range(&engine, &room, 9..=10).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
}

/// Fails the absorption of the active WAL into an orphan `.wal.compacting` at `fail_point_name`,
/// before the copy is in place, and checks that the active WAL is kept, no copy is left behind
/// and the room stays usable.
async fn failed_absorb_keeps_the_active_wal(room_name: &str, fail_point_name: &'static str) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new(room_name).unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);
    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=10).await;
    let wal_len = std::fs::metadata(&wal_path).unwrap().len();

    fail_point::arm(fail_point_name, &wal_path);
    assert!(engine.compact_room(&room).await.is_err());
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), wal_len);
    assert!(!absorbing_wal_path(&wal_path).exists());

    // A later compaction that also fails absorbs everything.
    apply_range(&engine, &room, 11..=12).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 12).await;
}

/// Writing the copy stops halfway through, as with a full disk.
#[tokio::test]
async fn failed_write_of_the_absorbed_copy_keeps_the_active_wal() {
    failed_absorb_keeps_the_active_wal("absorb-write", "compaction.absorb_write").await;
}

#[tokio::test]
async fn failed_rename_of_the_absorbed_copy_keeps_the_active_wal() {
    failed_absorb_keeps_the_active_wal("absorb-rename", "compaction.absorb_rename").await;
}

/// Fails a compaction of rows 1..=5 at every fail point in `fail_points`, and checks that the
/// room then refuses writes and compactions until it is reopened, and holds every row after.
async fn failed_step_marks_room_failed(room_name: &str, fail_points: &[&'static str]) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new(room_name).unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    for &fail_point_name in fail_points {
        fail_point::arm(fail_point_name, &wal_path);
    }
    assert!(engine.compact_room(&room).await.is_err());

    assert_room_failed(&engine, &room, 6).await;
    let result = engine.compact_room(&room).await;
    assert!(
        matches!(result, Err(StorageError::RoomFailed { .. })),
        "{result:?}"
    );

    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 5).await;
    apply_range(&reopened, &room, 6..=6).await;
}

#[tokio::test]
async fn failed_rotation_rollback_marks_room_failed_until_reopened() {
    failed_step_marks_room_failed(
        "rollback-failure",
        &["compaction.rotate_open", "compaction.rotate_rollback"],
    )
    .await;
}

#[tokio::test]
async fn failed_wal_sync_before_compaction_marks_room_failed_until_reopened() {
    failed_step_marks_room_failed("compaction-sync-failure", &["compaction.sync"]).await;
}

/// The new WAL's directory entry may not be durable: nothing may be acknowledged into it.
#[tokio::test]
async fn failed_directory_sync_after_wal_rotation_marks_room_failed_until_reopened() {
    failed_step_marks_room_failed("rotation-dir-sync", &["compaction.rotate_sync_dir"]).await;
}

/// The rename over `.wal.compacting` may not be durable: the active WAL must not be emptied.
#[tokio::test]
async fn failed_directory_sync_after_absorbing_marks_room_failed_until_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("absorb-dir-sync").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);
    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=10).await;
    let wal_len = std::fs::metadata(&wal_path).unwrap().len();

    fail_point::arm("compaction.absorb_sync_dir", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());

    assert_room_failed(&engine, &room, 11).await;
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), wal_len);
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 10).await;
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
/// failure, survives a reopen, with later deletes and updates still in effect. With
/// `reopen_first`, the room is reopened from the files the failure left before compacting
/// again; otherwise it is compacted again in place.
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
        .apply_batch(&room, vec![sequenced(6, delete)])
        .await
        .unwrap();
    apply_range(&engine, &room, 7..=10).await;
    let engine = if reopen_first {
        let reopened = reopen(engine, dir.path(), &room).await;
        assert_eq!(reopened.get_head_seq(&room).await.unwrap().get(), 10);
        assert!(!has_key(&reopened, &room, 2).await);
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
        .apply_batch(&room, vec![sequenced(11, rename)])
        .await
        .unwrap();
    assert_phase3_contents(&engine, &room).await;

    let reopened = reopen(engine, dir.path(), &room).await;
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
