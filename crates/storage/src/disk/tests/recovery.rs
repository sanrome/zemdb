use crate::disk::format::encode_wal_batch;
use crate::fail_point;
use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation, TableSchema,
    Value,
};

const USERS: u16 = 0;

fn schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String);
    Schema::builder().table(users).build()
}

fn insert_named(seq: u64, key: i64, name: &str) -> SequencedOperation {
    let row = CompactRow::new(vec![Value::Int(key), Value::String(name.into())]);
    SequencedOperation::with_default_origin(
        seq,
        Operation::insert(USERS, PrimaryKey::single(key), row, 100),
    )
}

fn insert(seq: u64) -> SequencedOperation {
    insert_named(seq, seq as i64, &format!("user {seq}"))
}

fn engine_at(dir: &Path) -> DiskStorageEngine {
    DiskStorageEngine::new(DiskStorageOptions::new(dir).auto_compact(false))
}

async fn assert_rows(engine: &DiskStorageEngine, room: &RoomId, last: u64) {
    assert_eq!(engine.get_head_seq(room).await.unwrap().get(), last);
    for seq in 1..=last {
        let row = engine
            .get(room, "users", &PrimaryKey::single(seq as i64))
            .await
            .unwrap();
        assert!(row.is_some(), "row {seq} lost");
    }
}

async fn name_of(engine: &DiskStorageEngine, room: &RoomId, key: i64) -> Value {
    let row = engine
        .get(room, "users", &PrimaryKey::single(key))
        .await
        .unwrap()
        .unwrap();
    row[1].clone()
}

/// Leaves the room as a crash after a failed compaction would: snapshot without the latest
/// writes, an orphan `.wal.compacting` holding 1..=5 and the active `.wal` holding 6..=8.
async fn leave_orphan_compacting(dir: &Path, room: &RoomId) {
    let engine = engine_at(dir);
    engine.open_room(room, schema()).await.unwrap();
    for seq in 1..=5 {
        engine.apply_batch(room, vec![insert(seq)]).await.unwrap();
    }
    fail_point::arm("compaction.phase2", &engine.snap_file_path(room));
    assert!(engine.compact_room(room).await.is_err());
    for seq in 6..=8 {
        engine.apply_batch(room, vec![insert(seq)]).await.unwrap();
    }
    // Dropped without close_room, as in a crash.
}

fn append_batch(path: &Path, ops: &[SequencedOperation]) {
    let bytes = encode_wal_batch(ops, None).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    file.write_all(&bytes).unwrap();
}

#[tokio::test]
async fn crash_while_folding_compacting_wal_recovers_all_data() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("fold-crash").unwrap();
    leave_orphan_compacting(dir.path(), &room).await;

    let engine = engine_at(dir.path());
    fail_point::arm("recovery.fold", &engine.wal_file_path(&room));
    assert!(engine.open_room(&room, schema()).await.is_err());

    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 8).await;
}

#[tokio::test]
async fn crash_before_compacting_wal_removal_recovers_all_data() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("cleanup-crash").unwrap();
    leave_orphan_compacting(dir.path(), &room).await;

    let engine = engine_at(dir.path());
    fail_point::arm("recovery.fold_cleanup", &engine.wal_file_path(&room));
    assert!(engine.open_room(&room, schema()).await.is_err());

    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 8).await;
}

#[tokio::test]
async fn recovered_compacting_wal_is_folded_and_removed() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("fold-ok").unwrap();
    leave_orphan_compacting(dir.path(), &room).await;

    let engine = engine_at(dir.path());
    engine.open_room(&room, schema()).await.unwrap();

    let compacting = engine.wal_file_path(&room).with_extension("wal.compacting");
    assert!(!compacting.exists());
    let room_handle = engine.get_room(&room).await.unwrap();
    assert_eq!(room_handle.state.read().await.snapshot_seq.get(), 8);
    drop(room_handle);
    engine.close_room(&room).await.unwrap();

    let reopened = engine_at(dir.path());
    reopened.open_room(&room, schema()).await.unwrap();
    assert_rows(&reopened, &room, 8).await;
}

#[tokio::test]
async fn replay_skips_records_already_applied() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("duplicate-replay").unwrap();
    let engine = engine_at(dir.path());
    let wal_path = engine.wal_file_path(&room);

    append_batch(
        &wal_path,
        &[
            insert_named(1, 1, "first"),
            insert_named(2, 2, "first"),
            insert_named(3, 1, "second"),
        ],
    );
    // A stale copy of operation 1 appended again, as a crash between copying and truncating
    // a WAL can leave behind. Re-applying it would revert key 1 to its first value.
    append_batch(&wal_path, &[insert_named(1, 1, "first")]);

    engine.open_room(&room, schema()).await.unwrap();

    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 3);
    assert_eq!(
        name_of(&engine, &room, 1).await,
        Value::String("second".into())
    );
}

#[tokio::test]
async fn replay_rejects_sequence_gap() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("gap-replay").unwrap();
    let engine = engine_at(dir.path());
    let wal_path = engine.wal_file_path(&room);

    append_batch(&wal_path, &[insert(1), insert(2), insert(3)]);
    append_batch(&wal_path, &[insert(5)]);

    let result = engine.open_room(&room, schema()).await;

    assert!(matches!(result, Err(StorageError::WalCorruption(_))));
}

#[tokio::test]
async fn checksum_mismatch_before_a_partial_header_is_a_torn_write() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("torn-before-partial-header").unwrap();
    let engine = engine_at(dir.path());
    let wal_path = engine.wal_file_path(&room);

    append_batch(&wal_path, &[insert(1)]);
    let valid_len = std::fs::metadata(&wal_path).unwrap().len();
    let mut damaged = encode_wal_batch(&[insert(2)], None).unwrap();
    *damaged.last_mut().unwrap() ^= 0xFF;
    // Followed by fewer bytes than a frame header, even though they start like one.
    damaged.extend_from_slice(&[0xBA, 0x7C, 0x01, 0x00, 0x00]);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(&damaged)
        .unwrap();

    engine.open_room(&room, schema()).await.unwrap();

    assert_rows(&engine, &room, 1).await;
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), valid_len);
}

#[tokio::test]
async fn checksum_mismatch_before_a_complete_frame_is_corruption_at_any_offset() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("corrupt-at-buffer-edge").unwrap();
    let engine = engine_at(dir.path());
    let wal_path = engine.wal_file_path(&room);

    // A first frame that ends one byte before a 64 KiB boundary, where a buffered reader that
    // looks ahead with a single read sees only one byte of the frame that follows.
    let frame_len = |name_len: usize| {
        encode_wal_batch(&[insert_named(1, 1, &"x".repeat(name_len))], None)
            .unwrap()
            .len()
    };
    let name_len = 64 * 1024 - 1 - frame_len(0);
    let mut first = encode_wal_batch(&[insert_named(1, 1, &"x".repeat(name_len))], None).unwrap();
    assert_eq!(first.len(), 64 * 1024 - 1);
    *first.last_mut().unwrap() ^= 0xFF;
    let mut wal = first;
    wal.extend_from_slice(&encode_wal_batch(&[insert(2)], None).unwrap());
    std::fs::write(&wal_path, &wal).unwrap();

    let result = engine.open_room(&room, schema()).await;

    // A complete, durable frame follows the damaged one: truncating would silently drop it.
    assert!(
        matches!(result, Err(StorageError::WalCorruption(_))),
        "unexpected result: {result:?}"
    );
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        wal.len() as u64
    );
}

#[tokio::test]
async fn failed_snapshot_existence_check_fails_open_and_keeps_the_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("snapshot-stat-failure").unwrap();
    let engine = engine_at(dir.path());
    let snap_path = engine.snap_file_path(&room);
    engine.open_room(&room, schema()).await.unwrap();
    for seq in 1..=3 {
        engine.apply_batch(&room, vec![insert(seq)]).await.unwrap();
    }
    // The snapshot now holds every row and the WAL is empty.
    engine.compact_room(&room).await.unwrap();
    engine.close_room(&room).await.unwrap();
    let snapshot = std::fs::read(&snap_path).unwrap();

    let engine = engine_at(dir.path());
    fail_point::arm("recovery.snapshot_exists", &snap_path);
    assert!(engine.open_room(&room, schema()).await.is_err());
    assert_eq!(std::fs::read(&snap_path).unwrap(), snapshot);

    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 3).await;
}

/// Names of the staged snapshot files left in `dir`.
fn staged_snapshot_files(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".snap.tmp."))
        .collect()
}

#[tokio::test]
async fn failed_initial_snapshot_write_leaves_no_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("initial-snapshot-failure").unwrap();
    let engine = engine_at(dir.path());
    let snap_path = engine.snap_file_path(&room);

    fail_point::arm("snapshot.stage_write", &snap_path);
    assert!(engine.open_room(&room, schema()).await.is_err());

    // A partial snapshot would keep the room from ever opening again.
    assert!(!snap_path.exists());
    assert!(staged_snapshot_files(dir.path()).is_empty());
    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 0).await;
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();
}

#[tokio::test]
async fn crash_before_initial_snapshot_rename_is_cleaned_up_on_next_open() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("initial-snapshot-crash").unwrap();
    let engine = engine_at(dir.path());
    let snap_path = engine.snap_file_path(&room);

    // The open stops for good between staging the initial snapshot and renaming it.
    let mut pause = fail_point::arm_pause("snapshot.install", &snap_path);
    let open = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move { engine.open_room(&room, schema()).await }
    });
    // Bounded, so that an open that never reaches the pause fails the test instead of hanging.
    tokio::time::timeout(Duration::from_secs(10), pause.reached())
        .await
        .expect("opening the room never reached the rename of the initial snapshot");
    open.abort();
    assert!(open.await.unwrap_err().is_cancelled());
    drop(pause);

    assert!(!snap_path.exists());
    assert_eq!(staged_snapshot_files(dir.path()).len(), 1);
    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 0).await;
    assert!(staged_snapshot_files(dir.path()).is_empty());
}
