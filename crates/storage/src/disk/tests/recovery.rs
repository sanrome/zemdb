use super::{WindowedReader, REPLAY_WINDOW};
use crate::disk::compactor::{absorbing_wal_path, compacting_wal_path};
use crate::disk::format::encode_wal_batch;
use crate::disk::tests::support::*;
use crate::fail_point;
use crate::files::{NativeFiles, RoomFiles};
use crate::{DiskStorageEngine, StorageEngine, StorageError};
use std::fmt::Debug;
use std::io::Write;
use std::path::Path;
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation, TableSchema,
    Value,
};

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
    let engine = open_engine(dir, room).await;
    apply_range(&engine, room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, room).await;
    apply_range(&engine, room, 6..=8).await;
    // Dropped without close_room, as in a crash.
}

fn append_raw(path: &Path, bytes: &[u8]) {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap()
        .write_all(bytes)
        .unwrap();
}

fn append_batch(path: &Path, ops: &[SequencedOperation]) {
    append_raw(path, &encode_wal_batch(ops, None).unwrap());
}

/// Checks that `result` is a `WalCorruption` error, naming `context` if it is not.
fn assert_wal_corruption<T: Debug>(result: &Result<T, StorageError>, context: &str) {
    assert!(
        matches!(result, Err(StorageError::WalCorruption(_))),
        "{context}: unexpected result: {result:?}"
    );
}

/// Crashes the recovery of a room with an orphan `.wal.compacting` at `fail_point_name`, while
/// it folds that file into the snapshot, and checks that the next open recovers every row.
async fn crash_while_folding_recovers_all_data(room_name: &str, fail_point_name: &'static str) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new(room_name).unwrap();
    leave_orphan_compacting(dir.path(), &room).await;

    let engine = engine_at(dir.path());
    fail_point::arm(fail_point_name, &engine.wal_file_path(&room));
    assert!(engine.open_room(&room, schema()).await.is_err());

    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 8).await;
}

#[tokio::test]
async fn crash_while_folding_compacting_wal_recovers_all_data() {
    crash_while_folding_recovers_all_data("fold-crash", "recovery.fold").await;
}

#[tokio::test]
async fn crash_before_compacting_wal_removal_recovers_all_data() {
    crash_while_folding_recovers_all_data("cleanup-crash", "recovery.fold_cleanup").await;
}

#[tokio::test]
async fn recovered_compacting_wal_is_folded_and_removed() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("fold-ok").unwrap();
    leave_orphan_compacting(dir.path(), &room).await;

    let engine = engine_at(dir.path());
    engine.open_room(&room, schema()).await.unwrap();

    assert!(!compacting_wal_path(&engine.wal_file_path(&room)).exists());
    let room_handle = engine.get_room(&room).await.unwrap();
    assert_eq!(room_handle.state.read().await.snapshot_seq.get(), 8);
    drop(room_handle);
    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 8).await;
}

/// Once recovery folds a rotated WAL into the snapshot, the active WAL is empty on disk, as in
/// memory: a batch of another size appended next lands at its start, not over older records.
#[tokio::test]
async fn folding_a_rotated_wal_empties_the_active_wal() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("fold-empties").unwrap();
    leave_orphan_compacting(dir.path(), &room).await;

    let engine = open_engine(dir.path(), &room).await;
    let wal_path = engine.wal_file_path(&room);
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);
    let long_name = "a name longer than the others".repeat(4);
    engine
        .apply_batch(&room, vec![insert_named(9, 9, &long_name)])
        .await
        .unwrap();

    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 9).await;
    assert_eq!(
        name_of(&reopened, &room, 9).await,
        Value::String(long_name.into())
    );
}

/// A WAL of many read windows, with a frame larger than a window, replays every batch.
#[tokio::test]
async fn wal_of_many_read_windows_replays_every_batch() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("many-windows").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let name = "x".repeat(1000);
    for seq in 1..=200 {
        let op = insert_named(seq, seq as i64, &name);
        engine.apply_batch(&room, vec![op]).await.unwrap();
    }
    let large = "y".repeat(200 * 1024);
    engine
        .apply_batch(&room, vec![insert_named(201, 201, &large)])
        .await
        .unwrap();
    let wal_len = std::fs::metadata(engine.wal_file_path(&room))
        .unwrap()
        .len();
    assert!(wal_len > 400 * 1024, "{wal_len}");

    let reopened = reopen(engine, dir.path(), &room).await;
    assert_rows(&reopened, &room, 201).await;
    assert_eq!(
        name_of(&reopened, &room, 1).await,
        Value::String(name.as_str().into())
    );
    assert_eq!(
        name_of(&reopened, &room, 200).await,
        Value::String(name.into())
    );
    assert_eq!(
        name_of(&reopened, &room, 201).await,
        Value::String(large.into())
    );
}

/// The windowed reader returns the bytes asked for whether they lie inside its window, end at
/// its last byte, or end one byte past it, and for a read longer than a window.
#[tokio::test]
async fn windowed_reader_reads_across_the_edge_of_its_window() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("room.wal");
    let bytes: Vec<u8> = (0..3 * REPLAY_WINDOW as u32)
        .map(|i| (i % 251) as u8)
        .collect();
    std::fs::write(&path, &bytes).unwrap();
    let file = NativeFiles.open_read(&path).await.unwrap();
    let mut reader = WindowedReader::new(&file, bytes.len() as u64);

    let edge = REPLAY_WINDOW;
    for (offset, len) in [
        (0, 18),
        (edge - 18, 18),
        (edge - 17, 18),
        (edge + 1, 2 * REPLAY_WINDOW - 1),
        (bytes.len() - 1, 1),
    ] {
        let read = reader.read(offset as u64, len).await.unwrap();
        assert_eq!(read, &bytes[offset..offset + len], "{offset}+{len}");
    }
}

/// The search for a valid frame after a damaged tail needs one thread of the blocking pool at
/// most: a recovery on a runtime whose pool has a single thread completes.
#[test]
fn damaged_tail_recovers_with_a_single_blocking_thread() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let room = RoomId::new("one-blocking-thread").unwrap();
            let engine = open_engine(&path, &room).await;
            apply_range(&engine, &room, 1..=3).await;
            engine.close_room(&room).await.unwrap();
            append_raw(&engine.wal_file_path(&room), &[0; 64]);
            let reopened = open_engine(&path, &room).await;
            assert_rows(&reopened, &room, 3).await;
        });
        done.send(()).unwrap();
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("the recovery of a damaged tail did not complete");
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

    assert_wal_corruption(&result, "a gap in the sequence");
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
    append_raw(&wal_path, &damaged);

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
    let frame = |name_len: usize| {
        encode_wal_batch(&[insert_named(1, 1, &"x".repeat(name_len))], None).unwrap()
    };
    let mut first = frame(64 * 1024 - 1 - frame(0).len());
    assert_eq!(first.len(), 64 * 1024 - 1);
    *first.last_mut().unwrap() ^= 0xFF;
    let mut wal = first;
    wal.extend_from_slice(&encode_wal_batch(&[insert(2)], None).unwrap());
    std::fs::write(&wal_path, &wal).unwrap();

    let result = engine.open_room(&room, schema()).await;

    // A complete, durable frame follows the damaged one: truncating would silently drop it.
    assert_wal_corruption(&result, "a damaged frame before a complete one");
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        wal.len() as u64
    );
}

#[tokio::test]
async fn failed_snapshot_existence_check_fails_open_and_keeps_the_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("snapshot-stat-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let snap_path = engine.snap_file_path(&room);
    apply_range(&engine, &room, 1..=3).await;
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

/// An error reading the snapshot's header or body, at `point`, fails the open with `Io`, not
/// as a damaged snapshot: its length was checked before the read, so the error is not a short
/// file. The room's files keep their bytes, and the next open reads them.
async fn snapshot_read_error_is_io_and_keeps_the_files(point: &'static str) {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("snapshot-read-failure").unwrap();
    let engine = open_engine(dir.path(), &room).await;
    let snap_path = engine.snap_file_path(&room);
    let wal_path = engine.wal_file_path(&room);
    apply_range(&engine, &room, 1..=3).await;
    engine.compact_room(&room).await.unwrap();
    apply_range(&engine, &room, 4..=5).await;
    engine.close_room(&room).await.unwrap();
    let files = || {
        (
            std::fs::read(&snap_path).unwrap(),
            std::fs::read(&wal_path).unwrap(),
        )
    };
    let before = files();

    let engine = engine_at(dir.path());
    fail_point::arm(point, &snap_path);
    let result = engine.open_room(&room, schema()).await;
    fail_point::disarm(point, &snap_path);
    assert!(
        matches!(result, Err(StorageError::Io(_))),
        "{point}: {result:?}"
    );
    assert!(files() == before, "{point}: a file changed");

    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 5).await;
}

#[tokio::test]
async fn snapshot_header_read_error_is_io_and_keeps_the_files() {
    snapshot_read_error_is_io_and_keeps_the_files("recovery.snapshot_header").await;
}

#[tokio::test]
async fn snapshot_body_read_error_is_io_and_keeps_the_files() {
    snapshot_read_error_is_io_and_keeps_the_files("recovery.snapshot_body").await;
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
    // What a crash between staging the initial snapshot and renaming it leaves behind.
    let staged = engine
        .snap_file_path(&room)
        .with_extension("snap.tmp.crashed");
    std::fs::write(&staged, b"partial").unwrap();

    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 0).await;
    assert!(staged_snapshot_files(dir.path()).is_empty());
}

/// Bytes a crash can leave where a frame was being written: a header still in zeros followed
/// by data that is not a frame.
fn zeroed_header_and_garbage() -> Vec<u8> {
    let mut bytes = vec![0u8; zemdb_core::BATCH_HEADER_SIZE];
    bytes.extend_from_slice(&[0x13; 100]);
    bytes.extend_from_slice(&[0xBA, 0x7C, 0x01, 0x02]);
    bytes
}

/// Damage done to the header of a frame, given the bytes from its start.
type HeaderDamage = fn(&mut [u8]);

#[tokio::test]
async fn damaged_length_in_the_middle_is_corruption_and_leaves_the_wal_intact() {
    let damage: [(&str, HeaderDamage); 2] = [
        ("flipped-length", |header| header[2] ^= 0x01),
        ("length-past-eof", |header| {
            header[2..6].copy_from_slice(&1_000_000u32.to_le_bytes())
        }),
    ];
    for (name, damage) in damage {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new(name).unwrap();
        let engine = engine_at(dir.path());
        let wal_path = engine.wal_file_path(&room);

        append_batch(&wal_path, &[insert(1)]);
        let second = std::fs::metadata(&wal_path).unwrap().len() as usize;
        append_batch(&wal_path, &[insert(2)]);
        append_batch(&wal_path, &[insert(3)]);
        let mut wal = std::fs::read(&wal_path).unwrap();
        damage(&mut wal[second..]);
        std::fs::write(&wal_path, &wal).unwrap();

        let result = engine.open_room(&room, schema()).await;

        assert_wal_corruption(&result, name);
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal, "{name}");
    }
}

#[tokio::test]
async fn zeroed_header_followed_by_garbage_is_a_torn_write() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("zeroed-header-garbage").unwrap();
    let engine = engine_at(dir.path());
    let wal_path = engine.wal_file_path(&room);

    append_batch(&wal_path, &[insert(1)]);
    append_batch(&wal_path, &[insert(2)]);
    let valid_len = std::fs::metadata(&wal_path).unwrap().len();
    append_raw(&wal_path, &zeroed_header_and_garbage());

    engine.open_room(&room, schema()).await.unwrap();

    assert_rows(&engine, &room, 2).await;
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), valid_len);
}

#[tokio::test]
async fn zeroed_header_followed_by_a_valid_frame_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("zeroed-header-frame").unwrap();
    let engine = engine_at(dir.path());
    let wal_path = engine.wal_file_path(&room);

    append_batch(&wal_path, &[insert(1)]);
    append_raw(&wal_path, &zeroed_header_and_garbage());
    append_batch(&wal_path, &[insert(2)]);
    let wal = std::fs::read(&wal_path).unwrap();

    let result = engine.open_room(&room, schema()).await;

    assert_wal_corruption(&result, "a zeroed header before a valid frame");
    assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
}

#[tokio::test]
async fn compacting_wal_with_a_torn_tail_is_folded() {
    let dir = tempfile::tempdir().unwrap();
    let room = RoomId::new("compacting-torn").unwrap();
    leave_orphan_compacting(dir.path(), &room).await;
    let engine = engine_at(dir.path());
    let compacting = compacting_wal_path(&engine.wal_file_path(&room));
    append_raw(&compacting, &zeroed_header_and_garbage());

    engine.open_room(&room, schema()).await.unwrap();

    assert_rows(&engine, &room, 8).await;
    assert!(!compacting.exists());
}

/// Writes to `path` the orphan `.wal.compacting` (1..=5) followed by a copy of the active WAL
/// (6..=8) whose first frame `damage` breaks, as an absorption cut short by a crash would.
fn write_damaged_absorb(
    engine: &DiskStorageEngine,
    room: &RoomId,
    damage: HeaderDamage,
    path: &Path,
) {
    let wal_path = engine.wal_file_path(room);
    let mut copy = std::fs::read(&wal_path).unwrap();
    damage(&mut copy);
    let compacting = std::fs::read(compacting_wal_path(&wal_path)).unwrap();
    std::fs::write(path, [compacting, copy].concat()).unwrap();
}

const ABSORB_DAMAGE: [(&str, HeaderDamage); 2] = [
    ("zeroed-header", |frame| {
        frame[..zemdb_core::BATCH_HEADER_SIZE].fill(0)
    }),
    ("lost-payload-byte", |frame| {
        frame[zemdb_core::BATCH_HEADER_SIZE + 3] ^= 0xFF
    }),
];

/// A crash while absorbing leaves the copy in a temporary file, whatever it holds: recovery
/// removes it, and `.wal.compacting` and the active WAL still hold every record.
#[tokio::test]
async fn crash_while_absorbing_leaves_a_copy_that_recovery_discards() {
    for (name, damage) in ABSORB_DAMAGE {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new(name).unwrap();
        leave_orphan_compacting(dir.path(), &room).await;
        let engine = engine_at(dir.path());
        let absorbing = absorbing_wal_path(&engine.wal_file_path(&room));
        write_damaged_absorb(&engine, &room, damage, &absorbing);

        engine.open_room(&room, schema()).await.unwrap();

        assert_rows(&engine, &room, 8).await;
        assert!(!absorbing.exists(), "{name}");
    }
}

/// `.wal.compacting` is only ever replaced whole, so a damaged frame followed by valid ones is
/// corruption there as well, even if the active WAL holds the same records.
#[tokio::test]
async fn damaged_rotated_wal_followed_by_valid_frames_is_corruption() {
    for (name, damage) in ABSORB_DAMAGE {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new(name).unwrap();
        leave_orphan_compacting(dir.path(), &room).await;
        let engine = engine_at(dir.path());
        let compacting = compacting_wal_path(&engine.wal_file_path(&room));
        write_damaged_absorb(&engine, &room, damage, &compacting);
        let before = file_contents(dir.path());

        let result = engine.open_room(&room, schema()).await;

        assert_wal_corruption(&result, name);
        assert!(file_contents(dir.path()) == before, "{name}: files changed");
    }
}

const ITEMS: u16 = 1;

/// `schema()` with an `items` table as well.
fn schema_with_items() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String);
    let items = TableSchema::builder("items")
        .table_id(ITEMS)
        .primary_key("id", DataType::Int)
        .column("data", DataType::Bytes);
    Schema::builder().table(users).table(items).build()
}

/// A record of a table the schema lacks is `SchemaMismatch` even if the WAL ends in a torn
/// write, and no file changes: the torn tail is only truncated once every record replays. The
/// same holds with that record in `.wal.compacting` and the torn tail in the active WAL.
#[tokio::test]
async fn schema_mismatch_is_reported_before_a_torn_tail_is_truncated() {
    for in_compacting in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let room = RoomId::new("mismatch-before-truncation").unwrap();
        let engine = engine_at(dir.path());
        engine.open_room(&room, schema_with_items()).await.unwrap();
        engine.apply_batch(&room, vec![insert(1)]).await.unwrap();
        let row = CompactRow::new(vec![Value::Int(7), Value::Bytes(vec![1, 2, 3].into())]);
        let item = Operation::insert(ITEMS, PrimaryKey::single(7i64), row, 100);
        engine
            .apply_batch(&room, vec![sequenced(2, item)])
            .await
            .unwrap();
        let mut head = 2;
        if in_compacting {
            fail_compaction_leaving_compacting_wal(&engine, &room).await;
            engine.apply_batch(&room, vec![insert(3)]).await.unwrap();
            head = 3;
        }
        engine.close_room(&room).await.unwrap();
        // The start of a header: a torn write.
        append_raw(&engine.wal_file_path(&room), &[0xBA, 0x7C, 1, 2, 3]);
        let before = file_contents(dir.path());

        let engine = engine_at(dir.path());
        let result = engine.open_room(&room, schema()).await;
        assert!(
            matches!(
                result,
                Err(StorageError::SchemaMismatch {
                    table_id: ITEMS,
                    ..
                })
            ),
            "{result:?}"
        );
        assert!(file_contents(dir.path()) == before, "files changed");

        engine.open_room(&room, schema_with_items()).await.unwrap();
        assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), head);
    }
}
