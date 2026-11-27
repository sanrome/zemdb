//! The unit tests of the disk engine in wasm32, on the file layer over the Origin Private File
//! System, in a dedicated Worker of a browser with `wasm-bindgen-test` (the native ones do not
//! build there). They reach what a step of the browser harness does not: the levels of
//! `zstd_level` there, the `.snap` that `ruzstd` compresses, the automatic compaction in a task
//! that nobody awaits, and the names of the staged snapshots, which the recovery removes.

use super::*;
use crate::disk::compactor::staged_snapshot_path;
use crate::files::OpfsFiles;
use crate::memory::state::RoomSnapshotPayload;
use crate::snapshot::deserialize_zstd;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use zemdb_core::{DataType, TableSchema, Value};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A data directory of its own for a test, under the root of the OPFS.
fn fresh(test: &str) -> PathBuf {
    let run = (js_sys::Math::random() * 1e12) as u64;
    PathBuf::from(format!("zemdb-storage/{test}-{run}"))
}

fn schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(0)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String);
    Schema::builder().table(users).build()
}

/// Inserts the row "user {seq}" under the key `seq`, at the sequence `seq`.
fn insert(seq: u64) -> SequencedOperation {
    let row = CompactRow::new(vec![
        Value::Int(seq as i64),
        Value::String(format!("user {seq}").into()),
    ]);
    let op = Operation::insert(0, PrimaryKey::single(seq as i64), row, 100);
    SequencedOperation::new(seq, op)
}

async fn apply_range(
    engine: &DiskStorageEngine,
    room: &RoomId,
    seqs: std::ops::RangeInclusive<u64>,
) {
    for seq in seqs {
        engine.apply_batch(room, vec![insert(seq)]).await.unwrap();
    }
}

/// Fails unless `room` is at `last` with every row up to it.
async fn assert_rows(engine: &DiskStorageEngine, room: &RoomId, last: u64) {
    assert_eq!(engine.get_head_seq(room).await.unwrap().get(), last);
    for key in 1..=last as i64 {
        let row = engine.get(room, "users", &PrimaryKey::single(key)).await;
        assert!(row.unwrap().is_some(), "row {key} lost");
    }
}

/// In WebAssembly `zstd_level` takes only the level that `ruzstd` gives, its `Fastest`, which is
/// the default there: any other, also the native default, is refused before anything is touched.
#[wasm_bindgen_test]
async fn zstd_level_takes_only_the_level_of_ruzstd() {
    let dir = fresh("zstd-level");
    let options = DiskStorageOptions::new(&dir);
    assert_eq!(options.zstd_level, 1);
    assert!(DiskStorageEngine::new(options.clone()).is_ok());
    for level in [3, 0, 2, -1, -7, 19, 22] {
        let refused = DiskStorageEngine::new(options.clone().zstd_level(level));
        assert!(
            matches!(&refused, Err(StorageError::InvalidOptions(message))
                if message == &format!("zstd_level must be between 1 and 1, got {level}")),
            "{level}: {refused:?}"
        );
    }
    assert!(!OpfsFiles.exists(&dir).await.unwrap());
}

/// A compaction writes the `.snap` with `ruzstd`: a Zstandard frame, compressed, that decodes to
/// the rows of the room, and from which the room opens again.
#[wasm_bindgen_test]
async fn a_compaction_writes_a_snapshot_that_ruzstd_compresses() {
    let dir = fresh("ruzstd-snap");
    let options = DiskStorageOptions::new(&dir).auto_compact(false);
    let engine = DiskStorageEngine::new(options.clone()).unwrap();
    let room = RoomId::new("snap").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    apply_range(&engine, &room, 1..=300).await;
    engine.compact_room(&room).await.unwrap();

    let (snapshot_len, serialized) = {
        let opened = engine.get_room(&room).await.unwrap();
        let state = opened.state.read().await;
        let raw = bincode::serialize(&RoomSnapshotRef {
            head_seq: state.head_seq,
            tables: state.data.tables(),
        })
        .unwrap();
        (state.snapshot_len, raw.len() as u64)
    };
    assert!(
        snapshot_len * 2 < serialized,
        "a snapshot of {serialized} bytes compressed to {snapshot_len}"
    );
    let file = OpfsFiles
        .open_read(&engine.snap_file_path(&room))
        .await
        .unwrap();
    let payload = file
        .read_at(format::HEADER_SIZE as u64, snapshot_len as usize)
        .await
        .unwrap();
    drop(file);
    assert_eq!(
        &payload[..4],
        &[0x28, 0xB5, 0x2F, 0xFD],
        "a Zstandard frame"
    );
    let decoded: RoomSnapshotPayload = deserialize_zstd(&payload, u64::MAX, None).unwrap();
    assert_eq!(decoded.head_seq.get(), 300);
    assert_eq!(decoded.tables[&0].len(), 300);

    engine.close_room(&room).await.unwrap();
    let engine = DiskStorageEngine::new(options).unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    assert_rows(&engine, &room, 300).await;
}

/// The automatic compaction runs in a task of its own that nobody awaits: once the batch that
/// triggers it returns, the compaction holds the compaction lock and has not run yet, and it
/// runs to its end, with the lock, while nothing awaits it.
#[wasm_bindgen_test]
async fn the_automatic_compaction_runs_in_a_task_that_nobody_awaits() {
    let dir = fresh("auto-compaction");
    let options = DiskStorageOptions::new(&dir)
        .min_compaction_bytes(1)
        .compaction_ratio(0.01);
    let engine = DiskStorageEngine::new(options).unwrap();
    let room = RoomId::new("auto").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    engine.apply_batch(&room, vec![insert(1)]).await.unwrap();

    let opened = engine.get_room(&room).await.unwrap();
    assert!(
        opened.compaction.try_lock().is_err(),
        "the batch waited for the compaction, or did not launch it"
    );
    assert_eq!(opened.state.read().await.snapshot_seq.get(), 0);
    drop(opened.compaction.lock().await);
    assert_eq!(opened.state.read().await.snapshot_seq.get(), 1);
    assert!(!OpfsFiles
        .exists(&compactor::compacting_wal_path(
            &engine.wal_file_path(&room)
        ))
        .await
        .unwrap());
}

/// A staged snapshot has a name of its own at random, which the layer takes, and which the
/// recovery finds by its prefix and removes when the room opens, as one that a cut leaves.
#[wasm_bindgen_test]
async fn the_recovery_removes_a_staged_snapshot_left_behind() {
    let dir = fresh("staged");
    let engine = DiskStorageEngine::new(DiskStorageOptions::new(&dir)).unwrap();
    let room = RoomId::new("staged").unwrap();
    let snap = engine.snap_file_path(&room);
    let staged = staged_snapshot_path(&snap).unwrap();
    assert_ne!(staged, staged_snapshot_path(&snap).unwrap());
    let name = staged.file_name().unwrap().to_str().unwrap();
    assert!(name.starts_with("room_staged.snap.tmp."), "{name}");

    OpfsFiles.create_dir_all(&dir).await.unwrap();
    let file = OpfsFiles.create_locked(&staged).await.unwrap().unwrap();
    file.write_at(0, b"partial".to_vec()).await.unwrap();
    drop(file);
    let listed = OpfsFiles.list(&dir, "room_staged.snap.tmp.").await.unwrap();
    assert_eq!(listed, std::slice::from_ref(&staged));

    engine.open_room(&room, schema()).await.unwrap();
    assert!(!OpfsFiles.exists(&staged).await.unwrap());
    assert!(OpfsFiles.exists(&snap).await.unwrap());
}
