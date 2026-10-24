use crate::fail_point;
use crate::memory::index::tests::{
    apply_random_batches, constrained_schema, evolved_schema, owned_schema,
    refused_batches_change_nothing, reloads_at_random_points_decide_as_the_new_schema,
    reloads_race_batches_and_readers, reloads_that_do_not_evolve_change_nothing,
    reloads_that_evolve_decide_as_the_new_schema, snapshot_indexes_are_built_off_the_runtime,
    snapshots_replace_the_indexes, snapshots_that_break_a_constraint_change_nothing,
    snapshots_that_meet_the_constraints_are_installed, RandomLog, BUILD_POINT,
};
use crate::{MemoryStorageEngine, StorageEngine, StorageError};
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

fn insert(seq: u64, key: i64) -> SequencedOperation {
    let row = CompactRow::new(vec![
        Value::Int(key),
        Value::String(format!("user {key}").into()),
    ]);
    SequencedOperation::new(
        seq,
        Operation::insert(USERS, PrimaryKey::single(key), row, 100),
    )
}

/// A snapshot of `room` holding one row per key, inserted in a single batch at sequences
/// `1..`, so that its head sequence is the number of keys.
async fn snapshot_of(room: &RoomId, keys: impl IntoIterator<Item = i64>) -> Vec<u8> {
    let source = MemoryStorageEngine::new();
    source.open_room(room, schema()).await.unwrap();
    let ops = (1u64..)
        .zip(keys)
        .map(|(seq, key)| insert(seq, key))
        .collect();
    source.apply_batch(room, ops).await.unwrap();
    source.create_snapshot(room).await.unwrap()
}

#[tokio::test]
async fn write_during_scan_shares_untouched_rows() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("shared-table").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    let ops = (0..1000).map(|key| insert(key as u64 + 1, key)).collect();
    engine.apply_batch(&room, ops).await.unwrap();

    // A scan keeps its own reference to the table while writes continue.
    let room_handle = engine.get_room(&room).unwrap();
    let held = room_handle.read().unwrap().data.tables()[&USERS].clone();
    engine
        .apply_batch(&room, vec![insert(1001, 5000)])
        .await
        .unwrap();
    let current = room_handle.read().unwrap().data.tables()[&USERS].clone();

    // A row far from the written key is not copied: both versions share it.
    let key = PrimaryKey::single(0i64);
    assert!(std::ptr::eq(
        held.get(&key).unwrap(),
        current.get(&key).unwrap()
    ));
    assert!(held.get(&PrimaryKey::single(5000i64)).is_none());
}

#[tokio::test]
async fn snapshot_is_visible_through_room_handle_taken_before() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("held-handle").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    engine.apply_batch(&room, vec![insert(1, 1)]).await.unwrap();
    // A writer that looked up the room before the snapshot keeps this handle.
    let handle = engine.get_room(&room).unwrap();

    let snapshot = snapshot_of(&RoomId::new("source").unwrap(), [10, 11, 12]).await;
    engine
        .apply_snapshot(&room, schema(), snapshot.into(), 3u64.into())
        .await
        .unwrap();

    let state = handle.read().unwrap();
    assert_eq!(state.head_seq.get(), 3);
    assert!(state.data.tables()[&USERS]
        .get(&PrimaryKey::single(12i64))
        .is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_is_decoded_without_blocking_the_runtime() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("decoded-off-the-runtime").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    let snapshot = snapshot_of(&room, 1..=3).await;

    let mut pause =
        fail_point::arm_blocking_pause("apply_snapshot.decode", Path::new(room.as_str()));
    // The runtime has a single thread: this task only runs if decoding leaves it free.
    let releaser = tokio::spawn(async move {
        pause.reached().await;
        pause.release();
    });
    let head = engine
        .apply_snapshot(&room, schema(), snapshot.into(), 3u64.into())
        .await
        .unwrap();
    assert_eq!(head.get(), 3);
    tokio::time::timeout(Duration::from_secs(10), releaser)
        .await
        .expect("decoding never reached its pause point")
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_goes_to_the_room_open_when_decoding_ends() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("reopened-while-decoding").unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    let snapshot = snapshot_of(&room, 1..=3).await;

    let mut pause =
        fail_point::arm_blocking_pause("apply_snapshot.decode", Path::new(room.as_str()));
    let apply = tokio::spawn({
        let (engine, room) = (engine.clone(), room.clone());
        async move {
            engine
                .apply_snapshot(&room, schema(), snapshot.into(), 3u64.into())
                .await
        }
    });
    // The room is closed and reopened, empty, while the snapshot is decoded.
    pause.reached().await;
    engine.close_room(&room).await.unwrap();
    engine.open_room(&room, schema()).await.unwrap();
    pause.release();

    assert_eq!(apply.await.unwrap().unwrap().get(), 3);
    assert_eq!(engine.get_head_seq(&room).await.unwrap().get(), 3);
}

/// After every batch of a random log, the room's indexes are those its rows give, and the batch
/// reports the updates of rows that do not exist.
#[tokio::test]
async fn indexes_follow_the_rows_of_random_batches() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("random-batches").unwrap();
    engine.open_room(&room, evolved_schema()).await.unwrap();
    let mut log = RandomLog::new(1, evolved_schema());
    apply_random_batches(&engine, &room, &mut log, 60).await;
}

#[tokio::test]
async fn refused_batches_change_neither_rows_nor_indexes() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("refused-batches").unwrap();
    engine.open_room(&room, constrained_schema()).await.unwrap();
    refused_batches_change_nothing(&engine, &room).await;
}

#[tokio::test]
async fn snapshots_replace_rows_and_indexes_together() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("snapshot-indexes").unwrap();
    engine.open_room(&room, constrained_schema()).await.unwrap();
    snapshots_replace_the_indexes(&engine, &room).await;
}

/// Applying a snapshot builds its indexes off the runtime: on a current-thread runtime, the
/// test's thread never builds them.
#[tokio::test(flavor = "current_thread")]
async fn snapshot_indexes_are_built_without_blocking_the_runtime() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("snapshot-indexes-off-the-runtime").unwrap();
    engine.open_room(&room, constrained_schema()).await.unwrap();
    snapshot_indexes_are_built_off_the_runtime(&engine, &room).await;
}

#[tokio::test]
async fn a_snapshot_that_breaks_a_constraint_changes_nothing() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("snapshot-breaks").unwrap();
    engine.open_room(&room, constrained_schema()).await.unwrap();
    snapshots_that_break_a_constraint_change_nothing(&engine, &room).await;
}

#[tokio::test]
async fn a_snapshot_that_meets_the_constraints_is_installed() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("snapshot-meets").unwrap();
    engine.open_room(&room, constrained_schema()).await.unwrap();
    snapshots_that_meet_the_constraints_are_installed(&engine, &room).await;
}

#[tokio::test]
async fn reloads_to_a_schema_that_does_not_evolve_change_nothing() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("reload-refused").unwrap();
    engine.open_room(&room, owned_schema()).await.unwrap();
    reloads_that_do_not_evolve_change_nothing(&engine, &room).await;
}

#[tokio::test]
async fn reload_of_an_unopened_room_is_room_not_found() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("reload-unopened").unwrap();
    let err = engine
        .reload_schema(&room, evolved_schema())
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::RoomNotFound(_)), "{err:?}");
}

/// A reload decides from then on as the new schema, and adds the indexes of its new
/// constraints without building them from the rows, which never hold the new columns.
#[tokio::test(flavor = "current_thread")]
async fn reloads_decide_as_the_new_schema() {
    let engine = MemoryStorageEngine::new();
    let room = RoomId::new("reload-evolves").unwrap();
    engine.open_room(&room, constrained_schema()).await.unwrap();
    reloads_that_evolve_decide_as_the_new_schema(&engine, &room).await;
    assert!(!fail_point::reached_on_this_thread(BUILD_POINT));
}

#[tokio::test]
async fn reloads_at_random_points_decide_as_a_room_open_with_the_new_schema() {
    for seed in 0..10 {
        let engine = MemoryStorageEngine::new();
        let room = RoomId::new("reload-random").unwrap();
        engine.open_room(&room, constrained_schema()).await.unwrap();
        reloads_at_random_points_decide_as_the_new_schema(&engine, &room, seed).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reloads_racing_batches_and_readers_apply_each_batch_under_one_schema() {
    for seed in 0..20 {
        let engine = MemoryStorageEngine::new();
        let room = RoomId::new("reload-race").unwrap();
        engine.open_room(&room, constrained_schema()).await.unwrap();
        reloads_race_batches_and_readers(&engine, &room, seed).await;
    }
}
