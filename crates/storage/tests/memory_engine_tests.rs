mod common;

use common::*;
use std::sync::Arc;
use zemdb_core::{PrimaryKey, RoomId, SequenceNumber, Value};
use zemdb_storage::{MemoryStorageEngine, StorageEngine, StorageError};

#[tokio::test]
async fn test_room_lifecycle() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-1").unwrap();

    engine.open_room(&room_id, users_schema()).await.unwrap();
    let err = engine
        .open_room(&room_id, users_schema())
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::RoomAlreadyOpen(_)));

    let head = engine.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head, SequenceNumber::from(0u64));

    engine.close_room(&room_id).await.unwrap();
    let err = engine
        .get(&room_id, "users", &PrimaryKey::single(1i64))
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::RoomNotFound(_)));
}

#[tokio::test]
async fn test_point_lookup_and_batch_mutation() {
    let engine = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-point").unwrap();
    engine.open_room(&room_id, users_schema()).await.unwrap();

    let alice = row(1, "Alice", 100, true);
    let bob = row(2, "Bob", 80, false);
    let ops = vec![
        insert(1, 1, alice.clone(), 10),
        insert(2, 2, bob.clone(), 20),
    ];

    let head = engine.apply_batch(&room_id, ops).await.unwrap();
    assert_eq!(head, SequenceNumber::from(2u64));

    assert_eq!(get_user(&engine, &room_id, 1).await, Some(alice));
    assert_eq!(get_user(&engine, &room_id, 2).await, Some(bob));
    assert_eq!(get_user(&engine, &room_id, 999).await, None);

    // Alice's score (column 2) goes to 120.
    engine
        .apply_batch(&room_id, vec![update(3, 1, 2, Value::Int(120), 30)])
        .await
        .unwrap();
    let updated_alice = get_user(&engine, &room_id, 1).await.unwrap();
    assert_eq!(updated_alice[2], Value::Int(120));

    engine
        .apply_batch(&room_id, vec![delete(4, 2, 40)])
        .await
        .unwrap();
    assert_eq!(get_user(&engine, &room_id, 2).await, None);
}

#[tokio::test]
async fn test_concurrent_rooms_isolation() {
    let engine = Arc::new(MemoryStorageEngine::new());
    let room_a = RoomId::new("room-a").unwrap();
    let room_b = RoomId::new("room-b").unwrap();

    engine.open_room(&room_a, users_schema()).await.unwrap();
    engine.open_room(&room_b, users_schema()).await.unwrap();

    let engine_a = Arc::clone(&engine);
    let ra = room_a.clone();
    let handle_a = tokio::spawn(async move {
        for i in 1..=20i64 {
            let name = format!("User A {i}");
            let ops = vec![insert(i as u64, i, row(i, &name, i * 10, true), 100)];
            engine_a.apply_batch(&ra, ops).await.unwrap();
        }
    });

    let engine_b = Arc::clone(&engine);
    let rb = room_b.clone();
    let handle_b = tokio::spawn(async move {
        for i in 1..=20i64 {
            let name = format!("User B {i}");
            let ops = vec![insert(i as u64, i, row(i, &name, i * 20, false), 100)];
            engine_b.apply_batch(&rb, ops).await.unwrap();
        }
    });

    // Reads of room A while both rooms are written.
    let engine_reader = Arc::clone(&engine);
    let ra_reader = room_a.clone();
    let handle_reader = tokio::spawn(async move {
        for _ in 0..10 {
            get_user(engine_reader.as_ref(), &ra_reader, 1).await;
            tokio::task::yield_now().await;
        }
    });

    handle_a.await.unwrap();
    handle_b.await.unwrap();
    handle_reader.await.unwrap();

    assert_eq!(
        engine.get_head_seq(&room_a).await.unwrap(),
        SequenceNumber::from(20u64)
    );
    assert_eq!(
        engine.get_head_seq(&room_b).await.unwrap(),
        SequenceNumber::from(20u64)
    );
    let user_a1 = get_user(engine.as_ref(), &room_a, 1).await.unwrap();
    let user_b1 = get_user(engine.as_ref(), &room_b, 1).await.unwrap();
    assert_eq!(user_a1[1], Value::String("User A 1".into()));
    assert_eq!(user_b1[1], Value::String("User B 1".into()));
}

#[tokio::test]
async fn test_snapshot_create_and_restore() {
    let engine_a = MemoryStorageEngine::new();
    let room_id = RoomId::new("room-snap").unwrap();
    engine_a.open_room(&room_id, users_schema()).await.unwrap();

    let snapshot_row = row(42, "Snapshot User", 999, true);
    engine_a
        .apply_batch(&room_id, vec![insert(1, 42, snapshot_row.clone(), 100)])
        .await
        .unwrap();

    let snapshot_bytes = engine_a.create_snapshot(&room_id).await.unwrap();
    assert!(!snapshot_bytes.is_empty());

    let engine_b = MemoryStorageEngine::new();
    engine_b.open_room(&room_id, users_schema()).await.unwrap();
    let restored_head = engine_b
        .apply_snapshot(&room_id, users_schema(), snapshot_bytes.into(), 1u64.into())
        .await
        .unwrap();
    assert_eq!(restored_head, SequenceNumber::from(1u64));
    assert_eq!(get_user(&engine_b, &room_id, 42).await, Some(snapshot_row));
    let head_b = engine_b.get_head_seq(&room_id).await.unwrap();
    assert_eq!(head_b, SequenceNumber::from(1u64));
}

#[tokio::test]
async fn test_sequence_mismatch_rejected() {
    sequence_gap_is_rejected(&MemoryStorageEngine::new()).await;
}

#[tokio::test]
async fn test_dynamic_column_update_resizing_memory() {
    let room_id = RoomId::new("room-dynamic-col").unwrap();
    column_update_grows_row(&MemoryStorageEngine::new(), &room_id).await;
}

/// `notes(id, author, body)` owned by `author`, outside the key, and `votes(voter, choice)` owned by
/// `voter`, its key, and immutable.
fn owned_schema() -> zemdb_core::Schema {
    use zemdb_core::{DataType, OwnerIdentity, Schema, TableSchema};
    Schema::builder()
        .table(
            TableSchema::builder("notes")
                .primary_key("id", DataType::Int)
                .column("author", DataType::String)
                .nullable_column("body", DataType::String)
                .owner("author", OwnerIdentity::ClientId),
        )
        .table(
            TableSchema::builder("votes")
                .primary_key("voter", DataType::String)
                .column("choice", DataType::Int)
                .owner("voter", OwnerIdentity::ClientId)
                .immutable(),
        )
        .build()
}

/// What the server rejects before sequencing (an update or delete of an immutable table, or an
/// operation that does not declare the owner of a table whose owner is outside the key) can
/// only reach a replica in a damaged log: the whole batch is rejected and nothing changes.
async fn operations_the_server_rejects_are_a_damaged_batch<E: StorageEngine>(engine: &E) {
    use zemdb_core::{ColumnUpdate, CompactRow, Operation, ValidationError};
    let room = RoomId::new("owned").unwrap();
    engine.open_room(&room, owned_schema()).await.unwrap();
    let note = sequenced(
        1,
        Operation::insert(
            0,
            PrimaryKey::single(1i64),
            CompactRow::new(vec![1i64.into(), "alice".into()]),
            1,
        ),
    );
    let vote_key = || PrimaryKey::single("alice");
    let damaged = [
        sequenced(2, Operation::delete(0, PrimaryKey::single(1i64), 1)),
        sequenced(
            2,
            Operation::update(
                0,
                PrimaryKey::single(1i64),
                vec![ColumnUpdate::new(2, "x")],
                1,
            ),
        ),
        sequenced(
            2,
            Operation::update(1, vote_key(), vec![ColumnUpdate::new(1, 2i64)], 1),
        ),
        sequenced(2, Operation::delete(1, vote_key(), 1)),
    ];
    for op in damaged {
        let err = engine
            .apply_batch(&room, vec![note.clone(), op.clone()])
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                StorageError::SchemaValidation(
                    ValidationError::MissingOwner { .. } | ValidationError::ImmutableTable { .. }
                )
            ),
            "{op:?}: {err:?}"
        );
        assert_eq!(engine.get_head_seq(&room).await.unwrap(), 0u64.into());
        assert_eq!(
            engine
                .get(&room, "notes", &PrimaryKey::single(1i64))
                .await
                .unwrap(),
            None
        );
    }
    let delete = Operation::delete_with_owner(0, PrimaryKey::single(1i64), "alice", 1);
    engine
        .apply_batch(&room, vec![note, sequenced(2, delete)])
        .await
        .unwrap();
    assert_eq!(engine.get_head_seq(&room).await.unwrap(), 2u64.into());
}

#[tokio::test]
async fn memory_rejects_a_batch_with_an_operation_the_server_rejects() {
    operations_the_server_rejects_are_a_damaged_batch(&MemoryStorageEngine::new()).await;
}

#[tokio::test]
async fn disk_rejects_a_batch_with_an_operation_the_server_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = disk_engine(dir.path());
    operations_the_server_rejects_are_a_damaged_batch(&engine).await;
    let room = RoomId::new("owned").unwrap();
    engine.close_room(&room).await.unwrap();
    drop(engine);

    // The delete that declares its owner is read back from the WAL and applied again.
    let engine = disk_engine(dir.path());
    engine.open_room(&room, owned_schema()).await.unwrap();
    assert_eq!(engine.get_head_seq(&room).await.unwrap(), 2u64.into());
    let key = PrimaryKey::single(1i64);
    assert_eq!(engine.get(&room, "notes", &key).await.unwrap(), None);
}
