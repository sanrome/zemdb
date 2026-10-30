//! The schema of the replicas of the client, against the server in process: learned from
//! `Registered` only, switched to only if it evolves from the replica's, and failing the room
//! closed if it does not.

mod common;

/// The generator of the storage's randomized tests, which its reference model uses.
#[path = "../../storage/tests/common/rng.rs"]
#[allow(dead_code)]
mod rng;

/// The reference model of a replica of the storage's tests, which shares no code with the
/// engines.
#[path = "../../storage/tests/common/oracle.rs"]
#[allow(dead_code)]
mod oracle;

use common::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use zemdb_client::{
    Client, ClientConfig, ClientError, FileMetadata, MetadataStore, RoomMetadata, RoomStatus,
    TransportClient,
};
use zemdb_core::ClientMessageKind::Sync;
use zemdb_core::*;
use zemdb_storage::{StorageEngine, StorageError};

fn tasks_id() -> SchemaId {
    SchemaId::new("tasks").unwrap()
}

fn tag_column() -> ColumnDef {
    ColumnDef::new("tag", DataType::String).nullable(true)
}

fn tag_unique() -> UniqueConstraint {
    UniqueConstraint::new("tasks_tag", ["tag"])
}

/// [`tasks_schema`] with the nullable string column `tag`, unique: its version 2.
fn tagged_schema() -> Schema {
    let mut schema = tasks_schema();
    let added =
        schema.add_column_with_constraints("tasks", tag_column(), vec![tag_unique()], vec![]);
    added.unwrap();
    schema
}

/// Adds to the schema `tasks` of `server` the column of [`tagged_schema`], and reloads it in its
/// rooms.
async fn add_tag_column(server: &TestServer) {
    let registry = &server.state.schema_registry;
    let added = registry.add_column_with_constraints(
        &tasks_id(),
        "tasks",
        tag_column(),
        vec![tag_unique()],
        vec![],
    );
    added.unwrap();
    server
        .state
        .room_manager
        .reload_schema_for_rooms(&tasks_id())
        .await;
}

/// An insert of row `id` with `tag` into `tasks` of [`tagged_schema`].
fn insert_tagged(id: i64, tag: &str) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("title", "task")
        .set("tag", tag)
        .build();
    tagged_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap()
}

/// An update of the tag of row `id` of `tasks` of [`tagged_schema`].
fn update_tag(id: i64, tag: &str) -> Operation {
    let fields = BTreeMap::from([("tag".to_string(), Value::from(tag))]);
    let pk = PrimaryKey::single(id);
    tagged_schema()
        .to_operation_update("tasks", pk, &fields, 1000)
        .unwrap()
}

/// [`tasks_schema`] with `title` an int, which does not evolve from it.
fn retyped_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::Int);
    Schema::builder().table(table).build()
}

/// A `SyncBatch` for the sync `correlation_id` of `room_id`, which announces the version
/// `version` of the schema `schema_id`.
fn batch(
    correlation_id: CorrelationId,
    room_id: &RoomId,
    head_seq: u64,
    ops: &[SequencedOperation],
    (schema_id, version): (&str, u64),
) -> Vec<u8> {
    encode_message(&ServerMessage::SyncBatch {
        correlation_id,
        room_id: room_id.clone(),
        head_seq: SequenceNumber::new(head_seq),
        ops: ops.to_vec(),
        has_more: false,
        snapshot_wanted: false,
        schema_version: version,
        schema_id: SchemaId::new(schema_id).unwrap(),
    })
    .unwrap()
}

/// `Registered` for the registration `correlation_id` of `room_id`, at the head 3 of a replica,
/// with `schema` as the schema `schema_id`.
fn registered(
    correlation_id: CorrelationId,
    room_id: &RoomId,
    room_incarnation: u128,
    schema_id: &str,
    schema: Schema,
) -> Vec<u8> {
    encode_message(&ServerMessage::Registered {
        correlation_id,
        room_id: room_id.clone(),
        head_seq: SequenceNumber::new(3),
        tail_seq: SequenceNumber::new(0),
        schema_id: SchemaId::new(schema_id).unwrap(),
        schema,
        active_snapshot_seq: None,
        room_incarnation,
    })
    .unwrap()
}

async fn metadata(dir: &Path, room_id: &RoomId) -> RoomMetadata {
    FileMetadata::new(dir).room(room_id).await.unwrap().unwrap()
}

/// The rows of the tasks 1 to 9 of `room_id` in `storage`, as the reference model keeps them.
async fn rows(storage: &impl StorageEngine, room_id: &RoomId) -> oracle::Rows {
    let mut table = BTreeMap::new();
    for id in 1..=9 {
        let pk = PrimaryKey::single(id);
        if let Some(row) = storage.get_by_id(room_id, 0, &pk).await.unwrap() {
            table.insert(pk, row);
        }
    }
    oracle::Rows::from([(0, table)])
}

/// Whether the server holds `client` in the roster of `room_id`: it refuses a sync of a client
/// that is not with `ClientNotRegistered`.
async fn in_roster(server: &TestServer, client: &Client, room_id: &RoomId) -> bool {
    let message = ClientMessage::Sync {
        correlation_id: CorrelationId::new(u64::MAX),
        room_id: room_id.clone(),
        client_id: client.client_id(),
        from_seq: SequenceNumber::new(0),
        max_batch_size: 1,
    };
    let token = token(&client.client_id(), room_id);
    let reply = server.transport().send(&message, &token).await.unwrap();
    let reply = decode_message(&reply).unwrap();
    !matches!(
        reply,
        ServerMessage::Error {
            code: ErrorCode::ClientNotRegistered,
            ..
        }
    )
}

/// Waits until `room_id` of `client` fails closed.
async fn until_failed_closed(client: &Client, room_id: &RoomId) {
    let failed = |status| {
        matches!(
            status,
            Ok(RoomStatus {
                failed_closed: true,
                ..
            })
        )
    };
    while !failed(client.status(room_id.clone()).await) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn failed_closed(head_seq: u64) -> RoomStatus {
    RoomStatus {
        failed_closed: true,
        ..status(head_seq, 1, false)
    }
}

#[tokio::test]
async fn a_column_added_in_the_middle_of_the_log_is_saved_and_reloaded_before_its_operations() {
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
    let mut oracle = oracle::Oracle::new(tasks_schema());
    oracle.apply_batch(&server.commit(&room_id, 1..=3).await);
    client.sync(room_id.clone()).await.unwrap();

    add_tag_column(&server).await;
    oracle.evolve(tagged_schema());
    let ops = [
        insert_tagged(4, "a"),
        insert_tagged(5, "a"),
        update_tag(1, "a"),
        update_tag(1, "b"),
        insert_tagged(6, "b"),
    ];
    let ops = server.commit_ops(&room_id, ops).await;
    // The new constraint judges the operations after it.
    let unique = oracle::Reason::Unique("tasks_tag".to_string());
    let rejected = oracle.apply_batch(&ops);
    assert_eq!(
        rejected,
        [(5, unique.clone()), (6, unique.clone()), (8, unique)]
    );

    // When the storage room takes the new schema, the metadata hold it already.
    let saved = Arc::new(Mutex::new(None));
    let (path, saved_then) = (dir.path().join("room_room-a.meta"), Arc::clone(&saved));
    storage.before_next_reload(move || {
        *saved_then.lock().unwrap() = Some(std::fs::read(path).unwrap());
    });
    client.sync(room_id.clone()).await.unwrap();

    // The batch announced the new version: the replica registered again with its cursor, and
    // took the schema of `Registered`.
    let incarnation = server.incarnation(&room_id).await;
    let registrations = [(None, None), (Some(3), Some(incarnation))];
    assert_eq!(transport.registrations(), registrations);
    let expected = RoomMetadata {
        room_incarnation: incarnation,
        schema_id: tasks_id(),
        schema: tagged_schema(),
    };
    let image = tempdir().unwrap();
    let saved = saved
        .lock()
        .unwrap()
        .take()
        .expect("the schema was reloaded");
    std::fs::write(image.path().join("room_room-a.meta"), saved).unwrap();
    assert_eq!(metadata(image.path(), &room_id).await, expected);
    assert_eq!(metadata(dir.path(), &room_id).await, expected);
    // It applied the operations with the decisions of the reference model.
    assert_eq!(client.status(room_id.clone()).await, Ok(status(8, 1, true)));
    let replica = rows(&storage, &room_id).await;
    assert_eq!(oracle::dump(&replica), oracle::dump(oracle.rows()));
}

/// Copies the files of `from` into `to`, but for their locks, which a process holds.
#[cfg(unix)]
fn copy_files(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap();
        if path.is_file() && !name.to_str().unwrap().ends_with(".lock") {
            std::fs::copy(&path, to.join(name)).unwrap();
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn a_cut_between_saving_a_new_schema_and_reloading_it_opens_with_the_new_one() {
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (server, room_id, _, storage, client) = setup(dir.path(), storage).await;
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();
    client.close_room(room_id.clone()).await.unwrap();
    add_tag_column(&server).await;

    // The files when the storage room takes the new schema, as a cut of the process leaves them.
    let image = tempdir().unwrap();
    let (from, to) = (dir.path().to_path_buf(), image.path().to_path_buf());
    storage.before_next_reload(move || copy_files(&from, &to));
    open_room(&client, &room_id).await.unwrap();
    client.shutdown().await.unwrap();

    // They hold the new schema, and the replica opens with it from its cursor, without
    // reloading it.
    let schema = metadata(image.path(), &room_id).await.schema;
    assert_eq!(schema, tagged_schema());
    let storage = TestEngine::new(engine(image.path()));
    let no_reload = StorageError::Other("A reload".to_string());
    storage.fail_next("reload_schema", no_reload);
    let transport = server.transport();
    let client = open_client(image.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    let cursor = (Some(3), Some(server.incarnation(&room_id).await));
    assert_eq!(transport.registrations(), [cursor]);
    let ops = [insert_tagged(4, "a"), insert_tagged(5, "a")];
    server.commit_ops(&room_id, ops).await;
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(5, 0, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4]);
}

#[tokio::test]
async fn a_reply_with_an_earlier_version_of_the_schema_applies_with_the_local_one() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    add_tag_column(&server).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = engine(dir.path());
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(metadata(dir.path(), &room_id).await.schema, tagged_schema());

    // Its operations need the local version, and apply with it, without registering.
    let ops = [insert_tagged(1, "a"), insert_tagged(2, "b")];
    let ops = server.commit_ops(&room_id, ops).await;
    answer_next(&transport, Sync, move |id, room| {
        batch(id, room, 2, &ops, ("tasks", 1))
    });
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(transport.registrations(), [(None, None)]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(2, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2]);

    // Neither is `Registered` with the earlier version: the replica keeps its own, without
    // reloading.
    client.close_room(room_id.clone()).await.unwrap();
    let storage = TestEngine::new(storage);
    let no_reload = StorageError::Other("A reload".to_string());
    storage.fail_next("reload_schema", no_reload);
    client.shutdown().await.unwrap();
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    let incarnation = server.incarnation(&room_id).await;
    transport.answer_next_registration(move |id, room| {
        registered(id, room, incarnation, "tasks", tasks_schema())
    });
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(2, 0, true)));
    assert_eq!(metadata(dir.path(), &room_id).await.schema, tagged_schema());
}

#[tokio::test]
async fn a_reply_that_announces_another_schema_registers_again_and_decides_with_registered() {
    for announced in [("tasks", 2), ("other", 1)] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, storage, client) =
            setup(dir.path(), engine(dir.path())).await;
        let ops = server.commit(&room_id, 1..=3).await;
        answer_next(&transport, Sync, move |id, room| {
            batch(id, room, 3, &ops, announced)
        });
        client.sync(room_id.clone()).await.unwrap();

        // `Registered` has the schema of the replica, so nothing changes, and the batch asked for
        // again applies.
        let incarnation = server.incarnation(&room_id).await;
        let registrations = [(None, None), (Some(0), Some(incarnation))];
        assert_eq!(transport.registrations(), registrations, "{announced:?}");
        let schema = metadata(dir.path(), &room_id).await.schema;
        assert_eq!(schema, tasks_schema(), "{announced:?}");
        let sent = [("sync", 0), ("sync", 0), ("ack", 3)];
        assert_eq!(requests(&transport), sent, "{announced:?}");
        assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3], "{announced:?}");
    }
}

#[tokio::test]
async fn a_server_that_keeps_announcing_another_schema_ends_the_sync() {
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    transport.answer_every(Sync, |message| {
        let (id, room) = request(message);
        batch(id, &room, 0, &[], ("tasks", 2))
    });
    let synced = client.sync(room_id.clone()).await;
    assert!(matches!(synced, Err(ClientError::Network(_))), "{synced:?}");
    assert_eq!(transport.registrations().len(), 2);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
}

#[tokio::test]
async fn a_room_reassigned_to_a_schema_that_extends_its_own_moves_the_replica_to_it() {
    let dir = tempdir().unwrap();
    let (server, room_id, _, storage, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();

    let noted = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .nullable_column("note", DataType::String);
    let noted = Schema::builder().table(noted).build();
    let noted_id = SchemaId::new("noted").unwrap();
    let registry = &server.state.schema_registry;
    registry
        .register_schema(noted_id.clone(), noted.clone())
        .unwrap();
    let manager = &server.state.room_manager;
    manager.shutdown_room(&room_id).await;
    manager
        .get_or_spawn(&room_id, Some(&noted_id))
        .await
        .unwrap();
    let row = RowBuilder::new()
        .set("id", 4)
        .set("title", "task")
        .set("note", "noted")
        .build();
    let op = noted.to_operation_insert("tasks", &row, 1000).unwrap();
    server.commit_ops(&room_id, [op]).await;

    client.sync(room_id.clone()).await.unwrap();
    let expected = RoomMetadata {
        room_incarnation: server.incarnation(&room_id).await,
        schema_id: noted_id,
        schema: noted,
    };
    assert_eq!(metadata(dir.path(), &room_id).await, expected);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(4, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4]);
}

#[tokio::test]
async fn a_schema_that_does_not_evolve_from_the_replicas_fails_the_room_closed() {
    // Another schema, or a later version of its own, that changes the type of a column.
    let mut retyped_later = retyped_schema();
    let note = ColumnDef::new("note", DataType::String).nullable(true);
    retyped_later.add_column("tasks", note).unwrap();
    for (schema_id, schema) in [("retyped", retyped_schema()), ("tasks", retyped_later)] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, storage, client) =
            setup(dir.path(), engine(dir.path())).await;
        server.commit(&room_id, 1..=3).await;
        client.sync(room_id.clone()).await.unwrap();
        client.close_room(room_id.clone()).await.unwrap();
        assert!(in_roster(&server, &client, &room_id).await);

        let incarnation = server.incarnation(&room_id).await;
        transport.answer_next_registration(move |id, room| {
            registered(id, room, incarnation, schema_id, schema)
        });
        let failed = Err(ClientError::FailedClosed(room_id.clone()));
        assert_eq!(open_room(&client, &room_id).await, failed, "{schema_id}");
        // It left the roster, once.
        assert_eq!(deregistrations(&transport), 1, "{schema_id}");
        assert!(!in_roster(&server, &client, &room_id).await, "{schema_id}");
        // The replica stays open with its schema, to be read.
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(failed_closed(3)), "{schema_id}");
        let local = metadata(dir.path(), &room_id).await;
        let local = (local.schema_id, local.schema);
        assert_eq!(local, (tasks_id(), tasks_schema()), "{schema_id}");
        assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3], "{schema_id}");

        // It neither registers nor syncs.
        server.commit(&room_id, 4..=5).await;
        let sent = transport.sent().len();
        assert_eq!(client.sync(room_id.clone()).await, failed, "{schema_id}");
        assert_eq!(open_room(&client, &room_id).await, failed, "{schema_id}");
        assert_eq!(transport.sent().len(), sent, "{schema_id}");
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(failed_closed(3)), "{schema_id}");
        assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3], "{schema_id}");

        // Opened again, it tries again, and the schema of the server is its own.
        client.close_room(room_id.clone()).await.unwrap();
        open_room(&client, &room_id).await.unwrap();
        client.sync(room_id.clone()).await.unwrap();
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(5, 1, true)), "{schema_id}");
        assert_eq!(deregistrations(&transport), 1, "{schema_id}");
    }
}

#[tokio::test]
async fn a_reply_that_comes_after_the_room_failed_closed_neither_applies_nor_registers() {
    for late_reply in ["batch", "not registered", "behind"] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, storage, client) =
            setup(dir.path(), engine(dir.path())).await;
        server.commit(&room_id, 1..=3).await;
        client.sync(room_id.clone()).await.unwrap();

        // A sync whose reply comes once another sync learned a schema that does not evolve from
        // the replica's: a batch with more operations, `ClientNotRegistered` or `BehindCompaction`.
        let ops = server.commit(&room_id, 4..=5).await;
        let incarnation = server.incarnation(&room_id).await;
        let mut syncs = 0;
        transport.answer(move |message| match message {
            ClientMessage::Sync {
                correlation_id,
                room_id,
                ..
            } => {
                syncs += 1;
                let id = *correlation_id;
                Some(Ok(match (syncs, late_reply) {
                    (1, "batch") => batch(id, room_id, 5, &ops, ("tasks", 1)),
                    (1, "not registered") => {
                        error_reply(id, room_id, ErrorCode::ClientNotRegistered)
                    }
                    (1, _) => error_reply(id, room_id, ErrorCode::BehindCompaction),
                    _ => batch(id, room_id, 3, &[], ("tasks", 2)),
                }))
            }
            ClientMessage::RegisterClient {
                correlation_id,
                room_id,
                ..
            } => Some(Ok(registered(
                *correlation_id,
                room_id,
                incarnation,
                "retyped",
                retyped_schema(),
            ))),
            _ => None,
        });
        let mut held = transport.hold_next_reply(Sync);
        let late = client.sync(room_id.clone());
        let failed = Err(ClientError::FailedClosed(room_id.clone()));
        let meanwhile = async {
            held.replied().await;
            assert_eq!(client.sync(room_id.clone()).await, failed);
            held.release();
        };
        let (late, ()) = tokio::join!(late, meanwhile);
        assert_eq!(late, failed, "{late_reply}");
        let registrations = [(None, None), (Some(3), Some(incarnation))];
        assert_eq!(transport.registrations(), registrations, "{late_reply}");
        assert_eq!(deregistrations(&transport), 1, "{late_reply}");
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(failed_closed(3)), "{late_reply}");
        assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3], "{late_reply}");
    }
}

#[tokio::test]
async fn a_registration_answered_after_the_room_failed_closed_changes_nothing() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();

    // A sync is told to register again, and then gets an operation that does not validate,
    // while another sync, told to register again too, waits for its registration.
    let op = SequencedOperation::new(SequenceNumber::new(4), insert_tagged(4, "a"));
    let mut syncs = 0;
    transport.answer(move |message| match message {
        ClientMessage::Sync {
            correlation_id,
            room_id,
            ..
        } => {
            syncs += 1;
            let id = *correlation_id;
            Some(Ok(match syncs {
                2 => batch(id, room_id, 4, std::slice::from_ref(&op), ("tasks", 1)),
                _ => error_reply(id, room_id, ErrorCode::ClientNotRegistered),
            }))
        }
        _ => None,
    });
    let mut first = transport.hold_next_reply(Sync);
    let mut second = transport.hold_next_reply(Sync);
    let failing = client.sync(room_id.clone());
    let failed = Err(ClientError::FailedClosed(room_id.clone()));
    let meanwhile = async {
        first.replied().await;
        first.release();
        second.replied().await;
        // The registration reaches the server late, and its reply comes once the room failed
        // closed: the failed sync waits for it before it deregisters.
        transport.slow_next(
            ClientMessageKind::RegisterClient,
            Duration::from_millis(300),
        );
        let late = transport.hold_next_reply(ClientMessageKind::RegisterClient);
        let registering = client.sync(room_id.clone());
        let release = async {
            while transport.registrations().len() < 3 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            second.release();
            until_failed_closed(&client, &room_id).await;
            late.release();
        };
        let (registering, ()) = tokio::join!(registering, release);
        assert_eq!(registering, failed);
    };
    let (failing, ()) = tokio::join!(failing, meanwhile);
    assert_eq!(failing, failed);
    // The registration did not register the replica that failed closed, and the client left
    // the roster after it.
    assert_eq!(client.status(room_id.clone()).await, Ok(failed_closed(3)));
    assert_eq!(deregistrations(&transport), 1);
    assert!(!in_roster(&server, &client, &room_id).await);
    assert_eq!(client.sync(room_id.clone()).await, failed);
}

#[tokio::test]
async fn a_deregistration_is_not_sent_once_the_room_opened_again() {
    for reopen in [true, false] {
        let dir = tempdir().unwrap();
        let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
        server.commit(&room_id, 1..=3).await;
        client.sync(room_id.clone()).await.unwrap();

        // As in the test above, a sync fails the room closed while another sync waits for its
        // registration. The room is closed then, and opened again by an open that waits for the
        // registration lock before the failed sync.
        let op = SequencedOperation::new(SequenceNumber::new(4), insert_tagged(4, "a"));
        let mut syncs = 0;
        transport.answer(move |message| match message {
            ClientMessage::Sync {
                correlation_id,
                room_id,
                ..
            } => {
                syncs += 1;
                let id = *correlation_id;
                Some(Ok(match syncs {
                    2 => batch(id, room_id, 4, std::slice::from_ref(&op), ("tasks", 1)),
                    _ => error_reply(id, room_id, ErrorCode::ClientNotRegistered),
                }))
            }
            _ => None,
        });
        let mut first = transport.hold_next_reply(Sync);
        let mut second = transport.hold_next_reply(Sync);
        let failing = client.sync(room_id.clone());
        let meanwhile = async {
            first.replied().await;
            first.release();
            second.replied().await;
            let late = transport.hold_next_reply(ClientMessageKind::RegisterClient);
            let registering = client.sync(room_id.clone());
            let reopening = async {
                while transport.registrations().len() < 3 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let opening = async {
                    if reopen {
                        open_room(&client, &room_id).await.unwrap();
                    }
                };
                let closing = async {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    second.release();
                    until_failed_closed(&client, &room_id).await;
                    client.close_room(room_id.clone()).await.unwrap();
                    late.release();
                };
                tokio::join!(opening, closing);
            };
            let (registering, ()) = tokio::join!(registering, reopening);
            assert!(registering.is_err(), "{reopen}: {registering:?}");
        };
        let (failing, ()) = tokio::join!(failing, meanwhile);
        let failed = Err(ClientError::FailedClosed(room_id.clone()));
        assert_eq!(failing, failed, "{reopen}");
        // Opened again, the replica is registered, and the client stays in the roster; only
        // closed, it leaves it.
        let status_now = client.status(room_id.clone()).await;
        let registered = Ok(status(3, 1, true));
        let closed = Err(ClientError::RoomClosed(room_id.clone()));
        assert_eq!(status_now, if reopen { registered } else { closed });
        assert_eq!(
            deregistrations(&transport),
            usize::from(!reopen),
            "{reopen}"
        );
        let in_roster = in_roster(&server, &client, &room_id).await;
        assert_eq!(in_roster, reopen, "{reopen}");
    }
}

#[tokio::test]
async fn an_operation_that_does_not_validate_with_the_schema_of_registered_fails_closed() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();

    // Every batch announces the schema of the replica, with an operation of the next version.
    let op = SequencedOperation::new(SequenceNumber::new(4), insert_tagged(4, "a"));
    transport.answer_every(Sync, move |message| {
        let (id, room) = request(message);
        batch(id, &room, 4, std::slice::from_ref(&op), ("tasks", 1))
    });
    let synced = client.sync(room_id.clone()).await;
    assert_eq!(synced, Err(ClientError::FailedClosed(room_id.clone())));
    // It registered again once, with its cursor, and then left the roster.
    let incarnation = server.incarnation(&room_id).await;
    let registrations = [(None, None), (Some(3), Some(incarnation))];
    assert_eq!(transport.registrations(), registrations);
    assert_eq!(deregistrations(&transport), 1);
    assert!(!in_roster(&server, &client, &room_id).await);
    assert_eq!(client.status(room_id.clone()).await, Ok(failed_closed(3)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);
}

#[tokio::test]
async fn an_operation_that_does_not_validate_registers_again_and_applies_with_the_new_schema() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();

    // A batch that announces the schema of the replica, with an operation of the next version.
    add_tag_column(&server).await;
    let ops = server.commit_ops(&room_id, [insert_tagged(4, "a")]).await;
    answer_next(&transport, Sync, move |id, room| {
        batch(id, room, 4, &ops, ("tasks", 1))
    });
    client.sync(room_id.clone()).await.unwrap();
    let incarnation = server.incarnation(&room_id).await;
    let registrations = [(None, None), (Some(3), Some(incarnation))];
    assert_eq!(transport.registrations(), registrations);
    assert_eq!(metadata(dir.path(), &room_id).await.schema, tagged_schema());
    assert_eq!(client.status(room_id.clone()).await, Ok(status(4, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4]);
}

#[tokio::test]
async fn a_failed_save_or_reload_of_a_new_schema_leaves_the_room_out() {
    for failing in ["save", "reload"] {
        let dir = tempdir().unwrap();
        let server = TestServer::new();
        let room_id = server.create_room("room-a").await;
        let transport = server.transport();
        let files = TestFiles::default();
        let storage = TestEngine::new(engine(dir.path()));
        let client = Client::open(
            ClientConfig::new("ana"),
            storage.clone(),
            FileMetadata::with_files(dir.path(), files.clone()),
            transport.clone(),
        );
        let client = client.await.unwrap();
        open_room(&client, &room_id).await.unwrap();
        server.commit(&room_id, 1..=3).await;
        client.sync(room_id.clone()).await.unwrap();

        add_tag_column(&server).await;
        match failing {
            "save" => files.fail_next("rename", "room_room-a.meta"),
            _ => {
                let error = StorageError::Other("injected".to_string());
                storage.fail_next("reload_schema", error);
            }
        }
        let synced = client.sync(room_id.clone()).await;
        assert!(
            matches!(synced, Err(ClientError::Storage(_))),
            "{failing}: {synced:?}"
        );
        let closed = Err(ClientError::RoomClosed(room_id.clone()));
        assert_eq!(client.status(room_id.clone()).await, closed, "{failing}");
        // The metadata hold the schema that the replica opens with next.
        let saved = match failing {
            "save" => tasks_schema(),
            _ => tagged_schema(),
        };
        assert_eq!(
            metadata(dir.path(), &room_id).await.schema,
            saved,
            "{failing}"
        );

        open_room(&client, &room_id).await.unwrap();
        server.commit_ops(&room_id, [insert_tagged(4, "a")]).await;
        client.sync(room_id.clone()).await.unwrap();
        let status_now = client.status(room_id.clone()).await;
        assert_eq!(status_now, Ok(status(4, 1, true)), "{failing}");
        let schema = metadata(dir.path(), &room_id).await.schema;
        assert_eq!(schema, tagged_schema(), "{failing}");
        assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4], "{failing}");
        assert_eq!(deregistrations(&transport), 0, "{failing}");
    }
}
