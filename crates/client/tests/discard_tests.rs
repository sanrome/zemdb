//! Discarding the local replica of a room at the request of the application, against the server
//! in process: in any state of the room, with the steps of the automatic discard in their order,
//! ending its writes in flight and its `watch` streams, and leaving the room out of the client,
//! for its next open to open it as new from the server.

mod common;

use common::*;
use futures::StreamExt;
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use zemdb_client::{
    ChangeStream, Client, ClientConfig, ClientError, FileMetadata, Key, MetadataStore, RoomHandle,
    RoomMetadata, RoomStatus, Row, Scan, ZemdbClient,
};
use zemdb_core::ClientMessageKind::{Commit, RegisterClient, RequestSnapshotChunk, Sync};
use zemdb_core::*;
use zemdb_storage::{DiskStorageEngine, StorageEngine, StorageError};

// ---------------------------------------------------------------------------------------------
// Helpers

fn closed<T>(room_id: &RoomId) -> Result<T, ClientError> {
    Err(ClientError::RoomClosed(room_id.clone()))
}

fn tasks_id() -> SchemaId {
    SchemaId::new("tasks").unwrap()
}

/// [`tasks_schema`] with `title` an int, which does not evolve from it.
fn retyped_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::Int);
    Schema::builder().table(table).build()
}

/// `Registered` for the registration `correlation_id` of `room_id`, at the head 2 of a replica,
/// with [`retyped_schema`] as the schema `retyped`.
fn retyped_registered(
    correlation_id: CorrelationId,
    room_id: &RoomId,
    incarnation: u128,
) -> Vec<u8> {
    encode_message(&ServerMessage::Registered {
        correlation_id,
        room_id: room_id.clone(),
        head_seq: SequenceNumber::new(2),
        tail_seq: SequenceNumber::new(0),
        schema_id: SchemaId::new("retyped").unwrap(),
        schema: retyped_schema(),
        active_snapshot_seq: None,
        room_incarnation: incarnation,
    })
    .unwrap()
}

/// The nullable string column `tag`.
fn tag() -> ColumnDef {
    ColumnDef::new("tag", DataType::String).nullable(true)
}

/// An insert of row `id` with the tag `new` into `tasks` of `schema`, which has the column.
fn insert_tagged(schema: &Schema, id: i64) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("title", "task")
        .set("tag", "new");
    schema
        .to_operation_insert("tasks", &row.build(), 1000)
        .unwrap()
}

/// [`tasks_schema`] with its table under another id: the files of a replica of it hold data of
/// a table that this schema does not have.
fn moved_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .table_id(4)
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    Schema::builder().table(table).build()
}

/// [`tasks_schema`] with its titles unique, which the tasks of a replica, all titled `task`,
/// break.
fn unique_titles_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .unique("tasks_title", ["title"]);
    Schema::builder().table(table).build()
}

async fn metadata(dir: &Path, room_id: &RoomId) -> Option<RoomMetadata> {
    FileMetadata::new(dir).room(room_id).await.unwrap()
}

/// Leaves in `dir` the replica of `room_id` of a client that opened it, synced the tasks 1 to 3
/// committed to the server, and shut down. Returns the incarnation of the room.
async fn replica(server: &TestServer, dir: &Path, room_id: &RoomId) -> u128 {
    server.commit(room_id, 1..=3).await;
    let client = open_client(dir, engine(dir), &server.transport()).await;
    open_room(&client, room_id).await.unwrap();
    client.sync(room_id.clone()).await.unwrap();
    client.shutdown().await.unwrap();
    server.incarnation(room_id).await
}

/// Moves the operations of the replica of `room_id` in `dir` into its snapshot.
async fn compact(dir: &Path, room_id: &RoomId) {
    let storage = engine(dir);
    storage.open_room(room_id, tasks_schema()).await.unwrap();
    storage.compact_room(room_id).await.unwrap();
    storage.close_room(room_id).await.unwrap();
}

/// The head and the tasks that the files of `room_id` in `dir` hold, opened with the schema of
/// the tasks.
async fn on_disk(dir: &Path, room_id: &RoomId) -> (u64, Vec<i64>) {
    let storage = engine(dir);
    storage.open_room(room_id, tasks_schema()).await.unwrap();
    let head = storage.get_head_seq(room_id).await.unwrap().get();
    let held = tasks(&storage, room_id).await;
    storage.close_room(room_id).await.unwrap();
    (head, held)
}

/// The facade of ana over `storage`, with her metadata in `dir`, without a `CryptoEngine`.
async fn facade(
    dir: &Path,
    storage: impl StorageEngine + 'static,
    transport: &TestTransport,
) -> ZemdbClient {
    let metadata = FileMetadata::new(dir);
    let client = ZemdbClient::open(
        ClientConfig::new("ana"),
        storage,
        metadata,
        transport.clone(),
        TestTokens::new(),
        None,
    );
    client.await.unwrap()
}

/// Opens `room_id` through `client`, with a token of the client.
async fn room(client: &ZemdbClient, room_id: &RoomId) -> Result<RoomHandle, ClientError> {
    let token = token(&client.client_id(), room_id);
    client.room(room_id.clone(), token).await
}

fn task_key(id: i64) -> Key {
    Key::new().with("id", id)
}

/// The row of the task `id` titled `title`, as a read gives it.
fn task(id: i64, title: impl Into<Value>) -> Row {
    Row::new().with("id", id).with("title", title)
}

/// The next element of `stream`, which comes within 10 s.
async fn next<T>(stream: &mut ChangeStream<T>) -> Option<Result<T, ClientError>> {
    let next = tokio::time::timeout(Duration::from_secs(10), stream.next()).await;
    next.expect("The stream gave nothing")
}

/// Checks that `future` does not end for a while, as it waits for something held.
async fn pending<T: std::fmt::Debug>(future: impl Future<Output = T>) {
    let ended = tokio::time::timeout(Duration::from_millis(200), future).await;
    assert!(ended.is_err(), "It ended: {ended:?}");
}

/// The status of a room open and registered at `head_seq` in the epoch `epoch`, with `watches`
/// subscriptions.
fn watched(head_seq: u64, epoch: u64, watches: usize) -> RoomStatus {
    RoomStatus {
        watches,
        ..status(head_seq, epoch, true)
    }
}

// ---------------------------------------------------------------------------------------------
// The states of a room

#[tokio::test]
async fn a_room_that_fails_closed_is_discarded_and_opens_with_the_schema_and_state_of_the_server() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=2).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport).await;
    let handle = room(&client, &room_id).await.unwrap();
    handle.sync().await.unwrap();
    handle.close().await.unwrap();
    // Meanwhile the schema of the room gets a column, and a member writes with it.
    let registry = &server.state.schema_registry;
    let added = registry.add_column_with_constraints(&tasks_id(), "tasks", tag(), vec![], vec![]);
    let tagged = Schema::clone(&added.unwrap());
    let manager = &server.state.room_manager;
    manager.reload_schema_for_rooms(&tasks_id()).await;
    server
        .commit_ops(&room_id, [insert_tagged(&tagged, 3)])
        .await;
    // A registration that announces a schema that does not evolve from that of the replica.
    let incarnation = server.incarnation(&room_id).await;
    transport.answer_next_registration(move |correlation_id, room_id| {
        retyped_registered(correlation_id, room_id, incarnation)
    });
    let handle = room(&client, &room_id).await.unwrap();
    let failing = RoomStatus {
        failed_closed: true,
        ..status(2, 1, false)
    };
    assert_eq!(handle.status().await, Ok(failing));
    let tasks = handle.table("tasks").unwrap();
    let mut first = tasks.watch(&task_key(1));
    assert_eq!(next(&mut first).await, Some(Ok(Some(task(1, "task")))));

    let sent = transport.sent().len();
    handle.discard().await.unwrap();
    // Nothing goes to the server, the stream ends, and the room is out of the client, without
    // metadata.
    assert_eq!(transport.sent().len(), sent);
    assert_eq!(next(&mut first).await, None);
    assert_eq!(handle.status().await, closed(&room_id));
    assert_eq!(handle.table("tasks").err(), closed::<()>(&room_id).err());
    assert_eq!(tasks.get(&task_key(1)).await, closed(&room_id));
    assert_eq!(metadata(dir.path(), &room_id).await, None);

    // The next open opens it as new, without a cursor, with the schema and the state of the
    // server; the old handles reach it again.
    room(&client, &room_id).await.unwrap();
    let registrations = [(None, None), (Some(2), Some(incarnation)), (None, None)];
    assert_eq!(transport.registrations(), registrations);
    let saved = metadata(dir.path(), &room_id).await.unwrap();
    assert_eq!((saved.schema_id, saved.schema), (tasks_id(), tagged));
    handle.sync().await.unwrap();
    // One epoch more for the discard, and one more for the open without metadata.
    assert_eq!(handle.status().await, Ok(status(3, 3, true)));
    let rows = tasks.scan(&Scan::new()).await.unwrap();
    let tagged_row = task(3, "task").with("tag", "new");
    let untagged = |id| task(id, "task").with("tag", Value::Null);
    assert_eq!(rows, [untagged(1), untagged(2), tagged_row]);
}

#[tokio::test]
async fn rooms_that_do_not_open_and_a_room_this_client_never_opened_are_discarded() {
    for case in [
        "schema mismatch",
        "snapshot constraints",
        "never opened",
        "empty",
    ] {
        let server = TestServer::new();
        let room_id = server.create_room("room-a").await;
        let dir = tempdir().unwrap();
        let mut transport = server.transport();
        let incarnation = server.incarnation(&room_id).await;
        let saved = |schema| {
            let metadata = FileMetadata::new(dir.path());
            let room = RoomMetadata {
                room_incarnation: incarnation,
                schema_id: tasks_id(),
                schema,
            };
            let room_id = room_id.clone();
            async move { metadata.save_room(&room_id, &room).await.unwrap() }
        };
        match case {
            "schema mismatch" | "snapshot constraints" => {
                replica(&server, dir.path(), &room_id).await;
                compact(dir.path(), &room_id).await;
                let schema = match case {
                    "schema mismatch" => moved_schema(),
                    _ => unique_titles_schema(),
                };
                saved(schema).await;
            }
            "never opened" => {
                replica(&server, dir.path(), &room_id).await;
            }
            _ => {
                server.commit(&room_id, 1..=3).await;
            }
        }
        let client = facade(dir.path(), engine(dir.path()), &transport).await;
        match case {
            "schema mismatch" | "snapshot constraints" => {
                let opened = room(&client, &room_id).await;
                let error = match case {
                    "schema mismatch" => "holds data of table id 0",
                    _ => "break the constraint 'tasks_title'",
                };
                let failed =
                    matches!(&opened, Err(ClientError::Storage(err)) if err.contains(error));
                assert!(failed, "{case}: {opened:?}");
            }
            "never opened" => {}
            _ => {
                // Without network, a room without metadata stays empty: a discard, and no handle.
                transport.offline();
                let opened = room(&client, &room_id).await;
                assert!(matches!(opened, Err(ClientError::Network(_))), "{opened:?}");
                transport.online();
            }
        }

        let sent = transport.sent().len();
        client.discard_room(room_id.clone()).await.unwrap();
        assert_eq!(transport.sent().len(), sent, "{case}");
        assert_eq!(metadata(dir.path(), &room_id).await, None, "{case}");
        client.shutdown().await.unwrap();
        // The data are deleted.
        assert_eq!(on_disk(dir.path(), &room_id).await, (0, vec![]), "{case}");

        // The next open opens it as new, from the server.
        transport = server.transport();
        let client = facade(dir.path(), engine(dir.path()), &transport).await;
        let handle = room(&client, &room_id).await.unwrap();
        assert_eq!(transport.registrations(), [(None, None)], "{case}");
        handle.sync().await.unwrap();
        // A new client, whose open without metadata starts the epoch 1.
        assert_eq!(handle.status().await, Ok(status(3, 1, true)), "{case}");
        let tasks = handle.table("tasks").unwrap();
        let rows = tasks.scan(&Scan::new()).await.unwrap();
        let expected: Vec<_> = (1..=3).map(|id| task(id, "task")).collect();
        assert_eq!(rows, expected, "{case}");
    }
}

// ---------------------------------------------------------------------------------------------
// What the discard ends

/// A `CommitAck` for the commit `message`, which assigns it `seq` without its operation in the
/// catch-up, which has more: a sync applies it.
fn commit_ack(message: &ClientMessage, seq: u64) -> Vec<u8> {
    let ClientMessage::Commit {
        correlation_id,
        room_id,
        mutation_id,
        ..
    } = message
    else {
        panic!("{message:?}");
    };
    encode_message(&ServerMessage::CommitAck {
        correlation_id: *correlation_id,
        room_id: room_id.clone(),
        mutation_id: *mutation_id,
        assigned_seq: SequenceNumber::new(seq),
        catchup_ops: Vec::new(),
        has_more: true,
        snapshot_wanted: false,
        schema_version: 1,
        schema_id: tasks_id(),
    })
    .unwrap()
}

/// The reply to `message`, a commit, with an error with `code`.
fn refusal(message: &ClientMessage, code: ErrorCode) -> Vec<u8> {
    let (correlation_id, room_id) = request(message);
    error_reply(correlation_id, &room_id, code)
}

/// Waits until the server replied to the message `held` holds, failing if `write` ends first.
async fn until_replied<T: std::fmt::Debug>(write: impl Future<Output = T>, held: &mut Held) {
    tokio::select! {
        written = write => panic!("The write ended first: {written:?}"),
        () = held.replied() => {}
    }
}

#[tokio::test(start_paused = true)]
async fn the_writes_in_flight_end_as_in_a_discard_and_the_next_open_starts_one_more_epoch() {
    for case in ["acked", "acked later", "refused", "uncertain"] {
        let dir = tempdir().unwrap();
        let (_server, room_id, transport, storage, client) =
            setup(dir.path(), engine(dir.path())).await;
        let mut held = match case {
            // The ack, fabricated, leaves the sequence to a sync, which is held.
            "acked" => {
                transport.answer_next(Commit, |message| commit_ack(message, 1));
                transport.hold_next_reply(Sync)
            }
            // The ack of the server comes after the discard.
            "acked later" => transport.hold_next_reply(Commit),
            // A refusal without effect, held, after an uncertain attempt or not.
            _ => {
                transport.answer_every(Commit, |message| refusal(message, ErrorCode::RoomLocked));
                if case == "uncertain" {
                    transport.lose_next_reply(Commit);
                }
                transport.hold_next_reply(Commit)
            }
        };
        let mut write = pin!(client.write(room_id.clone(), insert_task(1)));
        until_replied(&mut write, &mut held).await;
        let in_flight = client.status(room_id.clone()).await.unwrap();
        assert_eq!(in_flight.writes_in_flight, 1, "{case}");

        client.discard_room(room_id.clone()).await.unwrap();
        let sent = transport.sent().len();
        held.release();
        let written = write.await;
        match case {
            "refused" => assert!(
                matches!(written, Err(ClientError::Network(_))),
                "{case}: {written:?}"
            ),
            _ => assert_eq!(written, Err(ClientError::OutcomeUnknown), "{case}"),
        }
        // Nothing more goes out: neither an attempt nor a sync.
        assert_eq!(transport.sent().len(), sent, "{case}");

        // The open epoch was 1: one more for the discard, and one more for the next open.
        open_room(&client, &room_id).await.unwrap();
        let opened = client.status(room_id.clone()).await;
        assert_eq!(opened, Ok(status(0, 3, true)), "{case}");
        assert!(tasks(&storage, &room_id).await.is_empty(), "{case}");
    }
}

#[tokio::test]
async fn the_watch_streams_end_and_a_write_during_the_discard_is_room_closed() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=2).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = facade(dir.path(), storage.clone(), &transport).await;
    let handle = room(&client, &room_id).await.unwrap();
    handle.sync().await.unwrap();
    let tasks = handle.table("tasks").unwrap();
    let mut first = tasks.watch(&task_key(1));
    let mut all = tasks.watch_scan(&Scan::new());
    assert_eq!(next(&mut first).await, Some(Ok(Some(task(1, "task")))));
    let both = vec![task(1, "task"), task(2, "task")];
    assert_eq!(next(&mut all).await, Some(Ok(both)));
    // One that waits in a task of its own.
    let mut second = tasks.watch(&task_key(2));
    assert_eq!(next(&mut second).await, Some(Ok(Some(task(2, "task")))));
    let second = tokio::spawn(async move { second.next().await });
    assert_eq!(handle.status().await, Ok(watched(2, 1, 3)));

    // While the discard deletes the data: a watch made then, and a write.
    let mut held = storage.hold_next("delete_room");
    let discard = tokio::spawn({
        let handle = handle.clone();
        async move { handle.discard().await }
    });
    held.replied().await;
    let mut during = tasks.watch(&task_key(1));
    let sent = transport.sent().len();
    let written = tasks.insert(&task(3, "task")).await;
    assert_eq!(written.err(), closed::<()>(&room_id).err());
    assert_eq!(transport.sent().len(), sent);
    held.release();
    discard.await.unwrap().unwrap();
    assert_eq!(next(&mut first).await, None);
    assert_eq!(next(&mut all).await, None);
    assert_eq!(next(&mut during).await, None);
    let ended = tokio::time::timeout(Duration::from_secs(10), second).await;
    assert!(matches!(ended, Ok(Ok(None))), "{ended:?}");
    // One made afterwards ends at once.
    let mut after = tasks.watch(&task_key(1));
    assert_eq!(next(&mut after).await, None);

    // The room opened again is watched as new.
    room(&client, &room_id).await.unwrap();
    handle.sync().await.unwrap();
    let mut again = tasks.watch(&task_key(1));
    assert_eq!(next(&mut again).await, Some(Ok(Some(task(1, "task")))));
    assert_eq!(handle.status().await, Ok(watched(2, 3, 1)));
}

// ---------------------------------------------------------------------------------------------
// The requests in flight

#[tokio::test]
async fn a_registration_in_flight_goes_first_and_a_sync_in_flight_leaves_the_new_replica() {
    // A registration: the discard waits for it, under the registration lock.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, storage, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    client.close_room(room_id.clone()).await.unwrap();
    let mut held = transport.hold_next_reply(RegisterClient);
    let discarded = async {
        held.replied().await;
        let mut discard = pin!(client.discard_room(room_id.clone()));
        pending(&mut discard).await;
        held.release();
        discard.await
    };
    let (opened, discarded) = tokio::join!(open_room(&client, &room_id), discarded);
    opened.unwrap();
    discarded.unwrap();
    assert_eq!(client.status(room_id.clone()).await, closed(&room_id));
    open_room(&client, &room_id).await.unwrap();
    let incarnation = server.incarnation(&room_id).await;
    let registrations = [(None, None), (Some(0), Some(incarnation)), (None, None)];
    assert_eq!(transport.registrations(), registrations);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 3, true)));

    // A sync: its batch, of the epoch before the discard, is not applied to the replica of the
    // room created again, which the sync then catches up.
    let mut held = transport.hold_next_reply(Sync);
    let discarded = async {
        held.replied().await;
        client.discard_room(room_id.clone()).await.unwrap();
        server.recreate_room(&room_id).await;
        server.commit(&room_id, [7]).await;
        open_room(&client, &room_id).await.unwrap();
        held.release();
    };
    let (synced, ()) = tokio::join!(client.sync(room_id.clone()), discarded);
    synced.unwrap();
    assert_eq!(tasks(&storage, &room_id).await, [7]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(1, 5, true)));
}

#[tokio::test(start_paused = true)]
async fn a_download_in_flight_installs_nothing_in_the_new_replica() {
    // A replica at 0, behind the log of the room, pruned before 6, downloads the snapshot at 7.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let ops = server.commit(&room_id, 1..=9).await;
    server.prune(&room_id, 6).await;
    let seven = snapshot(&room_id, tasks_schema(), &ops[..7]).await;
    server.upload(&room_id, 7, seven).await;
    let dir = tempdir().unwrap();
    let (transport, storage) = (server.transport(), engine(dir.path()));
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();

    let mut held = transport.hold_next_reply(RequestSnapshotChunk);
    let discarded = async {
        held.replied().await;
        client.discard_room(room_id.clone()).await.unwrap();
        server.recreate_room(&room_id).await;
        server.commit(&room_id, [8]).await;
        open_room(&client, &room_id).await.unwrap();
        held.release();
    };
    let (synced, ()) = tokio::join!(client.sync(room_id.clone()), discarded);
    synced.unwrap();
    assert_eq!(tasks(&storage, &room_id).await, [8]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(1, 3, true)));
    let chunks = transport.sent().into_iter();
    let chunks = chunks.filter(|message| message.kind() == RequestSnapshotChunk);
    assert_eq!(chunks.count(), 1);
}

// ---------------------------------------------------------------------------------------------
// The steps

#[tokio::test]
async fn the_metadata_go_before_the_data_and_nothing_is_sent() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    replica(&server, dir.path(), &room_id).await;
    // What a save cut before its rename leaves.
    std::fs::write(dir.path().join("room_room-a.meta.tmp"), "cut").unwrap();

    let files = TestFiles::default();
    let storage = DiskStorageEngine::with_files(options(dir.path()), files.clone()).unwrap();
    let metadata_store = FileMetadata::with_files(dir.path(), files.clone());
    let transport = server.transport();
    let client = Client::open(
        ClientConfig::new("ana"),
        storage,
        metadata_store,
        transport.clone(),
        TestTokens::new(),
    );
    let client = client.await.unwrap();
    open_room(&client, &room_id).await.unwrap();
    let (logged, sent) = (files.log().len(), transport.sent().len());
    client.discard_room(room_id.clone()).await.unwrap();
    assert_eq!(transport.sent().len(), sent);

    let log = files.log();
    let discard = &log[logged..];
    let dir_name = dir.path().file_name().unwrap().to_str().unwrap();
    let synced = format!("sync_dir {dir_name}");
    assert_eq!(
        discard[..3],
        [
            "remove room_room-a.meta",
            "remove room_room-a.meta.tmp",
            &synced
        ]
    );
    let wal = discard
        .iter()
        .position(|entry| entry == "remove room_room-a.wal");
    let snapshot = discard
        .iter()
        .position(|entry| entry == "remove room_room-a.snap");
    assert!(
        wal.is_some() && snapshot.is_some() && wal < snapshot,
        "{discard:?}"
    );
    client.shutdown().await.unwrap();
    assert_eq!(on_disk(dir.path(), &room_id).await, (0, vec![]));
}

#[tokio::test]
async fn a_failed_step_stops_the_discard_and_the_next_open_deletes_again() {
    for method in ["delete_room", "close_room"] {
        let (server, dir) = (TestServer::new(), tempdir().unwrap());
        let room_id = server.create_room("room-a").await;
        let incarnation = replica(&server, dir.path(), &room_id).await;
        let transport = server.transport();
        let storage = TestEngine::new(engine(dir.path()));
        let client = open_client(dir.path(), storage.clone(), &transport).await;
        open_room(&client, &room_id).await.unwrap();

        storage.fail_next(method, StorageError::RoomLocked(room_id.clone()));
        let discarded = client.discard_room(room_id.clone()).await;
        let locked = matches!(&discarded, Err(ClientError::Storage(err)) if err.contains("locked"));
        assert!(locked, "{method}: {discarded:?}");
        assert_eq!(
            client.status(room_id.clone()).await,
            closed(&room_id),
            "{method}"
        );
        assert_eq!(metadata(dir.path(), &room_id).await, None, "{method}");

        // Without metadata, the next open deletes the data, and registers without a cursor.
        open_room(&client, &room_id).await.unwrap();
        let registrations = [(Some(3), Some(incarnation)), (None, None)];
        assert_eq!(transport.registrations(), registrations, "{method}");
        let opened = client.status(room_id.clone()).await;
        // The replica opened from its metadata in the epoch 0, and the failed discard started
        // none: the open without metadata does.
        assert_eq!(opened, Ok(status(0, 1, true)), "{method}");
        assert!(tasks(&storage, &room_id).await.is_empty(), "{method}");
    }

    // Metadata that are not removed keep the replica, which the next open opens with them.
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    let incarnation = replica(&server, dir.path(), &room_id).await;
    let transport = server.transport();
    let files = TestFiles::default();
    let metadata_store = FileMetadata::with_files(dir.path(), files.clone());
    let storage = engine(dir.path());
    let client = Client::open(
        ClientConfig::new("ana"),
        storage.clone(),
        metadata_store,
        transport.clone(),
        TestTokens::new(),
    );
    let client = client.await.unwrap();
    open_room(&client, &room_id).await.unwrap();
    files.fail_next("remove", "room_room-a.meta");
    let discarded = client.discard_room(room_id.clone()).await;
    assert!(
        matches!(discarded, Err(ClientError::Storage(_))),
        "{discarded:?}"
    );
    assert_eq!(client.status(room_id.clone()).await, closed(&room_id));
    open_room(&client, &room_id).await.unwrap();
    let cursor = (Some(3), Some(incarnation));
    assert_eq!(transport.registrations(), [cursor, cursor]);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 0, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn a_write_waiting_for_the_discard_is_room_closed_and_after_shutdown_it_is_room_closed() {
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (_server, room_id, transport, _, client) = setup(dir.path(), storage.clone()).await;
    let mut held = storage.hold_next("delete_room");
    let mut discard = pin!(client.discard_room(room_id.clone()));
    tokio::select! {
        discarded = &mut discard => panic!("The discard ended first: {discarded:?}"),
        () = held.replied() => {}
    }
    let sent = transport.sent().len();
    // It waits for the applier, which the discard holds.
    let mut write = pin!(client.write(room_id.clone(), insert_task(1)));
    pending(&mut write).await;
    held.release();
    (&mut discard).await.unwrap();
    assert_eq!(write.await, closed(&room_id));
    assert_eq!(transport.sent().len(), sent);

    // After shutdown, nothing is discarded.
    open_room(&client, &room_id).await.unwrap();
    client.shutdown().await.unwrap();
    let discarded = client.discard_room(room_id.clone()).await;
    assert_eq!(discarded, closed(&room_id));
    assert!(metadata(dir.path(), &room_id).await.is_some());
    // Nor does a room enter the client: its worker is still out of reach.
    let never_opened = RoomId::new("room-b").unwrap();
    let discarded = client.discard_room(never_opened.clone()).await;
    assert_eq!(discarded, closed(&never_opened));
    let stopped = client.stop_worker(never_opened.clone()).await;
    assert_eq!(stopped, closed(&never_opened));
}

#[tokio::test]
async fn a_discard_that_waits_while_the_client_shuts_down_touches_nothing() {
    let dir = tempdir().unwrap();
    let (_server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    client.close_room(room_id.clone()).await.unwrap();
    // An open whose registration is held has the registration lock, which the discard waits
    // for, past its first look at the shutdown, while the client shuts down.
    let mut held = transport.hold_next_reply(RegisterClient);
    let discarded = async {
        held.replied().await;
        let mut discard = pin!(client.discard_room(room_id.clone()));
        pending(&mut discard).await;
        client.shutdown().await.unwrap();
        held.release();
        discard.await
    };
    let (opened, discarded) = tokio::join!(open_room(&client, &room_id), discarded);
    assert_eq!(opened, closed(&room_id));
    assert_eq!(discarded, closed(&room_id));
    // The metadata, which the shutdown released for another client, are still there.
    assert!(metadata(dir.path(), &room_id).await.is_some());
}

#[tokio::test(start_paused = true)]
async fn the_worker_stops_and_nothing_goes_to_the_server() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let dir = tempdir().unwrap();
    let interval = Duration::from_secs(10);
    let config = ClientConfig {
        heartbeat_interval: interval,
        ..ClientConfig::new("ana")
    };
    let client = Client::open(
        config,
        engine(dir.path()),
        FileMetadata::new(dir.path()),
        transport.clone(),
        TestTokens::new(),
    );
    let client = Arc::new(client.await.unwrap());
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    let heartbeats = |transport: &TestTransport| {
        let sent = transport.sent().into_iter();
        sent.filter(|message| message.kind() == ClientMessageKind::Heartbeat)
            .count()
    };
    tokio::time::sleep(interval * 2).await;
    transport.settled().await;
    assert!(heartbeats(&transport) >= 1);

    let sent = transport.sent().len();
    client.discard_room(room_id.clone()).await.unwrap();
    tokio::time::sleep(interval * 5).await;
    transport.settled().await;
    assert_eq!(transport.sent().len(), sent);
    // The room opened again has no worker.
    open_room(&client, &room_id).await.unwrap();
    let opened = client.status(room_id.clone()).await.unwrap();
    assert_eq!(opened, status(0, 3, true));
}

#[tokio::test(start_paused = true)]
async fn the_worker_stops_first_so_its_registration_in_flight_does_not_hold_the_discard() {
    let (server, dir) = (TestServer::new(), tempdir().unwrap());
    let room_id = server.create_room("room-a").await;
    replica(&server, dir.path(), &room_id).await;
    let transport = server.transport();
    let config = ClientConfig {
        heartbeat_interval: Duration::from_secs(10),
        ..ClientConfig::new("ana")
    };
    let client = Client::open(
        config,
        engine(dir.path()),
        FileMetadata::new(dir.path()),
        transport.clone(),
        TestTokens::new(),
    );
    let client = Arc::new(client.await.unwrap());
    // Without network, the replica opens unregistered, and the first heartbeat of its worker
    // registers it, under the registration lock, with the reply held.
    transport.offline();
    open_room(&client, &room_id).await.unwrap();
    transport.online();
    let mut held = transport.hold_next_reply(RegisterClient);
    client.start_worker(room_id.clone()).await.unwrap();
    held.replied().await;

    let discarded = tokio::time::timeout(
        Duration::from_secs(10),
        client.discard_room(room_id.clone()),
    );
    discarded
        .await
        .expect("The discard waited for the worker")
        .unwrap();
    drop(held);
    assert_eq!(metadata(dir.path(), &room_id).await, None);
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(transport.registrations().last(), Some(&(None, None)));
    let opened = client.status(room_id.clone()).await.unwrap();
    assert_eq!(opened, status(0, 2, true));
}

#[tokio::test]
async fn a_worker_started_while_the_discard_waits_for_a_registration_is_stopped() {
    // Also when a step of the discard fails.
    for fails in [false, true] {
        let server = TestServer::new();
        let room_id = server.create_room("room-a").await;
        let transport = server.transport();
        let dir = tempdir().unwrap();
        let config = ClientConfig {
            heartbeat_interval: Duration::from_secs(50),
            ..ClientConfig::new("ana")
        };
        let storage = TestEngine::new(engine(dir.path()));
        let client = Client::open(
            config,
            storage.clone(),
            FileMetadata::new(dir.path()),
            transport.clone(),
            TestTokens::new(),
        );
        let client = Arc::new(client.await.unwrap());
        open_room(&client, &room_id).await.unwrap();
        client.close_room(room_id.clone()).await.unwrap();
        if fails {
            storage.fail_next("delete_room", StorageError::RoomLocked(room_id.clone()));
        }

        let mut held = transport.hold_next_reply(RegisterClient);
        let discarded = async {
            held.replied().await;
            let mut discard = pin!(client.discard_room(room_id.clone()));
            pending(&mut discard).await;
            // The discard waits for the registration lock, past its first stop of the worker:
            // a worker starts meanwhile, on the replica that the open opened.
            client.start_worker(room_id.clone()).await.unwrap();
            held.release();
            discard.await
        };
        let (opened, discarded) = tokio::join!(open_room(&client, &room_id), discarded);
        opened.unwrap();
        match fails {
            true => assert!(
                matches!(discarded, Err(ClientError::Storage(_))),
                "{discarded:?}"
            ),
            false => discarded.unwrap(),
        }
        assert_eq!(client.status(room_id.clone()).await, closed(&room_id));
        open_room(&client, &room_id).await.unwrap();
        let opened = client.status(room_id.clone()).await.unwrap();
        assert_eq!(opened.worker, None, "fails: {fails}");
    }
}

/// Table `blobs`: an int key `id` and a bytes `data`.
fn blobs_schema() -> Schema {
    let table = TableSchema::builder("blobs")
        .primary_key("id", DataType::Int)
        .column("data", DataType::Bytes);
    Schema::builder().table(table).build()
}

/// An insert of row `id` into `blobs` with `len` bytes that do not compress.
fn insert_blob(id: i64, len: usize) -> Operation {
    // SplitMix64, seeded with the id.
    let mut state = id as u64;
    let mut next = || {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let z = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)).to_le_bytes()
    };
    let data: Vec<u8> = (0..len.div_ceil(8))
        .flat_map(|_| next())
        .take(len)
        .collect();
    let row = RowBuilder::new().set("id", id).set("data", data).build();
    let insert = blobs_schema().to_operation_insert("blobs", &row, 1000);
    insert.unwrap()
}

#[tokio::test(start_paused = true)]
async fn an_upload_in_flight_sends_no_more_chunks() {
    // A member at 9, whose snapshot takes two chunks (the blobs 1 and 2 hold 2 MiB and 64 KiB),
    // and one at 0, behind the log, pruned before 6: the server designates the first to upload.
    let server = TestServer::pruning();
    let room_id = server
        .create_room_with("room-b", "blobs", blobs_schema())
        .await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = open_client(dir.path(), engine(dir.path()), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    let blobs =
        (1..=9).map(|id| insert_blob(id, if id <= 2 { (2 << 20) + (64 << 10) } else { 16 }));
    server.commit_ops(&room_id, blobs).await;
    client.sync(room_id.clone()).await.unwrap();
    server.prune(&room_id, 6).await;
    let behind = tempdir().unwrap();
    let lagging = open_client(behind.path(), engine(behind.path()), &server.transport()).await;
    open_room(&lagging, &room_id).await.unwrap();

    let mut held = transport.hold_next_reply(ClientMessageKind::UploadSnapshotChunk);
    let discarded = async {
        held.replied().await;
        client.discard_room(room_id.clone()).await.unwrap();
        held.release();
    };
    let (heartbeat, ()) = tokio::join!(client.heartbeat(room_id.clone()), discarded);
    // The upload ends without its second chunk, and the heartbeat, sent again, finds the room
    // out of the client.
    assert_eq!(heartbeat, closed(&room_id));
    let sent = transport.sent().into_iter();
    let chunks = sent.filter(|message| message.kind() == ClientMessageKind::UploadSnapshotChunk);
    assert_eq!(chunks.count(), 1);
    let active = server.state.snapshot_relay.active_snapshot_seq(&room_id);
    assert_eq!(active, None);
}

#[tokio::test(start_paused = true)]
async fn the_worker_stopped_first_acknowledges_the_head_of_the_replica_it_discards() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=3).await;
    let transport = server.transport();
    let dir = tempdir().unwrap();
    let interval = Duration::from_secs(10);
    let config = ClientConfig {
        heartbeat_interval: interval,
        ..ClientConfig::new("ana")
    };
    let client = Client::open(
        config,
        engine(dir.path()),
        FileMetadata::new(dir.path()),
        transport.clone(),
        TestTokens::new(),
    );
    let client = Arc::new(client.await.unwrap());
    open_room(&client, &room_id).await.unwrap();
    // The worker syncs the replica up to 3, without an acknowledgement of its own; then the
    // acknowledgement after its next heartbeat is held back, so the server took 0.
    transport.answer_every(ClientMessageKind::Ack, |message| {
        let (correlation_id, room_id) = request(message);
        error_reply(correlation_id, &room_id, ErrorCode::Unavailable)
    });
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(interval * 3).await;
    transport.settled().await;
    let head = client.status(room_id.clone()).await.unwrap().head_seq;
    assert_eq!(head, SequenceNumber::new(3));
    transport.answer(|_| None);

    let acks = |transport: &TestTransport| {
        let sent = requests(transport).into_iter();
        sent.filter(|(kind, _)| *kind == "ack").collect::<Vec<_>>()
    };
    let before = acks(&transport).len();
    client.discard_room(room_id.clone()).await.unwrap();
    // The last acknowledgement, with the head of the replica it discards, read while it was
    // still open; and nothing else.
    let sent = acks(&transport);
    assert_eq!(sent[before..], [("ack", 3)]);
    let kinds = transport.sent().into_iter().map(|message| message.kind());
    assert_eq!(kinds.last(), Some(ClientMessageKind::Ack));
    tokio::time::sleep(interval * 3).await;
    transport.settled().await;
    assert_eq!(acks(&transport).len(), before + 1);
}
