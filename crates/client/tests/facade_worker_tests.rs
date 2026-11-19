//! The facade with the worker of each room, against the server in process: `room` starts the
//! worker, in the mode of the room, with the first token of the token provider; the worker runs in
//! `live` while a room in `periodic` is watched; `sync` catches a room in `manual` up; and a
//! write in a room whose last request got no reply is `Offline`. With the paused clock, but for the
//! streams opened and dropped from several threads, and with the longest interval of the
//! heartbeats, so that, once one went, what a test waits for comes long before the next one.

mod common;

use common::*;
use futures::StreamExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::Instant;
use zemdb_client::{
    ClientConfig, ClientError, FileMetadata, Freshness, Key, MemoryMetadata, MetadataStore,
    RoomHandle, Row, Scan, TransportError, WorkerStatus, ZemdbClient,
};
use zemdb_core::ClientMessageKind::{Commit, Heartbeat, RegisterClient, Sync};
use zemdb_core::*;
use zemdb_storage::{MemoryStorageEngine, StorageEngine};

/// The interval of the heartbeats: the longest one.
const INTERVAL: Duration = Duration::from_secs(59);

/// The configuration of ana, with the worker of each room starting in `freshness`, and a
/// heartbeat every [`INTERVAL`].
fn config(freshness: Freshness) -> ClientConfig {
    ClientConfig {
        freshness,
        heartbeat_interval: INTERVAL,
        ..ClientConfig::new("ana")
    }
}

/// A facade with `config` over `storage` and `metadata`, through `transport`, with `tokens`.
async fn facade_over(
    storage: impl StorageEngine + 'static,
    metadata: impl MetadataStore + 'static,
    transport: &TestTransport,
    config: ClientConfig,
    tokens: &TestTokens,
) -> ZemdbClient {
    let client = ZemdbClient::open(
        config,
        storage,
        metadata,
        transport.clone(),
        tokens.clone(),
        None,
    );
    client.await.unwrap()
}

/// A facade with `config` over a memory engine and a memory store of metadata.
async fn facade(
    transport: &TestTransport,
    config: ClientConfig,
    tokens: &TestTokens,
) -> ZemdbClient {
    let (storage, metadata) = (MemoryStorageEngine::new(), MemoryMetadata::default());
    facade_over(storage, metadata, transport, config, tokens).await
}

/// A facade with `config` over a disk engine with its metadata in `dir`.
async fn on_disk(
    dir: &Path,
    transport: &TestTransport,
    config: ClientConfig,
    tokens: &TestTokens,
) -> ZemdbClient {
    let metadata = FileMetadata::new(dir);
    facade_over(engine(dir), metadata, transport, config, tokens).await
}

/// The worker of `room`, as its status gives it.
async fn worker(room: &RoomHandle) -> WorkerStatus {
    let status = room.status().await.unwrap();
    status.worker.expect("The room has no worker")
}

/// Waits until the worker of `room` is as `wanted` says, within `within`, and returns it.
async fn until_within(
    room: &RoomHandle,
    within: Duration,
    wanted: impl Fn(&WorkerStatus) -> bool,
) -> WorkerStatus {
    let start = Instant::now();
    loop {
        let worker = worker(room).await;
        if wanted(&worker) {
            return worker;
        }
        assert!(start.elapsed() < within, "{worker:?}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Waits until the worker of `room` is as `wanted` says, within a few seconds.
async fn until(room: &RoomHandle, wanted: impl Fn(&WorkerStatus) -> bool) -> WorkerStatus {
    until_within(room, Duration::from_secs(5), wanted).await
}

/// Whether the worker runs in `Live`, with its event stream open.
fn live(worker: &WorkerStatus) -> bool {
    worker.freshness == Freshness::Live && worker.stream_open
}

/// Whether the worker runs in `freshness`, which is not `Live`, with its event stream closed.
fn closed_in(freshness: Freshness) -> impl Fn(&WorkerStatus) -> bool {
    move |worker| worker.freshness == freshness && !worker.stream_open
}

/// When each message of `kind` went through `transport`, in order.
fn sent_of(transport: &TestTransport, kind: ClientMessageKind) -> Vec<Instant> {
    let sent = transport.sent_at().into_iter();
    let of = |(message, _): &(ClientMessage, Instant)| message.kind() == kind;
    sent.filter(of).map(|(_, at)| at).collect()
}

/// How many heartbeats went through `transport`.
fn heartbeats(transport: &TestTransport) -> usize {
    sent_of(transport, Heartbeat).len()
}

/// Waits until the next heartbeat through `transport` went, and a little more, so that the one
/// after it comes an interval later.
async fn after_a_heartbeat(transport: &TestTransport) {
    let (before, start) = (heartbeats(transport), Instant::now());
    while heartbeats(transport) == before {
        let late = start.elapsed() > INTERVAL + Duration::from_secs(2);
        assert!(!late, "No heartbeat within an interval");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
}

/// Table `notes`: an int key `id` and an encrypted string `text`.
fn notes_schema() -> Schema {
    let notes = TableSchema::builder("notes")
        .primary_key("id", DataType::Int)
        .encrypted_column("text", DataType::String);
    Schema::builder().table(notes).build()
}

/// `Registered` for the registration `correlation_id` of `room_id`, at its head 0, with `tasks`
/// of [`tasks_schema`] with `title` an int, which does not evolve from it.
fn retyped_registered(
    correlation_id: CorrelationId,
    room_id: &RoomId,
    incarnation: u128,
) -> Vec<u8> {
    let tasks = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::Int);
    encode_message(&ServerMessage::Registered {
        correlation_id,
        room_id: room_id.clone(),
        head_seq: SequenceNumber::new(0),
        tail_seq: SequenceNumber::new(0),
        schema_id: SchemaId::new("retyped").unwrap(),
        schema: Schema::builder().table(tasks).build(),
        active_snapshot_seq: None,
        room_incarnation: incarnation,
    })
    .unwrap()
}

fn task_row(id: i64) -> Row {
    Row::new().with("id", id).with("title", "task")
}

fn task_key(id: i64) -> Key {
    Key::new().with("id", id)
}

fn closed(room_id: &RoomId) -> ClientError {
    ClientError::RoomClosed(room_id.clone())
}

// ---------------------------------------------------------------------------------------------
// The worker of each room

#[tokio::test(start_paused = true)]
async fn room_starts_the_worker_a_second_room_does_not_start_another_and_close_stops_it() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Live), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    let started = until(&room, live).await;
    let running = WorkerStatus {
        freshness: Freshness::Live,
        effective: Freshness::Live,
        stream_open: true,
        running: true,
        connected: Some(true),
        last_error: None,
    };
    assert_eq!(started, running);

    // A second handle of the room keeps the worker: its stream stays open, and its heartbeats
    // keep their interval.
    after_a_heartbeat(&transport).await;
    let again = client.room(room_id.clone()).await.unwrap();
    after_a_heartbeat(&transport).await;
    let beats = sent_of(&transport, Heartbeat);
    assert_eq!(beats[1] - beats[0], INTERVAL);
    assert_eq!(transport.stream_opens().len(), 1);
    assert_eq!(transport.streams_open(), 1);
    assert_eq!(worker(&again).await, running);

    // Closing the room stops it: its stream closes, and no heartbeat goes after it.
    again.close().await.unwrap();
    assert_eq!(room.status().await, Err(closed(&room_id)));
    assert_eq!(transport.streams_open(), 0);
    let beats = heartbeats(&transport);
    tokio::time::sleep(INTERVAL * 3).await;
    assert_eq!(heartbeats(&transport), beats);
    assert_eq!(transport.stream_opens().len(), 1);

    // Opened again, the room has a worker again.
    let room = client.room(room_id.clone()).await.unwrap();
    until(&room, live).await;
    after_a_heartbeat(&transport).await;
    assert_eq!(heartbeats(&transport), beats + 1);
}

// ---------------------------------------------------------------------------------------------
// The mode of the room and its watch streams

#[tokio::test(start_paused = true)]
async fn in_periodic_the_first_watch_turns_live_on_and_dropping_the_last_turns_it_off() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, [1]).await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Periodic), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    let tasks = room.table("tasks").unwrap();
    assert!(closed_in(Freshness::Periodic)(&worker(&room).await));

    // The first stream asks the worker for `live` at once, and the worker opens the event stream
    // of the room; its sync brings what came before.
    let mut first = tasks.watch(&task_key(1));
    assert_eq!(worker(&room).await.freshness, Freshness::Live);
    until(&room, live).await;
    assert_eq!(first.next().await, Some(Ok(Some(task_row(1)))));
    let mut all = tasks.watch_scan(&Scan::new());
    assert_eq!(all.next().await, Some(Ok(vec![task_row(1)])));
    // What another client writes arrives by the stream, long before the next heartbeat.
    after_a_heartbeat(&transport).await;
    let committed = Instant::now();
    server.commit(&room_id, [2]).await;
    assert_eq!(all.next().await, Some(Ok(vec![task_row(1), task_row(2)])));
    assert!(committed.elapsed() < Duration::from_secs(1));
    assert_eq!(room.status().await.unwrap().watches, 2);

    // Dropping one leaves the other: still `live`. Dropping the last asks for `periodic` at once,
    // and the worker closes the stream.
    drop(first);
    assert!(live(&worker(&room).await));
    drop(all);
    assert_eq!(worker(&room).await.freshness, Freshness::Periodic);
    until(&room, closed_in(Freshness::Periodic)).await;
    assert_eq!(transport.streams_open(), 0);
    assert_eq!(transport.stream_opens().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn in_manual_and_in_live_the_watch_streams_do_not_change_the_mode() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Periodic), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    let tasks = room.table("tasks").unwrap();

    // `manual`, with streams or without them: the event stream never opens.
    room.set_freshness(Freshness::Manual);
    assert_eq!(worker(&room).await.freshness, Freshness::Manual);
    let watched = tasks.watch(&task_key(1));
    assert_eq!(worker(&room).await.freshness, Freshness::Manual);
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(watched);
    assert_eq!(worker(&room).await.freshness, Freshness::Manual);
    assert_eq!(transport.stream_opens().len(), 0);

    // `live`, with streams or without them: the event stream stays open.
    room.set_freshness(Freshness::Live);
    until(&room, live).await;
    let watched = tasks.watch_scan(&Scan::new());
    drop(watched);
    assert!(live(&worker(&room).await));
    assert_eq!(transport.stream_opens().len(), 1);

    // `periodic` while a stream is alive is `live`, until it is dropped.
    let watched = tasks.watch(&task_key(1));
    room.set_freshness(Freshness::Periodic);
    assert!(live(&worker(&room).await));
    drop(watched);
    until(&room, closed_in(Freshness::Periodic)).await;

    // The mode is the room's, also for the worker of its next open.
    room.set_freshness(Freshness::Manual);
    room.close().await.unwrap();
    let room = client.room(room_id.clone()).await.unwrap();
    assert_eq!(worker(&room).await.freshness, Freshness::Manual);
    let _watched = room.table("tasks").unwrap().watch(&task_key(1));
    assert_eq!(worker(&room).await.freshness, Freshness::Manual);
    assert_eq!(transport.stream_opens().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_streams_opened_and_dropped_at_once_end_in_the_mode_of_the_count_at_the_end() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Periodic), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    for kept in [0_usize, 3, 0, 1] {
        // Eight tasks open and drop streams at once; the first `kept` keep their last one.
        let opening = (0..8_usize).map(|task| {
            let tasks = room.table("tasks").unwrap();
            tokio::spawn(async move {
                let mut last = None;
                for id in 0..50_i64 {
                    let watch = match id % 2 {
                        0 => tasks.watch(&task_key(id)),
                        _ => tasks.watch(&task_key(-id)),
                    };
                    last = Some(watch);
                    tokio::task::yield_now().await;
                    if id % 7 == task as i64 {
                        let mut scan = tasks.watch_scan(&Scan::new());
                        let _ = scan.next().await;
                    }
                }
                last.filter(|_| task < kept)
            })
        });
        let kept_streams: Vec<_> = futures::future::join_all(opening).await;
        let kept_streams: Vec<_> = kept_streams.into_iter().map(Result::unwrap).collect();
        assert_eq!(kept_streams.iter().flatten().count(), kept);
        // The mode of the count at the end, asked for before the last drop returned.
        let status = room.status().await.unwrap();
        assert_eq!(status.watches, kept, "{kept}");
        let freshness = match kept {
            0 => Freshness::Periodic,
            _ => Freshness::Live,
        };
        assert_eq!(status.worker.unwrap().freshness, freshness, "{kept}");
        let wanted = |worker: &WorkerStatus| worker.stream_open == (kept > 0);
        until_within(&room, Duration::from_secs(10), wanted).await;
        drop(kept_streams);
    }
}

#[tokio::test(start_paused = true)]
async fn dropping_a_stream_after_shutdown_or_outside_a_runtime_does_not_panic() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Periodic), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    let tasks = room.table("tasks").unwrap();

    // Dropped in a thread without a runtime: the worker takes the mode all the same.
    let watched = tasks.watch(&task_key(1));
    until(&room, live).await;
    std::thread::spawn(move || drop(watched)).join().unwrap();
    assert_eq!(worker(&room).await.freshness, Freshness::Periodic);
    until(&room, closed_in(Freshness::Periodic)).await;

    // Dropped after the client shut down, which opens no room, and does not ask the provider.
    let watched = tasks.watch_scan(&Scan::new());
    until(&room, live).await;
    client.shutdown().await.unwrap();
    drop(watched);
    assert_eq!(room.status().await, Err(closed(&room_id)));
    let calls = tokens.calls();
    assert_eq!(
        client.room(room_id.clone()).await.map(drop),
        Err(closed(&room_id))
    );
    assert_eq!(tokens.calls(), calls);

    // The last reference to a client that did not shut down, dropped outside a runtime.
    let client = facade(&transport, config(Freshness::Periodic), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    let watched = room.table("tasks").unwrap().watch(&task_key(1));
    until(&room, live).await;
    drop((client, room));
    std::thread::spawn(move || drop(watched)).join().unwrap();
}

#[tokio::test(start_paused = true)]
async fn with_the_event_stream_turned_off_a_watch_leaves_the_room_in_periodic() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    transport.answer_events(|room_id| Some(Ok(refused(room_id, ErrorCode::EventsDisabled))));
    let client = facade(&transport, config(Freshness::Periodic), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    let _watched = room.table("tasks").unwrap().watch(&task_key(1));
    // `live`, which the worker runs as `periodic`, as the status says.
    let fallen = |worker: &WorkerStatus| worker.effective == Freshness::Periodic;
    let worker = until(&room, |worker| {
        transport.stream_opens().len() == 1 && fallen(worker)
    })
    .await;
    assert_eq!(
        (worker.freshness, worker.stream_open),
        (Freshness::Live, false)
    );
    // It does not try again for a while: the heartbeats bring what the others write.
    server.commit(&room_id, [1]).await;
    after_a_heartbeat(&transport).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let tasks = room.table("tasks").unwrap();
    assert_eq!(tasks.get(&task_key(1)).await, Ok(Some(task_row(1))));
    assert_eq!(transport.stream_opens().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn in_manual_sync_catches_the_replica_up() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Manual), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    let tasks = room.table("tasks").unwrap();
    let mut watched = tasks.watch(&task_key(1));
    assert_eq!(watched.next().await, Some(Ok(None)));

    // Neither the heartbeats nor the stream bring what another client writes.
    server.commit(&room_id, [1]).await;
    tokio::time::sleep(INTERVAL * 2).await;
    assert!(heartbeats(&transport) >= 1);
    assert_eq!(sent_of(&transport, Sync).len(), 0);
    assert_eq!(tasks.get(&task_key(1)).await, Ok(None));
    assert_eq!(worker(&room).await.freshness, Freshness::Manual);

    // A sync does, and the stream shows it.
    room.sync().await.unwrap();
    assert_eq!(watched.next().await, Some(Ok(Some(task_row(1)))));
    assert_eq!(
        room.status().await.unwrap().head_seq,
        SequenceNumber::new(1)
    );
}

// ---------------------------------------------------------------------------------------------
// Offline

#[tokio::test(start_paused = true)]
async fn without_network_a_write_is_offline_and_sends_nothing_and_the_reads_go_on() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, [1]).await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Manual), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    room.sync().await.unwrap();
    after_a_heartbeat(&transport).await;
    let tasks = room.table("tasks").unwrap();
    let mut watched = tasks.watch(&task_key(1));
    assert_eq!(watched.next().await, Some(Ok(Some(task_row(1)))));

    // A request without a reply: the writes are offline, before anything is sent.
    transport.offline();
    let synced = room.sync().await;
    assert!(matches!(synced, Err(ClientError::Network(_))), "{synced:?}");
    assert_eq!(worker(&room).await.connected, Some(false));
    let sent = transport.sent().len();
    let offline = Err(ClientError::Offline(room_id.clone()));
    assert_eq!(tasks.insert(&task_row(2)).await.map(drop), offline);
    let done = Row::new().with("title", "done");
    assert_eq!(tasks.update(&task_key(1), &done).await, offline);
    assert_eq!(tasks.delete(&task_key(1)).await, offline);
    assert_eq!(transport.sent().len(), sent);
    // The reads go on, and the stream is still there.
    assert_eq!(tasks.get(&task_key(1)).await, Ok(Some(task_row(1))));
    assert_eq!(tasks.scan(&Scan::new()).await, Ok(vec![task_row(1)]));
    assert_eq!(room.status().await.unwrap().watches, 1);

    // With the network back, a write waits for a request of the room with a reply.
    transport.online();
    assert_eq!(tasks.insert(&task_row(2)).await.map(drop), offline);
    assert_eq!(transport.sent().len(), sent);
    room.sync().await.unwrap();
    assert_eq!(tasks.insert(&task_row(2)).await, Ok(task_key(2)));
    let commits = sent_of(&transport, Commit).len();
    assert_eq!(commits, 1);

    // A network that goes down once the write passed the check leaves it to its retries, as any
    // write: it is not offline. The next one is.
    transport.offline();
    let written = tasks.insert(&task_row(3)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
    assert!(sent_of(&transport, Commit).len() > commits);
    assert_eq!(tasks.insert(&task_row(4)).await.map(drop), offline);
}

#[tokio::test(start_paused = true)]
async fn offline_comes_after_the_validation_and_before_the_encryption() {
    let server = TestServer::new();
    let room_id = server
        .create_room_with("room-a", "notes", notes_schema())
        .await;
    let (transport, tokens, crypto) = (server.transport(), TestTokens::new(), TestCrypto::new());
    let engine: Arc<dyn CryptoEngine> = Arc::clone(&crypto) as Arc<dyn CryptoEngine>;
    let (storage, metadata) = (MemoryStorageEngine::new(), MemoryMetadata::default());
    let client = ZemdbClient::open(
        config(Freshness::Manual),
        storage,
        metadata,
        transport.clone(),
        tokens.clone(),
        Some(engine),
    );
    let client = client.await.unwrap();
    let room = client.room(room_id.clone()).await.unwrap();
    after_a_heartbeat(&transport).await;
    let notes = room.table("notes").unwrap();
    transport.offline();
    assert!(room.sync().await.is_err());
    let sent = transport.sent().len();

    // A row that the schema does not take is invalid, whatever the connection.
    let colored = Row::new().with("id", 1).with("color", "red");
    let invalid = notes.insert(&colored).await;
    assert!(
        matches!(invalid, Err(ClientError::InvalidOperation(_))),
        "{invalid:?}"
    );
    // One that it takes is offline before it is encrypted.
    let note = Row::new().with("id", 1).with("text", "secret");
    let offline = Err(ClientError::Offline(room_id.clone()));
    assert_eq!(notes.insert(&note).await.map(drop), offline);
    assert_eq!(crypto.encryptions(), 0);
    assert_eq!(transport.sent().len(), sent);

    // Once a request gets a reply, it is encrypted and sent.
    transport.online();
    room.sync().await.unwrap();
    notes.insert(&note).await.unwrap();
    assert_eq!(crypto.encryptions(), 1);
    assert_eq!(sent_of(&transport, Commit).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_room_that_fails_closed_with_its_worker_running_is_not_offline() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Manual), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    after_a_heartbeat(&transport).await;
    // A sync registers the replica again, whose `Registered` announces a schema that does not
    // evolve from the replica's, and the deregistration that follows gets no reply: the room
    // fails closed while its worker runs, until its next heartbeat.
    let incarnation = server.incarnation(&room_id).await;
    transport.answer(move |message| match message {
        ClientMessage::Sync {
            correlation_id,
            room_id,
            ..
        } => Some(Ok(error_reply(
            *correlation_id,
            room_id,
            ErrorCode::ClientNotRegistered,
        ))),
        ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            ..
        } => Some(Ok(retyped_registered(
            *correlation_id,
            room_id,
            incarnation,
        ))),
        ClientMessage::DeregisterClient { .. } => Some(Err(TransportError("Offline".to_string()))),
        _ => None,
    });
    let failed_closed = Err(ClientError::FailedClosed(room_id.clone()));
    assert_eq!(room.sync().await, failed_closed);
    let worker = worker(&room).await;
    assert_eq!((worker.running, worker.connected), (true, Some(false)));
    // Its writes fail closed, before the worker stops and after.
    let tasks = room.table("tasks").unwrap();
    assert_eq!(tasks.insert(&task_row(1)).await.map(drop), failed_closed);
    until_within(&room, INTERVAL * 2, |worker| !worker.running).await;
    assert_eq!(tasks.insert(&task_row(1)).await.map(drop), failed_closed);
}

#[tokio::test(start_paused = true)]
async fn a_room_without_a_worker_is_not_offline() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Manual), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    room.close().await.unwrap();
    // A call of `room` cut while its registration waits for its reply: the room is open, without
    // a worker, and does not fail closed.
    let mut held = transport.hold_next_reply(RegisterClient);
    let opening = tokio::spawn({
        let (client, room_id) = (client.clone(), room_id.clone());
        async move { client.room(room_id).await.map(drop) }
    });
    held.replied().await;
    opening.abort();
    let _ = opening.await;
    drop(held);
    let status = room.status().await.unwrap();
    assert_eq!((status.worker, status.failed_closed), (None, false));
    // After a request without a reply, a write is not offline: it goes as in the core, which
    // registers the room first, until the deadline of the write.
    transport.offline();
    assert!(room.sync().await.is_err());
    let written = room.table("tasks").unwrap().insert(&task_row(1)).await;
    assert!(
        matches!(written, Err(ClientError::Network(_))),
        "{written:?}"
    );
    // The next call of `room` starts the worker.
    transport.online();
    let room = client.room(room_id.clone()).await.unwrap();
    assert!(worker(&room).await.running);
}

#[tokio::test(start_paused = true)]
async fn in_live_a_write_stays_offline_while_events_bring_operations_until_a_heartbeat() {
    // The state of the connection is that of the requests: in `live`, after a request
    // without a reply that is not a heartbeat, the writes are `Offline` until the next heartbeat
    // of the worker, an interval after the last one, although the event stream goes on bringing
    // the operations of the others meanwhile. A `sync` with a reply lets them go at once.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = facade(&transport, config(Freshness::Live), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    until(&room, live).await;
    after_a_heartbeat(&transport).await;
    let beat = *sent_of(&transport, Heartbeat).last().unwrap();
    let tasks = room.table("tasks").unwrap();
    let mut all = tasks.watch_scan(&Scan::new());
    assert_eq!(all.next().await, Some(Ok(vec![])));

    // A sync of the application whose reply is lost.
    transport.lose_next_reply(Sync);
    let synced = room.sync().await;
    assert!(matches!(synced, Err(ClientError::Network(_))), "{synced:?}");
    assert_eq!(worker(&room).await.connected, Some(false));

    // What another client writes goes on arriving by the stream, without a sync...
    let syncs = sent_of(&transport, Sync).len();
    server.commit(&room_id, [1]).await;
    assert_eq!(all.next().await, Some(Ok(vec![task_row(1)])));
    server.commit(&room_id, [2]).await;
    assert_eq!(all.next().await, Some(Ok(vec![task_row(1), task_row(2)])));
    assert_eq!(sent_of(&transport, Sync).len(), syncs);
    // ...but a write is `Offline`, with nothing sent, until the next heartbeat.
    let offline = Err(ClientError::Offline(room_id.clone()));
    let sent = transport.sent().len();
    tokio::time::sleep(INTERVAL / 2).await;
    assert_eq!(tasks.insert(&task_row(3)).await.map(drop), offline);
    assert_eq!(transport.sent().len(), sent);
    while sent_of(&transport, Heartbeat).last() == Some(&beat) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let next = *sent_of(&transport, Heartbeat).last().unwrap();
    assert_eq!(next - beat, INTERVAL);
    until(&room, |worker| worker.connected == Some(true)).await;
    assert_eq!(tasks.insert(&task_row(3)).await, Ok(task_key(3)));

    // The same lapse, ended by a `sync` with a reply instead of the heartbeat.
    transport.lose_next_reply(Sync);
    assert!(room.sync().await.is_err());
    assert_eq!(tasks.insert(&task_row(4)).await.map(drop), offline);
    room.sync().await.unwrap();
    assert_eq!(tasks.insert(&task_row(4)).await, Ok(task_key(4)));
}

// ---------------------------------------------------------------------------------------------
// The first token

#[tokio::test(start_paused = true)]
async fn the_first_token_has_60_s_and_one_not_in_the_format_of_the_server_is_config() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    // One that comes within the 60 s is taken.
    let transport = server.transport();
    let tokens = TestTokens::issuing(Issue::After(Duration::from_secs(59)));
    let client = facade(&transport, config(Freshness::Manual), &tokens).await;
    let room = client.room(room_id.clone()).await.unwrap();
    assert!(room.status().await.unwrap().registered);
    assert_eq!(
        transport.bearers(),
        [(RegisterClient, tokens.given()[0].clone())]
    );
    assert_eq!(tokens.calls(), 1);
    // One that does not come within them is no token: the registration goes without one.
    let transport = server.transport();
    let tokens = TestTokens::issuing(Issue::After(Duration::from_secs(61)));
    let client = facade(&transport, config(Freshness::Manual), &tokens).await;
    let opened = client.room(room_id.clone()).await;
    assert!(matches!(opened, Err(ClientError::Network(_))), "{opened:?}");
    assert_eq!(transport.bearers()[0], (RegisterClient, String::new()));
    // One that is not in the format of the server is `Config`, with nothing sent.
    let transport = server.transport();
    let tokens = TestTokens::issuing(Issue::Text("not-a-token".to_string()));
    let client = facade(&transport, config(Freshness::Manual), &tokens).await;
    let opened = client.room(room_id.clone()).await;
    assert!(matches!(opened, Err(ClientError::Config(_))), "{opened:?}");
    assert!(transport.sent().is_empty());
}

#[tokio::test(start_paused = true)]
async fn room_takes_the_first_token_from_the_provider_and_one_of_another_client_is_config() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let other = ClientId::new("eve#7").unwrap();

    // A token of another client: nothing is sent with it, and the room does not open.
    let (transport, tokens) = (server.transport(), TestTokens::new());
    tokens.then(Issue::ForClient(other.clone()));
    let client = facade(&transport, config(Freshness::Periodic), &tokens).await;
    let opened = client.room(room_id.clone()).await;
    assert!(matches!(opened, Err(ClientError::Config(_))), "{opened:?}");
    let message = opened.unwrap_err().to_string();
    assert!(!message.contains(&tokens.given()[0]), "{message}");
    assert!(transport.sent().is_empty());
    tokio::time::sleep(INTERVAL * 2).await;
    assert!(transport.sent().is_empty());

    // The next one is of the client, and the room opens with it, which its registration carries.
    let room = client.room(room_id.clone()).await.unwrap();
    let given = tokens.given();
    assert_eq!(
        tokens.asked(),
        vec![(client.client_id(), room_id.clone()); 2]
    );
    assert_eq!(transport.bearers(), [(RegisterClient, given[1].clone())]);
    assert!(worker(&room).await.running);

    // On an open room, a token of another client changes nothing: the room keeps its token
    // and its worker.
    tokens.then(Issue::ForClient(other));
    let opened = client.room(room_id.clone()).await;
    assert!(matches!(opened, Err(ClientError::Config(_))), "{opened:?}");
    after_a_heartbeat(&transport).await;
    let bearers = transport.bearers();
    assert_eq!(bearers.last(), Some(&(Heartbeat, given[1].clone())));
    assert_eq!(bearers.len(), 2);
    assert!(worker(&room).await.running);

    // Without a token from the provider, an open room keeps its own.
    tokens.then(Issue::Fail);
    client.room(room_id.clone()).await.unwrap();
    after_a_heartbeat(&transport).await;
    let bearers = transport.bearers();
    assert_eq!(bearers.last(), Some(&(Heartbeat, given[1].clone())));
    assert_eq!(bearers.len(), 3);
}

#[tokio::test(start_paused = true)]
async fn without_a_token_a_room_with_metadata_opens_to_be_read_and_the_renewal_brings_it() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, [1]).await;
    // A provider that fails, with the network; and one that does not answer, without it.
    for (issue, network) in [(Issue::Fail, true), (Issue::Hang, false)] {
        let case = format!("{issue:?}");
        let dir = tempdir().unwrap();
        let transport = server.transport();
        let tokens = TestTokens::new();
        let first = on_disk(dir.path(), &transport, config(Freshness::Periodic), &tokens).await;
        first
            .room(room_id.clone())
            .await
            .unwrap()
            .sync()
            .await
            .unwrap();
        first.shutdown().await.unwrap();
        drop(first);

        let transport = server.transport();
        let tokens = TestTokens::issuing(issue);
        if !network {
            transport.offline();
        }
        let client = on_disk(dir.path(), &transport, config(Freshness::Periodic), &tokens).await;
        let room = client.room(room_id.clone()).await.unwrap();
        // Read, unregistered: its registration went without a token, and was refused for it
        // (which asked the provider once more) or got no reply.
        let tasks = room.table("tasks").unwrap();
        assert_eq!(
            tasks.get(&task_key(1)).await,
            Ok(Some(task_row(1))),
            "{case}"
        );
        let status = room.status().await.unwrap();
        assert!(!status.registered, "{case}");
        assert_eq!(status.worker.unwrap().connected, Some(network), "{case}");
        assert_eq!(
            transport.bearers(),
            [(RegisterClient, String::new())],
            "{case}"
        );
        let asked = if network { 2 } else { 1 };
        assert_eq!(tokens.calls(), asked, "{case}");

        // Once the network and the provider are back, the next heartbeat of the worker registers
        // the room, renewing its token when the server refuses it.
        transport.online();
        tokens.set(Issue::Valid(Duration::from_secs(3600)));
        let start = Instant::now();
        while !room.status().await.unwrap().registered {
            assert!(start.elapsed() < INTERVAL * 3, "{case}: not registered");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let token = tokens.given().last().unwrap().clone();
        assert_eq!(tokens.calls(), asked + 1, "{case}");
        let bearers = transport.bearers();
        let registration = bearers
            .iter()
            .rev()
            .find(|(kind, _)| *kind == RegisterClient);
        assert_eq!(registration, Some(&(RegisterClient, token)), "{case}");
        client.shutdown().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn without_a_token_a_room_without_metadata_has_no_handle_nor_worker() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    for (issue, network) in [(Issue::Fail, true), (Issue::Hang, false)] {
        let case = format!("{issue:?}");
        let transport = server.transport();
        let tokens = TestTokens::issuing(issue);
        if !network {
            transport.offline();
        }
        let client = facade(&transport, config(Freshness::Live), &tokens).await;
        let opened = client.room(room_id.clone()).await;
        assert!(
            matches!(opened, Err(ClientError::Network(_))),
            "{case}: {opened:?}"
        );
        // No worker: no heartbeat, nor an event stream.
        let sent = transport.sent().len();
        tokio::time::sleep(INTERVAL * 2).await;
        assert_eq!(transport.sent().len(), sent, "{case}");
        assert_eq!(heartbeats(&transport), 0, "{case}");
        assert_eq!(transport.stream_opens().len(), 0, "{case}");
    }
}
