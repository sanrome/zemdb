//! The worker of a room in `live`, with the event stream of the room, against the server in
//! process and with the paused clock: the operations of each `ops` event applied without a sync,
//! once checked, and each other event as a notice for a sync, a registration or a heartbeat; the
//! stream opened again after its end, an error or its silence, with a wait; its epoch; its token;
//! the stream turned off; and the mode that opens and closes it.
//! With the longest interval of the heartbeats, so that, once a heartbeat went, what a test waits
//! for comes long before the next one: what reaches the replica meanwhile came by the stream.
#![allow(clippy::disallowed_methods)]

mod common;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::Bytes;
use common::*;
use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::Instant;
use zemdb_client::{
    Client, ClientConfig, ClientError, EventStream, FileMetadata, Freshness, MemoryMetadata,
    MetadataStore, TransportError, WorkerStatus,
};
use zemdb_core::ClientMessageKind::{
    Commit, Heartbeat, RegisterClient, RequestSnapshotChunk, Sync,
};
use zemdb_core::*;
use zemdb_storage::{MemoryStorageEngine, StorageEngine, StorageError};

/// The interval of the heartbeats: the longest one.
const INTERVAL: Duration = Duration::from_secs(59);

/// The configuration of ana, in `live`, with a heartbeat every [`INTERVAL`].
fn live() -> ClientConfig {
    ClientConfig {
        freshness: Freshness::Live,
        heartbeat_interval: INTERVAL,
        ..ClientConfig::new("ana")
    }
}

/// A client with `config` over `storage` and a memory store of metadata, through `transport`,
/// with `tokens`.
async fn client_over(
    storage: impl StorageEngine + 'static,
    transport: &TestTransport,
    config: ClientConfig,
    tokens: &TestTokens,
) -> Arc<Client> {
    let metadata = MemoryMetadata::default();
    let client = Client::open(config, storage, metadata, transport.clone(), tokens.clone());
    Arc::new(client.await.unwrap())
}

/// A server with `room-a`, and a client of it over a memory engine, with the room open and its
/// worker started.
struct Live {
    server: TestServer,
    room_id: RoomId,
    transport: TestTransport,
    storage: MemoryStorageEngine,
    tokens: TestTokens,
    client: Arc<Client>,
}

/// The room `room-a` of `server`, and a client with `config` whose worker runs, once its event
/// stream opened, with the sync after it.
async fn started(server: TestServer, config: ClientConfig) -> Live {
    let room_id = server.create_room("room-a").await;
    let (transport, storage, tokens) = (
        server.transport(),
        MemoryStorageEngine::new(),
        TestTokens::new(),
    );
    let client = client_over(storage.clone(), &transport, config, &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    Live {
        server,
        room_id,
        transport,
        storage,
        tokens,
        client,
    }
}

/// The worker of `room_id` of `client`, as its status gives it.
async fn worker(client: &Client, room_id: &RoomId) -> WorkerStatus {
    let status = client.status(room_id.clone()).await.unwrap();
    status.worker.unwrap()
}

/// Waits until the event stream of the worker of `room_id` is open, and the sync after it ended,
/// within a few seconds.
async fn opened(client: &Client, room_id: &RoomId) {
    let start = Instant::now();
    while !worker(client, room_id).await.stream_open {
        assert!(start.elapsed() < Duration::from_secs(5), "No stream opened");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
}

/// The head of the replica of `room_id` of `client`.
async fn head(client: &Client, room_id: &RoomId) -> u64 {
    client.status(room_id.clone()).await.unwrap().head_seq.get()
}

/// Waits until the replica of `room_id` of `client` reaches `at`, within a second: long before
/// the next heartbeat, once one went.
async fn reaches(client: &Client, room_id: &RoomId, at: u64) {
    let start = Instant::now();
    while head(client, room_id).await != at {
        let late = start.elapsed() >= Duration::from_secs(1);
        assert!(
            !late,
            "The replica of {room_id} did not reach {at} within a second"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// When each message of `kind` went through `transport`, in order.
fn sent_of(transport: &TestTransport, kind: ClientMessageKind) -> Vec<Instant> {
    let sent = transport.sent_at().into_iter();
    let of = |(message, _): &(ClientMessage, Instant)| message.kind() == kind;
    sent.filter(of).map(|(_, at)| at).collect()
}

/// How many syncs went through `transport`.
fn syncs(transport: &TestTransport) -> usize {
    sent_of(transport, Sync).len()
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

/// Waits until `transport` opened `opens` event streams, refused or not, within `within`.
async fn opens(transport: &TestTransport, opens: usize, within: Duration) {
    let start = Instant::now();
    while transport.stream_opens().len() < opens {
        assert!(start.elapsed() < within, "{:?}", transport.stream_opens());
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn in_live_what_another_client_writes_arrives_by_its_events_without_a_sync_or_a_heartbeat() {
    let Live {
        server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let (before, beats) = (syncs(&transport), heartbeats(&transport));
    for id in 1..=3 {
        server.commit(&room_id, [id]).await;
        reaches(&client, &room_id, id as u64).await;
    }
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);
    // By their events, with no sync, and no heartbeat meanwhile.
    assert_eq!(syncs(&transport), before);
    assert_eq!(heartbeats(&transport), beats);
    assert_eq!(transport.stream_opens().len(), 1);
    let status = worker(&client, &room_id).await;
    assert!(status.stream_open && status.effective == Freshness::Live);
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_ends_opens_again_after_a_wait_and_the_sync_after_it_brings_what_came_meanwhile(
) {
    let Live {
        server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let before = syncs(&transport);
    // The opening that follows the end waits before it reaches the server.
    let mut held = transport.hold_next_events();
    let ended = Instant::now();
    transport.end_streams();
    held.replied().await;
    let reopened = transport.stream_opens_at()[1];
    assert!(
        reopened - ended <= Duration::from_secs(1),
        "{reopened:?} {ended:?}"
    );
    assert!(!worker(&client, &room_id).await.stream_open);
    // What is written while no stream is open comes in no event.
    server.commit(&room_id, [1, 2]).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(head(&client, &room_id).await, 0);

    held.release();
    reaches(&client, &room_id, 2).await;
    assert_eq!(tasks(&storage, &room_id).await, [1, 2]);
    assert_eq!(syncs(&transport), before + 1);
    assert!(worker(&client, &room_id).await.stream_open);
    assert_eq!(transport.stream_opens().len(), 2);
    assert_eq!(transport.streams_open(), 1);
}

#[tokio::test(start_paused = true)]
async fn many_notices_during_a_held_sync_lead_to_a_single_sync_more_and_events_apply_meanwhile() {
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let before = syncs(&transport);
    let mut held = transport.hold_next_reply(Sync);
    transport.inject("event: lagged\ndata: true\n\n");
    held.replied().await;
    for _ in 0..5 {
        transport.inject("event: lagged\ndata: true\n\n");
    }
    // The operations of the events reach the replica while the sync waits for its reply.
    for id in 1..=3 {
        server.commit(&room_id, [id]).await;
    }
    reaches(&client, &room_id, 3).await;
    held.release();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(head(&client, &room_id).await, 3);
    assert_eq!(syncs(&transport), before + 2);
    let requests = requests(&transport);
    assert_eq!(requests[requests.len() - 2..], [("sync", 0), ("sync", 3)]);
}

#[tokio::test(start_paused = true)]
async fn lagged_and_head_advanced_lead_to_a_sync() {
    let Live {
        server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let before = syncs(&transport);
    transport.inject("event: lagged\ndata: true\n\n");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(syncs(&transport), before + 1);

    // An operation too large for an `ops` event: the server announces its head instead.
    let title = "x".repeat(5000);
    let row = RowBuilder::new()
        .set("id", 1)
        .set("title", title.as_str())
        .build();
    let op = tasks_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap();
    server.commit_ops(&room_id, [op]).await;
    reaches(&client, &room_id, 1).await;
    assert_eq!(tasks(&storage, &room_id).await, [1]);
    assert_eq!(syncs(&transport), before + 2);
}

#[tokio::test(start_paused = true)]
async fn schema_reloaded_registers_the_replica_with_its_cursor_and_it_takes_the_new_schema() {
    let dir = tempdir().unwrap();
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let client = Client::open(
        live(),
        MemoryStorageEngine::new(),
        FileMetadata::new(dir.path()),
        transport.clone(),
        TestTokens::new(),
    );
    let client = Arc::new(client.await.unwrap());
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;
    after_a_heartbeat(&transport).await;
    let (registrations, before) = (transport.registrations().len(), syncs(&transport));

    let schema_id = SchemaId::new("tasks").unwrap();
    let tag = || ColumnDef::new("tag", DataType::String).nullable(true);
    let registry = &server.state.schema_registry;
    let added = registry.add_column_with_constraints(&schema_id, "tasks", tag(), vec![], vec![]);
    added.unwrap();
    server
        .state
        .room_manager
        .reload_schema_for_rooms(&schema_id)
        .await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let incarnation = server.incarnation(&room_id).await;
    let registered = transport.registrations();
    assert_eq!(registered[registrations..], [(Some(1), Some(incarnation))]);
    let mut tagged = tasks_schema();
    tagged
        .add_column_with_constraints("tasks", tag(), vec![], vec![])
        .unwrap();
    let metadata = FileMetadata::new(dir.path()).room(&room_id).await;
    assert_eq!(metadata.unwrap().unwrap().schema, tagged);
    // The sync of the worker registered it first, and found nothing more.
    assert_eq!(syncs(&transport), before + 1);
    assert_eq!(head(&client, &room_id).await, 1);
}

/// A server that prunes, with `room-a` holding the tasks 1 to 9 and its log pruned before 6.
async fn pruned() -> (TestServer, RoomId, Vec<SequencedOperation>) {
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let ops = server.commit(&room_id, 1..=9).await;
    (server, room_id, ops)
}

#[tokio::test(start_paused = true)]
async fn snapshot_wanted_has_the_designee_upload_without_waiting_for_its_heartbeat() {
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = client_over(MemoryStorageEngine::new(), &transport, live(), &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    server.commit(&room_id, 1..=9).await;
    client.sync(room_id.clone()).await.unwrap();
    server.prune(&room_id, 6).await;
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    after_a_heartbeat(&transport).await;
    let beats = heartbeats(&transport);

    // A client behind the log registers: the server designates the client, and announces it.
    let other = server.transport();
    let lagging = client_over(MemoryStorageEngine::new(), &other, live(), &tokens).await;
    open_room(&lagging, &room_id).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let active = server.state.snapshot_relay.active_snapshot_seq(&room_id);
    assert_eq!(active, Some(SequenceNumber::new(9)));
    // In the task of the transfers, as `heartbeat` sends them: the heartbeat that learned of the
    // designation, and one after the upload.
    assert_eq!(heartbeats(&transport), beats + 2);
}

#[tokio::test(start_paused = true)]
async fn snapshot_available_has_a_replica_that_waits_for_one_download_it_without_its_heartbeat() {
    let (server, room_id, ops) = pruned().await;
    server.prune(&room_id, 6).await;
    let (transport, storage, tokens) = (
        server.transport(),
        MemoryStorageEngine::new(),
        TestTokens::new(),
    );
    let client = client_over(storage.clone(), &transport, live(), &tokens).await;
    // It registers behind the log, without a snapshot to download.
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    after_a_heartbeat(&transport).await;
    let beats = heartbeats(&transport);
    assert!(sent_of(&transport, RequestSnapshotChunk).is_empty());

    // The snapshot worker of the server uploads one, which the room announces.
    let snapshot = snapshot(&room_id, tasks_schema(), &ops).await;
    server.upload(&room_id, 9, snapshot).await;
    reaches(&client, &room_id, 9).await;
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    // In the task of the transfers, as `heartbeat` sends them: the heartbeat that learned of the
    // snapshot, and one after the download.
    assert_eq!(heartbeats(&transport), beats + 2);
}

#[tokio::test(start_paused = true)]
async fn an_unknown_event_and_a_snapshot_announced_to_a_replica_that_waits_for_none_do_nothing() {
    let Live {
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let (before, beats) = (syncs(&transport), heartbeats(&transport));
    transport.inject("event: snapshot_available\ndata: 3\n\n");
    transport.inject("event: renamed\ndata: 3\n\n");
    transport.inject(": a comment\n\n");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!((syncs(&transport), heartbeats(&transport)), (before, beats));
    assert!(sent_of(&transport, RequestSnapshotChunk).is_empty());
    assert!(worker(&client, &room_id).await.stream_open);
}

#[tokio::test(start_paused = true)]
async fn with_the_event_stream_off_live_falls_to_periodic_and_tries_it_again_after_events_retry() {
    let server = TestServer::with(|config| config.max_sse_streams = Some(0));
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let config = ClientConfig {
        heartbeat_interval: Duration::from_secs(10),
        events_retry: Duration::from_secs(120),
        ..live()
    };
    let client = client_over(MemoryStorageEngine::new(), &transport, config, &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let status = worker(&client, &room_id).await;
    assert_eq!(status.freshness, Freshness::Live);
    assert_eq!(status.effective, Freshness::Periodic);
    assert!(!status.stream_open && status.running, "{status:?}");
    assert_eq!(status.last_error, None);
    let first = transport.stream_opens_at()[0];

    // What another client writes comes with the next heartbeat.
    server.commit(&room_id, [1]).await;
    let start = Instant::now();
    while head(&client, &room_id).await != 1 {
        assert!(
            start.elapsed() <= Duration::from_secs(10),
            "No heartbeat synced"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // The stream again once `events_retry` went by, and not before.
    tokio::time::sleep_until(first + Duration::from_secs(119)).await;
    assert_eq!(transport.stream_opens().len(), 1);
    tokio::time::sleep_until(first + Duration::from_secs(121)).await;
    assert_eq!(transport.stream_opens().len(), 2);
    assert_eq!(
        worker(&client, &room_id).await.effective,
        Freshness::Periodic
    );
}

#[tokio::test(start_paused = true)]
async fn with_the_event_stream_off_an_events_retry_beyond_the_clock_runs_as_in_periodic_for_good() {
    let server = TestServer::with(|config| config.max_sse_streams = Some(0));
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    // The longest delay, as for "never".
    let config = ClientConfig {
        events_retry: Duration::MAX,
        ..live()
    };
    let client = client_over(MemoryStorageEngine::new(), &transport, config, &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.stream_opens().len(), 1);
    let status = worker(&client, &room_id).await;
    assert_eq!(status.effective, Freshness::Periodic);
    assert!(status.running && status.last_error.is_none(), "{status:?}");

    // What another client writes comes with the next heartbeat.
    server.commit(&room_id, [1]).await;
    let start = Instant::now();
    while head(&client, &room_id).await != 1 {
        assert!(
            start.elapsed() <= INTERVAL + Duration::from_secs(1),
            "No heartbeat synced"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // A day later, the stream was not tried again.
    tokio::time::sleep(Duration::from_secs(86_400)).await;
    assert_eq!(transport.stream_opens().len(), 1);
    let status = worker(&client, &room_id).await;
    assert!(status.running && status.last_error.is_none(), "{status:?}");
}

#[tokio::test(start_paused = true)]
async fn a_refusal_events_disabled_with_an_events_retry_beyond_the_clock_leaves_the_worker_running()
{
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    // The network refuses the opening as the server would with its event stream off.
    transport.answer_next_events(|room_id| Ok(refused(room_id, ErrorCode::EventsDisabled)));
    let config = ClientConfig {
        heartbeat_interval: Duration::from_secs(10),
        events_retry: Duration::MAX,
        ..live()
    };
    let client = client_over(MemoryStorageEngine::new(), &transport, config, &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.stream_opens().len(), 1);
    let before = heartbeats(&transport);
    tokio::time::sleep(Duration::from_secs(21)).await;
    let status = worker(&client, &room_id).await;
    assert!(status.running && status.last_error.is_none(), "{status:?}");
    assert_eq!(status.effective, Freshness::Periodic);
    assert!(heartbeats(&transport) >= before + 2);
}

#[tokio::test(start_paused = true)]
async fn an_events_retry_beyond_the_clock_holds_across_modes_and_a_new_worker_tries_the_stream() {
    let server = TestServer::with(|config| config.max_sse_streams = Some(0));
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let config = ClientConfig {
        heartbeat_interval: Duration::from_secs(10),
        events_retry: Duration::MAX,
        ..live()
    };
    let client = client_over(MemoryStorageEngine::new(), &transport, config, &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.stream_opens().len(), 1);
    // Out of `Live` and back: the wait holds.
    let periodic = client.set_freshness(room_id.clone(), Freshness::Periodic);
    periodic.await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    client
        .set_freshness(room_id.clone(), Freshness::Live)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(transport.stream_opens().len(), 1);
    assert_eq!(
        worker(&client, &room_id).await.effective,
        Freshness::Periodic
    );
    // A worker started again tries the stream again.
    client.stop_worker(room_id.clone()).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.stream_opens().len(), 2);
    let status = worker(&client, &room_id).await;
    assert!(status.running && status.last_error.is_none(), "{status:?}");
}

#[tokio::test(start_paused = true)]
async fn an_events_retry_that_fits_in_the_clock_by_less_than_a_millisecond_leaves_the_worker_running(
) {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    transport.answer_next_events(|room_id| Ok(refused(room_id, ErrorCode::EventsDisabled)));
    // It fits, and the timer rounds it up past the clock.
    let config = ClientConfig {
        heartbeat_interval: Duration::from_secs(10),
        events_retry: longest_from(Instant::now()) - Duration::from_micros(500),
        ..live()
    };
    let client = client_over(MemoryStorageEngine::new(), &transport, config, &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.stream_opens().len(), 1);
    let status = worker(&client, &room_id).await;
    assert!(status.running && status.last_error.is_none(), "{status:?}");
}

#[tokio::test(start_paused = true)]
async fn a_live_worker_whose_server_turns_its_event_stream_on_opens_it_after_events_retry() {
    let config = ClientConfig {
        events_retry: Duration::from_secs(120),
        ..live()
    };
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), config).await;
    after_a_heartbeat(&transport).await;
    // The stream ends, its reopening gets no response, and the next one is refused as by a
    // server with its event stream turned off: that opening clears the error of the one before,
    // and the worker runs as in `periodic`.
    let mut answers = vec![
        Err(TransportError("Cut".to_string())),
        Ok(refused(&room_id, ErrorCode::EventsDisabled)),
    ];
    transport.answer_events(move |_| (!answers.is_empty()).then(|| answers.remove(0)));
    let mut cut = transport.hold_next_events();
    transport.end_streams();
    cut.replied().await;
    // The next opening waits before its answer, so the status is that of the one without a
    // response.
    let mut refused = transport.hold_next_events();
    cut.release();
    refused.replied().await;
    let failed = worker(&client, &room_id).await;
    let network = matches!(failed.last_error, Some(ClientError::Network(_)));
    assert!(network && failed.effective == Freshness::Live, "{failed:?}");
    refused.release();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let disabled = worker(&client, &room_id).await;
    assert_eq!(disabled.effective, Freshness::Periodic);
    assert!(
        disabled.last_error.is_none() && !disabled.stream_open,
        "{disabled:?}"
    );

    // The server turns its event stream on: the worker opens it once `events_retry` went by.
    let refused_at = transport.stream_opens_at()[2];
    tokio::time::sleep_until(refused_at + Duration::from_secs(119)).await;
    assert_eq!(transport.stream_opens().len(), 3);
    let before = syncs(&transport);
    tokio::time::sleep_until(refused_at + Duration::from_secs(121)).await;
    let status = worker(&client, &room_id).await;
    assert_eq!(status.effective, Freshness::Live);
    assert!(status.stream_open, "{status:?}");
    // The sync after it opened.
    assert_eq!(syncs(&transport), before + 1);
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;
}

#[tokio::test(start_paused = true)]
async fn a_stream_refused_for_its_token_opens_again_with_a_renewed_one_and_a_forbidden_one_too() {
    let Live {
        room_id,
        transport,
        tokens,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    let first = transport.stream_opens()[0].clone();
    // The stream ends as its token expired, and its reopening is refused for it: the client
    // renews the token and opens it again at once.
    transport.answer_next_events(|room_id| Ok(refused(room_id, ErrorCode::Unauthorized)));
    transport.end_streams();
    opens(&transport, 3, Duration::from_secs(2)).await;
    opened(&client, &room_id).await;
    let given = tokens.given();
    assert_eq!(given.len(), 1);
    let renewed = [first.clone(), first, given[0].clone()];
    assert_eq!(transport.stream_opens(), renewed);

    // A reopening refused as forbidden: the next one, after a wait, renews the token first.
    transport.answer_next_events(|room_id| Ok(refused(room_id, ErrorCode::Forbidden)));
    transport.end_streams();
    opens(&transport, 5, Duration::from_secs(10)).await;
    opened(&client, &room_id).await;
    let given = tokens.given();
    assert_eq!(given.len(), 2);
    assert_eq!(
        transport.stream_opens()[3..],
        [given[0].clone(), given[1].clone()]
    );
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_its_token_ends_opens_again_with_the_same_token_while_it_is_valid() {
    // With the paused clock, the server ends the stream once the time of its token (300 s) went
    // by in virtual time, while the token is still valid on the wall clock.
    let Live {
        room_id,
        transport,
        tokens,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    let before = syncs(&transport);
    tokio::time::sleep(Duration::from_secs(302)).await;
    let opened = transport.stream_opens();
    assert_eq!(opened.len(), 2);
    assert_eq!(opened[0], opened[1]);
    assert_eq!(tokens.calls(), 0);
    // The sync after it: the replica is at the head, and no heartbeat syncs.
    assert_eq!(syncs(&transport), before + 1);
    assert!(worker(&client, &room_id).await.stream_open);
}

/// Answers the next sync that `transport` sends with `ClientNotRegistered`, and the registration
/// with a cursor that follows with `InvalidSequence`, as after the server lost sequences: the
/// replica is discarded, and registers again without a cursor.
fn discard_next_sync(transport: &TestTransport) {
    let (mut sync, mut registration) = (true, true);
    transport.answer(move |message| {
        let code = match message {
            ClientMessage::Sync { .. } if std::mem::take(&mut sync) => {
                ErrorCode::ClientNotRegistered
            }
            ClientMessage::RegisterClient {
                current_seq: Some(_),
                ..
            } if std::mem::take(&mut registration) => ErrorCode::InvalidSequence,
            _ => return None,
        };
        let (correlation_id, room_id) = match message {
            ClientMessage::RegisterClient {
                correlation_id,
                room_id,
                ..
            } => (*correlation_id, room_id.clone()),
            message => request(message),
        };
        Some(Ok(error_reply(correlation_id, &room_id, code)))
    });
}

/// The epoch of the replica of `room_id` of `client`.
async fn epoch(client: &Client, room_id: &RoomId) -> u64 {
    client.status(room_id.clone()).await.unwrap().epoch
}

#[tokio::test(start_paused = true)]
async fn a_stream_of_an_earlier_epoch_is_replaced_at_its_next_event_or_heartbeat_with_no_effect() {
    let Live {
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let opened_in = epoch(&client, &room_id).await;
    // A sync of the application discards the replica.
    discard_next_sync(&transport);
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(epoch(&client, &room_id).await, opened_in + 1);
    let (registrations, before, beats) = (
        transport.registrations().len(),
        syncs(&transport),
        heartbeats(&transport),
    );
    // An event of the stream of the earlier epoch: no heartbeat for it; the stream is replaced,
    // with the sync after it.
    transport.inject("event: snapshot_wanted\ndata: \n\n");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(heartbeats(&transport), beats);
    assert_eq!(transport.stream_opens().len(), 2);
    assert_eq!(transport.streams_open(), 1);
    assert_eq!(syncs(&transport), before + 1);
    assert_eq!(transport.registrations().len(), registrations);

    // Without an event, the next heartbeat notices the epoch.
    discard_next_sync(&transport);
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(epoch(&client, &room_id).await, opened_in + 2);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.stream_opens().len(), 2);
    after_a_heartbeat(&transport).await;
    assert_eq!(transport.stream_opens().len(), 3);
    assert_eq!(transport.streams_open(), 1);
    assert!(worker(&client, &room_id).await.stream_open);
}

#[tokio::test(start_paused = true)]
async fn a_stream_silent_for_45_seconds_is_replaced_and_one_with_its_keep_alives_is_not() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    transport.answer_next_events(|_| Ok(silent()));
    let client = client_over(MemoryStorageEngine::new(), &transport, live(), &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    let first = transport.stream_opens_at()[0];
    tokio::time::sleep_until(first + Duration::from_secs(44)).await;
    assert_eq!(transport.stream_opens().len(), 1);
    tokio::time::sleep_until(first + Duration::from_secs(47)).await;
    assert_eq!(transport.stream_opens().len(), 2);
    assert_eq!(transport.streams_open(), 1);
    // The stream of the server sends a keep-alive every 15 s.
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert_eq!(transport.stream_opens().len(), 2);
    assert!(worker(&client, &room_id).await.stream_open);
}

#[tokio::test(start_paused = true)]
async fn an_event_that_does_not_decode_leads_to_a_sync_and_one_too_long_to_another_stream() {
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let before = syncs(&transport);
    transport.inject("event: ops\ndata: not base64\n\n");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(syncs(&transport), before + 1);
    transport.inject("event: head_advanced\ndata: x\n\n");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(syncs(&transport), before + 2);
    let status = worker(&client, &room_id).await;
    assert!(status.running && status.stream_open, "{status:?}");
    assert_eq!(transport.stream_opens().len(), 1);

    // An event longer than 64 KiB ends the stream: another one, and the sync after it.
    let long = format!("event: ops\ndata: {}\n\n", "a".repeat(65 << 10));
    transport.inject(&long);
    opens(&transport, 2, Duration::from_secs(2)).await;
    opened(&client, &room_id).await;
    assert_eq!(transport.streams_open(), 1);
    assert_eq!(syncs(&transport), before + 3);
    let status = worker(&client, &room_id).await;
    assert!(status.running && status.last_error.is_none(), "{status:?}");
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;
}

#[tokio::test(start_paused = true)]
async fn setting_live_opens_the_stream_at_once_and_another_mode_or_a_stop_closes_it() {
    let config = ClientConfig {
        freshness: Freshness::Periodic,
        ..live()
    };
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started_without_stream(config).await;
    after_a_heartbeat(&transport).await;
    assert!(transport.stream_opens().is_empty());
    let set = |freshness| client.set_freshness(room_id.clone(), freshness);
    set(Freshness::Live).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.streams_open(), 1);
    assert!(worker(&client, &room_id).await.stream_open);
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;

    set(Freshness::Manual).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.streams_open(), 0);
    assert!(!worker(&client, &room_id).await.stream_open);
    set(Freshness::Live).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.streams_open(), 1);
    client.stop_worker(room_id.clone()).await.unwrap();
    assert_eq!(transport.streams_open(), 0);
    assert_eq!(transport.stream_opens().len(), 2);

    // An opening in flight is cut by another mode, and by a stop: nothing holds the client.
    let mut held = transport.hold_next_events();
    set(Freshness::Live).await.unwrap();
    held.replied().await;
    set(Freshness::Manual).await.unwrap();
    released(&client).await;
    let mut held_again = transport.hold_next_events();
    set(Freshness::Live).await.unwrap();
    held_again.replied().await;
    let stopped = client.stop_worker(room_id.clone());
    let stopped = tokio::time::timeout(Duration::from_secs(10), stopped).await;
    stopped.expect("The stop waited for the opening").unwrap();
    released(&client).await;
    drop((held, held_again));
}

/// Waits until nothing but the test holds `client`, within a second: a step of a worker in
/// flight lets it go at once.
async fn released(client: &Arc<Client>) {
    let start = Instant::now();
    while Arc::strong_count(client) > 1 {
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "The client is held"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// [`started`] with `config`, without waiting for an event stream.
async fn started_without_stream(config: ClientConfig) -> Live {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, storage, tokens) = (
        server.transport(),
        MemoryStorageEngine::new(),
        TestTokens::new(),
    );
    let client = client_over(storage.clone(), &transport, config, &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    Live {
        server,
        room_id,
        transport,
        storage,
        tokens,
        client,
    }
}

/// An open event stream that brings `text` and ends.
fn ending_with(text: &'static str) -> EventStream {
    let block = Ok(Bytes::from_static(text.as_bytes()));
    EventStream::Open(Box::pin(futures::stream::iter([block])))
}

#[tokio::test(start_paused = true)]
async fn a_stream_without_an_event_opens_again_after_a_wait_that_doubles_and_one_with_one_soon() {
    // Each stream ends at once, without an event, as through a proxy that cuts it: the waits
    // double up to `retry_max` (30 s), so about 25 in 300 s on average.
    let Live {
        room_id,
        transport,
        client,
        ..
    } = started_without_stream(live()).await;
    transport.answer_events(|_| Some(Ok(ending_with(": a comment\n\n"))));
    tokio::time::sleep(Duration::from_secs(300)).await;
    let ended = transport.stream_opens().len();
    assert!((5..50).contains(&ended), "{ended}");

    // A stream with an event starts the waits over: each one opens the next within a second.
    transport.answer_events(|_| Some(Ok(ending_with("event: lagged\ndata: true\n\n"))));
    opens(&transport, ended + 1, Duration::from_secs(31)).await;
    let start = transport.stream_opens().len();
    tokio::time::sleep(Duration::from_secs(60)).await;
    let opened = transport.stream_opens().len() - start;
    assert!(opened > 60, "{opened}");
    let status = worker(&client, &room_id).await;
    assert!(status.running, "{status:?}");
}

#[tokio::test(start_paused = true)]
async fn a_refused_stream_opens_again_after_a_wait_that_doubles_and_shows_the_error() {
    let Live {
        room_id,
        transport,
        client,
        ..
    } = started_without_stream(live()).await;
    transport.answer_events(|room_id| Some(Ok(refused(room_id, ErrorCode::RateLimited))));
    transport.end_streams();
    tokio::time::sleep(Duration::from_secs(300)).await;
    let refused = transport.stream_opens().len();
    assert!((5..50).contains(&refused), "{refused}");
    let status = worker(&client, &room_id).await;
    assert!(status.running && !status.stream_open, "{status:?}");
    // The heartbeats clear the error of the last opening.
    let opening = transport.stream_opens_at()[refused - 1];
    let heartbeat = *sent_of(&transport, Heartbeat).last().unwrap();
    match heartbeat > opening {
        true => assert_eq!(status.last_error, None),
        false => assert!(
            matches!(status.last_error, Some(ClientError::Network(_))),
            "{status:?}"
        ),
    }
}

#[tokio::test(start_paused = true)]
async fn an_opening_of_the_stream_that_finds_the_room_out_of_the_client_stops_the_worker() {
    // The stream ends, and before it opens again, a sync of the application gets `RoomReset` to
    // its registration, and its discard fails deleting the data: the room is out of the client,
    // and the opening stops the worker, without reaching the server, long before its next
    // heartbeat.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let storage = TestEngine::new(MemoryStorageEngine::new());
    let client = client_over(storage.clone(), &transport, live(), &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    after_a_heartbeat(&transport).await;
    let beats = heartbeats(&transport);
    let (mut sync, mut registration) = (true, true);
    transport.answer(move |message| {
        let code = match message.kind() {
            Sync if std::mem::take(&mut sync) => ErrorCode::ClientNotRegistered,
            RegisterClient if std::mem::take(&mut registration) => ErrorCode::RoomReset,
            _ => return None,
        };
        let (correlation_id, room_id) = match message {
            ClientMessage::RegisterClient {
                correlation_id,
                room_id,
                ..
            } => (*correlation_id, room_id.clone()),
            message => request(message),
        };
        Some(Ok(error_reply(correlation_id, &room_id, code)))
    });
    let io = StorageError::Io(std::io::Error::other("Fabricated"));
    storage.fail_next("delete_room", io);
    transport.end_streams();
    assert!(client.sync(room_id.clone()).await.is_err());
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(transport.stream_opens().len(), 1);

    transport.online();
    open_room(&client, &room_id).await.unwrap();
    let stopped = worker(&client, &room_id).await;
    assert!(!stopped.running, "{stopped:?}");
    let closed = Some(ClientError::RoomClosed(room_id.clone()));
    assert_eq!(stopped.last_error, closed);
    assert_eq!(heartbeats(&transport), beats);
}

/// A schema `other` of `tasks` whose `title` is an int: it does not evolve from
/// [`tasks_schema`].
fn retyped() -> Schema {
    let retyped = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::Int);
    Schema::builder().table(retyped).build()
}

/// Answers the next sync that `transport` sends with `ClientNotRegistered`, and the registration
/// that follows with a `Registered` of `room_incarnation` with [`retyped`]: the room fails
/// closed.
fn fail_next_sync_closed(transport: &TestTransport, room_incarnation: u128) {
    let (mut sync, mut registration) = (true, true);
    transport.answer(move |message| match message {
        ClientMessage::Sync {
            correlation_id,
            room_id,
            ..
        } if std::mem::take(&mut sync) => Some(Ok(error_reply(
            *correlation_id,
            room_id,
            ErrorCode::ClientNotRegistered,
        ))),
        ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            ..
        } if std::mem::take(&mut registration) => {
            let registered = ServerMessage::Registered {
                correlation_id: *correlation_id,
                room_id: room_id.clone(),
                head_seq: SequenceNumber::new(0),
                tail_seq: SequenceNumber::new(1),
                schema_id: SchemaId::new("other").unwrap(),
                schema: retyped(),
                active_snapshot_seq: None,
                room_incarnation,
            };
            Some(Ok(encode_message(&registered).unwrap()))
        }
        _ => None,
    });
}

#[tokio::test(start_paused = true)]
async fn an_opening_of_the_stream_that_finds_the_room_failing_closed_stops_the_worker() {
    // The stream ends, and before it opens again, a sync of the application gets a
    // `Registered` with a schema that does not evolve from the replica's: the room fails
    // closed, and the opening stops the worker without opening anything.
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let beats = heartbeats(&transport);
    let room_incarnation = server.incarnation(&room_id).await;
    fail_next_sync_closed(&transport, room_incarnation);
    transport.end_streams();
    let failed = client.sync(room_id.clone()).await;
    let failed_closed = ClientError::FailedClosed(room_id.clone());
    assert_eq!(failed, Err(failed_closed.clone()));
    tokio::time::sleep(Duration::from_secs(2)).await;
    let stopped = worker(&client, &room_id).await;
    assert!(!stopped.running, "{stopped:?}");
    assert_eq!(stopped.last_error, Some(failed_closed));
    assert_eq!(transport.stream_opens().len(), 1);
    assert_eq!(heartbeats(&transport), beats);
}

#[tokio::test(start_paused = true)]
async fn a_refusal_of_the_stream_for_another_room_or_a_request_is_an_error_and_an_opening_clears_it(
) {
    let Live {
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let other = RoomId::new("room-b").unwrap();
    let mut refusals = vec![
        refused(&other, ErrorCode::EventsDisabled),
        EventStream::Refused(
            encode_message(&ServerMessage::Error {
                correlation_id: Some(CorrelationId::new(7)),
                room_id: Some(room_id.clone()),
                code: ErrorCode::EventsDisabled,
                message: "Fabricated".to_string(),
            })
            .unwrap(),
        ),
    ];
    for count in [2, 4] {
        // An event starts the waits of the reopenings over: the next one comes within a second.
        transport.inject("event: lagged\ndata: true\n\n");
        tokio::time::sleep(Duration::from_millis(10)).await;
        let refusal = refusals.remove(0);
        transport.answer_next_events(move |_| Ok(refusal));
        let mut refusing = transport.hold_next_events();
        transport.end_streams();
        refusing.replied().await;
        // The next opening waits before it reaches the server, so the status is that of the
        // refusal.
        let mut next = transport.hold_next_events();
        refusing.release();
        next.replied().await;
        assert_eq!(transport.stream_opens().len(), count + 1);
        let status = worker(&client, &room_id).await;
        assert_eq!(status.effective, Freshness::Live);
        let network = matches!(status.last_error, Some(ClientError::Network(_)));
        assert!(network && !status.stream_open, "{status:?}");
        // The next opening, after a wait and long before the next heartbeat, clears the error.
        next.release();
        opened(&client, &room_id).await;
        assert_eq!(worker(&client, &room_id).await.last_error, None);
    }
}

#[tokio::test(start_paused = true)]
async fn a_room_that_the_server_stops_ends_its_stream_and_the_worker_opens_another() {
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    server.state.room_manager.shutdown_room(&room_id).await;
    opens(&transport, 2, Duration::from_secs(2)).await;
    opened(&client, &room_id).await;
    assert_eq!(transport.streams_open(), 1);
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;
}

#[tokio::test(start_paused = true)]
async fn a_worker_that_a_step_stops_with_its_stream_open_shows_it_closed() {
    // A sync of the application fails the room closed, and the next event of the stream leads
    // to a sync of the worker, which stops it.
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    fail_next_sync_closed(&transport, server.incarnation(&room_id).await);
    let failed_closed = ClientError::FailedClosed(room_id.clone());
    assert_eq!(
        client.sync(room_id.clone()).await,
        Err(failed_closed.clone())
    );
    transport.inject("event: lagged\ndata: true\n\n");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let stopped = worker(&client, &room_id).await;
    assert!(!stopped.running && !stopped.stream_open, "{stopped:?}");
    assert_eq!(stopped.last_error, Some(failed_closed));
    assert_eq!(transport.streams_open(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_stream_refused_for_its_token_every_time_renews_it_once_per_opening() {
    let Live {
        room_id,
        transport,
        tokens,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    transport.answer_events(|room_id| Some(Ok(refused(room_id, ErrorCode::Unauthorized))));
    transport.end_streams();
    tokio::time::sleep(Duration::from_secs(60)).await;
    // Each opening opens twice, with a token renewed in between, after a wait that doubles.
    let reopened = transport.stream_opens().len() - 1;
    assert!((4..100).contains(&reopened), "{reopened}");
    assert_eq!(reopened, tokens.calls() * 2);
    assert!(worker(&client, &room_id).await.running);
}

#[tokio::test(start_paused = true)]
async fn in_live_with_a_silent_open_stream_a_heartbeat_brings_what_another_writes() {
    // As through a proxy that holds the stream without closing it, for less than 45 s.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    transport.answer_next_events(|_| Ok(silent()));
    let config = ClientConfig {
        heartbeat_interval: Duration::from_secs(10),
        ..live()
    };
    let client = client_over(MemoryStorageEngine::new(), &transport, config, &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    let first = transport.stream_opens_at()[0];
    server.commit(&room_id, [1]).await;
    tokio::time::sleep_until(first + Duration::from_secs(21)).await;
    assert_eq!(head(&client, &room_id).await, 1);
    assert_eq!(transport.stream_opens().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_client_dropped_while_a_sync_of_its_worker_holds_it_ends_the_worker_at_the_next_event() {
    let Live {
        server: _server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let mut held = transport.hold_next_reply(Sync);
    transport.inject("event: lagged\ndata: true\n\n");
    held.replied().await;
    // Only the sync of the worker holds the client, long before the next heartbeat.
    drop(client);
    assert_eq!(transport.streams_open(), 1);
    transport.inject("event: lagged\ndata: true\n\n");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.streams_open(), 0);
    let _ = room_id;
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn a_worker_opens_one_stream_at_a_time() {
    // While an opening is in flight, the worker starts no other: at its start, and when the
    // stream ends.
    let Live {
        transport, client, ..
    } = started(TestServer::new(), live()).await;
    assert_eq!(transport.stream_opens().len(), 1);
    assert_eq!(transport.streams_open(), 1);
    transport.end_streams();
    opens(&transport, 2, Duration::from_secs(2)).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.stream_opens().len(), 2);
    assert_eq!(transport.streams_open(), 1);
    assert_eq!(Arc::strong_count(&client), 1);
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_a_heartbeat_replaces_shows_closed_during_the_ack_after_it() {
    // A silent stream, so that no event notices the discard: the heartbeat does, and the
    // acknowledgement after it, of a head that the server did not confirm, is held.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    transport.answer_next_events(|_| Ok(silent()));
    let config = ClientConfig {
        heartbeat_interval: Duration::from_secs(10),
        ..live()
    };
    let client = client_over(MemoryStorageEngine::new(), &transport, config, &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    after_a_heartbeat(&transport).await;
    discard_next_sync(&transport);
    client.sync(room_id.clone()).await.unwrap();
    server.commit(&room_id, [1]).await;
    transport.lose_next_reply(ClientMessageKind::Ack);
    assert!(client.sync(room_id.clone()).await.is_err());
    assert!(worker(&client, &room_id).await.stream_open);
    let mut held = transport.hold_next_reply(ClientMessageKind::Ack);
    held.replied().await;
    assert!(!worker(&client, &room_id).await.stream_open);
    held.release();
    opened(&client, &room_id).await;
    assert_eq!(transport.stream_opens().len(), 2);
}

/// The configuration of `person`, in `live`, with a heartbeat every [`INTERVAL`].
fn live_of(person: &str) -> ClientConfig {
    ClientConfig {
        freshness: Freshness::Live,
        heartbeat_interval: INTERVAL,
        ..ClientConfig::new(person)
    }
}

/// The text of `event` as the server writes it in the stream: an `ops` event, with its frame in
/// standard base64.
fn ops_text(event: &ServerMessage) -> String {
    let data = STANDARD.encode(encode_message(event).unwrap());
    format!("event: ops\ndata: {data}\n\n")
}

/// An `ops` event of `room_id` in `incarnation`, with `ops` up to `head`, checked with the version
/// `version` of the schema `tasks`.
fn ops_of(
    room_id: &RoomId,
    incarnation: u128,
    ops: Vec<SequencedOperation>,
    head: u64,
    version: u64,
) -> ServerMessage {
    ServerMessage::OpsEvent {
        room_id: room_id.clone(),
        room_incarnation: incarnation,
        head_seq: SequenceNumber::new(head),
        ops,
        schema_version: version,
        schema_id: SchemaId::new("tasks").unwrap(),
    }
}

/// `ops`, at the sequences after `head`, in order.
fn after(head: u64, ops: impl IntoIterator<Item = Operation>) -> Vec<SequencedOperation> {
    let sequenced = (head + 1..).zip(ops);
    let sequenced =
        sequenced.map(|(seq, op)| SequencedOperation::new(SequenceNumber::new(seq), op));
    sequenced.collect()
}

/// An insert of row `id` into `tasks`, with `title`.
fn insert_titled(id: i64, title: &str) -> Operation {
    let row = RowBuilder::new().set("id", id).set("title", title).build();
    tasks_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap()
}

/// The rows 1 to 4 of `tasks` in `room_id` of `storage`.
async fn rows(storage: &impl StorageEngine, room_id: &RoomId) -> Vec<Option<CompactRow>> {
    let mut rows = Vec::new();
    for id in 1..=4 {
        let row = storage.get(room_id, "tasks", &PrimaryKey::single(id)).await;
        rows.push(row.unwrap());
    }
    rows
}

/// The registration of `room_id` of `client` and whether it fails closed.
async fn registered(client: &Client, room_id: &RoomId) -> (bool, bool) {
    let status = client.status(room_id.clone()).await.unwrap();
    (status.registered, status.failed_closed)
}

#[tokio::test(start_paused = true)]
async fn what_several_clients_write_at_once_arrives_by_events_without_a_sync_as_a_sync_brings_it() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let mut clients = Vec::new();
    for person in ["ana", "bea", "cora"] {
        let (transport, storage) = (server.transport(), MemoryStorageEngine::new());
        let tokens = TestTokens::new();
        let client = client_over(storage.clone(), &transport, live_of(person), &tokens).await;
        open_room(&client, &room_id).await.unwrap();
        client.start_worker(room_id.clone()).await.unwrap();
        opened(&client, &room_id).await;
        clients.push((person, client, transport, storage));
    }
    let counts = |transport: &TestTransport| (syncs(transport), heartbeats(transport));
    let before: Vec<_> = clients.iter().map(|(.., t, _)| counts(t)).collect();

    // Each one inserts the rows 1 to 4 with its name, and updates them: an update sequenced
    // before an insert of its row finds none.
    let writes = clients.iter().flat_map(|(person, client, ..)| {
        let ops = (1..=4).flat_map(|id| [insert_titled(id, person), update_task(id)]);
        ops.map(|op| client.write(room_id.clone(), op))
    });
    for ended in join_all(writes).await {
        assert!(
            matches!(ended, Ok(()) | Err(ClientError::RowMissing)),
            "{ended:?}"
        );
    }
    for (_, client, ..) in &clients {
        reaches(client, &room_id, 24).await;
    }
    // No sync but for a first heartbeat in the middle of the writes, whose ack may report a head
    // that the replica did not reach yet.
    for ((person, _, transport, _), (syncs, beats)) in clients.iter().zip(before) {
        let (synced, beaten) = counts(transport);
        assert!(synced - syncs <= beaten - beats, "{person}");
    }

    // A client that syncs gets the same rows, with the same decisions.
    let (transport, storage, tokens) = (
        server.transport(),
        MemoryStorageEngine::new(),
        TestTokens::new(),
    );
    let synced = client_over(storage.clone(), &transport, live_of("dan"), &tokens).await;
    open_room(&synced, &room_id).await.unwrap();
    synced.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&synced, &room_id).await, 24);
    let expected = rows(&storage, &room_id).await;
    assert!(expected.iter().all(Option::is_some));
    for (person, _, _, storage) in &clients {
        assert_eq!(rows(storage, &room_id).await, expected, "{person}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_whose_sequence_its_event_applies_before_its_commit_ack_ends_with_its_decision() {
    // The server keeps its data in a directory that it removes once dropped.
    let Live {
        server: _server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let before = syncs(&transport);
    let mut held = transport.hold_next_reply(Commit);
    let writing = {
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        tokio::spawn(async move { client.write(room_id, update_task(1)).await })
    };
    held.replied().await;
    // The event of the commit reaches the replica before its ack, which waits.
    reaches(&client, &room_id, 1).await;
    held.release();
    assert_eq!(writing.await.unwrap(), Err(ClientError::RowMissing));
    assert_eq!(syncs(&transport), before);
    let status = client.status(room_id.clone()).await.unwrap();
    assert_eq!((status.writes_in_flight, status.retained), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn the_event_of_an_own_commit_whose_catch_up_applied_it_is_ignored() {
    let Live {
        server: _server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let before = syncs(&transport);
    let taken = transport.take_next_event("ops");
    client.write(room_id.clone(), insert_task(1)).await.unwrap();
    assert_eq!(head(&client, &room_id).await, 1);
    // The event comes once the catch-up of the commit applied its operation.
    transport.inject(&taken.text().await);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(syncs(&transport), before);
    assert_eq!(tasks(&storage, &room_id).await, [1]);
    let status = worker(&client, &room_id).await;
    assert!(
        status.stream_open && status.last_error.is_none(),
        "{status:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_event_and_a_sync_that_bring_the_same_sequences_apply_them_once() {
    let Live {
        server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let before = syncs(&transport);
    let taken = transport.take_next_event("ops");
    // The second event has a gap, and the sync it leads to waits for its reply, which brings
    // both; the first event comes meanwhile.
    let mut held = transport.hold_next_reply(Sync);
    server.commit(&room_id, [1, 2]).await;
    let first = taken.text().await;
    held.replied().await;
    transport.inject(&first);
    reaches(&client, &room_id, 1).await;
    held.release();
    reaches(&client, &room_id, 2).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(tasks(&storage, &room_id).await, [1, 2]);
    assert_eq!(syncs(&transport), before + 1);
    assert_eq!(worker(&client, &room_id).await.last_error, None);
}

#[tokio::test(start_paused = true)]
async fn after_a_lost_event_the_next_one_leaves_a_gap_that_a_sync_covers() {
    let Live {
        server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let before = syncs(&transport);
    let lost = transport.take_next_event("ops");
    server.commit(&room_id, [1]).await;
    let _ = lost.text().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(head(&client, &room_id).await, 0);
    server.commit(&room_id, [2]).await;
    reaches(&client, &room_id, 2).await;
    assert_eq!(tasks(&storage, &room_id).await, [1, 2]);
    assert_eq!(syncs(&transport), before + 1);
}

#[tokio::test(start_paused = true)]
async fn the_events_of_the_room_created_again_are_not_applied_and_the_replica_is_discarded() {
    let Live {
        server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    server.commit(&room_id, [1, 2]).await;
    reaches(&client, &room_id, 2).await;
    after_a_heartbeat(&transport).await;
    let (opened_in, incarnation) = (
        epoch(&client, &room_id).await,
        server.incarnation(&room_id).await,
    );
    let registrations = transport.registrations().len();
    // The stream opens again, with the same token, in the room created again, and the sync after
    // it waits for its reply.
    let mut held = transport.hold_next_reply(Sync);
    server.recreate_room(&room_id).await;
    held.replied().await;
    // Its operations follow the cursor of the replica from the third on.
    server.commit(&room_id, [7, 8, 9]).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(head(&client, &room_id).await, 2);
    assert_eq!(tasks(&storage, &room_id).await, [1, 2]);
    assert_eq!(registered(&client, &room_id).await, (false, false));

    // The registration with the cursor gets `RoomReset`: the replica is discarded, and registers
    // again without one.
    held.release();
    reaches(&client, &room_id, 3).await;
    assert_eq!(tasks(&storage, &room_id).await, [7, 8, 9]);
    assert_eq!(epoch(&client, &room_id).await, opened_in + 1);
    assert_eq!(
        transport.registrations()[registrations..],
        [(Some(2), Some(incarnation)), (None, None)]
    );
}

#[tokio::test(start_paused = true)]
async fn an_event_of_a_later_version_of_the_schema_registers_the_replica_which_takes_it_and_syncs()
{
    let dir = tempdir().unwrap();
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let client = Client::open(
        live(),
        MemoryStorageEngine::new(),
        FileMetadata::new(dir.path()),
        transport.clone(),
        TestTokens::new(),
    );
    let client = Arc::new(client.await.unwrap());
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    after_a_heartbeat(&transport).await;
    let (registrations, before) = (transport.registrations().len(), syncs(&transport));
    let incarnation = server.incarnation(&room_id).await;

    // The client learns of the new version from the event of a commit, without the event of the
    // reload: the operation is valid under both versions.
    let lost = transport.take_next_event("schema_reloaded");
    let schema_id = SchemaId::new("tasks").unwrap();
    let tag = || ColumnDef::new("tag", DataType::String).nullable(true);
    let registry = &server.state.schema_registry;
    let added = registry.add_column_with_constraints(&schema_id, "tasks", tag(), vec![], vec![]);
    added.unwrap();
    server
        .state
        .room_manager
        .reload_schema_for_rooms(&schema_id)
        .await;
    let _ = lost.text().await;
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;
    let registered = transport.registrations();
    assert_eq!(registered[registrations..], [(Some(0), Some(incarnation))]);
    assert_eq!(syncs(&transport), before + 1);
    let mut tagged = tasks_schema();
    tagged
        .add_column_with_constraints("tasks", tag(), vec![], vec![])
        .unwrap();
    let metadata = FileMetadata::new(dir.path()).room(&room_id).await;
    assert_eq!(metadata.unwrap().unwrap().schema, tagged);

    // An event of an earlier version of its own schema applies with the schema of the replica.
    let earlier = ops_of(&room_id, incarnation, after(1, [insert_task(2)]), 2, 1);
    transport.inject(&ops_text(&earlier));
    reaches(&client, &room_id, 2).await;
    assert_eq!(transport.registrations().len(), registrations + 1);
    assert_eq!(syncs(&transport), before + 1);
}

#[tokio::test(start_paused = true)]
async fn an_ops_event_of_another_room_without_operations_or_out_of_order_leads_to_a_sync_alone() {
    // Of another room, without operations, with a gap among them, short of its head, and past it.
    let Live {
        server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;
    after_a_heartbeat(&transport).await;
    let (registrations, before) = (transport.registrations().len(), syncs(&transport));
    let incarnation = server.incarnation(&room_id).await;
    let other = RoomId::new("room-b").unwrap();
    let mut apart = after(1, [insert_task(2), insert_task(3)]);
    apart[1] = SequencedOperation::new(SequenceNumber::new(4), insert_task(3));
    let events = [
        ops_of(&other, incarnation, after(1, [insert_task(2)]), 2, 1),
        ops_of(&room_id, incarnation, Vec::new(), 2, 1),
        ops_of(&room_id, incarnation, apart, 4, 1),
        ops_of(
            &room_id,
            incarnation,
            after(1, [insert_task(2), insert_task(3)]),
            4,
            1,
        ),
        ops_of(
            &room_id,
            incarnation,
            after(1, [insert_task(2), insert_task(3)]),
            2,
            1,
        ),
    ];
    for (count, event) in (1..).zip(&events) {
        transport.inject(&ops_text(event));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(syncs(&transport), before + count, "{event:?}");
        assert_eq!(tasks(&storage, &room_id).await, [1], "{event:?}");
    }
    assert_eq!(head(&client, &room_id).await, 1);
    assert_eq!(transport.registrations().len(), registrations);
    let status = worker(&client, &room_id).await;
    assert!(status.running && status.last_error.is_none(), "{status:?}");
}

/// An insert of row `id` into `tasks` of [`retyped`], whose `title` is an int: it does not
/// validate against [`tasks_schema`].
fn retyped_insert(id: i64) -> Operation {
    let row = RowBuilder::new().set("id", id).set("title", 7).build();
    retyped().to_operation_insert("tasks", &row, 1000).unwrap()
}

/// An insert of row `id` into a table `tags`, which [`tasks_schema`] does not have.
fn insert_tag(id: i64) -> Operation {
    let tags = TableSchema::builder("tags")
        .table_id(7)
        .primary_key("id", DataType::Int);
    let schema = Schema::builder().table(tags).build();
    let row = RowBuilder::new().set("id", id).build();
    schema.to_operation_insert("tags", &row, 1000).unwrap()
}

#[tokio::test(start_paused = true)]
async fn an_event_with_an_operation_that_does_not_validate_registers_and_syncs_and_never_fails_closed(
) {
    let Live {
        server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;
    after_a_heartbeat(&transport).await;
    let incarnation = server.incarnation(&room_id).await;
    // An operation that does not validate against the schema of the replica, one of a table that
    // the schema does not have, and one that does not validate after one that does.
    let events = [
        vec![retyped_insert(2)],
        vec![insert_tag(2)],
        vec![insert_task(2), retyped_insert(3)],
    ];
    for ops in events {
        let (registrations, before) = (transport.registrations().len(), syncs(&transport));
        let head = 1 + ops.len() as u64;
        let event = ops_of(&room_id, incarnation, after(1, ops), head, 1);
        transport.inject(&ops_text(&event));
        tokio::time::sleep(Duration::from_millis(10)).await;
        // The event leaves no head seen: the replica registers once and syncs, and it is the sync
        // that decides.
        let cursor = (Some(1), Some(incarnation));
        assert_eq!(
            transport.registrations()[registrations..],
            [cursor],
            "{event:?}"
        );
        assert_eq!(syncs(&transport), before + 1, "{event:?}");
        assert_eq!(tasks(&storage, &room_id).await, [1]);
        assert_eq!(registered(&client, &room_id).await, (true, false));
        let status = worker(&client, &room_id).await;
        assert!(status.running && status.last_error.is_none(), "{status:?}");
    }
    server.commit(&room_id, [2]).await;
    reaches(&client, &room_id, 2).await;
}

#[tokio::test(start_paused = true)]
async fn an_event_whose_operation_does_not_validate_leaves_no_head_seen_for_the_writes_after_it() {
    let Live {
        server,
        room_id,
        transport,
        storage,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;
    after_a_heartbeat(&transport).await;
    let incarnation = server.incarnation(&room_id).await;
    // Its operation at 2 does not validate, so the event is not the server's, and it reaches a
    // head of 10, past that of the server.
    let ops = [retyped_insert(2)]
        .into_iter()
        .chain((3..=10).map(insert_task));
    let event = ops_of(&room_id, incarnation, after(1, ops), 10, 1);
    // The sync that the event asks for waits for its reply: no head of the server comes before
    // the write.
    let mut held = transport.hold_next_reply(Sync);
    transport.inject(&ops_text(&event));
    held.replied().await;
    assert_eq!(registered(&client, &room_id).await, (true, false));
    let written = client.write(room_id.clone(), insert_task(4)).await;
    assert_eq!(commits(&transport).len(), 1);
    assert_eq!(tasks(&storage, &room_id).await, [1, 4]);
    held.release();
    // The write of sequence 2, with its acknowledgement, which the replica applied.
    assert_eq!(written, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn an_ops_event_of_a_stream_of_an_earlier_epoch_is_not_applied() {
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    discard_next_sync(&transport);
    client.sync(room_id.clone()).await.unwrap();
    // Discarded after `InvalidSequence`: the incarnation is the same.
    let incarnation = server.incarnation(&room_id).await;
    let event = ops_of(&room_id, incarnation, after(0, [insert_task(1)]), 1, 1);
    transport.inject(&ops_text(&event));
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(head(&client, &room_id).await, 0);
    assert_eq!(transport.stream_opens().len(), 2);
    assert_eq!(transport.streams_open(), 1);
}

/// A server with `room-a`, and a client of it in `live` over a [`TestEngine`], with the room open
/// and its worker started, once its event stream opened and a heartbeat went.
async fn engine_started() -> (
    TestServer,
    RoomId,
    TestTransport,
    TestEngine<MemoryStorageEngine>,
    Arc<Client>,
) {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let storage = TestEngine::new(MemoryStorageEngine::new());
    let client = client_over(storage.clone(), &transport, live(), &tokens).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    opened(&client, &room_id).await;
    after_a_heartbeat(&transport).await;
    (server, room_id, transport, storage, client)
}

#[tokio::test(start_paused = true)]
async fn while_an_event_is_applied_the_worker_reads_no_other_and_then_applies_them_in_order() {
    let (server, room_id, transport, storage, client) = engine_started().await;
    let before = syncs(&transport);
    let mut held = storage.hold_next("apply_batch");
    server.commit(&room_id, [1]).await;
    held.replied().await;
    for id in 2..=9 {
        server.commit(&room_id, [id]).await;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Nothing but the test and the application of the first event hold the client: no task
    // waits for each event.
    assert!(
        Arc::strong_count(&client) <= 2,
        "{}",
        Arc::strong_count(&client)
    );
    held.release();
    reaches(&client, &room_id, 9).await;
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(syncs(&transport), before);
}

#[tokio::test(start_paused = true)]
async fn stopping_the_worker_while_it_applies_an_event_leaves_the_batch_whole() {
    let (server, room_id, _transport, storage, client) = engine_started().await;
    let mut held = storage.hold_next("apply_batch");
    server.commit(&room_id, [1]).await;
    held.replied().await;
    // The stop does not wait for the application, and its last acknowledgement, which waits for
    // the applier, gives up after 5 s.
    let stopped = client.stop_worker(room_id.clone());
    let stopped = tokio::time::timeout(Duration::from_secs(30), stopped).await;
    stopped
        .expect("The stop waited for the application")
        .unwrap();
    assert!(client
        .status(room_id.clone())
        .await
        .unwrap()
        .worker
        .is_none());
    held.release();
    let start = Instant::now();
    while tasks(&storage, &room_id).await != [1] {
        assert!(start.elapsed() < Duration::from_secs(1), "Not applied");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn an_event_that_asks_for_a_sync_once_the_mode_turned_manual_leads_to_none() {
    let (server, room_id, transport, storage, client) = engine_started().await;
    let (registrations, before) = (transport.registrations().len(), syncs(&transport));
    // A sync holds the applier, and the change of the mode, which takes it too, waits behind it.
    let lost = transport.take_next_event("ops");
    server.commit(&room_id, [1]).await;
    let _ = lost.text().await;
    let mut held = storage.hold_next("apply_batch");
    transport.inject("event: lagged\ndata: true\n\n");
    held.replied().await;
    let setting = {
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        tokio::spawn(async move { client.set_freshness(room_id, Freshness::Manual).await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    // The event of 3, without that of 2: its application waits behind both, and finds a gap.
    let second = transport.take_next_event("ops");
    server.commit(&room_id, [2]).await;
    let _ = second.text().await;
    let third = transport.take_next_event("ops");
    server.commit(&room_id, [3]).await;
    transport.inject(&third.text().await);
    tokio::time::sleep(Duration::from_millis(10)).await;
    held.release();
    setting.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    // The sync that the gap asks for does not go in `Manual`.
    assert_eq!(head(&client, &room_id).await, 1);
    assert_eq!(registered(&client, &room_id).await, (true, false));
    assert_eq!(syncs(&transport), before + 1);
    assert_eq!(transport.registrations().len(), registrations);
    assert_eq!(worker(&client, &room_id).await.freshness, Freshness::Manual);
}

#[tokio::test(start_paused = true)]
async fn an_event_whose_batch_the_engine_refuses_registers_and_syncs_and_never_fails_closed() {
    let (server, room_id, transport, storage, client) = engine_started().await;
    let (registrations, before) = (transport.registrations().len(), syncs(&transport));
    let lost = transport.take_next_event("ops");
    server.commit(&room_id, [1]).await;
    let text = lost.text().await;
    // The event validates, and the engine refuses its batch, as it would one that does not.
    let invalid = StorageError::SchemaValidation(ValidationError::TableNotFound("tasks".into()));
    storage.fail_next("apply_batch", invalid);
    transport.inject(&text);
    reaches(&client, &room_id, 1).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(registered(&client, &room_id).await, (true, false));
    assert_eq!(transport.registrations().len(), registrations + 1);
    assert_eq!(syncs(&transport), before + 1);
    let status = worker(&client, &room_id).await;
    assert!(status.running && status.last_error.is_none(), "{status:?}");
}

#[tokio::test(start_paused = true)]
async fn an_error_of_the_storage_applying_an_event_shows_and_a_sync_brings_its_operations() {
    let (server, room_id, transport, storage, client) = engine_started().await;
    let before = syncs(&transport);
    let locked = StorageError::RoomLocked(room_id.clone());
    storage.fail_next("apply_batch", locked);
    let mut held = transport.hold_next_reply(Sync);
    server.commit(&room_id, [1]).await;
    held.replied().await;
    // The error of the application shows while the sync it leads to waits for its reply.
    let failed = worker(&client, &room_id).await;
    let storage_error = matches!(failed.last_error, Some(ClientError::Storage(_)));
    assert!(storage_error && failed.running, "{failed:?}");
    held.release();
    reaches(&client, &room_id, 1).await;
    assert_eq!(tasks(&storage, &room_id).await, [1]);
    assert_eq!(syncs(&transport), before + 1);
    assert_eq!(worker(&client, &room_id).await.last_error, None);
}

#[tokio::test(start_paused = true)]
async fn an_application_of_an_event_that_succeeds_clears_the_error_of_the_last_step() {
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    // A sync of the worker that the server refuses.
    answer_next(&transport, Sync, |correlation_id, room_id| {
        error_reply(correlation_id, room_id, ErrorCode::Unavailable)
    });
    transport.inject("event: lagged\ndata: true\n\n");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let failed = worker(&client, &room_id).await.last_error;
    assert!(
        matches!(failed, Some(ClientError::Network(_))),
        "{failed:?}"
    );
    let before = syncs(&transport);
    server.commit(&room_id, [1]).await;
    reaches(&client, &room_id, 1).await;
    assert_eq!(worker(&client, &room_id).await.last_error, None);
    assert_eq!(syncs(&transport), before);
}

#[tokio::test(start_paused = true)]
async fn an_ops_event_for_a_replica_that_fails_closed_leads_to_the_sync_that_stops_the_worker() {
    // A sync of the application fails the room closed: the next event, which follows the head,
    // is not applied, and the sync it leads to stops the worker.
    let Live {
        server,
        room_id,
        transport,
        client,
        ..
    } = started(TestServer::new(), live()).await;
    after_a_heartbeat(&transport).await;
    let incarnation = server.incarnation(&room_id).await;
    fail_next_sync_closed(&transport, incarnation);
    let failed_closed = ClientError::FailedClosed(room_id.clone());
    assert_eq!(
        client.sync(room_id.clone()).await,
        Err(failed_closed.clone())
    );
    let event = ops_of(&room_id, incarnation, after(0, [insert_task(1)]), 1, 1);
    transport.inject(&ops_text(&event));
    tokio::time::sleep(Duration::from_millis(10)).await;
    let stopped = worker(&client, &room_id).await;
    assert!(!stopped.running && !stopped.stream_open, "{stopped:?}");
    assert_eq!(stopped.last_error, Some(failed_closed));
    assert_eq!(head(&client, &room_id).await, 0);
}

#[tokio::test(start_paused = true)]
async fn an_ops_event_while_the_replica_is_empty_is_not_applied_and_marks_a_sync_more() {
    // The registration of the open got no response, and the sync after the stream opens
    // registers the replica, with its reply held: an event that comes meanwhile is not applied
    // to the empty replica, and marks a sync more.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, tokens) = (server.transport(), TestTokens::new());
    let client = client_over(MemoryStorageEngine::new(), &transport, live(), &tokens).await;
    transport.offline();
    assert!(open_room(&client, &room_id).await.is_err());
    transport.online();
    let mut held = transport.hold_next_reply(RegisterClient);
    client.start_worker(room_id.clone()).await.unwrap();
    held.replied().await;
    server.commit(&room_id, [1]).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    held.release();
    let start = Instant::now();
    while client
        .status(room_id.clone())
        .await
        .map(|status| status.head_seq.get())
        != Ok(1)
    {
        assert!(start.elapsed() < Duration::from_secs(1), "Not synced");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    // The sync after the opening, and the one that the event marked.
    assert!(syncs(&transport) >= 2, "{}", syncs(&transport));
}

#[tokio::test(start_paused = true)]
async fn a_client_dropped_while_an_application_holds_it_ends_the_worker_once_it_ends() {
    // A sync of the worker waits for its reply, and the application of an event for the engine:
    // only they hold the client, long before the next heartbeat.
    let (server, room_id, transport, storage, client) = engine_started().await;
    let incarnation = server.incarnation(&room_id).await;
    let mut sync = transport.hold_next_reply(Sync);
    transport.inject("event: lagged\ndata: true\n\n");
    sync.replied().await;
    let mut held = storage.hold_next("apply_batch");
    // The engine refuses the batch of the event once it goes on, so the application asks for a
    // sync.
    let invalid = StorageError::SchemaValidation(ValidationError::TableNotFound("tasks".into()));
    storage.fail_next("apply_batch", invalid);
    let event = ops_of(&room_id, incarnation, after(0, [insert_task(1)]), 1, 1);
    transport.inject(&ops_text(&event));
    held.replied().await;
    drop(client);
    assert_eq!(transport.streams_open(), 1);
    // The application asks for a sync: the worker finds the client dropped, and ends.
    held.release();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.streams_open(), 0);
    drop(sync);
}

#[tokio::test(start_paused = true)]
async fn a_write_that_waits_for_the_applier_when_an_event_comes_saw_the_head_of_the_event() {
    let (server, room_id, transport, storage, client) = engine_started().await;
    // A sync of the worker holds the applier, with its batch in the engine.
    let lost = transport.take_next_event("ops");
    server.commit(&room_id, [1]).await;
    let _ = lost.text().await;
    let mut held = storage.hold_next("apply_batch");
    transport.inject("event: lagged\ndata: true\n\n");
    held.replied().await;
    // A write waits for the applier, with the reply to its commit held.
    let mut commit = transport.hold_next_reply(Commit);
    let writing = {
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        tokio::spawn(async move { client.write(room_id, insert_task(9)).await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    // An event whose operation the replica rejects comes while the write waits: its head is seen
    // before its operations reach the applier, so the write, which enters before them, saw it,
    // and retains nothing of it.
    server.commit_ops(&room_id, [update_task(5)]).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    held.release();
    commit.replied().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let status = client.status(room_id.clone()).await.unwrap();
    assert!(status.head_seq.get() >= 2, "{status:?}");
    assert_eq!((status.writes_in_flight, status.retained), (1, 0));
    commit.release();
    assert_eq!(writing.await.unwrap(), Ok(()));
}
