//! The worker of a room against the server in process: its heartbeats, the snapshots it uploads
//! and downloads in a task of their own, the syncs of its modes, the retries of its steps, what
//! stops it, and its status.
//! With the paused clock, but where steps fail one after another: there, with the real clock,
//! short delays and a limit of its own, since a worker that retried without waiting would not let
//! a paused clock go on.

mod common;

use bytes::Bytes;
use common::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use zemdb_client::{
    Client, ClientConfig, ClientError, FileMetadata, Freshness, MemoryMetadata, RoomStatus,
    WorkerStatus,
};
use zemdb_core::ClientMessageKind::{
    Ack, Heartbeat, RegisterClient, RequestSnapshotChunk, Sync, UploadSnapshotChunk,
};
use zemdb_core::*;
use zemdb_server::RoomCommand;
use zemdb_storage::{MemoryStorageEngine, StorageEngine, StorageError};

/// The configuration of ana, with a heartbeat every `interval`.
fn every(interval: Duration) -> ClientConfig {
    ClientConfig {
        heartbeat_interval: interval,
        ..ClientConfig::new("ana")
    }
}

/// A client with `config` over `storage` and a memory store of metadata, through `transport`.
async fn client_over(
    storage: impl StorageEngine + 'static,
    transport: &TestTransport,
    config: ClientConfig,
) -> Arc<Client> {
    let metadata = MemoryMetadata::default();
    let tokens = TestTokens::new();
    let client = Client::open(config, storage, metadata, transport.clone(), tokens);
    Arc::new(client.await.unwrap())
}

/// A client with `config` over a memory engine, through `transport`.
async fn client_with(transport: &TestTransport, config: ClientConfig) -> Arc<Client> {
    client_over(MemoryStorageEngine::new(), transport, config).await
}

/// When each message of `kind` for `room_id` went through `transport`, in order.
fn sent(transport: &TestTransport, kind: ClientMessageKind, room_id: &RoomId) -> Vec<Instant> {
    let sent = transport.sent_at().into_iter();
    let of = |(message, _): &(ClientMessage, Instant)| {
        message.kind() == kind && request_of(message).1 == *room_id
    };
    sent.filter(of).map(|(_, at)| at).collect()
}

/// When each heartbeat of `room_id` went through `transport`, in order.
fn heartbeats(transport: &TestTransport, room_id: &RoomId) -> Vec<Instant> {
    sent(transport, Heartbeat, room_id)
}

/// The time between each two of `instants`.
fn gaps(instants: &[Instant]) -> Vec<Duration> {
    instants.windows(2).map(|pair| pair[1] - pair[0]).collect()
}

/// The correlation id and the room of `message`, a request of [`request`] or a registration.
fn request_of(message: &ClientMessage) -> (CorrelationId, RoomId) {
    match message {
        ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            ..
        } => (*correlation_id, room_id.clone()),
        message => request(message),
    }
}

/// The worker of `room_id` of `client`, as its status gives it.
async fn worker(client: &Client, room_id: &RoomId) -> Option<WorkerStatus> {
    client.status(room_id.clone()).await.unwrap().worker
}

/// How many clients of `room_id` the server takes as bootstrapping (behind its log) and as
/// connected, both with a lease that did not end.
async fn leases(server: &TestServer, room_id: &RoomId) -> (usize, usize) {
    let metrics = server
        .state
        .room_manager
        .ask(room_id, |reply| RoomCommand::GetMetrics { reply });
    let metrics = metrics.await.unwrap().unwrap();
    (metrics.bootstrapping_clients, metrics.connected_clients)
}

/// `future`, within 10 s: a worker that did not stop would hold a stop or a close forever.
async fn soon<T>(future: impl std::future::Future<Output = T>) -> T {
    let soon = tokio::time::timeout(Duration::from_secs(10), future).await;
    soon.expect("It did not end within 10 s")
}

fn network<T: std::fmt::Debug>(result: Result<T, ClientError>) {
    assert!(matches!(result, Err(ClientError::Network(_))), "{result:?}");
}

#[tokio::test(start_paused = true)]
async fn a_worker_sends_a_heartbeat_every_interval_the_first_at_random_within_the_first() {
    let server = TestServer::new();
    let interval = Duration::from_secs(10);
    let transport = server.transport();
    let client = client_with(&transport, every(interval)).await;
    let mut rooms = Vec::new();
    for room in 0..16 {
        let room_id = server.create_room(&format!("room-{room}")).await;
        open_room(&client, &room_id).await.unwrap();
        rooms.push(room_id);
    }
    // Without a worker, a room sends no heartbeat.
    tokio::time::sleep(interval * 3).await;
    assert!(heartbeats(&transport, &rooms[0]).is_empty());

    // Each heartbeat takes 2 s to reach the server: the interval counts from the start of the
    // one before.
    transport.slow(Duration::from_secs(2));
    let start = Instant::now();
    for room_id in &rooms {
        client.start_worker(room_id.clone()).await.unwrap();
    }
    tokio::time::sleep(interval * 5 + interval / 2).await;
    let mut firsts = Vec::new();
    for room_id in &rooms {
        let sent = heartbeats(&transport, room_id);
        assert!(sent.len() >= 5, "{room_id}: {sent:?}");
        assert!(sent[0] - start <= interval, "{room_id}: {sent:?}");
        let every = gaps(&sent).into_iter().all(|gap| gap == interval);
        assert!(every, "{room_id}: {sent:?}");
        firsts.push(sent[0]);
    }
    // At random over the interval: rooms whose workers start together do not send their
    // heartbeats together (with 16 rooms, a quarter of the interval fails once in 10^8).
    let spread = *firsts.iter().max().unwrap() - *firsts.iter().min().unwrap();
    assert!(spread > interval / 4, "{firsts:?}");
}

/// A client of a room over a memory engine.
struct Member {
    transport: TestTransport,
    storage: MemoryStorageEngine,
    client: Arc<Client>,
}

/// A client of `server` that opens `room_id`.
async fn member(server: &TestServer, room_id: &RoomId) -> Member {
    let (transport, storage) = (server.transport(), MemoryStorageEngine::new());
    let client = client_over(storage.clone(), &transport, ClientConfig::new("ana")).await;
    open_room(&client, room_id).await.unwrap();
    Member {
        transport,
        storage,
        client,
    }
}

/// A server that prunes, with `room-a` holding the tasks 1 to 9 and its log pruned before 6, and
/// two members with the default configuration: one at 9, and one at 0, behind the log, for which
/// the server designates the first to upload a snapshot.
async fn designated() -> (TestServer, RoomId, Member, Member) {
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let designee = member(&server, &room_id).await;
    server.commit(&room_id, 1..=9).await;
    designee.client.sync(room_id.clone()).await.unwrap();
    server.prune(&room_id, 6).await;
    let lagging = member(&server, &room_id).await;
    (server, room_id, designee, lagging)
}

#[tokio::test(start_paused = true)]
async fn the_heartbeats_go_on_during_a_long_download_and_the_snapshot_moves_without_a_step() {
    let (server, room_id, designee, lagging) = designated().await;
    let mut held = lagging.transport.hold_next_reply(RequestSnapshotChunk);
    designee.client.start_worker(room_id.clone()).await.unwrap();
    lagging.client.start_worker(room_id.clone()).await.unwrap();
    // Within two intervals, the designee uploads its snapshot, and a heartbeat of the client
    // behind the log announces it: the worker downloads it, with its first chunk held.
    tokio::time::sleep(Duration::from_secs(61)).await;
    held.replied().await;
    let active = server.state.snapshot_relay.active_snapshot_seq(&room_id);
    assert_eq!(active, Some(SequenceNumber::new(9)));

    // Three leases of the server (90 s) go by while the download lasts: the heartbeats go on,
    // one per interval, and the server still takes the client as behind its log, and the
    // designee as connected (the client that wrote the tasks lost its lease).
    let before = heartbeats(&lagging.transport, &room_id).len();
    tokio::time::sleep(Duration::from_secs(270)).await;
    let sent = heartbeats(&lagging.transport, &room_id);
    assert!(sent.len() - before >= 8, "{before} {sent:?}");
    let every = gaps(&sent)
        .into_iter()
        .all(|gap| gap == Duration::from_secs(30));
    assert!(every, "{sent:?}");
    assert_eq!(leases(&server, &room_id).await, (1, 1));
    // One download, which the heartbeats do not start again; and more than ten heartbeats of a
    // replica that waits for a snapshot announced do not register it again.
    assert_eq!(sent_of(&lagging.transport, RequestSnapshotChunk).len(), 1);
    assert!(sent.len() > 10, "{sent:?}");
    assert_eq!(lagging.transport.registrations().len(), 1);

    held.release();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let status = lagging.client.status(room_id.clone()).await.unwrap();
    assert_eq!(status.head_seq, SequenceNumber::new(9));
    let all = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    assert_eq!(tasks(&lagging.storage, &room_id).await, all);
    assert!(sent_of(&lagging.transport, Sync).is_empty());
}

/// When each message of `kind` went through `transport`, in order.
fn sent_of(transport: &TestTransport, kind: ClientMessageKind) -> Vec<Instant> {
    let sent = transport.sent_at().into_iter();
    let of = |(message, _): &(ClientMessage, Instant)| message.kind() == kind;
    sent.filter(of).map(|(_, at)| at).collect()
}

#[tokio::test(start_paused = true)]
async fn a_sync_during_the_download_of_the_worker_waits_for_it_and_downloads_nothing() {
    let (_server, room_id, designee, lagging) = designated().await;
    let mut held = lagging.transport.hold_next_reply(RequestSnapshotChunk);
    designee.client.start_worker(room_id.clone()).await.unwrap();
    lagging.client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(61)).await;
    held.replied().await;
    // The sync waits for the transfer of the worker, finds the snapshot installed, and syncs
    // from it.
    let client = Arc::clone(&lagging.client);
    let sync = tokio::spawn({
        let room_id = room_id.clone();
        async move { client.sync(room_id).await }
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!sync.is_finished());
    held.release();
    sync.await.unwrap().unwrap();
    assert_eq!(sent_of(&lagging.transport, RequestSnapshotChunk).len(), 1);
    let status = lagging.client.status(room_id.clone()).await.unwrap();
    assert_eq!(status.head_seq, SequenceNumber::new(9));
}

#[tokio::test]
async fn a_room_opened_without_network_registers_with_the_first_heartbeat_that_gets_a_reply() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let interval = Duration::from_millis(400);
    let config = ClientConfig {
        heartbeat_interval: interval,
        retry_base: Duration::from_millis(20),
        retry_max: Duration::from_millis(160),
        ..ClientConfig::new("ana")
    };
    let client = client_with(&transport, config).await;
    transport.offline();
    // Without metadata, the room stays empty, without status: that does not stop its worker.
    network(open_room(&client, &room_id).await);
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let closed = Err(ClientError::RoomClosed(room_id.clone()));
    assert_eq!(client.status(room_id.clone()).await, closed);
    // The worker tries to register again after a wait that grows, up to its longest: some 25
    // times in 2 s with these delays, against some 180 if the wait did not grow.
    let tried = sent(&transport, RegisterClient, &room_id);
    assert!((4..=50).contains(&tried.len()), "{tried:?}");
    let longest = gaps(&tried[1..]).into_iter().max().unwrap();
    assert!(longest > Duration::from_millis(40), "{tried:?}");

    // The first heartbeat with a reply registers it, and the next ones go every interval.
    transport.online();
    tokio::time::sleep(Duration::from_millis(1400)).await;
    let status = client.status(room_id.clone()).await.unwrap();
    assert!(status.registered, "{status:?}");
    let running = WorkerStatus {
        freshness: Freshness::Periodic,
        running: true,
        connected: Some(true),
        last_error: None,
    };
    assert_eq!(status.worker, Some(running.clone()));
    let sent = heartbeats(&transport, &room_id);
    assert!(sent.len() >= 3, "{sent:?}");
    let every = gaps(&sent).into_iter().all(|gap| gap > interval * 5 / 8);
    assert!(every, "{sent:?}");

    // Without network again, the worker goes on, with the error of its last step.
    transport.offline();
    tokio::time::sleep(interval * 2).await;
    let offline = worker(&client, &room_id).await.unwrap();
    assert!(
        offline.running && offline.connected == Some(false),
        "{offline:?}"
    );
    assert!(
        matches!(offline.last_error, Some(ClientError::Network(_))),
        "{offline:?}"
    );
    let offline = sent_of(&transport, Heartbeat).len();
    transport.online();
    tokio::time::sleep(interval * 2).await;
    assert_eq!(worker(&client, &room_id).await, Some(running));
    assert!(sent_of(&transport, Heartbeat).len() > offline);
}

/// A schema `other` of `tasks` whose `title` is an int: it does not evolve from
/// [`tasks_schema`].
fn retyped() -> Schema {
    let retyped = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::Int);
    Schema::builder().table(retyped).build()
}

/// A `Registered` of `room_incarnation` for the registration `message`, at 0, with [`retyped`].
fn registered_retyped(message: &ClientMessage, room_incarnation: u128) -> Vec<u8> {
    let (correlation_id, room_id) = request_of(message);
    encode_message(&ServerMessage::Registered {
        correlation_id,
        room_id,
        head_seq: SequenceNumber::new(0),
        tail_seq: SequenceNumber::new(1),
        schema_id: SchemaId::new("other").unwrap(),
        schema: retyped(),
        active_snapshot_seq: None,
        room_incarnation,
    })
    .unwrap()
}

/// Makes the next heartbeat through `transport` fail its room closed: its ack announces another
/// schema, and the registration that follows gets one that does not evolve from the replica's.
fn fail_next_heartbeat_closed(transport: &TestTransport, room_incarnation: u128) {
    let mut heartbeat = true;
    transport.answer(move |message| match message {
        ClientMessage::Heartbeat { .. } if std::mem::take(&mut heartbeat) => {
            let (correlation_id, room_id) = request(message);
            let ack = ServerMessage::HeartbeatAck {
                correlation_id,
                room_id,
                current_head_seq: SequenceNumber::new(0),
                snapshot_wanted: false,
                active_snapshot_seq: None,
                schema_version: 1,
                schema_id: SchemaId::new("other").unwrap(),
            };
            Some(Ok(encode_message(&ack).unwrap()))
        }
        ClientMessage::RegisterClient { .. } => {
            Some(Ok(registered_retyped(message, room_incarnation)))
        }
        _ => None,
    });
}

#[tokio::test(start_paused = true)]
async fn the_worker_tries_a_heartbeat_again_after_a_delay_that_doubles_and_starts_over() {
    // Without a heartbeat that fails again and again, so that a worker that did not wait would
    // not keep the clock from going on.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let interval = Duration::from_secs(50);
    let (base, max) = (Duration::from_secs(1), Duration::from_secs(8));
    let config = ClientConfig {
        retry_base: base,
        retry_max: max,
        // Not those of the writes.
        backoff_base: base * 20,
        backoff_max: max * 20,
        ..every(interval)
    };
    let client = client_with(&transport, config).await;
    open_room(&client, &room_id).await.unwrap();
    // How many heartbeats fail from now on.
    let failing = Arc::new(Mutex::new(0));
    let fails = Arc::clone(&failing);
    transport.answer(move |message| {
        if message.kind() != Heartbeat {
            return None;
        }
        let mut left = fails.lock().unwrap();
        match *left {
            0 => None,
            _ => {
                *left -= 1;
                let (correlation_id, room_id) = request(message);
                let unavailable = error_reply(correlation_id, &room_id, ErrorCode::Unavailable);
                Some(Ok(unavailable))
            }
        }
    });

    // Twelve heartbeats fail: each is sent again within a delay that doubles from the base up to
    // the longest, and the first that succeeds goes back to the interval.
    *failing.lock().unwrap() = 12;
    client.start_worker(room_id.clone()).await.unwrap();
    // The first within an interval, the retries within 79 s, and two more intervals.
    tokio::time::sleep(interval * 4).await;
    let sent = heartbeats(&transport, &room_id);
    let retries = &gaps(&sent)[..12];
    let ceilings = [1, 2, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8].map(Duration::from_secs);
    let within = retries
        .iter()
        .zip(ceilings)
        .all(|(gap, ceiling)| *gap <= ceiling);
    assert!(within, "{retries:?}");
    assert!(retries.iter().max().unwrap() > &base, "{retries:?}");
    assert_eq!(gaps(&sent)[12], interval, "{sent:?}");

    // After a heartbeat that succeeded, the delay starts again from the base.
    for _ in 0..4 {
        let before = heartbeats(&transport, &room_id).len();
        *failing.lock().unwrap() = 1;
        tokio::time::sleep(interval + base * 2).await;
        let sent = heartbeats(&transport, &room_id);
        let retried = sent[before + 1] - sent[before];
        assert!(retried <= base, "{sent:?}");
    }
    assert!(worker(&client, &room_id).await.unwrap().running);
}

#[tokio::test(start_paused = true)]
async fn a_transfer_that_fails_the_room_closed_stops_the_worker_at_once() {
    // A replica behind the log downloads a snapshot that does not install with its schema, and
    // registers again for it: the registration gets a schema that does not evolve.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let ops = server.commit(&room_id, 1..=9).await;
    server.prune(&room_id, 6).await;
    server
        .upload(
            &room_id,
            7,
            snapshot(&room_id, tasks_schema(), &ops[..7]).await,
        )
        .await;
    let transport = server.transport();
    let interval = Duration::from_secs(10);
    let client = client_with(&transport, every(interval)).await;
    open_room(&client, &room_id).await.unwrap();
    let incarnation = server.incarnation(&room_id).await;
    transport.answer(move |message| match message {
        ClientMessage::RequestSnapshotChunk {
            correlation_id,
            room_id,
            ..
        } => {
            let data = Bytes::from_static(b"not a snapshot");
            let chunk = ServerMessage::SnapshotChunk {
                correlation_id: *correlation_id,
                room_id: room_id.clone(),
                snapshot_head_seq: SequenceNumber::new(7),
                chunk_index: 0,
                total_chunks: 1,
                total_bytes: data.len() as u64,
                snapshot_hash: ServerMessage::compute_snapshot_hash(&data),
                data,
            };
            Some(Ok(encode_message(&chunk).unwrap()))
        }
        ClientMessage::RegisterClient { .. } => Some(Ok(registered_retyped(message, incarnation))),
        _ => None,
    });
    client.start_worker(room_id.clone()).await.unwrap();
    let mut waited = Duration::ZERO;
    while !client.status(room_id.clone()).await.unwrap().failed_closed {
        assert!(waited < interval * 3, "The room did not fail closed");
        tokio::time::sleep(Duration::from_millis(100)).await;
        waited += Duration::from_millis(100);
    }
    // Long before the next heartbeat.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let stopped = worker(&client, &room_id).await.unwrap();
    assert!(!stopped.running, "{stopped:?}");
    let failed_closed = Some(ClientError::FailedClosed(room_id.clone()));
    assert_eq!(stopped.last_error, failed_closed);
    assert_eq!(heartbeats(&transport, &room_id).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn stopping_the_worker_waits_until_its_heartbeat_and_its_transfer_ended() {
    // Whether the worker stopped with its heartbeat or its transfer in flight shows in the
    // references to the client: a task of the worker holds one while it runs a step. A limit of
    // its own, since a heartbeat or a transfer that went on would hold the stop.
    let limit = Duration::from_secs(5);
    let (_server, room_id, designee, lagging) = designated().await;
    // The designee uploads within the 60 s of its designation.
    designee.client.start_worker(room_id.clone()).await.unwrap();
    let mut held = lagging.transport.hold_next_reply(Heartbeat);
    lagging.client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(31)).await;
    held.replied().await;
    assert_eq!(Arc::strong_count(&lagging.client), 2);
    let stopped = tokio::time::timeout(limit, lagging.client.stop_worker(room_id.clone()));
    assert_eq!(stopped.await, Ok(Ok(())));
    assert_eq!(Arc::strong_count(&lagging.client), 1);
    drop(held);

    // With a download in flight: `stop_worker`, `close_room` and `shutdown`.
    for stop in 0..3 {
        let mut held = lagging.transport.hold_next_reply(RequestSnapshotChunk);
        lagging.client.start_worker(room_id.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(61)).await;
        held.replied().await;
        assert_eq!(Arc::strong_count(&lagging.client), 2);
        let (client, room) = (&lagging.client, room_id.clone());
        let stopped = match stop {
            0 => tokio::time::timeout(limit, client.stop_worker(room)).await,
            1 => tokio::time::timeout(limit, client.close_room(room)).await,
            _ => tokio::time::timeout(limit, client.shutdown()).await,
        };
        assert_eq!(stopped, Ok(Ok(())), "{stop}");
        assert_eq!(Arc::strong_count(&lagging.client), 1, "{stop}");
        drop(held);
        if stop == 1 {
            open_room(&lagging.client, &room_id).await.unwrap();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_room_failing_closed_stops_its_worker() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let interval = Duration::from_secs(10);
    let client = client_with(&transport, every(interval)).await;
    open_room(&client, &room_id).await.unwrap();
    fail_next_heartbeat_closed(&transport, server.incarnation(&room_id).await);
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(interval * 3).await;
    let status = client.status(room_id.clone()).await.unwrap();
    assert!(status.failed_closed, "{status:?}");
    let stopped = WorkerStatus {
        freshness: Freshness::Periodic,
        running: false,
        connected: Some(true),
        last_error: Some(ClientError::FailedClosed(room_id.clone())),
    };
    assert_eq!(status.worker, Some(stopped));
    assert_eq!(heartbeats(&transport, &room_id).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_larger_than_the_limit_and_an_error_of_the_storage_do_not_stop_the_worker() {
    // A replica behind the log, whose active snapshot is larger than the limit of the client: each
    // heartbeat asks for it again by a first chunk of 64 KiB, and the worker goes on.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let ops = server.commit(&room_id, 1..=9).await;
    server.prune(&room_id, 6).await;
    let snapshot = snapshot(&room_id, tasks_schema(), &ops[..7]).await;
    let len = snapshot.len() as u64;
    server.upload(&room_id, 7, snapshot).await;
    let transport = server.transport();
    let interval = Duration::from_secs(10);
    let config = ClientConfig {
        max_snapshot_bytes: len - 1,
        ..every(interval)
    };
    let storage = TestEngine::new(MemoryStorageEngine::new());
    let client = client_over(storage.clone(), &transport, config).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(interval * 3).await;
    let sizes = transport
        .sent()
        .into_iter()
        .filter_map(|message| match message {
            ClientMessage::RequestSnapshotChunk { chunk_size, .. } => Some(chunk_size),
            _ => None,
        });
    // One request of a chunk per heartbeat, wherever the heartbeats fall in the 30 s.
    let sizes: Vec<_> = sizes.collect();
    assert!((3..=4).contains(&sizes.len()), "{sizes:?}");
    let probes = sizes[1..].iter().all(|size| *size == 64 << 10);
    assert!(sizes[0] == 4 << 20 && probes, "{sizes:?}");
    assert_eq!(sizes.len(), heartbeats(&transport, &room_id).len());
    let oversized = worker(&client, &room_id).await.unwrap();
    assert!(oversized.running, "{oversized:?}");
    assert!(
        matches!(oversized.last_error, Some(ClientError::Config(_))),
        "{oversized:?}"
    );

    // An error of the storage with the room open is tried again after a wait.
    let before = heartbeats(&transport, &room_id).len();
    let io = || StorageError::Io(std::io::Error::other("Fabricated"));
    storage.fail_next("get_head_seq", io());
    tokio::time::sleep(interval * 3).await;
    assert!(heartbeats(&transport, &room_id).len() >= before + 2);
    assert!(worker(&client, &room_id).await.unwrap().running);
}

#[tokio::test(start_paused = true)]
async fn a_room_that_a_step_takes_out_of_the_client_stops_its_worker() {
    // A registration gets `RoomReset`, and its discard fails deleting the data: the room is out
    // of the client, and the next step of the worker stops it. Opening the room again does not
    // start it again.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let interval = Duration::from_secs(10);
    let storage = TestEngine::new(MemoryStorageEngine::new());
    let client = client_over(storage.clone(), &transport, every(interval)).await;
    open_room(&client, &room_id).await.unwrap();
    let (mut heartbeat, mut registration) = (true, true);
    transport.answer(move |message| {
        let (correlation_id, room_id) = request_of(message);
        let code = match message.kind() {
            Heartbeat if std::mem::take(&mut heartbeat) => ErrorCode::ClientNotRegistered,
            RegisterClient if std::mem::take(&mut registration) => ErrorCode::RoomReset,
            _ => return None,
        };
        Some(Ok(error_reply(correlation_id, &room_id, code)))
    });
    let io = StorageError::Io(std::io::Error::other("Fabricated"));
    storage.fail_next("delete_room", io);
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(interval * 2).await;
    let closed = Err(ClientError::RoomClosed(room_id.clone()));
    assert_eq!(client.status(room_id.clone()).await, closed);

    open_room(&client, &room_id).await.unwrap();
    let before = heartbeats(&transport, &room_id).len();
    tokio::time::sleep(Duration::from_secs(70)).await;
    assert_eq!(heartbeats(&transport, &room_id).len(), before);
    let stopped = WorkerStatus {
        freshness: Freshness::Periodic,
        running: false,
        connected: Some(true),
        last_error: Some(ClientError::RoomClosed(room_id.clone())),
    };
    assert_eq!(worker(&client, &room_id).await, Some(stopped));
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(interval * 2).await;
    assert!(heartbeats(&transport, &room_id).len() > before);
}

#[tokio::test(start_paused = true)]
async fn stopping_the_worker_or_closing_the_room_stops_it_and_that_of_a_dropped_client_ends() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let interval = Duration::from_secs(10);
    let client = client_with(&transport, every(interval)).await;
    open_room(&client, &room_id).await.unwrap();
    let beats = || heartbeats(&transport, &room_id).len();

    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(interval * 2).await;
    assert!(beats() >= 1);
    soon(client.stop_worker(room_id.clone())).await.unwrap();
    assert_eq!(worker(&client, &room_id).await, None);
    let stopped = beats();
    tokio::time::sleep(interval * 6).await;
    assert_eq!(beats(), stopped);
    // Stopping a room without a worker does nothing.
    soon(client.stop_worker(room_id.clone())).await.unwrap();

    // Closed and opened again, the room has no worker until it starts one.
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(interval * 2).await;
    assert!(beats() > stopped);
    soon(client.close_room(room_id.clone())).await.unwrap();
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(worker(&client, &room_id).await, None);
    let closed = beats();
    tokio::time::sleep(interval * 6).await;
    assert_eq!(beats(), closed);

    // The worker of a client that the application drops ends with it.
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(interval * 2).await;
    let dropped = transport.sent().len();
    assert!(beats() > closed);
    drop(client);
    tokio::time::sleep(interval * 6).await;
    assert_eq!(transport.sent().len(), dropped);
}

#[tokio::test(start_paused = true)]
async fn the_worker_of_a_client_dropped_with_a_download_in_flight_ends() {
    // The task of the download holds the client: once nothing else does, the worker stops, and
    // cuts the download.
    let (_server, room_id, designee, lagging) = designated().await;
    let held = lagging.transport.hold_next_reply(RequestSnapshotChunk);
    designee.client.start_worker(room_id.clone()).await.unwrap();
    lagging.client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(61)).await;
    let Member {
        transport, client, ..
    } = lagging;
    let mut held = held;
    held.replied().await;
    let dropped = Arc::downgrade(&client);
    drop(client);
    let before = heartbeats(&transport, &room_id).len();
    tokio::time::sleep(Duration::from_secs(90)).await;
    assert_eq!(heartbeats(&transport, &room_id).len(), before);
    assert!(dropped.upgrade().is_none());
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn the_workers_of_a_dropped_client_do_not_keep_each_other_alive() {
    // Two rooms whose heartbeats take 31 s each, so that one is always in flight: each holds the
    // client while it lasts, and no worker takes the other's for the application's.
    let server = TestServer::new();
    let transport = server.transport();
    let client = client_with(&transport, ClientConfig::new("ana")).await;
    for room in ["room-a", "room-b"] {
        let room_id = server.create_room(room).await;
        open_room(&client, &room_id).await.unwrap();
        client.start_worker(room_id).await.unwrap();
    }
    transport.slow(Duration::from_secs(31));
    tokio::time::sleep(Duration::from_secs(120)).await;
    let dropped = Arc::downgrade(&client);
    drop(client);
    tokio::time::sleep(Duration::from_secs(70)).await;
    let before = transport.sent().len();
    tokio::time::sleep(Duration::from_secs(600)).await;
    assert_eq!(transport.sent().len(), before);
    assert!(dropped.upgrade().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_worker_takes_a_held_client_for_dropped_with_many_threads() {
    // With the real clock: 20 rooms whose heartbeats go every 10 ms on several threads, while
    // the application takes and drops references to the client all the time.
    let server = TestServer::new();
    let transport = server.transport();
    let config = ClientConfig {
        heartbeat_interval: Duration::from_millis(10),
        retry_base: Duration::from_millis(5),
        retry_max: Duration::from_millis(5),
        ..ClientConfig::new("ana")
    };
    let client = client_with(&transport, config).await;
    let mut rooms = Vec::new();
    for room in 0..20 {
        let room_id = server.create_room(&format!("room-{room}")).await;
        open_room(&client, &room_id).await.unwrap();
        client.start_worker(room_id.clone()).await.unwrap();
        rooms.push(room_id);
    }
    let churn = tokio::spawn({
        let client = Arc::clone(&client);
        async move {
            for _ in 0..200_000 {
                let clone = Arc::clone(&client);
                tokio::task::yield_now().await;
                drop(clone);
            }
        }
    });
    tokio::time::sleep(Duration::from_secs(4)).await;
    churn.await.unwrap();
    for room_id in &rooms {
        let worker = worker(&client, room_id).await.unwrap();
        assert!(worker.running, "{room_id}: {worker:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_client_dropped_during_a_heartbeat_of_its_worker_lets_its_directory_go_after_it() {
    // Dropped without `shutdown`, the client holds the lock of its metadata while a step of its
    // worker is in flight, and lets it go once the step ends.
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let open = || {
        Client::open(
            ClientConfig::new("ana"),
            MemoryStorageEngine::new(),
            FileMetadata::new(dir.path()),
            transport.clone(),
            TestTokens::new(),
        )
    };
    let client = Arc::new(open().await.unwrap());
    open_room(&client, &room_id).await.unwrap();
    let mut held = transport.hold_next_reply(Heartbeat);
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(31)).await;
    held.replied().await;
    drop(client);
    let reopened = open().await;
    assert!(
        matches!(reopened, Err(ClientError::Storage(_))),
        "{reopened:?}"
    );
    held.release();
    tokio::time::sleep(Duration::from_secs(1)).await;
    open().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn closing_stops_the_worker_before_the_storage_room_closes() {
    // A client behind the log over an engine that holds its closes, with a download in flight:
    // when `close_room` and `shutdown` close the storage room, no task of the worker holds the
    // client anymore.
    let (server, room_id, designee, _lagging) = designated().await;
    designee.client.start_worker(room_id.clone()).await.unwrap();
    let (transport, storage) = (
        server.transport(),
        TestEngine::new(MemoryStorageEngine::new()),
    );
    let client = client_over(storage.clone(), &transport, ClientConfig::new("ana")).await;
    open_room(&client, &room_id).await.unwrap();
    for shutdown in [false, true] {
        let mut held = transport.hold_next_reply(RequestSnapshotChunk);
        client.start_worker(room_id.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(61)).await;
        held.replied().await;
        let mut closing = storage.hold_next("close_room");
        let closed = tokio::spawn({
            let (client, room_id) = (Arc::clone(&client), room_id.clone());
            async move {
                match shutdown {
                    false => client.close_room(room_id).await,
                    true => client.shutdown().await,
                }
            }
        });
        closing.replied().await;
        // The test and the task that closes.
        assert_eq!(Arc::strong_count(&client), 2, "{shutdown}");
        closing.release();
        assert_eq!(closed.await.unwrap(), Ok(()), "{shutdown}");
        drop(held);
        if !shutdown {
            open_room(&client, &room_id).await.unwrap();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_worker_started_during_a_discard_starts_once_it_ended() {
    // While a heartbeat discards the replica for `RoomReset`, the replica is out of the client:
    // starting the worker waits for the discard, and starts it in the empty room.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let storage = TestEngine::new(MemoryStorageEngine::new());
    let client = client_over(storage.clone(), &transport, ClientConfig::new("ana")).await;
    open_room(&client, &room_id).await.unwrap();
    let (mut heartbeat, mut registration) = (true, true);
    transport.answer(move |message| {
        let (correlation_id, room_id) = request_of(message);
        let code = match message.kind() {
            Heartbeat if std::mem::take(&mut heartbeat) => ErrorCode::ClientNotRegistered,
            RegisterClient if std::mem::take(&mut registration) => ErrorCode::RoomReset,
            _ => return None,
        };
        Some(Ok(error_reply(correlation_id, &room_id, code)))
    });
    let mut deleting = storage.hold_next("delete_room");
    let beat = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.heartbeat(room_id).await }
    });
    deleting.replied().await;
    let started = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.start_worker(room_id).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!started.is_finished());
    deleting.release();
    assert_eq!(beat.await.unwrap(), Ok(()));
    assert_eq!(started.await.unwrap(), Ok(()));
    let status = client.status(room_id.clone()).await.unwrap();
    assert_eq!((status.epoch, status.registered), (2, true));
    assert!(status.worker.unwrap().running);
}

#[tokio::test(start_paused = true)]
async fn the_status_gives_the_mode_of_the_worker_and_whether_the_last_request_got_a_reply() {
    let server = TestServer::new();
    let (room_a, room_b) = (
        server.create_room("room-a").await,
        server.create_room("room-b").await,
    );
    let transport = server.transport();
    let config = ClientConfig {
        freshness: Freshness::Live,
        ..ClientConfig::new("ana")
    };
    let client = client_with(&transport, config).await;
    // A room the client never opened, or closed, starts no worker.
    let never = RoomId::new("room-c").unwrap();
    let closed = Err(ClientError::RoomClosed(never.clone()));
    assert_eq!(client.start_worker(never.clone()).await, closed);
    assert_eq!(client.stop_worker(never.clone()).await, closed);
    open_room(&client, &room_a).await.unwrap();
    open_room(&client, &room_b).await.unwrap();
    assert_eq!(worker(&client, &room_a).await, None);

    // The mode of the configuration, which `set_freshness` changes; starting the worker again
    // leaves it as it is.
    client.start_worker(room_a.clone()).await.unwrap();
    let mode = |freshness| {
        Some(WorkerStatus {
            freshness,
            running: true,
            connected: Some(true),
            last_error: None,
        })
    };
    assert_eq!(worker(&client, &room_a).await, mode(Freshness::Live));
    client
        .set_freshness(room_a.clone(), Freshness::Manual)
        .await
        .unwrap();
    client.start_worker(room_a.clone()).await.unwrap();
    assert_eq!(worker(&client, &room_a).await, mode(Freshness::Manual));
    // A room without a worker starts one in the mode it gets.
    client
        .set_freshness(room_b.clone(), Freshness::Periodic)
        .await
        .unwrap();
    assert_eq!(worker(&client, &room_b).await, mode(Freshness::Periodic));
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert!(!heartbeats(&transport, &room_b).is_empty());

    // A request without a reply, of the worker or not, leaves the room without connection, and
    // the next one with a reply gives it back.
    transport.lose_next_reply(Sync);
    network(client.sync(room_a.clone()).await);
    let lost = worker(&client, &room_a).await.unwrap();
    assert_eq!((lost.connected, lost.last_error), (Some(false), None));
    client.sync(room_a.clone()).await.unwrap();
    assert_eq!(worker(&client, &room_a).await, mode(Freshness::Manual));

    soon(client.close_room(room_a.clone())).await.unwrap();
    let closed = ClientError::RoomClosed(room_a.clone());
    assert_eq!(
        client.start_worker(room_a.clone()).await,
        Err(closed.clone())
    );
    let status: Result<RoomStatus, _> = client.status(room_a.clone()).await;
    assert_eq!(status, Err(closed));
}

/// A server with `room-a`, and a client over a memory engine whose worker runs in `freshness`
/// with a heartbeat every `interval`, with the room open.
async fn worker_in(
    freshness: Freshness,
    interval: Duration,
) -> (
    TestServer,
    RoomId,
    TestTransport,
    MemoryStorageEngine,
    Arc<Client>,
) {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (transport, storage) = (server.transport(), MemoryStorageEngine::new());
    let config = ClientConfig {
        freshness,
        ..every(interval)
    };
    let client = client_over(storage.clone(), &transport, config).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    (server, room_id, transport, storage, client)
}

/// The head of the replica of `room_id` of `client`.
async fn head(client: &Client, room_id: &RoomId) -> u64 {
    client.status(room_id.clone()).await.unwrap().head_seq.get()
}

/// An interval and a little more: within it, the next heartbeat goes, and the sync it leads to
/// ends (with the paused clock, the server in process and memory engines, a sync takes no time).
fn past(interval: Duration) -> Duration {
    interval + Duration::from_millis(10)
}

#[tokio::test(start_paused = true)]
async fn in_periodic_and_live_what_another_client_writes_reaches_the_replica_within_an_interval() {
    // `live` without its event stream, which comes later, as `periodic`.
    let interval = Duration::from_secs(10);
    for freshness in [Freshness::Periodic, Freshness::Live] {
        let (server, room_id, transport, storage, client) = worker_in(freshness, interval).await;
        for id in 1..=3 {
            server.commit(&room_id, [id]).await;
            tokio::time::sleep(past(interval)).await;
            assert_eq!(head(&client, &room_id).await, id as u64, "{freshness:?}");
        }
        assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3], "{freshness:?}");
        // A sync for each head that a heartbeat found ahead, after the ack of the head that the
        // sync before left, and once the replica is there, only the ack of its head.
        let syncs = [
            ("sync", 0),
            ("ack", 1),
            ("sync", 1),
            ("ack", 2),
            ("sync", 2),
        ];
        assert_eq!(requests(&transport), syncs, "{freshness:?}");
        tokio::time::sleep(interval * 3).await;
        let requests = requests(&transport);
        assert_eq!(requests[..5], syncs, "{freshness:?}");
        assert_eq!(requests[5..], [("ack", 3)], "{freshness:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn in_manual_what_another_client_writes_reaches_the_replica_with_a_sync_or_a_write() {
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, storage, client) =
        worker_in(Freshness::Manual, interval).await;
    // The heartbeats go on, and none leads to a sync.
    server.commit(&room_id, [1]).await;
    tokio::time::sleep(interval * 3).await;
    assert!(heartbeats(&transport, &room_id).len() >= 3);
    assert_eq!(head(&client, &room_id).await, 0);
    assert!(sent_of(&transport, Sync).is_empty());

    // The sync and the writes of the application catch the replica up.
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&client, &room_id).await, 1);
    server.commit(&room_id, [2]).await;
    tokio::time::sleep(interval * 3).await;
    assert_eq!(head(&client, &room_id).await, 1);
    client.write(room_id.clone(), insert_task(3)).await.unwrap();
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3]);

    // Back in `periodic`, the next heartbeat leads to a sync.
    server.commit(&room_id, [4]).await;
    let syncs = sent_of(&transport, Sync).len();
    client
        .set_freshness(room_id.clone(), Freshness::Periodic)
        .await
        .unwrap();
    tokio::time::sleep(past(interval)).await;
    assert_eq!(head(&client, &room_id).await, 4);
    assert_eq!(sent_of(&transport, Sync).len(), syncs + 1);
}

#[tokio::test(start_paused = true)]
async fn in_manual_the_worker_still_uploads_the_snapshot_and_downloads_it() {
    let (server, room_id, designee, lagging) = designated().await;
    for member in [&designee, &lagging] {
        let client = &member.client;
        client
            .set_freshness(room_id.clone(), Freshness::Manual)
            .await
            .unwrap();
    }
    // Within the 60 s of the designation, and a heartbeat of the client behind the log.
    tokio::time::sleep(Duration::from_secs(91)).await;
    let active = server.state.snapshot_relay.active_snapshot_seq(&room_id);
    assert_eq!(active, Some(SequenceNumber::new(9)));
    assert_eq!(head(&lagging.client, &room_id).await, 9);
    let all = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    assert_eq!(tasks(&lagging.storage, &room_id).await, all);
    assert!(sent_of(&lagging.transport, Sync).is_empty());
}

#[tokio::test(start_paused = true)]
async fn heartbeats_with_the_head_ahead_during_a_held_sync_of_the_worker_ask_for_one_more() {
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, storage, _client) =
        worker_in(Freshness::Periodic, interval).await;
    let mut held = transport.hold_next_reply(Sync);
    server.commit(&room_id, [1]).await;
    tokio::time::sleep(past(interval)).await;
    held.replied().await;
    // Three heartbeats find the head ahead while the sync of the worker is held: no sync more is
    // sent meanwhile.
    server.commit(&room_id, 2..=4).await;
    let before = heartbeats(&transport, &room_id).len();
    tokio::time::sleep(interval * 3).await;
    assert!(heartbeats(&transport, &room_id).len() >= before + 3);
    assert_eq!(requests(&transport), [("sync", 0)]);

    // Once it ends, one more, from where it left the replica, long before the next heartbeat.
    held.release();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(requests(&transport), [("sync", 0), ("sync", 1)]);
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4]);
    // The next heartbeat acknowledges the head the two left.
    tokio::time::sleep(interval * 3).await;
    let requests = requests(&transport);
    assert_eq!(requests, [("sync", 0), ("sync", 1), ("ack", 4)]);
}

#[tokio::test(start_paused = true)]
async fn a_sync_marked_before_the_mode_turns_manual_does_not_start() {
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    let mut held = transport.hold_next_reply(Sync);
    server.commit(&room_id, [1]).await;
    tokio::time::sleep(past(interval)).await;
    held.replied().await;
    // A heartbeat marks one sync more, and then the mode turns `manual`.
    server.commit(&room_id, [2]).await;
    tokio::time::sleep(interval).await;
    client
        .set_freshness(room_id.clone(), Freshness::Manual)
        .await
        .unwrap();
    held.release();
    tokio::time::sleep(interval * 3).await;
    // In `manual` too, a heartbeat acknowledges the head that moved.
    assert_eq!(requests(&transport), [("sync", 0), ("ack", 1)]);
    assert_eq!(head(&client, &room_id).await, 1);
}

#[tokio::test(start_paused = true)]
async fn a_sync_of_the_worker_that_succeeds_clears_the_error_of_a_heartbeat() {
    // A failed heartbeat is retried after a backoff of up to 30 s.
    let interval = Duration::from_secs(10);
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let config = ClientConfig {
        retry_base: Duration::from_secs(30),
        retry_max: Duration::from_secs(30),
        ..every(interval)
    };
    let client = client_with(&transport, config).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    let mut held = transport.hold_next_reply(Sync);
    server.commit(&room_id, [1]).await;
    tokio::time::sleep(past(interval)).await;
    held.replied().await;
    // A heartbeat fails while the sync is held.
    transport.offline();
    tokio::time::sleep(interval).await;
    let failed = worker(&client, &room_id).await.unwrap();
    assert!(
        matches!(failed.last_error, Some(ClientError::Network(_))),
        "{failed:?}"
    );
    transport.online();
    held.release();
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(head(&client, &room_id).await, 1);
    assert_eq!(worker(&client, &room_id).await.unwrap().last_error, None);
}

#[tokio::test(start_paused = true)]
async fn a_batch_of_a_sync_of_the_worker_that_asks_for_a_snapshot_has_it_uploaded_at_once() {
    // A member at 7 of a room at 8, with its log pruned before 6 (the server seals a segment
    // every 5 operations, so 6 to 8 stay), and a client behind the log, for which the server
    // designates the member.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let designee = member(&server, &room_id).await;
    server.commit(&room_id, 1..=7).await;
    designee.client.sync(room_id.clone()).await.unwrap();
    server.commit(&room_id, [8]).await;
    server.prune(&room_id, 6).await;
    let _lagging = member(&server, &room_id).await;
    // The ack of the first heartbeat of its worker, as the server would give it a moment before
    // the designation, finds the head ahead and does not designate it; the batch of the sync
    // that follows does.
    answer_next(&designee.transport, Heartbeat, |correlation_id, room_id| {
        encode_message(&ServerMessage::HeartbeatAck {
            correlation_id,
            room_id: room_id.clone(),
            current_head_seq: SequenceNumber::new(8),
            snapshot_wanted: false,
            active_snapshot_seq: None,
            schema_version: 1,
            schema_id: SchemaId::new("tasks").unwrap(),
        })
        .unwrap()
    });
    designee.client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(31)).await;
    // A heartbeat in the task of the transfers confirms the designation, and the member uploads
    // its snapshot at once, without waiting for the next heartbeat of the interval (30 s).
    let active = server.state.snapshot_relay.active_snapshot_seq(&room_id);
    assert_eq!(active, Some(SequenceNumber::new(8)));
    let first = heartbeats(&designee.transport, &room_id)[0];
    let uploaded = sent_of(&designee.transport, UploadSnapshotChunk);
    assert!(
        uploaded[0] - first < Duration::from_secs(1),
        "{first:?} {uploaded:?}"
    );
    // Its own sync, and that of its worker; a second heartbeat of the worker, if its first came
    // within the first second, acknowledges the head of the sync.
    let requests = requests(&designee.transport);
    assert_eq!(requests[..3], [("sync", 0), ("ack", 7), ("sync", 7)]);
    assert!(
        requests[3..].iter().all(|ack| *ack == ("ack", 8)),
        "{requests:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_sync_of_the_application_during_one_of_the_worker_ends_and_both_leave_the_head() {
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    // The batch of the sync of the worker, up to 3, is held while the application syncs up to 5.
    let mut held = transport.hold_next_reply(Sync);
    server.commit(&room_id, 1..=3).await;
    tokio::time::sleep(past(interval)).await;
    held.replied().await;
    server.commit(&room_id, 4..=5).await;
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&client, &room_id).await, 5);
    held.release();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(head(&client, &room_id).await, 5);
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4, 5]);
    let running = worker(&client, &room_id).await.unwrap();
    assert!(
        running.running && running.last_error.is_none(),
        "{running:?}"
    );
    let requests = requests(&transport);
    assert_eq!(requests, [("sync", 0), ("sync", 0), ("ack", 5)]);
}

/// An insert of row `id` into `tasks` of [`retyped`]: it does not validate against
/// [`tasks_schema`].
fn insert_retyped(id: i64) -> Operation {
    let row = RowBuilder::new().set("id", id).set("title", 7).build();
    retyped().to_operation_insert("tasks", &row, 1000).unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_sync_of_the_worker_without_a_reply_goes_on_and_one_that_fails_closed_stops_it() {
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    // Without a reply, the worker goes on with the error, and the next heartbeat asks again.
    transport.lose_next_reply(Sync);
    server.commit(&room_id, [1]).await;
    tokio::time::sleep(past(interval)).await;
    let lost = worker(&client, &room_id).await.unwrap();
    assert!(lost.running, "{lost:?}");
    assert!(
        matches!(lost.last_error, Some(ClientError::Network(_))),
        "{lost:?}"
    );
    tokio::time::sleep(interval).await;
    assert_eq!(head(&client, &room_id).await, 1);
    assert_eq!(worker(&client, &room_id).await.unwrap().last_error, None);

    // Every batch brings an operation that does not validate against the schema of the replica,
    // also after the sync registered again: the room fails closed, and the worker stops at once.
    let op = SequencedOperation::new(SequenceNumber::new(2), insert_retyped(2));
    transport.answer_every(Sync, move |message| {
        let (correlation_id, room_id) = request(message);
        encode_message(&ServerMessage::SyncBatch {
            correlation_id,
            room_id,
            head_seq: SequenceNumber::new(2),
            ops: vec![op.clone()],
            has_more: false,
            snapshot_wanted: false,
            schema_version: 1,
            schema_id: SchemaId::new("tasks").unwrap(),
        })
        .unwrap()
    });
    server.commit(&room_id, [2]).await;
    let mut waited = Duration::ZERO;
    while !client.status(room_id.clone()).await.unwrap().failed_closed {
        assert!(waited < interval * 3, "The room did not fail closed");
        tokio::time::sleep(Duration::from_millis(100)).await;
        waited += Duration::from_millis(100);
    }
    let beats = heartbeats(&transport, &room_id).len();
    // Long before the next heartbeat.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let stopped = worker(&client, &room_id).await.unwrap();
    assert!(!stopped.running, "{stopped:?}");
    let failed_closed = Some(ClientError::FailedClosed(room_id.clone()));
    assert_eq!(stopped.last_error, failed_closed);
    assert_eq!(heartbeats(&transport, &room_id).len(), beats);
}

#[tokio::test(start_paused = true)]
async fn stopping_the_worker_cuts_its_sync_and_that_of_a_client_dropped_during_one_ends() {
    // Whether the sync of the worker still runs shows in the references to the client, which it
    // holds while it runs.
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    let mut held = transport.hold_next_reply(Sync);
    server.commit(&room_id, [1]).await;
    tokio::time::sleep(past(interval)).await;
    held.replied().await;
    assert_eq!(Arc::strong_count(&client), 2);
    soon(client.stop_worker(room_id.clone())).await.unwrap();
    assert_eq!(Arc::strong_count(&client), 1);
    drop(held);

    // Dropped with the sync of its worker in flight, the client goes once the next heartbeat
    // finds that only the sync holds it, and the heartbeats stop.
    let mut held = transport.hold_next_reply(Sync);
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(past(interval)).await;
    held.replied().await;
    let dropped = Arc::downgrade(&client);
    drop(client);
    tokio::time::sleep(past(interval)).await;
    assert!(dropped.upgrade().is_none());
    let before = transport.sent().len();
    tokio::time::sleep(interval * 6).await;
    assert_eq!(transport.sent().len(), before);
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn a_client_dropped_while_its_worker_marks_one_more_sync_goes_once_the_sync_ends() {
    // The worker of another room holds the client meanwhile, with its heartbeat in flight.
    let server = TestServer::new();
    let (room_a, room_b) = (
        server.create_room("room-a").await,
        server.create_room("room-b").await,
    );
    let transport = server.transport();
    let interval = Duration::from_secs(10);
    let client = client_with(&transport, every(interval)).await;
    open_room(&client, &room_a).await.unwrap();
    open_room(&client, &room_b).await.unwrap();
    let mut beating = transport.hold_next_reply(Heartbeat);
    client.start_worker(room_b.clone()).await.unwrap();
    tokio::time::sleep(past(interval)).await;
    beating.replied().await;
    let mut held = transport.hold_next_reply(Sync);
    client.start_worker(room_a.clone()).await.unwrap();
    server.commit(&room_a, [1]).await;
    tokio::time::sleep(past(interval)).await;
    held.replied().await;
    // A heartbeat marks one sync more meanwhile.
    server.commit(&room_a, [2]).await;
    tokio::time::sleep(interval).await;
    let dropped = Arc::downgrade(&client);
    drop(client);
    // The sync ends long before the next heartbeat: the worker stops instead of starting the
    // one more.
    let before = transport.sent().len();
    held.release();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(transport.sent().len(), before);
    // The other worker stops at its next heartbeat, and the client goes.
    beating.release();
    tokio::time::sleep(past(interval)).await;
    assert!(dropped.upgrade().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_write_whose_sequence_the_sync_of_the_worker_applies_ends_with_its_decision() {
    // The `CommitAck` of an update of a missing row is held while the sync of the worker applies
    // its sequence, and rejects it.
    let interval = Duration::from_secs(10);
    let (_server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    let mut held = transport.hold_next_reply(ClientMessageKind::Commit);
    let write = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.write(room_id, update_task(5)).await }
    });
    held.replied().await;
    tokio::time::sleep(past(interval)).await;
    assert_eq!(head(&client, &room_id).await, 1);
    held.release();
    assert_eq!(write.await.unwrap(), Err(ClientError::RowMissing));
    // The sync of the worker (and the ack of the next heartbeat, if it came already).
    let syncs: Vec<_> = (requests(&transport).into_iter())
        .filter(|(kind, _)| *kind == "sync")
        .collect();
    assert_eq!(syncs, [("sync", 0)]);
}

#[tokio::test(start_paused = true)]
async fn a_sync_of_the_worker_behind_the_log_downloads_the_snapshot_once_as_heartbeats_go_on() {
    // A replica at 0 of a room pruned before 6, with a snapshot at 9, whose worker runs in
    // `manual` meanwhile.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let (transport, storage) = (server.transport(), MemoryStorageEngine::new());
    let interval = Duration::from_secs(10);
    let config = ClientConfig {
        freshness: Freshness::Manual,
        ..every(interval)
    };
    let client = client_over(storage.clone(), &transport, config).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    let ops = server.commit(&room_id, 1..=9).await;
    server.prune(&room_id, 6).await;
    let snapshot = snapshot(&room_id, tasks_schema(), &ops).await;
    server.upload(&room_id, 9, snapshot).await;
    // In `periodic`, a heartbeat finds the head ahead: its sync gets `BehindCompaction`,
    // registers again, and downloads the snapshot that `Registered` announces, with its first
    // chunk held. The heartbeats go on, and theirs wait for that download.
    let mut held = transport.hold_next_reply(RequestSnapshotChunk);
    client
        .set_freshness(room_id.clone(), Freshness::Periodic)
        .await
        .unwrap();
    tokio::time::sleep(past(interval)).await;
    held.replied().await;
    let before = heartbeats(&transport, &room_id).len();
    tokio::time::sleep(interval * 2).await;
    assert!(heartbeats(&transport, &room_id).len() >= before + 2);
    held.release();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(head(&client, &room_id).await, 9);
    let all = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    assert_eq!(tasks(&storage, &room_id).await, all);
    assert_eq!(sent_of(&transport, RequestSnapshotChunk).len(), 1);
    let running = worker(&client, &room_id).await.unwrap();
    assert!(
        running.running && running.last_error.is_none(),
        "{running:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_schema_change_during_a_sync_of_the_worker_registers_it_again_and_applies_with_it() {
    // The ack of a heartbeat leaves the server before its schema changes, and the batch of the
    // sync it leads to announces the new version.
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    server.commit(&room_id, [1]).await;
    let mut held = transport.hold_next_reply(Heartbeat);
    tokio::time::sleep(past(interval)).await;
    held.replied().await;
    let schema_id = SchemaId::new("tasks").unwrap();
    let tag = || ColumnDef::new("tag", DataType::String).nullable(true);
    let registry = &server.state.schema_registry;
    let added = registry.add_column_with_constraints(&schema_id, "tasks", tag(), vec![], vec![]);
    added.unwrap();
    let rooms = &server.state.room_manager;
    rooms.reload_schema_for_rooms(&schema_id).await;
    let mut tagged = tasks_schema();
    tagged
        .add_column_with_constraints("tasks", tag(), vec![], vec![])
        .unwrap();
    let row = RowBuilder::new()
        .set("id", 2)
        .set("title", "task")
        .set("tag", "a")
        .build();
    let insert = tagged.to_operation_insert("tasks", &row, 1000).unwrap();
    server.commit_ops(&room_id, [insert]).await;
    held.release();
    tokio::time::sleep(Duration::from_millis(10)).await;
    // It did not apply the batch, registered again with its cursor, and took the new schema.
    let incarnation = server.incarnation(&room_id).await;
    let registrations = [(None, None), (Some(0), Some(incarnation))];
    assert_eq!(transport.registrations(), registrations);
    assert_eq!(requests(&transport), [("sync", 0), ("sync", 0)]);
    assert_eq!(head(&client, &room_id).await, 2);
    // The row 2, with its tag, applies only with the new schema.
    assert_eq!(tasks(&storage, &room_id).await, [1, 2]);
    let running = worker(&client, &room_id).await.unwrap();
    assert!(
        running.running && running.last_error.is_none(),
        "{running:?}"
    );
}

/// A change of a configuration.
type Change = fn(&mut ClientConfig);

#[tokio::test]
async fn a_configuration_of_the_worker_that_cannot_work_is_config() {
    // Each change of the configuration, and whether the client opens with it.
    let configurations: [(Change, bool); 9] = [
        (|config| config.heartbeat_interval = Duration::ZERO, false),
        (
            |config| config.heartbeat_interval = Duration::from_secs(60),
            false,
        ),
        (
            |config| config.heartbeat_interval = Duration::from_secs(600),
            false,
        ),
        (
            |config| config.heartbeat_interval = Duration::from_millis(59_999),
            true,
        ),
        (|config| config.retry_base = Duration::ZERO, false),
        (|config| config.retry_max = config.retry_base / 2, false),
        (|config| config.retry_max = config.retry_base, true),
        (|config| config.events_retry = Duration::ZERO, false),
        (
            |config| config.events_retry = Duration::from_millis(1),
            true,
        ),
    ];
    for (index, (change, works)) in configurations.into_iter().enumerate() {
        let mut config = ClientConfig::new("ana");
        change(&mut config);
        let storage = MemoryStorageEngine::new();
        let transport = TestServer::new().transport();
        let opened = Client::open(
            config,
            storage,
            MemoryMetadata::default(),
            transport,
            TestTokens::new(),
        );
        match (opened.await, works) {
            (Ok(_), true) | (Err(ClientError::Config(_)), false) => {}
            (opened, _) => panic!("{index}: {opened:?}"),
        }
    }
}

/// The heads that the acks through `transport` acknowledged, in order.
fn acks(transport: &TestTransport) -> Vec<u64> {
    let acks = requests(transport)
        .into_iter()
        .filter(|(kind, _)| *kind == "ack");
    acks.map(|(_, seq)| seq).collect()
}

/// Waits until the replica of `room_id` of `client` is at `head`, as a sync of its worker leaves
/// it, and returns at once: the next heartbeat, which acknowledges that head, is an interval
/// away. Sleeping an interval instead lets the next heartbeat in when the first one came within
/// the first milliseconds.
async fn synced_to(client: &Client, room_id: &RoomId, at: u64) {
    for _ in 0..60_000 {
        if head(client, room_id).await == at {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("The replica of {room_id} did not reach {at}");
}

#[tokio::test(start_paused = true)]
async fn the_worker_acknowledges_the_head_of_the_replica_once_an_interval_not_once_a_sync() {
    // Each sync asks for an operation at a time, so that it sends a request for each.
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    transport.sync_batch_size(1);
    // Each round writes right after the sync of the round before, so the next heartbeat syncs it.
    for round in 0..3 {
        server.commit(&room_id, round * 3 + 1..=round * 3 + 3).await;
        synced_to(&client, &room_id, (round as u64 + 1) * 3).await;
    }
    assert_eq!(sent_of(&transport, Sync).len(), 9);
    // With each heartbeat, the head of the replica if it moved: the first found it where it
    // registered.
    assert_eq!(acks(&transport), [3, 6]);
    tokio::time::sleep(past(interval)).await;
    assert_eq!(acks(&transport), [3, 6, 9]);
    // The server took the last one: without changes, none more, within ten heartbeats.
    tokio::time::sleep(interval * 4).await;
    assert_eq!(acks(&transport), [3, 6, 9]);
    assert!(heartbeats(&transport, &room_id).len() >= 8);

    // A room that never changes sends none in its first nine heartbeats, and the tenth
    // acknowledges its head anyway, since the server may have taken a lower cursor since.
    let (_server, room_id, transport, _storage, _client) =
        worker_in(Freshness::Periodic, interval).await;
    // The first heartbeat comes within the first 10 s, so the tenth within 90 s and 100 s.
    tokio::time::sleep(Duration::from_secs(89)).await;
    assert!(heartbeats(&transport, &room_id).len() >= 8);
    assert!(acks(&transport).is_empty());
    tokio::time::sleep(Duration::from_secs(12)).await;
    let beats = heartbeats(&transport, &room_id);
    assert!(beats.len() >= 10, "{beats:?}");
    assert_eq!(acks(&transport), [0]);
    assert_eq!(sent(&transport, Ack, &room_id), [beats[9]]);
}

#[tokio::test(start_paused = true)]
async fn an_ack_of_the_worker_refused_or_confirmed_below_registers_the_replica_with_its_cursor() {
    let interval = Duration::from_secs(10);
    for case in 0..4 {
        let (server, room_id, transport, _storage, client) =
            worker_in(Freshness::Periodic, interval).await;
        server.commit(&room_id, 1..=3).await;
        synced_to(&client, &room_id, 3).await;
        // A confirmation of the head with a head of the server below it, or a refusal that asks
        // to register (`BehindCompaction` too: the registration finds it inside the log).
        answer_next(&transport, Ack, move |correlation_id, room_id| match case {
            0 => encode_message(&ServerMessage::AckConfirmed {
                correlation_id,
                room_id: room_id.clone(),
                ack_seq: SequenceNumber::new(3),
                head_seq: SequenceNumber::new(2),
            })
            .unwrap(),
            1 => error_reply(correlation_id, room_id, ErrorCode::ClientNotRegistered),
            2 => error_reply(correlation_id, room_id, ErrorCode::InvalidSequence),
            _ => error_reply(correlation_id, room_id, ErrorCode::BehindCompaction),
        });
        tokio::time::sleep(past(interval)).await;
        assert_eq!(acks(&transport), [3], "{case}");
        let incarnation = server.incarnation(&room_id).await;
        let registrations = [(None, None), (Some(3), Some(incarnation))];
        assert_eq!(transport.registrations(), registrations, "{case}");
        // The registration presented the head: no ack more.
        tokio::time::sleep(interval * 3).await;
        assert_eq!(acks(&transport), [3], "{case}");
        assert_eq!(transport.registrations().len(), 2, "{case}");
        let running = worker(&client, &room_id).await.unwrap();
        assert!(
            running.running && running.last_error.is_none(),
            "{running:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn an_ack_of_the_worker_that_fails_goes_again_and_one_that_fails_the_room_closed_stops_it() {
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    server.commit(&room_id, 1..=2).await;
    synced_to(&client, &room_id, 2).await;
    // Without a reply, the worker goes on with the error, and the next heartbeat acknowledges
    // again, which clears it.
    transport.lose_next_reply(Ack);
    tokio::time::sleep(past(interval)).await;
    let lost = worker(&client, &room_id).await.unwrap();
    assert!(lost.running, "{lost:?}");
    assert!(
        matches!(lost.last_error, Some(ClientError::Network(_))),
        "{lost:?}"
    );
    tokio::time::sleep(past(interval)).await;
    assert_eq!(acks(&transport), [2, 2]);
    let running = worker(&client, &room_id).await.unwrap();
    assert!(
        running.running && running.last_error.is_none(),
        "{running:?}"
    );

    // A refusal of the ack that asks to register, whose registration gets a schema that does not
    // evolve from the replica's, fails the room closed and stops the worker.
    server.commit(&room_id, [3]).await;
    synced_to(&client, &room_id, 3).await;
    let incarnation = server.incarnation(&room_id).await;
    let (mut ack, mut registration) = (true, true);
    transport.answer(move |message| match message.kind() {
        Ack if std::mem::take(&mut ack) => {
            let (correlation_id, room_id) = request_of(message);
            let refused = error_reply(correlation_id, &room_id, ErrorCode::ClientNotRegistered);
            Some(Ok(refused))
        }
        RegisterClient if std::mem::take(&mut registration) => {
            Some(Ok(registered_retyped(message, incarnation)))
        }
        _ => None,
    });
    tokio::time::sleep(past(interval)).await;
    let status = client.status(room_id.clone()).await.unwrap();
    assert!(status.failed_closed, "{status:?}");
    let stopped = worker(&client, &room_id).await.unwrap();
    assert!(!stopped.running, "{stopped:?}");
    let failed_closed = Some(ClientError::FailedClosed(room_id.clone()));
    assert_eq!(stopped.last_error, failed_closed);
    assert_eq!(acks(&transport), [2, 2, 3]);
    let beats = heartbeats(&transport, &room_id).len();
    tokio::time::sleep(interval * 3).await;
    assert_eq!(heartbeats(&transport, &room_id).len(), beats);
}

/// A client over a disk engine in `dir`, with its metadata there, whose workers send a heartbeat
/// every `interval`.
async fn client_on_disk(
    dir: &std::path::Path,
    transport: &TestTransport,
    interval: Duration,
) -> Arc<Client> {
    let storage = engine(&dir.join("rooms"));
    let metadata = FileMetadata::new(dir.join("metadata"));
    let client = Client::open(
        every(interval),
        storage,
        metadata,
        transport.clone(),
        TestTokens::new(),
    );
    Arc::new(client.await.unwrap())
}

/// The tasks that the client over the disk engine in `dir` finds in `room_id` once it opens it
/// again.
async fn tasks_on_disk(
    dir: &std::path::Path,
    transport: &TestTransport,
    room_id: &RoomId,
) -> Vec<i64> {
    let storage = engine(&dir.join("rooms"));
    let metadata = FileMetadata::new(dir.join("metadata"));
    let client = Client::open(
        every(Duration::from_secs(10)),
        storage.clone(),
        metadata,
        transport.clone(),
        TestTokens::new(),
    );
    let client = client.await.unwrap();
    open_room(&client, room_id).await.unwrap();
    let tasks = tasks(&storage, room_id).await;
    client.shutdown().await.unwrap();
    tasks
}

#[tokio::test(start_paused = true)]
async fn closing_and_shutting_down_acknowledge_a_head_that_moved_within_their_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::new();
    let rooms = [
        server.create_room("room-a").await,
        server.create_room("room-b").await,
        server.create_room("room-c").await,
    ];
    let transport = server.transport();
    let interval = Duration::from_secs(10);
    let client = client_on_disk(dir.path(), &transport, interval).await;
    let room_id = &rooms[0];
    open_room(&client, room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    // The sync of a heartbeat leaves the replica at 2, before the next heartbeat acknowledges it:
    // closing acknowledges it, and no task of the worker sends anything afterwards.
    server.commit(room_id, 1..=2).await;
    synced_to(&client, room_id, 2).await;
    assert!(acks(&transport).is_empty());
    soon(client.close_room(room_id.clone())).await.unwrap();
    assert_eq!(acks(&transport), [2]);
    let closed = transport.sent().len();
    tokio::time::sleep(interval * 3).await;
    assert_eq!(transport.sent().len(), closed);

    // Without network (the ack never reaches the server), closing waits for the ack 5 s.
    open_room(&client, room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    server.commit(room_id, [3]).await;
    synced_to(&client, room_id, 3).await;
    transport.slow(Duration::from_secs(3600));
    let start = Instant::now();
    soon(client.close_room(room_id.clone())).await.unwrap();
    let waited = start.elapsed();
    assert!(waited >= Duration::from_secs(5), "{waited:?}");
    assert!(waited < Duration::from_secs(6), "{waited:?}");
    assert_eq!(acks(&transport), [2, 3]);

    // Shutting down acknowledges the heads of the rooms that moved, all at once: without
    // network, within the 5 s of one. With the writes before the workers start, the first
    // heartbeat of each syncs, and both come within the first interval, before any second one.
    transport.slow(Duration::ZERO);
    server.commit(&rooms[0], [4]).await;
    server.commit(&rooms[1], 1..=5).await;
    for room_id in &rooms {
        open_room(&client, room_id).await.unwrap();
        client.start_worker(room_id.clone()).await.unwrap();
    }
    synced_to(&client, &rooms[0], 4).await;
    synced_to(&client, &rooms[1], 5).await;
    assert_eq!(acks(&transport), [2, 3]);
    transport.slow(Duration::from_secs(3600));
    let start = Instant::now();
    soon(client.shutdown()).await.unwrap();
    let waited = start.elapsed();
    assert!(waited >= Duration::from_secs(5), "{waited:?}");
    assert!(waited < Duration::from_secs(6), "{waited:?}");
    let last = |room_id: &RoomId| {
        let acks = transport
            .sent_at()
            .into_iter()
            .filter_map(|(message, at)| match message {
                ClientMessage::Ack {
                    room_id: of,
                    ack_seq,
                    ..
                } if of == *room_id => Some((ack_seq.get(), at)),
                _ => None,
            });
        acks.last()
    };
    let (a, b) = (last(&rooms[0]).unwrap(), last(&rooms[1]).unwrap());
    assert_eq!((a.0, b.0), (4, 5));
    assert_eq!(a.1, b.1);
    // The room that did not move sends none.
    assert_eq!(last(&rooms[2]), None);
    let closed = transport.sent().len();
    tokio::time::sleep(interval * 3).await;
    assert_eq!(transport.sent().len(), closed);

    // The disk engine opens the rooms again with all that the replicas applied.
    transport.slow(Duration::ZERO);
    drop(client);
    assert_eq!(
        tasks_on_disk(dir.path(), &transport, &rooms[0]).await,
        [1, 2, 3, 4]
    );
    assert_eq!(
        tasks_on_disk(dir.path(), &transport, &rooms[1]).await,
        [1, 2, 3, 4, 5]
    );
}

#[tokio::test(start_paused = true)]
async fn the_last_ack_is_one_attempt_and_a_reply_that_asks_to_register_registers_nothing() {
    // Stopping the worker sends it too, as closing does.
    let interval = Duration::from_secs(10);
    for case in 0..2 {
        let (server, room_id, transport, _storage, client) =
            worker_in(Freshness::Periodic, interval).await;
        server.commit(&room_id, 1..=2).await;
        synced_to(&client, &room_id, 2).await;
        answer_next(&transport, Ack, move |correlation_id, room_id| match case {
            0 => error_reply(correlation_id, room_id, ErrorCode::ClientNotRegistered),
            _ => encode_message(&ServerMessage::AckConfirmed {
                correlation_id,
                room_id: room_id.clone(),
                ack_seq: SequenceNumber::new(2),
                head_seq: SequenceNumber::new(1),
            })
            .unwrap(),
        });
        let before = transport.sent().len();
        soon(client.stop_worker(room_id.clone())).await.unwrap();
        let kinds: Vec<_> = transport.sent()[before..]
            .iter()
            .map(ClientMessage::kind)
            .collect();
        assert_eq!(kinds, [Ack], "{case}");
        tokio::time::sleep(interval * 3).await;
        assert_eq!(transport.sent().len(), before + 1, "{case}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_close_during_a_stop_of_the_worker_waits_for_its_last_ack_also_if_the_stop_is_cut() {
    for cut in [false, true] {
        let interval = Duration::from_secs(10);
        let (server, room_id, transport, _storage, client) =
            worker_in(Freshness::Periodic, interval).await;
        server.commit(&room_id, 1..=2).await;
        synced_to(&client, &room_id, 2).await;
        // The reply to the last ack is held while the room closes.
        let mut held = transport.hold_next_reply(Ack);
        let stopping = tokio::spawn({
            let (client, room_id) = (Arc::clone(&client), room_id.clone());
            async move { client.stop_worker(room_id).await }
        });
        soon(held.replied()).await;
        if cut {
            stopping.abort();
        }
        let closing = tokio::spawn({
            let (client, room_id) = (Arc::clone(&client), room_id.clone());
            async move { client.close_room(room_id).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!closing.is_finished(), "{cut}");
        held.release();
        assert_eq!(soon(closing).await.unwrap(), Ok(()), "{cut}");
        assert_eq!(acks(&transport), [2], "{cut}");
        if !cut {
            assert_eq!(stopping.await.unwrap(), Ok(()));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_reader_that_syncs_only_with_its_worker_stays_connected_past_a_prune_and_is_designated() {
    // A room that gets no writes once the reader caught up with its worker: its log is pruned
    // past the cursor that the last sync of the worker left the server.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=10).await;
    let reader = member(&server, &room_id).await;
    reader.client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(head(&reader.client, &room_id).await, 10);
    server.prune(&room_id, 11).await;
    // Once the lease of the client that wrote ended, and the reader sent heartbeats after the
    // prune, the server takes the reader as connected, not behind its log.
    tokio::time::sleep(Duration::from_secs(91)).await;
    assert_eq!(leases(&server, &room_id).await, (0, 1));

    // A client behind the log gets the snapshot that the server designates the reader to upload,
    // without the reader calling a step.
    let lagging = member(&server, &room_id).await;
    lagging.client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(91)).await;
    assert!(!sent_of(&reader.transport, UploadSnapshotChunk).is_empty());
    assert_eq!(head(&lagging.client, &room_id).await, 10);
    let all = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    assert_eq!(tasks(&lagging.storage, &room_id).await, all);
}

/// Makes the next heartbeat through `transport` get `ClientNotRegistered`, and the next
/// `registrations` registrations a `Registered` of `room_incarnation` at the head 3 whose tail is
/// far beyond the log, which the client cannot check: the replica waits for a snapshot that the
/// server never announces, and none of its heartbeats leads to a sync.
fn wait_for_none(transport: &TestTransport, room_incarnation: u128, registrations: usize) {
    let (mut heartbeat, mut registrations) = (true, registrations);
    transport.answer(move |message| {
        let (correlation_id, room_id) = request_of(message);
        let reply = match message.kind() {
            Heartbeat if std::mem::take(&mut heartbeat) => {
                error_reply(correlation_id, &room_id, ErrorCode::ClientNotRegistered)
            }
            RegisterClient if registrations > 0 => {
                registrations -= 1;
                encode_message(&ServerMessage::Registered {
                    correlation_id,
                    room_id,
                    head_seq: SequenceNumber::new(3),
                    tail_seq: SequenceNumber::new(100),
                    schema_id: SchemaId::new("tasks").unwrap(),
                    schema: tasks_schema(),
                    active_snapshot_seq: None,
                    room_incarnation,
                })
                .unwrap()
            }
            _ => return None,
        };
        Some(Ok(reply))
    });
}

/// How many heartbeats went through `transport` between each two registrations, in order.
fn beats_between_registrations(transport: &TestTransport) -> Vec<usize> {
    let kinds: Vec<_> = transport.sent().iter().map(ClientMessage::kind).collect();
    let registered: Vec<_> = (kinds.iter().enumerate())
        .filter_map(|(index, kind)| (*kind == RegisterClient).then_some(index))
        .collect();
    let between = |pair: &[usize]| {
        kinds[pair[0]..pair[1]]
            .iter()
            .filter(|kind| **kind == Heartbeat)
            .count()
    };
    registered.windows(2).map(between).collect()
}

#[tokio::test(start_paused = true)]
async fn a_replica_that_waits_for_a_snapshot_nobody_announces_registers_again_in_ten_heartbeats() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=3).await;
    let incarnation = server.incarnation(&room_id).await;
    let transport = server.transport();
    let interval = Duration::from_secs(10);
    let client = client_with(&transport, every(interval)).await;
    open_room(&client, &room_id).await.unwrap();
    // The second registration, and the third, ten heartbeats after it, leave the replica waiting.
    wait_for_none(&transport, incarnation, 2);
    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(195)).await;
    assert_eq!(transport.registrations().len(), 3);
    assert_eq!(head(&client, &room_id).await, 0);
    assert!(sent_of(&transport, Sync).is_empty());

    // Ten heartbeats after the third, the fourth gets the tail of the server: the replica no
    // longer waits, and its heartbeat leads to a sync.
    tokio::time::sleep(Duration::from_secs(20)).await;
    let registered = (Some(0), Some(incarnation));
    let registrations = [(None, None), registered, registered, registered];
    assert_eq!(transport.registrations(), registrations);
    assert_eq!(beats_between_registrations(&transport), [1, 10, 10]);
    assert_eq!(head(&client, &room_id).await, 3);

    // Once it waited again, after heartbeats that did not leave it waiting, it takes ten
    // heartbeats again.
    tokio::time::sleep(interval * 15).await;
    assert_eq!(transport.registrations().len(), 4);
    wait_for_none(&transport, incarnation, 1);
    tokio::time::sleep(Duration::from_secs(95)).await;
    assert_eq!(transport.registrations().len(), 5);
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert_eq!(transport.registrations().len(), 6);
    assert_eq!(transport.registrations()[5], (Some(3), Some(incarnation)));
    let beats = beats_between_registrations(&transport);
    assert_eq!(beats[4], 10, "{beats:?}");

    // A heartbeat that fails while it waits does not count, nor start the count again: between
    // the two registrations, ten heartbeats that left it waiting and the one that failed.
    wait_for_none(&transport, incarnation, 1);
    tokio::time::sleep(Duration::from_secs(45)).await;
    transport.lose_next_reply(Heartbeat);
    tokio::time::sleep(Duration::from_secs(85)).await;
    assert_eq!(transport.registrations().len(), 8);
    let beats = beats_between_registrations(&transport);
    assert_eq!(beats[6], 11, "{beats:?}");
    let running = worker(&client, &room_id).await.unwrap();
    assert!(
        running.running && running.last_error.is_none(),
        "{running:?}"
    );
}

/// An ack of a heartbeat of `room_id` with a head far beyond that of the server.
fn beyond(correlation_id: CorrelationId, room_id: &RoomId) -> Vec<u8> {
    encode_message(&ServerMessage::HeartbeatAck {
        correlation_id,
        room_id: room_id.clone(),
        current_head_seq: SequenceNumber::new(1000),
        snapshot_wanted: false,
        active_snapshot_seq: None,
        schema_version: 1,
        schema_id: SchemaId::new("tasks").unwrap(),
    })
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_head_beyond_the_server_goes_with_the_next_reply_that_gives_the_real_one() {
    // An ack of a heartbeat with a head far beyond the server's, which the client cannot check,
    // raises the head it saw. The next heartbeat, sync or ack whose reply gives the real head
    // registers the replica again, which takes the head of `Registered` as the head it saw: a
    // later write ends with the decision of the replica, not as uncertain.
    for reply in [Heartbeat, Sync, Ack] {
        let server = TestServer::new();
        let room_id = server.create_room("room-a").await;
        let transport = server.transport();
        let client = client_with(&transport, ClientConfig::new("ana")).await;
        open_room(&client, &room_id).await.unwrap();
        server.commit(&room_id, 1..=2).await;
        let raise = || async {
            answer_next(&transport, Heartbeat, beyond);
            client.heartbeat(room_id.clone()).await.unwrap();
        };
        match reply {
            Heartbeat => {
                raise().await;
                client.heartbeat(room_id.clone()).await.unwrap();
            }
            Sync => {
                raise().await;
                client.sync(room_id.clone()).await.unwrap();
            }
            _ => {
                // The batch, sent before the head rose, is held until it rose: the ack after it
                // is sent with the head risen.
                let mut held = transport.hold_next_reply(Sync);
                let sync = tokio::spawn({
                    let (client, room_id) = (Arc::clone(&client), room_id.clone());
                    async move { client.sync(room_id).await }
                });
                held.replied().await;
                raise().await;
                held.release();
                sync.await.unwrap().unwrap();
            }
        }
        assert_eq!(transport.registrations().len(), 2, "{reply:?}");
        // A sync goes on once registered, and reaches the head.
        if reply != Heartbeat {
            assert_eq!(head(&client, &room_id).await, 2, "{reply:?}");
        }
        assert_eq!(
            client.write(room_id.clone(), insert_task(3)).await,
            Ok(()),
            "{reply:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_that_saw_the_head_beyond_the_server_keeps_it_once_the_replica_registers_again() {
    // Its commit goes once the head rose, and its `CommitAck` is held while a heartbeat gives the
    // real head: the sequence it gets is not after the head it saw, so its outcome is unknown.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let client = client_with(&transport, ClientConfig::new("ana")).await;
    open_room(&client, &room_id).await.unwrap();
    answer_next(&transport, Heartbeat, beyond);
    client.heartbeat(room_id.clone()).await.unwrap();
    let mut held = transport.hold_next_reply(ClientMessageKind::Commit);
    let write = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.write(room_id, insert_task(1)).await }
    });
    held.replied().await;
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(transport.registrations().len(), 2);
    held.release();
    assert_eq!(write.await.unwrap(), Err(ClientError::OutcomeUnknown));
    // A write after it ends with the decision of the replica.
    assert_eq!(client.write(room_id.clone(), insert_task(2)).await, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn the_ack_of_the_worker_compares_its_confirmation_with_the_head_seen_when_it_was_sent() {
    // In `manual`, a write moves the head with its catch-up, without an ack, and the ack of the
    // first heartbeat of the worker reports a head beyond the server's: the ack of the same step,
    // confirmed with the real head, registers the replica, without waiting for the next
    // heartbeat.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let config = ClientConfig {
        freshness: Freshness::Manual,
        ..every(Duration::from_secs(10))
    };
    let client = client_with(&transport, config).await;
    open_room(&client, &room_id).await.unwrap();
    server.commit(&room_id, 1..=2).await;
    client.write(room_id.clone(), insert_task(3)).await.unwrap();
    assert_eq!(head(&client, &room_id).await, 3);
    answer_next(&transport, Heartbeat, beyond);
    client.start_worker(room_id.clone()).await.unwrap();
    for _ in 0..20_000 {
        if !acks(&transport).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(acks(&transport), [3]);
    assert_eq!(transport.registrations().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn stopping_the_worker_acknowledges_once_its_sync_was_cut_and_ended() {
    // Whether the sync of the worker still runs shows in the references to the client, which it
    // holds while it runs.
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    server.commit(&room_id, [1]).await;
    synced_to(&client, &room_id, 1).await;
    // The next heartbeat acknowledges 1, and the batch of its sync is held; a write moves the
    // head past the one acknowledged meanwhile.
    let mut syncing = transport.hold_next_reply(Sync);
    server.commit(&room_id, [2]).await;
    syncing.replied().await;
    client.write(room_id.clone(), insert_task(3)).await.unwrap();
    assert_eq!(head(&client, &room_id).await, 3);
    let mut acking = transport.hold_next_reply(Ack);
    let stopping = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.stop_worker(room_id).await }
    });
    acking.replied().await;
    // The test, the stop, and the last ack: the sync ended before it.
    assert_eq!(Arc::strong_count(&client), 3);
    acking.release();
    assert_eq!(soon(stopping).await.unwrap(), Ok(()));
    assert_eq!(acks(&transport), [1, 3]);
    drop(syncing);
}

#[tokio::test(start_paused = true)]
async fn the_confirmation_of_an_ack_of_a_replica_discarded_meanwhile_is_not_taken_for_the_new() {
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    server.commit(&room_id, 1..=2).await;
    synced_to(&client, &room_id, 2).await;
    // The reply to the ack of the next heartbeat is held, and a heartbeat of the application
    // discards the replica for `RoomReset` meanwhile: the empty replica registers without a
    // cursor, which the server takes.
    let mut acking = transport.hold_next_reply(Ack);
    acking.replied().await;
    let (mut heartbeat, mut registration) = (true, true);
    transport.answer(move |message| {
        let (correlation_id, room_id) = request_of(message);
        let code = match message.kind() {
            Heartbeat if std::mem::take(&mut heartbeat) => ErrorCode::ClientNotRegistered,
            RegisterClient if std::mem::take(&mut registration) => ErrorCode::RoomReset,
            _ => return None,
        };
        Some(Ok(error_reply(correlation_id, &room_id, code)))
    });
    client.heartbeat(room_id.clone()).await.unwrap();
    let status = client.status(room_id.clone()).await.unwrap();
    assert_eq!((status.epoch, status.head_seq.get()), (2, 0));
    // The confirmation of the old head comes once the replica is another: the new one syncs
    // with the next heartbeat, and the one after acknowledges its head.
    acking.release();
    tokio::time::sleep(interval * 2 + Duration::from_millis(10)).await;
    assert_eq!(head(&client, &room_id).await, 2);
    assert_eq!(acks(&transport), [2, 2]);
}

#[tokio::test(start_paused = true)]
async fn the_worker_acknowledges_its_head_again_after_a_registration_that_lowered_the_cursor() {
    // A registration that the network delays, and whose caller was cut, reaches the server after
    // the worker acknowledged the head: the server takes its cursor, older. The room gets no
    // writes, and its log is pruned past that cursor: the tenth heartbeat acknowledges the head
    // anyway, so the server takes the reader as connected again, and designates it.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=10).await;
    let transport = server.transport();
    let reader = client_with(&transport, every(Duration::from_secs(10))).await;
    transport.slow_next(RegisterClient, Duration::from_secs(40));
    let opened = tokio::time::timeout(Duration::from_secs(1), open_room(&reader, &room_id));
    assert!(opened.await.is_err());
    reader.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(25)).await;
    assert_eq!(head(&reader, &room_id).await, 10);
    assert_eq!(acks(&transport), [10]);
    server.prune(&room_id, 11).await;
    // Past the delayed registration, the tenth heartbeat and the lease of the client that wrote.
    tokio::time::sleep(Duration::from_secs(120)).await;
    let acked = acks(&transport);
    assert!(
        acked.len() >= 2 && acked.iter().all(|seq| *seq == 10),
        "{acked:?}"
    );
    assert_eq!(leases(&server, &room_id).await, (0, 1));
    // A client behind the log gets the snapshot that the server designates the reader to upload.
    let lagging = member(&server, &room_id).await;
    lagging.client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(91)).await;
    assert_eq!(head(&lagging.client, &room_id).await, 10);
}

#[tokio::test(start_paused = true)]
async fn a_start_during_a_stop_of_the_worker_waits_for_it_and_a_close_after_waits_for_both() {
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    server.commit(&room_id, 1..=2).await;
    synced_to(&client, &room_id, 2).await;
    // The reply to the last ack of the worker that a stop stops is held.
    let mut held = transport.hold_next_reply(Ack);
    let stopping = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.stop_worker(room_id).await }
    });
    soon(held.replied()).await;
    // A start meanwhile waits for that stop, with its last ack: one worker at a time.
    let starting = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.start_worker(room_id).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!starting.is_finished());
    assert_eq!(worker(&client, &room_id).await, None);
    held.release();
    assert_eq!(soon(starting).await.unwrap(), Ok(()));
    assert_eq!(soon(stopping).await.unwrap(), Ok(()));
    assert!(worker(&client, &room_id).await.unwrap().running);
    // A close then stops the new worker, whose head the last ack of the old one acknowledged:
    // no ack more, and nothing after it.
    soon(client.close_room(room_id.clone())).await.unwrap();
    assert_eq!(acks(&transport), [2]);
    let closed = transport.sent().len();
    tokio::time::sleep(interval * 3).await;
    assert_eq!(transport.sent().len(), closed);
}

#[tokio::test(start_paused = true)]
async fn the_sync_of_the_worker_registers_after_a_head_beyond_the_server() {
    // The replica is at the head it acknowledged, so the step whose heartbeat reports a head
    // beyond the server's sends no ack: its sync, whose batch gives the real head, registers the
    // replica, and a write after it ends with the decision of the replica.
    let interval = Duration::from_secs(10);
    let (server, room_id, transport, _storage, client) =
        worker_in(Freshness::Periodic, interval).await;
    server.commit(&room_id, 1..=2).await;
    synced_to(&client, &room_id, 2).await;
    tokio::time::sleep(past(interval)).await;
    assert_eq!(acks(&transport), [2]);
    answer_next(&transport, Heartbeat, beyond);
    tokio::time::sleep(past(interval)).await;
    assert_eq!(acks(&transport), [2]);
    assert_eq!(transport.registrations().len(), 2);
    assert_eq!(client.write(room_id.clone(), insert_task(3)).await, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn a_batch_that_does_not_follow_its_cursor_changes_nothing_also_below_the_head_seen() {
    // After an ack of a heartbeat with a head beyond the server's, a batch with a gap (the
    // operation 2 without the 1) and a lower head is checked before its head is used: the sync
    // is `Network`, and registers nothing.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let client = client_with(&transport, ClientConfig::new("ana")).await;
    open_room(&client, &room_id).await.unwrap();
    let ops = server.commit(&room_id, 1..=2).await;
    answer_next(&transport, Heartbeat, beyond);
    client.heartbeat(room_id.clone()).await.unwrap();
    answer_next(&transport, Sync, move |correlation_id, room_id| {
        encode_message(&ServerMessage::SyncBatch {
            correlation_id,
            room_id: room_id.clone(),
            head_seq: SequenceNumber::new(2),
            ops: ops[1..].to_vec(),
            has_more: false,
            snapshot_wanted: false,
            schema_version: 1,
            schema_id: SchemaId::new("tasks").unwrap(),
        })
        .unwrap()
    });
    network(client.sync(room_id.clone()).await);
    assert_eq!(transport.registrations().len(), 1);
    assert_eq!(head(&client, &room_id).await, 0);
}

#[tokio::test(start_paused = true)]
async fn a_start_waiting_for_the_applier_while_a_stop_begins_waits_for_that_stop_too() {
    // A start that checks the room under the applier while a write holds it, and a stop that
    // begins meanwhile (from the delta review).
    let interval = Duration::from_secs(10);
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let engine = TestEngine::new(MemoryStorageEngine::new());
    let client = client_over(engine.clone(), &transport, every(interval)).await;
    open_room(&client, &room_id).await.unwrap();
    client.start_worker(room_id.clone()).await.unwrap();
    server.commit(&room_id, 1..=2).await;
    synced_to(&client, &room_id, 2).await;
    // A write holds the applier while its catch-up applies.
    let mut applying = engine.hold_next("apply_batch");
    let write = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.write(room_id, insert_task(3)).await }
    });
    soon(applying.replied()).await;
    let starting = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.start_worker(room_id).await }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    // The reply to the last ack of the worker that the stop stops is held.
    let mut acking = transport.hold_next_reply(Ack);
    let stopping = tokio::spawn({
        let (client, room_id) = (Arc::clone(&client), room_id.clone());
        async move { client.stop_worker(room_id).await }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    applying.release();
    soon(acking.replied()).await;
    // The start finds the stop once it has the applier, and waits for it.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!starting.is_finished());
    assert_eq!(worker(&client, &room_id).await, None);
    acking.release();
    assert_eq!(soon(stopping).await.unwrap(), Ok(()));
    assert_eq!(soon(starting).await.unwrap(), Ok(()));
    assert!(worker(&client, &room_id).await.unwrap().running);
    assert_eq!(soon(write).await.unwrap(), Ok(()));
    // The last ack of the old worker acknowledged the head that the write left.
    soon(client.close_room(room_id.clone())).await.unwrap();
    assert_eq!(acks(&transport), [3]);
}
