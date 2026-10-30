//! The heartbeats of the client and the download and the upload of snapshots, against the server
//! in process with a paused clock, whose rooms prune their log: a replica behind it catches up from
//! the active snapshot of the relay, which the client that the server designates uploads.

mod common;

use common::*;
use std::future::Future;
use std::path::Path;
use std::task::Poll;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::time::Instant;
use zemdb_client::{
    Client, ClientConfig, ClientError, FileMetadata, MemoryMetadata, TransportClient,
};
use zemdb_core::ClientMessageKind::{
    Ack, Commit, Heartbeat, RegisterClient, RequestSnapshotChunk, UploadSnapshotChunk,
};
use zemdb_core::*;
use zemdb_server::RoomCommand;
use zemdb_storage::{DiskStorageEngine, MemoryStorageEngine, StorageEngine, StorageError};

/// The schema `tasks` of the rooms of [`TestServer::create_room`], at its first version.
const TASKS: (&str, u64) = ("tasks", 1);

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
    blobs_schema()
        .to_operation_insert("blobs", &row, 1000)
        .unwrap()
}

/// [`tasks_schema`] with the nullable string column `tag`, unique.
fn tagged_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .nullable_column("tag", DataType::String)
        .unique("tasks_tag", ["tag"]);
    Schema::builder().table(table).build()
}

/// [`tagged_schema`] without its `UNIQUE`.
fn loose_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .nullable_column("tag", DataType::String);
    Schema::builder().table(table).build()
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

/// A server that prunes, with `room-a` holding the tasks 1 to 9 and its log pruned before 6: a
/// replica at 0 is behind it. With the operations of the room.
async fn pruned_room() -> (TestServer, RoomId, Vec<SequencedOperation>) {
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let ops = server.commit(&room_id, 1..=9).await;
    server.prune(&room_id, 6).await;
    (server, room_id, ops)
}

/// A server that prunes, with `room-b` holding the blobs 1 to 9, of 16 bytes, and its log pruned
/// before 6. With the operations of the room.
async fn pruned_blobs() -> (TestServer, RoomId, Vec<SequencedOperation>) {
    let server = TestServer::pruning();
    let room_id = server
        .create_room_with("room-b", "blobs", blobs_schema())
        .await;
    let ops = (1..=9).map(|id| insert_blob(id, 16));
    let ops = server.commit_ops(&room_id, ops).await;
    server.prune(&room_id, 6).await;
    (server, room_id, ops)
}

/// A server with `room-a` whose retained log has a gap: a client at 2, the tasks 3 to 6, and their
/// segment from 3 emptied, as damage that the server finds when it reads it. With the operations
/// of the room.
async fn gap() -> (
    TestServer,
    RoomId,
    Vec<SequencedOperation>,
    TestTransport,
    Client,
    TempDir,
) {
    let server = TestServer::with(|config| {
        config.ram_max_ops = 2;
        config.ram_ttl_secs = 0;
    });
    let room_id = server.create_room("room-a").await;
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    let mut ops = server.commit(&room_id, 1..=2).await;
    client.sync(room_id.clone()).await.unwrap();
    ops.extend(server.commit(&room_id, 3..=6).await);
    // The RAM window empties at the next disk maintenance, and the server reads the segment.
    tokio::time::sleep(Duration::from_secs(31)).await;
    let rooms = server.state.config.data_dir.join("rooms");
    let segments = rooms.join(room_id.as_str()).join("segments");
    for entry in std::fs::read_dir(&segments).unwrap() {
        let name = entry.unwrap().file_name();
        if name
            .to_string_lossy()
            .starts_with("segment_0000000000000003")
        {
            std::fs::write(segments.join(name), b"").unwrap();
        }
    }
    (server, room_id, ops, transport, client, dir)
}

/// The snapshot at `seq` of a replica of the room of `ops`, of [`pruned_blobs`], whose blobs 1 and
/// 2 hold 2 MiB and 64 KiB each: it takes two chunks.
async fn big_snapshot(room_id: &RoomId, ops: &[SequencedOperation], seq: usize) -> Vec<u8> {
    let mut ops = ops[..seq].to_vec();
    for (id, op) in (1..).zip(&mut ops[..2]) {
        *op = SequencedOperation::new(op.seq, insert_blob(id, (2 << 20) + (64 << 10)));
    }
    snapshot(room_id, blobs_schema(), &ops).await
}

fn hash(snapshot: &[u8]) -> [u8; 32] {
    ServerMessage::compute_snapshot_hash(snapshot)
}

/// A client over a disk engine in `dir` that opens `room_id` of `server`.
async fn joined(
    server: &TestServer,
    room_id: &RoomId,
    dir: &Path,
) -> (TestTransport, DiskStorageEngine, Client) {
    let (transport, storage) = (server.transport(), engine(dir));
    let client = open_client(dir, storage.clone(), &transport).await;
    open_room(&client, room_id).await.unwrap();
    (transport, storage, client)
}

/// A client with `config` over a disk engine in `dir`.
async fn client_with(dir: &Path, transport: &TestTransport, config: ClientConfig) -> Client {
    let metadata = FileMetadata::new(dir);
    let client = Client::open(config, engine(dir), metadata, transport.clone());
    client.await.unwrap()
}

/// The kinds of the messages sent through `transport`, in order.
fn kinds(transport: &TestTransport) -> Vec<ClientMessageKind> {
    transport.sent().iter().map(ClientMessage::kind).collect()
}

/// How many messages of `kind` went through `transport`.
fn count(transport: &TestTransport, kind: ClientMessageKind) -> usize {
    kinds(transport)
        .into_iter()
        .filter(|sent| *sent == kind)
        .count()
}

/// The chunks requested through `transport`, with their index and their anchor, in order.
fn chunks(transport: &TestTransport) -> Vec<(u32, Option<[u8; 32]>)> {
    let chunk = |message: ClientMessage| match message {
        ClientMessage::RequestSnapshotChunk {
            chunk_index,
            snapshot_hash,
            ..
        } => Some((chunk_index, snapshot_hash)),
        _ => None,
    };
    transport.sent().into_iter().filter_map(chunk).collect()
}

/// The chunk `index` of the active snapshot of `room_id`, by chunks of 4 MiB, as the snapshot
/// worker gets it.
async fn chunk(
    server: &TestServer,
    room_id: &RoomId,
    index: u32,
    hash: Option<[u8; 32]>,
) -> ServerMessage {
    let message = ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(0),
        room_id: room_id.clone(),
        chunk_index: index,
        chunk_size: 4 << 20,
        snapshot_hash: hash,
        room_incarnation: server.incarnation(room_id).await,
    };
    let reply = server.transport().send(&message, ADMIN_SECRET).await;
    decode_message(&reply.unwrap()).unwrap()
}

/// `chunk` as the reply to `message`, a request of a chunk, once `change` changes it.
fn reply_to(
    message: &ClientMessage,
    mut chunk: ServerMessage,
    change: impl FnOnce(&mut ServerMessage),
) -> Vec<u8> {
    if let ServerMessage::SnapshotChunk { correlation_id, .. } = &mut chunk {
        *correlation_id = request(message).0;
    }
    change(&mut chunk);
    encode_message(&chunk).unwrap()
}

/// Answers the next request of the chunk `index` through `transport` with `chunk`, once `change`
/// changes it.
fn answer_chunk(
    transport: &TestTransport,
    index: u32,
    chunk: ServerMessage,
    change: impl FnOnce(&mut ServerMessage) + Send + 'static,
) {
    let mut answer = Some((chunk, change));
    transport.answer(move |message| match message {
        ClientMessage::RequestSnapshotChunk { chunk_index, .. } if *chunk_index == index => {
            let (chunk, change) = answer.take()?;
            Some(Ok(reply_to(message, chunk, change)))
        }
        _ => None,
    });
}

/// A `HeartbeatAck` for the request `correlation_id` of `room_id` at the head `head`, which
/// announces the version `version` of the schema `schema_id`, and the snapshot at `snapshot`.
fn heartbeat_ack(
    correlation_id: CorrelationId,
    room_id: &RoomId,
    head: u64,
    (schema_id, version): (&str, u64),
    snapshot: Option<u64>,
) -> Vec<u8> {
    encode_message(&ServerMessage::HeartbeatAck {
        correlation_id,
        room_id: room_id.clone(),
        current_head_seq: SequenceNumber::new(head),
        snapshot_wanted: false,
        active_snapshot_seq: snapshot.map(SequenceNumber::new),
        schema_version: version,
        schema_id: SchemaId::new(schema_id).unwrap(),
    })
    .unwrap()
}

/// A `CommitAck` for the commit `message` that assigns it `seq`, without a catch-up and with
/// more.
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
        schema_id: SchemaId::new("tasks").unwrap(),
    })
    .unwrap()
}

/// A reply to the request `correlation_id` of `room_id`.
type Reply = fn(CorrelationId, &RoomId) -> Vec<u8>;

/// Makes the next heartbeat through `transport` fail its room closed: its ack announces another
/// schema, and the registration that follows gets one that does not evolve from the replica's.
fn fail_next_heartbeat_closed(transport: &TestTransport, room_incarnation: u128) {
    let retyped = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::Int);
    let retyped = Schema::builder().table(retyped).build();
    transport.answer(move |message| match message {
        ClientMessage::Heartbeat { .. } => {
            let (id, room) = request(message);
            Some(Ok(heartbeat_ack(id, &room, 3, ("other", 1), None)))
        }
        ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            ..
        } => Some(Ok(encode_message(&ServerMessage::Registered {
            correlation_id: *correlation_id,
            room_id: room_id.clone(),
            head_seq: SequenceNumber::new(3),
            tail_seq: SequenceNumber::new(1),
            schema_id: SchemaId::new("other").unwrap(),
            schema: retyped.clone(),
            active_snapshot_seq: None,
            room_incarnation,
        })
        .unwrap())),
        _ => None,
    });
}

/// A `Registered` of `room_incarnation` for the registration `message`, with the schema `tasks`, at
/// the head `tail` of a log retained from `tail`, which announces the snapshot at `snapshot`.
fn registered_at(
    message: &ClientMessage,
    room_incarnation: u128,
    tail: u64,
    snapshot: Option<u64>,
) -> Vec<u8> {
    let ClientMessage::RegisterClient {
        correlation_id,
        room_id,
        ..
    } = message
    else {
        panic!("{message:?}");
    };
    encode_message(&ServerMessage::Registered {
        correlation_id: *correlation_id,
        room_id: room_id.clone(),
        head_seq: SequenceNumber::new(tail),
        tail_seq: SequenceNumber::new(tail),
        schema_id: SchemaId::new("tasks").unwrap(),
        schema: tasks_schema(),
        active_snapshot_seq: snapshot.map(SequenceNumber::new),
        room_incarnation,
    })
    .unwrap()
}

/// Makes the next heartbeat through `transport` get `ClientNotRegistered`, and the registration
/// that follows, [`registered_at`] with `tail` and `snapshot`.
fn register_next_heartbeat(
    transport: &TestTransport,
    room_incarnation: u128,
    tail: u64,
    snapshot: Option<u64>,
) {
    let (mut heartbeat, mut registration) = (true, true);
    transport.answer(move |message| match message.kind() {
        Heartbeat if std::mem::take(&mut heartbeat) => {
            let (id, room) = request(message);
            Some(Ok(error_reply(id, &room, ErrorCode::ClientNotRegistered)))
        }
        RegisterClient if std::mem::take(&mut registration) => {
            Some(Ok(registered_at(message, room_incarnation, tail, snapshot)))
        }
        _ => None,
    });
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

/// How many clients of `room_id` the server takes as bootstrapping, behind its log.
async fn bootstrapping(server: &TestServer, room_id: &RoomId) -> usize {
    let metrics = server
        .state
        .room_manager
        .ask(room_id, |reply| RoomCommand::GetMetrics { reply });
    metrics.await.unwrap().unwrap().bootstrapping_clients
}

/// The cursor that the server keeps for `client_id` in `room_id`.
async fn cursor(server: &TestServer, room_id: &RoomId, client_id: ClientId) -> Option<u64> {
    let ask = |reply| RoomCommand::GetClientCursor { client_id, reply };
    let cursor = server.state.room_manager.ask(room_id, ask).await;
    cursor.unwrap().unwrap().map(|seq| seq.get())
}

/// The head of `room_id` of `client`.
async fn head(client: &Client, room_id: &RoomId) -> u64 {
    client.status(room_id.clone()).await.unwrap().head_seq.get()
}

fn network<T: std::fmt::Debug>(result: Result<T, ClientError>) {
    assert!(matches!(result, Err(ClientError::Network(_))), "{result:?}");
}

#[tokio::test(start_paused = true)]
async fn a_replica_behind_the_log_downloads_the_active_snapshot_and_syncs_from_it() {
    let (server, room_id, ops) = pruned_room().await;
    server
        .upload(
            &room_id,
            7,
            snapshot(&room_id, tasks_schema(), &ops[..7]).await,
        )
        .await;
    let dir = tempdir().unwrap();
    let (transport, storage, client) = joined(&server, &room_id, dir.path()).await;
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(9, 1, true)));
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    // One chunk, of the incarnation of the replica, and the sync goes on from the snapshot.
    assert_eq!(chunks(&transport), [(0, None)]);
    let incarnation = server.incarnation(&room_id).await;
    let incarnations = transport
        .sent()
        .into_iter()
        .filter_map(|message| match message {
            ClientMessage::RequestSnapshotChunk {
                room_incarnation, ..
            } => Some(room_incarnation),
            _ => None,
        });
    assert_eq!(incarnations.collect::<Vec<_>>(), [incarnation]);
    assert_eq!(requests(&transport), [("sync", 7), ("ack", 9)]);

    // Two syncs at once download it once.
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    let (first, second) = tokio::join!(client.sync(room_id.clone()), client.sync(room_id.clone()));
    assert_eq!((first, second), (Ok(()), Ok(())));
    assert_eq!(chunks(&transport), [(0, None)]);
    assert_eq!(head(&client, &room_id).await, 9);
}

#[tokio::test(start_paused = true)]
async fn without_a_usable_snapshot_the_replica_waits_for_a_heartbeat_that_announces_one() {
    let (server, room_id, ops) = pruned_room().await;
    let dir = tempdir().unwrap();
    let (transport, storage, client) = joined(&server, &room_id, dir.path()).await;
    // Without one, a sync sends nothing, and a heartbeat only itself.
    let sent = transport.sent().len();
    network(client.sync(room_id.clone()).await);
    assert_eq!(transport.sent().len(), sent);
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(kinds(&transport)[sent..], [Heartbeat]);

    // Uploaded, the sync still waits until a heartbeat announces it, which downloads it.
    let snapshot = snapshot(&room_id, tasks_schema(), &ops[..7]).await;
    server.upload(&room_id, 7, snapshot).await;
    let sent = transport.sent().len();
    network(client.sync(room_id.clone()).await);
    assert_eq!(transport.sent().len(), sent);
    client.heartbeat(room_id.clone()).await.unwrap();
    // It heartbeats again from the snapshot.
    let sent_now = kinds(&transport)[sent..].to_vec();
    assert_eq!(sent_now, [Heartbeat, RequestSnapshotChunk, Heartbeat]);
    assert_eq!(head(&client, &room_id).await, 7);
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(tasks(&storage, &room_id).await, [1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // So does a first chunk without a usable snapshot (`RoomNotFound`), also for a sync that
    // waited for that download.
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    answer_next(&transport, RequestSnapshotChunk, |id, room| {
        error_reply(id, room, ErrorCode::RoomNotFound)
    });
    let (first, second) = tokio::join!(client.sync(room_id.clone()), client.sync(room_id.clone()));
    network(first);
    network(second);
    assert_eq!(chunks(&transport), [(0, None)]);
    let sent = transport.sent().len();
    network(client.sync(room_id.clone()).await);
    assert_eq!(transport.sent().len(), sent);
    client.heartbeat(room_id.clone()).await.unwrap();
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(9, 1, true)));
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_replaced_during_the_download_is_downloaded_again_from_its_first_chunk() {
    let (server, room_id, ops) = pruned_blobs().await;
    let seven = big_snapshot(&room_id, &ops, 7).await;
    let eight = big_snapshot(&room_id, &ops, 8).await;
    server.upload(&room_id, 7, seven.clone()).await;
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    // The snapshot at 8 replaces it once its first chunk is served.
    let mut held = transport.hold_next_reply(RequestSnapshotChunk);
    let (server_ref, room, replacement) = (&server, room_id.clone(), eight.clone());
    let replace = async move {
        held.replied().await;
        server_ref.upload(&room, 8, replacement).await;
        held.release();
    };
    let (synced, ()) = tokio::join!(client.sync(room_id.clone()), replace);
    synced.unwrap();
    let (seven, eight) = (Some(hash(&seven)), Some(hash(&eight)));
    let chunks_sent = chunks(&transport);
    assert_eq!(chunks_sent, [(0, None), (1, seven), (0, None), (1, eight)]);
    assert_eq!(requests(&transport), [("sync", 8), ("ack", 9)]);

    // A second replacement in the same download is `Network`, and nothing is applied.
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    transport.answer(|message| match message {
        ClientMessage::RequestSnapshotChunk { chunk_index: 1, .. } => {
            let (id, room) = request(message);
            Some(Ok(error_reply(id, &room, ErrorCode::SnapshotSuperseded)))
        }
        _ => None,
    });
    network(client.sync(room_id.clone()).await);
    assert_eq!(
        chunks(&transport),
        [(0, None), (1, eight), (0, None), (1, eight)]
    );
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    transport.online();
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&client, &room_id).await, 9);
}

#[tokio::test(start_paused = true)]
async fn a_chunk_that_does_not_follow_the_first_is_not_used() {
    let (server, room_id, ops) = pruned_blobs().await;
    let snapshot = big_snapshot(&room_id, &ops, 7).await;
    server.upload(&room_id, 7, snapshot.clone()).await;
    let first = chunk(&server, &room_id, 0, None).await;
    let second = chunk(&server, &room_id, 1, Some(hash(&snapshot))).await;
    let dir = tempdir().unwrap();
    let (transport, storage, client) = joined(&server, &room_id, dir.path()).await;
    type Change = fn(&mut ServerMessage);
    let changes: [(&str, u32, Change); 10] = [
        ("another snapshot", 1, |chunk| {
            if let ServerMessage::SnapshotChunk { snapshot_hash, .. } = chunk {
                snapshot_hash[0] ^= 1;
            }
        }),
        ("another sequence", 1, |chunk| {
            if let ServerMessage::SnapshotChunk {
                snapshot_head_seq, ..
            } = chunk
            {
                *snapshot_head_seq = SequenceNumber::new(8);
            }
        }),
        ("another length", 1, |chunk| {
            if let ServerMessage::SnapshotChunk { total_bytes, .. } = chunk {
                *total_bytes += 1;
            }
        }),
        ("more chunks", 1, |chunk| {
            if let ServerMessage::SnapshotChunk { total_chunks, .. } = chunk {
                *total_chunks = 3;
            }
        }),
        ("another index", 1, |chunk| {
            if let ServerMessage::SnapshotChunk { chunk_index, .. } = chunk {
                *chunk_index = 0;
            }
        }),
        ("a shorter last chunk", 1, |chunk| {
            if let ServerMessage::SnapshotChunk { data, .. } = chunk {
                *data = data.slice(1..);
            }
        }),
        ("a shorter first chunk", 0, |chunk| {
            if let ServerMessage::SnapshotChunk { data, .. } = chunk {
                *data = data.slice(1..);
            }
        }),
        ("more chunks than its bytes take", 0, |chunk| {
            if let ServerMessage::SnapshotChunk { total_chunks, .. } = chunk {
                *total_chunks = 3;
            }
        }),
        ("another correlation", 1, |chunk| {
            if let ServerMessage::SnapshotChunk { correlation_id, .. } = chunk {
                *correlation_id = CorrelationId::new(correlation_id.get() + 1);
            }
        }),
        ("another room", 1, |chunk| {
            if let ServerMessage::SnapshotChunk { room_id, .. } = chunk {
                *room_id = RoomId::new("room-x").unwrap();
            }
        }),
    ];
    // An error of another request, which would leave the replica waiting for an announcement.
    let errors: [Reply; 2] = [
        |id, room| {
            let other = CorrelationId::new(id.get() + 1);
            error_reply(other, room, ErrorCode::RoomNotFound)
        },
        |id, _| error_reply(id, &RoomId::new("room-x").unwrap(), ErrorCode::RoomNotFound),
    ];
    for reply in errors {
        let sent = chunks(&transport).len();
        answer_next(&transport, RequestSnapshotChunk, reply);
        network(client.sync(room_id.clone()).await);
        assert_eq!(chunks(&transport)[sent..], [(0, None)]);
    }
    for (case, index, change) in changes {
        let sent = chunks(&transport).len();
        let chunk = if index == 0 { &first } else { &second };
        answer_chunk(&transport, index, chunk.clone(), change);
        network(client.sync(room_id.clone()).await);
        let requested: Vec<_> = chunks(&transport)[sent..].iter().map(|(i, _)| *i).collect();
        let expected: &[u32] = if index == 0 { &[0] } else { &[0, 1] };
        assert_eq!(requested, expected, "{case}");
        assert_eq!(
            client.status(room_id.clone()).await,
            Ok(status(0, 1, true)),
            "{case}"
        );
    }
    // Chunks that follow each other, of bytes without the hash they announce.
    let flipped = |chunk: &mut ServerMessage| {
        if let ServerMessage::SnapshotChunk { snapshot_hash, .. } = chunk {
            snapshot_hash[0] ^= 1;
        }
    };
    let mut answers = [(0, first.clone()), (1, second.clone())].into_iter();
    let mut next = answers.next();
    transport.answer(move |message| match (message, &next) {
        (ClientMessage::RequestSnapshotChunk { chunk_index, .. }, Some((index, chunk)))
            if chunk_index == index =>
        {
            let reply = reply_to(message, chunk.clone(), flipped);
            next = answers.next();
            Some(Ok(reply))
        }
        _ => None,
    });
    network(client.sync(room_id.clone()).await);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    let key = PrimaryKey::single(1);
    assert_eq!(storage.get(&room_id, "blobs", &key).await.unwrap(), None);

    // Those of the server are used.
    transport.online();
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&client, &room_id).await, 9);
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_larger_than_the_limit_of_the_client_is_not_downloaded() {
    let (server, room_id, ops) = pruned_room().await;
    let snapshot = snapshot(&room_id, tasks_schema(), &ops[..7]).await;
    let len = snapshot.len() as u64;
    server.upload(&room_id, 7, snapshot).await;
    for (max_snapshot_bytes, downloaded) in [(len - 1, false), (len, true)] {
        let dir = tempdir().unwrap();
        let transport = server.transport();
        let config = ClientConfig {
            max_snapshot_bytes,
            ..ClientConfig::new("ana")
        };
        let client = client_with(dir.path(), &transport, config).await;
        open_room(&client, &room_id).await.unwrap();
        let synced = client.sync(room_id.clone()).await;
        assert_eq!(chunks(&transport), [(0, None)]);
        match downloaded {
            true => assert_eq!(synced, Ok(())),
            false => {
                assert!(matches!(synced, Err(ClientError::Config(_))), "{synced:?}");
                assert_eq!(head(&client, &room_id).await, 0);
            }
        }
    }

    // Without a limit, a size that does not fit in memory is `Config` too, without aborting.
    let first = chunk(&server, &room_id, 0, None).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let config = ClientConfig {
        max_snapshot_bytes: u64::MAX,
        ..ClientConfig::new("ana")
    };
    let client = client_with(dir.path(), &transport, config).await;
    open_room(&client, &room_id).await.unwrap();
    answer_chunk(&transport, 0, first, |chunk| {
        if let ServerMessage::SnapshotChunk {
            total_chunks,
            total_bytes,
            data,
            ..
        } = chunk
        {
            (*total_chunks, *total_bytes) = (u32::MAX, u64::from(u32::MAX) << 22);
            *data = vec![0; 4 << 20].into();
        }
    });
    let synced = client.sync(room_id.clone()).await;
    assert!(matches!(synced, Err(ClientError::Config(_))), "{synced:?}");
    assert_eq!(chunks(&transport), [(0, None)]);
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_of_a_later_schema_is_installed_once_the_replica_takes_it_from_registered() {
    let (server, room_id, ops) = pruned_room().await;
    server
        .upload(
            &room_id,
            7,
            snapshot(&room_id, tasks_schema(), &ops[..7]).await,
        )
        .await;
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    // Before the download, the room takes the version 2, and a snapshot of it at 10 with a tag.
    let registry = &server.state.schema_registry;
    let tag = ColumnDef::new("tag", DataType::String).nullable(true);
    let unique = UniqueConstraint::new("tasks_tag", ["tag"]);
    let tasks_id = SchemaId::new("tasks").unwrap();
    let added = registry.add_column_with_constraints(&tasks_id, "tasks", tag, vec![unique], vec![]);
    added.unwrap();
    server
        .state
        .room_manager
        .reload_schema_for_rooms(&tasks_id)
        .await;
    let tagged = server.commit_ops(&room_id, [insert_tagged(10, "a")]).await;
    let all = [ops, tagged].concat();
    let version_2 = registry.get_schema(&tasks_id).unwrap();
    server
        .upload(
            &room_id,
            10,
            snapshot(&room_id, (*version_2).clone(), &all).await,
        )
        .await;

    // A heartbeat that already registered again, for `ClientNotRegistered` (with a `Registered` and
    // an ack of the version 1, as from before the change), cannot register again for it:
    // `Network`, and the snapshot is not dropped.
    let incarnation = server.incarnation(&room_id).await;
    let mut answered = 0;
    transport.answer(move |message| {
        let (id, room) = match message.kind() {
            Heartbeat | RegisterClient => request_of(message),
            _ => return None,
        };
        let reply = match (answered, message.kind()) {
            (0, Heartbeat) => error_reply(id, &room, ErrorCode::ClientNotRegistered),
            (1, RegisterClient) => registered_at(message, incarnation, 9, Some(10)),
            (2, Heartbeat) => heartbeat_ack(id, &room, 10, TASKS, Some(10)),
            _ => return None,
        };
        answered += 1;
        Some(Ok(reply))
    });
    network(client.heartbeat(room_id.clone()).await);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    transport.online();
    // It does not install with the version 1: the replica registers again and installs it with
    // the version 2, without downloading it again.
    let (chunks_before, registrations) =
        (chunks(&transport).len(), transport.registrations().len());
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(
        client.status(room_id.clone()).await,
        Ok(status(10, 1, true))
    );
    assert_eq!(transport.registrations().len(), registrations + 1);
    assert_eq!(chunks(&transport).len(), chunks_before + 1);
    // The replica holds the `UNIQUE` of the version 2.
    let written = client.write(room_id.clone(), insert_tagged(11, "a")).await;
    assert!(
        matches!(written, Err(ClientError::ConstraintViolation(_))),
        "{written:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_whose_rows_break_a_constraint_is_dropped_and_the_replica_kept() {
    let server = TestServer::pruning();
    let room_id = server
        .create_room_with("room-a", "tagged", tagged_schema())
        .await;
    let ops = (1..=9).map(|id| insert_tagged(id, &id.to_string()));
    let ops = server.commit_ops(&room_id, ops).await;
    server.prune(&room_id, 6).await;
    // The rows 1 and 2 of the snapshot have the same tag.
    let mut repeated = ops[..7].to_vec();
    repeated[1] = SequencedOperation::new(repeated[1].seq, insert_tagged(2, "1"));
    let repeated = snapshot(&room_id, loose_schema(), &repeated).await;
    server.upload(&room_id, 7, repeated).await;
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    // It registers again, as for a schema it did not have, and then drops the snapshot.
    network(client.sync(room_id.clone()).await);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    assert_eq!(transport.registrations().len(), 2);
    assert_eq!(chunks(&transport), [(0, None)]);
    // It waits until a heartbeat announces another snapshot: one that announces it again does
    // not download it again.
    let sent = transport.sent().len();
    network(client.sync(room_id.clone()).await);
    assert_eq!(transport.sent().len(), sent);
    for _ in 0..3 {
        client.heartbeat(room_id.clone()).await.unwrap();
    }
    assert_eq!(kinds(&transport)[sent..], [Heartbeat; 3]);
    server
        .upload(
            &room_id,
            8,
            snapshot(&room_id, tagged_schema(), &ops[..8]).await,
        )
        .await;
    client.heartbeat(room_id.clone()).await.unwrap();
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(9, 1, true)));
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_that_does_not_install_leaves_the_replica_as_it_was() {
    let (server, room_id, ops) = pruned_room().await;
    server
        .upload(
            &room_id,
            7,
            snapshot(&room_id, tasks_schema(), &ops[..7]).await,
        )
        .await;
    let first = chunk(&server, &room_id, 0, None).await;

    // At another head than its chunks announce (`SnapshotHeadMismatch`), it is dropped, without
    // registering again: a heartbeat announces it again.
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let transport = server.transport();
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    answer_chunk(&transport, 0, first, |chunk| {
        if let ServerMessage::SnapshotChunk {
            snapshot_head_seq, ..
        } = chunk
        {
            *snapshot_head_seq = SequenceNumber::new(6);
        }
    });
    network(client.sync(room_id.clone()).await);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    assert_eq!(transport.registrations().len(), 1);
    let sent = transport.sent().len();
    network(client.sync(room_id.clone()).await);
    assert_eq!(transport.sent().len(), sent);

    // An error of the storage installing it, after which the storage room may have changed,
    // closes and opens it again: the replica registers again and downloads it again.
    let failed = StorageError::RoomFailed {
        room_id: room_id.clone(),
        reason: "injected".to_string(),
    };
    for error in [StorageError::Io(std::io::Error::other("injected")), failed] {
        storage.fail_next("apply_snapshot", error);
        let installed = client.heartbeat(room_id.clone()).await;
        assert!(
            matches!(installed, Err(ClientError::Storage(_))),
            "{installed:?}"
        );
        assert_eq!(
            client.status(room_id.clone()).await,
            Ok(status(0, 1, false))
        );
    }
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(9, 1, true)));
    assert_eq!(chunks(&transport).len(), 4);

    // Nor is one that ends its download once the room failed closed.
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    let incarnation = server.incarnation(&room_id).await;
    let mut held = transport.hold_next_reply(RequestSnapshotChunk);
    let (client_ref, transport_ref, room) = (&client, &transport, room_id.clone());
    let fail = async move {
        held.replied().await;
        fail_next_heartbeat_closed(transport_ref, incarnation);
        let failed = client_ref.heartbeat(room.clone()).await;
        assert_eq!(failed, Err(ClientError::FailedClosed(room)));
        transport_ref.online();
        held.release();
    };
    let (synced, ()) = tokio::join!(client.sync(room_id.clone()), fail);
    assert_eq!(synced, Err(ClientError::FailedClosed(room_id.clone())));
    let failed = client.status(room_id.clone()).await.unwrap();
    assert_eq!((failed.head_seq.get(), failed.failed_closed), (0, true));

    // Behind the replica (`SnapshotBehind`), it is dropped, and the replica no longer waits.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    let ops = server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();
    server
        .upload(
            &room_id,
            2,
            snapshot(&room_id, tasks_schema(), &ops[..2]).await,
        )
        .await;
    server.commit(&room_id, [4]).await;
    // A registration that finds it behind the log, with the snapshot at 2 announced.
    register_next_heartbeat(&transport, server.incarnation(&room_id).await, 9, Some(2));
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(chunks(&transport), [(0, None)]);
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(4, 1, true)));
}

#[tokio::test(start_paused = true)]
async fn a_room_created_again_before_a_download_never_installs_its_snapshot() {
    // With a snapshot of other rows in the new room, its first chunk is `RoomReset`: the replica
    // registers again with its cursor, gets `RoomReset` and is discarded.
    let (server, room_id, ops) = pruned_room().await;
    server
        .upload(
            &room_id,
            7,
            snapshot(&room_id, tasks_schema(), &ops[..7]).await,
        )
        .await;
    let old = server.incarnation(&room_id).await;
    let dir = tempdir().unwrap();
    let (transport, storage, client) = joined(&server, &room_id, dir.path()).await;
    server.recreate_room(&room_id).await;
    server.commit(&room_id, [7, 8]).await;
    // A snapshot at 2 of the tasks 1 and 2, which the relay takes: the new room keeps its log.
    let other =
        [1, 2].map(|id| SequencedOperation::new(SequenceNumber::new(id), insert_task(id as i64)));
    let other = snapshot(&room_id, tasks_schema(), &other).await;
    server.upload(&room_id, 2, other).await;
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(2, 2, true)));
    assert_eq!(tasks(&storage, &room_id).await, [7, 8]);
    assert_eq!(chunks(&transport), [(0, None)]);
    let cursors: Vec<_> = transport.registrations();
    assert_eq!(cursors, [(None, None), (Some(0), Some(old)), (None, None)]);

    // Without a snapshot in the new room, it is `RoomNotFound`: the replica waits, and its
    // heartbeat gets `ClientNotRegistered`, registers again and gets `RoomReset`.
    let (server, room_id, ops) = pruned_room().await;
    server
        .upload(
            &room_id,
            7,
            snapshot(&room_id, tasks_schema(), &ops[..7]).await,
        )
        .await;
    let dir = tempdir().unwrap();
    let (transport, storage, client) = joined(&server, &room_id, dir.path()).await;
    server.recreate_room(&room_id).await;
    server.commit(&room_id, [7, 8]).await;
    network(client.sync(room_id.clone()).await);
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    client.heartbeat(room_id.clone()).await.unwrap();
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(2, 2, true)));
    assert_eq!(tasks(&storage, &room_id).await, [7, 8]);
    assert_eq!(chunks(&transport), [(0, None)]);

    // A heartbeat that gets `RoomReset` to a chunk registers again too: here the room is the
    // same, and the heartbeat downloads it again.
    let (server, room_id, ops) = pruned_room().await;
    let snapshot = snapshot(&room_id, tasks_schema(), &ops[..7]).await;
    server.upload(&room_id, 7, snapshot).await;
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    answer_next(&transport, RequestSnapshotChunk, |id, room| {
        error_reply(id, room, ErrorCode::RoomReset)
    });
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(transport.registrations().len(), 2);
    assert_eq!(chunks(&transport).len(), 2);
    assert_eq!(head(&client, &room_id).await, 7);

    // Against a server that answers `RoomReset` to every chunk and registers the replica all the
    // same, a sync and a heartbeat register again once each.
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    transport.answer_every(RequestSnapshotChunk, |message| {
        let (id, room) = request(message);
        error_reply(id, &room, ErrorCode::RoomReset)
    });
    network(client.sync(room_id.clone()).await);
    assert_eq!(chunks(&transport).len(), 2);
    network(client.heartbeat(room_id.clone()).await);
    assert_eq!(chunks(&transport).len(), 4);
}

#[tokio::test(start_paused = true)]
async fn a_room_created_again_during_a_download_never_installs_its_snapshot() {
    // Between two chunks: the anchored one is `SnapshotSuperseded`, and the first one again,
    // `RoomReset`.
    let (server, room_id, ops) = pruned_blobs().await;
    let seven = big_snapshot(&room_id, &ops, 7).await;
    server.upload(&room_id, 7, seven.clone()).await;
    let dir = tempdir().unwrap();
    let (transport, storage, client) = joined(&server, &room_id, dir.path()).await;
    let mut held = transport.hold_next_reply(RequestSnapshotChunk);
    let (server_ref, room) = (&server, room_id.clone());
    let recreate = async move {
        held.replied().await;
        server_ref
            .state
            .room_manager
            .delete_room(&room)
            .await
            .unwrap();
        let schema = blobs_schema();
        server_ref
            .create_room_with(room.as_str(), "blobs", schema)
            .await;
        let new_ops = server_ref.commit_ops(&room, [insert_blob(5, 16)]).await;
        let other = snapshot(&room, blobs_schema(), &new_ops).await;
        server_ref.upload(&room, 1, other).await;
        held.release();
    };
    let (synced, ()) = tokio::join!(client.sync(room_id.clone()), recreate);
    synced.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(1, 2, true)));
    let seven = Some(hash(&seven));
    assert_eq!(chunks(&transport), [(0, None), (1, seven), (0, None)]);
    for (id, held) in [(1, false), (5, true)] {
        let row = storage
            .get(&room_id, "blobs", &PrimaryKey::single(id))
            .await;
        assert_eq!(row.unwrap().is_some(), held);
    }

    // A download that ends after a discard is not installed.
    let (server, room_id, ops) = pruned_room().await;
    server
        .upload(
            &room_id,
            7,
            snapshot(&room_id, tasks_schema(), &ops[..7]).await,
        )
        .await;
    let dir = tempdir().unwrap();
    let (transport, storage, client) = joined(&server, &room_id, dir.path()).await;
    let mut held = transport.hold_next_reply(RequestSnapshotChunk);
    let (server_ref, client_ref, room) = (&server, &client, room_id.clone());
    let discard = async move {
        held.replied().await;
        server_ref.recreate_room(&room).await;
        server_ref.commit(&room, [8]).await;
        // The heartbeat gets `ClientNotRegistered`, and its registration `RoomReset`.
        client_ref.heartbeat(room.clone()).await.unwrap();
        held.release();
    };
    let (synced, ()) = tokio::join!(client.sync(room_id.clone()), discard);
    synced.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(1, 2, true)));
    assert_eq!(tasks(&storage, &room_id).await, [8]);
    assert_eq!(chunks(&transport), [(0, None)]);
}

#[tokio::test(start_paused = true)]
async fn a_write_whose_sequence_the_installed_snapshot_covers_is_an_unknown_outcome() {
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let config = ClientConfig {
        write_timeout: Duration::from_secs(600),
        ..ClientConfig::new("ana")
    };
    let client = client_with(dir.path(), &transport, config).await;
    open_room(&client, &room_id).await.unwrap();
    let (server, transport, client) = (&server, &transport, &client);
    // The first write gets the sequence 1, which a sync applies, with its rejection, before its
    // `CommitAck` comes; the second, the sequence 2, which the replica never applies one by one.
    let mut first_held = transport.hold_next_reply(Commit);
    let mut second_held = transport.hold_next_reply(Commit);
    let first = client.write(room_id.clone(), update_task(8));
    let room = room_id.clone();
    let rest = async move {
        first_held.replied().await;
        client.sync(room.clone()).await.unwrap();
        let second = client.write(room.clone(), update_task(9));
        let room_ref = room.clone();
        let rest = async move {
            second_held.replied().await;
            let mut ops = vec![
                SequencedOperation::new(SequenceNumber::new(1), update_task(8)),
                SequencedOperation::new(SequenceNumber::new(2), update_task(9)),
            ];
            ops.extend(server.commit(&room_ref, 3..=9).await);
            server.prune(&room_ref, 6).await;
            let snapshot = snapshot(&room_ref, tasks_schema(), &ops[..7]).await;
            server.upload(&room_ref, 7, snapshot).await;
            // A third write, assigned the sequence 6, which it waits for: its sync gets
            // `BehindCompaction`, registers again, and installs the snapshot.
            transport.answer_next(Commit, |message| commit_ack(message, 6));
            let third = client.write(room_ref.clone(), insert_task(1)).await;
            assert_eq!(third, Err(ClientError::OutcomeUnknown));
            first_held.release();
            second_held.release();
        };
        tokio::join!(second, rest).0
    };
    let (first, second) = tokio::join!(first, rest);
    // The first keeps the decision of the replica, and the second's is unknown.
    assert_eq!(first, Err(ClientError::RowMissing));
    assert_eq!(second, Err(ClientError::OutcomeUnknown));
    assert_eq!(head(client, &room_id).await, 9);
    let status_now = client.status(room_id.clone()).await.unwrap();
    assert_eq!((status_now.writes_in_flight, status_now.retained), (0, 0));

    // Without a snapshot yet, a write with its `CommitAck` that waits learns of one with its
    // heartbeats.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let dir = tempdir().unwrap();
    let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
    let ops = server.commit(&room_id, 1..=9).await;
    server.prune(&room_id, 6).await;
    transport.answer_next(Commit, |message| commit_ack(message, 6));
    let upload = async {
        for _ in 0..60_000 {
            if count(&transport, Heartbeat) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let snapshot = snapshot(&room_id, tasks_schema(), &ops[..7]).await;
        server.upload(&room_id, 7, snapshot).await;
    };
    let written = client.write(room_id.clone(), insert_task(1));
    let written = tokio::time::timeout(Duration::from_secs(600), written);
    let (written, ()) = tokio::join!(written, upload);
    let written = written.expect("the write never learned of the snapshot");
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
}

#[tokio::test(start_paused = true)]
async fn a_commit_behind_the_log_catches_up_and_ends_with_the_decision_of_the_replica() {
    let server = TestServer::pruning();
    let mut clients = Vec::new();
    for room in ["room-a", "room-b", "room-c", "room-d"] {
        let room_id = server.create_room(room).await;
        let dir = tempdir().unwrap();
        let (transport, storage, client) = joined(&server, &room_id, dir.path()).await;
        let ops = server.commit(&room_id, 1..=9).await;
        server.prune(&room_id, 6).await;
        clients.push((room_id, ops, transport, storage, client, dir));
    }

    // With the snapshot uploaded: `BehindCompaction`, and the retry after installing it.
    let (room_id, ops, transport, storage, client, _dir) = &clients[0];
    server
        .upload(
            room_id,
            7,
            snapshot(room_id, tasks_schema(), &ops[..7]).await,
        )
        .await;
    assert_eq!(client.write(room_id.clone(), update_task(1)).await, Ok(()));
    let sent = commits(transport);
    assert_eq!(
        sent.iter().map(|(_, cursor)| *cursor).collect::<Vec<_>>(),
        [0, 7]
    );
    assert_eq!(sent[0].0, sent[1].0);
    assert_eq!(
        client.status(room_id.clone()).await,
        Ok(status(10, 1, true))
    );
    assert_eq!(tasks(storage, room_id).await, [1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // Uploaded while the write waits for it: its heartbeats find it.
    let (room_id, ops, transport, _, client, _dir) = &clients[1];
    let upload = async {
        while count(transport, Heartbeat) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        server
            .upload(
                room_id,
                7,
                snapshot(room_id, tasks_schema(), &ops[..7]).await,
            )
            .await;
    };
    let (written, ()) = tokio::join!(client.write(room_id.clone(), update_task(1)), upload);
    assert_eq!(written, Ok(()));
    assert_eq!(head(client, room_id).await, 10);

    // Never uploaded: the write had no effect.
    let (room_id, _, transport, _, client, _dir) = &clients[2];
    network(client.write(room_id.clone(), update_task(1)).await);
    assert_eq!(commits(transport).len(), 1);
    // With a backoff between them.
    let heartbeats = count(transport, Heartbeat);
    assert!((3..60).contains(&heartbeats), "{heartbeats}");

    // A download still in progress at the deadline is cut: the write had no effect.
    let (room_id, ops, transport, _, client, _dir) = &clients[3];
    let snapshot_now = snapshot(room_id, tasks_schema(), &ops[..7]).await;
    server.upload(room_id, 7, snapshot_now).await;
    let _held = transport.hold_next_reply(RequestSnapshotChunk);
    let start = Instant::now();
    let written = client.write(room_id.clone(), update_task(1));
    let written = tokio::time::timeout(Duration::from_secs(60), written).await;
    network(written.expect("the write did not end at its deadline"));
    assert_eq!(start.elapsed(), Duration::from_secs(30));
    assert_eq!(commits(transport).len(), 1);

    // A `BehindCompaction` that reaches a commit once the replica moved past its cursor was
    // delayed: the write retries without registering again.
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    transport.answer_next(Commit, |message| {
        let (id, room) = request(message);
        error_reply(id, &room, ErrorCode::BehindCompaction)
    });
    let mut held = transport.hold_next_reply(Commit);
    let (server_ref, client_ref, room) = (&server, &client, room_id.clone());
    let meanwhile = async move {
        held.replied().await;
        server_ref.commit(&room, [2]).await;
        client_ref.sync(room).await.unwrap();
        held.release();
    };
    let (written, ()) = tokio::join!(client.write(room_id.clone(), insert_task(1)), meanwhile);
    assert_eq!(written, Ok(()));
    assert_eq!(transport.registrations().len(), 1);
    let cursors: Vec<_> = commits(&transport)
        .iter()
        .map(|(_, cursor)| *cursor)
        .collect();
    assert_eq!(cursors, [0, 1]);

    // An error of the storage reading the head after `BehindCompaction` ends the write.
    let dir = tempdir().unwrap();
    let storage = TestEngine::new(engine(dir.path()));
    let (_server, room_id, transport, storage, client) = setup(dir.path(), storage).await;
    let failing = storage.clone();
    transport.answer_next(Commit, move |message| {
        failing.fail_next("get_head_seq", StorageError::Other("injected".to_string()));
        let (id, room) = request(message);
        error_reply(id, &room, ErrorCode::BehindCompaction)
    });
    let written = client.write(room_id.clone(), insert_task(1)).await;
    assert!(
        matches!(written, Err(ClientError::Storage(_))),
        "{written:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_heartbeat_takes_the_head_the_schema_and_the_snapshot_its_ack_announces() {
    let dir = tempdir().unwrap();
    let (server, room_id, transport, _, client) = setup(dir.path(), engine(dir.path())).await;
    server.commit(&room_id, 1..=3).await;
    client.sync(room_id.clone()).await.unwrap();
    let sent = transport.sent().len();
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(kinds(&transport)[sent..], [Heartbeat]);

    // `ClientNotRegistered`, a head below that of the replica, another schema or a later version
    // of its own register it again with its cursor, and it heartbeats again.
    let replies: [(&str, Reply); 4] = [
        ("not registered", |id, room| {
            error_reply(id, room, ErrorCode::ClientNotRegistered)
        }),
        ("a head below", |id, room| {
            heartbeat_ack(id, room, 2, TASKS, None)
        }),
        ("another schema", |id, room| {
            heartbeat_ack(id, room, 3, ("other", 1), None)
        }),
        ("a later version", |id, room| {
            heartbeat_ack(id, room, 3, ("tasks", 2), None)
        }),
    ];
    for (case, reply) in replies {
        let sent = transport.sent().len();
        let registrations = transport.registrations().len();
        answer_next(&transport, Heartbeat, reply);
        client.heartbeat(room_id.clone()).await.unwrap();
        assert_eq!(
            kinds(&transport)[sent..],
            [Heartbeat, RegisterClient, Heartbeat],
            "{case}"
        );
        let registration = transport.registrations()[registrations];
        let incarnation = server.incarnation(&room_id).await;
        assert_eq!(registration, (Some(3), Some(incarnation)), "{case}");
    }
    // A second time in a heartbeat, the server contradicts itself: with a head below, or with
    // another schema.
    for (head, schema) in [(2, TASKS), (3, ("other", 1))] {
        transport.answer_every(Heartbeat, move |message| {
            let (id, room) = request(message);
            heartbeat_ack(id, &room, head, schema, None)
        });
        network(client.heartbeat(room_id.clone()).await);
    }
    transport.online();

    // A reply that does not answer the heartbeat changes nothing.
    let replies: [Reply; 4] = [
        |id, room| heartbeat_ack(CorrelationId::new(id.get() + 1), room, 2, TASKS, None),
        |id, _| heartbeat_ack(id, &RoomId::new("room-x").unwrap(), 2, TASKS, None),
        |id, room| error_reply(id, room, ErrorCode::Unavailable),
        |_, _| vec![0xff; 8],
    ];
    for reply in replies {
        let sent = transport.sent().len();
        answer_next(&transport, Heartbeat, reply);
        network(client.heartbeat(room_id.clone()).await);
        assert_eq!(kinds(&transport)[sent..], [Heartbeat]);
    }
    assert_eq!(client.status(room_id.clone()).await, Ok(status(3, 1, true)));

    // The head it reports is seen: a write assigned a sequence at or below it is an unknown
    // outcome.
    answer_next(&transport, Heartbeat, |id, room| {
        heartbeat_ack(id, room, 9, TASKS, None)
    });
    client.heartbeat(room_id.clone()).await.unwrap();
    transport.answer_next(Commit, |message| commit_ack(message, 5));
    let written = client.write(room_id.clone(), insert_task(4)).await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));

    // A registration that finds the replica behind the log leaves it waiting for a snapshot, and
    // one that finds it inside the log ends its wait.
    register_next_heartbeat(&transport, server.incarnation(&room_id).await, 9, None);
    client.heartbeat(room_id.clone()).await.unwrap();
    let sent = transport.sent().len();
    network(client.sync(room_id.clone()).await);
    assert_eq!(transport.sent().len(), sent);
    answer_next(&transport, Heartbeat, |id, room| {
        error_reply(id, room, ErrorCode::ClientNotRegistered)
    });
    client.heartbeat(room_id.clone()).await.unwrap();
    let sent = transport.sent().len();
    client.sync(room_id.clone()).await.unwrap();
    let synced = kinds(&transport)[sent..].to_vec();
    assert_eq!(synced, [ClientMessageKind::Sync, ClientMessageKind::Ack]);

    // A room that fails closed sends no heartbeat.
    fail_next_heartbeat_closed(&transport, server.incarnation(&room_id).await);
    let failed = client.heartbeat(room_id.clone()).await;
    assert_eq!(failed, Err(ClientError::FailedClosed(room_id.clone())));
    assert_eq!(deregistrations(&transport), 1);
    let sent = transport.sent().len();
    let failed = client.heartbeat(room_id.clone()).await;
    assert_eq!(failed, Err(ClientError::FailedClosed(room_id.clone())));
    assert_eq!(transport.sent().len(), sent);

    // A room that is not registered registers first.
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = open_client(dir.path(), engine(dir.path()), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    client.close_room(room_id.clone()).await.unwrap();
    transport.offline();
    open_room(&client, &room_id).await.unwrap();
    transport.online();
    let sent = transport.sent().len();
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(kinds(&transport)[sent..], [RegisterClient, Heartbeat]);
    let closed = RoomId::new("room-z").unwrap();
    assert_eq!(
        client.heartbeat(closed.clone()).await,
        Err(ClientError::RoomClosed(closed))
    );
}

#[tokio::test(start_paused = true)]
async fn a_replica_behind_a_cursor_the_server_holds_ahead_registers_and_gets_a_snapshot() {
    // The ack of the head 9 reaches the server once its memory room opened again at 0, and
    // registered with the cursor 0: it leaves the server a cursor ahead of the replica (INV-K5).
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let ops = server.commit(&room_id, 1..=9).await;
    let transport = server.transport();
    let storage = MemoryStorageEngine::new();
    let opened = Client::open(
        ClientConfig::new("ana"),
        storage,
        MemoryMetadata::default(),
        transport.clone(),
    );
    let client = opened.await.unwrap();
    open_room(&client, &room_id).await.unwrap();
    transport.slow_next(Ack, Duration::from_secs(5));
    let cut = tokio::time::timeout(Duration::from_secs(1), client.sync(room_id.clone())).await;
    assert!(cut.is_err());
    client.close_room(room_id.clone()).await.unwrap();
    open_room(&client, &room_id).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(0, 1, true)));
    server.prune(&room_id, 6).await;
    assert_eq!(cursor(&server, &room_id, client.client_id()).await, Some(9));
    // `BehindCompaction` registers the replica with its cursor: the server takes it, and asks for
    // a snapshot, which a heartbeat finds.
    network(client.sync(room_id.clone()).await);
    assert_eq!(cursor(&server, &room_id, client.client_id()).await, Some(0));
    assert_eq!(bootstrapping(&server, &room_id).await, 1);
    let snapshot = snapshot(&room_id, tasks_schema(), &ops[..7]).await;
    server.upload(&room_id, 7, snapshot).await;
    client.heartbeat(room_id.clone()).await.unwrap();
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&client, &room_id).await, 9);
}

#[tokio::test(start_paused = true)]
async fn a_replica_that_runs_into_a_gap_of_the_log_waits_for_the_snapshot_above_it() {
    // The server answers `BehindCompaction` to a cursor inside the log, and asks for a snapshot
    // above the gap: once a registration found the replica inside the log, it waits for it.
    let (server, room_id, ops, transport, client, _dir) = gap().await;
    network(client.sync(room_id.clone()).await);
    assert_eq!(transport.registrations().len(), 2);
    let sent = transport.sent().len();
    network(client.sync(room_id.clone()).await);
    assert_eq!(transport.sent().len(), sent);
    let snapshot_now = snapshot(&room_id, tasks_schema(), &ops[..6]).await;
    server.upload(&room_id, 6, snapshot_now).await;
    client.heartbeat(room_id.clone()).await.unwrap();
    client.sync(room_id.clone()).await.unwrap();
    assert_eq!(client.status(room_id.clone()).await, Ok(status(6, 1, true)));

    // So does a write whose sync runs into it, which then ends with the decision of the replica.
    let (server, room_id, ops, transport, client, _dir) = gap().await;
    let (server_ref, room) = (&server, room_id.clone());
    let upload = async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let snapshot_now = snapshot(&room, tasks_schema(), &ops[..6]).await;
        server_ref.upload(&room, 6, snapshot_now).await;
    };
    let written = client.write(room_id.clone(), insert_task(7));
    let written = tokio::time::timeout(Duration::from_secs(600), written);
    let (written, ()) = tokio::join!(written, upload);
    assert_eq!(written.expect("the write never ended"), Ok(()));
    assert!(transport.registrations().len() < 5);
}

#[tokio::test(start_paused = true)]
async fn a_download_cut_at_any_point_installs_nothing_halfway() {
    let (server, room_id, ops) = pruned_room().await;
    server
        .upload(
            &room_id,
            7,
            snapshot(&room_id, tasks_schema(), &ops[..7]).await,
        )
        .await;
    for polls in 1.. {
        let dir = tempdir().unwrap();
        let (transport, _, client) = joined(&server, &room_id, dir.path()).await;
        transport.slow(Duration::from_millis(2));
        let mut synced = Box::pin(client.sync(room_id.clone()));
        let mut finished = false;
        for _ in 0..polls {
            let polled = std::future::poll_fn(|cx| Poll::Ready(synced.as_mut().poll(cx)));
            if let Poll::Ready(synced) = polled.await {
                synced.unwrap();
                finished = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        drop(synced);
        // The replica is at 0 or at the snapshot, and the next sync catches up.
        let head_now = head(&client, &room_id).await;
        assert!([0, 7, 9].contains(&head_now), "{polls}: {head_now}");
        client.sync(room_id.clone()).await.unwrap();
        assert_eq!(head(&client, &room_id).await, 9, "{polls}");
        client.shutdown().await.unwrap();
        if finished {
            break;
        }
    }
}

/// The schema `blobs` of [`blobs_schema`], at its first version.
const BLOBS: (&str, u64) = ("blobs", 1);

/// A client of a room over a disk engine in a directory of its own.
struct Member {
    transport: TestTransport,
    storage: DiskStorageEngine,
    client: Client,
    _dir: TempDir,
}

/// A client of `server` that opens `room_id` ([`joined`]).
async fn member(server: &TestServer, room_id: &RoomId) -> Member {
    let dir = tempdir().unwrap();
    let (transport, storage, client) = joined(server, room_id, dir.path()).await;
    Member {
        transport,
        storage,
        client,
        _dir: dir,
    }
}

/// The inserts of the blobs 1 to 9: the first two of 2 MiB and 64 KiB, and the others of 16 bytes,
/// so that a snapshot of them takes two chunks.
fn blobs() -> impl Iterator<Item = Operation> {
    (1..=9).map(|id| insert_blob(id, if id <= 2 { (2 << 20) + (64 << 10) } else { 16 }))
}

/// A server that prunes, with `room_id` of `schema` holding `ops` (9 of them) and its log pruned
/// before 6, and two members: one at 9, and one at 0, behind the log, for which the server
/// designates the first to upload a snapshot. With the operations of the room.
async fn designated_with(
    room_id: &str,
    (schema_id, schema): (&str, Schema),
    ops: impl IntoIterator<Item = Operation>,
) -> (TestServer, RoomId, Vec<SequencedOperation>, Member, Member) {
    let server = TestServer::pruning();
    let room_id = server.create_room_with(room_id, schema_id, schema).await;
    let designee = member(&server, &room_id).await;
    let ops = server.commit_ops(&room_id, ops).await;
    designee.client.sync(room_id.clone()).await.unwrap();
    server.prune(&room_id, 6).await;
    let lagging = member(&server, &room_id).await;
    (server, room_id, ops, designee, lagging)
}

/// [`designated_with`] `room-b` of [`blobs`]: the snapshot of the designated member takes two
/// chunks.
async fn designated() -> (TestServer, RoomId, Vec<SequencedOperation>, Member, Member) {
    designated_with("room-b", ("blobs", blobs_schema()), blobs()).await
}

/// A server with `room-b` of [`blobs`], which keeps its log, and a member of it at 9: its server
/// asks for no snapshot.
async fn blobs_room() -> (TestServer, RoomId, Member) {
    let server = TestServer::new();
    let room_id = server
        .create_room_with("room-b", "blobs", blobs_schema())
        .await;
    server.commit_ops(&room_id, blobs()).await;
    let member = member(&server, &room_id).await;
    member.client.sync(room_id.clone()).await.unwrap();
    (server, room_id, member)
}

/// A chunk uploaded: its index, the number of chunks and its length, and the sequence, the size,
/// the hash and the incarnation of its snapshot.
type Upload = (u32, u32, usize, (u64, u64, [u8; 32], u128));

/// The chunks uploaded through `transport`, in order.
fn uploads(transport: &TestTransport) -> Vec<Upload> {
    let upload = |message: ClientMessage| match message {
        ClientMessage::UploadSnapshotChunk {
            snapshot_head_seq,
            chunk_index,
            total_chunks,
            total_bytes,
            snapshot_hash,
            data,
            room_incarnation,
            ..
        } => {
            let snapshot = (
                snapshot_head_seq.get(),
                total_bytes,
                snapshot_hash,
                room_incarnation,
            );
            Some((chunk_index, total_chunks, data.len(), snapshot))
        }
        _ => None,
    };
    transport.sent().into_iter().filter_map(upload).collect()
}

/// The indexes of the chunks uploaded through `transport`, in order.
fn uploaded(transport: &TestTransport) -> Vec<u32> {
    uploads(transport)
        .into_iter()
        .map(|(index, ..)| index)
        .collect()
}

/// The active snapshot of `room_id` in the relay of `server`, by its sequence.
fn active(server: &TestServer, room_id: &RoomId) -> Option<u64> {
    let active = server.state.snapshot_relay.active_snapshot_seq(room_id);
    active.map(|seq| seq.get())
}

/// A `HeartbeatAck` for the heartbeat `message` at the head `head`, which announces `schema` and no
/// snapshot, and asks its client for one.
fn wanted(message: &ClientMessage, head: u64, (schema_id, version): (&str, u64)) -> Vec<u8> {
    let (correlation_id, room_id) = request(message);
    encode_message(&ServerMessage::HeartbeatAck {
        correlation_id,
        room_id,
        current_head_seq: SequenceNumber::new(head),
        snapshot_wanted: true,
        active_snapshot_seq: None,
        schema_version: version,
        schema_id: SchemaId::new(schema_id).unwrap(),
    })
    .unwrap()
}

/// A `SyncBatch` for the sync `message` of a room at 9, without operations, which
/// announces the schema `schema_id` and asks its client for a snapshot if `snapshot_wanted`.
fn sync_batch(message: &ClientMessage, schema_id: &str, snapshot_wanted: bool) -> Vec<u8> {
    let (correlation_id, room_id) = request(message);
    encode_message(&ServerMessage::SyncBatch {
        correlation_id,
        room_id,
        head_seq: SequenceNumber::new(9),
        ops: Vec::new(),
        has_more: false,
        snapshot_wanted,
        schema_version: 1,
        schema_id: SchemaId::new(schema_id).unwrap(),
    })
    .unwrap()
}

/// The acknowledgment of the chunk `message`, staged or not, once `change` changes it.
fn upload_ack(message: &ClientMessage, staged: bool, change: fn(&mut ServerMessage)) -> Vec<u8> {
    let ClientMessage::UploadSnapshotChunk {
        correlation_id,
        room_id,
        chunk_index,
        total_chunks,
        ..
    } = message
    else {
        panic!("{message:?}");
    };
    let mut ack = ServerMessage::SnapshotUploadChunkAck {
        correlation_id: *correlation_id,
        room_id: room_id.clone(),
        chunk_index: *chunk_index,
        total_chunks: *total_chunks,
        staged,
    };
    change(&mut ack);
    encode_message(&ack).unwrap()
}

/// Makes the next sync through `transport` fail its room of blobs closed: its batch announces
/// another schema, and the registration that follows gets one that does not evolve from the
/// replica's.
fn fail_next_sync_closed(transport: &TestTransport, room_incarnation: u128) {
    let retyped = TableSchema::builder("blobs")
        .primary_key("id", DataType::Int)
        .column("data", DataType::Int);
    let retyped = Schema::builder().table(retyped).build();
    let (mut sync, mut registration) = (true, true);
    transport.answer(move |message| match message {
        ClientMessage::Sync { .. } if std::mem::take(&mut sync) => {
            Some(Ok(sync_batch(message, "other", false)))
        }
        ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            ..
        } if std::mem::take(&mut registration) => {
            Some(Ok(encode_message(&ServerMessage::Registered {
                correlation_id: *correlation_id,
                room_id: room_id.clone(),
                head_seq: SequenceNumber::new(9),
                tail_seq: SequenceNumber::new(1),
                schema_id: SchemaId::new("other").unwrap(),
                schema: retyped.clone(),
                active_snapshot_seq: None,
                room_incarnation,
            })
            .unwrap()))
        }
        _ => None,
    });
}

/// Answers the first heartbeat through `transport` with an ack of the room of blobs at 9 that asks
/// for a snapshot, and the first chunk uploaded with what `reply` gives for it.
fn ask_upload(
    transport: &TestTransport,
    mut reply: impl FnMut(&ClientMessage) -> Vec<u8> + Send + 'static,
) {
    let (mut heartbeat, mut chunk) = (true, true);
    transport.answer(move |message| match message.kind() {
        Heartbeat if std::mem::take(&mut heartbeat) => Some(Ok(wanted(message, 9, BLOBS))),
        UploadSnapshotChunk if std::mem::take(&mut chunk) => Some(Ok(reply(message))),
        _ => None,
    });
}

fn failed_closed<T: std::fmt::Debug>(result: Result<T, ClientError>) {
    let failed = matches!(result, Err(ClientError::FailedClosed(_)));
    assert!(failed, "{result:?}");
}

#[tokio::test(start_paused = true)]
async fn the_designated_client_uploads_a_snapshot_that_a_client_behind_the_log_downloads() {
    // Of one chunk, from a memory engine: once a client registers behind the log, the server
    // designates the client at 9, whose heartbeat uploads the snapshot at its head, with the
    // incarnation of its metadata, and is sent again.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let (transport, storage) = (server.transport(), MemoryStorageEngine::new());
    let config = ClientConfig::new("ana");
    let client = Client::open(
        config,
        storage.clone(),
        MemoryMetadata::default(),
        transport.clone(),
    );
    let client = client.await.unwrap();
    open_room(&client, &room_id).await.unwrap();
    server.commit(&room_id, 1..=9).await;
    client.sync(room_id.clone()).await.unwrap();
    server.prune(&room_id, 6).await;
    let lagging = member(&server, &room_id).await;
    let sent = transport.sent().len();
    client.heartbeat(room_id.clone()).await.unwrap();
    let sent_now = kinds(&transport)[sent..].to_vec();
    assert_eq!(sent_now, [Heartbeat, UploadSnapshotChunk, Heartbeat]);
    let snapshot = storage.create_snapshot(&room_id).await.unwrap();
    let (len, incarnation) = (snapshot.len(), server.incarnation(&room_id).await);
    let uploaded = (9, len as u64, hash(&snapshot), incarnation);
    assert_eq!(uploads(&transport), [(0, 1, len, uploaded)]);
    assert_eq!(active(&server, &room_id), Some(9));
    // The client behind the log downloads it with a heartbeat that announces it, and syncs from it.
    lagging.client.heartbeat(room_id.clone()).await.unwrap();
    lagging.client.sync(room_id.clone()).await.unwrap();
    let all = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    assert_eq!(tasks(&lagging.storage, &room_id).await, all);
    assert_eq!(chunks(&lagging.transport), [(0, None)]);
    assert_eq!(requests(&lagging.transport), [("sync", 9), ("ack", 9)]);

    // Of two chunks, from a disk engine: a snapshot of more than 4 MiB, in chunks of the same
    // length but the last, which holds the rest.
    let (server, room_id, _, designee, lagging) = designated().await;
    designee.client.heartbeat(room_id.clone()).await.unwrap();
    let snapshot = designee.storage.create_snapshot(&room_id).await.unwrap();
    let (len, incarnation) = (snapshot.len(), server.incarnation(&room_id).await);
    assert!(len > 4 << 20 && len % (4 << 20) != 0, "{len}");
    let uploaded = (9, len as u64, hash(&snapshot), incarnation);
    let first = len.div_ceil(2);
    let expected = [(0, 2, first, uploaded), (1, 2, len - first, uploaded)];
    assert_eq!(uploads(&designee.transport), expected);
    lagging.client.heartbeat(room_id.clone()).await.unwrap();
    lagging.client.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&lagging.client, &room_id).await, 9);
    let downloaded = [(0, None), (1, Some(hash(&snapshot)))];
    assert_eq!(chunks(&lagging.transport), downloaded);
    let blob = PrimaryKey::single(1);
    let blob = lagging.storage.get(&room_id, "blobs", &blob).await;
    assert!(blob.unwrap().is_some());
}

#[tokio::test(start_paused = true)]
async fn the_snapshot_uploaded_is_that_of_the_head_it_announces() {
    // A write that comes while the designated client creates its snapshot waits for the applier:
    // the snapshot holds the operations up to the head it announces, and the client behind the log
    // installs it.
    let server = TestServer::pruning();
    let room_id = server.create_room("room-a").await;
    let dir = tempdir().unwrap();
    let (transport, storage) = (server.transport(), TestEngine::new(engine(dir.path())));
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    server.commit(&room_id, 1..=9).await;
    client.sync(room_id.clone()).await.unwrap();
    server.prune(&room_id, 6).await;
    let lagging = member(&server, &room_id).await;
    storage.slow_snapshot(Duration::from_secs(1));
    let write = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        client.write(room_id.clone(), insert_task(10)).await
    };
    let (heartbeat, written) = tokio::join!(client.heartbeat(room_id.clone()), write);
    heartbeat.unwrap();
    written.unwrap();
    let (.., (seq, _, _, _)) = uploads(&transport)[0];
    assert_eq!(seq, 9);
    lagging.client.heartbeat(room_id.clone()).await.unwrap();
    lagging.client.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&lagging.client, &room_id).await, 10);

    // A write applied between the heartbeat and the creation of the snapshot is in it, and the
    // snapshot announces its sequence.
    let schema = ("tasks", tasks_schema());
    let ops = (1..=9).map(insert_task);
    let (_server, room_id, _, designee, lagging) = designated_with("room-a", schema, ops).await;
    let mut held = designee.transport.hold_next_reply(Heartbeat);
    let (client, room) = (&designee.client, room_id.clone());
    let write = async move {
        held.replied().await;
        client.write(room, insert_task(10)).await.unwrap();
        held.release();
    };
    let (heartbeat, ()) = tokio::join!(designee.client.heartbeat(room_id.clone()), write);
    heartbeat.unwrap();
    let (.., (seq, _, _, _)) = uploads(&designee.transport)[0];
    assert_eq!(seq, 10);
    lagging.client.heartbeat(room_id.clone()).await.unwrap();
    lagging.client.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&lagging.client, &room_id).await, 10);
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_wanted_by_a_sync_is_confirmed_with_a_heartbeat_and_not_by_a_write() {
    // The batch of a sync asks the designated client for a snapshot: the sync confirms it with a
    // heartbeat, whose ack asks for it too, and which uploads it.
    let sync = ClientMessageKind::Sync;
    let (server, room_id, _, designee, _lagging) = designated().await;
    let sent = designee.transport.sent().len();
    designee.client.sync(room_id.clone()).await.unwrap();
    let sent_now = kinds(&designee.transport)[sent..].to_vec();
    let upload = [UploadSnapshotChunk, UploadSnapshotChunk];
    assert_eq!(
        sent_now,
        [[sync, Ack, Heartbeat].as_slice(), &upload, &[Heartbeat]].concat()
    );
    assert_eq!(active(&server, &room_id), Some(9));
    // Without a batch that asks for one, a sync sends no heartbeat; with one whose heartbeat does
    // not ask for it, it uploads nothing.
    let sent = designee.transport.sent().len();
    designee.client.sync(room_id.clone()).await.unwrap();
    assert_eq!(kinds(&designee.transport)[sent..], [sync, Ack]);
    let transport = &designee.transport;
    transport.answer_next(sync, |message| sync_batch(message, "blobs", true));
    designee.client.sync(room_id.clone()).await.unwrap();
    assert_eq!(
        kinds(&designee.transport)[sent + 2..],
        [sync, Ack, Heartbeat]
    );
    assert_eq!(uploaded(&designee.transport), [0, 1]);

    // A write confirms none, so that its decision does not wait for an upload: neither that of its
    // commit ack nor that of the batch of its sync, which takes the sequence of a commit ack
    // without its catch-up.
    let schema = ("tasks", tasks_schema());
    let ops = (1..=9).map(insert_task);
    let (server, room_id, _, designee, _lagging) = designated_with("room-a", schema, ops).await;
    let sent = designee.transport.sent().len();
    let write = designee.client.write(room_id.clone(), insert_task(10));
    write.await.unwrap();
    server.commit(&room_id, [11]).await;
    let transport = &designee.transport;
    transport.answer_next(Commit, |message| commit_ack(message, 11));
    let write = designee.client.write(room_id.clone(), update_task(10));
    write.await.unwrap();
    let sent_now = kinds(&designee.transport)[sent..].to_vec();
    assert_eq!(sent_now, [Commit, Commit, sync, Ack]);
    assert_eq!(active(&server, &room_id), None);
    // The heartbeat that follows uploads it.
    designee.client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(active(&server, &room_id), Some(11));
}

#[tokio::test(start_paused = true)]
async fn an_upload_that_gets_superseded_stops() {
    // The snapshot worker uploads one at 9 between the designation and the upload: the first chunk
    // of the designated client gets `SnapshotSuperseded`, and it uploads no more.
    let (server, room_id, ops, designee, lagging) = designated().await;
    let other = big_snapshot(&room_id, &ops, 9).await;
    let mut held = designee.transport.hold_next_reply(Heartbeat);
    let (server_ref, room, staged) = (&server, room_id.clone(), other.clone());
    let upload = async move {
        held.replied().await;
        server_ref.upload(&room, 9, staged).await;
        held.release();
    };
    let (heartbeat, ()) = tokio::join!(designee.client.heartbeat(room_id.clone()), upload);
    network(heartbeat);
    assert_eq!(uploaded(&designee.transport), [0]);
    // The client behind the log catches up from that of the worker.
    lagging.client.heartbeat(room_id.clone()).await.unwrap();
    lagging.client.sync(room_id.clone()).await.unwrap();
    assert_eq!(head(&lagging.client, &room_id).await, 9);
    assert_eq!(chunks(&lagging.transport)[1], (1, Some(hash(&other))));
}

#[tokio::test(start_paused = true)]
async fn a_room_that_fails_closed_or_waits_for_a_snapshot_uploads_none() {
    // A replica that waits for a snapshot uploads none, also when its heartbeat asks for it.
    let (_server, room_id, _, _designee, lagging) = designated().await;
    let transport = &lagging.transport;
    transport.answer_next(Heartbeat, |message| wanted(message, 9, BLOBS));
    lagging.client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(uploaded(&lagging.transport), [0; 0]);

    // One that fails closed between the ack that asks for it and the upload uploads nothing, and
    // one that fails closed during the upload, no more chunks: its heartbeat ends with
    // `FailedClosed`.
    for (held, chunks_sent) in [(Heartbeat, vec![]), (UploadSnapshotChunk, vec![0])] {
        let (server, room_id, _, designee, _lagging) = designated().await;
        let mut held = designee.transport.hold_next_reply(held);
        let incarnation = server.incarnation(&room_id).await;
        fail_next_sync_closed(&designee.transport, incarnation);
        let (client, room) = (&designee.client, room_id.clone());
        let fail = async move {
            held.replied().await;
            failed_closed(client.sync(room).await);
            held.release();
        };
        let (heartbeat, ()) = tokio::join!(designee.client.heartbeat(room_id.clone()), fail);
        failed_closed(heartbeat);
        assert_eq!(uploaded(&designee.transport), chunks_sent);
        assert_eq!(active(&server, &room_id), None);
    }
}

#[tokio::test(start_paused = true)]
async fn a_room_created_again_after_the_designation_gets_no_snapshot_of_the_old_one() {
    // Between the designation and the upload: the first chunk, of the old incarnation, gets
    // `RoomReset`; the replica registers again with its cursor, gets `RoomReset` and is discarded.
    let (server, room_id, _, designee, _lagging) = designated().await;
    let old = server.incarnation(&room_id).await;
    let mut held = designee.transport.hold_next_reply(Heartbeat);
    let (server_ref, room) = (&server, room_id.clone());
    let recreate = async move {
        held.replied().await;
        server_ref.recreate_room(&room).await;
        held.release();
    };
    let (heartbeat, ()) = tokio::join!(designee.client.heartbeat(room_id.clone()), recreate);
    heartbeat.unwrap();
    let incarnations: Vec<_> = uploads(&designee.transport)
        .into_iter()
        .map(|(index, .., (_, _, _, incarnation))| (index, incarnation))
        .collect();
    assert_eq!(incarnations, [(0, old)]);
    let registrations = designee.transport.registrations();
    let last = &registrations[registrations.len() - 2..];
    assert_eq!(last, [(Some(9), Some(old)), (None, None)]);
    let status_now = designee.client.status(room_id.clone()).await;
    assert_eq!(status_now, Ok(status(0, 2, true)));
    assert_eq!(active(&server, &room_id), None);

    // During the upload, a sync gets `ClientNotRegistered` from the new room, registers again and
    // discards the replica: no more chunks are sent.
    let (server, room_id, _, designee, _lagging) = designated().await;
    let mut held = designee.transport.hold_next_reply(UploadSnapshotChunk);
    let (server_ref, client, room) = (&server, &designee.client, room_id.clone());
    let discard = async move {
        held.replied().await;
        server_ref.recreate_room(&room).await;
        client.sync(room).await.unwrap();
        held.release();
    };
    let (heartbeat, ()) = tokio::join!(designee.client.heartbeat(room_id.clone()), discard);
    heartbeat.unwrap();
    assert_eq!(uploaded(&designee.transport), [0]);
    let status_now = designee.client.status(room_id.clone()).await;
    assert_eq!(status_now, Ok(status(0, 2, true)));
    assert_eq!(active(&server, &room_id), None);

    // After a discard in the same incarnation (`InvalidSequence` to the registration of a sync,
    // fabricated), neither, though the relay would take them.
    let (server, room_id, _, designee, _lagging) = designated().await;
    let mut held = designee.transport.hold_next_reply(UploadSnapshotChunk);
    let (mut sync, mut registration) = (true, true);
    designee
        .transport
        .answer(move |message| match message.kind() {
            ClientMessageKind::Sync if std::mem::take(&mut sync) => {
                let (id, room) = request(message);
                Some(Ok(error_reply(id, &room, ErrorCode::ClientNotRegistered)))
            }
            RegisterClient if std::mem::take(&mut registration) => {
                let (id, room) = request_of(message);
                Some(Ok(error_reply(id, &room, ErrorCode::InvalidSequence)))
            }
            _ => None,
        });
    let (client, room) = (&designee.client, room_id.clone());
    let discard = async move {
        held.replied().await;
        // Discarded, the replica registers behind the log, and waits for a snapshot.
        network(client.sync(room).await);
        held.release();
    };
    let (heartbeat, ()) = tokio::join!(designee.client.heartbeat(room_id.clone()), discard);
    heartbeat.unwrap();
    assert_eq!(uploaded(&designee.transport), [0]);
    let status_now = designee.client.status(room_id.clone()).await.unwrap();
    assert_eq!(status_now.epoch, 2);
    assert_eq!(active(&server, &room_id), None);
}

#[tokio::test(start_paused = true)]
async fn a_chunk_is_sent_again_after_a_transient_failure() {
    // A chunk of a download without a reply, or with `Unavailable` or `Timeout`, is requested
    // again, and the download goes on.
    let (server, room_id, ops) = pruned_room().await;
    let snapshot = snapshot(&room_id, tasks_schema(), &ops[..7]).await;
    server.upload(&room_id, 7, snapshot).await;
    for code in [None, Some(ErrorCode::Unavailable), Some(ErrorCode::Timeout)] {
        let lagging = member(&server, &room_id).await;
        match code {
            None => lagging.transport.lose_next_reply(RequestSnapshotChunk),
            Some(code) => answer_next(&lagging.transport, RequestSnapshotChunk, move |id, room| {
                error_reply(id, room, code)
            }),
        }
        lagging.client.sync(room_id.clone()).await.unwrap();
        assert_eq!(chunks(&lagging.transport), [(0, None), (0, None)]);
        assert_eq!(head(&lagging.client, &room_id).await, 9);
    }
    // A few times, after a backoff.
    let lagging = member(&server, &room_id).await;
    lagging
        .transport
        .answer_every(RequestSnapshotChunk, |message| {
            let (id, room) = request(message);
            error_reply(id, &room, ErrorCode::Unavailable)
        });
    let start = Instant::now();
    network(lagging.client.sync(room_id.clone()).await);
    assert_eq!(chunks(&lagging.transport).len(), 4);
    let waited = start.elapsed();
    assert!(
        waited > Duration::ZERO && waited < Duration::from_secs(1),
        "{waited:?}"
    );

    // The last chunk of an upload without a reply is sent again, and the relay, which staged it,
    // acknowledges it again.
    let (server, room_id, _, designee, _lagging) = designated().await;
    let mut held = designee.transport.hold_next_reply(UploadSnapshotChunk);
    let transport = designee.transport.clone();
    let lose = async move {
        held.replied().await;
        transport.lose_next_reply(UploadSnapshotChunk);
        held.release();
    };
    let (heartbeat, ()) = tokio::join!(designee.client.heartbeat(room_id.clone()), lose);
    heartbeat.unwrap();
    assert_eq!(uploaded(&designee.transport), [0, 1, 1]);
    assert_eq!(active(&server, &room_id), Some(9));
    // `Unavailable` to every chunk stops it after a few.
    let (_server, room_id, _, designee, _lagging) = designated().await;
    let transport = &designee.transport;
    transport.answer_every(UploadSnapshotChunk, |message| {
        let (id, room) = request(message);
        error_reply(id, &room, ErrorCode::Unavailable)
    });
    network(designee.client.heartbeat(room_id.clone()).await);
    assert_eq!(uploaded(&designee.transport), [0, 0, 0, 0]);

    // Nor is a chunk sent again once the replica may not upload: here a sync discards it in the
    // same incarnation (`InvalidSequence` to its registration, fabricated) while the chunk waits
    // for its retry.
    let schema = ("tasks", tasks_schema());
    let ops = (1..=9).map(insert_task);
    let (server, room_id, _, designee, _lagging) = designated_with("room-a", schema, ops).await;
    let mut held = designee.transport.hold_next_reply(UploadSnapshotChunk);
    let (mut chunk, mut sync, mut registration) = (true, true, true);
    designee
        .transport
        .answer(move |message| match message.kind() {
            UploadSnapshotChunk if std::mem::take(&mut chunk) => {
                let (id, room) = request(message);
                Some(Ok(error_reply(id, &room, ErrorCode::Unavailable)))
            }
            ClientMessageKind::Sync if std::mem::take(&mut sync) => {
                let (id, room) = request(message);
                Some(Ok(error_reply(id, &room, ErrorCode::ClientNotRegistered)))
            }
            RegisterClient if std::mem::take(&mut registration) => {
                let (id, room) = request_of(message);
                Some(Ok(error_reply(id, &room, ErrorCode::InvalidSequence)))
            }
            _ => None,
        });
    let (client, room) = (&designee.client, room_id.clone());
    let discard = async move {
        // The discard ends while the chunk waits for its retry.
        held.replied().await;
        held.release();
        network(client.sync(room).await);
    };
    let (heartbeat, ()) = tokio::join!(designee.client.heartbeat(room_id.clone()), discard);
    heartbeat.unwrap();
    assert_eq!(uploaded(&designee.transport), [0]);
    assert_eq!(active(&server, &room_id), None);
}

#[tokio::test(start_paused = true)]
async fn a_replica_whose_upload_failed_uploads_none_for_a_minute() {
    // A relay that takes no snapshot of the room, above its limit; the heartbeats and the batches
    // that ask for one are fabricated. The sync that confirms ends with the `Network` of its
    // upload, and for a minute neither a sync nor a heartbeat uploads again.
    let server = TestServer::with(|config| config.max_snapshot_bytes = 64);
    let room_id = server.create_room("room-a").await;
    server.commit(&room_id, 1..=9).await;
    let designee = member(&server, &room_id).await;
    designee.client.sync(room_id.clone()).await.unwrap();
    designee.transport.answer(|message| match message.kind() {
        Heartbeat => Some(Ok(wanted(message, 9, TASKS))),
        ClientMessageKind::Sync => Some(Ok(sync_batch(message, "tasks", true))),
        _ => None,
    });
    network(designee.client.sync(room_id.clone()).await);
    assert_eq!(uploaded(&designee.transport), [0]);
    designee.client.sync(room_id.clone()).await.unwrap();
    designee.client.heartbeat(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(59)).await;
    designee.client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(uploaded(&designee.transport), [0]);
    tokio::time::sleep(Duration::from_secs(1)).await;
    network(designee.client.heartbeat(room_id.clone()).await);
    assert_eq!(uploaded(&designee.transport), [0, 0]);

    // A snapshot that could not be created is an upload that failed too.
    let dir = tempdir().unwrap();
    let (transport, storage) = (server.transport(), TestEngine::new(engine(dir.path())));
    let client = open_client(dir.path(), storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    client.sync(room_id.clone()).await.unwrap();
    transport.answer_every(Heartbeat, |message| wanted(message, 9, TASKS));
    let error = std::io::Error::new(std::io::ErrorKind::OutOfMemory, "Injected");
    storage.fail_next("create_snapshot", StorageError::Io(error));
    let created = client.heartbeat(room_id.clone()).await;
    assert!(
        matches!(created, Err(ClientError::Storage(_))),
        "{created:?}"
    );
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(uploaded(&transport), [0; 0]);
}

#[tokio::test(start_paused = true)]
async fn the_replies_to_an_upload_are_checked_before_they_are_used() {
    // In a room that keeps its log, whose server asks for no snapshot: the heartbeat that asks for
    // one is fabricated. An ack of another chunk stops the upload.
    let (server, room_id, _) = blobs_room().await;
    type Change = fn(&mut ServerMessage);
    let changes: [(&str, Change); 4] = [
        ("another correlation", |ack| {
            if let ServerMessage::SnapshotUploadChunkAck { correlation_id, .. } = ack {
                *correlation_id = CorrelationId::new(correlation_id.get() + 1);
            }
        }),
        ("another room", |ack| {
            if let ServerMessage::SnapshotUploadChunkAck { room_id, .. } = ack {
                *room_id = RoomId::new("room-x").unwrap();
            }
        }),
        ("another index", |ack| {
            if let ServerMessage::SnapshotUploadChunkAck { chunk_index, .. } = ack {
                *chunk_index = 1;
            }
        }),
        ("more chunks", |ack| {
            if let ServerMessage::SnapshotUploadChunkAck { total_chunks, .. } = ack {
                *total_chunks = 3;
            }
        }),
    ];
    for (case, change) in changes {
        let designee = member(&server, &room_id).await;
        designee.client.sync(room_id.clone()).await.unwrap();
        ask_upload(&designee.transport, move |message| {
            upload_ack(message, false, change)
        });
        network(designee.client.heartbeat(room_id.clone()).await);
        assert_eq!(uploaded(&designee.transport), [0], "{case}");
    }
    // An error of another request: `RoomReset` of another correlation or of another room does not
    // register the replica again.
    let errors: [Reply; 2] = [
        |id, room| {
            let other = CorrelationId::new(id.get() + 1);
            error_reply(other, room, ErrorCode::RoomReset)
        },
        |id, _| error_reply(id, &RoomId::new("room-x").unwrap(), ErrorCode::RoomReset),
    ];
    for error in errors {
        let designee = member(&server, &room_id).await;
        designee.client.sync(room_id.clone()).await.unwrap();
        ask_upload(&designee.transport, move |message| {
            let (id, room) = request(message);
            error(id, &room)
        });
        network(designee.client.heartbeat(room_id.clone()).await);
        assert_eq!(uploaded(&designee.transport), [0]);
        assert_eq!(designee.transport.registrations().len(), 1);
    }

    // A chunk staged ends the upload, also before the last one: the relay holds that snapshot.
    let designee = member(&server, &room_id).await;
    designee.client.sync(room_id.clone()).await.unwrap();
    ask_upload(&designee.transport, |message| {
        upload_ack(message, true, |_| {})
    });
    designee.client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(uploaded(&designee.transport), [0]);
    // The last one not staged is `Network`.
    let designee = member(&server, &room_id).await;
    designee.client.sync(room_id.clone()).await.unwrap();
    let mut heartbeat = true;
    designee
        .transport
        .answer(move |message| match message.kind() {
            Heartbeat if std::mem::take(&mut heartbeat) => Some(Ok(wanted(message, 9, BLOBS))),
            UploadSnapshotChunk => Some(Ok(upload_ack(message, false, |_| {}))),
            _ => None,
        });
    network(designee.client.heartbeat(room_id.clone()).await);
    assert_eq!(uploaded(&designee.transport), [0, 1]);

    // `RoomReset` to a chunk registers the replica again with its cursor, and the heartbeat is sent
    // again; here the room is the same. A heartbeat that registered again already is `Network`.
    let designee = member(&server, &room_id).await;
    designee.client.sync(room_id.clone()).await.unwrap();
    ask_upload(&designee.transport, |message| {
        let (id, room) = request(message);
        error_reply(id, &room, ErrorCode::RoomReset)
    });
    let sent = designee.transport.sent().len();
    designee.client.heartbeat(room_id.clone()).await.unwrap();
    let sent_now = kinds(&designee.transport)[sent..].to_vec();
    assert_eq!(
        sent_now,
        [Heartbeat, UploadSnapshotChunk, RegisterClient, Heartbeat]
    );
    assert_eq!(
        designee.transport.registrations()[1],
        (Some(9), Some(server.incarnation(&room_id).await))
    );
    let designee = member(&server, &room_id).await;
    designee.client.sync(room_id.clone()).await.unwrap();
    let (mut first, mut second, mut chunk) = (true, true, true);
    designee
        .transport
        .answer(move |message| match message.kind() {
            Heartbeat if std::mem::take(&mut first) => {
                let (id, room) = request(message);
                Some(Ok(error_reply(id, &room, ErrorCode::ClientNotRegistered)))
            }
            Heartbeat if std::mem::take(&mut second) => Some(Ok(wanted(message, 9, BLOBS))),
            UploadSnapshotChunk if std::mem::take(&mut chunk) => {
                let (id, room) = request(message);
                Some(Ok(error_reply(id, &room, ErrorCode::RoomReset)))
            }
            _ => None,
        });
    let sent = designee.transport.sent().len();
    network(designee.client.heartbeat(room_id.clone()).await);
    let sent_now = kinds(&designee.transport)[sent..].to_vec();
    assert_eq!(
        sent_now,
        [Heartbeat, RegisterClient, Heartbeat, UploadSnapshotChunk]
    );
}

#[tokio::test(start_paused = true)]
async fn a_heartbeat_uploads_once_and_two_upload_one_at_a_time() {
    // A heartbeat whose second ack asks for a snapshot too uploads none again.
    let (server, room_id, designee) = blobs_room().await;
    let transport = &designee.transport;
    transport.answer_every(Heartbeat, |message| wanted(message, 9, BLOBS));
    let sent = designee.transport.sent().len();
    designee.client.heartbeat(room_id.clone()).await.unwrap();
    let sent_now = kinds(&designee.transport)[sent..].to_vec();
    let upload = [UploadSnapshotChunk, UploadSnapshotChunk];
    assert_eq!(
        sent_now,
        [[Heartbeat].as_slice(), &upload, &[Heartbeat]].concat()
    );
    assert_eq!(active(&server, &room_id), Some(9));

    // Two heartbeats at once upload one after the other: while the reply to the first chunk of one
    // is held, the other sends nothing, and then it finds the snapshot staged at its first chunk.
    let room_id = server
        .create_room_with("room-c", "blobs", blobs_schema())
        .await;
    server.commit_ops(&room_id, blobs()).await;
    let designee = member(&server, &room_id).await;
    designee.client.sync(room_id.clone()).await.unwrap();
    let transport = designee.transport.clone();
    transport.answer_every(Heartbeat, |message| wanted(message, 9, BLOBS));
    let mut held = transport.hold_next_reply(UploadSnapshotChunk);
    let hold = async move {
        held.replied().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(uploaded(&transport), [0]);
        held.release();
    };
    let (first, second, ()) = tokio::join!(
        designee.client.heartbeat(room_id.clone()),
        designee.client.heartbeat(room_id.clone()),
        hold
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(uploaded(&designee.transport), [0, 1, 0]);
}
