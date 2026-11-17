//! The simulation: several clients against the server in process, over a network that loses,
//! delays and repeats their requests, with calls dropped at any await and clients over a disk
//! engine crashed and opened again from their directory; between steps, the server restarts,
//! prunes its log by quota, adds columns, moves the room to another schema or creates it again,
//! and clients join the pruned room. An archivist, a client whose requests never fail and whose
//! calls are never dropped, catches up after each step: the operations it receives are the log of
//! the room, which the reference model of the storage applies. After each step:
//! - every replica holds what the model holds at its head, in the incarnation of its metadata;
//! - each write ended with the decision of the model on the sequence its commit got, as uncertain
//!   only if an attempt was or its replica may have skipped that sequence, and with any other
//!   error only if it had no effect;
//! - a replica was discarded only if its room was created again, or it opened without metadata;
//! - no client keeps anything for a write once none is in flight.
//!
//! At the end the network heals, and every client reaches the head of the room with the state of
//! the model. It runs over random schemas and over the one of the reference app, with its
//! scenarios.
//!
//! With workers, some clients run the worker of their room, in a mode drawn at random that steps
//! change, while the steps call them too and they are crashed or have their room closed and
//! opened again, which stops the worker until the client starts it again. Their event streams
//! lose, delay and cut events, tell of events missed, go silent and end as their token expires;
//! the network takes tokens as expired, the token provider gives one already expired now and
//! then, and the server turns its event stream off in some restarts. After each step, also no
//! worker stopped; and once the network heals, the clients whose worker syncs by itself reach the
//! head of the room within a fixed number of intervals without a call of the test, and those in
//! `Manual`, with a sync.
//!
//! Fuzzing, the network also puts other replies in place of some of the server's: random bytes,
//! the reply damaged, random messages, errors of the request, and snapshots that match their hash
//! but install with no schema of the room, or announced larger than the limit of the clients. None
//! tells a client otherwise than the server what it takes on trust. No client panics, every
//! replica holds what the model holds at its head, and no download goes past the first chunk of a
//! snapshot larger than the limit; a write may end with any error, and a call may wait for what
//! those replies announced, until it is dropped after ten minutes and its room opens again at the
//! end.
//!
//! The ignored variants run longer: `cargo test --release -- --ignored`. A run prints
//! its seed first, and `SIM_SEED` runs from another one (`SIM_SEEDS`, how many): a seed repeats
//! the steps and the events, but not the faults, drawn in the order the requests are sent, which
//! the jitter of the backoffs and the blocking pool change; nor, with workers, the writes before
//! the network heals, which go on until enough of them land.

mod common;
#[path = "../../core/tests/common/mod.rs"]
mod generators;
#[allow(dead_code)]
#[path = "../../storage/tests/common/oracle.rs"]
mod oracle;
#[allow(dead_code)]
#[path = "../../storage/tests/common/rng.rs"]
mod rng;

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request};
use bytes::Bytes;
use common::{
    error_reply, options, refused, request, Issue, TestServer, TestTokens, TestTransport,
    ADMIN_SECRET, AUTH_SECRET,
};
use futures::future::join_all;
use futures::StreamExt;
use oracle::{Oracle, Reason, Rows};
use rng::Rng;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::future::{poll_fn, Future};
use std::io;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::task::Poll;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;
use zemdb_client::{
    Client, ClientConfig, ClientError, EventBlocks, EventStream, FileMetadata, Freshness,
    MemoryMetadata, MetadataStore, RoomMetadata, TransportClient, TransportError, Violation,
};
use zemdb_core::protocol::PROTOCOL_MAGIC;
use zemdb_core::*;
use zemdb_server::{build_router, generate_client_token, ServerConfig};
use zemdb_storage::snapshot::{decode_snapshot_envelope, encode_snapshot_envelope};
use zemdb_storage::{
    DiskStorageEngine, MemoryStorageEngine, ScanOptions, StorageEngine, StorageError,
};

/// The index of the archivist among the clients.
const ARCHIVIST: usize = 0;

/// The model is copied every this many sequences.
const COPIES: usize = 16;

/// The largest snapshot the clients download: longer than a chunk of 4 MiB, so that a client that
/// downloads a longer one asks for its second chunk.
const LIMIT: u64 = 6 << 20;

/// How often the workers send a heartbeat, and the longest wait before they try a step again.
const INTERVAL: Duration = Duration::from_secs(10);

/// How long a worker in `Live` waits to open its event stream again once the server answered that
/// it is turned off.
const EVENTS_RETRY: Duration = Duration::from_secs(60);

/// How many intervals a client whose worker syncs by itself takes at most to reach the head of
/// the room once the network heals: its next heartbeat, which may register it again with a
/// renewed token after a backoff, the designation of a client to upload a snapshot if it is
/// behind the log, which the server gives another after 60 s if that one does not start, the
/// upload, the heartbeat that announces the snapshot, and the sync after the download.
const HEAL_INTERVALS: u32 = 12;

/// The panics of the process, also those that a task catches.
static PANICS: AtomicUsize = AtomicUsize::new(0);

/// Counts the panics of the process in [`PANICS`].
fn count_panics() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            PANICS.fetch_add(1, Ordering::SeqCst);
            hook(info)
        }));
    });
}

// ---------------------------------------------------------------------------------------------
// The network

/// What the network saw of the commits of one write.
#[derive(Default)]
struct Commits {
    /// The mutation and client ids its attempts carried.
    ids: BTreeSet<(MutationId, ClientId)>,
    /// Its attempts, and those whose reply reached the client and was neither `Unavailable`,
    /// `Timeout` nor `Internal`.
    sent: u32,
    answered: u32,
    /// The sequences of the `CommitAck`s that reached the client.
    acks: BTreeSet<u64>,
}

/// What the network records for the checks.
#[derive(Default)]
struct Record {
    /// The operations that reached the archivist, in the incarnation of the room it follows.
    log: BTreeMap<u64, Operation>,
    /// The commits of each write, by the timestamp of its operation.
    commits: HashMap<u64, Commits>,
    /// How many chunks of a snapshot each client received, and how many chunks of its uploads the
    /// relay took, whether their reply reached it or not.
    downloads: HashMap<usize, u64>,
    uploads: HashMap<usize, u64>,
}

/// The network between the clients and the server: the transport to the server, which a restart
/// replaces, the faults it injects while it is faulty, and the replies it puts in place of the
/// server's while it fuzzes.
struct Network {
    server: Mutex<TestTransport>,
    rng: Mutex<Rng>,
    faulty: AtomicBool,
    /// Whether the server restarts: no request and no opening of an event stream reaches it.
    down: AtomicBool,
    /// The openings of event streams that may reach the server, which a restart waits for.
    openings: Arc<AtomicUsize>,
    /// The tokens that expired: the server refuses each request and opening with one with
    /// `Unauthorized`.
    expired: Mutex<HashSet<String>>,
    /// How many `ops` events reached the clients.
    ops: AtomicUsize,
    /// The clients without network: none of their requests reaches the server, and their event
    /// streams are cut.
    offline: Mutex<HashSet<usize>>,
    /// How many times it injected each fault of the event streams and of the tokens.
    faults: Mutex<BTreeMap<&'static str, usize>>,
    fuzz: Mutex<Option<Fuzz>>,
    record: Mutex<Record>,
    /// The schemas of the room so far, and the hashes of the snapshots announced larger than
    /// the limit.
    schemas: Mutex<Vec<Schema>>,
    oversized: Mutex<HashSet<[u8; 32]>>,
}

impl Network {
    /// Records what the checks need of `reply`, the reply to `message` of client `client` that
    /// reached it.
    fn observe(&self, client: usize, message: &ClientMessage, reply: &[u8]) {
        let Ok(reply) = decode_message::<ServerMessage>(reply) else {
            return;
        };
        let mut record = self.record.lock().unwrap();
        match (message, reply) {
            (
                ClientMessage::Commit {
                    op, mutation_id, ..
                },
                reply,
            ) => {
                let commits = record.commits.get_mut(&op.timestamp()).unwrap();
                match reply {
                    ServerMessage::CommitAck {
                        mutation_id: acked,
                        assigned_seq,
                        ..
                    } if acked == *mutation_id => {
                        commits.acks.insert(assigned_seq.get());
                        commits.answered += 1;
                    }
                    ServerMessage::Error { code, .. }
                        if !matches!(
                            code,
                            ErrorCode::Unavailable | ErrorCode::Timeout | ErrorCode::Internal
                        ) =>
                    {
                        commits.answered += 1
                    }
                    _ => {}
                }
            }
            (_, ServerMessage::SyncBatch { ops, .. }) if client == ARCHIVIST => {
                for op in ops {
                    let logged = record.log.entry(op.seq.get()).or_insert(op.op.clone());
                    assert_eq!(*logged, op.op, "Two operations at {}", op.seq.get());
                }
            }
            (_, ServerMessage::SnapshotChunk { .. }) => {
                *record.downloads.entry(client).or_default() += 1
            }
            _ => {}
        }
    }

    /// Records whether the relay took a chunk that client `client` sent, by `reply`, the reply of
    /// the server, which may not reach the client.
    fn took(&self, client: usize, reply: &[u8]) {
        if let Ok(ServerMessage::SnapshotUploadChunkAck { .. }) = decode_message(reply) {
            *self
                .record
                .lock()
                .unwrap()
                .uploads
                .entry(client)
                .or_default() += 1;
        }
    }

    fn downloads(&self, client: usize) -> u64 {
        let record = self.record.lock().unwrap();
        record.downloads.get(&client).copied().unwrap_or_default()
    }

    fn uploads(&self, client: usize) -> u64 {
        let record = self.record.lock().unwrap();
        record.uploads.get(&client).copied().unwrap_or_default()
    }

    /// Whether `token` expired.
    fn expired(&self, token: &str) -> bool {
        self.expired.lock().unwrap().contains(token)
    }

    /// Takes `token` as expired from now on.
    fn expire(&self, token: &str) {
        self.expired.lock().unwrap().insert(token.to_string());
    }

    /// Whether client `client` has no network.
    fn offline(&self, client: usize) -> bool {
        self.offline.lock().unwrap().contains(&client)
    }

    /// Counts an injection of `fault`.
    fn injected(&self, fault: &'static str) {
        *self.faults.lock().unwrap().entry(fault).or_default() += 1;
    }

    /// Counts `reply`, of the server, if it refuses the token of its request: one that the token
    /// provider gave already expired, since those the network takes as expired do not reach it.
    fn refused(&self, reply: &[u8]) {
        let refusal = decode_message::<ServerMessage>(reply);
        if let Ok(ServerMessage::Error {
            code: ErrorCode::Unauthorized,
            ..
        }) = refusal
        {
            self.injected("token refused by the server");
        }
    }

    /// `reply`, the reply to `message`, or now and then, while the network fuzzes, another in its
    /// place: to the request of the first chunk of a snapshot that fits in one, half the time, the
    /// chunk of another snapshot ([`Fuzz::snapshot`]), unless that one installs with a schema of
    /// the room; and otherwise, one time in five, another reply ([`Fuzz::reply`]), unless it lies
    /// ([`lies`]).
    async fn fuzzed(&self, message: &ClientMessage, reply: Vec<u8>) -> Vec<u8> {
        let (data, total_bytes, correlation_id, room_id, snapshot_head_seq, chunk_size) = {
            let mut fuzz = self.fuzz.lock().unwrap();
            let Some(fuzz) = fuzz.as_mut() else {
                return reply;
            };
            let real = decode_message::<ServerMessage>(&reply).ok();
            match (message, real) {
                (
                    ClientMessage::RequestSnapshotChunk { chunk_size, .. },
                    Some(ServerMessage::SnapshotChunk {
                        correlation_id,
                        room_id,
                        snapshot_head_seq,
                        chunk_index: 0,
                        total_chunks: 1,
                        data,
                        ..
                    }),
                ) if fuzz.rng.coin() => {
                    let (data, total_bytes) = fuzz.snapshot(&data, *chunk_size);
                    let seq = snapshot_head_seq;
                    (data, total_bytes, correlation_id, room_id, seq, *chunk_size)
                }
                (_, real) => {
                    let Some(fuzzed) = fuzz.reply(message, &reply) else {
                        return reply;
                    };
                    let decoded = decode_message::<ServerMessage>(&fuzzed);
                    let lie = decoded.is_ok_and(|decoded| lies(message, &decoded, real.as_ref()));
                    return if lie { reply } else { fuzzed };
                }
            }
        };
        if self.installs(&data, &room_id, snapshot_head_seq).await {
            return reply;
        }
        let snapshot_hash = ServerMessage::compute_snapshot_hash(&data);
        if total_bytes > LIMIT {
            self.oversized.lock().unwrap().insert(snapshot_hash);
        }
        let total_chunks = total_bytes.div_ceil(u64::from(chunk_size));
        let chunk = ServerMessage::SnapshotChunk {
            correlation_id,
            room_id,
            snapshot_head_seq,
            chunk_index: 0,
            total_chunks: u32::try_from(total_chunks).unwrap_or(u32::MAX),
            total_bytes,
            snapshot_hash,
            data: data.into(),
        };
        encode_message(&chunk).unwrap()
    }

    /// Whether `snapshot` installs at `seq` in `room_id` with a schema of the room.
    async fn installs(&self, snapshot: &[u8], room_id: &RoomId, seq: SequenceNumber) -> bool {
        let snapshot = Bytes::copy_from_slice(snapshot);
        let schemas = self.schemas.lock().unwrap().clone();
        for schema in schemas {
            let storage = MemoryStorageEngine::new();
            storage.open_room(room_id, schema.clone()).await.unwrap();
            let installed = storage.apply_snapshot(room_id, schema, snapshot.clone(), seq);
            if installed.await.is_ok() {
                return true;
            }
        }
        false
    }
}

/// What the client takes from a reply on trust: the operations of a batch or a catch-up that
/// follow the cursor of its request, the schema and the incarnation of `Registered`, and the
/// sequence of a `CommitAck`.
type Trusted<'a> = (
    &'a [SequencedOperation],
    Option<(&'a Schema, u128)>,
    Option<SequenceNumber>,
);

fn trusted(reply: &ServerMessage) -> Trusted<'_> {
    match reply {
        ServerMessage::SyncBatch { ops, .. } => (ops, None, None),
        ServerMessage::CommitAck {
            catchup_ops,
            assigned_seq,
            ..
        } => (catchup_ops, None, Some(*assigned_seq)),
        ServerMessage::Registered {
            schema,
            room_incarnation,
            ..
        } => (&[], Some((schema, *room_incarnation)), None),
        _ => (&[], None, None),
    }
}

/// Whether `fuzzed`, in place of `real`, the reply of the server to `message`, tells the client
/// otherwise what it takes on trust: a reply the client cannot tell from the server's.
fn lies(message: &ClientMessage, fuzzed: &ServerMessage, real: Option<&ServerMessage>) -> bool {
    let (ops, room, seq) = trusted(fuzzed);
    let (real_ops, real_room, real_seq) = real.map_or((&[][..], None, None), trusted);
    let cursor = match message {
        ClientMessage::Sync { from_seq, .. } => from_seq.get(),
        ClientMessage::Commit { last_ack_seq, .. } => last_ack_seq.get(),
        _ => 0,
    };
    let follow = (ops.iter().zip(1..)).all(|(op, i)| op.seq.get().checked_sub(i) == Some(cursor));
    (follow && ops.iter().any(|op| !real_ops.contains(op)))
        || room.is_some_and(|room| Some(room) != real_room)
        || seq.is_some_and(|seq| Some(seq) != real_seq)
}

/// What the network fuzzes with: its generator, valid replies that a damaged one takes bytes
/// from, and the codes of the errors.
struct Fuzz {
    rng: generators::Rng,
    pool: Vec<Vec<u8>>,
    codes: Vec<ErrorCode>,
}

impl Fuzz {
    fn new(seed: u64) -> Self {
        let samples = generators::sample_server_messages();
        Self {
            rng: generators::Rng::new(seed.reverse_bits()),
            pool: samples.iter().map(|m| encode_message(m).unwrap()).collect(),
            codes: generators::all_error_codes(),
        }
    }

    /// One time in five, a reply in place of `reply`, the reply to `message`: random bytes, after
    /// a valid header now and then; `reply` damaged; a random message; or an error of `message`
    /// with a random code.
    fn reply(&mut self, message: &ClientMessage, reply: &[u8]) -> Option<Vec<u8>> {
        let rng = &mut self.rng;
        if !rng.chance(1, 5) {
            return None;
        }
        Some(match rng.below(4) {
            0 => {
                let mut bytes = Vec::new();
                if rng.coin() {
                    bytes.extend(PROTOCOL_MAGIC);
                    bytes.extend([PROTOCOL_VERSION, 0]);
                }
                bytes.extend(rng.bytes_below(256));
                bytes
            }
            1 => {
                let mut bytes = reply.to_vec();
                generators::mutate(rng, &mut bytes, PROTOCOL_HEADER_LEN, &self.pool);
                bytes
            }
            2 => encode_message(&generators::server_message(rng, &self.codes)).unwrap(),
            _ => {
                let (correlation_id, room_id) = request(message);
                error_reply(correlation_id, &room_id, *rng.pick(&self.codes))
            }
        })
    }

    /// A snapshot in place of `snapshot`, with the length it is announced with: random bytes;
    /// `snapshot` damaged; its payload damaged and sealed, raw or with Zstd; its payload with
    /// Zstd, with the body damaged and sealed; or one announced as long as the limit, one byte
    /// longer or of a random length, in chunks of `chunk_size`, of which only the first comes.
    fn snapshot(&mut self, snapshot: &[u8], chunk_size: u32) -> (Vec<u8>, u64) {
        let rng = &mut self.rng;
        let payload = decode_snapshot_envelope(snapshot).unwrap();
        let damaged = |rng: &mut generators::Rng, mut bytes: Vec<u8>| {
            generators::mutate(rng, &mut bytes, 0, &[]);
            bytes
        };
        let data = match rng.below(6) {
            0 => rng.bytes_below(256),
            1 => damaged(rng, snapshot.to_vec()),
            2 => encode_snapshot_envelope(&damaged(rng, payload), false, 0).unwrap(),
            3 => encode_snapshot_envelope(&damaged(rng, payload), true, 3).unwrap(),
            4 => {
                let zstd = encode_snapshot_envelope(&payload, true, 3).unwrap();
                let body = damaged(rng, zstd[SNAPSHOT_HEADER_LEN..].to_vec());
                let len = payload.len() as u32;
                let header =
                    SnapshotEnvelopeHeader::for_body(SnapshotCompression::Zstd, len, &body);
                [&header.to_bytes()[..], &body].concat()
            }
            _ => {
                let longer = rng.u64();
                let total_bytes = *rng.pick(&[LIMIT, LIMIT + 1, longer]);
                // Its first chunk, of the bytes of its length, so that its hash is its own.
                let len = total_bytes.min(u64::from(chunk_size)) as usize;
                let mut data = total_bytes.to_le_bytes().repeat(len.div_ceil(8));
                data.truncate(len);
                return (data, total_bytes);
            }
        };
        let total_bytes = data.len() as u64;
        (data, total_bytes)
    }
}

/// No response to a request.
fn lost() -> TransportError {
    TransportError("Lost".to_string())
}

/// The reply of the server to a request of `room_id` with a token that expired.
fn unauthorized(room_id: &RoomId) -> Vec<u8> {
    encode_message(&ServerMessage::Error {
        correlation_id: None,
        room_id: Some(room_id.clone()),
        code: ErrorCode::Unauthorized,
        message: "Expired".to_string(),
    })
    .unwrap()
}

/// An opening of an event stream on its way to the server, counted until it ends.
struct Opening(Arc<AtomicUsize>);

impl Opening {
    fn new(openings: &Arc<AtomicUsize>) -> Self {
        openings.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(openings))
    }
}

impl Drop for Opening {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The `lagged` event, as the server writes it for events that a stream missed.
const LAGGED: &[u8] = b"event: lagged\ndata: true\n\n";

/// `blocks`, the event stream that `token` of client `client` opened, with the faults that `net`
/// injects at each block while it is faulty: a block lost, or late, with those after it; `lagged`
/// in its place; the connection cut; a stream that goes silent; or its token that expires, which
/// ends it. The stream of a client without network is cut. It counts the `ops` events that reach
/// the client.
fn faulty_blocks(
    blocks: EventBlocks,
    net: Arc<Network>,
    (client, token): (usize, String),
) -> EventBlocks {
    let blocks = futures::stream::unfold(Some(blocks), move |blocks| {
        let (net, token) = (Arc::clone(&net), token.clone());
        async move {
            let mut blocks = blocks?;
            loop {
                let block = blocks.next().await?;
                if net.offline(client) {
                    return Some((Err(TransportError("Offline".to_string())), None));
                }
                let fault = {
                    let mut rng = net.rng.lock().unwrap();
                    let faulty = net.faulty.load(Ordering::SeqCst);
                    (faulty.then(|| rng.below(100)), rng.below(3000))
                };
                match fault {
                    (Some(0 | 1), _) => {
                        net.injected("event lost");
                        continue;
                    }
                    (Some(2 | 3), delay) => {
                        net.injected("event late");
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                    (Some(4), _) => {
                        net.injected("lagged");
                        return Some((Ok(Bytes::from_static(LAGGED)), Some(blocks)));
                    }
                    (Some(5), _) => {
                        net.injected("stream cut");
                        return Some((Err(TransportError("Cut".to_string())), None));
                    }
                    (Some(6), _) => {
                        net.injected("stream silent");
                        std::future::pending().await
                    }
                    (Some(7), _) => {
                        net.injected("stream expired");
                        net.expire(&token);
                        return None;
                    }
                    _ => {}
                }
                if block
                    .as_ref()
                    .is_ok_and(|block| block.starts_with(b"event: ops\n"))
                {
                    net.ops.fetch_add(1, Ordering::SeqCst);
                }
                return Some((block, Some(blocks)));
            }
        }
    });
    Box::pin(blocks)
}

/// The transport of client `client` over the network, which loses, delays and repeats its
/// requests, answers some with `Unavailable`, expires their tokens, and fuzzes their replies, if
/// it is `faulty`; and which loses, delays and cuts the events of its streams. It checks that no
/// download goes past the first chunk of a snapshot larger than the limit.
#[derive(Clone)]
struct Link {
    net: Arc<Network>,
    client: usize,
    faulty: bool,
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Link")
            .field("client", &self.client)
            .finish()
    }
}

#[async_trait]
impl TransportClient for Link {
    async fn send(&self, message: &ClientMessage, token: &str) -> Result<Vec<u8>, TransportError> {
        let net = &self.net;
        if let ClientMessage::RequestSnapshotChunk {
            snapshot_hash: Some(hash),
            ..
        } = message
        {
            let oversized = net.oversized.lock().unwrap().contains(hash);
            assert!(!oversized, "A snapshot larger than the limit is downloaded");
        }
        let (fault, delay) = {
            let mut rng = net.rng.lock().unwrap();
            let faulty = self.faulty && net.faulty.load(Ordering::SeqCst);
            (faulty.then(|| rng.below(20)), rng.below(3000))
        };
        if let ClientMessage::Commit {
            op,
            mutation_id,
            client_id,
            ..
        } = message
        {
            let mut record = net.record.lock().unwrap();
            let commits = record.commits.entry(op.timestamp()).or_default();
            commits.sent += 1;
            commits.ids.insert((*mutation_id, client_id.clone()));
        }
        let deliver = || async {
            // A server that restarts takes nothing, and a client without network sends nothing.
            if net.down.load(Ordering::SeqCst) || net.offline(self.client) {
                return Err(lost());
            }
            let server = net.server.lock().unwrap().clone();
            let reply = server.send(message, token).await;
            if let Ok(reply) = &reply {
                net.took(self.client, reply);
                net.refused(reply);
            }
            reply
        };
        // Nothing answers a client without network, or a request while the server restarts.
        if net.down.load(Ordering::SeqCst) || net.offline(self.client) {
            return Err(lost());
        }
        let (_, room_id) = request(message);
        let expired = self.faulty && net.expired(token);
        let reply = match fault {
            // The server refuses a token that expired before anything else.
            _ if expired => unauthorized(&room_id),
            // Lost on its way, or its reply lost.
            Some(0) => return Err(lost()),
            Some(1) => {
                let _ = deliver().await;
                return Err(lost());
            }
            // Late, so that requests sent after it can reach the server first.
            Some(2 | 3) => {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                deliver().await?
            }
            // Twice, with the reply to either.
            Some(4) => {
                let first = deliver().await?;
                let second = deliver().await?;
                if delay % 2 == 0 {
                    first
                } else {
                    second
                }
            }
            // Answered with `Unavailable` once the server is done with it, as the server does
            // without room for the response in its budget.
            Some(5) => {
                let _ = deliver().await;
                let (correlation_id, room_id) = request(message);
                error_reply(correlation_id, &room_id, ErrorCode::Unavailable)
            }
            // Its token expires now.
            Some(6) => {
                net.injected("token expired");
                net.expire(token);
                unauthorized(&room_id)
            }
            _ => deliver().await?,
        };
        let reply = match self.faulty {
            true => net.fuzzed(message, reply).await,
            false => reply,
        };
        net.observe(self.client, message, &reply);
        Ok(reply)
    }

    /// The event stream of `room_id`, which, if the transport is `faulty`, gets no response now
    /// and then, is refused for a token that expires or for a server without room for it, and
    /// loses, delays and cuts its events ([`faulty_blocks`]).
    async fn events(&self, room_id: &RoomId, token: &str) -> Result<EventStream, TransportError> {
        let net = &self.net;
        if net.down.load(Ordering::SeqCst) || net.offline(self.client) {
            return Err(lost());
        }
        let faulty = self.faulty && net.faulty.load(Ordering::SeqCst);
        let fault = faulty.then(|| net.rng.lock().unwrap().below(20));
        match fault {
            _ if self.faulty && net.expired(token) => {
                return Ok(refused(room_id, ErrorCode::Unauthorized));
            }
            Some(0) => {
                net.injected("opening lost");
                return Err(lost());
            }
            Some(1) => {
                net.injected("opening expired");
                net.expire(token);
                return Ok(refused(room_id, ErrorCode::Unauthorized));
            }
            Some(2) => {
                net.injected("opening refused");
                return Ok(refused(room_id, ErrorCode::RateLimited));
            }
            _ => {}
        }
        let server = net.server.lock().unwrap().clone();
        let opened = {
            let _opening = Opening::new(&net.openings);
            server.events(room_id, token).await?
        };
        if let EventStream::Refused(refusal) = &opened {
            net.refused(refusal);
        }
        Ok(match opened {
            EventStream::Open(blocks) if self.faulty => {
                let of = (self.client, token.to_string());
                EventStream::Open(faulty_blocks(blocks, Arc::clone(net), of))
            }
            opened => opened,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// The clients

/// A metadata store that the checks read too.
#[derive(Debug, Clone)]
struct Shared(Arc<dyn MetadataStore>);

impl Shared {
    /// The metadata of `room_id`, as the checks read them.
    async fn of(&self, room_id: &RoomId) -> Option<RoomMetadata> {
        self.0.room(room_id).await.unwrap()
    }
}

#[async_trait]
impl MetadataStore for Shared {
    async fn open(&self) -> io::Result<u128> {
        self.0.open().await
    }

    async fn close(&self) {
        self.0.close().await
    }

    async fn room(&self, room_id: &RoomId) -> io::Result<Option<RoomMetadata>> {
        self.0.room(room_id).await
    }

    async fn save_room(&self, room_id: &RoomId, metadata: &RoomMetadata) -> io::Result<()> {
        self.0.save_room(room_id, metadata).await
    }

    async fn remove_room(&self, room_id: &RoomId) -> io::Result<()> {
        self.0.remove_room(room_id).await
    }
}

/// A client of the simulation, over a memory engine or the disk engine of its directory.
struct Member {
    person: &'static str,
    dir: Option<TempDir>,
    client: Arc<Client>,
    /// Its engine, which the checks read too.
    storage: Arc<dyn StorageEngine>,
    metadata: Shared,
    /// Its token provider, and how many tokens the client had asked for when the provider was
    /// told to give one already expired, until the client asks for it.
    tokens: TestTokens,
    expiring: Option<usize>,
    /// The mode of the worker of its room, if it runs one.
    worker: Option<Freshness>,
    /// The incarnation of its metadata when it opened, the last one if it had none, and the
    /// opens of the room that may discard it: its epoch counts no discard but one per such open
    /// and one per incarnation after that one.
    since: usize,
    discards: u64,
    /// The discards that the opens of the step may add once a registration got `RoomReset`.
    resetting: u64,
}

impl Member {
    /// Counts an open of `room_id`, which discards the replica if it has no metadata. One with the
    /// metadata of an incarnation before `last` may find none too, in this step alone: a
    /// registration with `RoomReset` in the same step, of its worker or of another call, discards
    /// the replica first, and the open discards the empty replica once more.
    async fn opening(&mut self, room_id: &RoomId, last: Option<u128>) {
        let metadata = self.metadata.of(room_id).await;
        match metadata {
            None => self.discards += 1,
            Some(metadata) if last.is_some_and(|last| metadata.room_incarnation != last) => {
                self.resetting += 1
            }
            Some(_) => {}
        }
    }
}

/// A client of `person` over `storage`, `metadata`, `link` and `tokens`, with `storage` for the
/// checks.
async fn client<S: StorageEngine + Clone + 'static>(
    person: &str,
    storage: S,
    metadata: &Shared,
    (link, tokens): (&Link, &TestTokens),
) -> Result<(Arc<Client>, Arc<dyn StorageEngine>), ClientError> {
    let config = ClientConfig {
        max_snapshot_bytes: LIMIT,
        heartbeat_interval: INTERVAL,
        retry_max: INTERVAL,
        events_retry: EVENTS_RETRY,
        ..ClientConfig::new(person)
    };
    let client = Client::open(
        config,
        storage.clone(),
        metadata.clone(),
        link.clone(),
        tokens.clone(),
    )
    .await?;
    Ok((Arc::new(client), Arc::new(storage)))
}

/// Opens `room_id` in `client`, with a token for a day and a second more per open before, so that
/// each open gives the room a token of its own, which replaces one the network took as expired.
async fn open_room(client: &Client, room_id: &RoomId) -> Result<(), ClientError> {
    static OPENS: AtomicU64 = AtomicU64::new(0);
    let valid = Duration::from_secs(86_400 + OPENS.fetch_add(1, Ordering::SeqCst));
    let token = generate_client_token(&client.client_id(), room_id, valid, AUTH_SECRET);
    client.open_room(room_id.clone(), token).await
}

/// Every row of every table of `schema` in `room_id` of `storage`, or the error of reading them:
/// a worker may be discarding the replica.
async fn rows(
    storage: &dyn StorageEngine,
    room_id: &RoomId,
    schema: &Schema,
) -> Result<Rows, StorageError> {
    let mut rows = Rows::new();
    for table in schema.table_ids() {
        let mut scan = storage
            .scan_by_id(room_id, table, ScanOptions::new())
            .await?;
        while let Some(row) = scan.next().await {
            let (pk, row) = row?;
            rows.entry(table).or_default().insert(pk, row);
        }
    }
    Ok(rows)
}

/// The rows of `rows` without the nulls at the end of each: an update widens a row to the width
/// of the schema it is applied with, which may be a later one in one replica than in another.
fn trimmed(rows: &Rows) -> Vec<(u16, Vec<String>, Vec<String>)> {
    let mut dump = oracle::dump(rows);
    for (_, _, row) in &mut dump {
        while row.last().is_some_and(|value| value == "Null") {
            row.pop();
        }
    }
    dump
}

/// What the replica decides on an operation that the model applies, or rejects for `reason`.
fn decision(reason: Option<&Reason>) -> Result<(), ClientError> {
    let violation = match reason.cloned() {
        None => return Ok(()),
        Some(Reason::MissingRow) => return Err(ClientError::RowMissing),
        Some(Reason::Duplicate) => Violation::Duplicate,
        Some(Reason::NotOwner) => Violation::NotOwner,
        Some(Reason::Unique(constraint)) => Violation::Unique { constraint },
        Some(Reason::ForeignKey(constraint)) => Violation::ForeignKey { constraint },
        Some(Reason::Restrict(constraint)) => Violation::Restrict { constraint },
    };
    Err(ClientError::ConstraintViolation(violation))
}

// ---------------------------------------------------------------------------------------------
// The model

/// An incarnation of the room: its log, as the archivist received it, and the model that
/// applies it.
struct Incarnation {
    id: u128,
    log: Vec<SequencedOperation>,
    /// The model at the head of the log, the operations it rejected, and copies of it every few
    /// sequences.
    model: Oracle,
    rejected: HashMap<u64, Reason>,
    copies: Vec<Oracle>,
}

impl Incarnation {
    fn new(id: u128, schema: Schema) -> Self {
        Self {
            id,
            log: Vec::new(),
            model: Oracle::new(schema.clone()),
            rejected: HashMap::new(),
            copies: vec![Oracle::new(schema)],
        }
    }

    /// Applies `ops`, the next operations of the log.
    fn extend(&mut self, ops: Vec<SequencedOperation>) {
        for op in ops {
            let rejected = self.model.apply_batch(std::slice::from_ref(&op));
            self.rejected.extend(rejected);
            self.log.push(op);
            if self.log.len() % COPIES == 0 {
                self.copies.push(self.model.clone());
            }
        }
    }

    /// The rows of the model at `head`. Every schema of the room extends the ones before it
    /// with constraints over its new columns alone, which no earlier operation writes: `schema`,
    /// the last, decides each operation as the one it was sequenced under.
    fn rows(&self, head: usize, schema: &Schema) -> Rows {
        let mut model = self.copies[head / COPIES].clone();
        model.evolve(schema.clone());
        model.apply_batch(&self.log[head / COPIES * COPIES..head]);
        model.rows().clone()
    }

    /// The sequences of the operations of the log tagged `tag`.
    fn landed(&self, tag: u64) -> Vec<u64> {
        let tagged = self.log.iter().filter(|op| op.op.timestamp() == tag);
        tagged.map(|op| op.seq.get()).collect()
    }
}

// ---------------------------------------------------------------------------------------------
// The reference app

const RANKINGS: u16 = 0;
const MEMBERS: u16 = 1;
const ROUNDS: u16 = 2;
const VOTES: u16 = 3;

/// The ranking by votes: in each round of a ranking, each member votes for another, and each vote
/// is a point.
fn ranking_schema() -> Schema {
    let rankings = TableSchema::builder("rankings")
        .table_id(RANKINGS)
        .primary_key("ranking_id", DataType::Uuid)
        .column("creator", DataType::String)
        .column("name", DataType::String)
        .owner("creator", OwnerIdentity::ClientId);
    let members = TableSchema::builder("members")
        .table_id(MEMBERS)
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("member_id", DataType::String)
        .column("display_name", DataType::String)
        .owner("member_id", OwnerIdentity::ClientId)
        .foreign_key("members_ranking", ["ranking_id"], "rankings");
    let rounds = TableSchema::builder("rounds")
        .table_id(ROUNDS)
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("round_id", DataType::Uuid)
        .column("opened_by", DataType::String)
        .column("title", DataType::String)
        .nullable_column("notes", DataType::String)
        .encrypted_column("notes", DataType::String)
        .owner("opened_by", OwnerIdentity::ClientId)
        .foreign_key("rounds_ranking", ["ranking_id"], "rankings")
        .foreign_key("rounds_member", ["ranking_id", "opened_by"], "members")
        .unique("rounds_title", ["ranking_id", "title"]);
    let votes = TableSchema::builder("votes")
        .table_id(VOTES)
        .primary_key("voter", DataType::String)
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("round_id", DataType::Uuid)
        .column("candidate", DataType::String)
        .owner("voter", OwnerIdentity::ClientId)
        .immutable()
        .foreign_key("votes_round", ["ranking_id", "round_id"], "rounds")
        .foreign_key("votes_voter", ["ranking_id", "voter"], "members")
        .foreign_key("votes_candidate", ["ranking_id", "candidate"], "members");
    Schema::builder()
        .table(rankings)
        .table(members)
        .table(rounds)
        .table(votes)
        .build()
}

fn uuid(n: u128) -> Value {
    Value::Uuid(n.to_be_bytes())
}

/// An insert into `table` of `row`, whose first `key` values are its key.
fn insert(table: u16, key: usize, row: Vec<Value>) -> Operation {
    let pk = PrimaryKey::composite(row[..key].to_vec());
    Operation::insert(table, pk, CompactRow::new(row), 0)
}

fn ranking(ranking: Value, creator: &str) -> Operation {
    insert(RANKINGS, 1, vec![ranking, creator.into(), "Amigos".into()])
}

fn member(ranking: Value, person: &str) -> Operation {
    insert(MEMBERS, 2, vec![ranking, person.into(), person.into()])
}

/// A round, with its notes as the bytes they are encrypted to.
fn round(ranking: Value, round: Value, person: &str, title: &str) -> Operation {
    let notes = Value::Bytes(b"sealed".to_vec().into());
    insert(
        ROUNDS,
        2,
        vec![ranking, round, person.into(), title.into(), notes],
    )
}

fn vote(voter: &str, ranking: Value, round: Value, candidate: &str) -> Operation {
    insert(
        VOTES,
        3,
        vec![voter.into(), ranking, round, candidate.into()],
    )
}

/// An operation of `person` over two rankings, three rounds and three persons: mostly votes,
/// and few deletes.
fn ranking_operation(rng: &mut Rng, person: &str) -> Operation {
    let ranking_id = uuid(1 + u128::from(rng.below(2)));
    let round_id = uuid(11 + u128::from(rng.below(3)));
    let candidate = ["A", "B", "C"][rng.below(3) as usize];
    let title = ["Ronda 1", "Ronda 2"][rng.below(2) as usize];
    match rng.below(20) {
        0..=2 => ranking(ranking_id, person),
        3 => {
            let pk = PrimaryKey::composite([ranking_id]);
            Operation::delete_with_owner(RANKINGS, pk, person, 0)
        }
        4..=6 => member(ranking_id, person),
        7 => Operation::delete(
            MEMBERS,
            PrimaryKey::composite([ranking_id, person.into()]),
            0,
        ),
        8..=10 => round(ranking_id, round_id, person, title),
        11 => {
            let pk = PrimaryKey::composite([ranking_id, round_id]);
            let updates = vec![ColumnUpdate::new(2, person), ColumnUpdate::new(3, title)];
            Operation::update(ROUNDS, pk, updates, 0)
        }
        12 => {
            let pk = PrimaryKey::composite([ranking_id, round_id]);
            Operation::delete_with_owner(ROUNDS, pk, person, 0)
        }
        _ => vote(person, ranking_id, round_id, candidate),
    }
}

/// `schema` with a nullable column `e{n}` added to a random table, without constraints.
fn plain_column(rng: &mut Rng, schema: &Schema, n: usize) -> Schema {
    let tables: Vec<String> = schema.tables().map(|t| t.name().to_string()).collect();
    let table = &tables[rng.below(tables.len() as u64) as usize];
    let column = ColumnDef::new(format!("e{n}"), DataType::Int).nullable(true);
    let mut evolved = schema.clone();
    let added = evolved.add_column_with_constraints(table, column, Vec::new(), Vec::new());
    added.unwrap();
    evolved
}

/// The request of the Control Plane that adds to `old` the column of `new`, with the constraints
/// that come with it.
fn added_column(old: &Schema, new: &Schema) -> serde_json::Value {
    let before = |table: &TableSchema| old.get_table_by_id(table.table_id()).unwrap();
    let table = new
        .tables()
        .find(|t| before(t).columns().len() < t.columns().len());
    let table = table.unwrap();
    json!({
        "table_name": table.name(),
        "column": table.columns().last().unwrap(),
        "unique": &table.unique_constraints()[before(table).unique_constraints().len()..],
        "foreign_keys": &table.foreign_keys()[before(table).foreign_keys().len()..],
    })
}

// ---------------------------------------------------------------------------------------------
// The simulation

/// The schemas a simulation runs over.
#[derive(Debug, Clone, Copy)]
enum Kind {
    /// The schema of the reference model, with columns added with constraints over them, and
    /// random operations of its two owners.
    Random,
    /// The schema of the reference app, with operations of its members.
    Ranking,
}

/// What a client does in a step.
#[derive(Debug, Clone)]
enum Action {
    Write(Operation),
    Sync,
    Heartbeat,
    /// Closes the room and opens it again.
    Reopen,
    /// Sets the mode of the worker of the room.
    Freshness(Freshness),
}

/// An action of client `member`, dropped after `polls` polls of it, and the client crashed
/// afterwards if `crash`.
#[derive(Debug, Clone)]
struct Plan {
    member: usize,
    action: Action,
    polls: Option<u64>,
    crash: bool,
}

impl Plan {
    fn new(member: usize, action: Action) -> Self {
        Self {
            member,
            action,
            polls: None,
            crash: false,
        }
    }
}

/// Runs `action`, dropping it at the await where it is after `polls` polls, as a caller that
/// stops waiting: `None` if it was dropped. A call that has not ended in ten minutes fails,
/// unless it `may_wait`, and is dropped.
async fn cut<T>(action: impl Future<Output = T>, polls: Option<u64>, may_wait: bool) -> Option<T> {
    let mut action = pin!(tokio::time::timeout(Duration::from_secs(600), action));
    let mut polls = polls.unwrap_or(u64::MAX);
    poll_fn(|cx| match polls.checked_sub(1) {
        None => Poll::Ready(None),
        Some(left) => {
            polls = left;
            let ended = action.as_mut().poll(cx);
            ended.map(|ended| {
                assert!(ended.is_ok() || may_wait, "A call that does not end");
                ended.ok()
            })
        }
    })
    .await
}

struct Sim {
    kind: Kind,
    /// Whether the network fuzzes the replies, and whether some clients run a worker.
    fuzz: bool,
    workers: bool,
    /// Whether the server has its event stream turned off since its last restart.
    events_off: bool,
    /// The clients whose worker syncs by itself that are behind the log, without network.
    behind_log: BTreeSet<usize>,
    rng: Rng,
    server: TestServer,
    net: Arc<Network>,
    room: RoomId,
    schema_id: SchemaId,
    schema: Schema,
    /// The schemas registered and the columns added, which name the next ones.
    schemas: usize,
    columns: usize,
    incarnations: Vec<Incarnation>,
    /// The archivist first.
    members: Vec<Member>,
    tags: u64,
}

impl Sim {
    /// A server with a room of the schema of `kind`, the archivist and four clients: a disk and a
    /// memory one of each of two persons, those of the second with a worker if `workers`, the
    /// one over the disk in `Live` and the other in a random mode. The network fuzzes the replies
    /// if `fuzz`.
    async fn new(seed: u64, kind: Kind, fuzz: bool, workers: bool) -> Self {
        eprintln!(
            "Simulation of seed {seed} over {kind:?}, fuzzing: {fuzz}, with workers: {workers}: \
             SIM_SEED={seed} repeats its steps"
        );
        count_panics();
        let server = TestServer::with(|config| config.ram_max_ops = 5);
        let schema = match kind {
            Kind::Random => oracle::schema(),
            Kind::Ranking => ranking_schema(),
        };
        let net = Arc::new(Network {
            server: Mutex::new(server.transport()),
            rng: Mutex::new(Rng::new(!seed)),
            faulty: AtomicBool::new(true),
            down: AtomicBool::new(false),
            openings: Arc::default(),
            expired: Mutex::default(),
            ops: AtomicUsize::new(0),
            offline: Mutex::default(),
            faults: Mutex::default(),
            fuzz: Mutex::new(fuzz.then(|| Fuzz::new(seed))),
            record: Mutex::default(),
            schemas: Mutex::new(vec![schema.clone()]),
            oversized: Mutex::default(),
        });
        let mut sim = Self {
            kind,
            fuzz,
            workers,
            events_off: false,
            behind_log: BTreeSet::new(),
            rng: Rng::new(seed),
            server,
            net,
            room: RoomId::new("room-sim").unwrap(),
            schema_id: SchemaId::new("sim-0").unwrap(),
            schema,
            schemas: 0,
            columns: 0,
            incarnations: Vec::new(),
            members: Vec::new(),
            tags: 0,
        };
        let request = json!({"schema_id": sim.schema_id, "schema": sim.schema});
        sim.admin(Method::POST, "/schemas", Some(request)).await;
        sim.create_room().await;
        for (person, disk, worker) in [
            ("Z", false, None),
            ("A", true, None),
            ("B", true, Some(Freshness::Live)),
            ("B", false, Some(sim.freshness())),
            ("A", false, None),
        ] {
            sim.join(person, disk, worker.filter(|_| workers)).await;
        }
        sim
    }

    /// A random mode for a worker, `Live` half of the time.
    fn freshness(&mut self) -> Freshness {
        match self.rng.below(4) {
            0 | 1 => Freshness::Live,
            2 => Freshness::Periodic,
            _ => Freshness::Manual,
        }
    }

    /// Sends `body` to `path` of the Control Plane, and returns the JSON of its answer.
    async fn admin(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let body = body.map_or_else(Body::empty, |body| Body::from(body.to_string()));
        let request = Request::builder()
            .method(method)
            .uri(format!("/admin{path}"))
            .header(AUTHORIZATION, format!("Bearer {ADMIN_SECRET}"))
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap();
        let response = build_router(self.server.state.clone()).oneshot(request);
        let response = response.await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(status.is_success(), "{path}: {status} {text}");
        serde_json::from_slice(&body).unwrap_or_default()
    }

    /// Creates the room with its schema, through the Control Plane: a new incarnation, with a log
    /// of its own.
    async fn create_room(&mut self) {
        let request = json!({"room_id": self.room, "schema_id": self.schema_id});
        self.admin(Method::POST, "/rooms", Some(request)).await;
        let id = self.server.incarnation(&self.room).await;
        self.incarnations
            .push(Incarnation::new(id, self.schema.clone()));
        self.net.record.lock().unwrap().log.clear();
    }

    /// Opens client `index` of `person` over a memory engine, or over the disk engine of `dir`,
    /// and the room in it, with its worker in `worker`, if any.
    async fn open(
        &self,
        index: usize,
        person: &'static str,
        dir: Option<TempDir>,
        worker: Option<Freshness>,
    ) -> Member {
        let link = Link {
            net: Arc::clone(&self.net),
            client: index,
            faulty: index != ARCHIVIST,
        };
        let metadata = Shared(match &dir {
            Some(dir) => Arc::new(FileMetadata::new(dir.path())),
            None => Arc::new(MemoryMetadata::default()),
        });
        let tokens = TestTokens::new();
        // A client that crashed holds its metadata until the tasks it left end, those of its
        // worker too.
        let mut opened = None;
        for _ in 0..6000 {
            let attempt = match &dir {
                Some(dir) => {
                    let storage = DiskStorageEngine::new(options(dir.path())).unwrap();
                    client(person, storage, &metadata, (&link, &tokens)).await
                }
                None => {
                    let storage = MemoryStorageEngine::new();
                    client(person, storage, &metadata, (&link, &tokens)).await
                }
            };
            match attempt {
                Ok(client) => {
                    opened = Some(client);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        let (client, storage) = opened.expect("The metadata of a crashed client stay held");
        let incarnation = metadata.of(&self.room).await;
        let incarnation = incarnation.map(|metadata| self.incarnation(metadata.room_incarnation));
        let _ = open_room(&client, &self.room).await;
        if let Some(worker) = worker {
            let _ = client.set_freshness(self.room.clone(), worker).await;
        }
        Member {
            person,
            dir,
            client,
            storage,
            metadata,
            tokens,
            expiring: None,
            worker,
            since: incarnation.unwrap_or(self.incarnations.len() - 1),
            discards: u64::from(incarnation.is_none()),
            resetting: 0,
        }
    }

    /// The index of the incarnation `id` of the room.
    fn incarnation(&self, id: u128) -> usize {
        let index = self
            .incarnations
            .iter()
            .position(|incarnation| incarnation.id == id);
        index.expect("An incarnation of the room")
    }

    /// Adds a client of `person`, over a disk engine if `disk`, with the room open, and its
    /// worker in `worker`, if any.
    async fn join(&mut self, person: &'static str, disk: bool, worker: Option<Freshness>) {
        let dir = disk.then(|| tempdir().unwrap());
        let member = self.open(self.members.len(), person, dir, worker).await;
        self.members.push(member);
    }

    /// Crashes client `index`, over a disk engine, and opens it again from its directory, with its
    /// worker in the mode it had: the tasks it left run on.
    async fn crash(&mut self, index: usize) {
        let mut crashed = self.members.remove(index);
        let (person, dir, worker) = (crashed.person, crashed.dir.take(), crashed.worker);
        drop(crashed);
        let member = self.open(index, person, dir, worker).await;
        self.members.insert(index, member);
    }

    /// Waits until the server is done with every request and every opening of an event stream,
    /// and the replicas with the changes that dropped calls left: the paused clock moves only
    /// once every task waits. Fails if they do not end within ten minutes.
    async fn settle(&self) {
        let server = self.net.server.lock().unwrap().clone();
        let settled = async {
            loop {
                server.settled().await;
                if self.net.openings.load(Ordering::SeqCst) == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let settled = tokio::time::timeout(Duration::from_secs(600), settled).await;
        settled.expect("A request or an opening of an event stream does not end");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    /// Restarts the server over its directory, pruning the log of its rooms by quota at each
    /// maintenance if `pruning`, and with its event stream turned off if `events_off`. The server
    /// takes no request meanwhile, and restarts once it is done with those it took: the workers
    /// send theirs at any time. Shutting its rooms down ends their event streams.
    async fn restart(&mut self, pruning: bool) {
        self.net.down.store(true, Ordering::SeqCst);
        self.settle().await;
        let quota = match pruning {
            true => 0,
            false => ServerConfig::default().max_room_disk_bytes,
        };
        let streams = self.events_off.then_some(0);
        let restarted = self.server.restart(|config| {
            config.max_room_disk_bytes = quota;
            config.max_sse_streams = streams;
        });
        restarted.await;
        *self.net.server.lock().unwrap() = self.server.transport();
        self.net.down.store(false, Ordering::SeqCst);
    }

    /// Prunes every sealed segment of the log, wherever the cursors of the clients are: a
    /// restart with a quota of nothing, until a maintenance of the room prunes the log, or a
    /// minute if it has no sealed segment. Returns whether it pruned the log.
    async fn prune(&mut self) -> bool {
        self.restart(true).await;
        let manager = &self.server.state.room_manager;
        let tail = manager.log_bounds(&self.room).await.unwrap().tail_seq;
        let mut pruned = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            pruned = manager.log_bounds(&self.room).await.unwrap().tail_seq > tail;
            if pruned {
                break;
            }
        }
        self.restart(false).await;
        pruned
    }

    /// Changes the schema of the room in the model to `schema`.
    fn evolve(&mut self, schema: Schema) {
        let incarnation = self.incarnations.last_mut().unwrap();
        incarnation.model.evolve(schema.clone());
        self.net.schemas.lock().unwrap().push(schema.clone());
        self.schema = schema;
    }

    /// Adds a column to a table of the schema of the room through the Control Plane, with
    /// constraints over it over random schemas.
    async fn add_column(&mut self) {
        self.columns += 1;
        let schema = match self.kind {
            Kind::Random => oracle::add_random_column(&mut self.rng, &self.schema, self.columns),
            Kind::Ranking => plain_column(&mut self.rng, &self.schema, self.columns),
        };
        let path = format!("/schemas/{}/columns", self.schema_id);
        let request = added_column(&self.schema, &schema);
        self.admin(Method::POST, &path, Some(request)).await;
        self.evolve(schema);
    }

    /// Moves the room to a new schema that extends its own with a column and keeps its
    /// constraints: shuts the room down and spawns it with that schema, until the Control Plane
    /// reports it: a request of a worker in between may spawn it again with its schema.
    async fn reassign(&mut self) {
        self.columns += 1;
        self.schemas += 1;
        let schema = plain_column(&mut self.rng, &self.schema, self.columns);
        let schema_id = SchemaId::new(format!("sim-{}", self.schemas)).unwrap();
        // A schema registered starts at its first version.
        let mut registered = serde_json::to_value(&schema).unwrap();
        registered["version"] = json!(Schema::FIRST_VERSION);
        let request = json!({"schema_id": schema_id, "schema": registered});
        self.admin(Method::POST, "/schemas", Some(request)).await;
        let manager = &self.server.state.room_manager;
        let path = format!("/rooms/{}", self.room);
        for attempt in 0.. {
            if self.admin(Method::GET, &path, None).await["schema_id"] == json!(schema_id) {
                break;
            }
            assert!(attempt < 100, "The room does not move to its new schema");
            manager.shutdown_room(&self.room).await;
            manager
                .get_or_spawn(&self.room, Some(&schema_id))
                .await
                .unwrap();
        }
        self.schema_id = schema_id;
        self.evolve(schema);
    }

    /// Deletes the room and creates it again.
    async fn recreate(&mut self) {
        let path = format!("/rooms/{}", self.room);
        self.admin(Method::DELETE, &path, None).await;
        self.create_room().await;
    }

    /// Before step `step`, an event of the server: every few steps, the next one, and a few more
    /// at random. With workers, a restart turns the event stream off now and then, and the next
    /// one on again. A pruned room gets a client that joins it, every other with a worker.
    async fn event(&mut self, step: usize) {
        const EVENTS: usize = 8;
        let event = match step % EVENTS {
            7 => step / EVENTS,
            _ if self.rng.chance(3) => self.rng.below(EVENTS as u64) as usize,
            _ => return,
        };
        match event % EVENTS {
            0 | 4 => {
                self.events_off = self.workers && !self.events_off && self.rng.chance(30);
                self.restart(false).await
            }
            1 | 5 => {
                if self.prune().await && self.members.len() < 8 {
                    let n = self.members.len();
                    let person = match self.kind {
                        Kind::Random => ["A", "B"][n % 2],
                        Kind::Ranking => "C",
                    };
                    let worker = (self.workers && n % 2 == 1).then(|| self.freshness());
                    self.join(person, n % 2 == 0, worker).await;
                }
            }
            2 | 6 => self.add_column().await,
            3 => self.reassign().await,
            _ => self.recreate().await,
        }
    }

    /// `op` tagged with a timestamp of its own, by which the network and the log tell its write.
    fn tag(&mut self, op: Operation) -> Operation {
        self.tags += 1;
        Operation::new(op.table_id(), op.pk().clone(), self.tags, op.kind().clone())
    }

    /// A random operation for client `member`: over random schemas, of its own owner but for a
    /// few, which the server refuses.
    fn operation(&mut self, member: usize) -> Operation {
        let person = self.members[member].person;
        let op = match self.kind {
            Kind::Random => {
                let client_id = self.members[member].client.client_id();
                let mut op = oracle::random_operation(&mut self.rng, &self.schema);
                while self.schema.check_owner(&op, &client_id).is_err() && !self.rng.chance(10) {
                    op = oracle::random_operation(&mut self.rng, &self.schema);
                }
                op
            }
            Kind::Ranking => ranking_operation(&mut self.rng, person),
        };
        self.tag(op)
    }

    /// What the clients do in a random step, none, one or two calls each: some of the calls
    /// dropped at a random await, and the client crashed afterwards if it is over a disk engine.
    /// A client with a worker changes its mode now and then. The token provider of a client gives
    /// one already expired now and then, once it gave the one before.
    fn plans(&mut self) -> Vec<Plan> {
        for member in &mut self.members[ARCHIVIST + 1..] {
            // Drawn first: the renewals depend on the faults, which a seed does not repeat.
            let expired = self.rng.chance(5);
            let calls = member.tokens.calls();
            if expired && member.expiring.is_none_or(|asked| calls > asked) {
                member.tokens.then(Issue::Valid(Duration::ZERO));
                member.expiring = Some(calls);
            }
        }
        let mut plans = Vec::new();
        for member in (1..self.members.len()).flat_map(|member| [member, member]) {
            if !self.rng.chance(50) {
                continue;
            }
            let worker = self.members[member].worker.is_some();
            let action = match self.rng.below(10) {
                _ if worker && self.rng.chance(10) => Action::Freshness(self.freshness()),
                0..=4 => Action::Write(self.operation(member)),
                5 | 6 => Action::Sync,
                7 | 8 => Action::Heartbeat,
                _ => Action::Reopen,
            };
            // Most calls end within a dozen polls.
            let polls = self.rng.chance(15).then(|| 1 + self.rng.below(12));
            let disk = self.members[member].dir.is_some();
            let crash = polls.is_some() && disk && self.rng.chance(40);
            plans.push(Plan {
                member,
                action,
                polls,
                crash,
            });
        }
        plans
    }

    /// Runs `plans` at once and crashes the clients they say; with workers, they then run by
    /// themselves for a random time within an interval. Then the archivist catches up, every write
    /// that ended and every replica are checked, and each client with a worker starts it again if
    /// it stopped.
    async fn step(&mut self, plans: &[Plan]) {
        let last = self.incarnations.last().map(|incarnation| incarnation.id);
        for plan in plans {
            if let Action::Reopen = plan.action {
                self.members[plan.member].opening(&self.room, last).await;
            }
        }
        let room_id = &self.room;
        let mut before = Vec::new();
        for (index, member) in self.members.iter().enumerate() {
            let status = member.client.status(room_id.clone()).await;
            before.push((
                status.ok().map(|status| status.epoch),
                self.net.downloads(index),
            ));
        }
        let may_wait = self.net.fuzz.lock().unwrap().is_some();
        let net = &self.net;
        let outcomes = join_all(plans.iter().map(|plan| {
            let client = &self.members[plan.member].client;
            let action = async move {
                match &plan.action {
                    Action::Write(op) => client.write(room_id.clone(), op.clone()).await,
                    Action::Sync => client.sync(room_id.clone()).await,
                    Action::Heartbeat => client.heartbeat(room_id.clone()).await,
                    Action::Reopen => {
                        let _ = client.close_room(room_id.clone()).await;
                        open_room(client, room_id).await
                    }
                    Action::Freshness(freshness) => {
                        client.set_freshness(room_id.clone(), *freshness).await
                    }
                }
            };
            // The epoch and the downloads of the client once a write ended: what its replica may
            // have skipped meanwhile.
            async move {
                let outcome = cut(action, plan.polls, may_wait).await;
                let status = client.status(room_id.clone()).await;
                let ended = (
                    status.ok().map(|status| status.epoch),
                    net.downloads(plan.member),
                );
                (outcome, ended)
            }
        }))
        .await;
        let crashed: BTreeSet<usize> = plans.iter().filter(|p| p.crash).map(|p| p.member).collect();
        for &member in &crashed {
            self.crash(member).await;
        }
        if self.workers {
            let idle = self.rng.below(INTERVAL.as_millis() as u64);
            tokio::time::sleep(Duration::from_millis(idle)).await;
        }
        self.settle().await;
        self.archive().await;
        for (plan, (outcome, (epoch_then, downloads_then))) in plans.iter().zip(outcomes) {
            let (Action::Write(op), Some(result)) = (&plan.action, outcome) else {
                continue;
            };
            let (epoch, downloads) = before[plan.member];
            let epoch_changed = epoch_then != epoch;
            let downloaded = downloads_then > downloads;
            let reopened = plans
                .iter()
                .any(|other| other.member == plan.member && matches!(other.action, Action::Reopen));
            let skipped = crashed.contains(&plan.member) || epoch_changed || downloaded || reopened;
            self.check_write(plan.member, op, result, skipped);
        }
        for (index, member) in self.members.iter().enumerate() {
            let of = |plan: &&Plan| plan.member == index;
            let reopened = plans
                .iter()
                .filter(of)
                .any(|p| matches!(p.action, Action::Reopen));
            let changed =
                (plans.iter().filter(of)).any(|p| matches!(p.action, Action::Freshness(_)));
            let closed = reopened && changed && !crashed.contains(&index);
            self.check(member, closed).await;
        }
        // The discards of the step that the opens did add stay; the others go.
        for member in &mut self.members {
            if let Ok(status) = member.client.status(self.room.clone()).await {
                let after = (self.incarnations.len() - 1 - member.since) as u64;
                let added = status.epoch.saturating_sub(member.discards + after);
                member.discards += added.min(member.resetting);
                member.resetting = 0;
            }
        }
        self.run_workers().await;
    }

    /// Has each client with a worker run it, in the mode it has, as an application does: also
    /// once a crash, or a close of its room, stopped it, and before its room opens again, so
    /// that its first heartbeat registers the replica that a registration that failed left empty.
    async fn run_workers(&mut self) {
        for member in &mut self.members {
            let Some(mut freshness) = member.worker else {
                continue;
            };
            // That of a step that changed it, also if the step was cut.
            if let Ok(status) = member.client.status(self.room.clone()).await {
                if let Some(worker) = status.worker {
                    freshness = worker.freshness;
                }
            }
            member.worker = Some(freshness);
            let _ = member
                .client
                .set_freshness(self.room.clone(), freshness)
                .await;
        }
    }

    /// Has the archivist catch up with the room, and the model apply what it received.
    async fn archive(&mut self) {
        let archivist = &self.members[ARCHIVIST].client;
        let _ = archivist.sync(self.room.clone()).await;
        let manager = &self.server.state.room_manager;
        let head = manager.log_bounds(&self.room).await.unwrap().head_seq.get();
        let status = archivist.status(self.room.clone()).await.unwrap();
        assert_eq!(status.head_seq.get(), head, "The archivist is behind");
        let incarnation = self.incarnations.last_mut().unwrap();
        let record = self.net.record.lock().unwrap();
        let next = incarnation.log.len() as u64 + 1;
        let log = (next..=head).map(|seq| {
            let op = record
                .log
                .get(&seq)
                .expect("The archivist received the whole log");
            SequencedOperation::new(seq, op.clone())
        });
        incarnation.extend(log.collect());
    }

    /// Checks how a write of `op` by client `member` ended: with the decision of the model on
    /// the sequence its commit got; as uncertain only if an attempt was and no `CommitAck` came,
    /// or one came and its replica may have `skipped` the sequence, discarded, with a snapshot or
    /// closed; and with any other error only if it had no effect. Fuzzing, a reply in place of the
    /// server's may end it with any error.
    fn check_write(
        &self,
        member: usize,
        op: &Operation,
        result: Result<(), ClientError>,
        skipped: bool,
    ) {
        let record = self.net.record.lock().unwrap();
        let commits = record.commits.get(&op.timestamp());
        let incarnation = self.incarnations.last().unwrap();
        let landed = incarnation.landed(op.timestamp());
        let acks: Vec<u64> = commits.map_or(Vec::new(), |c| c.acks.iter().copied().collect());
        let uncertain = commits.is_some_and(|c| c.sent > c.answered);
        let write = format!(
            "The write of {op:?} by client {member} ended with {result:?}, with acks {acks:?}, \
             {uncertain} uncertain and in the log at {landed:?}"
        );
        assert!(
            commits.is_none_or(|c| c.ids.len() == 1),
            "{write}: other ids"
        );
        assert!(landed.len() <= 1, "{write}: sequenced twice");
        assert!(acks.is_empty() || acks == landed, "{write}");
        match result {
            Ok(()) | Err(ClientError::ConstraintViolation(_) | ClientError::RowMissing) => {
                assert_eq!(acks.len(), 1, "{write}: without a CommitAck");
                let expected = decision(incarnation.rejected.get(&acks[0]));
                assert_eq!(result, expected, "{write}");
            }
            _ if self.fuzz => {}
            Err(ClientError::OutcomeUnknown) => match acks.is_empty() {
                true => assert!(uncertain, "{write}"),
                false => assert!(skipped, "{write}: its replica applied it"),
            },
            Err(_) => assert!(!uncertain && landed.is_empty(), "{write}: it had an effect"),
        }
    }

    /// Checks the replica of `member`, if its room is open: it holds what the model holds at its
    /// head; it was discarded only once per open without metadata and once per incarnation after
    /// that of its metadata when the client opened, unless fuzzing; it does not fail closed; its
    /// room keeps nothing for a write, with none in flight; and its worker, if it has one, runs,
    /// unless the step `closed` its room while it changed the mode of the worker: a worker that the
    /// change started while the room closed stops at its next step, as the room is out of the
    /// client. Its worker may change the replica meanwhile: its rows are read again until its head
    /// and its epoch are the same before and after reading them.
    async fn check(&self, member: &Member, closed: bool) {
        let client = member.client.client_id();
        let mut unread = String::new();
        for _ in 0..1000 {
            let Ok(status) = member.client.status(self.room.clone()).await else {
                return;
            };
            assert!(!status.failed_closed, "{client} fails closed");
            let kept = (status.writes_in_flight, status.retained);
            assert_eq!(kept, (0, 0), "{client} keeps something for its writes");
            let after = self.incarnations.len() - 1 - member.since;
            let discards = member.discards + member.resetting + after as u64;
            assert!(
                self.fuzz || status.epoch <= discards,
                "{client} discarded a sound replica"
            );
            if let Some(worker) = &status.worker {
                let room_closed = Some(ClientError::RoomClosed(self.room.clone()));
                let stopped = closed && worker.last_error == room_closed;
                assert!(
                    worker.running || stopped,
                    "The worker of {client} stopped: {worker:?}"
                );
            }
            let metadata = member.metadata.of(&self.room).await;
            let replica = rows(&*member.storage, &self.room, &self.schema).await;
            let again = member.client.status(self.room.clone()).await;
            let still = again.is_ok_and(|again| {
                (again.epoch, again.head_seq) == (status.epoch, status.head_seq)
            });
            match (metadata, replica, still) {
                (Some(metadata), Ok(replica), true) => {
                    let incarnation = self.incarnation(metadata.room_incarnation);
                    let incarnation = &self.incarnations[incarnation];
                    let head = status.head_seq.get() as usize;
                    assert!(
                        head <= incarnation.log.len(),
                        "{client} is ahead of the log"
                    );
                    let model = incarnation.rows(head, &self.schema);
                    assert_eq!(trimmed(&replica), trimmed(&model), "{client} at {head}");
                    return;
                }
                (metadata, replica, _) => {
                    let incarnation = metadata.map(|metadata| metadata.room_incarnation);
                    unread = format!("{incarnation:?}, {:?}", replica.err());
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("The replica of {client} does not hold still: {unread}");
    }

    /// Sets the mode of the worker of each client that runs one, in turn `Periodic`, `Live` and
    /// `Manual`, as an application does, so that the network heals with each mode.
    async fn assign_modes(&mut self) {
        let modes = [Freshness::Periodic, Freshness::Live, Freshness::Manual];
        let workers = self
            .members
            .iter_mut()
            .filter(|member| member.worker.is_some());
        for (member, freshness) in workers.zip(modes.into_iter().cycle()) {
            member.worker = Some(freshness);
            let _ = member
                .client
                .set_freshness(self.room.clone(), freshness)
                .await;
        }
    }

    /// The clients whose worker syncs by itself.
    fn syncing(&self) -> Vec<usize> {
        let syncs =
            |member: &Member| matches!(member.worker, Some(Freshness::Live | Freshness::Periodic));
        (0..self.members.len())
            .filter(|&index| syncs(&self.members[index]))
            .collect()
    }

    /// Leaves the clients with a worker without network while the others write, until the log is
    /// past their heads by ten sequences if `pruning`, or by one: once they are back, they are
    /// behind the head. If `pruning`, the log is pruned too: those whose worker syncs by itself
    /// are behind the log, most of them, and catch up from a snapshot that another client
    /// uploads, such as the archivist, at the head.
    async fn isolate(&mut self, pruning: bool) {
        let workers: BTreeSet<usize> = (self.members.iter().enumerate())
            .filter(|(_, member)| member.worker.is_some())
            .map(|(index, _)| index)
            .collect();
        // With their room open, as their application leaves it, so that their heads are known.
        for &index in &workers {
            self.opened(index).await;
        }
        self.run_workers().await;
        *self.net.offline.lock().unwrap() = workers.iter().copied().collect();
        // What they took before that, and the changes it brought.
        self.settle().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut top = 0;
        for &index in &workers {
            if let Ok(status) = self.members[index].client.status(self.room.clone()).await {
                top = top.max(status.head_seq.get());
            }
        }
        let past = top + if pruning { 10 } else { 1 };
        let writers: Vec<usize> = (ARCHIVIST + 1..self.members.len())
            .filter(|index| !workers.contains(index))
            .collect();
        for _ in 0..30 {
            if self.incarnations.last().unwrap().log.len() as u64 >= past {
                break;
            }
            let plans: Vec<Plan> = writers
                .iter()
                .map(|&index| Plan::new(index, Action::Write(self.operation(index))))
                .collect();
            self.step(&plans).await;
        }
        let head = self.incarnations.last().unwrap().log.len() as u64;
        assert!(head >= past, "The others write nothing: {head}, not {past}");
        if !pruning {
            return;
        }
        assert!(self.prune().await, "The log was not pruned");
        let manager = &self.server.state.room_manager;
        let tail = manager.log_bounds(&self.room).await.unwrap().tail_seq.get();
        for index in self.syncing() {
            let status = self.members[index].client.status(self.room.clone()).await;
            if status.is_ok_and(|status| status.head_seq.get() + 1 < tail) {
                self.behind_log.insert(index);
            }
        }
        assert!(
            self.syncing().is_empty() || !self.behind_log.is_empty(),
            "No client with a worker is behind the log at {tail}"
        );
    }

    /// Whether `member` is registered at the head of the room, in its last incarnation.
    async fn at_head(&self, member: &Member) -> bool {
        let incarnation = self.incarnations.last().unwrap();
        let status = member.client.status(self.room.clone()).await;
        let metadata = member.metadata.of(&self.room).await;
        status.is_ok_and(|status| {
            status.registered && status.head_seq.get() as usize == incarnation.log.len()
        }) && metadata.is_some_and(|metadata| metadata.room_incarnation == incarnation.id)
    }

    /// Gives the clients with a worker their network back, and has every client open its room:
    /// those whose worker syncs by itself, all of them behind the head, reach it within
    /// [`HEAL_INTERVALS`], with no other call, and those behind the log, with a snapshot; while
    /// those without a worker send a heartbeat every interval, the archivist too, as their
    /// application does: the server may designate one of them to upload a snapshot.
    async fn rejoin(&mut self) {
        let syncing = self.syncing();
        if !self.net.offline.lock().unwrap().is_empty() {
            for &index in &syncing {
                let member = &self.members[index];
                let client = member.client.client_id();
                assert!(
                    !self.at_head(member).await,
                    "{client} is at the head without network"
                );
            }
        }
        let behind_log = std::mem::take(&mut self.behind_log);
        let downloads: Vec<u64> = behind_log.iter().map(|&i| self.net.downloads(i)).collect();
        self.net.offline.lock().unwrap().clear();
        let back = tokio::time::Instant::now();
        for member in 0..self.members.len() {
            self.opened(member).await;
        }
        self.run_workers().await;
        let mut beat = back;
        loop {
            let mut behind = Vec::new();
            for &index in &syncing {
                let member = &self.members[index];
                if !self.at_head(member).await {
                    behind.push(member.client.client_id());
                }
            }
            if behind.is_empty() {
                for (&index, downloads) in behind_log.iter().zip(downloads) {
                    let client = self.members[index].client.client_id();
                    let downloaded = self.net.downloads(index) > downloads;
                    assert!(
                        downloaded,
                        "{client} left the log behind without a snapshot"
                    );
                }
                return;
            }
            let elapsed = back.elapsed();
            assert!(
                elapsed <= INTERVAL * HEAL_INTERVALS,
                "{behind:?} did not reach the head of the room by themselves in {elapsed:?}"
            );
            if tokio::time::Instant::now() >= beat {
                for member in &self.members {
                    if member.worker.is_none() {
                        let _ = member.client.heartbeat(self.room.clone()).await;
                    }
                }
                beat += INTERVAL;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Heals the network, and has every client open its room until all of them are registered at
    /// the head of the room, in its last incarnation. First, those whose worker syncs by itself
    /// get there by themselves ([`rejoin`](Self::rejoin)); with workers, once more after they were
    /// without network again while the others wrote, behind the head alone. Then those without a
    /// worker sync and send heartbeats, and those whose worker runs in `Manual` sync. No panic
    /// happened meanwhile.
    async fn heal(&mut self) {
        self.net.faulty.store(false, Ordering::SeqCst);
        // The replies in place of the server's may have left a replica waiting for a snapshot that
        // the server does not announce, until its room opens again.
        if self.net.fuzz.lock().unwrap().take().is_some() {
            for member in &self.members[ARCHIVIST + 1..] {
                let _ = member.client.close_room(self.room.clone()).await;
            }
        }
        self.rejoin().await;
        if self.workers {
            self.isolate(false).await;
            self.rejoin().await;
        }
        for _ in 0..60 {
            for member in 0..self.members.len() {
                self.opened(member).await;
            }
            self.run_workers().await;
            for action in [Action::Sync, Action::Heartbeat] {
                let calls = |member: &Member| match member.worker {
                    None => true,
                    Some(Freshness::Manual) => matches!(action, Action::Sync),
                    Some(_) => false,
                };
                let members = (1..self.members.len()).filter(|&i| calls(&self.members[i]));
                let plans: Vec<Plan> = members.map(|i| Plan::new(i, action.clone())).collect();
                self.step(&plans).await;
            }
            let mut done = true;
            for member in &self.members {
                done &= self.at_head(member).await;
            }
            if done {
                let panics = PANICS.load(Ordering::SeqCst);
                assert_eq!(
                    panics, 0,
                    "A panic in the process, which a task may have caught"
                );
                return;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        panic!("The clients did not reach the head of the room");
    }

    /// Opens the room in client `member` until it is open. Each attempt counts as an open without
    /// metadata if the client has none, also that of an empty replica, which does not discard it.
    async fn opened(&mut self, member: usize) {
        for _ in 0..100 {
            let member = &mut self.members[member];
            if member.client.status(self.room.clone()).await.is_ok() {
                return;
            }
            member.opening(&self.room, None).await;
            let _ = open_room(&member.client, &self.room).await;
        }
        panic!("The room does not open");
    }

    /// Syncs client `member` until a sync ends well.
    async fn synced(&self, member: usize) {
        for _ in 0..100 {
            if self.members[member]
                .client
                .sync(self.room.clone())
                .await
                .is_ok()
            {
                return;
            }
        }
        panic!("Every sync fails");
    }

    /// Writes each of `writes` at once, as its client, and again in the next steps until it is in
    /// the log; returns the decision of the model on each.
    async fn land(&mut self, writes: &[(usize, Operation)]) -> Vec<Option<Reason>> {
        let mut decisions = vec![None; writes.len()];
        let mut pending: Vec<usize> = (0..writes.len()).collect();
        for _ in 0..30 {
            let plans: Vec<(usize, Plan)> = pending
                .iter()
                .map(|&i| {
                    let op = self.tag(writes[i].1.clone());
                    (i, Plan::new(writes[i].0, Action::Write(op)))
                })
                .collect();
            let steps: Vec<Plan> = plans.iter().map(|(_, plan)| plan.clone()).collect();
            self.step(&steps).await;
            let incarnation = self.incarnations.last().unwrap();
            pending.clear();
            for (i, plan) in plans {
                let Action::Write(op) = plan.action else {
                    unreachable!()
                };
                match incarnation.landed(op.timestamp())[..] {
                    [seq] => decisions[i] = incarnation.rejected.get(&seq).cloned(),
                    _ => pending.push(i),
                }
            }
            if pending.is_empty() {
                return decisions;
            }
        }
        panic!("Writes that never reach the log");
    }

    /// The points of each candidate in the model: one per vote.
    fn standings(&self) -> BTreeMap<String, usize> {
        let model = &self.incarnations.last().unwrap().model;
        let mut points = BTreeMap::new();
        for vote in model
            .rows()
            .get(&VOTES)
            .into_iter()
            .flat_map(|votes| votes.values())
        {
            let Value::String(candidate) = &vote[3] else {
                panic!("A candidate is a person");
            };
            *points.entry(candidate.to_string()).or_default() += 1;
        }
        points
    }
}

/// `count` seeds from `first`, or as many as `SIM_SEEDS` from `SIM_SEED`.
fn seeds(first: u64, count: u64) -> std::ops::Range<u64> {
    let setting =
        |name, default| std::env::var(name).map_or(default, |value| value.parse().unwrap());
    let first = setting("SIM_SEED", first);
    first..first + setting("SIM_SEEDS", count)
}

/// The faults of the event streams and of the tokens.
const FAULTS: [&str; 10] = [
    "event lost",
    "event late",
    "lagged",
    "stream cut",
    "stream silent",
    "stream expired",
    "opening lost",
    "opening expired",
    "opening refused",
    "token expired",
];

/// A simulation of `steps` random steps over `kind`, fuzzing the replies if `fuzz`, and else with
/// workers, with the network healed at the end: then some event with operations reached a client,
/// and the event streams or their openings had faults. Returns how many times the network
/// injected each fault of the event streams and of the tokens.
async fn simulate(
    seed: u64,
    kind: Kind,
    steps: usize,
    fuzz: bool,
) -> BTreeMap<&'static str, usize> {
    let mut sim = Sim::new(seed, kind, fuzz, !fuzz).await;
    for step in 0..steps {
        sim.event(step).await;
        let plans = sim.plans();
        sim.step(&plans).await;
    }
    if sim.workers {
        sim.assign_modes().await;
        sim.isolate(true).await;
    }
    sim.heal().await;
    if sim.workers {
        let ops = sim.net.ops.load(Ordering::SeqCst);
        assert!(ops > 0, "No event with operations reached a client");
    }
    let faults = sim.net.faults.lock().unwrap().clone();
    // Of a block or of an opening: some runs have few blocks of their own.
    let streams = FAULTS[..9]
        .iter()
        .filter_map(|fault| faults.get(fault))
        .sum::<usize>();
    assert!(
        fuzz || streams > 0,
        "No fault of an event stream: {faults:?}"
    );
    faults
}

#[tokio::test(start_paused = true)]
async fn replicas_converge_through_faults_restarts_pruning_and_schema_changes() {
    for seed in seeds(0, 1) {
        simulate(seed, Kind::Random, 96, false).await;
    }
}

#[tokio::test(start_paused = true)]
async fn rankings_converge_through_faults_restarts_pruning_and_schema_changes() {
    for seed in seeds(0, 1) {
        simulate(seed, Kind::Ranking, 96, false).await;
    }
}

#[tokio::test(start_paused = true)]
#[ignore = "long-running simulation; run with --ignored in release"]
async fn replicas_converge_over_many_long_simulations() {
    let mut faults = BTreeMap::new();
    for seed in seeds(1, 16) {
        for kind in [Kind::Random, Kind::Ranking] {
            for (fault, count) in simulate(seed, kind, 400, false).await {
                *faults.entry(fault).or_insert(0) += count;
            }
        }
    }
    // Every fault of the event streams and of the tokens, and a token that the provider gave
    // already expired, which the server refused.
    for fault in FAULTS.iter().chain(&["token refused by the server"]) {
        assert!(
            faults.get(fault).is_some_and(|count| *count > 0),
            "{fault}: {faults:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn no_reply_in_place_of_the_servers_panics_a_client_or_moves_its_replica_off_the_log() {
    for seed in seeds(0, 1) {
        simulate(seed, Kind::Random, 96, true).await;
        simulate(seed, Kind::Ranking, 96, true).await;
    }
}

#[tokio::test(start_paused = true)]
#[ignore = "long-running simulation; run with --ignored in release"]
async fn no_reply_in_place_of_the_servers_panics_a_client_over_many_long_simulations() {
    for seed in seeds(1, 16) {
        simulate(seed, Kind::Random, 400, true).await;
        simulate(seed, Kind::Ranking, 400, true).await;
    }
}

/// The scenarios of the reference app, over the faulty network: the members of a ranking vote and
/// every replica reaches the same standings; what breaks a rule of the ranking is rejected; a
/// person votes from two devices; and a member joins the ranking after its log was pruned, from
/// the snapshot of another member.
#[tokio::test(start_paused = true)]
async fn the_ranking_converges_and_rejects_what_breaks_its_rules() {
    let mut sim = Sim::new(seeds(0, 1).start, Kind::Ranking, false, false).await;
    // A and B over disk engines, and the second device of B over a memory engine.
    let (a, b, b2) = (1, 2, 3);
    let (r, first, second) = (uuid(1), uuid(11), uuid(12));
    let rejected = |reason: fn(String) -> Reason, name: &str| Some(reason(name.to_string()));
    assert_eq!(sim.land(&[(a, ranking(r.clone(), "A"))]).await, [None]);
    let members = [(a, member(r.clone(), "A")), (b, member(r.clone(), "B"))];
    assert_eq!(sim.land(&members).await, [None, None]);
    let rounds = [(a, round(r.clone(), first.clone(), "A", "Ronda 1"))];
    assert_eq!(sim.land(&rounds).await, [None]);
    let same_title = [(b, round(r.clone(), second.clone(), "B", "Ronda 1"))];
    let unique = rejected(Reason::Unique, "rounds_title");
    assert_eq!(sim.land(&same_title).await, [unique]);
    // B votes from both devices at once: one vote is taken, and the other is a duplicate.
    let votes = [
        (a, vote("A", r.clone(), first.clone(), "B")),
        (b, vote("B", r.clone(), first.clone(), "A")),
        (b2, vote("B", r.clone(), first.clone(), "A")),
    ];
    let mut votes = sim.land(&votes).await;
    votes[1..].sort_by_key(Option::is_some);
    assert_eq!(votes, [None, None, Some(Reason::Duplicate)]);
    let rounds = [(a, round(r.clone(), second.clone(), "A", "Ronda 2"))];
    assert_eq!(sim.land(&rounds).await, [None]);
    let not_a_member = [(a, vote("A", r.clone(), second.clone(), "C"))];
    let candidate = rejected(Reason::ForeignKey, "votes_candidate");
    assert_eq!(sim.land(&not_a_member).await, [candidate]);
    // C joins the ranking once its log is pruned, from the snapshot that A uploads: A reaches the
    // head before the log is pruned and confirms it last, so the server designates it.
    sim.synced(a).await;
    assert!(sim.prune().await);
    sim.synced(a).await;
    sim.join("C", true, None).await;
    let c = sim.members.len() - 1;
    sim.opened(c).await;
    for _ in 0..20 {
        if sim.net.uploads(a) > 0 {
            break;
        }
        sim.step(&[Plan::new(a, Action::Heartbeat)]).await;
    }
    let voter = rejected(Reason::ForeignKey, "votes_voter");
    let early = [(c, vote("C", r.clone(), second.clone(), "A"))];
    assert_eq!(sim.land(&early).await, [voter]);
    assert_eq!(sim.land(&[(c, member(r.clone(), "C"))]).await, [None]);
    assert_eq!(sim.land(&early).await, [None]);
    // A member of another person keeps the creator of a ranking from deleting it.
    let other = uuid(2);
    assert_eq!(sim.land(&[(a, ranking(other.clone(), "A"))]).await, [None]);
    assert_eq!(sim.land(&[(b, member(other.clone(), "B"))]).await, [None]);
    let delete = Operation::delete_with_owner(RANKINGS, PrimaryKey::composite([other]), "A", 0);
    let restrict = rejected(Reason::Restrict, "members_ranking");
    assert_eq!(sim.land(&[(a, delete)]).await, [restrict]);
    sim.heal().await;
    assert!(sim.net.uploads(a) > 0 && sim.net.downloads(c) > 0);
    assert_eq!(sim.net.uploads(ARCHIVIST), 0);
    let standings = BTreeMap::from([("A".to_string(), 2), ("B".to_string(), 1)]);
    assert_eq!(sim.standings(), standings);
}
