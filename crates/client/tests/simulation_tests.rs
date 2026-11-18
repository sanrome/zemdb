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
//! With workers and without fuzzing, two clients also go through the facade, of two persons without
//! another client of the facade: one with the `CryptoEngine` of the tests and one without it, whose
//! writes and reads of an encrypted column are `Crypto`; each with its worker, in a random mode,
//! the first without the precheck and the second with it. They write what the others write, by the
//! names of the columns, with the plaintext of each encrypted value, which carries a mark, and now
//! and then with the owner column, which the facade does not send; and they open and drop `watch`
//! and `watch_scan` streams of random keys and ranges, each read by a task of its own, as an
//! application reads it. The core of the others writes the encrypted notes of the reference app
//! encrypted, as a member with the key does. After each step, also:
//! - every operation that a client of the facade sent declares the owner of its client;
//! - no request holds the mark, nor the snapshot of an upload, decoded;
//! - each write of the facade ended as a write of the core does, and one that the facade decides
//!   locally (`PrecheckFailed`, `Offline`, `InvalidOperation`, `Crypto`, `FailedClosed`) sent
//!   nothing; one with the owner column, also of the owner of the client, or with a key of another
//!   owner is `InvalidOperation`;
//! - with the streams read until they hold still, the last element of each is the value of a
//!   direct read, or an error after it, of a state in between; no stream gave its last value
//!   again; and the status of the room counts the subscriptions of the streams alive.
//!
//! Fuzzing, with workers too, the network also puts other replies in place of some of the
//! server's: random bytes, the reply damaged, random messages, errors of the request, and
//! snapshots that match their hash but install with no schema of the room, or announced larger
//! than the limit of the clients. In place of some openings of event streams, it puts refusals at
//! random; and in place of some of their blocks, random bytes, the event damaged, its frame
//! damaged, a line without an end, an `ops` event with a random message, with an operation that
//! does not validate, or with operations of the room that the log does not have at its
//! sequences, of another room or of another incarnation, and other events with random data. None
//! tells a client otherwise than the server what it takes on trust, nor brings it operations it
//! cannot tell from the server's. No client panics, no worker stops, every replica holds what the
//! model holds at its head, and no download goes past the first chunk of a snapshot larger than
//! the limit; a write may end with any error, and a call may wait for what those replies
//! announced, until it is dropped after ten minutes. Before the network heals, it fuzzes no more,
//! and the rooms open again.
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
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::Bytes;
use common::{
    additional_data, encoded, error_reply, options, refused, request, Issue, TestCrypto,
    TestServer, TestTokens, TestTransport, ADMIN_SECRET, AUTH_SECRET,
};
use futures::future::join_all;
use futures::stream::BoxStream;
use futures::StreamExt;
use oracle::{Oracle, Reason, Rows};
use rng::Rng;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::future::{poll_fn, Future};
use std::io;
use std::ops::Bound;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::{tempdir, TempDir};
use tokio::task::JoinHandle;
use tower::ServiceExt;
use zemdb_client::{
    Client, ClientConfig, ClientError, Direction, EventBlocks, EventStream, FileMetadata,
    Freshness, Key, MemoryMetadata, MetadataStore, RoomHandle, RoomMetadata, RoomStatus, Row, Scan,
    TableHandle, TransportClient, TransportError, Violation, ZemdbClient,
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

/// The room of the simulation, which keeps its id when it is created again.
const ROOM: &str = "room-sim";

/// The mark that the plaintext of each encrypted value carries, which no request holds.
const MARK: &[u8] = b"NOTES-IN-THE-CLEAR";

/// The most `watch` streams that a client of the facade keeps at once.
const WATCHES: usize = 4;

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
    /// The commits of each write, by its client and the timestamp of its operation: a tag of its
    /// own in a write of the core, and the wall clock in one of the facade, which writes once per
    /// step at most, a millisecond after the step before. Two clients of the facade write the same
    /// operation only in a table without an owner, which one of them writes at most in a step:
    /// elsewhere, each operation declares the owner of its client.
    commits: HashMap<(usize, u64), Commits>,
    /// The operations that each client of the facade sent in the step, by their mutation ids.
    sent: HashMap<usize, BTreeMap<MutationId, Operation>>,
    /// How many chunks of a snapshot each client received, and how many chunks of its uploads the
    /// relay took, whether their reply reached it or not.
    downloads: HashMap<usize, u64>,
    uploads: HashMap<usize, u64>,
    /// The chunks sent of each snapshot uploaded, by its hash, until they are all there, and the
    /// hashes of the snapshots decoded.
    uploaded: HashMap<[u8; 32], BTreeMap<u32, Bytes>>,
    decoded: HashSet<[u8; 32]>,
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
    /// How many times the checks saw each case of the facade.
    cases: Mutex<BTreeMap<&'static str, usize>>,
    /// The mark as the `CryptoEngine` of the tests encrypts it in a value, which the requests and
    /// the snapshots of the encrypted values hold.
    sealed_mark: Vec<u8>,
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
                let commits = record.commits.get_mut(&(client, op.timestamp())).unwrap();
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

    /// Counts a `case` of the facade that the checks saw.
    fn saw(&self, case: &'static str) {
        *self.cases.lock().unwrap().entry(case).or_default() += 1;
    }

    /// Checks that `message`, which client `client` sends, does not hold the mark, nor the snapshot
    /// it uploads, decoded once each of its chunks was sent: the plaintext of an encrypted value
    /// never goes out. It counts the commits of the facade, and the snapshots, that hold a value
    /// encrypted: the encrypted values go out, encrypted.
    fn check_sealed(&self, client: usize, facade: bool, message: &ClientMessage) {
        let bytes = encode_message(message).unwrap();
        assert!(
            !holds(&bytes, MARK),
            "Client {client} sends the plaintext of an encrypted value: {message:?}"
        );
        if facade
            && matches!(message, ClientMessage::Commit { .. })
            && holds(&bytes, &self.sealed_mark)
        {
            self.saw("commit with a value encrypted");
        }
        let ClientMessage::UploadSnapshotChunk {
            chunk_index,
            total_chunks,
            snapshot_hash,
            data,
            ..
        } = message
        else {
            return;
        };
        let snapshot = {
            let mut record = self.record.lock().unwrap();
            if record.decoded.contains(snapshot_hash) {
                return;
            }
            let chunks = record.uploaded.entry(*snapshot_hash).or_default();
            chunks.insert(*chunk_index, data.clone());
            if chunks.len() < *total_chunks as usize {
                return;
            }
            let chunks = record.uploaded.remove(snapshot_hash).unwrap();
            record.decoded.insert(*snapshot_hash);
            chunks.into_values().flatten().collect::<Vec<u8>>()
        };
        let payload = decode_snapshot_envelope(&snapshot).unwrap();
        assert!(
            !holds(&payload, MARK),
            "Client {client} uploads a snapshot with the plaintext of an encrypted value"
        );
        self.saw("snapshot uploaded, decoded");
        if holds(&payload, &self.sealed_mark) {
            self.saw("snapshot with a value encrypted");
        }
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

    /// `block`, of the event stream of `room_id` whose lines `lines` read so far, or now and then,
    /// while the network fuzzes, another in its place ([`Fuzz::event`]), unless a line that it
    /// ends lies ([`event_lies`]). Returns whether it is another.
    fn fuzzed_block(&self, room_id: &RoomId, lines: &Lines, block: Bytes) -> (Bytes, bool) {
        let mut fuzz = self.fuzz.lock().unwrap();
        let Some(fuzz) = fuzz.as_mut() else {
            return (block, false);
        };
        let real = ops_event(&block);
        let Some((kind, fuzzed)) = fuzz.event(&block, room_id, real.as_ref()) else {
            return (block, false);
        };
        let real = match real {
            Some(ServerMessage::OpsEvent { ops, .. }) => ops,
            _ => Vec::new(),
        };
        let schemas = self.schemas.lock().unwrap();
        let lies = (lines.clone().read(&fuzzed).iter())
            .any(|line| event_lies(line, room_id, &real, &fuzz.incarnations, &schemas));
        if lies {
            self.injected("forgery left out");
            return (block, false);
        }
        self.injected(kind);
        (Bytes::from(fuzzed), true)
    }

    /// While the network fuzzes, now and then a refusal in place of `opened`, the opening of the
    /// event stream of `room_id` ([`Fuzz::refusal`]).
    fn fuzzed_opening(&self, room_id: &RoomId, opened: EventStream) -> EventStream {
        let refusal =
            (self.fuzz.lock().unwrap().as_mut()).and_then(|fuzz| fuzz.refusal(room_id, &opened));
        match refusal {
            Some(refusal) => {
                self.injected("refusal at random");
                EventStream::Refused(refusal)
            }
            None => opened,
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
/// from, and those replies as `ops` events, for a damaged event; the codes of the errors; the
/// last operations that `ops` events brought, which forged events carry; and the incarnations
/// that forged events carry, of no room.
struct Fuzz {
    rng: generators::Rng,
    pool: Vec<Vec<u8>>,
    events: Vec<Vec<u8>>,
    codes: Vec<ErrorCode>,
    ops: Vec<Operation>,
    incarnations: HashSet<u128>,
}

impl Fuzz {
    fn new(seed: u64) -> Self {
        let samples = generators::sample_server_messages();
        let pool: Vec<Vec<u8>> = samples.iter().map(|m| encode_message(m).unwrap()).collect();
        Self {
            rng: generators::Rng::new(seed.reverse_bits()),
            events: pool.iter().map(|frame| ops_block(frame)).collect(),
            pool,
            codes: generators::all_error_codes(),
            ops: Vec::new(),
            incarnations: HashSet::new(),
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

    /// One time in five, a block in place of `block`, of the event stream of `room_id`, whose
    /// `ops` event is `real`, if it is one, with the name of what it is: random bytes, mostly
    /// pieces of the syntax of a stream; `block` damaged; the frame of `real` damaged, in base64
    /// again; a line without an end, at times longer than a client reads; an `ops` event with a
    /// random message; `real` with other operations of the room at its sequences, of another
    /// room or of another incarnation; another event with random data; or `real` with one of its
    /// operations in its table and of its key but with random data, which does not validate,
    /// unless by chance. Without `real`, a random message in place of those made of it.
    fn event(
        &mut self,
        block: &[u8],
        room_id: &RoomId,
        real: Option<&ServerMessage>,
    ) -> Option<(&'static str, Vec<u8>)> {
        if let Some(ServerMessage::OpsEvent { ops, .. }) = real {
            self.ops.extend(ops.iter().map(|op| op.op.clone()));
            let excess = self.ops.len().saturating_sub(64);
            self.ops.drain(..excess);
        }
        let rng = &mut self.rng;
        if !rng.chance(1, 5) {
            return None;
        }
        let kind = rng.below(9);
        let forged = match (kind, real) {
            (5 | 6, Some(real)) => forged(rng, real, &self.ops),
            _ => None,
        };
        Some(match (kind, real, forged) {
            (0, ..) => ("random bytes", random_stream(rng, &self.pool)),
            (1, ..) => {
                let mut bytes = block.to_vec();
                generators::mutate(rng, &mut bytes, 0, &self.events);
                ("event damaged", bytes)
            }
            (2, Some(real), _) => {
                let mut frame = encode_message(real).unwrap();
                generators::mutate(rng, &mut frame, PROTOCOL_HEADER_LEN, &self.pool);
                ("frame damaged", ops_block(&frame))
            }
            (3, ..) => {
                let mut line = b"data: ".to_vec();
                line.resize(line.len() + rng.below(2 * EVENT_LIMIT), b'A');
                ("endless line", line)
            }
            (5 | 6, _, Some(mut forged)) => {
                let ServerMessage::OpsEvent {
                    room_id: of,
                    room_incarnation,
                    ..
                } = &mut forged
                else {
                    unreachable!("A forged event is an ops event")
                };
                let name = match kind {
                    5 => {
                        while *of == *room_id {
                            *of = generators::room_id(rng);
                        }
                        "another room"
                    }
                    _ => {
                        *room_incarnation = rng.u128();
                        self.incarnations.insert(*room_incarnation);
                        "another incarnation"
                    }
                };
                (name, ops_block(&encode_message(&forged).unwrap()))
            }
            (7, ..) => ("random event", random_event(rng)),
            (8, Some(real), _) => {
                let mut invalid = real.clone();
                if let ServerMessage::OpsEvent { ops, .. } = &mut invalid {
                    if !ops.is_empty() {
                        let at = rng.below(ops.len());
                        let op = &mut ops[at].op;
                        let (table, pk) = (op.table_id(), op.pk().clone());
                        let kind = generators::operation_kind(rng);
                        *op = Operation::new(table, pk, op.timestamp(), kind);
                    }
                }
                (
                    "invalid operation",
                    ops_block(&encode_message(&invalid).unwrap()),
                )
            }
            _ => {
                let message = generators::server_message(rng, &self.codes);
                (
                    "random message",
                    ops_block(&encode_message(&message).unwrap()),
                )
            }
        })
    }

    /// One time in five, a refusal of the event stream of `room_id` in place of `opened`: random
    /// bytes; the refusal of `opened` damaged, if it is one; a random message; or an error of the
    /// room with a random code.
    fn refusal(&mut self, room_id: &RoomId, opened: &EventStream) -> Option<Vec<u8>> {
        let rng = &mut self.rng;
        if !rng.chance(1, 5) {
            return None;
        }
        Some(match (rng.below(4), opened) {
            (0, _) => rng.bytes_below(256),
            (1, EventStream::Refused(refusal)) => {
                let mut bytes = refusal.clone();
                generators::mutate(rng, &mut bytes, PROTOCOL_HEADER_LEN, &self.pool);
                bytes
            }
            (1 | 2, _) => encode_message(&generators::server_message(rng, &self.codes)).unwrap(),
            _ => encode_message(&ServerMessage::Error {
                correlation_id: None,
                room_id: Some(room_id.clone()),
                code: *rng.pick(&self.codes),
                message: "Fabricated".to_string(),
            })
            .unwrap(),
        })
    }
}

/// The block of an `ops` event with `frame` as its data, as the server writes it.
fn ops_block(frame: &[u8]) -> Vec<u8> {
    format!("event: ops\ndata: {}\n\n", STANDARD.encode(frame)).into_bytes()
}

/// The `ops` event that `block` is, as the server writes it.
fn ops_event(block: &[u8]) -> Option<ServerMessage> {
    let data = block.strip_prefix(b"event: ops\ndata: ")?;
    let end = data.iter().position(|&byte| byte == b'\n')?;
    let frame = STANDARD.decode(&data[..end]).ok()?;
    let event = decode_message(&frame).ok()?;
    matches!(event, ServerMessage::OpsEvent { .. }).then_some(event)
}

/// `event`, an `ops` event, with another operation of `pool` at each of its sequences, if `pool`
/// has another one for each: operations of the room that the log does not have there.
fn forged(
    rng: &mut generators::Rng,
    event: &ServerMessage,
    pool: &[Operation],
) -> Option<ServerMessage> {
    let mut forged = event.clone();
    let ServerMessage::OpsEvent { ops, .. } = &mut forged else {
        return None;
    };
    for op in ops {
        let others: Vec<&Operation> = pool.iter().filter(|other| **other != op.op).collect();
        if others.is_empty() {
            return None;
        }
        op.op = (*rng.pick(&others)).clone();
    }
    Some(forged)
}

/// Random bytes, mostly pieces of the syntax of an event stream, numbers, and the frames of
/// `pool` in base64.
fn random_stream(rng: &mut generators::Rng, pool: &[Vec<u8>]) -> Vec<u8> {
    const PIECES: [&[u8]; 15] = [
        b"data:",
        b"data: ",
        b"event: ",
        b"ops",
        b"head_advanced",
        b"snapshot_available",
        b"lagged",
        b"\n",
        b"\r",
        b"\r\n",
        b"\n\n",
        b":",
        b" ",
        b"=",
        b"\xc3\xb1",
    ];
    let mut bytes = Vec::new();
    for _ in 0..rng.below(100) {
        match rng.below(10) {
            0 => bytes.push(rng.next_u64() as u8),
            1 => bytes.extend(STANDARD.encode(rng.pick(pool)).bytes()),
            2 => bytes.extend(rng.u64().to_string().bytes()),
            _ => {
                let piece: &&[u8] = rng.pick(&PIECES);
                bytes.extend_from_slice(piece)
            }
        }
    }
    bytes
}

/// An event of the stream other than `ops`, or of a name the client does not know, with random
/// data: a number, text or nothing.
fn random_event(rng: &mut generators::Rng) -> Vec<u8> {
    const NAMES: [&str; 6] = [
        "head_advanced",
        "snapshot_available",
        "schema_reloaded",
        "snapshot_wanted",
        "lagged",
        "unknown",
    ];
    let name = rng.pick(&NAMES);
    let data = match rng.below(3) {
        0 => rng.u64().to_string(),
        1 => generators::text(rng, 16),
        _ => String::new(),
    };
    format!("event: {name}\ndata: {data}\n\n").into_bytes()
}

/// The lines of an event stream as a client reads them: a line feed, a carriage return or both
/// end each one. The part of a line longer than a client reads is left out.
#[derive(Clone, Default)]
struct Lines {
    /// The line in progress, and whether it is longer than a client reads.
    line: Vec<u8>,
    long: bool,
}

impl Lines {
    /// The lines that `block` ends, after what came before it, except those longer than a
    /// client reads.
    fn read(&mut self, block: &[u8]) -> Vec<Vec<u8>> {
        let mut ended = Vec::new();
        for &byte in block {
            match byte {
                b'\n' | b'\r' => {
                    let line = std::mem::take(&mut self.line);
                    if !std::mem::take(&mut self.long) {
                        ended.push(line);
                    }
                }
                _ if self.long => {}
                _ if self.line.len() == EVENT_LIMIT => {
                    self.line.clear();
                    self.long = true;
                }
                _ => self.line.push(byte),
            }
        }
        ended
    }
}

/// Whether `line`, of the event stream of `room_id`, is the data of an `ops` event that the
/// client cannot tell from one of the server's, in place of an event with `real`, the operations
/// of the server: of the room, with operations in order that reach its head, of an incarnation
/// not in `forged`, since the client takes those of the room for its own, and with operations
/// from one of its sequences on, those after the head of a replica, that validate with a schema
/// of `schemas` and bring some other than those of `real`: the client drops what its replica
/// holds and validates the rest. Its name, and the other lines of its event, are not looked at:
/// an event with more than one `data` line is not base64.
fn event_lies(
    line: &[u8],
    room_id: &RoomId,
    real: &[SequencedOperation],
    forged: &HashSet<u128>,
    schemas: &[Schema],
) -> bool {
    let Some(data) = line.strip_prefix(b"data:") else {
        return false;
    };
    let data = data.strip_prefix(b" ").unwrap_or(data);
    let Ok(frame) = STANDARD.decode(data) else {
        return false;
    };
    let Ok(ServerMessage::OpsEvent {
        room_id: of,
        room_incarnation,
        head_seq,
        ops,
        ..
    }) = decode_message(&frame)
    else {
        return false;
    };
    let in_order =
        (ops.windows(2)).all(|pair| pair[0].seq.get().checked_add(1) == Some(pair[1].seq.get()));
    let whole = in_order
        && ops.first().is_some_and(|op| op.seq.get() > 0)
        && ops.last().is_some_and(|op| op.seq == head_seq);
    let validates = |ops: &[SequencedOperation]| {
        let valid =
            |schema: &Schema| (ops.iter()).all(|op| schema.validate_operation(&op.op).is_ok());
        schemas.iter().any(valid)
    };
    // What the client applies after the head of its replica, from any of its sequences.
    let forges = |from: usize| {
        let applied = &ops[from..];
        applied.iter().any(|op| !real.contains(op)) && validates(applied)
    };
    of == *room_id && whole && !forged.contains(&room_incarnation) && (0..ops.len()).any(forges)
}

/// Whether `bytes` hold `needle`.
fn holds(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|window| window == needle)
}

/// The plaintext of an encrypted value: a note with the mark, `n` after it.
fn note(n: u64) -> Value {
    let mut note = String::from_utf8(MARK.to_vec()).unwrap();
    note.push_str(&format!(" {n}"));
    Value::String(note.into())
}

/// The mark as the `CryptoEngine` of the tests encrypts it in a note: the plaintext of a note,
/// with its type, is the encoding of the protocol of its value, which puts the mark at the same
/// place in each, and the `CryptoEngine` masks each byte by its place alone.
fn sealed_mark() -> Vec<u8> {
    let plaintext = encoded(&note(0));
    let at = (plaintext.windows(MARK.len()))
        .position(|window| window == MARK)
        .unwrap();
    let ciphertext = TestCrypto::seal(b"", &plaintext);
    let tag = ciphertext.len() - plaintext.len();
    let sealed = ciphertext[tag + at..tag + at + MARK.len()].to_vec();
    assert_eq!(
        encoded(&note(u64::MAX))
            .windows(MARK.len())
            .position(|w| w == MARK),
        Some(at)
    );
    assert_ne!(sealed, MARK);
    sealed
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

/// The longest event, and so the longest line, that a client reads from an event stream.
const EVENT_LIMIT: usize = 64 << 10;

/// `blocks`, the event stream of `room_id` that `token` of client `client` opened, with the faults
/// that `net` injects at each block while it is faulty: a block lost, or late, with those after
/// it; `lagged` in its place; the connection cut; a stream that goes silent; or its token that
/// expires, which ends it. While the network fuzzes, another block in place of some
/// ([`Network::fuzzed_block`]). The stream of a client without network is cut. It counts the
/// `ops` events of the server that reach the client, and those of a client of the facade if it
/// is one.
fn faulty_blocks(
    blocks: EventBlocks,
    net: Arc<Network>,
    (room_id, client, token, facade): (RoomId, usize, String, bool),
) -> EventBlocks {
    let blocks = futures::stream::unfold(Some((blocks, Lines::default())), move |state| {
        let (net, room_id, token) = (Arc::clone(&net), room_id.clone(), token.clone());
        async move {
            let (mut blocks, mut lines) = state?;
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
                let block = match fault {
                    (Some(0 | 1), _) => {
                        net.injected("event lost");
                        continue;
                    }
                    (Some(2 | 3), delay) => {
                        net.injected("event late");
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        block
                    }
                    (Some(4), _) => {
                        net.injected("lagged");
                        Ok(Bytes::from_static(LAGGED))
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
                    _ => block,
                };
                let Ok(block) = block else {
                    return Some((block, Some((blocks, lines))));
                };
                let (block, fuzzed) = net.fuzzed_block(&room_id, &lines, block);
                lines.read(&block);
                if !fuzzed && block.starts_with(b"event: ops\n") {
                    net.ops.fetch_add(1, Ordering::SeqCst);
                    if facade {
                        net.saw("ops event to a client of the facade");
                    }
                }
                return Some((Ok(block), Some((blocks, lines))));
            }
        }
    });
    Box::pin(blocks)
}

/// The transport of client `client` over the network, which loses, delays and repeats its
/// requests, answers some with `Unavailable`, expires their tokens, and fuzzes their replies, if
/// it is `faulty`; and which loses, delays and cuts the events of its streams. It checks that no
/// download goes past the first chunk of a snapshot larger than the limit, that no request holds
/// the plaintext of an encrypted value ([`Network::check_sealed`]), and, for a client of the
/// `facade`, that each operation it sends declares the owner of its client.
#[derive(Clone)]
struct Link {
    net: Arc<Network>,
    client: usize,
    faulty: bool,
    facade: bool,
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
        net.check_sealed(self.client, self.facade, message);
        if let ClientMessage::Commit { op, client_id, .. } = message {
            // The owner column is the same in every schema of the room.
            let owner = match self.facade {
                true => net
                    .schemas
                    .lock()
                    .unwrap()
                    .last()
                    .unwrap()
                    .check_owner(op, client_id),
                false => Ok(()),
            };
            assert!(
                owner.is_ok(),
                "{client_id}, of the facade, sends {op:?}, with another owner"
            );
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
            let commits = record
                .commits
                .entry((self.client, op.timestamp()))
                .or_default();
            commits.sent += 1;
            commits.ids.insert((*mutation_id, client_id.clone()));
            if self.facade {
                let sent = record.sent.entry(self.client).or_default();
                sent.insert(*mutation_id, op.clone());
            }
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
    /// loses, delays and cuts its events ([`faulty_blocks`]); and while the network fuzzes, gets
    /// a refusal at random in place of some openings, and other blocks in place of some of its.
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
        if !self.faulty {
            return Ok(opened);
        }
        Ok(match net.fuzzed_opening(room_id, opened) {
            EventStream::Open(blocks) => {
                let of = (room_id.clone(), self.client, token.to_string(), self.facade);
                EventStream::Open(faulty_blocks(blocks, Arc::clone(net), of))
            }
            refused => refused,
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

/// What a client of the facade has: the `CryptoEngine` of the tests, or none, and the precheck, or
/// not.
#[derive(Debug, Clone, Copy)]
struct Through {
    crypto: bool,
    precheck: bool,
}

/// A client of the simulation, over a memory engine or the disk engine of its directory, of the
/// core or of the facade.
struct Member {
    person: &'static str,
    dir: Option<TempDir>,
    device: Device,
    /// What it has, if it is a client of the facade.
    facade: Option<Through>,
    /// Its engine, which the checks read too.
    storage: Arc<dyn StorageEngine>,
    metadata: Shared,
    /// Its token provider, and how many tokens the client had asked for when the provider was
    /// told to give one already expired, until the client asks for it.
    tokens: TestTokens,
    expiring: Option<usize>,
    /// The mode of the worker of its room, if it runs one: the mode of the room, in a client of the
    /// facade, whose worker runs in `Live` while a room in `Periodic` is watched.
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

/// A client of the simulation: one of the core, which the steps call through its low-level API,
/// or one of the facade.
enum Device {
    Core(Arc<Client>),
    Facade(Facade),
}

impl Device {
    fn client_id(&self) -> ClientId {
        match self {
            Self::Core(client) => client.client_id(),
            Self::Facade(facade) => facade.client.client_id(),
        }
    }

    fn facade(&self) -> Option<&Facade> {
        match self {
            Self::Core(_) => None,
            Self::Facade(facade) => Some(facade),
        }
    }

    async fn status(&self, room_id: &RoomId) -> Result<RoomStatus, ClientError> {
        match self {
            Self::Core(client) => client.status(room_id.clone()).await,
            Self::Facade(facade) => facade.handle(room_id)?.status().await,
        }
    }

    async fn sync(&self, room_id: &RoomId) -> Result<(), ClientError> {
        match self {
            Self::Core(client) => client.sync(room_id.clone()).await,
            Self::Facade(facade) => facade.handle(room_id)?.sync().await,
        }
    }

    /// A heartbeat of the core: the worker of a client of the facade sends its heartbeats.
    async fn heartbeat(&self, room_id: &RoomId) -> Result<(), ClientError> {
        match self {
            Self::Core(client) => client.heartbeat(room_id.clone()).await,
            Self::Facade(_) => unreachable!("A client of the facade has no heartbeats of its own"),
        }
    }

    /// A write of the core, of `op` as it is.
    async fn write(&self, room_id: &RoomId, op: &Operation) -> Result<(), ClientError> {
        match self {
            Self::Core(client) => client.write(room_id.clone(), op.clone()).await,
            Self::Facade(_) => unreachable!("A client of the facade writes by the names"),
        }
    }

    async fn close_room(&self, room_id: &RoomId) -> Result<(), ClientError> {
        match self {
            Self::Core(client) => client.close_room(room_id.clone()).await,
            Self::Facade(facade) => facade.handle(room_id)?.close().await,
        }
    }

    /// Opens `room_id`: in the core with a token of its own ([`open_room`]), and in the facade with
    /// one of its token provider, which starts its worker.
    async fn open_room(&self, room_id: &RoomId) -> Result<(), ClientError> {
        match self {
            Self::Core(client) => open_room(client, room_id).await,
            Self::Facade(facade) => facade.open(room_id).await,
        }
    }

    /// Sets the mode of the worker of `room_id` to `freshness`: the core starts the worker in it if
    /// it has none that runs, and the facade sets the mode of the room, which its worker takes if
    /// it runs.
    async fn set_freshness(
        &self,
        room_id: &RoomId,
        freshness: Freshness,
    ) -> Result<(), ClientError> {
        match self {
            Self::Core(client) => client.set_freshness(room_id.clone(), freshness).await,
            Self::Facade(facade) => {
                facade.handle(room_id)?.set_freshness(freshness);
                Ok(())
            }
        }
    }

    /// Runs the worker of `room_id` in `freshness`, as an application does: the core starts it, or
    /// sets its mode; the facade sets the mode of the room, and, if its room is open without a
    /// worker that runs, gets its handle again, which starts one.
    async fn run_worker(&self, room_id: &RoomId, freshness: Freshness) -> Result<(), ClientError> {
        self.set_freshness(room_id, freshness).await?;
        let Self::Facade(facade) = self else {
            return Ok(());
        };
        let status = facade.handle(room_id)?.status().await?;
        match status.worker.is_some_and(|worker| worker.running) {
            true => Ok(()),
            false => facade.open(room_id).await,
        }
    }
}

/// A client of the facade: the handle of its room once it opened it, which reaches the room as the
/// client has it at each call, and its `watch` streams.
struct Facade {
    client: ZemdbClient,
    room: OnceLock<RoomHandle>,
    watches: Mutex<Vec<Watcher>>,
}

impl Facade {
    fn new(client: ZemdbClient) -> Self {
        Self {
            client,
            room: OnceLock::new(),
            watches: Mutex::default(),
        }
    }

    /// The handle of `room_id`, or `RoomClosed` if it never opened it.
    fn handle(&self, room_id: &RoomId) -> Result<&RoomHandle, ClientError> {
        self.room
            .get()
            .ok_or_else(|| ClientError::RoomClosed(room_id.clone()))
    }

    async fn open(&self, room_id: &RoomId) -> Result<(), ClientError> {
        let handle = self.client.room(room_id.clone()).await?;
        let _ = self.room.set(handle);
        Ok(())
    }

    /// Writes `call` in `room_id`.
    async fn write(&self, room_id: &RoomId, call: &Call) -> Result<(), ClientError> {
        let room = self.handle(room_id)?;
        match call {
            Call::Insert(table, row) => room.table(table)?.insert(row).await.map(drop),
            Call::Update(table, key, changes) => room.table(table)?.update(key, changes).await,
            Call::Delete(table, key) => room.table(table)?.delete(key).await,
        }
    }

    /// Opens the stream of `watch` in `room_id`, and drops the oldest if it has [`WATCHES`].
    fn watch(&self, room_id: &RoomId, watch: &Watch) -> Result<(), ClientError> {
        let room = self.handle(room_id)?;
        let watched = match watch {
            Watch::Row(table, key) => Watched::Row(room.table(table)?, key.clone()),
            Watch::Rows(table, scan) => Watched::Rows(room.table(table)?, scan.clone()),
        };
        let watcher = Watcher::new(watched);
        let mut watches = self.watches.lock().unwrap();
        if watches.len() == WATCHES {
            watches.remove(0);
        }
        watches.push(watcher);
        Ok(())
    }

    /// Drops the stream `n` modulo how many it has, if it has one.
    fn unwatch(&self, n: usize) {
        let mut watches = self.watches.lock().unwrap();
        if !watches.is_empty() {
            let n = n % watches.len();
            watches.remove(n);
        }
    }
}

/// A write of a step, as the checks take it: of the core, of its operation, tagged; or of the
/// facade, with what makes the facade refuse it, if any.
enum Written<'a> {
    Core(&'a Operation),
    Facade(Option<&'static str>),
}

/// A write of a client of the facade, by the names of the columns: an insert of a row, an update
/// of the row of a key with some changes, or a delete of the row of a key, in a table.
#[derive(Debug, Clone)]
enum Call {
    Insert(String, Row),
    Update(String, Key, Row),
    Delete(String, Key),
}

/// What a `watch` of a client of the facade reads: the row of a key, or the rows of a scan, of a
/// table.
#[derive(Debug, Clone)]
enum Watch {
    Row(String, Key),
    Rows(String, Scan),
}

/// A [`Watch`] of a table of a room.
#[derive(Debug, Clone)]
enum Watched {
    Row(TableHandle, Key),
    Rows(TableHandle, Scan),
}

impl Watched {
    /// The stream of its values, as rows.
    fn stream(&self) -> BoxStream<'static, Result<Vec<Row>, ClientError>> {
        match self {
            Self::Row(table, key) => {
                let rows = |row: Option<Row>| -> Vec<Row> { row.into_iter().collect() };
                table.watch(key).map(move |row| row.map(rows)).boxed()
            }
            Self::Rows(table, scan) => table.watch_scan(scan).boxed(),
        }
    }

    /// What a direct read of the replica gives, as its stream reads it.
    async fn read(&self) -> Result<Vec<Row>, ClientError> {
        match self {
            Self::Row(table, key) => Ok(table.get(key).await?.into_iter().collect()),
            Self::Rows(table, scan) => table.scan(scan).await,
        }
    }
}

/// What the stream of a watch gave: how many elements, the last, and its last value; whether it
/// gave a value equal to the one before, and whether it ended.
#[derive(Default)]
struct Seen {
    elements: usize,
    last: Option<Result<Vec<Row>, ClientError>>,
    value: Option<Vec<Row>>,
    repeated: bool,
    ended: bool,
}

/// A `watch` or a `watch_scan` of a client of the facade, whose stream a task of its own reads, as
/// an application reads it, until the watch is dropped, which drops the stream at any `.await`.
struct Watcher {
    watched: Watched,
    seen: Arc<Mutex<Seen>>,
    task: JoinHandle<()>,
}

impl Watcher {
    /// Subscribes the stream of `watched` now, and starts the task that reads it.
    fn new(watched: Watched) -> Self {
        let mut stream = watched.stream();
        let seen = Arc::new(Mutex::new(Seen::default()));
        let shared = Arc::clone(&seen);
        let task = tokio::spawn(async move {
            while let Some(element) = stream.next().await {
                let mut seen = shared.lock().unwrap();
                seen.elements += 1;
                if let Ok(rows) = &element {
                    seen.repeated |= seen.value.as_ref() == Some(rows);
                    seen.value = Some(rows.clone());
                }
                seen.last = Some(element);
            }
            shared.lock().unwrap().ended = true;
        });
        Self {
            watched,
            seen,
            task,
        }
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The client of `person` over `storage`, `metadata`, `link` and `tokens`, with `config`, with
/// `storage` for the checks: of the core, or of the facade, `through` what it has.
async fn device<S: StorageEngine + Clone + 'static>(
    config: ClientConfig,
    storage: S,
    metadata: &Shared,
    (link, tokens): (&Link, &TestTokens),
    through: Option<Through>,
) -> Result<(Device, Arc<dyn StorageEngine>), ClientError> {
    let (metadata, link, tokens) = (metadata.clone(), link.clone(), tokens.clone());
    let device = match through {
        None => {
            let client = Client::open(config, storage.clone(), metadata, link, tokens);
            Device::Core(Arc::new(client.await?))
        }
        Some(through) => {
            let crypto = through
                .crypto
                .then(|| TestCrypto::new() as Arc<dyn CryptoEngine>);
            let client = ZemdbClient::open(config, storage.clone(), metadata, link, tokens, crypto);
            Device::Facade(Facade::new(client.await?))
        }
    };
    Ok((device, Arc::new(storage)))
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

/// The case of `result`, the end of a write of the facade: one that the facade decides locally
/// sends nothing.
fn facade_case(result: &Result<(), ClientError>) -> &'static str {
    match result {
        Ok(()) => "write taken",
        Err(ClientError::ConstraintViolation(_) | ClientError::RowMissing) => {
            "write rejected by the replica"
        }
        Err(ClientError::OutcomeUnknown) => "write unknown",
        Err(ClientError::PrecheckFailed(_)) => "decided locally: PrecheckFailed",
        Err(ClientError::Offline(_)) => "decided locally: Offline",
        Err(ClientError::InvalidOperation(_)) => "decided locally: InvalidOperation",
        Err(ClientError::Crypto(_)) => "decided locally: Crypto",
        Err(ClientError::FailedClosed(_)) => "decided locally: FailedClosed",
        Err(_) => "write failed",
    }
}

/// The wall clock, in milliseconds, as the facade takes the timestamps of its writes.
fn wall_clock() -> u64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    u64::try_from(since.as_millis()).unwrap()
}

/// The key of the row of `op`, by the names of the columns of the primary key of `table`.
fn key_of(table: &TableSchema, op: &Operation) -> Key {
    let mut key = Key::new();
    for (name, value) in table.primary_key().iter().zip(op.pk().values()) {
        key.set(name.clone(), value.clone());
    }
    key
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

    /// The sequences of the operations of the log equal to `op`.
    fn landed_as(&self, op: &Operation) -> Vec<u64> {
        let same = self.log.iter().filter(|logged| logged.op == *op);
        same.map(|op| op.seq.get()).collect()
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

/// The position of `notes` in `rounds`.
const NOTES: u16 = 4;

/// A round, with its notes encrypted as a member with the key encrypts them, with the additional
/// data of their cell: the clients of the facade read them.
fn round(ranking: Value, round: Value, person: &str, title: &str) -> Operation {
    let notes = notes(&ranking, &round, NOTES);
    insert(
        ROUNDS,
        2,
        vec![ranking, round, person.into(), title.into(), notes],
    )
}

/// The notes of the round `round` of `ranking`, always the same plaintext, encrypted with the
/// additional data of the cell of the column at `column` of its row: those of another column than
/// [`NOTES`] do not decrypt (`Decryption`).
fn notes(ranking: &Value, round: &Value, column: u16) -> Value {
    let room_id = RoomId::new(ROOM).unwrap();
    let cell = additional_data(&room_id, ROUNDS, &[ranking.clone(), round.clone()], column);
    Value::Bytes(TestCrypto::seal(&cell, &encoded(&note(0))).into())
}

fn vote(voter: &str, ranking: Value, round: Value, candidate: &str) -> Operation {
    insert(
        VOTES,
        3,
        vec![voter.into(), ranking, round, candidate.into()],
    )
}

/// An operation of `person` over two rankings, three rounds and three persons: mostly votes,
/// and few deletes. The notes of a round change now and then to some that do not decrypt, and
/// back to those of the core: a round that the core opened is then the same row as before.
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
        roll @ (13 | 14) => {
            let column = if roll == 13 { NOTES - 1 } else { NOTES };
            let notes = notes(&ranking_id, &round_id, column);
            let pk = PrimaryKey::composite([ranking_id, round_id]);
            let updates = vec![
                ColumnUpdate::new(2, person),
                ColumnUpdate::new(NOTES, notes),
            ];
            Operation::update(ROUNDS, pk, updates, 0)
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
    /// A write of the core, and one of the facade, with what makes the facade refuse it, if any.
    Write(Operation),
    Call(Call, Option<&'static str>),
    Sync,
    Heartbeat,
    /// Closes the room and opens it again.
    Reopen,
    /// Sets the mode of the worker of the room.
    Freshness(Freshness),
    /// Opens a `watch` stream of the facade, and drops one, of those it has.
    Watch(Watch),
    Unwatch(usize),
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
    /// The notes written by the facade, which number the next one, and the wall clock, in
    /// milliseconds, at the end of the last step with writes of the facade.
    notes: u64,
    wall: u64,
}

impl Sim {
    /// A server with a room of the schema of `kind`, the archivist and four clients of the core: a
    /// disk and a memory one of each of two persons, those of the second with a worker if
    /// `workers`, the one over the disk in `Live` and the other in a random mode. With `workers`
    /// and without `fuzz`, also two clients of the facade, of two persons without another, each
    /// with its worker in a random mode: one over the disk, with the `CryptoEngine` of the tests
    /// and without the precheck, so that its writes of encrypted values go out also when its
    /// replica would reject them, and one over memory, without it and with the precheck. The
    /// network fuzzes the replies and the event streams if `fuzz`.
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
            cases: Mutex::default(),
            sealed_mark: sealed_mark(),
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
            room: RoomId::new(ROOM).unwrap(),
            schema_id: SchemaId::new("sim-0").unwrap(),
            schema,
            schemas: 0,
            columns: 0,
            incarnations: Vec::new(),
            members: Vec::new(),
            tags: 0,
            notes: 0,
            wall: 0,
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
            sim.join(person, disk, worker.filter(|_| workers), None)
                .await;
        }
        if workers && !fuzz {
            let persons = match kind {
                Kind::Random => ["A", "B"],
                Kind::Ranking => ["C", "B"],
            };
            for (person, crypto) in persons.into_iter().zip([true, false]) {
                let worker = Some(sim.freshness());
                let precheck = !crypto;
                let through = Some(Through { crypto, precheck });
                sim.join(person, crypto, worker, through).await;
            }
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
    /// with `tokens` as its token provider, and the room in it, with its worker in `worker`, if
    /// any: of the core, or of the facade, `through` what it has.
    async fn open(
        &self,
        (index, person): (usize, &'static str),
        dir: Option<TempDir>,
        (worker, through): (Option<Freshness>, Option<Through>),
        tokens: TestTokens,
    ) -> Member {
        let link = Link {
            net: Arc::clone(&self.net),
            client: index,
            faulty: index != ARCHIVIST,
            facade: through.is_some(),
        };
        let config = ClientConfig {
            max_snapshot_bytes: LIMIT,
            heartbeat_interval: INTERVAL,
            retry_max: INTERVAL,
            events_retry: EVENTS_RETRY,
            precheck: through.is_none_or(|through| through.precheck),
            freshness: worker.unwrap_or(Freshness::Periodic),
            ..ClientConfig::new(person)
        };
        let metadata = Shared(match &dir {
            Some(dir) => Arc::new(FileMetadata::new(dir.path())),
            None => Arc::new(MemoryMetadata::default()),
        });
        // A client that crashed holds its metadata until the tasks it left end, those of its
        // worker too.
        let mut opened = None;
        for _ in 0..6000 {
            let (config, of) = (config.clone(), (&link, &tokens));
            let attempt = match &dir {
                Some(dir) => {
                    let storage = DiskStorageEngine::new(options(dir.path())).unwrap();
                    device(config, storage, &metadata, of, through).await
                }
                None => {
                    let storage = MemoryStorageEngine::new();
                    device(config, storage, &metadata, of, through).await
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
        let (device, storage) = opened.expect("The metadata of a crashed client stay held");
        let incarnation = metadata.of(&self.room).await;
        let incarnation = incarnation.map(|metadata| self.incarnation(metadata.room_incarnation));
        let _ = device.open_room(&self.room).await;
        if let Some(worker) = worker {
            let _ = device.run_worker(&self.room, worker).await;
        }
        Member {
            person,
            dir,
            device,
            facade: through,
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
    /// worker in `worker`, if any: of the core, or of the facade, `through` what it has.
    async fn join(
        &mut self,
        person: &'static str,
        disk: bool,
        worker: Option<Freshness>,
        through: Option<Through>,
    ) {
        let dir = disk.then(|| tempdir().unwrap());
        let of = (self.members.len(), person);
        let member = self
            .open(of, dir, (worker, through), TestTokens::new())
            .await;
        self.members.push(member);
    }

    /// Crashes client `index`, over a disk engine, and opens it again from its directory, with its
    /// worker in the mode it had, what it had of the facade and its token provider: the tasks it
    /// left run on, and its `watch` streams are dropped. The provider is of its application, which
    /// a crash of the device leaves as it was: a new one would give the tokens of the first again,
    /// for the same client, room and expiry, which the network took as expired.
    async fn crash(&mut self, index: usize) {
        let mut crashed = self.members.remove(index);
        let (person, dir, worker) = (crashed.person, crashed.dir.take(), crashed.worker);
        let (through, tokens, expiring) =
            (crashed.facade, crashed.tokens.clone(), crashed.expiring);
        drop(crashed);
        let mut member = self
            .open((index, person), dir, (worker, through), tokens)
            .await;
        member.expiring = expiring;
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
    /// one on again. A pruned room gets a client of the core that joins it, every other with a
    /// worker, up to eight of them.
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
                let core = self.members.iter().filter(|m| m.facade.is_none()).count();
                if self.prune().await && core < 8 {
                    let n = self.members.len();
                    let person = match self.kind {
                        Kind::Random => ["A", "B"][n % 2],
                        Kind::Ranking => "C",
                    };
                    let worker = (self.workers && n % 2 == 1).then(|| self.freshness());
                    self.join(person, n % 2 == 0, worker, None).await;
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
    /// few, which the server refuses. For a client of the facade, which declares its own owner and
    /// refuses a key of another, also of another owner: over random schemas, as they are drawn,
    /// and over the schema of the reference app, one in five of any person; and the one with the
    /// `CryptoEngine` opens a round of its own two times in five, with notes.
    fn operation(&mut self, member: usize) -> Operation {
        let person = self.members[member].person;
        let facade = self.members[member].facade;
        let crypto = facade.is_some_and(|through| through.crypto);
        let facade = facade.is_some();
        let op = match self.kind {
            Kind::Random if facade => oracle::random_operation(&mut self.rng, &self.schema),
            Kind::Ranking if crypto && self.rng.chance(40) => {
                let (ranking_id, round_id) = (1 + self.rng.below(2), 11 + self.rng.below(3));
                let title = ["Ronda 1", "Ronda 2"][self.rng.below(2) as usize];
                let (ranking_id, round_id) = (u128::from(ranking_id), u128::from(round_id));
                round(uuid(ranking_id), uuid(round_id), person, title)
            }
            Kind::Random => {
                let client_id = self.members[member].device.client_id();
                let mut op = oracle::random_operation(&mut self.rng, &self.schema);
                while self.schema.check_owner(&op, &client_id).is_err() && !self.rng.chance(10) {
                    op = oracle::random_operation(&mut self.rng, &self.schema);
                }
                op
            }
            Kind::Ranking if facade && self.rng.chance(20) => self.random_operation(),
            Kind::Ranking => ranking_operation(&mut self.rng, person),
        };
        self.tag(op)
    }

    /// A random operation of the schema of the room, of any person.
    fn random_operation(&mut self) -> Operation {
        match self.kind {
            Kind::Random => oracle::random_operation(&mut self.rng, &self.schema),
            Kind::Ranking => {
                let person = ["A", "B", "C"][self.rng.below(3) as usize];
                ranking_operation(&mut self.rng, person)
            }
        }
    }

    /// The write of the facade of `person` that makes `op`, by the names of the columns of the
    /// schema of the room: the values of its row but its nulls, or its changes, and the key of its
    /// row. In place of the ciphertext of an encrypted column, it writes a plaintext, and an
    /// update also writes one half of the time. The owner column, which the client fills, goes in
    /// the row or the changes one time in ten, with the owner of the client or another, and in the
    /// key, if it has it, two times in five with the owner of the client, and one in five with
    /// another: the facade refuses all of those but the key with the owner of the client.
    fn call(&mut self, person: &str, op: &Operation) -> (Call, Option<&'static str>) {
        let table = self.schema.get_table_by_id(op.table_id()).unwrap().clone();
        let owner = table.owner().map(|owner| owner.column().to_string());
        let is_owner = |name: &str| owner.as_deref() == Some(name);
        let other = self.other(person);
        let given = self.rng.chance(10).then(|| match self.rng.chance(50) {
            true => Value::from(person),
            false => other.clone(),
        });
        let mut key = Key::new();
        let mut foreign = false;
        for (name, value) in table.primary_key().iter().zip(op.pk().values()) {
            match self.rng.below(5) {
                _ if !is_owner(name) => key.set(name.clone(), value.clone()),
                0 | 1 => None,
                2 | 3 => key.set(name.clone(), person),
                _ => {
                    foreign = true;
                    key.set(name.clone(), other.clone())
                }
            };
        }
        // What the facade refuses: the owner column in a row or the changes, and a key of another.
        let given = given.filter(|_| owner.is_some());
        let refused = match (op.kind(), &given) {
            (OperationKind::Delete { .. }, _) | (OperationKind::Update { .. }, None) if foreign => {
                Some("refused: a key of another owner")
            }
            (OperationKind::Delete { .. }, _) => None,
            (_, Some(_)) => Some("refused: the owner column given"),
            (_, None) => None,
        };
        let name = table.name().to_string();
        let call = match op.kind() {
            OperationKind::Insert { row } => {
                let mut values = Row::new();
                for (column, value) in table.columns().iter().zip(row.values()) {
                    match &given {
                        _ if !is_owner(column.name()) => {}
                        Some(given) => {
                            values.set(column.name(), given.clone());
                            continue;
                        }
                        None => continue,
                    }
                    if !value.is_null() {
                        values.set(column.name(), self.plaintext(column, value));
                    }
                }
                Call::Insert(name, values)
            }
            OperationKind::Update { updates } => {
                let mut changes = Row::new();
                for update in updates {
                    let column = &table.columns()[usize::from(update.column_idx())];
                    if !is_owner(column.name()) {
                        changes.set(column.name(), self.plaintext(column, update.value()));
                    }
                }
                for column in table.columns().iter().filter(|c| c.is_encrypted()) {
                    if self.rng.chance(50) {
                        changes.set(column.name(), self.plaintext(column, &Value::Null));
                    }
                }
                if let (Some(owner), Some(given)) = (&owner, given) {
                    changes.set(owner.clone(), given);
                }
                Call::Update(name, key, changes)
            }
            OperationKind::Delete { .. } => Call::Delete(name, key),
        };
        (call, refused)
    }

    /// Another person than `person`, as the owner columns hold them.
    fn other(&mut self, person: &str) -> Value {
        let persons = match self.kind {
            Kind::Random => &["A", "B"][..],
            Kind::Ranking => &["A", "B", "C"][..],
        };
        let others: Vec<&str> = persons.iter().copied().filter(|p| *p != person).collect();
        Value::from(others[self.rng.below(others.len() as u64) as usize])
    }

    /// What a client of the facade writes in `column` in place of `value`: in an encrypted column,
    /// the plaintext of a new note, or a null one time in five, if it takes it; in another,
    /// `value`.
    fn plaintext(&mut self, column: &ColumnDef, value: &Value) -> Value {
        if !column.is_encrypted() {
            return value.clone();
        }
        if column.is_nullable() && self.rng.chance(20) {
            return Value::Null;
        }
        self.notes += 1;
        note(self.notes)
    }

    /// A random `watch` of a client of the facade: of the row of the key of a random operation,
    /// or of a scan of its table, from that key, or from the start, to the key of another of the
    /// table or to the end, forward or backward, with a limit now and then, and only some of the
    /// columns now and then; of the table of the rounds of the reference app if `rounds`.
    fn watch(&mut self, rounds: bool) -> Watch {
        let mut op = self.random_operation();
        while rounds && op.table_id() != ROUNDS {
            op = self.random_operation();
        }
        let table = self.schema.get_table_by_id(op.table_id()).unwrap().clone();
        let name = table.name().to_string();
        if self.rng.chance(50) {
            return Watch::Row(name, key_of(&table, &op));
        }
        let other = self.random_operation();
        let end = (other.table_id() == op.table_id()).then(|| key_of(&table, &other));
        let bound = |rng: &mut Rng, key: Option<Key>| match (key, rng.below(3)) {
            (None, _) | (_, 0) => Bound::Unbounded,
            (Some(key), 1) => Bound::Included(key),
            (Some(key), _) => Bound::Excluded(key),
        };
        let mut scan = Scan {
            start: bound(&mut self.rng, Some(key_of(&table, &op))),
            end: bound(&mut self.rng, end),
            ..Scan::new()
        };
        if self.rng.chance(50) {
            scan.direction = Direction::Backward;
        }
        if self.rng.chance(30) {
            scan.limit = Some(1 + self.rng.below(3) as usize);
        }
        if self.rng.chance(30) {
            let columns = table.columns().iter().filter(|_| self.rng.chance(50));
            let columns: Vec<String> = columns.map(|c| c.name().to_string()).collect();
            scan.columns = (!columns.is_empty()).then_some(columns);
        }
        Watch::Rows(name, scan)
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
        let (mut wrote, mut unowned) = (BTreeSet::new(), false);
        for member in (1..self.members.len()).flat_map(|member| [member, member]) {
            if !self.rng.chance(50) {
                continue;
            }
            let worker = self.members[member].worker.is_some();
            let facade = self.members[member].facade.is_some();
            // A client of the facade writes once per step at most, and only one of them in a table
            // without an owner, so that their writes are told apart by their timestamps and their
            // owners; and it watches in place of the heartbeats of the core.
            let action = match self.rng.below(10) {
                _ if worker && self.rng.chance(10) => Action::Freshness(self.freshness()),
                0..=4 if facade && !wrote.insert(member) => Action::Sync,
                0..=4 if facade => {
                    let op = self.operation(member);
                    let table = self.schema.get_table_by_id(op.table_id()).unwrap();
                    match table.owner() {
                        None if std::mem::replace(&mut unowned, true) => Action::Sync,
                        _ => {
                            let person = self.members[member].person;
                            let (call, refused) = self.call(person, &op);
                            Action::Call(call, refused)
                        }
                    }
                }
                0..=4 => Action::Write(self.operation(member)),
                5 | 6 if !facade => Action::Sync,
                5 => Action::Sync,
                6 | 7 if facade => {
                    // Of the rounds, half of the time, in the client that reads their notes.
                    let crypto = self.members[member].facade.is_some_and(|f| f.crypto);
                    let ranking = matches!(self.kind, Kind::Ranking);
                    let rounds = crypto && ranking && self.rng.chance(50);
                    Action::Watch(self.watch(rounds))
                }
                8 if facade => Action::Unwatch(self.rng.below(WATCHES as u64) as usize),
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
    /// that ended, every replica and the `watch` streams of the facade are checked, and each client
    /// with a worker starts it again if it stopped.
    async fn step(&mut self, plans: &[Plan]) {
        let last = self.incarnations.last().map(|incarnation| incarnation.id);
        for plan in plans {
            if let Action::Reopen = plan.action {
                self.members[plan.member].opening(&self.room, last).await;
            }
        }
        // The writes of the facade, whose timestamps are of the wall clock, go a millisecond
        // after those of the step before.
        let calls = plans.iter().any(|p| matches!(p.action, Action::Call(..)));
        while calls && wall_clock() <= self.wall {
            std::thread::sleep(Duration::from_micros(100));
        }
        self.net.record.lock().unwrap().sent.clear();
        let room_id = &self.room;
        let mut before = Vec::new();
        for (index, member) in self.members.iter().enumerate() {
            let status = member.device.status(room_id).await;
            before.push((
                status.ok().map(|status| status.epoch),
                self.net.downloads(index),
            ));
        }
        let may_wait = self.net.fuzz.lock().unwrap().is_some();
        let net = &self.net;
        let outcomes = join_all(plans.iter().map(|plan| {
            let device = &self.members[plan.member].device;
            let action = async move {
                match &plan.action {
                    Action::Write(op) => device.write(room_id, op).await,
                    Action::Call(call, _) => device.facade().unwrap().write(room_id, call).await,
                    Action::Sync => device.sync(room_id).await,
                    Action::Heartbeat => device.heartbeat(room_id).await,
                    Action::Reopen => {
                        let _ = device.close_room(room_id).await;
                        device.open_room(room_id).await
                    }
                    Action::Freshness(freshness) => device.set_freshness(room_id, *freshness).await,
                    Action::Watch(watch) => device.facade().unwrap().watch(room_id, watch),
                    Action::Unwatch(n) => {
                        device.facade().unwrap().unwatch(*n);
                        Ok(())
                    }
                }
            };
            // The epoch and the downloads of the client once a write ended: what its replica may
            // have skipped meanwhile.
            async move {
                let outcome = cut(action, plan.polls, may_wait).await;
                let status = device.status(room_id).await;
                let ended = (
                    status.ok().map(|status| status.epoch),
                    net.downloads(plan.member),
                );
                (outcome, ended)
            }
        }))
        .await;
        if calls {
            self.wall = wall_clock();
        }
        // The mode of the room in a client of the facade, which sets it at once.
        for plan in plans {
            let member = &mut self.members[plan.member];
            if let (Action::Freshness(freshness), Some(_)) = (&plan.action, member.facade) {
                member.worker = Some(*freshness);
            }
        }
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
            let written = match &plan.action {
                Action::Write(op) => Written::Core(op),
                Action::Call(_, refused) => Written::Facade(*refused),
                _ => continue,
            };
            let Some(result) = outcome else {
                continue;
            };
            let (epoch, downloads) = before[plan.member];
            let epoch_changed = epoch_then != epoch;
            let downloaded = downloads_then > downloads;
            let reopened = plans
                .iter()
                .any(|other| other.member == plan.member && matches!(other.action, Action::Reopen));
            let skipped = crashed.contains(&plan.member) || epoch_changed || downloaded || reopened;
            self.check_write(plan.member, written, result, skipped);
        }
        for (index, member) in self.members.iter().enumerate() {
            let of = |plan: &&Plan| plan.member == index;
            let reopens = (plans.iter().filter(of))
                .filter(|p| matches!(p.action, Action::Reopen))
                .count();
            let changed =
                (plans.iter().filter(of)).any(|p| matches!(p.action, Action::Freshness(_)));
            // In the facade, an open starts the worker, as the change of the mode does in the core.
            let closed = match member.facade {
                None => reopens > 0 && changed,
                Some(_) => reopens > 1,
            };
            self.check(member, closed && !crashed.contains(&index))
                .await;
            self.check_watches(member).await;
        }
        // The discards of the step that the opens did add stay; the others go.
        for member in &mut self.members {
            if let Ok(status) = member.device.status(&self.room).await {
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
            // That of a step that changed it, also if the step was cut. In the facade, the mode
            // of the room, which the step set.
            if let (Ok(status), None) = (member.device.status(&self.room).await, member.facade) {
                if let Some(worker) = status.worker {
                    freshness = worker.freshness;
                }
            }
            member.worker = Some(freshness);
            let _ = member.device.run_worker(&self.room, freshness).await;
        }
    }

    /// Has the archivist catch up with the room, and the model apply what it received.
    async fn archive(&mut self) {
        let archivist = &self.members[ARCHIVIST].device;
        let _ = archivist.sync(&self.room).await;
        let manager = &self.server.state.room_manager;
        let head = manager.log_bounds(&self.room).await.unwrap().head_seq.get();
        let status = archivist.status(&self.room).await.unwrap();
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

    /// Checks how a write by client `member` ended, of the operation it sent, if any: with the
    /// decision of the model on the sequence its commit got; as uncertain only if an attempt was
    /// and no `CommitAck` came, or one came and its replica may have `skipped` the sequence,
    /// discarded, with a snapshot or closed; and with any other error only if it had no effect. One
    /// that the facade decides locally sent nothing, and one that it refuses is `InvalidOperation`,
    /// unless the room is not open (`RoomClosed`). Fuzzing, a reply in place of the server's may
    /// end it with any error.
    fn check_write(
        &self,
        member: usize,
        written: Written,
        result: Result<(), ClientError>,
        skipped: bool,
    ) {
        let record = self.net.record.lock().unwrap();
        let incarnation = self.incarnations.last().unwrap();
        let (op, landed) = match written {
            Written::Core(op) => (Some(op), incarnation.landed(op.timestamp())),
            Written::Facade(_) => {
                let sent = record.sent.get(&member);
                let sent: Vec<&Operation> = sent.into_iter().flat_map(|s| s.values()).collect();
                assert!(sent.len() <= 1, "Client {member} sent {sent:?} for a write");
                let op = sent.first().copied();
                (op, op.map_or(Vec::new(), |op| incarnation.landed_as(op)))
            }
        };
        let commits = op.and_then(|op| record.commits.get(&(member, op.timestamp())));
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
        if let Written::Facade(refused) = written {
            let case = facade_case(&result);
            if case.starts_with("decided locally") {
                assert!(
                    op.is_none(),
                    "{write}: the facade decided it locally and sent it"
                );
            }
            self.net.saw(case);
            if let Some(refused) = refused {
                let closed = matches!(result, Err(ClientError::RoomClosed(_)));
                let invalid = matches!(result, Err(ClientError::InvalidOperation(_)));
                assert!(
                    invalid || closed,
                    "{write}: the facade did not refuse it, {refused}"
                );
                if invalid {
                    self.net.saw(refused);
                }
            }
        }
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
    /// unless the step `closed` its room while it changed the mode of the worker, or, in the
    /// facade, while it opened the room again: a worker that the change, or the open, started
    /// while the room closed stops at its next step, as the room is out of the client. Its worker
    /// may change the replica meanwhile: its rows are read again until its head and its epoch are
    /// the same before and after reading them.
    async fn check(&self, member: &Member, closed: bool) {
        let client = member.device.client_id();
        let mut unread = String::new();
        for _ in 0..1000 {
            let Ok(status) = member.device.status(&self.room).await else {
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
            let again = member.device.status(&self.room).await;
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

    /// Checks the `watch` streams of `member`, if it is a client of the facade with its room open,
    /// once they read what the replica changed: the last element of each is the value of a direct
    /// read of the replica, or an error after it, of a state in between; none gave its last value
    /// again; and the status of the room counts a subscription per stream alive, without those
    /// dropped or ended. Its worker may change the replica meanwhile: the streams and the replica
    /// are read again until no stream gives an element between a direct read of each and the
    /// next, and the head and the epoch of the replica are the same before and after.
    async fn check_watches(&self, member: &Member) {
        let Some(facade) = member.device.facade() else {
            return;
        };
        let client = member.device.client_id();
        let mut unread = String::new();
        for _ in 0..1000 {
            let Ok(status) = member.device.status(&self.room).await else {
                return;
            };
            // The paused clock moves once every task waits, the tasks of the streams too.
            tokio::time::sleep(Duration::from_millis(1)).await;
            let watched: Vec<(Watched, Arc<Mutex<Seen>>)> = {
                let mut watches = facade.watches.lock().unwrap();
                watches.retain(|watcher| !watcher.seen.lock().unwrap().ended);
                let watched = watches.iter();
                watched
                    .map(|w| (w.watched.clone(), Arc::clone(&w.seen)))
                    .collect()
            };
            let elements = || -> Vec<(usize, bool)> {
                let seen = watched.iter().map(|(_, seen)| seen.lock().unwrap());
                seen.map(|seen| (seen.elements, seen.ended)).collect()
            };
            let given = elements();
            let mut reads = Vec::new();
            for (watched, _) in &watched {
                reads.push(watched.read().await);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            let again = member.device.status(&self.room).await;
            let still = again.as_ref().is_ok_and(|again| {
                (again.epoch, again.head_seq) == (status.epoch, status.head_seq)
            });
            let closed = (reads.iter()).any(|read| matches!(read, Err(ClientError::RoomClosed(_))));
            if !still || closed || elements() != given {
                unread = format!("{given:?} and then {:?}, {reads:?}", elements());
                continue;
            }
            let alive = again.unwrap().watches;
            assert_eq!(
                alive,
                watched.len(),
                "The status of {client} counts {alive} watches, with {} streams alive",
                watched.len()
            );
            for ((watched, seen), read) in watched.iter().zip(reads) {
                let seen = seen.lock().unwrap();
                assert!(
                    !seen.repeated,
                    "A watch of {client} gave its last value again: {watched:?}"
                );
                let last = seen.last.as_ref();
                let reflects = match (&read, last) {
                    (Ok(rows), Some(Ok(last))) => rows == last,
                    (Ok(rows), Some(Err(_))) => seen.value.as_ref() == Some(rows),
                    (Err(err), Some(Err(last))) => err == last,
                    _ => false,
                };
                let values = read.iter().flatten().flat_map(|row| row.iter());
                let clear = |value: &Value| match value {
                    Value::String(text) => holds(text.as_bytes(), MARK),
                    _ => false,
                };
                if values.into_iter().any(|(_, value)| clear(value)) {
                    self.net.saw("note read in the clear");
                }
                assert!(
                    reflects,
                    "A watch of {client}, {watched:?}, gave {last:?} last, after {:?}, and a read \
                     gives {read:?}",
                    seen.value
                );
                self.net.saw("watch checked");
            }
            return;
        }
        panic!("The watches of {client} do not hold still: {unread}");
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
            let _ = member.device.run_worker(&self.room, freshness).await;
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
    /// past their heads, and its head before, by ten sequences if `pruning`, or by one: once they
    /// are back, they are behind the head. If `pruning`, the log is pruned too: those whose worker syncs by itself
    /// are behind the log, most of them, and catch up from a snapshot that another client
    /// uploads, such as the archivist, at the head. A replica that is still of an earlier
    /// incarnation of the room (a client that crashed opens again from its metadata, and its
    /// registration may fail before it is without network) is behind the head and the log at any
    /// head of its own.
    async fn isolate(&mut self, pruning: bool) {
        let workers: BTreeSet<usize> = (self.members.iter().enumerate())
            .filter(|(_, member)| member.worker.is_some())
            .map(|(index, _)| index)
            .collect();
        // With their rooms open, as their applications leave them, so that the heads of the
        // workers are known and the others write.
        for index in 0..self.members.len() {
            self.opened(index).await;
        }
        self.run_workers().await;
        *self.net.offline.lock().unwrap() = workers.iter().copied().collect();
        // What they took before that, and the changes it brought.
        self.settle().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        // A replica of an earlier incarnation, whose registrations did not meet the room created
        // again, is behind the head at any head of its own.
        let mut top = 0;
        for &index in &workers {
            if let (Ok(status), true) = (
                self.members[index].device.status(&self.room).await,
                self.of_last(&self.members[index]).await,
            ) {
                top = top.max(status.head_seq.get());
            }
        }
        // Past the head of the log too, which may be ahead of theirs, so that the writes seal a
        // segment that the prune drops.
        let head = self.incarnations.last().unwrap().log.len() as u64;
        let past = top.max(head) + if pruning { 10 } else { 1 };
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
        // One of an earlier incarnation is discarded with the `RoomReset` of its registration, and
        // registers again without a cursor: behind the log too.
        for index in self.syncing() {
            let status = self.members[index].device.status(&self.room).await;
            let earlier = !self.of_last(&self.members[index]).await;
            if status.is_ok_and(|status| status.head_seq.get() + 1 < tail || earlier) {
                self.behind_log.insert(index);
            }
        }
        assert!(
            self.syncing().is_empty() || !self.behind_log.is_empty(),
            "No client with a worker is behind the log at {tail}"
        );
    }

    /// Whether the replica of `member` is of the last incarnation of the room, by its metadata.
    async fn of_last(&self, member: &Member) -> bool {
        let last = self.incarnations.last().unwrap().id;
        let metadata = member.metadata.of(&self.room).await;
        metadata.is_some_and(|metadata| metadata.room_incarnation == last)
    }

    /// Whether `member` is registered at the head of the room, in its last incarnation.
    async fn at_head(&self, member: &Member) -> bool {
        let incarnation = self.incarnations.last().unwrap();
        let status = member.device.status(&self.room).await;
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
                let client = member.device.client_id();
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
                    behind.push(member.device.client_id());
                }
            }
            if behind.is_empty() {
                for (&index, downloads) in behind_log.iter().zip(downloads) {
                    let client = self.members[index].device.client_id();
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
                        let _ = member.device.heartbeat(&self.room).await;
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
        self.unfuzz().await;
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

    /// Has the network fuzz no more. The replies in place of the server's may have left a replica
    /// waiting for a snapshot that the server does not announce, until its room opens again: every
    /// client but the archivist closes its room, which stops its worker.
    async fn unfuzz(&mut self) {
        if self.net.fuzz.lock().unwrap().take().is_some() {
            for member in &self.members[ARCHIVIST + 1..] {
                let _ = member.device.close_room(&self.room).await;
            }
        }
    }

    /// Opens the room in client `member` until it is open. Each attempt counts as an open without
    /// metadata if the client has none, also that of an empty replica, which does not discard it.
    async fn opened(&mut self, member: usize) {
        for _ in 0..100 {
            let member = &mut self.members[member];
            if member.device.status(&self.room).await.is_ok() {
                return;
            }
            member.opening(&self.room, None).await;
            let _ = member.device.open_room(&self.room).await;
        }
        panic!("The room does not open");
    }

    /// Syncs client `member` until a sync ends well.
    async fn synced(&self, member: usize) {
        for _ in 0..100 {
            if self.members[member].device.sync(&self.room).await.is_ok() {
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

/// The cases of the facade: its writes taken, rejected by the replica and decided locally without
/// a network, with a key or a row that it refuses, without a `CryptoEngine` and by the precheck; a
/// commit, and a snapshot uploaded, that hold encrypted values, and the snapshots uploaded,
/// decoded; the `watch` streams checked, and the events with operations that reached a client of
/// the facade; the writes that it refuses, with the owner column or a key of another owner; and a
/// note read in the clear.
const CASES: [&str; 14] = [
    "write taken",
    "write rejected by the replica",
    "decided locally: Offline",
    "decided locally: InvalidOperation",
    "decided locally: Crypto",
    "decided locally: PrecheckFailed",
    "commit with a value encrypted",
    "snapshot uploaded, decoded",
    "snapshot with a value encrypted",
    "watch checked",
    "ops event to a client of the facade",
    "refused: the owner column given",
    "refused: a key of another owner",
    "note read in the clear",
];

/// What the network counted in a simulation: how many times it injected each fault of the event
/// streams and of the tokens, and how many times the checks saw each case of the facade.
type Counts = BTreeMap<&'static str, usize>;

/// What the network puts in place of blocks of the event streams while it fuzzes.
const FUZZED: [&str; 9] = [
    "random bytes",
    "event damaged",
    "frame damaged",
    "endless line",
    "random message",
    "another room",
    "another incarnation",
    "random event",
    "invalid operation",
];

/// A simulation of `steps` random steps over `kind`, with workers, fuzzing the replies and the
/// event streams if `fuzz`, with the network healed at the end, once it fuzzes no more: then some
/// event with operations of the server reached a client, the event streams or their openings had
/// faults, and, fuzzing, other blocks took the place of some; and without fuzzing, the clients of
/// the facade wrote, had streams checked and got events with operations, and the snapshots
/// uploaded were decoded, and, over the reference app, a commit of the facade and a snapshot held
/// its notes encrypted. Returns what the network injected (the faults of the event streams and of
/// the tokens, and each block and refusal in place of the server's) and the cases of the facade
/// that the checks saw.
async fn simulate(seed: u64, kind: Kind, steps: usize, fuzz: bool) -> (Counts, Counts) {
    let mut sim = Sim::new(seed, kind, fuzz, true).await;
    for step in 0..steps {
        sim.event(step).await;
        let plans = sim.plans();
        sim.step(&plans).await;
    }
    sim.unfuzz().await;
    sim.assign_modes().await;
    sim.isolate(true).await;
    sim.heal().await;
    let ops = sim.net.ops.load(Ordering::SeqCst);
    assert!(ops > 0, "No event with operations reached a client");
    let cases = sim.net.cases.lock().unwrap().clone();
    if !fuzz {
        let mut seen = vec![CASES[0], CASES[7], CASES[9], CASES[10]];
        if let Kind::Ranking = kind {
            seen.extend([CASES[6], CASES[8]]);
        }
        for case in seen {
            assert!(
                cases.get(case).is_some_and(|count| *count > 0),
                "{case}: {cases:?}"
            );
        }
    }
    let faults = sim.net.faults.lock().unwrap().clone();
    // Of a block or of an opening: some runs have few blocks of their own.
    let streams = FAULTS[..9]
        .iter()
        .filter_map(|fault| faults.get(fault))
        .sum::<usize>();
    assert!(streams > 0, "No fault of an event stream: {faults:?}");
    let fuzzed = FUZZED
        .iter()
        .filter_map(|kind| faults.get(kind))
        .sum::<usize>();
    assert!(
        !fuzz || fuzzed > 0,
        "No block in place of the server's: {faults:?}"
    );
    (faults, cases)
}

/// Adds the counts of `faults` to `total`.
fn add(total: &mut Counts, faults: Counts) {
    for (fault, count) in faults {
        *total.entry(fault).or_insert(0) += count;
    }
}

/// Checks that `faults` has every one of `kinds`.
fn every(faults: &Counts, kinds: &[&str]) {
    for kind in kinds {
        let happened = faults.get(kind).is_some_and(|count| *count > 0);
        assert!(happened, "{kind}: {faults:?}");
    }
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
    let (mut faults, mut cases) = (Counts::new(), Counts::new());
    for seed in seeds(1, 16) {
        for kind in [Kind::Random, Kind::Ranking] {
            let (injected, seen) = simulate(seed, kind, 400, false).await;
            add(&mut faults, injected);
            add(&mut cases, seen);
        }
    }
    // Every fault of the event streams and of the tokens, and a token that the provider gave
    // already expired, which the server refused; and every case of the facade.
    every(&faults, &FAULTS);
    every(&faults, &["token refused by the server"]);
    every(&cases, &CASES);
}

#[tokio::test(start_paused = true)]
async fn no_reply_or_event_in_place_of_the_servers_panics_a_client_or_moves_its_replica_off_the_log(
) {
    for seed in seeds(0, 1) {
        simulate(seed, Kind::Random, 96, true).await;
        simulate(seed, Kind::Ranking, 96, true).await;
    }
}

#[tokio::test(start_paused = true)]
#[ignore = "long-running simulation; run with --ignored in release"]
async fn no_reply_or_event_in_place_of_the_servers_panics_a_client_over_many_long_simulations() {
    let mut faults = BTreeMap::new();
    for seed in seeds(1, 16) {
        for kind in [Kind::Random, Kind::Ranking] {
            add(&mut faults, simulate(seed, kind, 400, true).await.0);
        }
    }
    // Every kind of block in place of the server's, and a refusal at random.
    every(&faults, &FUZZED);
    every(&faults, &["refusal at random"]);
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
    sim.join("C", true, None, None).await;
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
