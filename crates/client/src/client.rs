use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;
use zemdb_core::{
    decode_message, ClientId, ClientMessage, CorrelationId, ErrorCode, MutationId, Operation,
    RoomId, SchemaId, SequenceNumber, SequencedOperation, ServerMessage,
};
use zemdb_storage::{StorageEngine, StorageError};

use crate::{ClientConfig, ClientError, MetadataStore, RoomMetadata, TransportClient};

/// The size of the chunks of a download, the largest that the relay serves, and the largest of an
/// upload.
const CHUNK_BYTES: u32 = 4 << 20;

/// The first chunk of a download after one of a snapshot larger than `max_snapshot_bytes`: the
/// smallest the relay serves, enough to learn whether the snapshot it serves still is.
const PROBE_BYTES: u32 = 64 << 10;

/// How many times a chunk of a snapshot is sent again after a transient failure.
const CHUNK_RETRIES: u32 = 3;

/// How long a replica whose upload failed uploads no other: the time the server gives a client it
/// designates to start uploading, after which it designates one again.
const UPLOAD_PAUSE: Duration = Duration::from_secs(60);

/// The client of a device: one replica per room it opens.
pub struct Client {
    client_id: ClientId,
    config: ClientConfig,
    local: Local,
    transport: Box<dyn TransportClient>,
    /// Every room the client opened, open or not: a room keeps its epoch while the client lives.
    rooms: Mutex<HashMap<RoomId, Arc<Room>>>,
    correlation_ids: AtomicU64,
}

/// The diagnosis of a room open in the client ([`Client::status`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomStatus {
    /// The head sequence of the replica.
    pub head_seq: SequenceNumber,
    /// How many times the client discarded the replica since it first opened the room, counting
    /// the discard of a room opened without metadata.
    pub epoch: u64,
    /// Whether the client is registered in the room.
    pub registered: bool,
    /// Whether the room fails closed: its schema changed to one that does not evolve from the
    /// replica's, or the server sent an operation that does not validate against it.
    pub failed_closed: bool,
    /// The writes in flight.
    pub writes_in_flight: usize,
    /// The rejections and the ranges without a report that the room keeps for the writes in
    /// flight.
    pub retained: usize,
}

/// The local side of the client, which the changes of the replicas use from their tasks.
#[derive(Clone)]
struct Local {
    storage: Arc<dyn StorageEngine>,
    metadata: Arc<dyn MetadataStore>,
    /// Set by `shutdown`: no replica opens afterwards.
    shut_down: Arc<AtomicBool>,
}

/// A room of the client.
struct Room {
    id: RoomId,
    /// The applier: every change of the replica runs under it, in a task of its own.
    applier: Arc<tokio::sync::Mutex<()>>,
    /// Held by an opening, with its registration: one registration at a time.
    registration: tokio::sync::Mutex<()>,
    /// Held by the download or the upload of a snapshot: one at a time.
    transfer: tokio::sync::Mutex<()>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    replica: Replica,
    /// How many times the client discarded the replica.
    epoch: u64,
    /// The token of the last `open_room`.
    token: String,
    writes: Writes,
}

/// The replica of a room.
#[derive(Default)]
enum Replica {
    /// Out of the client: not opened, closed, or a change of it failed.
    #[default]
    Closed,
    /// Without data or metadata, until `Registered` gives the metadata to open it with: opened
    /// without metadata, or discarded.
    Empty,
    /// Open in the storage engine.
    Open(OpenReplica),
}

struct OpenReplica {
    /// The metadata that describe all of its data.
    metadata: RoomMetadata,
    registered: bool,
    /// Behind the log of the server, it waits for a snapshot: with whether a reply announced a
    /// usable one.
    awaits_snapshot: Option<bool>,
    /// The sequence of the last snapshot it dropped, which it does not download again.
    dropped: Option<SequenceNumber>,
    /// The last chunk of a download it checked announced a snapshot larger than
    /// `max_snapshot_bytes`: the next download asks for a first chunk of [`PROBE_BYTES`].
    oversized: bool,
    /// When its last upload failed.
    upload_failed: Option<Instant>,
    /// Its schema changed to one that does not evolve from its own, or the server sent an
    /// operation that does not validate against it: it applies nothing more, and neither
    /// registers nor syncs, until it opens again.
    failed_closed: bool,
}

impl OpenReplica {
    fn new(metadata: RoomMetadata, registered: bool) -> Self {
        Self {
            metadata,
            registered,
            awaits_snapshot: None,
            dropped: None,
            oversized: false,
            upload_failed: None,
            failed_closed: false,
        }
    }

    fn fail_closed(&mut self) {
        self.failed_closed = true;
        self.registered = false;
    }
}

/// The writes in flight in a room, and what its applier retains for them. A replica that is not
/// open, or fails closed, has none.
#[derive(Default)]
struct Writes {
    /// The highest head of the server that the replies of the epoch reported.
    head_seen: SequenceNumber,
    entries: HashMap<MutationId, Entry>,
    /// The decisions other than `Ok` of the sequences after the lowest `seen` of the writes
    /// without their `CommitAck`, by their first sequence, with their last: a rejection, or a
    /// range the replica passed without a report, whose outcome is unknown.
    retained: BTreeMap<SequenceNumber, (SequenceNumber, ClientError)>,
}

/// A write in flight.
enum Entry {
    /// Without its `CommitAck`: the applier retains the decisions of the sequences after
    /// `seen`, the head the write saw before its first attempt, which its sequence is after.
    Sent { seen: SequenceNumber },
    /// With its `CommitAck`: its sequence, and the decision of the replica on it once it has
    /// one.
    Acked {
        seq: SequenceNumber,
        decision: Option<Result<(), ClientError>>,
    },
}

/// The decision of the replica on `seq` that `retained` keeps: `Ok` unless it keeps another.
fn decision(
    retained: &BTreeMap<SequenceNumber, (SequenceNumber, ClientError)>,
    seq: SequenceNumber,
) -> Result<(), ClientError> {
    match retained.range(..=seq).next_back() {
        Some((_, (last, err))) if seq <= *last => Err(err.clone()),
        _ => Ok(()),
    }
}

impl Writes {
    /// The lowest `seen` of the writes without their `CommitAck`.
    fn seen(&self) -> Option<SequenceNumber> {
        let sent = self.entries.values().filter_map(|entry| match entry {
            Entry::Sent { seen } => Some(*seen),
            Entry::Acked { .. } => None,
        });
        sent.min()
    }

    /// Drops what no write without its `CommitAck` needs.
    fn prune(&mut self) {
        let seen = self.seen();
        let needed = |last: &SequenceNumber| seen.is_some_and(|seen| *last > seen);
        self.retained.retain(|_, (last, _)| needed(last));
    }

    /// Takes the decisions of the sequences the replica applied, up to `head`: `Ok`, but for
    /// the ranges of `decisions`. The writes waiting for one of them get it (the head moves
    /// through every sequence after theirs), and the writes without their `CommitAck` retain
    /// them.
    fn applied(
        &mut self,
        head: SequenceNumber,
        decisions: impl IntoIterator<Item = (SequenceNumber, SequenceNumber, ClientError)>,
    ) {
        let decisions = decisions
            .into_iter()
            .map(|(first, last, err)| (first, (last, err)));
        self.retained.extend(decisions);
        for entry in self.entries.values_mut() {
            if let Entry::Acked {
                seq,
                decision: slot,
            } = entry
            {
                if slot.is_none() && *seq <= head {
                    *slot = Some(decision(&self.retained, *seq));
                }
            }
        }
        self.prune();
    }

    /// Takes the `CommitAck` of the write `mutation_id`, which assigns it `seq`, with the
    /// replica at `head` (`None` if it could not be read). A sequence after the head is waited
    /// for, and one at or below it takes the decision retained; one at or below what the write
    /// saw is unknown, since the server sequenced it before the write's first attempt: it lost
    /// the sequences after it and assigned them again.
    fn acked(
        &mut self,
        mutation_id: &MutationId,
        seq: SequenceNumber,
        head: Option<SequenceNumber>,
    ) {
        let Some(&Entry::Sent { seen }) = self.entries.get(mutation_id) else {
            return;
        };
        let decision = match head {
            Some(head) if seq > seen && seq > head => None,
            Some(_) if seq > seen => Some(decision(&self.retained, seq)),
            _ => Some(Err(ClientError::OutcomeUnknown)),
        };
        let acked = Entry::Acked { seq, decision };
        self.entries.insert(*mutation_id, acked);
        self.prune();
    }
}

/// A write in flight in a room, from before its first attempt: its entry leaves the room when
/// the write ends or its call is dropped.
struct InFlight {
    room: Arc<Room>,
    mutation_id: MutationId,
}

impl InFlight {
    /// Enters the write `mutation_id` in `room`, whose replica is at `head`, under its applier:
    /// it saw the higher of `head` and the head seen.
    fn new(room: &Arc<Room>, mutation_id: MutationId, head: SequenceNumber) -> Self {
        let writes = &mut room.state().writes;
        let seen = head.max(writes.head_seen);
        writes.entries.insert(mutation_id, Entry::Sent { seen });
        Self {
            room: Arc::clone(room),
            mutation_id,
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let writes = &mut self.room.state().writes;
        if writes.entries.remove(&self.mutation_id).is_some() {
            writes.prune();
        }
    }
}

/// The delays between the retries of a step: a random one up to `backoff_base`, doubled after
/// each retry up to `backoff_max` (full jitter).
struct Backoff {
    base: Duration,
    max: Duration,
    retries: u32,
}

impl Backoff {
    fn new(config: &ClientConfig) -> Self {
        Self {
            base: config.backoff_base,
            max: config.backoff_max,
            retries: 0,
        }
    }

    fn next(&mut self) -> Duration {
        let ceiling = (self.base.saturating_mul(2u32.saturating_pow(self.retries))).min(self.max);
        self.retries = self.retries.saturating_add(1);
        let random = Uuid::new_v4().as_u128() % (ceiling.as_nanos() + 1);
        Duration::from_nanos(u64::try_from(random).unwrap_or(u64::MAX))
    }
}

/// Whether a reply that announces the version `version` of the schema `schema_id` changes the
/// schema of a replica with `metadata`: another schema, or a later version of its own. An
/// earlier version of its own comes in a delayed reply.
fn changes(metadata: &RoomMetadata, schema_id: &SchemaId, version: u64) -> bool {
    *schema_id != metadata.schema_id || version > metadata.schema.version()
}

impl Room {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn is_open(&self) -> bool {
        matches!(self.state().replica, Replica::Open(_))
    }

    /// Changes the open replica with `change` if the replica is still that of `epoch`.
    fn update(&self, epoch: u64, change: impl FnOnce(&mut OpenReplica)) {
        let mut state = self.state();
        if state.epoch == epoch {
            if let Replica::Open(open) = &mut state.replica {
                change(open);
            }
        }
    }

    /// Takes `head`, a head of the server that a reply to a request of `epoch` reported.
    fn saw(&self, epoch: u64, head: SequenceNumber) {
        let mut state = self.state();
        if state.epoch == epoch {
            let writes = &mut state.writes;
            writes.head_seen = writes.head_seen.max(head);
        }
    }

    /// Whether a reply that announces the version `version` of the schema `schema_id` changes the
    /// schema of the replica.
    fn changes(&self, (schema_id, version): &(SchemaId, u64)) -> bool {
        let state = self.state();
        let open = match &state.replica {
            Replica::Open(open) => open,
            _ => return false,
        };
        changes(&open.metadata, schema_id, *version)
    }

    /// Takes the usable snapshot that a reply to a request of `epoch` announced, at `snapshot`, if
    /// the replica waits for one: whether it waits for one announced, other than one it dropped.
    fn announced(&self, epoch: u64, snapshot: Option<SequenceNumber>) -> bool {
        let mut waits = false;
        self.update(epoch, |open| {
            if let Some(announced) = &mut open.awaits_snapshot {
                waits = snapshot.is_some() && snapshot != open.dropped;
                *announced = waits;
            }
        });
        waits
    }

    /// Whether the replica waits for a snapshot.
    fn awaits_snapshot(&self) -> bool {
        matches!(&self.state().replica, Replica::Open(open) if open.awaits_snapshot.is_some())
    }

    /// The incarnation of the replica of `epoch`, if it may upload a snapshot: it is open, does
    /// not fail closed, does not wait for a snapshot, and no upload of it failed in the last
    /// [`UPLOAD_PAUSE`].
    fn uploads(&self, epoch: u64) -> Option<u128> {
        let state = self.state();
        match &state.replica {
            Replica::Open(open)
                if state.epoch == epoch
                    && !open.failed_closed
                    && open.awaits_snapshot.is_none()
                    && open
                        .upload_failed
                        .is_none_or(|failed| failed.elapsed() >= UPLOAD_PAUSE) =>
            {
                Some(open.metadata.room_incarnation)
            }
            _ => None,
        }
    }

    /// Whether the write `mutation_id` is still in flight: closing the room, discarding its
    /// replica or failing it closed ends every write.
    fn in_flight(&self, mutation_id: &MutationId) -> bool {
        self.state().writes.entries.contains_key(mutation_id)
    }

    /// The decision of the write `mutation_id`, with its `CommitAck`, once it has one: an
    /// unknown outcome if its room ended it.
    fn decision(&self, mutation_id: &MutationId) -> Option<Result<(), ClientError>> {
        match self.state().writes.entries.get(mutation_id) {
            Some(Entry::Acked { decision, .. }) => decision.clone(),
            _ => Some(Err(ClientError::OutcomeUnknown)),
        }
    }

    /// Leaves the replica of `epoch` unregistered, for the next request to register it again
    /// with its cursor. Once per sync (`again`): a server that asks again contradicts itself.
    fn register_again(&self, epoch: u64, again: &mut bool) -> Result<(), ClientError> {
        if std::mem::replace(again, true) {
            return Err(ClientError::Network(
                "The server asked to register again twice".to_string(),
            ));
        }
        self.update(epoch, |open| open.registered = false);
        Ok(())
    }
}

/// What the next request of a replica presents, read under its applier, so that no change of
/// the replica is halfway.
struct Request {
    epoch: u64,
    token: String,
    /// The head and the incarnation of an open replica.
    cursor: Option<(SequenceNumber, u128)>,
    registered: bool,
    awaits_snapshot: Option<bool>,
    failed_closed: bool,
}

/// What the applier did with a batch.
enum Applied {
    /// The replica is at this head after it.
    Head(SequenceNumber),
    /// Nothing: the replica is closed, fails closed or is no longer of the epoch of the request.
    Stale,
    /// Nothing: its reply announces a change of the schema, or one of its operations does not
    /// validate against the schema of the replica, which registers again to learn the schema.
    Schema,
}

/// The last of `ops`, or `from_seq` if there are none, if they are the operations after
/// `from_seq` in order.
fn last_after(from_seq: SequenceNumber, ops: &[SequencedOperation]) -> Option<SequenceNumber> {
    let in_order = (ops.iter().zip(1..))
        .all(|(op, index)| op.seq.get().checked_sub(index) == Some(from_seq.get()));
    in_order.then(|| ops.last().map_or(from_seq, |op| op.seq))
}

/// Whether `ops`, of a batch with `head_seq` and `has_more` in reply to a sync after `from_seq`,
/// are the operations after it in order, up to the head at most and, unless there are more,
/// reaching it.
fn follows(
    from_seq: SequenceNumber,
    head_seq: SequenceNumber,
    ops: &[SequencedOperation],
    has_more: bool,
) -> bool {
    // A batch with more brings some.
    last_after(from_seq, ops).is_some_and(|last| {
        last <= head_seq
            && if has_more {
                !ops.is_empty()
            } else {
                last == head_seq
            }
    })
}

/// The client id of `person` on the device `device_id`: `person#device`, with the device id in
/// hexadecimal, so that its owner is `person`.
fn client_id(person: &str, device_id: u128) -> Result<ClientId, ClientError> {
    if person.is_empty() || person.contains('#') {
        return Err(ClientError::Config(format!(
            "The person {person:?} must not be empty or hold '#'"
        )));
    }
    ClientId::new(format!("{person}#{device_id:032x}"))
        .map_err(|err| ClientError::Config(err.to_string()))
}

impl Client {
    /// Opens the client of `config.person` over `storage`, `metadata` and `transport`. It holds
    /// `metadata` until [`shutdown`](Self::shutdown): another client over the same metadata
    /// fails. The metadata are those of `storage`, which no other client uses: a disk engine
    /// with [`FileMetadata`](crate::FileMetadata) in its data directory, or a memory engine with
    /// [`MemoryMetadata`](crate::MemoryMetadata). Its client id is `person#device`, with the
    /// device id that `metadata` keeps.
    ///
    /// An invalid person or backoff is `Config`, before `metadata` is touched; an error of
    /// `metadata`, `Storage`.
    pub async fn open(
        config: ClientConfig,
        storage: impl StorageEngine + 'static,
        metadata: impl MetadataStore + 'static,
        transport: impl TransportClient + 'static,
    ) -> Result<Self, ClientError> {
        // Every device id makes a valid client id if one does.
        client_id(&config.person, 0)?;
        if config.backoff_base.is_zero() || config.backoff_max < config.backoff_base {
            return Err(ClientError::Config(
                "The backoff must start above zero, and its longest delay must not be shorter"
                    .to_string(),
            ));
        }
        let device_id = metadata.open().await?;
        Ok(Self {
            client_id: client_id(&config.person, device_id)?,
            config,
            local: Local {
                storage: Arc::new(storage),
                metadata: Arc::new(metadata),
                shut_down: Arc::default(),
            },
            transport: Box::new(transport),
            rooms: Mutex::default(),
            correlation_ids: AtomicU64::new(0),
        })
    }

    /// The client id, which the application sends to its backend to get the tokens of its
    /// rooms.
    pub fn client_id(&self) -> ClientId {
        self.client_id.clone()
    }

    /// Opens `room_id`, with `token` for its requests: opens its replica from its metadata and
    /// registers it, with the head of its storage room as its cursor and the incarnation of its
    /// metadata. Opening a room that is open registers it if it is not, with the new token.
    ///
    /// A room without metadata (new, or whose metadata are lost or do not verify) holds data of
    /// an unknown incarnation: its data are deleted, and it opens with the metadata of
    /// `Registered`. So are damaged data (`SnapshotCorruption`, `WalCorruption`); any other
    /// error opening the storage room, or reading the metadata, is `Storage` and keeps them.
    /// `RoomReset` or `InvalidSequence` to the cursor discard the replica, which registers again
    /// without a cursor.
    ///
    /// Without a valid reply, or refused by the server (`Network`), a room with metadata opens
    /// all the same, unregistered, to be read; one without them fails, and its next open
    /// registers it. A step that cannot write or remove the metadata, or delete the data, stops:
    /// the room is left out of the client with the error, and the next open resumes it.
    ///
    /// The schema comes from `Registered`: an open replica takes another schema, or a later
    /// version of its own, only if it evolves from its own, saving it before its storage room
    /// reloads it. If it does not, the room fails closed (`FailedClosed`), and stays open, to be
    /// read: the client leaves the roster of the room, and the room neither registers nor syncs
    /// until it is closed and opened again.
    pub async fn open_room(&self, room_id: RoomId, token: String) -> Result<(), ClientError> {
        let room = {
            let mut rooms = self.rooms.lock().unwrap_or_else(PoisonError::into_inner);
            let room = rooms.entry(room_id.clone()).or_insert_with(|| {
                Arc::new(Room {
                    id: room_id,
                    applier: Arc::default(),
                    registration: tokio::sync::Mutex::default(),
                    transfer: tokio::sync::Mutex::default(),
                    state: Mutex::default(),
                })
            });
            Arc::clone(room)
        };
        let _registration = room.registration.lock().await;
        room.state().token = token;
        self.change(&room, Local::open).await?;
        match self.register(&room).await {
            // Without a valid reply, a replica with metadata opens all the same, to be read.
            Err(ClientError::Network(err)) => {
                let opened = self.change(&room, |_, room| async move { Ok(room.is_open()) });
                match opened.await? {
                    true => Ok(()),
                    false => Err(ClientError::Network(err)),
                }
            }
            registered => registered,
        }
    }

    /// Registers `room`, under its registration lock, unless it is registered: with the head of
    /// its storage room as its cursor and the incarnation of its metadata, or without them if it
    /// has none. `RoomReset` or `InvalidSequence` to a cursor discard the replica, which
    /// registers again without one. A reply for a replica discarded meanwhile changes nothing,
    /// and the replica registers again. A replica that fails closed does not register.
    async fn register(&self, room: &Arc<Room>) -> Result<(), ClientError> {
        loop {
            let request = self.change(room, Local::request).await?;
            if request.failed_closed {
                return Err(ClientError::FailedClosed(room.id.clone()));
            }
            if request.registered {
                return Ok(());
            }
            let (epoch, token) = (request.epoch, request.token);
            let (cursor, incarnation) = request.cursor.unzip();
            let correlation_id = self.correlation_id();
            let message = ClientMessage::RegisterClient {
                correlation_id,
                room_id: room.id.clone(),
                client_id: self.client_id.clone(),
                auth_token: token.clone(),
                current_seq: cursor,
                known_incarnation: incarnation,
            };
            // A reply is checked before it is used: its correlation and its room are the
            // request's when it carries them, and a cursor's incarnation does not change.
            match self.send(&message, &token).await? {
                ServerMessage::Registered {
                    correlation_id: reply_id,
                    room_id,
                    head_seq,
                    tail_seq,
                    schema_id,
                    schema,
                    active_snapshot_seq,
                    room_incarnation,
                } if reply_id == correlation_id
                    && room_id == room.id
                    && incarnation.is_none_or(|known| known == room_incarnation) =>
                {
                    room.saw(epoch, head_seq);
                    let metadata = RoomMetadata {
                        room_incarnation,
                        schema_id,
                        schema,
                    };
                    let snapshot = (tail_seq, active_snapshot_seq);
                    let registered = move |local: Local, room| {
                        local.registered(room, epoch, cursor, metadata, snapshot)
                    };
                    match self.change(room, registered).await {
                        // Its schema failed the replica closed.
                        Err(err @ ClientError::FailedClosed(_)) => {
                            self.deregister(room, &token).await;
                            return Err(err);
                        }
                        registered => registered?,
                    }
                }
                ServerMessage::Error {
                    correlation_id: reply_id,
                    room_id,
                    code,
                    message,
                } if reply_id.is_none_or(|reply_id| reply_id == correlation_id)
                    && room_id.as_ref().is_none_or(|room_id| *room_id == room.id) =>
                {
                    match code {
                        ErrorCode::RoomReset | ErrorCode::InvalidSequence if cursor.is_some() => {
                            let discard = move |local: Local, room| local.discard_open(room, epoch);
                            self.change(room, discard).await?;
                        }
                        code => {
                            return Err(ClientError::Network(format!(
                                "The server refused the registration: {code:?}: {message}"
                            )))
                        }
                    }
                }
                reply => {
                    return Err(ClientError::Network(format!(
                        "Invalid reply to a registration: {reply:?}"
                    )))
                }
            }
        }
    }

    /// Takes the client out of the roster of `room`, whose replica failed closed, so that the
    /// client neither holds the pruning of the log back nor is chosen to upload a snapshot. Its
    /// caller holds the registration lock, so it goes after a registration in flight, and
    /// nothing is sent if the replica opened again meanwhile, and no longer fails closed. Its
    /// reply is not used: a lost deregistration, or one that a cut call never sent, leaves the
    /// entry until its lease ends.
    async fn deregister(&self, room: &Room, token: &str) {
        let reopened = matches!(&room.state().replica, Replica::Open(open) if !open.failed_closed);
        if reopened {
            return;
        }
        let message = ClientMessage::DeregisterClient {
            correlation_id: self.correlation_id(),
            room_id: room.id.clone(),
            client_id: self.client_id.clone(),
        };
        let _ = self.send(&message, token).await;
    }

    fn correlation_id(&self) -> CorrelationId {
        CorrelationId::new(self.correlation_ids.fetch_add(1, Ordering::Relaxed))
    }

    /// Sends `message` with `token` and decodes its reply. No reply, or one that does not
    /// decode, is `Network`.
    async fn send(
        &self,
        message: &ClientMessage,
        token: &str,
    ) -> Result<ServerMessage, ClientError> {
        let reply = (self.transport.send(message, token).await)
            .map_err(|err| ClientError::Network(err.to_string()))?;
        decode_message(&reply).map_err(|err| ClientError::Network(format!("Invalid reply: {err}")))
    }

    /// Runs `change` on `room` under its applier, in a task of its own, once the changes before
    /// it ended: a caller that stops waiting does not cut it halfway. A replica that is not open
    /// after it, or fails closed, ends the writes in flight, and retains nothing.
    async fn change<T, F>(
        &self,
        room: &Arc<Room>,
        change: impl FnOnce(Local, Arc<Room>) -> F,
    ) -> Result<T, ClientError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, ClientError>> + Send + 'static,
    {
        let applier = Arc::clone(&room.applier).lock_owned().await;
        let change = change(self.local.clone(), Arc::clone(room));
        let room = Arc::clone(room);
        let task = tokio::spawn(async move {
            let _applier = applier;
            let changed = change.await;
            let mut state = room.state();
            if !matches!(&state.replica, Replica::Open(open) if !open.failed_closed) {
                state.writes = Writes::default();
            }
            changed
        });
        task.await
            .map_err(|err| ClientError::Storage(format!("A change of the replica failed: {err}")))?
    }

    /// Reacts to `reply` to the request `correlation_id`, a sync, an ack or a heartbeat of the
    /// replica of `epoch` at `cursor`, when it is not the reply the request asks for: an error of
    /// the request (which may come without its correlation id and its room) that asks to register
    /// again is `Ok`: `ClientNotRegistered`, `InvalidSequence`, and `BehindCompaction` as
    /// [`Local::behind`] takes it, once a registration of the call (`again`) or not. Any other
    /// error, or reply, is `Network`.
    async fn refused(
        &self,
        room: &Arc<Room>,
        epoch: u64,
        cursor: SequenceNumber,
        correlation_id: CorrelationId,
        reply: ServerMessage,
        again: bool,
    ) -> Result<(), ClientError> {
        let invalid =
            || ClientError::Network("Invalid reply to a sync, an ack or a heartbeat".into());
        let ServerMessage::Error {
            correlation_id: reply_id,
            room_id,
            code,
            message,
        } = reply
        else {
            return Err(invalid());
        };
        if reply_id.is_some_and(|reply_id| reply_id != correlation_id)
            || room_id.is_some_and(|room_id| room_id != room.id)
        {
            return Err(invalid());
        }
        match code {
            ErrorCode::ClientNotRegistered | ErrorCode::InvalidSequence => return Ok(()),
            ErrorCode::BehindCompaction => {
                let behind = move |local: Local, room| local.behind(room, epoch, cursor, again);
                if self.change(room, behind).await? {
                    return Ok(());
                }
            }
            _ => {}
        }
        Err(ClientError::Network(format!(
            "The server refused the request: {code:?}: {message}"
        )))
    }

    /// The head of the storage room of `room_id`, read outside its applier: a storage room
    /// closed meanwhile is `RoomClosed`.
    async fn head(&self, room_id: &RoomId) -> Result<SequenceNumber, ClientError> {
        match self.local.storage.get_head_seq(room_id).await {
            Err(StorageError::RoomNotFound(_)) => Err(ClientError::RoomClosed(room_id.clone())),
            head => Ok(head?),
        }
    }

    /// Catches `room_id` up with the head of the server: asks for the operations after the head
    /// of its replica until the server has no more, applies them through its applier, and
    /// acknowledges the head reached. A room that is not registered registers first.
    ///
    /// A reply is checked before it is used: a batch with the correlation id and the room of the
    /// request, whose operations follow the cursor of the request in order, up to its head at
    /// most and, unless it has more, reaching it (with more, it brings some); and a confirmation
    /// of the head acknowledged. Any other reply, or none, is `Network`. A head below the cursor
    /// of the request, `InvalidSequence` and `ClientNotRegistered` register again, with the
    /// cursor, and the sync goes on; a second time in a sync is `Network`. `BehindCompaction`,
    /// or `Registered` behind the log, leave the replica waiting for a snapshot: it downloads
    /// and installs the one that `Registered` or a heartbeat announced, as
    /// [`heartbeat`](Self::heartbeat) says, and the sync goes on from it. Without one announced,
    /// the sync is `Network`, without sending anything.
    ///
    /// A batch is applied only with the schema it announces or a later version of it: one that
    /// announces another schema, or a later version, is not applied, and the replica registers
    /// again to take the schema of `Registered`, as an open does. So does a batch with an
    /// operation that does not validate against the schema of the replica; once the sync
    /// registered again for a signal, it fails the room closed. The sync of a room that fails
    /// closed is `FailedClosed`, without sending anything.
    ///
    /// A batch that asks the client for a snapshot (`snapshot_wanted`) is confirmed with a
    /// heartbeat once the sync ends, which uploads it as [`heartbeat`](Self::heartbeat) says.
    pub async fn sync(&self, room_id: RoomId) -> Result<(), ClientError> {
        match self.synced(room_id.clone()).await? {
            true => self.heartbeat(room_id).await,
            false => Ok(()),
        }
    }

    /// [`sync`](Self::sync) without its heartbeat: returns whether a batch asked the client for a
    /// snapshot.
    async fn synced(&self, room_id: RoomId) -> Result<bool, ClientError> {
        let room = self.room(&room_id)?;
        let (mut again, mut wanted) = (false, false);
        loop {
            let request = self.change(&room, Local::request).await?;
            let Some((from_seq, _)) = request.cursor.filter(|_| request.registered) else {
                let _registration = room.registration.lock().await;
                self.register(&room).await?;
                continue;
            };
            if let Some(announced) = request.awaits_snapshot {
                if !announced {
                    return Err(ClientError::Network(
                        "The replica waits for a snapshot".to_string(),
                    ));
                }
                self.snapshot(&room, &mut again).await?;
                continue;
            }
            let (epoch, token) = (request.epoch, request.token);
            let correlation_id = self.correlation_id();
            let message = ClientMessage::Sync {
                correlation_id,
                room_id: room.id.clone(),
                client_id: self.client_id.clone(),
                from_seq,
                // As many as the server serves in a reply.
                max_batch_size: u32::MAX,
            };
            let (head_seq, ops, has_more, schema) = match self.send(&message, &token).await? {
                ServerMessage::SyncBatch {
                    correlation_id: reply_id,
                    room_id,
                    head_seq,
                    ops,
                    has_more,
                    snapshot_wanted,
                    schema_version,
                    schema_id,
                } if reply_id == correlation_id && room_id == room.id => {
                    wanted |= snapshot_wanted;
                    (head_seq, ops, has_more, (schema_id, schema_version))
                }
                reply => {
                    let refused =
                        self.refused(&room, epoch, from_seq, correlation_id, reply, again);
                    refused.await?;
                    room.register_again(epoch, &mut again)?;
                    continue;
                }
            };
            // The server may have lost sequences that the replica applied.
            if head_seq < from_seq {
                room.register_again(epoch, &mut again)?;
                continue;
            }
            if !follows(from_seq, head_seq, &ops, has_more) {
                return Err(ClientError::Network(
                    "Invalid batch in reply to a sync".to_string(),
                ));
            }
            room.saw(epoch, head_seq);
            let applied = move |local: Local, room| local.apply(room, epoch, ops, schema, again);
            let head = match self.change(&room, applied).await {
                Ok(Applied::Head(head)) => head,
                Ok(Applied::Stale) => continue,
                Ok(Applied::Schema) => {
                    room.register_again(epoch, &mut again)?;
                    continue;
                }
                Err(err @ ClientError::FailedClosed(_)) => {
                    let _registration = room.registration.lock().await;
                    self.deregister(&room, &token).await;
                    return Err(err);
                }
                Err(err) => return Err(err),
            };
            if has_more || head < head_seq {
                continue;
            }

            let correlation_id = self.correlation_id();
            let message = ClientMessage::Ack {
                correlation_id,
                room_id: room.id.clone(),
                client_id: self.client_id.clone(),
                ack_seq: head,
            };
            match self.send(&message, &token).await? {
                ServerMessage::AckConfirmed {
                    correlation_id: reply_id,
                    room_id,
                    ack_seq,
                    head_seq,
                } if reply_id == correlation_id && room_id == room.id && ack_seq == head => {
                    room.saw(epoch, head_seq);
                    if head_seq >= head {
                        return Ok(wanted);
                    }
                }
                reply => {
                    self.refused(&room, epoch, head, correlation_id, reply, again)
                        .await?
                }
            }
            room.register_again(epoch, &mut again)?;
        }
    }

    /// Writes `op` to `room_id`: commits it, and returns the decision of the replica on the
    /// sequence the server assigned it (`Ok`, `ConstraintViolation` or `RowMissing`) once the
    /// replica applied it, catching up with syncs if the `CommitAck` does not reach it. A room
    /// that is not registered registers first.
    ///
    /// Until its `CommitAck`, it retries with a backoff, with the same mutation id, which the
    /// server deduplicates. It sends no attempt after the deadline of the configuration (one
    /// beyond the clock is `Config`), at which an attempt, a registration or the wait for the
    /// applier still in progress are cut. An
    /// attempt without a valid reply, or with `Unavailable`, `Timeout` or `Internal`, may have
    /// been sequenced: from then on, the write ends as `OutcomeUnknown` unless a `CommitAck`
    /// comes. `ClientNotRegistered` and `InvalidSequence` register the replica again with its
    /// cursor, and `RequestTimeout`, `RoomLocked` and `BehindCompaction` had no effect: it
    /// retries after them, and after a registration without a valid reply. After
    /// `BehindCompaction`, or `Registered` behind the log, the replica waits for a snapshot: the
    /// write catches up with heartbeats ([`heartbeat`](Self::heartbeat)) until it installs one,
    /// and sends nothing meanwhile. `SchemaViolation`,
    /// `BadRequest` and `Forbidden` are `Validation`, and any other error of the server or the
    /// deadline end it with `Network`. Closing the room, discarding its replica or failing it
    /// closed ends it with `Network`, or as `OutcomeUnknown` after an uncertain attempt or once
    /// its `CommitAck` comes, also one that comes after. An error of the client (`RoomClosed` or
    /// `FailedClosed` before it enters its room, `Storage` or `Config`) ends it with that error.
    /// Without an uncertain attempt or a `CommitAck`, the write had no effect.
    ///
    /// A `CommitAck` is checked before it is used: its correlation id, its room, its mutation
    /// id, and a catch-up that follows the cursor of the commit in order and, unless it has
    /// more, reaches the sequence. From before its first attempt until its `CommitAck`, the
    /// room retains the rejections, and the sequences applied without a report, after the
    /// head the write saw then, which its sequence is after: a sequence the replica applied
    /// before the `CommitAck` takes the decision retained, and one at or below that head is an
    /// unknown outcome. After the `CommitAck`, it waits for its sequence without a deadline: if
    /// the room is closed, its replica discarded, or failing closed before the replica applies
    /// it with its report, the outcome is unknown. A write leaves a reply that asks the client
    /// for a snapshot to the next heartbeat or sync, so that its decision does not wait for the
    /// upload.
    pub async fn write(&self, room_id: RoomId, op: Operation) -> Result<(), ClientError> {
        let room = self.room(&room_id)?;
        let mutation_id = MutationId::from(Uuid::new_v4().as_u128());
        let deadline = (Instant::now().checked_add(self.config.write_timeout))
            .ok_or_else(|| ClientError::Config("The write timeout is too long".to_string()))?;
        let mut backoff = Backoff::new(&self.config);
        // Whether an attempt may have been sequenced.
        let mut uncertain = false;
        let ends = |uncertain, err| {
            if uncertain {
                ClientError::OutcomeUnknown
            } else {
                err
            }
        };
        let expired = || ClientError::Network("No commit ack before the deadline".to_string());
        // Closing the room, discarding its replica or failing it closed ends the write.
        let ended =
            || ClientError::Network("The room closed, was discarded or failed closed".into());
        let mut in_flight = None;
        let (seq, ops, schema, epoch) = loop {
            // What the write saw is read under the applier, with the head it delivered.
            let request = tokio::time::timeout_at(deadline, async {
                let _applier = room.applier.lock().await;
                let request = self.local.clone().request(Arc::clone(&room)).await;
                if let Ok(Request {
                    cursor: Some((head, _)),
                    registered: true,
                    ..
                }) = &request
                {
                    let head = *head;
                    in_flight.get_or_insert_with(|| InFlight::new(&room, mutation_id, head));
                }
                request
            });
            let request = request.await;
            let gone = in_flight.is_some() && !room.in_flight(&mutation_id);
            if gone {
                return Err(ends(uncertain, ended()));
            }
            // Nothing is sent after the deadline.
            let Some(request) = request.ok().filter(|_| Instant::now() < deadline) else {
                return Err(ends(uncertain, expired()));
            };
            let request = request.map_err(|err| ends(uncertain, err))?;
            let ready = request.registered && request.awaits_snapshot.is_none();
            let Some((cursor, _)) = request.cursor.filter(|_| ready) else {
                // It registers, or, behind the log, catches up with a heartbeat, which downloads
                // the snapshot it announces.
                let readied = tokio::time::timeout_at(deadline, async {
                    if request.registered {
                        return self.heartbeat(room.id.clone()).await;
                    }
                    let _registration = room.registration.lock().await;
                    self.register(&room).await
                });
                match readied.await {
                    Ok(Ok(())) if !request.registered => continue,
                    // Without a valid reply, or none by the deadline, it retries until then.
                    Ok(Ok(()) | Err(ClientError::Network(_))) | Err(_) => {}
                    Ok(Err(_)) if !room.in_flight(&mutation_id) && in_flight.is_some() => {
                        return Err(ends(uncertain, ended()))
                    }
                    Ok(Err(err)) => return Err(ends(uncertain, err)),
                }
                tokio::time::sleep_until((Instant::now() + backoff.next()).min(deadline)).await;
                continue;
            };
            let (epoch, correlation_id) = (request.epoch, self.correlation_id());
            let message = ClientMessage::Commit {
                correlation_id,
                room_id: room.id.clone(),
                client_id: self.client_id.clone(),
                mutation_id,
                last_ack_seq: cursor,
                op: op.clone(),
            };
            // An attempt without a reply by the deadline is cut.
            let reply = tokio::time::timeout_at(deadline, self.send(&message, &request.token));
            match reply.await.ok().and_then(Result::ok) {
                Some(ServerMessage::CommitAck {
                    correlation_id: reply_id,
                    room_id,
                    mutation_id: acked,
                    assigned_seq,
                    catchup_ops,
                    has_more,
                    schema_version,
                    schema_id,
                    ..
                }) if reply_id == correlation_id
                    && room_id == room.id
                    && acked == mutation_id
                    && last_after(cursor, &catchup_ops)
                        .is_some_and(|last| has_more || assigned_seq <= last) =>
                {
                    let last = catchup_ops.last().map_or(assigned_seq, |op| op.seq);
                    room.saw(epoch, assigned_seq.max(last));
                    break (
                        assigned_seq,
                        catchup_ops,
                        (schema_id, schema_version),
                        epoch,
                    );
                }
                Some(ServerMessage::Error {
                    correlation_id: reply_id,
                    room_id,
                    code,
                    message,
                }) if reply_id.is_none_or(|reply_id| reply_id == correlation_id)
                    && room_id.as_ref().is_none_or(|room_id| *room_id == room.id) =>
                {
                    let refused = format!("The server refused the write: {code:?}: {message}");
                    match code {
                        ErrorCode::SchemaViolation
                        | ErrorCode::BadRequest
                        | ErrorCode::Forbidden => {
                            return Err(ends(uncertain, ClientError::Validation(refused)))
                        }
                        ErrorCode::ClientNotRegistered | ErrorCode::InvalidSequence => {
                            room.update(epoch, |open| open.registered = false)
                        }
                        ErrorCode::RequestTimeout | ErrorCode::RoomLocked => {}
                        ErrorCode::BehindCompaction => {
                            let behind =
                                move |local: Local, room| local.behind(room, epoch, cursor, false);
                            let behind =
                                tokio::time::timeout_at(deadline, self.change(&room, behind));
                            if let Ok(Err(err)) = behind.await {
                                return Err(ends(uncertain, err));
                            }
                        }
                        ErrorCode::Unavailable | ErrorCode::Timeout | ErrorCode::Internal => {
                            uncertain = true
                        }
                        _ => return Err(ends(uncertain, ClientError::Network(refused))),
                    }
                }
                // No reply, or one that does not answer the commit: it may have been sequenced.
                _ => uncertain = true,
            }
            tokio::time::sleep_until((Instant::now() + backoff.next()).min(deadline)).await;
        };

        let acked =
            move |local: Local, room| local.acked(room, epoch, mutation_id, seq, ops, schema);
        // A catch-up that announces another schema is not applied, and the sync registers again.
        if let Ok(Applied::Schema) = self.change(&room, acked).await {
            room.update(epoch, |open| open.registered = false);
        }
        let mut backoff = Backoff::new(&self.config);
        loop {
            if let Some(decision) = room.decision(&mutation_id) {
                return decision;
            }
            // Its errors are retried: the room ends the write if it closes or fails closed. Behind
            // the log, the replica learns of a snapshot with a heartbeat. The sync confirms no
            // snapshot wanted: the decision would wait for its upload.
            let _ = match room.awaits_snapshot() {
                true => self.heartbeat(room.id.clone()).await,
                false => self.synced(room.id.clone()).await.map(|_| ()),
            };
            if let Some(decision) = room.decision(&mutation_id) {
                return decision;
            }
            tokio::time::sleep(backoff.next()).await;
        }
    }

    /// Sends a heartbeat for `room_id`, which renews the lease of the client in its room, and
    /// reacts to its `HeartbeatAck`. A room that is not registered registers first, and one that
    /// fails closed sends nothing (`FailedClosed`).
    ///
    /// A reply is checked before it is used: a `HeartbeatAck` with the correlation id and the
    /// room of the request. Any other reply, or none, is `Network`. `ClientNotRegistered`, a head
    /// below that of the replica when the heartbeat was sent, and an ack that announces another
    /// schema, or a later version of its own, register the replica again with its cursor, and
    /// the heartbeat is sent again; a second time in a heartbeat is `Network`.
    ///
    /// A replica that waits for a snapshot takes whether the ack announces a usable one, and
    /// downloads the one it announces by chunks of 4 MiB, each checked against the first. One
    /// larger than `max_snapshot_bytes` is `Config`, also the next time, from a first chunk of
    /// 64 KiB. `SnapshotSuperseded` downloads it again from its first chunk, once;
    /// `RoomNotFound` leaves the replica waiting for a heartbeat that announces one; and
    /// `RoomReset` registers it again with its cursor. The snapshot is installed with the schema
    /// of the replica once its bytes match its hash, and the next sync goes on from it. One that
    /// does not install with that schema registers the replica again, for `Registered` to give
    /// it the schema of the room, and is dropped if it does not install with it either.
    ///
    /// An ack that asks the client for a snapshot (`snapshot_wanted`) designates it to upload
    /// one, once per heartbeat, and the heartbeat is sent again from the state that the upload
    /// left. The snapshot is created at the head of the replica, under its applier, unless the
    /// replica fails closed or waits for a snapshot, and uploaded in `n = ⌈T / 4 MiB⌉` chunks of
    /// `⌈T / n⌉` bytes but the last, as the relay takes them, with the incarnation of the
    /// replica; no chunk is sent once the replica is discarded. `RoomReset` registers the
    /// replica again with its cursor, and `SnapshotSuperseded`, or any other reply, stops the
    /// upload with `Network`; a replica whose upload failed uploads no other for a minute. A
    /// chunk of a download or an upload without a reply, or with `Unavailable` or `Timeout`, is
    /// sent again after a backoff, a few times.
    pub async fn heartbeat(&self, room_id: RoomId) -> Result<(), ClientError> {
        let room = self.room(&room_id)?;
        let (mut again, mut uploaded) = (false, false);
        loop {
            let request = self.change(&room, Local::request).await?;
            let Some((head, _)) = request.cursor.filter(|_| request.registered) else {
                let _registration = room.registration.lock().await;
                self.register(&room).await?;
                continue;
            };
            let (epoch, correlation_id) = (request.epoch, self.correlation_id());
            let message = ClientMessage::Heartbeat {
                correlation_id,
                room_id: room.id.clone(),
                client_id: self.client_id.clone(),
            };
            let reply = self.send(&message, &request.token).await?;
            let (head_seq, schema, snapshot, wanted) = match reply {
                ServerMessage::HeartbeatAck {
                    correlation_id: reply_id,
                    room_id,
                    current_head_seq,
                    snapshot_wanted,
                    active_snapshot_seq,
                    schema_version,
                    schema_id,
                } if reply_id == correlation_id && room_id == room.id => (
                    current_head_seq,
                    (schema_id, schema_version),
                    active_snapshot_seq,
                    snapshot_wanted,
                ),
                reply => {
                    self.refused(&room, epoch, head, correlation_id, reply, again)
                        .await?;
                    room.register_again(epoch, &mut again)?;
                    continue;
                }
            };
            // The server may have lost sequences that the replica applied.
            if head_seq < head {
                room.register_again(epoch, &mut again)?;
                continue;
            }
            room.saw(epoch, head_seq);
            if room.changes(&schema) {
                room.register_again(epoch, &mut again)?;
                continue;
            }
            if wanted && !std::mem::replace(&mut uploaded, true) {
                if let Err(err) = self.upload(&room, &request, &mut again).await {
                    room.update(epoch, |open| open.upload_failed = Some(Instant::now()));
                    return Err(err);
                }
                continue;
            }
            if !room.announced(epoch, snapshot) {
                return Ok(());
            }
            // The next heartbeat takes the replica as the download left it.
            self.snapshot(&room, &mut again).await?;
        }
    }

    /// Downloads the active snapshot of `room`, if its replica waits for one announced, and
    /// installs it ([`Local::install`]). One download at a time: one that waits for another
    /// finds the snapshot installed. `RoomReset` to a chunk leaves the replica to register again,
    /// and a snapshot that does not install with the schema of the replica registers it again,
    /// once per call of the caller (`again`), before installing it once more.
    async fn snapshot(&self, room: &Arc<Room>, again: &mut bool) -> Result<(), ClientError> {
        let _transfer = room.transfer.lock().await;
        let request = self.change(room, Local::request).await?;
        let announced = request.awaits_snapshot == Some(true);
        let Some((_, incarnation)) = request.cursor.filter(|_| announced) else {
            return Ok(());
        };
        let epoch = request.epoch;
        let Some((seq, snapshot)) = self.download(room, &request, incarnation).await? else {
            return room.register_again(epoch, again);
        };
        // Whether the replica registered again for this snapshot.
        let mut signaled = false;
        loop {
            let snapshot = snapshot.clone();
            let install =
                move |local: Local, room| local.install(room, epoch, seq, snapshot, signaled);
            match self.change(room, install).await? {
                Applied::Schema => {
                    room.register_again(epoch, again)?;
                    let _registration = room.registration.lock().await;
                    self.register(room).await?;
                    signaled = true;
                }
                Applied::Head(_) | Applied::Stale => return Ok(()),
            }
        }
    }

    /// Downloads the active snapshot of `room` with the incarnation `incarnation`, for the replica
    /// of `request`, by chunks of 4 MiB: the first without an anchor, and the others anchored to
    /// its hash. Each reply is checked before it is used: a chunk with the correlation id, the
    /// room and the index of the request, with the sequence, the number of chunks, the size and
    /// the hash of the first, a number of chunks that its size takes, and the length its index
    /// takes. Returns its sequence and its bytes once they match its hash, or `None` on
    /// `RoomReset`. One larger than `max_snapshot_bytes` is `Config`, once its first chunk is
    /// checked; the next download asks for a first chunk of [`PROBE_BYTES`], which is `Config`
    /// again if the snapshot is still larger than the limit, and downloads one that fits from
    /// its first chunk again, by chunks of 4 MiB. `SnapshotSuperseded` downloads it again from
    /// its first chunk, once per download, and `RoomNotFound` leaves the replica waiting for a
    /// heartbeat that announces one. Any other reply, or none, is `Network`
    /// ([`send_chunk`](Self::send_chunk) sends a chunk again a few times).
    async fn download(
        &self,
        room: &Room,
        request: &Request,
        incarnation: u128,
    ) -> Result<Option<(SequenceNumber, Bytes)>, ClientError> {
        let probe = matches!(&room.state().replica, Replica::Open(open) if open.oversized);
        let mut size = if probe { PROBE_BYTES } else { CHUNK_BYTES };
        let (mut first, mut bytes, mut index) = (None, Vec::new(), 0);
        let mut superseded = false;
        let (seq, hash) = loop {
            if let Some((seq, total_chunks, _, hash)) = first {
                if index >= total_chunks {
                    break (seq, hash);
                }
            }
            let correlation_id = self.correlation_id();
            let message = ClientMessage::RequestSnapshotChunk {
                correlation_id,
                room_id: room.id.clone(),
                chunk_index: index,
                chunk_size: size,
                snapshot_hash: first.map(|(.., hash)| hash),
                room_incarnation: incarnation,
            };
            match self.send_chunk(&message, &request.token, || true).await? {
                Some(ServerMessage::SnapshotChunk {
                    correlation_id: reply_id,
                    room_id,
                    snapshot_head_seq,
                    chunk_index,
                    total_chunks,
                    total_bytes,
                    snapshot_hash,
                    data,
                }) if reply_id == correlation_id && room_id == room.id && chunk_index == index => {
                    let chunk = u64::from(size);
                    let snapshot = (snapshot_head_seq, total_chunks, total_bytes, snapshot_hash);
                    let len = total_bytes.saturating_sub(u64::from(index) * chunk);
                    if *first.get_or_insert(snapshot) != snapshot
                        || u64::from(total_chunks) != total_bytes.div_ceil(chunk)
                        || u64::try_from(data.len()) != Ok(len.min(chunk))
                    {
                        return Err(ClientError::Network(
                            "Invalid chunk of a snapshot".to_string(),
                        ));
                    }
                    let max = self.config.max_snapshot_bytes;
                    room.update(request.epoch, |open| open.oversized = total_bytes > max);
                    if total_bytes > max {
                        return Err(ClientError::Config(format!(
                            "The snapshot of {total_bytes} bytes is larger than the limit of {max}"
                        )));
                    }
                    // One that fits after a probe is downloaded from its start.
                    if size == PROBE_BYTES {
                        (size, first, bytes, index) = (CHUNK_BYTES, None, Vec::new(), 0);
                        continue;
                    }
                    // The snapshot fits within the limit, and is held once.
                    if index == 0 {
                        let len = usize::try_from(total_bytes).ok();
                        if len.is_none_or(|len| bytes.try_reserve_exact(len).is_err()) {
                            return Err(ClientError::Config(format!(
                                "A snapshot of {total_bytes} bytes does not fit in memory"
                            )));
                        }
                    }
                    bytes.extend_from_slice(&data);
                    index += 1;
                }
                Some(ServerMessage::Error {
                    correlation_id: reply_id,
                    room_id,
                    code,
                    message,
                }) if reply_id.is_none_or(|reply_id| reply_id == correlation_id)
                    && room_id.as_ref().is_none_or(|room_id| *room_id == room.id) =>
                {
                    match code {
                        ErrorCode::SnapshotSuperseded
                            if !std::mem::replace(&mut superseded, true) =>
                        {
                            (first, bytes, index) = (None, Vec::new(), 0);
                        }
                        ErrorCode::RoomReset => return Ok(None),
                        code => {
                            if code == ErrorCode::RoomNotFound {
                                room.announced(request.epoch, None);
                            }
                            return Err(ClientError::Network(format!(
                                "The server refused a chunk of the snapshot: {code:?}: {message}"
                            )));
                        }
                    }
                }
                _ => {
                    return Err(ClientError::Network(
                        "Invalid reply to a request of a chunk".to_string(),
                    ))
                }
            }
        };
        // Hashing a large snapshot would stall the runtime.
        let bytes = Bytes::from(bytes);
        let hashed = bytes.clone();
        let hashed =
            tokio::task::spawn_blocking(move || ServerMessage::compute_snapshot_hash(&hashed));
        if hashed.await.ok() != Some(hash) {
            return Err(ClientError::Network(
                "The snapshot does not match its hash".to_string(),
            ));
        }
        Ok(Some((seq, bytes)))
    }

    /// Uploads a snapshot of the replica of `request` in `room`, created at its head under the
    /// applier, unless the replica may not upload one ([`Room::uploads`]), in `n = ⌈T / 4 MiB⌉`
    /// chunks of `⌈T / n⌉` bytes but the last, which holds the rest, as the relay takes them,
    /// each with the incarnation of the replica. One upload at a time, also with the downloads.
    /// No chunk is sent, or sent again, once the replica may not upload: after a discard, the
    /// incarnation may be the same. Each reply is checked before it is used: an ack with the
    /// correlation id, the room, the index and the number of chunks of the chunk, and the upload
    /// ends with the one that staged it. `RoomReset` leaves the replica to register again with
    /// its cursor, once per call of the caller (`again`). Any other reply, or none, is `Network`,
    /// and the upload stops.
    async fn upload(
        &self,
        room: &Arc<Room>,
        request: &Request,
        again: &mut bool,
    ) -> Result<(), ClientError> {
        let _transfer = room.transfer.lock().await;
        let epoch = request.epoch;
        let created = move |local: Local, room| local.create_snapshot(room, epoch);
        let Some((seq, incarnation, snapshot)) = self.change(room, created).await? else {
            return Ok(());
        };
        // Hashing a large snapshot would stall the runtime.
        let snapshot = Bytes::from(snapshot);
        let hashed = snapshot.clone();
        let hashed =
            tokio::task::spawn_blocking(move || ServerMessage::compute_snapshot_hash(&hashed));
        let hash = (hashed.await)
            .map_err(|err| ClientError::Storage(format!("Hashing the snapshot failed: {err}")))?;
        let total_bytes = snapshot.len() as u64;
        let chunks = snapshot.len().div_ceil(CHUNK_BYTES as usize).max(1);
        let total_chunks = u32::try_from(chunks).unwrap_or(u32::MAX);
        for (chunk_index, data) in
            (0..).zip(snapshot.chunks(snapshot.len().div_ceil(chunks).max(1)))
        {
            let correlation_id = self.correlation_id();
            let message = ClientMessage::UploadSnapshotChunk {
                correlation_id,
                room_id: room.id.clone(),
                snapshot_head_seq: seq,
                chunk_index,
                total_chunks,
                total_bytes,
                snapshot_hash: hash,
                data: snapshot.slice_ref(data),
                room_incarnation: incarnation,
            };
            let sends = || room.uploads(epoch).is_some();
            match self.send_chunk(&message, &request.token, sends).await? {
                None => return Ok(()),
                Some(ServerMessage::SnapshotUploadChunkAck {
                    correlation_id: reply_id,
                    room_id,
                    chunk_index: index,
                    total_chunks: chunks,
                    staged,
                }) if reply_id == correlation_id
                    && room_id == room.id
                    && index == chunk_index
                    && chunks == total_chunks =>
                {
                    if staged {
                        return Ok(());
                    }
                }
                Some(ServerMessage::Error {
                    correlation_id: reply_id,
                    room_id,
                    code,
                    message,
                }) if reply_id.is_none_or(|reply_id| reply_id == correlation_id)
                    && room_id.as_ref().is_none_or(|room_id| *room_id == room.id) =>
                {
                    if code == ErrorCode::RoomReset {
                        return room.register_again(epoch, again);
                    }
                    return Err(ClientError::Network(format!(
                        "The server refused a chunk of the upload: {code:?}: {message}"
                    )));
                }
                _ => {
                    return Err(ClientError::Network(
                        "Invalid reply to a chunk of an upload".to_string(),
                    ))
                }
            }
        }
        Err(ClientError::Network(
            "The upload of the snapshot did not complete".to_string(),
        ))
    }

    /// Sends `message`, a chunk of a snapshot, with `token` as [`send`](Self::send) does, if
    /// `sends` says it may, and again after a backoff, up to [`CHUNK_RETRIES`] times, while it
    /// gets a transient failure (no reply, `Unavailable` or `Timeout`) and `sends` still says it
    /// may. Returns the last reply, or `None` if `sends` said no.
    async fn send_chunk(
        &self,
        message: &ClientMessage,
        token: &str,
        sends: impl Fn() -> bool,
    ) -> Result<Option<ServerMessage>, ClientError> {
        let (mut backoff, mut retries) = (Backoff::new(&self.config), 0);
        loop {
            if !sends() {
                return Ok(None);
            }
            let reply = self.send(message, token).await;
            let transient = matches!(
                &reply,
                Err(_)
                    | Ok(ServerMessage::Error {
                        code: ErrorCode::Unavailable | ErrorCode::Timeout,
                        ..
                    })
            );
            if !transient || retries == CHUNK_RETRIES {
                return reply.map(Some);
            }
            retries += 1;
            tokio::time::sleep(backoff.next()).await;
        }
    }

    /// The room `room_id` of the client; `RoomClosed` if the client never opened it.
    fn room(&self, room_id: &RoomId) -> Result<Arc<Room>, ClientError> {
        let rooms = self.rooms.lock().unwrap_or_else(PoisonError::into_inner);
        let room = rooms.get(room_id).map(Arc::clone);
        room.ok_or_else(|| ClientError::RoomClosed(room_id.clone()))
    }

    /// The diagnosis of `room_id`; `RoomClosed` if its replica is not open in the client.
    pub async fn status(&self, room_id: RoomId) -> Result<RoomStatus, ClientError> {
        let room = self.room(&room_id)?;
        let status = {
            let state = room.state();
            let Replica::Open(open) = &state.replica else {
                return Err(ClientError::RoomClosed(room_id));
            };
            RoomStatus {
                head_seq: SequenceNumber::new(0),
                epoch: state.epoch,
                registered: open.registered,
                failed_closed: open.failed_closed,
                writes_in_flight: state.writes.entries.len(),
                retained: state.writes.retained.len(),
            }
        };
        Ok(RoomStatus {
            head_seq: self.head(&room_id).await?,
            ..status
        })
    }

    /// Closes `room_id` once the changes of its replica in progress ended, closing its storage
    /// room, which waits for the operations of the storage in progress. An error closing it is
    /// reported, and the room leaves the client all the same.
    pub async fn close_room(&self, room_id: RoomId) -> Result<(), ClientError> {
        let room = self.room(&room_id)?;
        self.change(&room, Local::close).await
    }

    /// Shuts the client down: closes every room as [`close_room`](Self::close_room) does, so that
    /// the runtime can be shut down without losing anything, and releases the metadata for
    /// another client. The first error closing a room is reported, once every room is closed. No
    /// room opens afterwards.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        self.local.shut_down.store(true, Ordering::SeqCst);
        let rooms: Vec<_> = {
            let rooms = self.rooms.lock().unwrap_or_else(PoisonError::into_inner);
            rooms.values().map(Arc::clone).collect()
        };
        let mut closed = Ok(());
        for room in rooms {
            match self.change(&room, Local::close).await {
                Err(ClientError::RoomClosed(_)) => {}
                result => closed = closed.and(result),
            }
        }
        self.local.metadata.close().await;
        closed
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

/// The changes of a replica, which run under its applier, in a task of their own.
impl Local {
    /// The request that the replica presents; `RoomClosed` if it is closed.
    async fn request(self, room: Arc<Room>) -> Result<Request, ClientError> {
        let (mut request, incarnation) = {
            let state = room.state();
            let open = match &state.replica {
                Replica::Closed => return Err(ClientError::RoomClosed(room.id.clone())),
                Replica::Empty => None,
                Replica::Open(open) => Some(open),
            };
            let request = Request {
                epoch: state.epoch,
                token: state.token.clone(),
                cursor: None,
                registered: open.is_some_and(|open| open.registered),
                awaits_snapshot: open.and_then(|open| open.awaits_snapshot),
                failed_closed: open.is_some_and(|open| open.failed_closed),
            };
            (request, open.map(|open| open.metadata.room_incarnation))
        };
        if let Some(incarnation) = incarnation {
            request.cursor = Some((self.storage.get_head_seq(&room.id).await?, incarnation));
        }
        Ok(request)
    }

    /// Opens the replica of a room out of the client from its metadata. Without them, or with
    /// damaged data, discards it; any other error keeps it, and the room stays out of the
    /// client.
    async fn open(self, room: Arc<Room>) -> Result<(), ClientError> {
        if !matches!(room.state().replica, Replica::Closed) {
            return Ok(());
        }
        if self.shut_down.load(Ordering::SeqCst) {
            return Err(ClientError::RoomClosed(room.id.clone()));
        }
        let Some(metadata) = self.metadata.room(&room.id).await? else {
            return self.discard(room).await;
        };
        match self
            .storage
            .open_room(&room.id, metadata.schema.clone())
            .await
        {
            Ok(()) => {
                room.state().replica = Replica::Open(OpenReplica::new(metadata, false));
                Ok(())
            }
            Err(StorageError::SnapshotCorruption(_) | StorageError::WalCorruption(_)) => {
                self.discard(room).await
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Takes `Registered` for the replica of `epoch` that presented `cursor`, with the metadata
    /// it gives: an empty replica saves them before its data, and opens its storage room with
    /// their schema; an open one is registered, unless its head went back below the cursor (a
    /// memory room that opened again at 0), and then it registers again with its head. A
    /// replica of another epoch is left as it is, and so is one that fails closed. A replica
    /// behind the log, which the room retains from `tail_seq`, waits for a snapshot, and takes the
    /// usable one that `Registered` announced, at `snapshot`; one inside it no longer waits.
    ///
    /// An open replica takes another schema, or a later version of its own, only if it evolves
    /// from its own: it saves the metadata before its storage room reloads the schema, and an
    /// error at either step leaves the room out of the client, to open next with the metadata
    /// saved. If the schema does not evolve, the replica fails closed (`FailedClosed`).
    async fn registered(
        self,
        room: Arc<Room>,
        epoch: u64,
        cursor: Option<SequenceNumber>,
        metadata: RoomMetadata,
        (tail_seq, snapshot): (SequenceNumber, Option<SequenceNumber>),
    ) -> Result<(), ClientError> {
        let waits =
            |head: SequenceNumber| (head.get() < tail_seq.get().saturating_sub(1)).then_some(false);
        let local = match &mut *room.state() {
            State {
                replica: Replica::Closed,
                ..
            } => return Err(ClientError::RoomClosed(room.id.clone())),
            state if state.epoch != epoch => return Ok(()),
            State {
                replica: Replica::Open(open),
                ..
            } if open.failed_closed => return Ok(()),
            State {
                replica: Replica::Open(open),
                ..
            } => Some(open.metadata.clone()),
            // Out of the client until it opens.
            State { replica, .. } => {
                *replica = Replica::Closed;
                None
            }
        };
        let Some(local) = local else {
            self.metadata.save_room(&room.id, &metadata).await?;
            self.storage
                .open_room(&room.id, metadata.schema.clone())
                .await?;
            let mut open = OpenReplica::new(metadata, true);
            open.awaits_snapshot = waits(SequenceNumber::new(0));
            room.state().replica = Replica::Open(open);
            room.announced(epoch, snapshot);
            return Ok(());
        };
        let head = self.storage.get_head_seq(&room.id).await?;
        if cursor.is_some_and(|cursor| cursor > head) {
            return Ok(());
        }
        if changes(&local, &metadata.schema_id, metadata.schema.version()) {
            if !metadata.schema.evolves_from(&local.schema) {
                room.update(epoch, OpenReplica::fail_closed);
                return Err(ClientError::FailedClosed(room.id.clone()));
            }
            let switched = async {
                self.metadata.save_room(&room.id, &metadata).await?;
                let schema = metadata.schema.clone();
                Ok::<_, ClientError>(self.storage.reload_schema(&room.id, schema).await?)
            };
            if let Err(err) = switched.await {
                self.close(room).await?;
                return Err(err);
            }
            room.update(epoch, |open| open.metadata = metadata);
        }
        room.update(epoch, |open| {
            open.registered = true;
            open.awaits_snapshot = waits(head);
        });
        room.announced(epoch, snapshot);
        Ok(())
    }

    /// Applies `ops`, a batch in reply to a request of `epoch` that announces the version of the
    /// schema `schema`, after dropping those the replica holds, and returns its head after them.
    /// A batch for a replica that is closed, fails closed or is no longer that of `epoch` is not
    /// applied (`Stale`), and neither is one that does not start right after the head. Nor is
    /// one that announces a change of the schema (`Schema`), or one with an operation that does
    /// not validate against the schema of the replica: `Schema`, and once the sync registered
    /// again for a signal (`again`), which gave the replica the schema of the room, it fails the
    /// replica closed (`FailedClosed`). An error after which the storage room may have changed
    /// (`Io`, `RoomFailed`) is returned once the storage room is closed and opened again from
    /// its files, as an open does.
    async fn apply(
        self,
        room: Arc<Room>,
        epoch: u64,
        ops: Vec<SequencedOperation>,
        (schema_id, version): (SchemaId, u64),
        again: bool,
    ) -> Result<Applied, ClientError> {
        {
            let state = room.state();
            let Replica::Open(open) = &state.replica else {
                return Ok(Applied::Stale);
            };
            if state.epoch != epoch || open.failed_closed {
                return Ok(Applied::Stale);
            }
            if changes(&open.metadata, &schema_id, version) {
                return Ok(Applied::Schema);
            }
        }
        let head = self.storage.get_head_seq(&room.id).await?;
        let ops: Vec<_> = ops.into_iter().skip_while(|op| op.seq <= head).collect();
        if ops
            .first()
            .is_none_or(|op| op.seq.get().checked_sub(1) != Some(head.get()))
        {
            return Ok(Applied::Head(head));
        }
        match self.storage.apply_batch(&room.id, ops).await {
            Ok(report) => {
                let rejected = report.rejected().iter();
                let rejected = rejected.map(|(seq, reason)| (*seq, *seq, reason.clone().into()));
                room.state().writes.applied(report.head_seq(), rejected);
                Ok(Applied::Head(report.head_seq()))
            }
            Err(StorageError::SchemaValidation(_)) if again => {
                room.update(epoch, OpenReplica::fail_closed);
                Err(ClientError::FailedClosed(room.id.clone()))
            }
            Err(StorageError::SchemaValidation(_)) => Ok(Applied::Schema),
            Err(err @ (StorageError::Io(_) | StorageError::RoomFailed { .. })) => {
                self.reopen(room, head).await?;
                Err(err.into())
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Closes the storage room after an error after which it may have changed, at `head` before
    /// the change, and opens it again from its files, as an open does. The sequences after `head`
    /// up to the one it opens at have no report: all of them, if that one cannot be read.
    async fn reopen(self, room: Arc<Room>, head: SequenceNumber) -> Result<(), ClientError> {
        self.clone().close(Arc::clone(&room)).await?;
        self.clone().open(Arc::clone(&room)).await?;
        let reopened = self.storage.get_head_seq(&room.id).await;
        let reopened = reopened.unwrap_or(SequenceNumber::new(u64::MAX));
        if reopened > head {
            let unknown = (head.next(), reopened, ClientError::OutcomeUnknown);
            room.state().writes.applied(reopened, [unknown]);
        }
        Ok(())
    }

    /// Installs `snapshot`, the snapshot at `seq` downloaded for the replica of `epoch`, with the
    /// schema of its metadata, unless the replica is closed, fails closed or is no longer that of
    /// `epoch` (`Stale`). Once installed, the replica no longer waits for a snapshot, and the
    /// sequences after its head up to `seq` have no report. One behind the replica changes
    /// nothing, and the replica no longer waits either. One that does not install with the schema
    /// of the replica (`SnapshotCorruption`, `SnapshotConstraints`) is `Schema`, for the replica
    /// to register again; once it did for it (`signaled`), it is dropped (`Network`), as is one at
    /// another head than its chunks announce, and the replica waits for a heartbeat that
    /// announces another. An error after which the storage room may have changed reopens it, as
    /// [`apply`](Self::apply) does.
    async fn install(
        self,
        room: Arc<Room>,
        epoch: u64,
        seq: SequenceNumber,
        snapshot: Bytes,
        signaled: bool,
    ) -> Result<Applied, ClientError> {
        let schema = match &*room.state() {
            State {
                replica: Replica::Open(open),
                epoch: current,
                ..
            } if *current == epoch && !open.failed_closed => open.metadata.schema.clone(),
            _ => return Ok(Applied::Stale),
        };
        let head = self.storage.get_head_seq(&room.id).await?;
        let installed = self.storage.apply_snapshot(&room.id, schema, snapshot, seq);
        let installed = match installed.await {
            Ok(installed) => installed,
            Err(StorageError::SnapshotBehind { .. }) => head,
            Err(err) => {
                return match err {
                    StorageError::SnapshotCorruption(_)
                    | StorageError::SnapshotConstraints { .. }
                        if !signaled =>
                    {
                        Ok(Applied::Schema)
                    }
                    StorageError::SnapshotCorruption(_)
                    | StorageError::SnapshotConstraints { .. }
                    | StorageError::SnapshotHeadMismatch { .. } => {
                        room.update(epoch, |open| open.dropped = Some(seq));
                        room.announced(epoch, None);
                        let dropped = format!("The snapshot does not install: {err}");
                        Err(ClientError::Network(dropped))
                    }
                    StorageError::Io(_) | StorageError::RoomFailed { .. } => {
                        self.reopen(room, head).await?;
                        Err(err.into())
                    }
                    err => Err(err.into()),
                }
            }
        };
        room.update(epoch, |open| open.awaits_snapshot = None);
        if installed > head {
            let unknown = (head.next(), installed, ClientError::OutcomeUnknown);
            room.state().writes.applied(installed, [unknown]);
        }
        Ok(Applied::Head(installed))
    }

    /// Creates a snapshot of the replica of `epoch` to upload it, if it may ([`Room::uploads`]),
    /// with its head, which no change moves under the applier, and its incarnation.
    async fn create_snapshot(
        self,
        room: Arc<Room>,
        epoch: u64,
    ) -> Result<Option<(SequenceNumber, u128, Vec<u8>)>, ClientError> {
        let Some(incarnation) = room.uploads(epoch) else {
            return Ok(None);
        };
        let head = self.storage.get_head_seq(&room.id).await?;
        let snapshot = self.storage.create_snapshot(&room.id).await?;
        Ok(Some((head, incarnation, snapshot)))
    }

    /// Takes the `CommitAck` of the write `mutation_id` with its sequence `seq`, and applies its
    /// catch-up `ops`, in reply to a request of `epoch` and announcing `schema`, as
    /// [`apply`](Self::apply) does, without failing closed.
    async fn acked(
        self,
        room: Arc<Room>,
        epoch: u64,
        mutation_id: MutationId,
        seq: SequenceNumber,
        ops: Vec<SequencedOperation>,
        schema: (SchemaId, u64),
    ) -> Result<Applied, ClientError> {
        let head = self.storage.get_head_seq(&room.id).await.ok();
        room.state().writes.acked(&mutation_id, seq, head);
        self.apply(room, epoch, ops, schema, false).await
    }

    /// Takes `BehindCompaction` to a request of the replica of `epoch` at `cursor`, if it is still
    /// at it: one that moved past it, or that is no longer of `epoch`, got a delayed reply. The
    /// replica registers again (one that fails closed does not: `register` refuses), and
    /// `Registered` tells whether it waits for a snapshot; the registration leaves the server its
    /// cursor, which may have been ahead (INV-K5), so that the server asks for one. Once a
    /// registration of the call (`again`) found it inside the log, it comes from a gap of the log
    /// (INV-Y2): the replica stays registered and waits for the snapshot above it, which the
    /// server asks for. Returns whether the replica registers again.
    async fn behind(
        self,
        room: Arc<Room>,
        epoch: u64,
        cursor: SequenceNumber,
        again: bool,
    ) -> Result<bool, ClientError> {
        let at_cursor = room.is_open() && self.storage.get_head_seq(&room.id).await? == cursor;
        let mut register = false;
        room.update(epoch, |open| match at_cursor {
            true if again => open.awaits_snapshot = Some(false),
            true => (open.registered, register) = (false, true),
            false => {}
        });
        Ok(register)
    }

    /// Discards the replica of `epoch`. One closed meanwhile is left as it is (`RoomClosed`), and
    /// so is one of another epoch.
    async fn discard_open(self, room: Arc<Room>, epoch: u64) -> Result<(), ClientError> {
        {
            let state = room.state();
            if matches!(state.replica, Replica::Closed) {
                return Err(ClientError::RoomClosed(room.id.clone()));
            }
            if state.epoch != epoch {
                return Ok(());
            }
        }
        self.discard(room).await
    }

    /// Discards the replica: removes its metadata, closes its storage room and
    /// deletes its data, and starts a new epoch, empty until `Registered`. The metadata go
    /// first, so an error or a crash at any step leaves a room without metadata, which its next
    /// open deletes again; an error leaves the room out of the client.
    async fn discard(self, room: Arc<Room>) -> Result<(), ClientError> {
        let replica = std::mem::take(&mut room.state().replica);
        let removed = self.metadata.remove_room(&room.id).await;
        if let Replica::Open(_) = replica {
            self.storage.close_room(&room.id).await?;
        }
        removed?;
        self.storage.delete_room(&room.id).await?;
        let mut state = room.state();
        state.replica = Replica::Empty;
        state.epoch += 1;
        Ok(())
    }

    /// Takes the room out of the client, closing its storage room if open. A failed close is
    /// reported, and the room is out all the same.
    async fn close(self, room: Arc<Room>) -> Result<(), ClientError> {
        let replica = std::mem::take(&mut room.state().replica);
        match replica {
            Replica::Open(_) => Ok(self.storage.close_room(&room.id).await?),
            Replica::Empty => Ok(()),
            Replica::Closed => Err(ClientError::RoomClosed(room.id.clone())),
        }
    }
}
