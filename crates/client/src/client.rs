use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use zemdb_core::{
    decode_message, ClientId, ClientMessage, CorrelationId, ErrorCode, RoomId, SchemaId,
    SequenceNumber, SequencedOperation, ServerMessage,
};
use zemdb_storage::{StorageEngine, StorageError};

use crate::{ClientConfig, ClientError, MetadataStore, RoomMetadata, TransportClient};

/// The client of a device: one replica per room it opens.
pub struct Client {
    client_id: ClientId,
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
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    replica: Replica,
    /// How many times the client discarded the replica.
    epoch: u64,
    /// The token of the last `open_room`.
    token: String,
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
    /// Behind the log of the server, it waits for a snapshot.
    awaits_snapshot: bool,
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
            awaits_snapshot: false,
            failed_closed: false,
        }
    }

    fn fail_closed(&mut self) {
        self.failed_closed = true;
        self.registered = false;
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
    awaits_snapshot: bool,
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

/// Whether `ops`, of a batch with `head_seq` and `has_more` in reply to a sync after `from_seq`,
/// are the operations after it in order, up to the head at most and, unless there are more,
/// reaching it.
fn follows(
    from_seq: SequenceNumber,
    head_seq: SequenceNumber,
    ops: &[SequencedOperation],
    has_more: bool,
) -> bool {
    let in_order = (ops.iter().zip(1..))
        .all(|(op, index)| op.seq.get().checked_sub(index) == Some(from_seq.get()));
    let last = ops.last().map_or(from_seq, |op| op.seq);
    // A batch with more brings some.
    in_order
        && last <= head_seq
        && if has_more {
            !ops.is_empty()
        } else {
            last == head_seq
        }
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
    /// An invalid person is `Config`, before `metadata` is touched; an error of `metadata`,
    /// `Storage`.
    pub async fn open(
        config: ClientConfig,
        storage: impl StorageEngine + 'static,
        metadata: impl MetadataStore + 'static,
        transport: impl TransportClient + 'static,
    ) -> Result<Self, ClientError> {
        // Every device id makes a valid client id if one does.
        client_id(&config.person, 0)?;
        let device_id = metadata.open().await?;
        Ok(Self {
            client_id: client_id(&config.person, device_id)?,
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
                    schema_id,
                    schema,
                    room_incarnation,
                    ..
                } if reply_id == correlation_id
                    && room_id == room.id
                    && incarnation.is_none_or(|known| known == room_incarnation) =>
                {
                    let metadata = RoomMetadata {
                        room_incarnation,
                        schema_id,
                        schema,
                    };
                    let registered =
                        move |local: Local, room| local.registered(room, epoch, cursor, metadata);
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
    /// it ended: a caller that stops waiting does not cut it halfway.
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
        let task = tokio::spawn(async move {
            let _applier = applier;
            change.await
        });
        task.await
            .map_err(|err| ClientError::Storage(format!("A change of the replica failed: {err}")))?
    }

    /// Reacts to `reply` to the request `correlation_id`, a sync or an ack of the replica of
    /// `epoch` at `cursor`, when it is not the reply the request asks for: an error of the
    /// request (which may come without its correlation id and its room) that asks to register
    /// again, `ClientNotRegistered` or `InvalidSequence`, is `Ok`. Any other error is `Network`,
    /// and after `BehindCompaction` the replica waits for a snapshot if it is still at `cursor`
    /// (if it moved past it, the reply was delayed). Any other reply is `Network` too.
    async fn refused(
        &self,
        room: &Arc<Room>,
        epoch: u64,
        cursor: SequenceNumber,
        correlation_id: CorrelationId,
        reply: ServerMessage,
    ) -> Result<(), ClientError> {
        let invalid = || ClientError::Network("Invalid reply to a sync or an ack".to_string());
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
                let behind = move |local: Local, room| local.behind(room, epoch, cursor);
                self.change(room, behind).await?;
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
    /// cursor, and the sync goes on; a second time in a sync is `Network`. `BehindCompaction`
    /// leaves the replica waiting for a snapshot, and the sync of a replica that waits is
    /// `Network`, without sending anything.
    ///
    /// A batch is applied only with the schema it announces or a later version of it: one that
    /// announces another schema, or a later version, is not applied, and the replica registers
    /// again to take the schema of `Registered`, as an open does. So does a batch with an
    /// operation that does not validate against the schema of the replica; once the sync
    /// registered again for a signal, it fails the room closed. The sync of a room that fails
    /// closed is `FailedClosed`, without sending anything.
    pub async fn sync(&self, room_id: RoomId) -> Result<(), ClientError> {
        let room = self.room(&room_id)?;
        let mut again = false;
        loop {
            let request = self.change(&room, Local::request).await?;
            let Some((from_seq, _)) = request.cursor.filter(|_| request.registered) else {
                let _registration = room.registration.lock().await;
                self.register(&room).await?;
                continue;
            };
            if request.awaits_snapshot {
                return Err(ClientError::Network(
                    "The replica waits for a snapshot".to_string(),
                ));
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
                    schema_version,
                    schema_id,
                    ..
                } if reply_id == correlation_id && room_id == room.id => {
                    (head_seq, ops, has_more, (schema_id, schema_version))
                }
                reply => {
                    self.refused(&room, epoch, from_seq, correlation_id, reply)
                        .await?;
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
                    if head_seq >= head {
                        return Ok(());
                    }
                }
                reply => {
                    self.refused(&room, epoch, head, correlation_id, reply)
                        .await?
                }
            }
            room.register_again(epoch, &mut again)?;
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
        let (registered, failed_closed, epoch) = {
            let state = room.state();
            let Replica::Open(OpenReplica {
                registered,
                failed_closed,
                ..
            }) = state.replica
            else {
                return Err(ClientError::RoomClosed(room_id));
            };
            (registered, failed_closed, state.epoch)
        };
        Ok(RoomStatus {
            head_seq: self.head(&room_id).await?,
            epoch,
            registered,
            failed_closed,
            writes_in_flight: 0,
            retained: 0,
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
                awaits_snapshot: open.is_some_and(|open| open.awaits_snapshot),
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
    /// replica of another epoch is left as it is, and so is one that fails closed.
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
    ) -> Result<(), ClientError> {
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
            room.state().replica = Replica::Open(OpenReplica::new(metadata, true));
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
        room.update(epoch, |open| open.registered = true);
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
            Ok(report) => Ok(Applied::Head(report.head_seq())),
            Err(StorageError::SchemaValidation(_)) if again => {
                room.update(epoch, OpenReplica::fail_closed);
                Err(ClientError::FailedClosed(room.id.clone()))
            }
            Err(StorageError::SchemaValidation(_)) => Ok(Applied::Schema),
            Err(err @ (StorageError::Io(_) | StorageError::RoomFailed { .. })) => {
                self.clone().close(Arc::clone(&room)).await?;
                self.open(room).await?;
                Err(err.into())
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Leaves the replica of `epoch` waiting for a snapshot if it is still at `cursor`: one that
    /// moved past it got a delayed reply.
    async fn behind(
        self,
        room: Arc<Room>,
        epoch: u64,
        cursor: SequenceNumber,
    ) -> Result<(), ClientError> {
        if room.is_open() && self.storage.get_head_seq(&room.id).await? == cursor {
            room.update(epoch, |open| open.awaits_snapshot = true);
        }
        Ok(())
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
