use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use zemdb_core::{
    decode_message, ClientId, ClientMessage, CorrelationId, ErrorCode, RoomId, SequenceNumber,
    ServerMessage,
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
    /// replica's.
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
    /// Open in the storage engine, with the metadata that describe all of its data.
    Open {
        metadata: RoomMetadata,
        registered: bool,
    },
}

impl Room {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn is_open(&self) -> bool {
        matches!(self.state().replica, Replica::Open { .. })
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
            Err(ClientError::Network(_)) if room.is_open() => Ok(()),
            registered => registered,
        }
    }

    /// Registers `room`, under its registration lock, unless it is registered: with the head of
    /// its storage room as its cursor and the incarnation of its metadata, or without them if it
    /// has none. `RoomReset` or `InvalidSequence` to a cursor discard the replica, which
    /// registers again without one.
    async fn register(&self, room: &Arc<Room>) -> Result<(), ClientError> {
        loop {
            let (incarnation, token) = {
                let state = room.state();
                let incarnation = match &state.replica {
                    Replica::Closed => return Err(ClientError::RoomClosed(room.id.clone())),
                    Replica::Empty => None,
                    Replica::Open {
                        registered: true, ..
                    } => return Ok(()),
                    Replica::Open { metadata, .. } => Some(metadata.room_incarnation),
                };
                (incarnation, state.token.clone())
            };
            let cursor = match incarnation {
                Some(_) => Some(self.head(&room.id).await?),
                None => None,
            };
            let correlation_id =
                CorrelationId::new(self.correlation_ids.fetch_add(1, Ordering::Relaxed));
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
                    return self
                        .change(room, |local, room| local.registered(room, metadata))
                        .await;
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
                            self.change(room, Local::discard_open).await?;
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
    async fn change<F>(
        &self,
        room: &Arc<Room>,
        change: impl FnOnce(Local, Arc<Room>) -> F,
    ) -> Result<(), ClientError>
    where
        F: Future<Output = Result<(), ClientError>> + Send + 'static,
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

    /// The head of the storage room of `room_id`, read outside its applier: a storage room
    /// closed meanwhile is `RoomClosed`.
    async fn head(&self, room_id: &RoomId) -> Result<SequenceNumber, ClientError> {
        match self.local.storage.get_head_seq(room_id).await {
            Err(StorageError::RoomNotFound(_)) => Err(ClientError::RoomClosed(room_id.clone())),
            head => Ok(head?),
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
        let (registered, epoch) = {
            let state = room.state();
            let Replica::Open { registered, .. } = state.replica else {
                return Err(ClientError::RoomClosed(room_id));
            };
            (registered, state.epoch)
        };
        Ok(RoomStatus {
            head_seq: self.head(&room_id).await?,
            epoch,
            registered,
            failed_closed: false,
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
                room.state().replica = Replica::Open {
                    metadata,
                    registered: false,
                };
                Ok(())
            }
            Err(StorageError::SnapshotCorruption(_) | StorageError::WalCorruption(_)) => {
                self.discard(room).await
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Takes `Registered`, with the metadata it gives: an empty replica saves them before its
    /// data, and opens its storage room with their schema; an open one is registered.
    async fn registered(self, room: Arc<Room>, metadata: RoomMetadata) -> Result<(), ClientError> {
        match &mut room.state().replica {
            Replica::Closed => return Err(ClientError::RoomClosed(room.id.clone())),
            Replica::Open { registered, .. } => {
                *registered = true;
                return Ok(());
            }
            // Out of the client until it opens.
            replica => *replica = Replica::Closed,
        }
        self.metadata.save_room(&room.id, &metadata).await?;
        self.storage
            .open_room(&room.id, metadata.schema.clone())
            .await?;
        room.state().replica = Replica::Open {
            metadata,
            registered: true,
        };
        Ok(())
    }

    /// Discards the replica of an open room. One closed meanwhile is left as it is.
    async fn discard_open(self, room: Arc<Room>) -> Result<(), ClientError> {
        if !room.is_open() {
            return Err(ClientError::RoomClosed(room.id.clone()));
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
        if let Replica::Open { .. } = replica {
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
            Replica::Open { .. } => Ok(self.storage.close_room(&room.id).await?),
            Replica::Empty => Ok(()),
            Replica::Closed => Err(ClientError::RoomClosed(room.id.clone())),
        }
    }
}
