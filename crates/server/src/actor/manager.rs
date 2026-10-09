use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Mutex, OwnedMutexGuard};
use tracing::info;
use zemdb_core::id::{RoomId, SchemaId, SequenceNumber};
use zemdb_core::schema::Schema;

use crate::actor::command::RoomCommand;
use crate::actor::room::{ActorExit, RoomActor};
use crate::actor::snapshot_demand::DESIGNATION_TIMEOUT;
use crate::config::ServerConfig;
use crate::durable;
use crate::error::ServerError;
use crate::log::{RoomLifecycleOverrides, RoomLifecyclePolicy};
use crate::relay::SnapshotRelay;
use crate::schema_registry::SchemaRegistry;

/// File, inside the room directory, recording the room's schema assignment and lifecycle
/// overrides.
const META_ROOM_FILE: &str = "meta_room.json";

/// How long a request waits for a room actor to take its command and reply.
pub const ACTOR_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a request whose reply was dropped waits for the actor to finish, to tell a panic
/// from a deliberate stop.
const ACTOR_EXIT_WAIT: Duration = Duration::from_secs(1);

/// How many times a request is sent to a room whose mailbox closed before taking it.
const MAX_SEND_ATTEMPTS: usize = 3;

/// Persistent room configuration (`meta_room.json`): the room's schema and the lifecycle
/// settings in which it differs from the server defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomMetadata {
    pub room_id: RoomId,
    pub schema_id: SchemaId,
    /// Lifecycle overrides of the room; a file without them has none.
    #[serde(default)]
    pub lifecycle: RoomLifecycleOverrides,
}

/// A room actor as seen by the manager, from its spawn until its task has finished.
#[derive(Debug, Clone)]
struct RoomSlot {
    sender: mpsc::Sender<RoomCommand>,
    /// Set once the actor task has finished, after the actor released the room's files.
    exit: watch::Receiver<Option<ActorExit>>,
    /// Tells this actor apart from a later one of the same room.
    actor_id: u64,
}

impl RoomSlot {
    /// Whether the actor still accepts commands.
    fn is_live(&self) -> bool {
        !self.sender.is_closed()
    }

    /// Waits until the actor task has finished and returns how the actor ended. `None` if the
    /// task vanished without reporting (it was cancelled), which also released the files.
    async fn finished(&self) -> Option<ActorExit> {
        let mut exit = self.exit.clone();
        exit.wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|exit| *exit)
    }

    /// The error for a request whose reply the actor dropped. The actor drops a reply only
    /// when it stops: on purpose (a failure that requires reopening the room from disk) the
    /// request is `Unavailable` and may be retried; if the actor panicked while handling it,
    /// the request is `Internal`, since retrying it could panic again. Commands that were
    /// still queued behind a panic are answered `Unavailable` by the actor itself.
    async fn dropped_reply_error(&self, room_id: &RoomId) -> ServerError {
        match tokio::time::timeout(ACTOR_EXIT_WAIT, self.finished()).await {
            Ok(Some(ActorExit::Panicked)) => {
                ServerError::Internal(format!("Room {room_id} failed while handling the request"))
            }
            _ => ServerError::Unavailable(format!("Room {room_id} actor stopped before replying")),
        }
    }
}

/// Exclusive right to spawn, create or delete one room.
struct SpawnGuard<'a> {
    locks: &'a DashMap<RoomId, Arc<Mutex<()>>>,
    room_id: RoomId,
    guard: OwnedMutexGuard<()>,
}

impl Drop for SpawnGuard<'_> {
    /// Forgets the room's lock unless another task holds a reference to it, while the lock is
    /// still held (`guard` is dropped after this). The map and this guard account for two
    /// references; any other belongs to a task that cloned the lock and waits for it, so the
    /// entry stays and that task and every later caller share one mutex. Cloning the lock and
    /// this removal both take the map shard's lock: a caller either cloned it before the
    /// check, or creates a new mutex after the removal, when this critical section is over.
    fn drop(&mut self) {
        let mutex = OwnedMutexGuard::mutex(&self.guard);
        self.locks.remove_if(&self.room_id, |_, lock| {
            Arc::ptr_eq(lock, mutex) && Arc::strong_count(lock) == 2
        });
    }
}

/// A caller waiting for a room's spawn lock.
struct SpawnLockWait<'a> {
    locks: &'a DashMap<RoomId, Arc<Mutex<()>>>,
    room_id: RoomId,
    lock: Arc<Mutex<()>>,
    acquiring: Option<Pin<Box<dyn Future<Output = OwnedMutexGuard<()>> + Send>>>,
}

impl Drop for SpawnLockWait<'_> {
    /// A caller abandoned while waiting (a timeout, a client that went away) forgets the lock
    /// under the rule of [`SpawnGuard`], so that its entry does not outlive every user. The
    /// pending acquisition, which holds its own reference, is dropped first; then the map and
    /// `lock` account for two references. After a successful acquisition the guard holds one
    /// more, so nothing is removed.
    fn drop(&mut self) {
        self.acquiring = None;
        self.locks.remove_if(&self.room_id, |_, lock| {
            Arc::ptr_eq(lock, &self.lock) && Arc::strong_count(lock) == 2
        });
    }
}

/// Sharded room actor manager providing lazy spawning and routing of commands to room actors.
#[derive(Debug)]
pub struct RoomManager {
    /// Room actors from their spawn until their task finishes. The task of a stopped actor
    /// removes its own slot, so stopped rooms leave nothing behind.
    rooms: Arc<DashMap<RoomId, RoomSlot>>,
    /// Cached `meta_room.json` of the rooms seen by this manager.
    room_meta: DashMap<RoomId, RoomMetadata>,
    spawn_locks: DashMap<RoomId, Arc<Mutex<()>>>,
    next_actor_id: AtomicU64,
    config: Arc<ServerConfig>,
    schema_registry: Arc<SchemaRegistry>,
    snapshot_relay: Arc<SnapshotRelay>,
    data_dir: PathBuf,
    /// Lifecycle policy of a room without overrides.
    default_policy: RoomLifecyclePolicy,
    /// Time a client designated to upload a snapshot has to start the upload; always
    /// [`DESIGNATION_TIMEOUT`] outside tests.
    designation_timeout: Duration,
}

impl RoomManager {
    /// Creates a new RoomManager with the given configuration, schema registry, and snapshot
    /// relay. The configuration provides the server defaults of the room lifecycle policy.
    pub fn new(
        config: Arc<ServerConfig>,
        schema_registry: Arc<SchemaRegistry>,
        snapshot_relay: Arc<SnapshotRelay>,
    ) -> Self {
        let data_dir = config.data_dir.clone();
        let default_policy = config.default_lifecycle_policy();
        Self {
            rooms: Arc::new(DashMap::new()),
            room_meta: DashMap::new(),
            spawn_locks: DashMap::new(),
            next_actor_id: AtomicU64::new(0),
            config,
            schema_registry,
            snapshot_relay,
            data_dir,
            default_policy,
            designation_timeout: DESIGNATION_TIMEOUT,
        }
    }

    /// Replaces the server defaults of the room lifecycle policy, which [`new`](Self::new)
    /// takes from the configuration, for rooms spawned afterwards. Rooms still apply their
    /// overrides on top. The configuration counts durations in whole seconds; this accepts any
    /// duration, for example sub-second thresholds in tests.
    pub fn with_default_policy(mut self, policy: RoomLifecyclePolicy) -> Self {
        self.default_policy = policy;
        self
    }

    /// Shortens the time a designated snapshot uploader has to start, for rooms spawned
    /// afterwards.
    #[cfg(test)]
    pub(crate) fn set_designation_timeout(&mut self, timeout: Duration) {
        self.designation_timeout = timeout;
    }

    /// Registers `sender` as the live actor of `room_id`, for tests that play the actor
    /// themselves. The returned sender reports how that actor ended.
    #[cfg(test)]
    pub(crate) fn install_test_actor(
        &self,
        room_id: &RoomId,
        sender: mpsc::Sender<RoomCommand>,
    ) -> watch::Sender<Option<ActorExit>> {
        let (exit_tx, exit_rx) = watch::channel(None);
        self.rooms.insert(
            room_id.clone(),
            RoomSlot {
                sender,
                exit: exit_rx,
                actor_id: u64::MAX,
            },
        );
        exit_tx
    }

    /// Retrieves an existing room actor sender or lazily spawns one, recovering the room from
    /// disk with its effective lifecycle policy. With `schema_id`, the room is assigned that
    /// schema (keeping its lifecycle overrides), creating it if needed.
    pub async fn get_or_spawn(
        &self,
        room_id: &RoomId,
        schema_id: Option<&SchemaId>,
    ) -> Result<mpsc::Sender<RoomCommand>, ServerError> {
        self.slot(room_id, schema_id).await.map(|slot| slot.sender)
    }

    /// The live actor of `room_id`, spawning it if needed.
    async fn slot(
        &self,
        room_id: &RoomId,
        schema_id: Option<&SchemaId>,
    ) -> Result<RoomSlot, ServerError> {
        // 1. Fast check if room is already active and channel is open
        if let Some(slot) = self.live_slot(room_id) {
            return Ok(slot);
        }

        // 2. Acquire per-room spawn lock to prevent duplicate instantiation
        let _guard = self.lock_room(room_id).await;

        // 3. Double-check if another task spawned the room while waiting for the lock
        if let Some(slot) = self.live_slot(room_id) {
            return Ok(slot);
        }

        // 3b. A previous actor for this room stopped on its own (after a failed log write, a
        // panic, or for inactivity). Wait until it has fully exited and released the room's
        // files before recovering the room from disk.
        // The slot stays in the map while waiting, so a request abandoned at this point
        // leaves the next one still waiting for the same actor.
        self.wait_for_previous_actor(room_id).await;

        // 4. Resolve the room's schema and lifecycle overrides
        let meta = self.resolve_metadata(room_id, schema_id)?;

        // 5. Resolve Schema definition from registry
        let schema = self
            .schema_registry
            .get_schema(&meta.schema_id)
            .ok_or_else(|| ServerError::SchemaNotFound(meta.schema_id.to_string()))?;

        // 6. Spawn RoomActor with its effective policy
        let slot = self.spawn_actor(room_id, &meta, schema)?;
        info!(room = %room_id, "RoomActor lazily initialized and registered in RoomManager");
        Ok(slot)
    }

    /// The actor of `room_id`, if it still accepts commands.
    fn live_slot(&self, room_id: &RoomId) -> Option<RoomSlot> {
        self.rooms
            .get(room_id)
            .filter(|slot| slot.is_live())
            .map(|slot| slot.clone())
    }

    /// Takes the spawn lock of `room_id`.
    async fn lock_room(&self, room_id: &RoomId) -> SpawnGuard<'_> {
        let lock = self.spawn_locks.entry(room_id.clone()).or_default().clone();
        let mut wait = SpawnLockWait {
            locks: &self.spawn_locks,
            room_id: room_id.clone(),
            acquiring: Some(Box::pin(Arc::clone(&lock).lock_owned())),
            lock,
        };
        let guard = match wait.acquiring.as_mut() {
            Some(acquiring) => acquiring.await,
            None => unreachable!("the acquisition is set above"),
        };
        SpawnGuard {
            locks: &self.spawn_locks,
            room_id: room_id.clone(),
            guard,
        }
    }

    /// Waits until the previous actor task of `room_id`, if any, has finished. Cancel-safe:
    /// the slot is only removed by the actor task itself; map guards are never held across an
    /// `.await`.
    async fn wait_for_previous_actor(&self, room_id: &RoomId) {
        let previous = self.rooms.get(room_id).map(|slot| slot.clone());
        if let Some(previous) = previous {
            previous.finished().await;
        }
    }

    /// The metadata of `room_id`: cached, read from `meta_room.json`, or, when `schema_id`
    /// is given, written with that schema and the room's current lifecycle overrides.
    fn resolve_metadata(
        &self,
        room_id: &RoomId,
        schema_id: Option<&SchemaId>,
    ) -> Result<RoomMetadata, ServerError> {
        let room_dir = self.room_dir(room_id);
        let meta_room_path = room_dir.join(META_ROOM_FILE);
        let known = match self.room_meta.get(room_id).map(|meta| meta.clone()) {
            Some(meta) => Some(meta),
            None if meta_room_path.exists() => Some(read_room_metadata(&meta_room_path)?),
            None => None,
        };
        let meta = match (schema_id, known) {
            (Some(schema_id), known) => {
                let meta = RoomMetadata {
                    room_id: room_id.clone(),
                    schema_id: schema_id.clone(),
                    lifecycle: known.map(|meta| meta.lifecycle).unwrap_or_default(),
                };
                write_room_metadata(&room_dir, &meta)?;
                meta
            }
            (None, Some(meta)) => meta,
            (None, None) => {
                return Err(ServerError::RoomNotFound(format!(
                    "No schema assigned or directory found for room '{}'",
                    room_id
                )))
            }
        };
        self.room_meta.insert(room_id.clone(), meta.clone());
        Ok(meta)
    }

    /// Opens the room with its effective lifecycle policy (the server defaults overlaid with
    /// its overrides) and starts its actor task. The task reports how the actor ended once
    /// the actor has released the room's files, and then forgets the slot (unless a newer
    /// actor replaced it), so a room that stopped and receives no more requests leaves nothing
    /// behind. A request arriving before that still finds the closed slot and waits.
    fn spawn_actor(
        &self,
        room_id: &RoomId,
        meta: &RoomMetadata,
        schema: Arc<Schema>,
    ) -> Result<RoomSlot, ServerError> {
        let policy = self.default_policy.with_overrides(&meta.lifecycle);
        let (sender, actor) = RoomActor::open(
            room_id.clone(),
            meta.schema_id.clone(),
            schema,
            &self.data_dir,
            Arc::clone(&self.config),
            policy,
            Arc::clone(&self.snapshot_relay),
            self.designation_timeout,
        )?;
        let actor_id = self.next_actor_id.fetch_add(1, Ordering::Relaxed);
        let (exit_tx, exit_rx) = watch::channel(None);
        let slot = RoomSlot {
            sender,
            exit: exit_rx,
            actor_id,
        };
        self.rooms.insert(room_id.clone(), slot.clone());

        let rooms = Arc::clone(&self.rooms);
        let room_id = room_id.clone();
        tokio::spawn(async move {
            let exit = actor.run().await;
            rooms.remove_if(&room_id, |_, slot| slot.actor_id == actor_id);
            exit_tx.send_replace(Some(exit));
        });
        Ok(slot)
    }

    fn room_dir(&self, room_id: &RoomId) -> PathBuf {
        self.data_dir.join("rooms").join(room_id.as_str())
    }

    /// Fast check if a room exists in memory or on disk.
    pub fn room_exists(&self, room_id: &RoomId) -> bool {
        if self.rooms.contains_key(room_id) || self.room_meta.contains_key(room_id) {
            return true;
        }
        self.room_dir(room_id).join(META_ROOM_FILE).exists()
    }

    /// The lifecycle overrides of a room this manager has opened or created.
    pub fn lifecycle_overrides(&self, room_id: &RoomId) -> Option<RoomLifecycleOverrides> {
        self.room_meta
            .get(room_id)
            .map(|meta| meta.lifecycle.clone())
    }

    /// Gets an existing active sender for the room, if running.
    pub fn get_room(&self, room_id: &RoomId) -> Option<mpsc::Sender<RoomCommand>> {
        self.live_slot(room_id).map(|slot| slot.sender)
    }

    /// Gracefully closes and shuts down an active room actor, awaiting task termination.
    pub async fn shutdown_room(&self, room_id: &RoomId) -> bool {
        let Some(slot) = self.rooms.get(room_id).map(|slot| slot.clone()) else {
            return false;
        };
        let (tx, rx) = oneshot::channel();
        if slot
            .sender
            .send(RoomCommand::Shutdown { reply: tx })
            .await
            .is_ok()
        {
            let _ = rx.await;
        }
        slot.finished().await;
        true
    }

    /// Gracefully shuts down all active room actors, awaiting task terminations.
    pub async fn shutdown_all(&self) {
        let room_ids: Vec<RoomId> = self.rooms.iter().map(|kv| kv.key().clone()).collect();
        for id in room_ids {
            self.shutdown_room(&id).await;
        }
    }

    /// Returns a list of all currently active RoomIds.
    pub fn list_active_rooms(&self) -> Vec<RoomId> {
        self.rooms
            .iter()
            .filter(|kv| kv.value().is_live())
            .map(|kv| kv.key().clone())
            .collect()
    }

    /// Explicitly creates and provisions a new room. Returns error if room already exists.
    ///
    /// `lifecycle` holds the settings in which the room differs from the server defaults
    /// (`None`: none). They are validated before anything is written (`BadRequest`) and
    /// stored in `meta_room.json`, so they also apply whenever the room reopens.
    pub async fn create_room(
        &self,
        room_id: RoomId,
        schema_id: SchemaId,
        lifecycle: Option<RoomLifecycleOverrides>,
    ) -> Result<RoomMetadata, ServerError> {
        let lifecycle = lifecycle.unwrap_or_default();
        lifecycle.validate()?;

        let _guard = self.lock_room(&room_id).await;

        let room_dir = self.room_dir(&room_id);
        let meta_room_path = room_dir.join(META_ROOM_FILE);

        if meta_room_path.exists() || self.rooms.contains_key(&room_id) {
            return Err(ServerError::RoomAlreadyExists(room_id.to_string()));
        }

        // Validate schema exists in SchemaRegistry
        let schema = self
            .schema_registry
            .get_schema(&schema_id)
            .ok_or_else(|| ServerError::SchemaNotFound(schema_id.to_string()))?;

        let meta = RoomMetadata {
            room_id: room_id.clone(),
            schema_id,
            lifecycle,
        };
        write_room_metadata(&room_dir, &meta)?;
        self.room_meta.insert(room_id.clone(), meta.clone());

        // Read the schema again now that the room is listed for schema reloads: an evolution
        // whose reload did not list it happened before this read.
        let schema = self
            .schema_registry
            .get_schema(&meta.schema_id)
            .unwrap_or(schema);
        self.spawn_actor(&room_id, &meta, schema)?;
        Ok(meta)
    }

    /// Deletes a room: gracefully shuts down the active actor and purges the room directory from disk.
    pub async fn delete_room(&self, room_id: &RoomId) -> Result<(), ServerError> {
        let _guard = self.lock_room(room_id).await;

        self.shutdown_room(room_id).await;
        self.room_meta.remove(room_id);
        // A room recreated with the same id must not inherit this room's snapshot.
        self.snapshot_relay.purge_room(room_id).await?;

        let room_dir = self.room_dir(room_id);
        if room_dir.exists() {
            fs::remove_dir_all(&room_dir)?;
            Ok(())
        } else {
            Err(ServerError::RoomNotFound(room_id.to_string()))
        }
    }

    /// Retained log range of a room as `(tail_seq, head_seq)`, spawning its actor if needed.
    /// A room that does not exist is `RoomNotFound`.
    pub async fn log_bounds(
        &self,
        room_id: &RoomId,
    ) -> Result<(SequenceNumber, SequenceNumber), ServerError> {
        self.ask(room_id, |reply| RoomCommand::GetLogBounds { reply })
            .await?
    }

    /// Sends a command to the room's actor, spawning it if needed, and waits for its reply,
    /// bounded by [`ACTOR_TIMEOUT`].
    ///
    /// A room that does not exist is `RoomNotFound`. A command that finds the actor's mailbox
    /// closed was not executed, so it is sent to the room respawned from disk; that is what
    /// happens to requests racing with a room shutting down for inactivity. An actor that
    /// dropped the command's reply stopped while handling it or while it was queued: the
    /// request is `Unavailable` (it may be retried; the next request respawns the room), or
    /// `Internal` if the actor panicked while handling it. An actor that does not take and
    /// answer the command in time is `Timeout`, which may be retried.
    pub async fn ask<R>(
        &self,
        room_id: &RoomId,
        command: impl FnOnce(oneshot::Sender<R>) -> RoomCommand,
    ) -> Result<R, ServerError> {
        let (tx, rx) = oneshot::channel();
        let mut command = command(tx);
        let exchange = async move {
            let mut attempts = 0;
            let slot = loop {
                let slot = self.slot(room_id, None).await?;
                match slot.sender.send(command).await {
                    Ok(()) => break slot,
                    Err(mpsc::error::SendError(unsent)) => {
                        attempts += 1;
                        if attempts == MAX_SEND_ATTEMPTS {
                            return Err(ServerError::Unavailable(format!(
                                "Room {room_id} actor stopped"
                            )));
                        }
                        command = unsent;
                    }
                }
            };
            Ok((slot, rx.await))
        };
        let (slot, reply) = tokio::time::timeout(ACTOR_TIMEOUT, exchange)
            .await
            .map_err(|_| {
                ServerError::Timeout(format!("Room {room_id} actor did not answer in time"))
            })??;
        // Telling a panic from a deliberate stop has its own bound, outside the actor
        // timeout: a request that waited long in the mailbox must still get `Internal` if the
        // actor panicked while handling it.
        match reply {
            Ok(reply) => Ok(reply),
            Err(_) => Err(slot.dropped_reply_error(room_id).await),
        }
    }

    /// Reloads the schema across all active rooms associated with `schema_id`.
    pub async fn reload_schema_for_rooms(
        &self,
        schema_id: &SchemaId,
        schema: Arc<zemdb_core::schema::Schema>,
    ) -> Vec<RoomId> {
        // The targets are collected first so that no map guard is held while waiting for the
        // actors: a concurrent writer to the same shard would otherwise block on the lock.
        // A room is listed here as soon as its metadata is resolved, before its actor reads the
        // schema from the registry.
        let room_ids: Vec<RoomId> = self
            .room_meta
            .iter()
            .filter(|kv| &kv.value().schema_id == schema_id)
            .map(|kv| kv.key().clone())
            .collect();

        let mut reloaded = Vec::new();
        for room_id in room_ids {
            // An actor being spawned may have read the previous schema: the spawn lock makes
            // the reload wait for it. Once the reload is queued, any later actor of the room
            // reads the registry, which already holds the new schema.
            let reply = {
                let _guard = self.lock_room(&room_id).await;
                let Some(sender) = self.get_room(&room_id) else {
                    continue;
                };
                let (tx, rx) = oneshot::channel();
                let cmd = RoomCommand::ReloadSchema {
                    schema: Arc::clone(&schema),
                    reply: tx,
                };
                if sender.send(cmd).await.is_err() {
                    continue;
                }
                rx
            };
            let _ = reply.await;
            reloaded.push(room_id);
        }
        reloaded
    }
}

/// Atomically and durably writes a room's `meta_room.json`, creating its directory if needed.
fn write_room_metadata(room_dir: &Path, meta: &RoomMetadata) -> Result<(), ServerError> {
    durable::create_dir_all_synced(room_dir)?;
    let json = serde_json::to_string_pretty(meta).map_err(|e| {
        ServerError::Serialization(format!("Failed to serialize meta_room.json: {}", e))
    })?;
    durable::write_atomic(&room_dir.join(META_ROOM_FILE), json.as_bytes())?;
    Ok(())
}

/// Reads a room's `meta_room.json`, discarding a temporary file left by an interrupted write.
/// An unreadable file, or one with invalid lifecycle overrides, is an error: the room cannot
/// be served without knowing its schema and policy.
fn read_room_metadata(meta_room_path: &Path) -> Result<RoomMetadata, ServerError> {
    match fs::remove_file(durable::tmp_path_for(meta_room_path)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let content = fs::read_to_string(meta_room_path)?;
    let meta: RoomMetadata = serde_json::from_str(&content).map_err(|e| {
        ServerError::Serialization(format!("Failed to parse meta_room.json: {}", e))
    })?;
    meta.lifecycle
        .check(|setting| format!("lifecycle.{}", setting.field()))
        .map_err(|e| ServerError::Serialization(format!("Invalid meta_room.json: {e}")))?;
    Ok(meta)
}

#[cfg(test)]
#[path = "tests/manager.rs"]
mod tests;
