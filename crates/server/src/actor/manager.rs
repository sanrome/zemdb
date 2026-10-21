use dashmap::DashMap;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Mutex, OwnedMutexGuard};
use tracing::{info, warn};
use uuid::Uuid;
use zemdb_core::id::{RoomId, SchemaId};
use zemdb_core::schema::Schema;

use crate::actor::command::RoomCommand;
use crate::actor::room::{room_dir, ActorExit, ActorTimings, RoomActor};
use crate::blocking::blocking_io;
use crate::config::ServerConfig;
use crate::durable;
use crate::error::ServerError;
use crate::fail_point;
use crate::log::{RoomLifecycleOverrides, RoomLifecyclePolicy};
use crate::relay::{LogBounds, SnapshotRelay};
use crate::response_budget::InflightBudget;
use crate::schema_registry::SchemaRegistry;

/// File, inside the room directory, recording the room's schema assignment and lifecycle
/// overrides.
const META_ROOM_FILE: &str = "meta_room.json";

/// Prefix of the name a room's directory is renamed to when the room is deleted. No room id
/// contains `.`, so it never names a room.
const DELETED_ROOM_PREFIX: &str = ".deleted-";

/// How long a request waits for a room actor to take its command and reply.
pub const ACTOR_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a request whose reply was dropped waits for the actor to finish, to tell a panic
/// from a deliberate stop.
const ACTOR_EXIT_WAIT: Duration = Duration::from_secs(1);

/// How many times a request is sent to a room whose mailbox closed before taking it.
const MAX_SEND_ATTEMPTS: usize = 3;

/// How many rooms `shutdown_all` closes at once.
const SHUTDOWN_CONCURRENCY: usize = 64;

/// Persistent room configuration (`meta_room.json`): the room's schema, its incarnation and the
/// lifecycle settings in which it differs from the server defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomMetadata {
    pub room_id: RoomId,
    pub schema_id: SchemaId,
    /// Random id drawn when the room is created: a room deleted and created again with the
    /// same id gets a new one, so that clients can tell their cursor belongs to another room.
    pub room_incarnation: u128,
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
        let lock = OwnedMutexGuard::mutex(&self.guard);
        forget_lock_if_unused(self.locks, &self.room_id, lock);
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
        forget_lock_if_unused(self.locks, &self.room_id, &self.lock);
    }
}

/// Removes `room_id`'s entry from `locks` if it is still `lock` and only the map and the
/// caller reference it (the rule explained on [`SpawnGuard`]'s `Drop`).
fn forget_lock_if_unused(
    locks: &DashMap<RoomId, Arc<Mutex<()>>>,
    room_id: &RoomId,
    lock: &Arc<Mutex<()>>,
) {
    locks.remove_if(room_id, |_, entry| {
        Arc::ptr_eq(entry, lock) && Arc::strong_count(entry) == 2
    });
}

/// Sharded room actor manager providing lazy spawning and routing of commands to room actors.
#[derive(Debug)]
pub struct RoomManager {
    /// Room actors from their spawn until their task finishes. The task of a stopped actor
    /// removes its own slot, so stopped rooms leave nothing behind.
    rooms: Arc<DashMap<RoomId, RoomSlot>>,
    /// Cached `meta_room.json` of the rooms whose actor is running or being spawned. An
    /// entry is added before the actor reads its schema, so schema reloads never miss it,
    /// and removed when the actor ends or fails to spawn.
    room_meta: Arc<DashMap<RoomId, RoomMetadata>>,
    spawn_locks: DashMap<RoomId, Arc<Mutex<()>>>,
    next_actor_id: AtomicU64,
    config: Arc<ServerConfig>,
    schema_registry: Arc<SchemaRegistry>,
    snapshot_relay: Arc<SnapshotRelay>,
    /// Bytes of large responses in flight, which the room actors and the handlers reserve.
    response_budget: Arc<InflightBudget>,
    data_dir: PathBuf,
    /// Lifecycle policy of a room without overrides.
    default_policy: RoomLifecyclePolicy,
    /// Timings of the room actors; always [`ActorTimings::default`] outside tests.
    timings: ActorTimings,
}

impl RoomManager {
    /// Creates a new RoomManager with the given configuration, schema registry, and snapshot
    /// relay. The configuration provides the server defaults of the room lifecycle policy.
    /// Removes what the deletions of rooms left behind in `data_dir` ([`delete_room`]).
    ///
    /// [`delete_room`]: Self::delete_room
    pub fn new(
        config: Arc<ServerConfig>,
        schema_registry: Arc<SchemaRegistry>,
        snapshot_relay: Arc<SnapshotRelay>,
    ) -> Self {
        let data_dir = config.data_dir.clone();
        let default_policy = config.default_lifecycle_policy();
        // A deletion cut short leaves its renamed directory behind; the room is already gone.
        if let Err(err) = remove_deleted_rooms(&data_dir.join("rooms")) {
            warn!(error = %err, "Failed to remove the directories of deleted rooms");
        }
        Self {
            response_budget: Arc::new(InflightBudget::new(
                config.max_inflight_response_bytes,
                config.max_inflight_response_bytes_per_client,
            )),
            rooms: Arc::new(DashMap::new()),
            room_meta: Arc::new(DashMap::new()),
            spawn_locks: DashMap::new(),
            next_actor_id: AtomicU64::new(0),
            config,
            schema_registry,
            snapshot_relay,
            data_dir,
            default_policy,
            timings: ActorTimings::default(),
        }
    }

    /// The budget of response bytes in flight that the room actors reserve from.
    pub(crate) fn response_budget(&self) -> &Arc<InflightBudget> {
        &self.response_budget
    }

    /// Replaces the server defaults of the room lifecycle policy, which [`new`](Self::new)
    /// takes from the configuration, for rooms spawned afterwards. Rooms still apply their
    /// overrides on top. The configuration counts durations in whole seconds; this accepts any
    /// duration, for example sub-second thresholds in tests.
    pub fn with_default_policy(mut self, policy: RoomLifecyclePolicy) -> Self {
        self.default_policy = policy;
        self
    }

    /// Replaces the timings of the room actors spawned afterwards (snapshot designation
    /// timeout and maintenance tick periods).
    #[cfg(test)]
    pub(crate) fn set_timings(&mut self, timings: ActorTimings) {
        self.timings = timings;
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
        if let Some(slot) = self.live_slot(room_id) {
            return Ok(slot);
        }

        let _guard = self.lock_room(room_id).await;

        // Another task may have spawned the room while this one waited for the lock.
        if let Some(slot) = self.live_slot(room_id) {
            return Ok(slot);
        }

        // A previous actor for this room stopped on its own (after a failed log write, a
        // panic, or for inactivity). Wait until it has fully exited and released the room's
        // files before recovering the room from disk.
        // The slot stays in the map while waiting, so a request abandoned at this point
        // leaves the next one still waiting for the same actor.
        self.wait_for_previous_actor(room_id).await;

        // Resolving the metadata and spawning the actor read and write the room's files.
        let slot = blocking_io(|| {
            let meta = self.resolve_metadata(room_id, schema_id)?;
            let schema = self
                .schema_registry
                .get_schema(&meta.schema_id)
                .ok_or_else(|| ServerError::SchemaNotFound(meta.schema_id.to_string()))?;
            self.spawn_actor(room_id, &meta, schema)
        })
        .inspect_err(|_| {
            self.room_meta.remove(room_id);
        })?;
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
        let guard = wait
            .acquiring
            .as_mut()
            .expect("the acquisition is set above")
            .await;
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
        let known = self.stored_metadata(room_id)?;
        let meta = match (schema_id, known) {
            (Some(schema_id), known) => {
                if let Some(known) = &known {
                    self.check_reassignment(room_id, &known.schema_id, schema_id)?;
                }
                let (room_incarnation, lifecycle) = known.map_or_else(
                    || (new_incarnation(), Default::default()),
                    |meta| (meta.room_incarnation, meta.lifecycle),
                );
                let meta = RoomMetadata {
                    room_id: room_id.clone(),
                    schema_id: schema_id.clone(),
                    room_incarnation,
                    lifecycle,
                };
                write_room_metadata(&room_dir(&self.data_dir, room_id), &meta)?;
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

    /// Rejects reassigning `room_id` from the schema `from` to the schema `to` unless `to`
    /// extends `from` (it keeps its tables, keys and columns and only adds nullable columns and
    /// tables) and both declare the same constraints, owners and immutable tables: the room's
    /// log was sequenced, and its replicas decide on its operations, under `from`. A schema with
    /// other columns would give the old operations' values to other columns, which a constraint
    /// added over them later would judge.
    fn check_reassignment(
        &self,
        room_id: &RoomId,
        from: &SchemaId,
        to: &SchemaId,
    ) -> Result<(), ServerError> {
        if from == to {
            return Ok(());
        }
        let schema = |id: &SchemaId| {
            self.schema_registry
                .get_schema(id)
                .ok_or_else(|| ServerError::SchemaNotFound(id.to_string()))
        };
        let (old, new) = (schema(from)?, schema(to)?);
        if !new.extends(&old) {
            return Err(ServerError::BadRequest(format!(
                "Room '{room_id}' cannot move from schema '{from}' to '{to}': '{to}' does not \
                 keep the tables, keys and columns of '{from}', adding only nullable columns \
                 and tables"
            )));
        }
        if old.has_same_constraints(&new) {
            return Ok(());
        }
        Err(ServerError::BadRequest(format!(
            "Room '{room_id}' cannot move from schema '{from}' to '{to}': their constraints, \
             owners or immutable tables differ"
        )))
    }

    /// The metadata of `room_id`, cached or read from its `meta_room.json`, if it has any.
    fn stored_metadata(&self, room_id: &RoomId) -> Result<Option<RoomMetadata>, ServerError> {
        if let Some(meta) = self.room_meta.get(room_id) {
            return Ok(Some(meta.clone()));
        }
        let path = room_dir(&self.data_dir, room_id).join(META_ROOM_FILE);
        durable::exists(&path)?
            .then(|| read_room_metadata(&path))
            .transpose()
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
            meta.room_incarnation,
            meta.schema_id.clone(),
            schema,
            &self.data_dir,
            Arc::clone(&self.config),
            policy,
            Arc::clone(&self.snapshot_relay),
            Arc::clone(&self.response_budget),
            self.timings,
        )?;
        let actor_id = self.next_actor_id.fetch_add(1, Ordering::Relaxed);
        let (exit_tx, exit_rx) = watch::channel(None);
        let slot = RoomSlot {
            sender,
            exit: exit_rx,
            actor_id,
        };
        self.rooms.insert(room_id.clone(), slot.clone());

        let (rooms, room_meta) = (Arc::clone(&self.rooms), Arc::clone(&self.room_meta));
        let relay = Arc::clone(&self.snapshot_relay);
        let room_id = room_id.clone();
        tokio::spawn(async move {
            let exit = actor.run().await;
            // Before the exit is reported: a respawn waits for it, under the spawn lock, before
            // adding the room's metadata and snapshot floor again. Taking the spawn lock here
            // would deadlock.
            room_meta.remove(&room_id);
            relay.clear_snapshot_floor(&room_id);
            rooms.remove_if(&room_id, |_, slot| slot.actor_id == actor_id);
            exit_tx.send_replace(Some(exit));
        });
        Ok(slot)
    }

    /// Fast check if a room exists in memory or on disk.
    pub fn room_exists(&self, room_id: &RoomId) -> Result<bool, ServerError> {
        Ok(self.rooms.contains_key(room_id)
            || blocking_io(|| {
                durable::exists(&room_dir(&self.data_dir, room_id).join(META_ROOM_FILE))
            })?)
    }

    /// The lifecycle overrides of a room, cached or read from its `meta_room.json`, under the
    /// spawn lock that every writer of that file holds.
    pub async fn lifecycle_overrides(
        &self,
        room_id: &RoomId,
    ) -> Result<RoomLifecycleOverrides, ServerError> {
        let _guard = self.lock_room(room_id).await;
        blocking_io(|| self.stored_metadata(room_id))?
            .map(|meta| meta.lifecycle)
            .ok_or_else(|| ServerError::RoomNotFound(room_id.to_string()))
    }

    /// The sender of the room's actor, if it is running.
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

    /// Gracefully shuts down all active room actors, up to `SHUTDOWN_CONCURRENCY` at once,
    /// awaiting task terminations.
    pub async fn shutdown_all(&self) {
        let room_ids: Vec<RoomId> = self.rooms.iter().map(|kv| kv.key().clone()).collect();
        futures::stream::iter(room_ids)
            .for_each_concurrent(SHUTDOWN_CONCURRENCY, |id| async move {
                self.shutdown_room(&id).await;
            })
            .await;
    }

    /// The rooms whose actor is running.
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

        let room_dir = room_dir(&self.data_dir, &room_id);
        let meta_room_path = room_dir.join(META_ROOM_FILE);

        if self.rooms.contains_key(&room_id) || blocking_io(|| durable::exists(&meta_room_path))? {
            return Err(ServerError::RoomAlreadyExists(room_id.to_string()));
        }

        let schema = self
            .schema_registry
            .get_schema(&schema_id)
            .ok_or_else(|| ServerError::SchemaNotFound(schema_id.to_string()))?;
        // Not even a room whose directory was lost, or whose deletion was cut short, may pass
        // its snapshot on to this one.
        self.snapshot_relay.purge_room(&room_id).await?;

        let meta = RoomMetadata {
            room_id: room_id.clone(),
            schema_id,
            room_incarnation: new_incarnation(),
            lifecycle,
        };
        blocking_io(|| write_room_metadata(&room_dir, &meta))?;
        self.room_meta.insert(room_id.clone(), meta.clone());

        // Read the schema again now that the room is listed for schema reloads: an evolution
        // whose reload did not list it happened before this read.
        let schema = self
            .schema_registry
            .get_schema(&meta.schema_id)
            .unwrap_or(schema);
        blocking_io(|| self.spawn_actor(&room_id, &meta, schema)).inspect_err(|_| {
            self.room_meta.remove(&room_id);
        })?;
        Ok(meta)
    }

    /// Deletes a room: shuts down its actor, renames its directory to a name no room can have,
    /// purges the room from the relay and removes the renamed directory. A room whose log is
    /// open in another process is `RoomLocked`, and nothing is deleted.
    ///
    /// The room is gone once the rename is durable, and failures afterwards are only logged. A
    /// purge or removal that fails or is cut short (a crash, or the future dropped) leaves the
    /// renamed directory, which the next start removes ([`new`](Self::new)), and possibly the
    /// room's snapshot in the relay, which no room offers and [`create_room`](Self::create_room)
    /// purges.
    pub async fn delete_room(&self, room_id: &RoomId) -> Result<(), ServerError> {
        let _guard = self.lock_room(room_id).await;

        self.shutdown_room(room_id).await;
        self.room_meta.remove(room_id);
        let room_dir = room_dir(&self.data_dir, room_id);
        let deleted = room_dir.with_file_name(format!("{DELETED_ROOM_PREFIX}{}", Uuid::new_v4()));
        // A log open elsewhere (another process) keeps the room. The lock is released before
        // the rename, which on Windows fails on a directory holding a locked file; meanwhile
        // the room lock keeps this process from reopening the log, and `server.lock` keeps
        // others off.
        let moved = blocking_io(|| {
            drop(crate::log::tiered_log::lock_log(&room_dir)?);
            fs::rename(&room_dir, &deleted)?;
            Ok::<_, ServerError>(durable::sync_parent(&room_dir)?)
        });
        // No room directory: reported once the relay is purged.
        let missing =
            matches!(&moved, Err(ServerError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound);
        if !missing {
            moved?;
        }
        // A room recreated with the same id must not inherit this room's snapshot.
        let purged = self.snapshot_relay.purge_room(room_id).await;
        if missing {
            purged?;
            return Err(ServerError::RoomNotFound(room_id.to_string()));
        }
        // The room is gone. What a failure leaves is removed at the next start, or purged when
        // a room with this id is created again.
        let removed = blocking_io(|| {
            fail_point::check("room_removal", &room_dir)?;
            Ok::<_, ServerError>(fs::remove_dir_all(&deleted)?)
        });
        for err in [purged.err(), removed.err()].into_iter().flatten() {
            warn!(room = %room_id, error = %err, "Failed to remove what a deleted room left");
        }
        Ok(())
    }

    /// Retained log range of a room, and the highest cursor from which it found a gap, spawning
    /// its actor if needed. A room that does not exist is `RoomNotFound`.
    pub async fn log_bounds(&self, room_id: &RoomId) -> Result<LogBounds, ServerError> {
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

    /// Sends the registry's current version of `schema_id` to every running room that uses
    /// it, and returns the rooms it was sent to.
    ///
    /// A room is listed first and checked again under its spawn lock: one that moved to
    /// another schema meanwhile, or was deleted and created again with one, is skipped, so a
    /// room never runs a schema other than its own (and its actor ignores a reload of another
    /// schema as well).
    ///
    /// Each room gets the version the registry holds when the command is sent, read under the
    /// room's spawn lock, not a version chosen by the caller. The registry only moves forward
    /// and a room's commands are applied in the order they are sent, so concurrent reloads
    /// (two column additions in a row) can arrive in any order without taking a room back to
    /// an older schema: whichever is sent last carries the newest version.
    pub async fn reload_schema_for_rooms(&self, schema_id: &SchemaId) -> Vec<RoomId> {
        fail_point::hook(
            "schema_reload_before_rooms",
            &self.schema_registry.schema_path(schema_id),
        );
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
                // The room was listed before its lock was taken: it may have moved to another
                // schema since, or been deleted and created again with one.
                let same_schema = self
                    .room_meta
                    .get(&room_id)
                    .is_some_and(|meta| &meta.schema_id == schema_id);
                if !same_schema {
                    continue;
                }
                let Some(sender) = self.get_room(&room_id) else {
                    continue;
                };
                let Some(schema) = self.schema_registry.get_schema(schema_id) else {
                    continue;
                };
                let (tx, rx) = oneshot::channel();
                let cmd = RoomCommand::ReloadSchema {
                    schema_id: schema_id.clone(),
                    schema,
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

/// Removes the directories that deleted rooms left in `rooms_dir` (see [`DELETED_ROOM_PREFIX`]).
fn remove_deleted_rooms(rooms_dir: &Path) -> std::io::Result<()> {
    let entries = match fs::read_dir(rooms_dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        entries => entries?,
    };
    for entry in entries {
        let path = entry?.path();
        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(DELETED_ROOM_PREFIX))
        {
            fs::remove_dir_all(path)?;
        }
    }
    Ok(())
}

/// A random incarnation for a new room.
fn new_incarnation() -> u128 {
    uuid::Uuid::new_v4().as_u128()
}

/// Atomically and durably writes a room's `meta_room.json`, creating its directory if needed.
fn write_room_metadata(room_dir: &Path, meta: &RoomMetadata) -> Result<(), ServerError> {
    durable::create_dir_all_synced(room_dir)?;
    let json = serde_json::to_string_pretty(meta).map_err(|e| {
        ServerError::Serialization(format!("Failed to serialize meta_room.json: {}", e))
    })?;
    durable::write_checked(&room_dir.join(META_ROOM_FILE), json.as_bytes())?;
    Ok(())
}

/// Reads a room's `meta_room.json`, discarding a temporary file left by an interrupted write.
/// An unreadable file, one whose checksum does not match, or one with invalid lifecycle
/// overrides, is an error: the room cannot be served without knowing its schema and policy.
fn read_room_metadata(meta_room_path: &Path) -> Result<RoomMetadata, ServerError> {
    durable::remove_file_if_exists(&durable::tmp_path_for(meta_room_path))?;
    let bytes = fs::read(meta_room_path)?;
    let unreadable = |e: &dyn std::fmt::Display| {
        ServerError::Serialization(format!("Failed to parse meta_room.json: {e}"))
    };
    let json = durable::verified_json(&bytes).ok_or_else(|| unreadable(&"checksum mismatch"))?;
    let meta: RoomMetadata = serde_json::from_str(json).map_err(|e| unreadable(&e))?;
    meta.lifecycle
        .check(|setting| format!("lifecycle.{}", setting.field()))
        .map_err(|e| ServerError::Serialization(format!("Invalid meta_room.json: {e}")))?;
    Ok(meta)
}

#[cfg(test)]
#[path = "tests/manager.rs"]
mod tests;
