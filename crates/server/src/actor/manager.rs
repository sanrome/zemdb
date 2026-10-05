use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};
use zemdb_core::id::{RoomId, SchemaId, SequenceNumber};

use crate::actor::command::RoomCommand;
use crate::actor::room::RoomActor;
use crate::config::ServerConfig;
use crate::durable;
use crate::error::ServerError;
use crate::log::RoomLifecyclePolicy;
use crate::relay::SnapshotRelay;
use crate::schema_registry::SchemaRegistry;

/// File, inside the room directory, recording the room's schema assignment.
const META_ROOM_FILE: &str = "meta_room.json";

/// Interval between checks while waiting for a stopped room actor to finish exiting.
const PREVIOUS_ACTOR_POLL: Duration = Duration::from_millis(5);

/// Persistent room configuration linking a RoomId to its assigned SchemaId.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomMetadata {
    pub room_id: RoomId,
    pub schema_id: SchemaId,
}

/// Sharded room actor manager providing lazy spawning and routing of commands to room actors.
#[derive(Debug)]
pub struct RoomManager {
    rooms: DashMap<RoomId, mpsc::Sender<RoomCommand>>,
    room_schemas: DashMap<RoomId, SchemaId>,
    spawn_locks: DashMap<RoomId, Arc<tokio::sync::Mutex<()>>>,
    room_handles: DashMap<RoomId, tokio::task::JoinHandle<()>>,
    config: Arc<ServerConfig>,
    schema_registry: Arc<SchemaRegistry>,
    snapshot_relay: Arc<SnapshotRelay>,
    data_dir: PathBuf,
}

impl RoomManager {
    /// Creates a new RoomManager with the given configuration, schema registry, and snapshot relay.
    pub fn new(
        config: Arc<ServerConfig>,
        schema_registry: Arc<SchemaRegistry>,
        snapshot_relay: Arc<SnapshotRelay>,
    ) -> Self {
        let data_dir = config.data_dir.clone();
        Self {
            rooms: DashMap::new(),
            room_schemas: DashMap::new(),
            spawn_locks: DashMap::new(),
            room_handles: DashMap::new(),
            config,
            schema_registry,
            snapshot_relay,
            data_dir,
        }
    }

    /// Retrieves an existing room actor sender or lazily spawns a new one with default lifecycle policy.
    pub async fn get_or_spawn(
        &self,
        room_id: &RoomId,
        schema_id: Option<&SchemaId>,
    ) -> Result<mpsc::Sender<RoomCommand>, ServerError> {
        self.get_or_spawn_with_policy(room_id, schema_id, RoomLifecyclePolicy::default())
            .await
    }

    /// Retrieves an existing room actor sender or lazily spawns a new one with a custom lifecycle policy.
    pub async fn get_or_spawn_with_policy(
        &self,
        room_id: &RoomId,
        schema_id: Option<&SchemaId>,
        lifecycle_policy: RoomLifecyclePolicy,
    ) -> Result<mpsc::Sender<RoomCommand>, ServerError> {
        // 1. Fast check if room is already active and channel is open
        if let Some(sender) = self.rooms.get(room_id) {
            if !sender.is_closed() {
                return Ok(sender.clone());
            }
        }

        // 2. Acquire per-room spawn lock to prevent duplicate instantiation
        let lock = self
            .spawn_locks
            .entry(room_id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        // 3. Double-check if another task spawned the room while waiting for the lock
        if let Some(sender) = self.rooms.get(room_id) {
            if !sender.is_closed() {
                return Ok(sender.clone());
            }
        }

        // 3b. A previous actor for this room stopped on its own (for example after a failed
        // log write). Wait until it has fully exited and released the room's files before
        // recovering the room from disk.
        // The handle stays in the map while waiting, so a request abandoned at this point
        // leaves the next one still waiting for the same actor.
        self.wait_for_previous_actor(room_id).await;

        // 4. Resolve SchemaId
        let room_dir = self.data_dir.join("rooms").join(room_id.as_str());
        let meta_room_path = room_dir.join(META_ROOM_FILE);

        let resolved_schema_id = if let Some(sid) = schema_id {
            // Save or update meta_room.json
            write_room_metadata(&room_dir, room_id, sid)?;
            self.room_schemas.insert(room_id.clone(), sid.clone());
            sid.clone()
        } else if let Some(sid) = self.room_schemas.get(room_id) {
            sid.clone()
        } else if meta_room_path.exists() {
            let meta = read_room_metadata(&meta_room_path)?;
            self.room_schemas
                .insert(room_id.clone(), meta.schema_id.clone());
            meta.schema_id
        } else {
            return Err(ServerError::RoomNotFound(format!(
                "No schema assigned or directory found for room '{}'",
                room_id
            )));
        };

        // 5. Resolve Schema definition from registry
        let schema = self
            .schema_registry
            .get_schema(&resolved_schema_id)
            .ok_or_else(|| ServerError::SchemaNotFound(resolved_schema_id.to_string()))?;

        // 6. Spawn RoomActor and retain handle
        let (sender, handle) = RoomActor::spawn(
            room_id.clone(),
            resolved_schema_id,
            schema,
            &self.data_dir,
            Arc::clone(&self.config),
            lifecycle_policy,
            Arc::clone(&self.snapshot_relay),
        )?;

        self.rooms.insert(room_id.clone(), sender.clone());
        self.room_handles.insert(room_id.clone(), handle);
        info!(room = %room_id, "RoomActor lazily initialized and registered in RoomManager");

        Ok(sender)
    }

    /// Waits until the previous actor task of `room_id`, if any, has finished, then forgets it.
    ///
    /// Polls `is_finished` instead of awaiting the handle so that the handle is only removed
    /// once the task is done, keeping this cancel-safe; map guards are never held across an
    /// `.await`.
    async fn wait_for_previous_actor(&self, room_id: &RoomId) {
        loop {
            let finished = self.room_handles.get(room_id).map(|h| h.is_finished());
            match finished {
                None => return,
                Some(true) => {
                    if let Some((_, previous)) = self.room_handles.remove(room_id) {
                        if let Err(err) = previous.await {
                            warn!(room = %room_id, error = %err, "Previous RoomActor task ended abnormally");
                        }
                    }
                    return;
                }
                Some(false) => tokio::time::sleep(PREVIOUS_ACTOR_POLL).await,
            }
        }
    }

    /// Fast check if a room exists in memory or on disk.
    pub fn room_exists(&self, room_id: &RoomId) -> bool {
        if self.rooms.contains_key(room_id) || self.room_schemas.contains_key(room_id) {
            return true;
        }
        let meta_room_path = self
            .data_dir
            .join("rooms")
            .join(room_id.as_str())
            .join(META_ROOM_FILE);
        meta_room_path.exists()
    }

    /// Gets an existing active sender for the room, if running.
    pub fn get_room(&self, room_id: &RoomId) -> Option<mpsc::Sender<RoomCommand>> {
        self.rooms
            .get(room_id)
            .and_then(|s| if s.is_closed() { None } else { Some(s.clone()) })
    }

    /// Gracefully closes and shuts down an active room actor, awaiting task termination.
    pub async fn shutdown_room(&self, room_id: &RoomId) -> bool {
        let sender = self.rooms.remove(room_id);
        let handle = self.room_handles.remove(room_id);
        if let Some((_, sender)) = sender {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender
                .send(RoomCommand::Shutdown { reply: tx })
                .await
                .is_ok()
            {
                let _ = rx.await;
            }
            if let Some((_, handle)) = handle {
                let _ = handle.await;
            }
            true
        } else {
            false
        }
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
            .filter(|kv| !kv.value().is_closed())
            .map(|kv| kv.key().clone())
            .collect()
    }

    /// Explicitly creates and provisions a new room. Returns error if room already exists.
    pub async fn create_room(
        &self,
        room_id: RoomId,
        schema_id: SchemaId,
        lifecycle_policy: Option<RoomLifecyclePolicy>,
    ) -> Result<RoomMetadata, ServerError> {
        let lock = self
            .spawn_locks
            .entry(room_id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        let room_dir = self.data_dir.join("rooms").join(room_id.as_str());
        let meta_room_path = room_dir.join(META_ROOM_FILE);

        if meta_room_path.exists() || self.rooms.contains_key(&room_id) {
            return Err(ServerError::RoomAlreadyExists(room_id.to_string()));
        }

        // Validate schema exists in SchemaRegistry
        let schema = self
            .schema_registry
            .get_schema(&schema_id)
            .ok_or_else(|| ServerError::SchemaNotFound(schema_id.to_string()))?;

        let meta = write_room_metadata(&room_dir, &room_id, &schema_id)?;
        self.room_schemas.insert(room_id.clone(), schema_id.clone());

        // Spawn actor and retain handle
        let (sender, handle) = RoomActor::spawn(
            room_id.clone(),
            schema_id,
            schema,
            &self.data_dir,
            Arc::clone(&self.config),
            lifecycle_policy.unwrap_or_default(),
            Arc::clone(&self.snapshot_relay),
        )?;

        self.rooms.insert(room_id.clone(), sender);
        self.room_handles.insert(room_id, handle);
        Ok(meta)
    }

    /// Deletes a room: gracefully shuts down the active actor and purges the room directory from disk.
    pub async fn delete_room(&self, room_id: &RoomId) -> Result<(), ServerError> {
        let lock = self
            .spawn_locks
            .entry(room_id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        self.shutdown_room(room_id).await;
        self.room_schemas.remove(room_id);
        // A room recreated with the same id must not inherit this room's snapshot.
        self.snapshot_relay.purge_room(room_id).await?;

        let room_dir = self.data_dir.join("rooms").join(room_id.as_str());
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
        let sender = self.get_or_spawn(room_id, None).await?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        sender
            .send(RoomCommand::GetLogBounds { reply: tx })
            .await
            .map_err(|_| ServerError::Internal(format!("Room {room_id} actor stopped")))?;
        rx.await
            .map_err(|_| ServerError::Internal(format!("Room {room_id} actor stopped")))
    }

    /// Reloads the schema across all active rooms associated with `schema_id`.
    pub async fn reload_schema_for_rooms(
        &self,
        schema_id: &SchemaId,
        schema: Arc<zemdb_core::schema::Schema>,
    ) -> Vec<RoomId> {
        let mut reloaded = Vec::new();
        for kv in self.room_schemas.iter() {
            if kv.value() == schema_id {
                let room_id = kv.key().clone();
                if let Some(sender) = self.get_room(&room_id) {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let cmd = RoomCommand::ReloadSchema {
                        schema: Arc::clone(&schema),
                        reply: tx,
                    };
                    if sender.send(cmd).await.is_ok() {
                        let _ = rx.await;
                        reloaded.push(room_id);
                    }
                }
            }
        }
        reloaded
    }
}

/// Atomically and durably writes `meta_room.json` for a room, creating its directory if needed.
fn write_room_metadata(
    room_dir: &Path,
    room_id: &RoomId,
    schema_id: &SchemaId,
) -> Result<RoomMetadata, ServerError> {
    durable::create_dir_all_synced(room_dir)?;
    let meta = RoomMetadata {
        room_id: room_id.clone(),
        schema_id: schema_id.clone(),
    };
    let json = serde_json::to_string_pretty(&meta).map_err(|e| {
        ServerError::Serialization(format!("Failed to serialize meta_room.json: {}", e))
    })?;
    durable::write_atomic(&room_dir.join(META_ROOM_FILE), json.as_bytes())?;
    Ok(meta)
}

/// Reads a room's `meta_room.json`, discarding a temporary file left by an interrupted write.
/// An unreadable file is an error: the room cannot be served without knowing its schema.
fn read_room_metadata(meta_room_path: &Path) -> Result<RoomMetadata, ServerError> {
    match fs::remove_file(durable::tmp_path_for(meta_room_path)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let content = fs::read_to_string(meta_room_path)?;
    serde_json::from_str(&content)
        .map_err(|e| ServerError::Serialization(format!("Failed to parse meta_room.json: {}", e)))
}

#[cfg(test)]
#[path = "tests/manager.rs"]
mod tests;
