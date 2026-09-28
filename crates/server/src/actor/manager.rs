use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use dashmap::DashMap;
use rimdb_core::id::{RoomId, SchemaId};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::info;

use crate::actor::command::RoomCommand;
use crate::actor::room::RoomActor;
use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::log::RoomLifecyclePolicy;
use crate::schema_registry::SchemaRegistry;

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
    config: Arc<ServerConfig>,
    schema_registry: Arc<SchemaRegistry>,
    data_dir: PathBuf,
}

impl RoomManager {
    /// Creates a new RoomManager with the given configuration and schema registry.
    pub fn new(config: Arc<ServerConfig>, schema_registry: Arc<SchemaRegistry>) -> Self {
        let data_dir = config.data_dir.clone();
        Self {
            rooms: DashMap::new(),
            room_schemas: DashMap::new(),
            config,
            schema_registry,
            data_dir,
        }
    }

    /// Retrieves an existing room actor sender or lazily spawns a new one with default lifecycle policy.
    pub fn get_or_spawn(
        &self,
        room_id: &RoomId,
        schema_id: Option<&SchemaId>,
    ) -> Result<mpsc::Sender<RoomCommand>, ServerError> {
        self.get_or_spawn_with_policy(room_id, schema_id, RoomLifecyclePolicy::default())
    }

    /// Retrieves an existing room actor sender or lazily spawns a new one with a custom lifecycle policy.
    pub fn get_or_spawn_with_policy(
        &self,
        room_id: &RoomId,
        schema_id: Option<&SchemaId>,
        lifecycle_policy: RoomLifecyclePolicy,
    ) -> Result<mpsc::Sender<RoomCommand>, ServerError> {
        // 1. Check if room is already active and channel is open
        if let Some(sender) = self.rooms.get(room_id) {
            if !sender.is_closed() {
                return Ok(sender.clone());
            }
        }

        // 2. Resolve SchemaId
        let room_dir = self.data_dir.join("rooms").join(room_id.as_str());
        let meta_room_path = room_dir.join("meta_room.json");

        let resolved_schema_id = if let Some(sid) = schema_id {
            // Save or update meta_room.json
            fs::create_dir_all(&room_dir)?;
            let meta = RoomMetadata {
                room_id: room_id.clone(),
                schema_id: sid.clone(),
            };
            let json = serde_json::to_string_pretty(&meta).map_err(|e| {
                ServerError::Serialization(format!("Failed to serialize meta_room.json: {}", e))
            })?;
            fs::write(&meta_room_path, json.as_bytes())?;
            self.room_schemas.insert(room_id.clone(), sid.clone());
            sid.clone()
        } else if let Some(sid) = self.room_schemas.get(room_id) {
            sid.clone()
        } else if meta_room_path.exists() {
            let content = fs::read_to_string(&meta_room_path)?;
            let meta: RoomMetadata = serde_json::from_str(&content).map_err(|e| {
                ServerError::Serialization(format!("Failed to parse meta_room.json: {}", e))
            })?;
            self.room_schemas.insert(room_id.clone(), meta.schema_id.clone());
            meta.schema_id
        } else {
            return Err(ServerError::SchemaNotFound(format!(
                "No schema assigned for room '{}'",
                room_id
            )));
        };

        // 3. Resolve Schema definition from registry
        let schema = self
            .schema_registry
            .get_schema(&resolved_schema_id)
            .ok_or_else(|| ServerError::SchemaNotFound(resolved_schema_id.to_string()))?;

        // 4. Spawn RoomActor
        let (sender, _handle) = RoomActor::spawn(
            room_id.clone(),
            resolved_schema_id,
            schema,
            &self.data_dir,
            Arc::clone(&self.config),
            lifecycle_policy,
        )?;

        self.rooms.insert(room_id.clone(), sender.clone());
        info!(room = %room_id, "RoomActor lazily initialized and registered in RoomManager");

        Ok(sender)
    }

    /// Gets an existing active sender for the room, if running.
    pub fn get_room(&self, room_id: &RoomId) -> Option<mpsc::Sender<RoomCommand>> {
        self.rooms.get(room_id).and_then(|s| {
            if s.is_closed() {
                None
            } else {
                Some(s.clone())
            }
        })
    }

    /// Closes a room by dropping its sender from the registry.
    pub fn close_room(&self, room_id: &RoomId) -> bool {
        self.rooms.remove(room_id).is_some()
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
    pub fn create_room(
        &self,
        room_id: RoomId,
        schema_id: SchemaId,
        lifecycle_policy: Option<RoomLifecyclePolicy>,
    ) -> Result<RoomMetadata, ServerError> {
        let room_dir = self.data_dir.join("rooms").join(room_id.as_str());
        let meta_room_path = room_dir.join("meta_room.json");

        if meta_room_path.exists() || self.rooms.contains_key(&room_id) {
            return Err(ServerError::RoomAlreadyExists(room_id.to_string()));
        }

        // Validate schema exists in SchemaRegistry
        let schema = self
            .schema_registry
            .get_schema(&schema_id)
            .ok_or_else(|| ServerError::SchemaNotFound(schema_id.to_string()))?;

        fs::create_dir_all(&room_dir)?;
        let meta = RoomMetadata {
            room_id: room_id.clone(),
            schema_id: schema_id.clone(),
        };
        let json = serde_json::to_string_pretty(&meta).map_err(|e| {
            ServerError::Serialization(format!("Failed to serialize meta_room.json: {}", e))
        })?;
        fs::write(&meta_room_path, json.as_bytes())?;
        self.room_schemas.insert(room_id.clone(), schema_id.clone());

        // Spawn actor
        let (sender, _handle) = RoomActor::spawn(
            room_id.clone(),
            schema_id,
            schema,
            &self.data_dir,
            Arc::clone(&self.config),
            lifecycle_policy.unwrap_or_default(),
        )?;

        self.rooms.insert(room_id, sender);
        Ok(meta)
    }

    /// Deletes a room: closes the active actor and purges the room directory from disk.
    pub fn delete_room(&self, room_id: &RoomId) -> Result<(), ServerError> {
        self.close_room(room_id);
        self.room_schemas.remove(room_id);

        let room_dir = self.data_dir.join("rooms").join(room_id.as_str());
        if room_dir.exists() {
            fs::remove_dir_all(&room_dir)?;
            Ok(())
        } else {
            Err(ServerError::RoomNotFound(room_id.to_string()))
        }
    }

    /// Reloads the schema across all active rooms associated with `schema_id`.
    pub async fn reload_schema_for_rooms(
        &self,
        schema_id: &SchemaId,
        schema: Arc<rimdb_core::schema::Schema>,
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

