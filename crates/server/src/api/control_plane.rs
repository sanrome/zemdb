use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zemdb_core::id::{RoomId, SchemaId};
use zemdb_core::schema::{ColumnDef, Schema};

use crate::actor::command::{RoomCommand, RoomMetrics};
use crate::actor::manager::RoomMetadata;
use crate::api::auth::AdminAuth;
use crate::api::extract::{AdminJson, AdminPath};
use crate::api::router::AppState;
use crate::error::ServerError;
use crate::log::RoomLifecycleOverrides;

/// Request payload for declaring or registering a new schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSchemaRequest {
    pub schema_id: SchemaId,
    pub schema: Schema,
}

/// Request payload for DDL append-only column evolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddColumnRequest {
    pub table_name: String,
    pub column: ColumnDef,
}

/// Request payload for provisioning a new room.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRoomRequest {
    pub room_id: RoomId,
    pub schema_id: SchemaId,
    /// Lifecycle settings in which the room differs from the server defaults. Unknown fields
    /// and values out of range are `BadRequest`.
    #[serde(default)]
    pub lifecycle: Option<RoomLifecycleOverrides>,
}

/// Response of `GET /admin/rooms/:room_id`: the room's metrics, including the effective
/// lifecycle policy its actor runs with (`lifecycle`), plus the room's stored overrides.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomDetails {
    #[serde(flatten)]
    pub metrics: RoomMetrics,
    /// The settings in which the room differs from the server defaults.
    pub lifecycle_overrides: RoomLifecycleOverrides,
}

/// `POST /admin/schemas`: Declares and registers a new schema template.
pub async fn create_schema(
    _auth: AdminAuth,
    State(state): State<AppState>,
    AdminJson(req): AdminJson<CreateSchemaRequest>,
) -> Result<(StatusCode, Json<Schema>), ServerError> {
    let schema_arc = state
        .schema_registry
        .register_schema(req.schema_id, req.schema)?;
    Ok((StatusCode::CREATED, Json((*schema_arc).clone())))
}

/// `GET /admin/schemas/:schema_id`: Retrieves an existing schema definition.
pub async fn get_schema(
    _auth: AdminAuth,
    State(state): State<AppState>,
    AdminPath(sid): AdminPath<SchemaId>,
) -> Result<Json<Schema>, ServerError> {
    let schema = state
        .schema_registry
        .get_schema(&sid)
        .ok_or_else(|| ServerError::SchemaNotFound(sid.to_string()))?;
    Ok(Json((*schema).clone()))
}

/// `POST /admin/schemas/:schema_id/columns`: Evolves a schema by appending a nullable column,
/// automatically notifying all running room actors to reload the new schema in memory.
///
/// The reload runs in its own task: once the column is durable, every running room must get
/// the new schema even if this request is dropped while the rooms are being reloaded. The
/// handler still waits for it before answering.
pub async fn add_column(
    _auth: AdminAuth,
    State(state): State<AppState>,
    AdminPath(sid): AdminPath<SchemaId>,
    AdminJson(req): AdminJson<AddColumnRequest>,
) -> Result<Json<Schema>, ServerError> {
    let updated = state
        .schema_registry
        .add_column(&sid, &req.table_name, req.column)?;

    let room_manager = Arc::clone(&state.room_manager);
    let reload = tokio::spawn(async move { room_manager.reload_schema_for_rooms(&sid).await });
    // The column is durable, so the answer is a success either way; a reload task that failed
    // only leaves its rooms to read the new schema from the registry when they respawn.
    if let Err(err) = reload.await {
        tracing::warn!(error = %err, "Schema reload task failed after a column was added");
    }

    Ok(Json((*updated).clone()))
}

/// `POST /admin/rooms`: Explicitly provisions an isolated room.
pub async fn create_room(
    _auth: AdminAuth,
    State(state): State<AppState>,
    AdminJson(req): AdminJson<CreateRoomRequest>,
) -> Result<(StatusCode, Json<RoomMetadata>), ServerError> {
    let meta = state
        .room_manager
        .create_room(req.room_id, req.schema_id, req.lifecycle)
        .await?;
    Ok((StatusCode::CREATED, Json(meta)))
}

/// `GET /admin/rooms/:room_id`: Retrieves operational metrics and the lifecycle policy of a
/// room.
pub async fn get_room(
    _auth: AdminAuth,
    State(state): State<AppState>,
    AdminPath(rid): AdminPath<RoomId>,
) -> Result<Json<RoomDetails>, ServerError> {
    if !state.room_manager.room_exists(&rid) {
        return Err(ServerError::RoomNotFound(rid.to_string()));
    }
    let metrics = state
        .room_manager
        .ask(&rid, |reply| RoomCommand::GetMetrics { reply })
        .await??;
    // The actor is running, so the manager knows the room's metadata, unless the room was
    // deleted in the meantime.
    let lifecycle_overrides = state
        .room_manager
        .lifecycle_overrides(&rid)
        .ok_or_else(|| ServerError::RoomNotFound(rid.to_string()))?;
    Ok(Json(RoomDetails {
        metrics,
        lifecycle_overrides,
    }))
}

/// `DELETE /admin/rooms/:room_id`: Shuts down a room actor and purges its directory from disk.
pub async fn delete_room(
    _auth: AdminAuth,
    State(state): State<AppState>,
    AdminPath(rid): AdminPath<RoomId>,
) -> Result<StatusCode, ServerError> {
    state.room_manager.delete_room(&rid).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
#[path = "tests/control_plane.rs"]
mod tests;
