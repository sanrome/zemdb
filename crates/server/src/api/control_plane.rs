use std::sync::Arc;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use rimdb_core::id::{RoomId, SchemaId};
use rimdb_core::schema::{ColumnDef, Schema};
use serde::{Deserialize, Serialize};

use crate::actor::command::{RoomCommand, RoomMetrics};
use crate::actor::manager::RoomMetadata;
use crate::api::auth::AdminAuth;
use crate::api::router::AppState;
use crate::error::ServerError;
use crate::log::RoomLifecyclePolicy;

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
    #[serde(default)]
    pub lifecycle: Option<RoomLifecyclePolicy>,
}

/// `POST /admin/schemas`: Declares and registers a new schema template.
pub async fn create_schema(
    _auth: AdminAuth,
    State(state): State<AppState>,
    Json(req): Json<CreateSchemaRequest>,
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
    Path(schema_id): Path<String>,
) -> Result<Json<Schema>, ServerError> {
    let sid = SchemaId::new(schema_id);
    let schema = state
        .schema_registry
        .get_schema(&sid)
        .ok_or_else(|| ServerError::SchemaNotFound(sid.to_string()))?;
    Ok(Json((*schema).clone()))
}

/// `POST /admin/schemas/:schema_id/columns`: Evolves a schema by appending a nullable column,
/// automatically notifying all running room actors to reload the new schema in memory.
pub async fn add_column(
    _auth: AdminAuth,
    State(state): State<AppState>,
    Path(schema_id): Path<String>,
    Json(req): Json<AddColumnRequest>,
) -> Result<Json<Schema>, ServerError> {
    let sid = SchemaId::new(schema_id);
    let updated = state
        .schema_registry
        .add_column(&sid, &req.table_name, req.column)?;

    // Propagate schema evolution in hot memory across active rooms
    state
        .room_manager
        .reload_schema_for_rooms(&sid, Arc::clone(&updated))
        .await;

    Ok(Json((*updated).clone()))
}

/// `POST /admin/rooms`: Explicitly provisions an isolated room.
pub async fn create_room(
    _auth: AdminAuth,
    State(state): State<AppState>,
    Json(req): Json<CreateRoomRequest>,
) -> Result<(StatusCode, Json<RoomMetadata>), ServerError> {
    let meta = state
        .room_manager
        .create_room(req.room_id, req.schema_id, req.lifecycle)
        .await?;
    Ok((StatusCode::CREATED, Json(meta)))
}

/// `GET /admin/rooms/:room_id`: Retrieves operational metrics for a room.
pub async fn get_room(
    _auth: AdminAuth,
    State(state): State<AppState>,
    Path(room_id): Path<String>,
) -> Result<Json<RoomMetrics>, ServerError> {
    let rid = RoomId::new(room_id);
    let sender = state.room_manager.get_or_spawn(&rid, None).await?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    sender
        .send(RoomCommand::GetMetrics { reply: tx })
        .await
        .map_err(|_| ServerError::Internal("Room actor channel closed".to_string()))?;

    let metrics = rx
        .await
        .map_err(|_| ServerError::Internal("No response from room actor".to_string()))?;
    Ok(Json(metrics))
}

/// `DELETE /admin/rooms/:room_id`: Shuts down a room actor and purges its directory from disk.
pub async fn delete_room(
    _auth: AdminAuth,
    State(state): State<AppState>,
    Path(room_id): Path<String>,
) -> Result<StatusCode, ServerError> {
    let rid = RoomId::new(room_id);
    state.room_manager.delete_room(&rid).await?;
    Ok(StatusCode::NO_CONTENT)
}
