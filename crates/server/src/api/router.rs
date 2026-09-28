use std::sync::Arc;
use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, get, post};
use axum::Router;

use crate::actor::manager::RoomManager;
use crate::api::{control_plane, data_plane, sse};
use crate::config::ServerConfig;
use crate::relay::{self, SnapshotRelay};
use crate::schema_registry::SchemaRegistry;

/// Shared application state injected into Axum route handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServerConfig>,
    pub schema_registry: Arc<SchemaRegistry>,
    pub room_manager: Arc<RoomManager>,
    pub snapshot_relay: Arc<SnapshotRelay>,
}

impl AppState {
    /// Constructs a new AppState instance.
    pub fn new(
        config: Arc<ServerConfig>,
        schema_registry: Arc<SchemaRegistry>,
        room_manager: Arc<RoomManager>,
        snapshot_relay: Arc<SnapshotRelay>,
    ) -> Self {
        Self {
            config,
            schema_registry,
            room_manager,
            snapshot_relay,
        }
    }
}

/// Assembles the unified Axum router for Control Plane, Data Plane, SSE, and Snapshot Relay.
pub fn build_router(state: AppState) -> Router {
    let admin_routes = Router::new()
        .route("/schemas", post(control_plane::create_schema))
        .route("/schemas/:schema_id", get(control_plane::get_schema))
        .route(
            "/schemas/:schema_id/columns",
            post(control_plane::add_column),
        )
        .route("/rooms", post(control_plane::create_room))
        .route("/rooms/:room_id", get(control_plane::get_room))
        .route("/rooms/:room_id", delete(control_plane::delete_room));

    let data_routes = Router::new()
        .route("/rooms/:room_id/register", post(data_plane::register))
        .route("/rooms/:room_id/commit", post(data_plane::commit))
        .route("/rooms/:room_id/sync", post(data_plane::sync))
        .route("/rooms/:room_id/ack", post(data_plane::ack))
        .route("/rooms/:room_id/heartbeat", post(data_plane::heartbeat))
        .route("/rooms/:room_id/schema", post(data_plane::get_schema))
        .route("/rooms/:room_id/deregister", post(data_plane::deregister))
        .route("/rooms/:room_id/events", get(sse::room_events))
        .route(
            "/rooms/:room_id/snapshot/upload",
            post(relay::upload_snapshot),
        )
        .route(
            "/rooms/:room_id/snapshot/chunk",
            post(relay::request_chunk),
        );

    Router::new()
        .nest("/admin", admin_routes)
        .merge(data_routes)
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(state)
}
