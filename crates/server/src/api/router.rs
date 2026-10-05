use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, get, post};
use axum::Router;
use std::sync::Arc;
use tokio::sync::watch;

use crate::actor::manager::RoomManager;
use crate::api::{control_plane, data_plane, sse};
use crate::config::ServerConfig;
use crate::relay::{self, SnapshotRelay};
use crate::schema_registry::SchemaRegistry;

/// Server-wide shutdown notification shared by every handler.
///
/// Once triggered it stays triggered. Long-lived responses (SSE streams) end when it fires,
/// so that a graceful shutdown does not wait for them forever.
#[derive(Debug, Clone)]
pub struct ShutdownSignal {
    tx: Arc<watch::Sender<bool>>,
}

impl ShutdownSignal {
    /// Creates a signal that has not fired yet.
    pub fn new() -> Self {
        let (tx, _) = watch::channel(false);
        Self { tx: Arc::new(tx) }
    }

    /// Fires the signal. Idempotent.
    pub fn trigger(&self) {
        self.tx.send_replace(true);
    }

    /// Resolves once the signal has fired (immediately if it already has).
    pub async fn wait(&self) {
        let mut rx = self.tx.subscribe();
        // The sender lives as long as `self`, so waiting can only end by the signal firing.
        rx.wait_for(|triggered| *triggered).await.ok();
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared application state injected into Axum route handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServerConfig>,
    pub schema_registry: Arc<SchemaRegistry>,
    pub room_manager: Arc<RoomManager>,
    pub snapshot_relay: Arc<SnapshotRelay>,
    pub shutdown: ShutdownSignal,
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
            shutdown: ShutdownSignal::new(),
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
        .route("/rooms/:room_id/snapshot/chunk", post(relay::request_chunk))
        .route(
            "/rooms/:room_id/snapshot/upload-chunk",
            post(relay::upload_chunk),
        );

    Router::new()
        .nest("/admin", admin_routes)
        .merge(data_routes)
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(state)
}
