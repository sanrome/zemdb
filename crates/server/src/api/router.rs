use axum::extract::{DefaultBodyLimit, Request};
use axum::http::{StatusCode, Uri};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use zemdb_core::protocol::codec::MAX_FRAME_SIZE;
use zemdb_core::protocol::messages::ServerMessage;

use crate::actor::manager::RoomManager;
use crate::api::body_timeout::{limit_body_read_time, BodyReadLimit};
use crate::api::data_plane::binary_response;
use crate::api::{control_plane, data_plane, relay, sse};
use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::relay::SnapshotRelay;
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

/// Body size limit of `POST /rooms/:room_id/register` (64 KiB). A `RegisterClient` frame
/// takes a few hundred bytes, and the endpoint is not authenticated before its body is read,
/// so it does not get the general limit of `MAX_FRAME_SIZE`: a large body sent quickly and
/// then held back short of its end would keep its buffer for as long as the minimum body rate
/// allows. A larger body is a binary `BadRequest` frame with HTTP 413, as for the general limit.
pub const MAX_REGISTER_BODY_SIZE: usize = 64 * 1024;

/// Assembles the unified Axum router for Control Plane, Data Plane, SSE, and Snapshot Relay.
///
/// Request bodies are limited to `MAX_FRAME_SIZE` bytes ([`MAX_REGISTER_BODY_SIZE`] for
/// registration) and must arrive in full within the
/// configured `body_read_timeout_secs`, extended by one second per
/// `body_min_rate_bytes_per_sec` bytes received (otherwise 408).
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
        .route(
            "/rooms/:room_id/register",
            post(data_plane::register).layer(DefaultBodyLimit::max(MAX_REGISTER_BODY_SIZE)),
        )
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
        )
        .method_not_allowed_fallback(|uri: Uri| async move {
            unmatched(StatusCode::METHOD_NOT_ALLOWED, &uri)
        });

    let body_read_limit = BodyReadLimit {
        base: Duration::from_secs(state.config.body_read_timeout_secs),
        min_rate: state.config.body_min_rate_bytes_per_sec,
    };
    Router::new()
        .nest("/admin", admin_routes)
        .merge(data_routes)
        .fallback(|uri: Uri| async move { unmatched(StatusCode::NOT_FOUND, &uri) })
        // Exactly the largest frame the codec produces. Single-request snapshot uploads, whose
        // body is a raw snapshot, share the same limit.
        .layer(DefaultBodyLimit::max(MAX_FRAME_SIZE))
        // A body that arrives too slowly would hold its request (and up to the size limit of
        // memory) indefinitely.
        .layer(middleware::map_request(
            move |request: Request| async move { limit_body_read_time(request, body_read_limit) },
        ))
        .with_state(state)
}

/// Answer to a request no route handles (unknown path, or a method the path does not
/// accept). Under `/rooms/` it is a binary `BadRequest` frame with the given status, since
/// every Data Plane response is a frame; elsewhere it is the bare status.
fn unmatched(status: StatusCode, uri: &Uri) -> Response {
    if !uri.path().starts_with("/rooms/") {
        return status.into_response();
    }
    let err = ServerError::BadRequest(if status == StatusCode::METHOD_NOT_ALLOWED {
        format!("Method not allowed for {}", uri.path())
    } else {
        format!("Unknown endpoint {}", uri.path())
    });
    binary_response(
        status,
        &ServerMessage::Error {
            correlation_id: None,
            room_id: None,
            code: err.to_error_code(),
            message: err.to_string(),
        },
    )
}
