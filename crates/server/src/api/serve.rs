//! HTTP serving with graceful shutdown.

use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::api::router::{build_router, AppState};
use crate::error::ServerError;

/// Time allowed, once shutdown starts, for in-flight requests to finish and rooms to close.
pub const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(10);

/// Serves the API on `listener` until `signal` resolves or `state.shutdown` is triggered,
/// then shuts down in order:
///
/// 1. stops accepting connections and fires `state.shutdown`, which ends open SSE streams;
/// 2. waits for in-flight requests to complete;
/// 3. shuts down every room actor, which persists its clients roster.
///
/// If steps 2 and 3 take longer than `grace`, returns an error without waiting further; the
/// caller is expected to exit the process. Commits are durable before they are acknowledged,
/// so an abandoned shutdown only loses unflushed client cursors.
pub async fn serve_until_shutdown<F>(
    listener: TcpListener,
    state: AppState,
    signal: F,
    grace: Duration,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let shutdown = state.shutdown.clone();
    let room_manager = Arc::clone(&state.room_manager);
    let app = build_router(state);

    let trigger = shutdown.clone();
    let graceful = async move {
        tokio::select! {
            () = signal => {}
            () = trigger.wait() => {}
        }
        info!("Shutdown requested: no longer accepting connections");
        trigger.trigger();
    };

    let server = axum::serve(listener, app)
        .with_graceful_shutdown(graceful)
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        res = &mut server => {
            // The server stopped on its own (accept error); still close the rooms cleanly,
            // within the same grace period.
            if tokio::time::timeout(grace, room_manager.shutdown_all()).await.is_err() {
                warn!("Room shutdown did not finish within {:?}", grace);
            }
            return res.map_err(ServerError::from);
        }
        () = shutdown.wait() => {}
    }

    let drain = async {
        let served = server.await;
        info!("Connections drained; shutting down rooms");
        room_manager.shutdown_all().await;
        served
    };
    match tokio::time::timeout(grace, drain).await {
        Ok(served) => served.map_err(ServerError::from),
        Err(_) => Err(ServerError::Internal(format!(
            "Graceful shutdown did not finish within {:?}",
            grace
        ))),
    }
}
