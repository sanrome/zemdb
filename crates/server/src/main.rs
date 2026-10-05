use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use zemdb_server::{
    serve_until_shutdown, AppState, RoomManager, SchemaRegistry, ServerConfig, SnapshotRelay,
    SHUTDOWN_GRACE_PERIOD,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config_path = env::args().nth(1);
    let config = Arc::new(ServerConfig::load_with_env(config_path.as_deref())?);

    let schemas_dir = config.data_dir.join("schemas");
    let schema_registry = Arc::new(SchemaRegistry::new(schemas_dir)?);
    let snapshots_dir = config.data_dir.join("snapshots");
    let snapshot_relay = Arc::new(SnapshotRelay::new(
        snapshots_dir,
        Duration::from_secs(config.snapshot_ttl_secs),
        config.max_snapshot_bytes,
    )?);
    snapshot_relay.spawn_expiry_sweeper();
    let room_manager = Arc::new(RoomManager::new(
        Arc::clone(&config),
        Arc::clone(&schema_registry),
        Arc::clone(&snapshot_relay),
    ));

    let state = AppState::new(
        Arc::clone(&config),
        schema_registry,
        room_manager,
        snapshot_relay,
    );

    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await?;

    info!(
        host = %config.host,
        port = config.port,
        data_dir = ?config.data_dir,
        "ZemDB coordination server listening on http://{}",
        addr
    );

    println!("ZemDB coordination server listening on http://{}", addr);

    if let Err(err) =
        serve_until_shutdown(listener, state, os_shutdown_signal(), SHUTDOWN_GRACE_PERIOD).await
    {
        // Either the server failed or the shutdown exceeded its grace period. Exit right away
        // instead of letting the runtime wait for tasks that may never finish.
        error!(error = %err, "Server stopped abnormally; forcing exit");
        std::process::exit(1);
    }

    info!("ZemDB coordination server stopped");
    Ok(())
}

/// Resolves on SIGINT or SIGTERM on Unix; on Windows on Ctrl+C, Ctrl+Close (console window
/// closed) or Ctrl+Shutdown (system shutdown).
async fn os_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    () = ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(err) => {
                warn!(error = %err, "Cannot listen for SIGTERM; only SIGINT triggers shutdown");
                ctrl_c().await;
            }
        }
    }
    #[cfg(windows)]
    {
        use tokio::signal::windows::{ctrl_close, ctrl_shutdown};
        match (ctrl_close(), ctrl_shutdown()) {
            (Ok(mut close), Ok(mut shutdown)) => {
                tokio::select! {
                    () = ctrl_c() => {}
                    _ = close.recv() => {}
                    _ = shutdown.recv() => {}
                }
            }
            _ => {
                warn!("Cannot listen for console close or system shutdown; only Ctrl+C triggers shutdown");
                ctrl_c().await;
            }
        }
    }
    #[cfg(not(any(unix, windows)))]
    ctrl_c().await;
}

/// Resolves on Ctrl+C. If the handler cannot be installed it never resolves, so that the
/// failure does not shut the server down immediately.
async fn ctrl_c() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        warn!(error = %err, "Cannot listen for Ctrl+C");
        std::future::pending::<()>().await;
    }
}
