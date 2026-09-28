use rimdb_server::{
    build_router, AppState, RoomManager, SchemaRegistry, ServerConfig, SnapshotRelay,
};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::info;

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
    )?);
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
    let app = build_router(state);

    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await?;

    info!(
        host = %config.host,
        port = config.port,
        data_dir = ?config.data_dir,
        "RimDB coordination server listening on http://{}",
        addr
    );

    println!("RimDB coordination server listening on http://{}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

