//! Server state shared by the unit tests of the api modules.

use crate::api::router::AppState;
use crate::config::ServerConfig;
use crate::relay::SnapshotRelay;
use crate::schema_registry::SchemaRegistry;
use crate::RoomManager;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// The parts of the server state, built over `dir` the way the server builds them.
pub(super) struct TestState {
    pub(super) config: Arc<ServerConfig>,
    pub(super) registry: Arc<SchemaRegistry>,
    pub(super) relay: Arc<SnapshotRelay>,
    pub(super) manager: Arc<RoomManager>,
}

impl TestState {
    /// A state whose data directory is `dir` and whose configuration is adjusted by
    /// `configure`, with an empty registry.
    pub(super) fn new(dir: &Path, configure: impl FnOnce(&mut ServerConfig)) -> Self {
        let mut config = ServerConfig {
            data_dir: dir.to_path_buf(),
            ..ServerConfig::default()
        };
        configure(&mut config);
        let config = Arc::new(config);
        let registry = Arc::new(SchemaRegistry::new(dir.join("schemas")).unwrap());
        let relay = Arc::new(
            SnapshotRelay::new(
                dir.join("snapshots"),
                Duration::from_secs(60),
                config.max_snapshot_bytes,
                Duration::from_secs(config.snapshot_upload_max_secs),
            )
            .unwrap(),
        );
        let manager = Arc::new(RoomManager::new(
            Arc::clone(&config),
            Arc::clone(&registry),
            Arc::clone(&relay),
        ));
        Self {
            config,
            registry,
            relay,
            manager,
        }
    }

    pub(super) fn app_state(&self) -> AppState {
        AppState::new(
            Arc::clone(&self.config),
            Arc::clone(&self.registry),
            Arc::clone(&self.manager),
            Arc::clone(&self.relay),
        )
    }
}
