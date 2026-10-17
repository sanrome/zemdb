//! Fixtures shared by the unit tests of the actor modules.

use crate::actor::manager::RoomManager;
use crate::actor::room::ActorTimings;
use crate::config::ServerConfig;
use crate::relay::SnapshotRelay;
use crate::schema_registry::SchemaRegistry;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use zemdb_core::id::{SchemaId, SequenceNumber};
use zemdb_core::schema::{Schema, TableSchema};
use zemdb_core::value::DataType;

/// Longest a test waits for a condition that depends on real time.
pub(super) const POLL_CEILING: Duration = Duration::from_secs(10);

/// Timings of the rooms under test: both maintenance ticks every 500 ms, so that tests waiting
/// in real time for a tick stay fast.
pub(super) const TEST_TIMINGS: ActorTimings = ActorTimings {
    designation_timeout: crate::actor::snapshot_demand::DESIGNATION_TIMEOUT,
    fast_tick: Duration::from_millis(500),
    slow_tick: Duration::from_millis(500),
};

pub(super) fn seq(n: u64) -> SequenceNumber {
    SequenceNumber::new(n)
}

pub(super) fn test_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    Schema::builder().table(table).build()
}

/// A manager on `dir` with [`TEST_TIMINGS`], whose registry holds `test_schema` under each of
/// `schemas` (registered only if missing, so that a manager reopened on the same directory
/// finds them already there), and whose configuration is adjusted by `configure`.
pub(super) fn test_manager(
    dir: &Path,
    schemas: &[&str],
    configure: impl FnOnce(&mut ServerConfig),
) -> (RoomManager, Arc<SnapshotRelay>) {
    let mut config = ServerConfig {
        data_dir: dir.to_path_buf(),
        ..ServerConfig::default()
    };
    configure(&mut config);
    let registry = Arc::new(SchemaRegistry::new(dir.join("schemas")).unwrap());
    for id in schemas {
        let id = SchemaId::new(*id).unwrap();
        if registry.get_schema(&id).is_none() {
            registry.register_schema(id, test_schema()).unwrap();
        }
    }
    let relay = Arc::new(
        SnapshotRelay::new(
            dir.join("snapshots"),
            Duration::from_secs(60),
            ServerConfig::default().max_snapshot_bytes,
            Duration::from_secs(ServerConfig::default().snapshot_upload_max_secs),
        )
        .unwrap(),
    );
    let mut manager = RoomManager::new(Arc::new(config), registry, Arc::clone(&relay));
    manager.set_timings(TEST_TIMINGS);
    (manager, relay)
}

/// Polls `condition` every 20 ms until it holds, failing the test after [`POLL_CEILING`].
pub(super) async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + POLL_CEILING;
    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// [`wait_until`] for a condition that has to ask the room, polled every 50 ms.
pub(super) async fn wait_until_async<F: Future<Output = bool>>(
    what: &str,
    mut condition: impl FnMut() -> F,
) {
    let deadline = tokio::time::Instant::now() + POLL_CEILING;
    while !condition().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
