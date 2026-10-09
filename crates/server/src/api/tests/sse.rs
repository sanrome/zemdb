use super::*;
use crate::config::ServerConfig;
use crate::relay::SnapshotRelay;
use crate::schema_registry::SchemaRegistry;
use crate::RoomManager;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc;

/// App state whose room `room` is played by a task that refuses the first `refusals`
/// subscriptions with `Unavailable`, as an actor shutting down for inactivity does, and
/// accepts the next ones.
fn state_with_refusing_room(dir: &TempDir, refusals: usize) -> (AppState, RoomId) {
    let config = Arc::new(ServerConfig {
        data_dir: dir.path().to_path_buf(),
        ..ServerConfig::default()
    });
    let registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    let relay = Arc::new(
        SnapshotRelay::new(
            dir.path().join("snapshots"),
            Duration::from_secs(60),
            config.max_snapshot_bytes,
        )
        .unwrap(),
    );
    let manager = Arc::new(RoomManager::new(
        Arc::clone(&config),
        Arc::clone(&registry),
        Arc::clone(&relay),
    ));
    let room_id = RoomId::new("room").unwrap();
    let (sender, mut receiver) = mpsc::channel(8);
    let exit = manager.install_test_actor(&room_id, sender);
    tokio::spawn(async move {
        let _exit = exit;
        let (events, _) = broadcast::channel(8);
        let mut refused = 0;
        while let Some(command) = receiver.recv().await {
            if let RoomCommand::SubscribeEvents { reply } = command {
                if refused < refusals {
                    refused += 1;
                    let _ = reply.send(Err(ServerError::Unavailable("stopping".to_string())));
                } else {
                    let _ = reply.send(Ok(events.subscribe()));
                }
            }
        }
    });
    (AppState::new(config, registry, manager, relay), room_id)
}

#[tokio::test]
async fn subscription_refused_by_a_stopping_room_is_retried() {
    let dir = tempdir().unwrap();
    let (state, room_id) = state_with_refusing_room(&dir, SUBSCRIBE_ATTEMPTS - 1);
    assert!(subscribe(&state, &room_id).await.is_ok());
}

#[tokio::test]
async fn subscription_retries_are_bounded() {
    let dir = tempdir().unwrap();
    let (state, room_id) = state_with_refusing_room(&dir, SUBSCRIBE_ATTEMPTS);
    assert!(matches!(
        subscribe(&state, &room_id).await,
        Err(ServerError::Unavailable(_))
    ));
}
