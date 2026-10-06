use super::*;
use crate::durable;
use crate::fail_point;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use zemdb_core::schema::{Schema, TableSchema};
use zemdb_core::value::DataType;

fn test_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .build()
        .unwrap();
    Schema::from_tables(vec![table])
}

fn new_manager(dir: &TempDir) -> RoomManager {
    let config = Arc::new(ServerConfig {
        data_dir: dir.path().to_path_buf(),
        ..ServerConfig::default()
    });
    let registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
    registry
        .register_schema(SchemaId::new("first").unwrap(), test_schema())
        .unwrap();
    registry
        .register_schema(SchemaId::new("second").unwrap(), test_schema())
        .unwrap();
    let relay = Arc::new(
        SnapshotRelay::new(
            dir.path().join("snapshots"),
            Duration::from_secs(60),
            ServerConfig::default().max_snapshot_bytes,
        )
        .unwrap(),
    );
    RoomManager::new(config, registry, relay)
}

fn meta_room_path(dir: &TempDir, room_id: &RoomId) -> PathBuf {
    dir.path()
        .join("rooms")
        .join(room_id.as_str())
        .join("meta_room.json")
}

async fn schema_of(sender: &mpsc::Sender<RoomCommand>) -> SchemaId {
    let (tx, rx) = tokio::sync::oneshot::channel();
    sender
        .send(RoomCommand::GetSchema { reply: tx })
        .await
        .unwrap();
    rx.await.unwrap().unwrap().0
}

#[tokio::test]
async fn interrupted_meta_room_rewrite_keeps_previous_assignment() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("room-a").unwrap();
    let manager = new_manager(&dir);
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    manager.shutdown_all().await;

    // A crash after the new metadata reached the temporary file but before the rename.
    let meta_path = meta_room_path(&dir, &room_id);
    fail_point::arm("write_atomic_before_rename", &meta_path);
    let manager = new_manager(&dir);
    let res = manager
        .get_or_spawn(&room_id, Some(&SchemaId::new("second").unwrap()))
        .await;
    assert!(res.is_err(), "the interrupted metadata write must fail");

    let manager = new_manager(&dir);
    let sender = manager.get_or_spawn(&room_id, None).await.unwrap();
    assert_eq!(schema_of(&sender).await, SchemaId::new("first").unwrap());
    assert!(!durable::tmp_path_for(&meta_path).exists());
    manager.shutdown_all().await;
}

#[tokio::test]
async fn interrupted_room_creation_leaves_no_room() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("room-b").unwrap();
    let manager = new_manager(&dir);

    fail_point::arm(
        "write_atomic_before_rename",
        &meta_room_path(&dir, &room_id),
    );
    let res = manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await;
    assert!(res.is_err(), "the interrupted metadata write must fail");
    assert!(!manager.room_exists(&room_id));

    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    manager.shutdown_all().await;
}

#[tokio::test]
async fn unreadable_meta_room_fails_to_load() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("room-c").unwrap();
    let meta_path = meta_room_path(&dir, &room_id);
    fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
    fs::write(&meta_path, b"{\"room_id\": \"room-c\", \"schema_").unwrap();

    let manager = new_manager(&dir);
    let res = manager.get_or_spawn(&room_id, None).await;
    assert!(matches!(res, Err(ServerError::Serialization(_))));
}

#[tokio::test]
async fn cancelled_respawn_keeps_waiting_for_the_previous_actor() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("room-d").unwrap();
    let manager = new_manager(&dir);
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    manager.shutdown_all().await;

    // A stopped actor (closed mailbox) whose task has not finished releasing the room yet.
    let (closed_tx, closed_rx) = mpsc::channel::<RoomCommand>(1);
    drop(closed_rx);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    manager.rooms.insert(room_id.clone(), closed_tx);
    manager.room_handles.insert(
        room_id.clone(),
        tokio::spawn(async move {
            release_rx.await.ok();
        }),
    );

    // The request is abandoned while waiting for the previous actor.
    let abandoned = tokio::time::timeout(
        Duration::from_millis(100),
        manager.get_or_spawn(&room_id, None),
    )
    .await;
    assert!(abandoned.is_err());
    assert!(
        manager.room_handles.contains_key(&room_id),
        "the previous actor must still be tracked after a cancelled respawn"
    );

    release_tx.send(()).unwrap();
    let sender = manager.get_or_spawn(&room_id, None).await.unwrap();
    assert!(!sender.is_closed());
    manager.shutdown_all().await;
}

#[tokio::test]
async fn first_room_creation_makes_rooms_directory_durable() {
    let dir = tempdir().unwrap();
    let manager = new_manager(&dir);

    fail_point::arm("sync_dir", dir.path());
    let res = manager
        .create_room(
            RoomId::new("room-e").unwrap(),
            SchemaId::new("first").unwrap(),
            None,
        )
        .await;

    assert!(res.is_err(), "creating rooms/ must sync the data directory");
}

/// Runs `scenario` in its own thread and waits up to `limit` for it to finish. A panic in the
/// scenario is propagated. A scenario that never finishes (for example a deadlocked runtime)
/// is left behind and reported as `false`.
fn finishes_within(limit: Duration, scenario: impl FnOnce() + Send + 'static) -> bool {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        scenario();
        let _ = done_tx.send(());
    });
    match done_rx.recv_timeout(limit) {
        Ok(()) => true,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(panic) = handle.join() {
                std::panic::resume_unwind(panic);
            }
            true
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => false,
    }
}

#[test]
fn schema_reload_does_not_block_concurrent_room_creation() {
    // A single-threaded runtime: while the reload waits for a room actor's reply, the room
    // creation runs on the same thread. If the reload kept a guard of the room map across that
    // wait, the creation would block the thread on the map's lock and nothing could progress.
    let finished = finishes_within(Duration::from_secs(10), || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = tempdir().unwrap();
            let manager = Arc::new(new_manager(&dir));
            let schema_id = SchemaId::new("first").unwrap();
            let existing = RoomId::new("room-f").unwrap();
            manager
                .create_room(existing.clone(), schema_id.clone(), None)
                .await
                .unwrap();

            // A new room whose schema assignment lands in the same map shard as the existing
            // one: while a read guard on the existing entry is held, writing to it is refused.
            let new_room = {
                let _existing_guard = manager.room_schemas.get(&existing).unwrap();
                (0..)
                    .map(|i| RoomId::new(format!("room-g{i}")).unwrap())
                    .find(|id| manager.room_schemas.try_get_mut(id).is_locked())
                    .unwrap()
            };

            let reload = tokio::spawn({
                let manager = Arc::clone(&manager);
                let schema_id = schema_id.clone();
                async move {
                    manager
                        .reload_schema_for_rooms(&schema_id, Arc::new(test_schema()))
                        .await
                }
            });
            let create = tokio::spawn({
                let manager = Arc::clone(&manager);
                let schema_id = schema_id.clone();
                async move { manager.create_room(new_room, schema_id, None).await }
            });

            assert_eq!(reload.await.unwrap(), vec![existing]);
            create.await.unwrap().unwrap();
            manager.shutdown_all().await;
        });
    });
    assert!(
        finished,
        "schema reload and room creation deadlocked on the room map"
    );
}
