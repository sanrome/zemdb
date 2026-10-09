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
    let (exit_tx, exit_rx) = watch::channel(None);
    manager.rooms.insert(
        room_id.clone(),
        RoomSlot {
            sender: closed_tx,
            exit: exit_rx,
            actor_id: u64::MAX,
        },
    );

    // The request is abandoned while waiting for the previous actor.
    let abandoned = tokio::time::timeout(
        Duration::from_millis(100),
        manager.get_or_spawn(&room_id, None),
    )
    .await;
    assert!(abandoned.is_err());
    assert!(
        manager.rooms.contains_key(&room_id),
        "the previous actor must still be tracked after a cancelled respawn"
    );

    exit_tx.send_replace(Some(ActorExit::Stopped));
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
                let _existing_guard = manager.room_meta.get(&existing).unwrap();
                (0..)
                    .map(|i| RoomId::new(format!("room-g{i}")).unwrap())
                    .find(|id| manager.room_meta.try_get_mut(id).is_locked())
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

#[tokio::test(start_paused = true)]
async fn log_bounds_of_a_stalled_actor_times_out() {
    let dir = tempdir().unwrap();
    let manager = new_manager(&dir);
    let room_id = RoomId::new("stalled").unwrap();
    // An actor that accepts commands but never answers them.
    let (sender, _receiver) = mpsc::channel(8);
    let (_exit_tx, exit_rx) = watch::channel(None);
    manager.rooms.insert(
        room_id.clone(),
        RoomSlot {
            sender,
            exit: exit_rx,
            actor_id: u64::MAX,
        },
    );

    let result = tokio::time::timeout(Duration::from_secs(60), manager.log_bounds(&room_id))
        .await
        .expect("log_bounds must give up on a stalled actor");
    assert!(matches!(result, Err(ServerError::Timeout(_))), "{result:?}");
}

const POLL_CEILING: Duration = Duration::from_secs(10);

/// Polls `condition` until it holds, failing the test after [`POLL_CEILING`].
async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + POLL_CEILING;
    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn room_dir(dir: &TempDir, room_id: &RoomId) -> PathBuf {
    dir.path().join("rooms").join(room_id.as_str())
}

async fn effective_policy(manager: &RoomManager, room_id: &RoomId) -> RoomLifecyclePolicy {
    manager
        .ask(room_id, |reply| RoomCommand::GetMetrics { reply })
        .await
        .unwrap()
        .unwrap()
        .lifecycle
}

fn custom_overrides() -> RoomLifecycleOverrides {
    RoomLifecycleOverrides {
        ram_max_ops: Some(3),
        lease_timeout_secs: Some(42),
        dormant_after_secs: Some(3600),
        idle_timeout_secs: Some(0),
        ..RoomLifecycleOverrides::default()
    }
}

#[tokio::test]
async fn lifecycle_overrides_survive_a_respawn_and_a_restart() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("policy-a").unwrap();
    let manager = new_manager(&dir);
    let defaults = manager.default_policy.clone();
    let expected = RoomLifecyclePolicy {
        ram_max_ops: 3,
        lease_timeout: Duration::from_secs(42),
        dormant_after: Some(Duration::from_secs(3600)),
        idle_timeout: None,
        ..defaults.clone()
    };
    manager
        .create_room(
            room_id.clone(),
            SchemaId::new("first").unwrap(),
            Some(custom_overrides()),
        )
        .await
        .unwrap();
    assert_eq!(effective_policy(&manager, &room_id).await, expected);

    // The actor stops and the next request respawns it.
    assert!(manager.shutdown_room(&room_id).await);
    assert_eq!(effective_policy(&manager, &room_id).await, expected);

    // Assigning another schema keeps the overrides.
    manager.shutdown_room(&room_id).await;
    manager
        .get_or_spawn(&room_id, Some(&SchemaId::new("second").unwrap()))
        .await
        .unwrap();
    assert_eq!(effective_policy(&manager, &room_id).await, expected);
    manager.shutdown_all().await;

    // A restarted server reads them from meta_room.json.
    let manager = new_manager(&dir);
    assert_eq!(effective_policy(&manager, &room_id).await, expected);
    assert_eq!(
        manager.lifecycle_overrides(&room_id),
        Some(custom_overrides())
    );
    let stored: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(meta_room_path(&dir, &room_id)).unwrap()).unwrap();
    assert_eq!(
        stored["lifecycle"],
        serde_json::to_value(custom_overrides()).unwrap()
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn meta_room_without_lifecycle_has_no_overrides() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("policy-b").unwrap();
    let meta_path = meta_room_path(&dir, &room_id);
    fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
    fs::write(
        &meta_path,
        br#"{"room_id": "policy-b", "schema_id": "first"}"#,
    )
    .unwrap();

    let manager = new_manager(&dir);
    let defaults = manager.default_policy.clone();
    assert_eq!(effective_policy(&manager, &room_id).await, defaults);
    assert_eq!(
        manager.lifecycle_overrides(&room_id),
        Some(RoomLifecycleOverrides::default())
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn invalid_stored_overrides_fail_to_load() {
    let dir = tempdir().unwrap();
    for (name, lifecycle) in [
        ("policy-c", r#"{"ram_max_ops": 0}"#),
        ("policy-d", r#"{"ram_ttl": 5}"#),
    ] {
        let room_id = RoomId::new(name).unwrap();
        let meta_path = meta_room_path(&dir, &room_id);
        fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
        fs::write(
            &meta_path,
            format!(r#"{{"room_id": "{name}", "schema_id": "first", "lifecycle": {lifecycle}}}"#),
        )
        .unwrap();

        let res = new_manager(&dir).get_or_spawn(&room_id, None).await;
        assert!(
            matches!(res, Err(ServerError::Serialization(_))),
            "{name}: {res:?}"
        );
    }
}

#[tokio::test]
async fn invalid_overrides_are_rejected_before_creating_the_room() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("policy-e").unwrap();
    let manager = new_manager(&dir);
    let res = manager
        .create_room(
            room_id.clone(),
            SchemaId::new("first").unwrap(),
            Some(RoomLifecycleOverrides {
                snapshot_demand_ttl_secs: Some(1),
                ..RoomLifecycleOverrides::default()
            }),
        )
        .await;
    assert!(matches!(res, Err(ServerError::BadRequest(_))), "{res:?}");
    assert!(!manager.room_exists(&room_id));
    assert!(!room_dir(&dir, &room_id).exists());
}

#[tokio::test]
async fn inactive_room_shuts_down_and_reopens_with_its_overrides() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("idle-a").unwrap();
    let manager = new_manager(&dir);
    let overrides = RoomLifecycleOverrides {
        idle_timeout_secs: Some(1),
        lease_timeout_secs: Some(42),
        ..RoomLifecycleOverrides::default()
    };
    manager
        .create_room(
            room_id.clone(),
            SchemaId::new("first").unwrap(),
            Some(overrides),
        )
        .await
        .unwrap();

    for _ in 0..2 {
        assert!(manager.get_room(&room_id).is_some());
        // Once the actor has exited, the manager keeps nothing for the room but its metadata.
        wait_until("the inactive room shuts down", || {
            !manager.rooms.contains_key(&room_id)
        })
        .await;
        assert!(manager.spawn_locks.is_empty());

        // The next request reopens the room, with its overrides: it shuts down again.
        let policy = effective_policy(&manager, &room_id).await;
        assert_eq!(policy.idle_timeout, Some(Duration::from_secs(1)));
        assert_eq!(policy.lease_timeout, Duration::from_secs(42));
    }
    manager.shutdown_all().await;
}

#[tokio::test]
async fn room_with_an_event_subscriber_stays_open() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("idle-b").unwrap();
    let manager = new_manager(&dir).with_default_policy(RoomLifecyclePolicy {
        idle_timeout: Some(Duration::from_millis(100)),
        ..RoomLifecyclePolicy::default()
    });
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    let events = manager
        .ask(&room_id, |reply| RoomCommand::SubscribeEvents { reply })
        .await
        .unwrap()
        .unwrap();

    // Several maintenance ticks without commands.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(manager.get_room(&room_id).is_some());

    drop(events);
    wait_until("the room shuts down once unsubscribed", || {
        !manager.rooms.contains_key(&room_id)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_racing_an_inactivity_shutdown_are_never_refused() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("idle-c").unwrap();
    // Every maintenance tick that finds no command in the last millisecond shuts the room down,
    // so requests keep arriving while the actor closes its mailbox and drains it.
    let manager = Arc::new(new_manager(&dir).with_default_policy(RoomLifecyclePolicy {
        idle_timeout: Some(Duration::from_millis(1)),
        ..RoomLifecyclePolicy::default()
    }));
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut writers = Vec::new();
    for writer in 0..4u8 {
        let manager = Arc::clone(&manager);
        let room_id = room_id.clone();
        writers.push(tokio::spawn(async move {
            let client_id = zemdb_core::id::ClientId::new(format!("writer-{writer}")).unwrap();
            manager
                .ask(&room_id, |reply| RoomCommand::RegisterClient {
                    client_id: client_id.clone(),
                    current_seq: None,
                    reply,
                })
                .await
                .unwrap()
                .unwrap();
            let mut commits = 0u64;
            let mut n = 0u32;
            while tokio::time::Instant::now() < deadline {
                n += 1;
                if n.is_multiple_of(3) {
                    manager
                        .ask(&room_id, |reply| RoomCommand::Heartbeat {
                            client_id: client_id.clone(),
                            reply,
                        })
                        .await
                        .unwrap_or_else(|err| panic!("heartbeat {n}: {err:?}"))
                        .unwrap();
                } else {
                    let mut mutation = [writer; 16];
                    mutation[..4].copy_from_slice(&n.to_be_bytes());
                    let row = zemdb_core::value::RowBuilder::new()
                        .set("id", i64::from(n))
                        .set("title", "task")
                        .build();
                    let op = test_schema()
                        .to_operation_insert("tasks", &row, 1000)
                        .unwrap();
                    manager
                        .ask(&room_id, |reply| RoomCommand::Commit {
                            client_id: client_id.clone(),
                            mutation_id: zemdb_core::id::MutationId::new(mutation),
                            last_ack_seq: SequenceNumber::new(0),
                            op,
                            reply,
                        })
                        .await
                        .unwrap_or_else(|err| panic!("commit {n}: {err:?}"))
                        .unwrap();
                    commits += 1;
                }
                tokio::time::sleep(Duration::from_millis(u64::from(n % 4))).await;
            }
            commits
        }));
    }
    let mut commits = 0;
    for writer in writers {
        commits += writer.await.unwrap();
    }

    let actors = manager.next_actor_id.load(Ordering::Relaxed);
    assert!(actors >= 3, "the room restarted only {} times", actors - 1);
    // Every acknowledged commit is in the log exactly once.
    let (_, head_seq) = manager.log_bounds(&room_id).await.unwrap();
    assert_eq!(head_seq, SequenceNumber::new(commits));
    manager.shutdown_all().await;
}

#[tokio::test]
async fn panic_while_handling_a_command_is_internal_and_queued_commands_are_unavailable() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("panicky").unwrap();
    let manager = new_manager(&dir);
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();

    fail_point::arm("room_command_panic", &room_dir(&dir, &room_id));
    // On this single-threaded runtime both commands are queued before the actor runs: the
    // first one panics and the second one is still in the mailbox.
    let (panicked, queued) = tokio::join!(
        manager.ask(&room_id, |reply| RoomCommand::GetLogBounds { reply }),
        manager.ask(&room_id, |reply| RoomCommand::GetLogBounds { reply }),
    );
    assert!(
        matches!(panicked, Err(ServerError::Internal(_))),
        "{panicked:?}"
    );
    assert!(
        matches!(queued, Ok(Err(ServerError::Unavailable(_)))),
        "{queued:?}"
    );

    // The next request reopens the room from disk.
    let bounds = manager.log_bounds(&room_id).await.unwrap();
    assert_eq!(bounds, (SequenceNumber::new(1), SequenceNumber::new(0)));
    manager.shutdown_all().await;
}

#[tokio::test]
async fn deliberate_stop_with_a_queued_command_is_unavailable() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("stopping").unwrap();
    let manager = new_manager(&dir);
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    let sender = manager.get_room(&room_id).unwrap();

    // A shutdown queued ahead of the request: the actor stops and drops the request.
    let (shutdown_tx, _shutdown_rx) = oneshot::channel();
    sender
        .send(RoomCommand::Shutdown { reply: shutdown_tx })
        .await
        .unwrap();
    let res = manager
        .ask(&room_id, |reply| RoomCommand::GetLogBounds { reply })
        .await;
    assert!(matches!(res, Err(ServerError::Unavailable(_))), "{res:?}");
    manager.log_bounds(&room_id).await.unwrap();
    manager.shutdown_all().await;
}

#[tokio::test]
async fn spawned_actor_reports_a_panic() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("panicky-task").unwrap();
    let config = Arc::new(ServerConfig {
        data_dir: dir.path().to_path_buf(),
        ..ServerConfig::default()
    });
    let relay = Arc::new(
        SnapshotRelay::new(
            dir.path().join("snapshots"),
            Duration::from_secs(60),
            config.max_snapshot_bytes,
        )
        .unwrap(),
    );
    let (sender, handle) = RoomActor::spawn(
        room_id.clone(),
        SchemaId::new("first").unwrap(),
        Arc::new(test_schema()),
        dir.path(),
        config,
        RoomLifecyclePolicy::default(),
        relay,
    )
    .unwrap();

    fail_point::arm("room_command_panic", &room_dir(&dir, &room_id));
    let (tx, rx) = oneshot::channel();
    sender
        .send(RoomCommand::GetLogBounds { reply: tx })
        .await
        .unwrap();
    assert!(rx.await.is_err());
    assert_eq!(handle.await.unwrap(), ActorExit::Panicked);
    assert!(sender.is_closed());
}

#[tokio::test]
async fn spawn_lock_is_shared_with_waiters_and_forgotten_when_released() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("locked").unwrap();
    let manager = Arc::new(new_manager(&dir));

    let first = manager.lock_room(&room_id).await;
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let (acquired_tx, acquired_rx) = oneshot::channel::<()>();
    let waiter = tokio::spawn({
        let manager = Arc::clone(&manager);
        let room_id = room_id.clone();
        async move {
            let _second = manager.lock_room(&room_id).await;
            acquired_tx.send(()).unwrap();
            release_rx.await.unwrap();
        }
    });
    // The waiter holds a reference to the lock while it waits for it.
    wait_until("the waiter waits for the lock", || {
        manager
            .spawn_locks
            .get(&room_id)
            .is_some_and(|lock| Arc::strong_count(&lock) > 2)
    })
    .await;

    // Releasing the lock while the waiter still waits for it must keep its entry: a new
    // caller has to wait for the same mutex the waiter now holds.
    drop(first);
    acquired_rx.await.unwrap();
    assert!(manager.spawn_locks.contains_key(&room_id));
    let third = tokio::time::timeout(Duration::from_millis(100), manager.lock_room(&room_id)).await;
    assert!(
        third.is_err(),
        "a new caller acquired the lock while the waiter held it"
    );

    release_tx.send(()).unwrap();
    waiter.await.unwrap();
    drop(manager.lock_room(&room_id).await);
    assert!(
        manager.spawn_locks.is_empty(),
        "released spawn locks must not accumulate"
    );
}

#[tokio::test]
async fn command_refused_by_a_closing_mailbox_goes_to_the_respawned_room() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("closing").unwrap();
    let manager = Arc::new(new_manager(&dir));
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    manager.shutdown_all().await;

    // An actor whose full mailbox makes the next request wait to be queued.
    let (sender, mut receiver) = mpsc::channel::<RoomCommand>(1);
    let (exit_tx, exit_rx) = watch::channel(None);
    let (filler_tx, _filler_rx) = oneshot::channel();
    sender
        .try_send(RoomCommand::GetLogBounds { reply: filler_tx })
        .unwrap();
    manager.rooms.insert(
        room_id.clone(),
        RoomSlot {
            sender,
            exit: exit_rx,
            actor_id: u64::MAX,
        },
    );
    let request = tokio::spawn({
        let manager = Arc::clone(&manager);
        let room_id = room_id.clone();
        async move { manager.log_bounds(&room_id).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!request.is_finished());

    // The actor closes its mailbox (as when it shuts down for inactivity) and exits without
    // ever taking the waiting request.
    receiver.close();
    drop(receiver);
    exit_tx.send_replace(Some(ActorExit::Stopped));

    let bounds = request.await.unwrap();
    assert_eq!(
        bounds.unwrap(),
        (SequenceNumber::new(1), SequenceNumber::new(0))
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn client_inactivity_keeps_counting_across_an_inactivity_shutdown() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("idle-lease").unwrap();
    // The lease outlives the room's idle timeout: the client is still Connected when the room
    // shuts down.
    let manager = new_manager(&dir).with_default_policy(RoomLifecyclePolicy {
        idle_timeout: Some(Duration::from_millis(200)),
        lease_timeout: Duration::from_secs(3),
        ..RoomLifecyclePolicy::default()
    });
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    let client = zemdb_core::id::ClientId::new("quiet").unwrap();
    manager
        .ask(&room_id, |reply| RoomCommand::RegisterClient {
            client_id: client.clone(),
            current_seq: None,
            reply,
        })
        .await
        .unwrap()
        .unwrap();
    let last_activity = tokio::time::Instant::now();
    wait_until("the inactive room shuts down", || {
        !manager.rooms.contains_key(&room_id)
    })
    .await;

    // The room reopens before the lease expires. The lease keeps counting from the client's
    // last activity, not from the reopening.
    tokio::time::sleep_until(last_activity + Duration::from_millis(2500)).await;
    let deadline = last_activity + Duration::from_millis(4500);
    loop {
        let metrics = manager
            .ask(&room_id, |reply| RoomCommand::GetMetrics { reply })
            .await
            .unwrap()
            .unwrap();
        if metrics.disconnected_clients == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lease restarted when the room reopened"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    manager.shutdown_all().await;
}

#[tokio::test]
async fn schema_reload_racing_a_respawn_reaches_the_new_actor() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("evolving").unwrap();
    let schema_id = SchemaId::new("first").unwrap();
    let manager = Arc::new(new_manager(&dir));
    manager
        .create_room(room_id.clone(), schema_id.clone(), None)
        .await
        .unwrap();
    manager.shutdown_all().await;

    // A respawn is in progress: it holds the spawn lock and has read the previous schema.
    let spawning = manager.lock_room(&room_id).await;
    let previous = manager.schema_registry.get_schema(&schema_id).unwrap();
    let meta = manager.room_meta.get(&room_id).unwrap().clone();

    // Meanwhile the schema evolves and the reload runs.
    let evolved = Arc::new(test_schema());
    let reload = tokio::spawn({
        let manager = Arc::clone(&manager);
        let schema_id = schema_id.clone();
        let evolved = Arc::clone(&evolved);
        async move { manager.reload_schema_for_rooms(&schema_id, evolved).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The respawn completes with the schema it read.
    manager.spawn_actor(&room_id, &meta, previous).unwrap();
    drop(spawning);

    assert_eq!(reload.await.unwrap(), vec![room_id.clone()]);
    let (_, schema) = manager
        .ask(&room_id, |reply| RoomCommand::GetSchema { reply })
        .await
        .unwrap()
        .unwrap();
    assert!(
        Arc::ptr_eq(&schema, &evolved),
        "the respawned actor kept the previous schema"
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn abandoned_waiter_does_not_leave_the_spawn_lock_behind() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("abandoned").unwrap();
    let manager = new_manager(&dir);

    let holder = manager.lock_room(&room_id).await;
    let mut waiter = Box::pin(manager.lock_room(&room_id));
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    // The holder releases the lock to the waiter, which is abandoned before it runs again
    // (its request timed out or its client went away).
    drop(holder);
    drop(waiter);

    assert!(
        manager.spawn_locks.is_empty(),
        "the abandoned waiter left the spawn lock behind"
    );
    // The room can still be locked.
    drop(manager.lock_room(&room_id).await);
    assert!(manager.spawn_locks.is_empty());
}

#[tokio::test(start_paused = true)]
async fn panic_after_a_long_wait_in_the_mailbox_is_still_internal() {
    let dir = tempdir().unwrap();
    let manager = Arc::new(new_manager(&dir));
    let room_id = RoomId::new("slow").unwrap();
    let (sender, mut receiver) = mpsc::channel(8);
    let (exit_tx, exit_rx) = watch::channel(None);
    manager.rooms.insert(
        room_id.clone(),
        RoomSlot {
            sender,
            exit: exit_rx,
            actor_id: u64::MAX,
        },
    );
    let request = tokio::spawn({
        let manager = Arc::clone(&manager);
        let room_id = room_id.clone();
        async move { manager.log_bounds(&room_id).await }
    });

    // The command waits in the mailbox for almost the whole actor timeout, then the actor
    // panics on it: its reply is dropped, and the actor reports the panic a little later.
    let command = receiver.recv().await.unwrap();
    tokio::time::advance(ACTOR_TIMEOUT - Duration::from_millis(100)).await;
    drop(command);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(500)).await;
    // Let the request observe the time that passed before the actor reports the panic.
    tokio::time::sleep(Duration::from_millis(1)).await;
    exit_tx.send_replace(Some(ActorExit::Panicked));

    let result = request.await.unwrap();
    assert!(
        matches!(result, Err(ServerError::Internal(_))),
        "{result:?}"
    );
}

#[tokio::test]
async fn activity_keeps_a_room_open() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("busy").unwrap();
    let manager = new_manager(&dir).with_default_policy(RoomLifecyclePolicy {
        idle_timeout: Some(Duration::from_millis(300)),
        ..RoomLifecyclePolicy::default()
    });
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    let actor_id = manager.rooms.get(&room_id).unwrap().actor_id;

    // Commands more often than the idle timeout, for several timeouts.
    for _ in 0..20 {
        manager.log_bounds(&room_id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        manager.rooms.get(&room_id).map(|slot| slot.actor_id),
        Some(actor_id),
        "the room shut down while it received commands"
    );
    manager.shutdown_all().await;
}
