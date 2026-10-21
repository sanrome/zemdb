use super::*;
use crate::actor::test_support::{
    snapshot_envelope, test_manager, test_schema, wait_until, POLL_CEILING,
};
use crate::durable;
use crate::fail_point;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use zemdb_core::id::SequenceNumber;
use zemdb_core::schema::{OwnerIdentity, TableBuilder, TableSchema};
use zemdb_core::value::DataType;

fn new_manager(dir: &TempDir) -> RoomManager {
    test_manager(dir.path(), &["first", "second"], |_| {}).0
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
    assert!(!manager.room_exists(&room_id).unwrap());

    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    manager.shutdown_all().await;
}

#[tokio::test]
async fn failure_to_check_for_meta_room_does_not_create_the_room_again() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("room-c").unwrap();
    let first = SchemaId::new("first").unwrap();
    let manager = new_manager(&dir);
    manager
        .create_room(room_id.clone(), first.clone(), None)
        .await
        .unwrap();
    manager.shutdown_all().await;
    let meta_path = meta_room_path(&dir, &room_id);
    let stored = fs::read(&meta_path).unwrap();

    let manager = new_manager(&dir);
    fail_point::arm("exists", &meta_path);
    assert!(manager.create_room(room_id, first, None).await.is_err());
    assert_eq!(fs::read(&meta_path).unwrap(), stored);
}

#[tokio::test]
async fn unreadable_meta_room_fails_to_load() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("room-c").unwrap();
    let meta_path = meta_room_path(&dir, &room_id);
    let manager = new_manager(&dir);
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    manager.shutdown_all().await;
    let stored = fs::read(&meta_path).unwrap();

    // Truncated, and one bit flipped in a digit of the incarnation, which keeps the JSON valid.
    let needle = b"\"room_incarnation\": ";
    let at = stored
        .windows(needle.len())
        .position(|w| w == needle)
        .unwrap()
        + needle.len()
        + 1;
    let mut flipped = stored.clone();
    flipped[at] ^= 1;
    for damaged in [stored[..stored.len() / 2].to_vec(), flipped] {
        fs::write(&meta_path, damaged).unwrap();
        let res = new_manager(&dir).get_or_spawn(&room_id, None).await;
        assert!(matches!(res, Err(ServerError::Serialization(_))), "{res:?}");
    }
}

/// Every single bit flip and every truncation of a `meta_room.json` (with lifecycle overrides)
/// makes reading it fail, through the same function that spawning the room and the admin `GET`
/// of its overrides use.
#[tokio::test]
async fn every_damage_to_meta_room_fails_to_load() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("room-x").unwrap();
    let meta_path = meta_room_path(&dir, &room_id);
    let manager = new_manager(&dir);
    let lifecycle = RoomLifecycleOverrides {
        max_room_disk_bytes: Some(64 * 1024 * 1024),
        idle_timeout_secs: Some(60),
        ..RoomLifecycleOverrides::default()
    };
    manager
        .create_room(
            room_id.clone(),
            SchemaId::new("first").unwrap(),
            Some(lifecycle),
        )
        .await
        .unwrap();
    manager.shutdown_all().await;
    let stored = fs::read(&meta_path).unwrap();
    assert!(read_room_metadata(&meta_path).is_ok());

    for (damage, bytes) in crate::test_support::every_truncation_and_bit_flip(&stored) {
        fs::write(&meta_path, &bytes).unwrap();
        let read = read_room_metadata(&meta_path);
        assert!(
            read.is_err(),
            "a meta_room.json {damage} loaded as {read:?}: {:?}",
            String::from_utf8_lossy(&bytes)
        );
    }
    fs::write(&meta_path, &stored).unwrap();
    let manager = new_manager(&dir);
    assert_eq!(
        manager
            .lifecycle_overrides(&room_id)
            .await
            .unwrap()
            .idle_timeout_secs,
        Some(60)
    );
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

/// Wherever the deletion of a room stops (its directory lost without one, a crash right after
/// it is renamed away, a failure to purge the relay, or a crash halfway through removing its
/// files), after a restart the room is gone and a room created again with the same id starts
/// empty, without the old room's snapshot.
#[tokio::test]
async fn room_recreated_after_an_interrupted_deletion_starts_empty() {
    let (room_id, schema_id) = (
        RoomId::new("reborn").unwrap(),
        SchemaId::new("first").unwrap(),
    );
    let client_id = zemdb_core::id::ClientId::new("writer").unwrap();
    for cut in [
        "directory lost",
        "after the rename",
        "while purging",
        "while removing",
    ] {
        let dir = tempdir().unwrap();
        let (rooms, room_dir) = (dir.path().join("rooms"), room_dir(&dir, &room_id));
        let (manager, relay) = test_manager(dir.path(), &["first"], |_| {});
        manager
            .create_room(room_id.clone(), schema_id.clone(), None)
            .await
            .unwrap();
        manager
            .ask(&room_id, |reply| RoomCommand::RegisterClient {
                client_id: client_id.clone(),
                current_seq: None,
                known_incarnation: None,
                reply,
            })
            .await
            .unwrap()
            .unwrap();
        let row = zemdb_core::value::RowBuilder::new()
            .set("id", 1)
            .set("title", "old")
            .build();
        let op = test_schema()
            .to_operation_insert("tasks", &row, 1000)
            .unwrap();
        manager
            .ask(&room_id, |reply| RoomCommand::Commit {
                client_id: client_id.clone(),
                mutation_id: zemdb_core::id::MutationId::new([1; 16]),
                last_ack_seq: SequenceNumber::new(0),
                op,
                reply,
            })
            .await
            .unwrap()
            .unwrap();
        relay
            .stage_snapshot(
                &room_id,
                &crate::relay::Uploader::Admin,
                SequenceNumber::new(1),
                snapshot_envelope(),
                manager.log_bounds(&room_id),
            )
            .await
            .unwrap();
        // Creating the room again fails without touching its snapshot.
        let again = manager
            .create_room(room_id.clone(), schema_id.clone(), None)
            .await;
        assert!(
            matches!(again, Err(ServerError::RoomAlreadyExists(_))),
            "{again:?}"
        );
        assert_eq!(
            relay.active_snapshot_seq(&room_id),
            Some(SequenceNumber::new(1))
        );

        match cut {
            "directory lost" => {
                manager.shutdown_all().await;
                fs::remove_dir_all(&room_dir).unwrap();
            }
            "after the rename" => {
                fail_point::arm("sync_dir", &rooms);
                assert!(manager.delete_room(&room_id).await.is_err());
                assert_eq!(
                    relay.active_snapshot_seq(&room_id),
                    Some(SequenceNumber::new(1))
                );
            }
            "while purging" => {
                fail_point::arm("sync_dir", &dir.path().join("snapshots"));
                manager.delete_room(&room_id).await.unwrap();
            }
            _ => {
                fail_point::arm("room_removal", &room_dir);
                manager.delete_room(&room_id).await.unwrap();
            }
        }
        // Whatever is left lost its metadata first, as `remove_dir_all` may do.
        for entry in fs::read_dir(&rooms).unwrap() {
            durable::remove_file_if_exists(&entry.unwrap().path().join("meta_room.json")).unwrap();
        }

        let (manager, relay) = test_manager(dir.path(), &["first"], |_| {});
        assert_eq!(fs::read_dir(&rooms).unwrap().count(), 0, "{cut}");
        manager
            .create_room(room_id.clone(), schema_id.clone(), None)
            .await
            .unwrap();
        let head_seq = manager.log_bounds(&room_id).await.unwrap().head_seq;
        assert_eq!(head_seq, SequenceNumber::new(0), "{cut}");
        assert_eq!(relay.active_snapshot_seq(&room_id), None, "{cut}");
        manager.shutdown_all().await;
    }
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
                async move { manager.reload_schema_for_rooms(&schema_id).await }
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
        manager.lifecycle_overrides(&room_id).await.unwrap(),
        custom_overrides()
    );
    let stored: serde_json::Value = serde_json::from_str(
        durable::verified_json(&fs::read(meta_room_path(&dir, &room_id)).unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        stored["lifecycle"],
        serde_json::to_value(custom_overrides()).unwrap()
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn room_incarnation_survives_a_schema_change_and_a_restart() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("incarnated").unwrap();
    let manager = new_manager(&dir);
    let created = manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    manager.shutdown_room(&room_id).await;
    manager
        .get_or_spawn(&room_id, Some(&SchemaId::new("second").unwrap()))
        .await
        .unwrap();
    manager.shutdown_all().await;

    // A restarted server accepts a cursor of the same room.
    let manager = new_manager(&dir);
    let registered = manager
        .ask(&room_id, |reply| RoomCommand::RegisterClient {
            client_id: zemdb_core::id::ClientId::new("back").unwrap(),
            current_seq: Some(SequenceNumber::new(0)),
            known_incarnation: Some(created.room_incarnation),
            reply,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(registered.room_incarnation, created.room_incarnation);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn meta_room_without_lifecycle_has_no_overrides() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("policy-b").unwrap();
    let meta_path = meta_room_path(&dir, &room_id);
    fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
    durable::write_checked(
        &meta_path,
        br#"{"room_id": "policy-b", "schema_id": "first", "room_incarnation": 1}"#,
    )
    .unwrap();

    let manager = new_manager(&dir);
    let defaults = manager.default_policy.clone();
    assert_eq!(effective_policy(&manager, &room_id).await, defaults);
    assert_eq!(
        manager.lifecycle_overrides(&room_id).await.unwrap(),
        RoomLifecycleOverrides::default()
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
        durable::write_checked(
            &meta_path,
            format!(r#"{{"room_id": "{name}", "schema_id": "first", "lifecycle": {lifecycle}}}"#)
                .as_bytes(),
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
    assert!(!manager.room_exists(&room_id).unwrap());
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
            Some(overrides.clone()),
        )
        .await
        .unwrap();

    for _ in 0..2 {
        assert!(manager.get_room(&room_id).is_some());
        assert!(manager.snapshot_relay.snapshot_floor(&room_id).is_some());
        // Once the actor has exited, the manager keeps nothing for the room, nor the relay its
        // snapshot floor; its overrides are read from disk.
        wait_until("the inactive room shuts down", || {
            !manager.rooms.contains_key(&room_id)
        })
        .await;
        assert!(manager.spawn_locks.is_empty());
        assert!(manager.room_meta.is_empty());
        assert_eq!(manager.snapshot_relay.snapshot_floor(&room_id), None);
        assert_eq!(
            manager.lifecycle_overrides(&room_id).await.unwrap(),
            overrides
        );

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
    // A zero idle timeout: every maintenance tick shuts the room down, however busy it is, so
    // requests keep arriving while the actor closes its mailbox and drains it.
    let manager = Arc::new(new_manager(&dir).with_default_policy(RoomLifecyclePolicy {
        idle_timeout: Some(Duration::ZERO),
        ..RoomLifecyclePolicy::default()
    }));
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();

    // Writers keep going until the room has restarted twice.
    let deadline = tokio::time::Instant::now() + POLL_CEILING;
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
                    known_incarnation: None,
                    reply,
                })
                .await
                .unwrap()
                .unwrap();
            let mut commits = 0u64;
            let mut n = 0u32;
            while manager.next_actor_id.load(Ordering::Relaxed) < 3
                && tokio::time::Instant::now() < deadline
            {
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
    let bounds = manager.log_bounds(&room_id).await.unwrap();
    assert_eq!(bounds.head_seq, SequenceNumber::new(commits));
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
    assert_eq!(
        (bounds.tail_seq, bounds.head_seq),
        (SequenceNumber::new(1), SequenceNumber::new(0))
    );
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
            Duration::from_secs(config.snapshot_upload_max_secs),
        )
        .unwrap(),
    );
    let (sender, actor) = RoomActor::open(
        room_id.clone(),
        1,
        SchemaId::new("first").unwrap(),
        Arc::new(test_schema()),
        dir.path(),
        config,
        RoomLifecyclePolicy::default(),
        relay,
        Arc::new(InflightBudget::new(u64::MAX, u64::MAX)),
        ActorTimings::default(),
    )
    .unwrap();
    let handle = tokio::spawn(actor.run());

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

    let bounds = request.await.unwrap().unwrap();
    assert_eq!(
        (bounds.tail_seq, bounds.head_seq),
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
        lease_timeout: Duration::from_secs(5),
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
            known_incarnation: None,
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
    // last activity (expiring at 5 s), not from the reopening (it would expire at 9 s).
    tokio::time::sleep_until(last_activity + Duration::from_secs(4)).await;
    let deadline = last_activity + Duration::from_millis(7500);
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

    // A respawn is in progress: it holds the spawn lock, has resolved the room's metadata and
    // has read the previous schema.
    let spawning = manager.lock_room(&room_id).await;
    let meta = manager.resolve_metadata(&room_id, None).unwrap();
    let previous = manager.schema_registry.get_schema(&schema_id).unwrap();

    // Meanwhile the schema evolves and the reload runs.
    let evolved = manager
        .schema_registry
        .add_column(
            &schema_id,
            "tasks",
            zemdb_core::schema::ColumnDef::new("priority", DataType::Int).nullable(true),
        )
        .unwrap();
    let reload = tokio::spawn({
        let manager = Arc::clone(&manager);
        let schema_id = schema_id.clone();
        async move { manager.reload_schema_for_rooms(&schema_id).await }
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
        idle_timeout: Some(Duration::from_millis(1500)),
        ..RoomLifecyclePolicy::default()
    });
    manager
        .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
        .await
        .unwrap();
    let actor_id = manager.rooms.get(&room_id).unwrap().actor_id;

    // Commands far more often than the idle timeout, for two timeouts.
    for _ in 0..60 {
        manager.log_bounds(&room_id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        manager.rooms.get(&room_id).map(|slot| slot.actor_id),
        Some(actor_id),
        "the room shut down while it received commands"
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn room_that_fails_to_spawn_leaves_no_metadata() {
    let dir = tempdir().unwrap();
    let room_id = RoomId::new("no-schema").unwrap();
    let meta_path = meta_room_path(&dir, &room_id);
    fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
    durable::write_checked(
        &meta_path,
        br#"{"room_id": "no-schema", "schema_id": "missing", "room_incarnation": 1}"#,
    )
    .unwrap();

    let manager = new_manager(&dir);
    let result = manager.get_or_spawn(&room_id, None).await;
    assert!(
        matches!(result, Err(ServerError::SchemaNotFound(_))),
        "{result:?}"
    );
    assert!(manager.room_meta.is_empty());

    // A room whose log cannot be opened: `segments` is a file, not a directory.
    let blocked = RoomId::new("blocked").unwrap();
    fs::create_dir_all(room_dir(&dir, &blocked)).unwrap();
    fs::write(room_dir(&dir, &blocked).join("segments"), b"").unwrap();
    let schema = SchemaId::new("first").unwrap();
    assert!(manager.create_room(blocked, schema, None).await.is_err());
    assert!(manager.room_meta.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rooms_shut_down_together_and_save_their_rosters() {
    let dir = tempdir().unwrap();
    let manager = Arc::new(new_manager(&dir));
    let client = zemdb_core::id::ClientId::new("member").unwrap();
    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let mut rooms = Vec::new();
    for i in 0..3 {
        let room_id = RoomId::new(format!("closing-{i}")).unwrap();
        manager
            .create_room(room_id.clone(), SchemaId::new("first").unwrap(), None)
            .await
            .unwrap();
        manager
            .ask(&room_id, |reply| RoomCommand::RegisterClient {
                client_id: client.clone(),
                current_seq: None,
                known_incarnation: None,
                reply,
            })
            .await
            .unwrap()
            .unwrap();
        // The fast tick writes the registration; once removed, only the stop writes it again
        // (activity alone does not make the roster dirty).
        let roster = room_dir(&dir, &room_id).join(format!("meta_clients_{room_id}.json"));
        wait_until("the registration is written", || roster.exists()).await;
        fs::remove_file(&roster).unwrap();
        manager
            .ask(&room_id, |reply| RoomCommand::Heartbeat {
                client_id: client.clone(),
                reply,
            })
            .await
            .unwrap()
            .unwrap();

        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let reached_tx = reached_tx.clone();
        fail_point::arm_hook("room_shutdown", &room_dir(&dir, &room_id), move || {
            reached_tx.send(()).unwrap();
            tokio::task::block_in_place(|| release_rx.recv_timeout(Duration::from_secs(10)))
                .unwrap();
        });
        rooms.push((roster, release_tx));
    }

    let shutdown = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move { manager.shutdown_all().await }
    });
    // Every room is in its shutdown before any of them is let go.
    for _ in &rooms {
        reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the rooms shut down one at a time");
    }
    for (_, release) in &rooms {
        release.send(()).unwrap();
    }
    shutdown.await.unwrap();
    for (roster, _) in &rooms {
        assert!(roster.exists(), "{} was not saved", roster.display());
    }
}

/// The test schema with its table changed by `change`.
fn variant_of_test_schema(change: impl FnOnce(TableBuilder) -> TableBuilder) -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    Schema::builder().table(change(table)).build()
}

#[tokio::test]
async fn a_room_moves_only_to_a_schema_with_the_same_constraints_owners_and_immutability() {
    let dir = tempdir().unwrap();
    let manager = new_manager(&dir);
    let variants = [
        (
            "unique",
            variant_of_test_schema(|t| t.unique("tasks_title", ["title"])),
        ),
        (
            "owned",
            variant_of_test_schema(|t| t.owner("title", OwnerIdentity::ClientId)),
        ),
        ("immutable", variant_of_test_schema(TableBuilder::immutable)),
    ];
    for (id, schema) in &variants {
        manager
            .schema_registry
            .register_schema(SchemaId::new(*id).unwrap(), schema.clone())
            .unwrap();
    }
    let plain = RoomId::new("plain-room").unwrap();
    let strict = RoomId::new("strict-room").unwrap();
    for (room_id, schema) in [(&plain, "first"), (&strict, "unique")] {
        manager
            .create_room(room_id.clone(), SchemaId::new(schema).unwrap(), None)
            .await
            .unwrap();
        manager.shutdown_room(room_id).await;
    }

    let moves = [
        (&plain, "unique"),
        (&plain, "owned"),
        (&plain, "immutable"),
        (&strict, "first"),
        (&strict, "owned"),
    ];
    for (room_id, target) in moves {
        let meta = fs::read(meta_room_path(&dir, room_id)).unwrap();
        let res = manager
            .get_or_spawn(room_id, Some(&SchemaId::new(target).unwrap()))
            .await;
        assert!(
            matches!(res, Err(ServerError::BadRequest(_))),
            "{room_id} to {target}: {res:?}"
        );
        assert_eq!(fs::read(meta_room_path(&dir, room_id)).unwrap(), meta);
    }

    // Both rooms still open with their own schema, and a schema with the same constraints
    // is still accepted.
    let sender = manager.get_or_spawn(&strict, None).await.unwrap();
    assert_eq!(schema_of(&sender).await, SchemaId::new("unique").unwrap());
    // A running room is never reassigned: its actor keeps its schema and its metadata.
    let meta = fs::read(meta_room_path(&dir, &strict)).unwrap();
    let running = manager
        .get_or_spawn(&strict, Some(&SchemaId::new("first").unwrap()))
        .await
        .unwrap();
    assert_eq!(schema_of(&running).await, SchemaId::new("unique").unwrap());
    assert_eq!(fs::read(meta_room_path(&dir, &strict)).unwrap(), meta);
    manager.shutdown_room(&plain).await;
    let sender = manager
        .get_or_spawn(&plain, Some(&SchemaId::new("second").unwrap()))
        .await
        .unwrap();
    assert_eq!(schema_of(&sender).await, SchemaId::new("second").unwrap());
    manager.shutdown_all().await;
}

#[tokio::test]
async fn a_room_moves_only_to_a_schema_that_extends_its_own() {
    let dir = tempdir().unwrap();
    let manager = new_manager(&dir);
    let tasks = || TableSchema::builder("tasks").primary_key("id", DataType::Int);
    let one = |table: TableBuilder| Schema::builder().table(table).build();
    let variants = [
        ("fewer", one(tasks())),
        ("renamed", one(tasks().column("name", DataType::String))),
        ("retyped", one(tasks().column("title", DataType::Int))),
        (
            "loose",
            one(tasks().nullable_column("title", DataType::String)),
        ),
        (
            "sealed",
            one(tasks().encrypted_column("title", DataType::String)),
        ),
        (
            "strict-extra",
            one(tasks()
                .column("title", DataType::String)
                .column("extra", DataType::Int)),
        ),
        (
            "other-key",
            one(tasks().primary_key("title", DataType::String)),
        ),
        (
            "other-name",
            one(TableSchema::builder("todos")
                .primary_key("id", DataType::Int)
                .column("title", DataType::String)),
        ),
        (
            "other-id",
            one(tasks().table_id(1).column("title", DataType::String)),
        ),
        (
            "wider",
            Schema::builder()
                .table(
                    tasks()
                        .column("title", DataType::String)
                        .nullable_column("note", DataType::Int),
                )
                .table(TableSchema::builder("notes").primary_key("id", DataType::Int))
                .build(),
        ),
    ];
    for (id, schema) in &variants {
        manager
            .schema_registry
            .register_schema(SchemaId::new(*id).unwrap(), schema.clone())
            .unwrap();
    }
    let plain = RoomId::new("plain-room").unwrap();
    let wide = RoomId::new("wide-room").unwrap();
    for (room_id, schema) in [(&plain, "first"), (&wide, "wider")] {
        manager
            .create_room(room_id.clone(), SchemaId::new(schema).unwrap(), None)
            .await
            .unwrap();
        manager.shutdown_room(room_id).await;
    }

    let moves = [
        (&plain, "fewer"),
        (&plain, "renamed"),
        (&plain, "retyped"),
        (&plain, "loose"),
        (&plain, "sealed"),
        (&plain, "strict-extra"),
        (&plain, "other-key"),
        (&plain, "other-name"),
        (&plain, "other-id"),
        (&wide, "first"),
    ];
    for (room_id, target) in moves {
        let meta = fs::read(meta_room_path(&dir, room_id)).unwrap();
        let res = manager
            .get_or_spawn(room_id, Some(&SchemaId::new(target).unwrap()))
            .await;
        assert!(
            matches!(res, Err(ServerError::BadRequest(_))),
            "{room_id} to {target}: {res:?}"
        );
        assert_eq!(fs::read(meta_room_path(&dir, room_id)).unwrap(), meta);
    }

    // The rooms still open with their own schema, and a schema that only adds a nullable
    // column and a table is accepted.
    let sender = manager.get_or_spawn(&wide, None).await.unwrap();
    assert_eq!(schema_of(&sender).await, SchemaId::new("wider").unwrap());
    let sender = manager
        .get_or_spawn(&plain, Some(&SchemaId::new("wider").unwrap()))
        .await
        .unwrap();
    assert_eq!(schema_of(&sender).await, SchemaId::new("wider").unwrap());
    manager.shutdown_all().await;
}
