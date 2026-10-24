use super::*;
use crate::actor::test_support::seq;
use crate::durable;
use crate::fail_point;
use std::collections::HashSet;
use tempfile::tempdir;

/// Backdates the last heartbeat of `client` by `secs` seconds.
fn set_inactive_for(tracker: &mut ClientLeaseTracker, client: &ClientId, secs: u64) {
    tracker.get_client_mut(client).unwrap().last_heartbeat =
        Instant::now() - Duration::from_secs(secs);
}

fn persisted_cursor(path: &Path, client_id: &ClientId) -> Option<SequenceNumber> {
    let roster: serde_json::Value =
        serde_json::from_str(durable::verified_json(&fs::read(path).unwrap()).unwrap()).unwrap();
    let entries: Vec<ClientEntry> = serde_json::from_value(roster["clients"].clone()).unwrap();
    entries
        .into_iter()
        .find(|e| &e.client_id == client_id)
        .map(|e| e.last_ack_seq)
}

#[test]
fn unreadable_roster_opens_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&ClientId::new("a").unwrap(), None, seq(1));
    tracker.save().unwrap();
    let saved = fs::read(&path).unwrap();

    // Truncated, empty, not UTF-8, and one bit flipped in the cursor, which keeps the JSON valid.
    let mut not_utf8 = saved.clone();
    not_utf8[0] ^= 0x80;
    let mut flipped = saved.clone();
    let needle = b"\"last_ack_seq\":";
    flipped[saved
        .windows(needle.len())
        .position(|w| w == needle)
        .unwrap()
        + needle.len()] ^= 1;
    for damaged in [
        saved[..saved.len() / 2].to_vec(),
        Vec::new(),
        not_utf8,
        flipped,
    ] {
        fs::write(&path, damaged).unwrap();
        let tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
        assert_eq!(tracker.client_counts().4, 0);
    }
}

/// Every single bit flip and every truncation of a roster (two clients and a snapshot demand)
/// discards it whole: no client, no cursor and no demand from a file that did not verify.
#[test]
fn every_damage_to_the_roster_discards_it() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&ClientId::new("a").unwrap(), None, seq(7));
    tracker.register_client(&ClientId::new("b").unwrap(), None, seq(9));
    tracker.set_snapshot_demand(Some(SystemTime::now()));
    tracker.save().unwrap();
    let saved = fs::read(&path).unwrap();
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert_eq!(reopened.clients.len(), 2);
    assert!(reopened.snapshot_demand().is_some());

    for (damage, bytes) in crate::test_support::every_truncation_and_bit_flip(&saved) {
        fs::write(&path, &bytes).unwrap();
        let tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
        assert!(
            tracker.clients.is_empty() && tracker.snapshot_demand().is_none(),
            "a roster {damage} kept {} clients and the demand {:?}: {:?}",
            tracker.clients.len(),
            tracker.snapshot_demand(),
            String::from_utf8_lossy(&bytes)
        );
    }
}

#[test]
fn interrupted_roster_save_keeps_previous_roster() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let first = ClientId::new("first").unwrap();
    let second = ClientId::new("second").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&first, None, seq(1));
    tracker.save().unwrap();
    tracker.register_client(&second, None, seq(1));

    fail_point::arm("write_atomic_before_rename", &path);
    assert!(tracker.persist_if_dirty().is_err());
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.get_client(&first).is_some());
    assert!(reopened.get_client(&second).is_none());

    // The change stays pending for the next attempt.
    tracker.persist_if_dirty().unwrap();
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.get_client(&second).is_some());
}

#[test]
fn membership_changes_are_written_only_when_flushed() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    let clients: Vec<ClientId> = (0..1000)
        .map(|i| ClientId::new(format!("client-{i}")).unwrap())
        .collect();
    for client in &clients {
        tracker.register_client(client, None, seq(1));
    }
    assert!(!path.exists());

    tracker.persist_if_dirty().unwrap();
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert_eq!(reopened.client_counts().4, clients.len());

    tracker.deregister_client(&clients[0]);
    assert!(tracker.get_client(&clients[0]).is_none());
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.get_client(&clients[0]).is_some());
    tracker.persist_if_dirty().unwrap();
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.get_client(&clients[0]).is_none());
}

#[test]
fn stale_roster_tmp_is_removed_on_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let tmp = durable::tmp_path_for(&path);
    fs::write(&tmp, b"partial roster left by a crash").unwrap();

    ClientLeaseTracker::open_or_create(&path).unwrap();

    assert!(!tmp.exists());
}

#[test]
fn cursor_updates_are_persisted_only_when_flushed() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, None, seq(1));
    tracker.save().unwrap();
    tracker.observe(&client, Some(seq(5)), seq(1));
    tracker.observe(&client, Some(seq(7)), seq(1));

    assert_eq!(persisted_cursor(&path, &client), Some(seq(0)));

    tracker.persist_if_dirty().unwrap();
    assert_eq!(persisted_cursor(&path, &client), Some(seq(7)));
}

#[test]
fn observe_moves_the_cursor_only_forward() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, None, seq(1));
    tracker.observe(&client, Some(seq(4)), seq(1));
    let state = tracker.observe(&client, Some(seq(2)), seq(1));

    assert_eq!(state, Some(ClientState::Connected));
    assert_eq!(tracker.get_client(&client).unwrap().last_ack_seq, seq(4));
}

#[test]
fn observe_derives_the_state_from_the_cursor() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();
    let tail = seq(10);

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, Some(seq(3)), tail);
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Bootstrapping
    );

    // Activity without a cursor, or with one still behind the log, keeps it bootstrapping and
    // leaves the cursor alone.
    assert_eq!(
        tracker.observe(&client, None, tail),
        Some(ClientState::Bootstrapping)
    );
    assert_eq!(
        tracker.observe(&client, Some(seq(7)), tail),
        Some(ClientState::Bootstrapping)
    );
    assert_eq!(tracker.get_client(&client).unwrap().last_ack_seq, seq(3));

    // tail - 1 is the oldest cursor the log can continue from.
    assert_eq!(
        tracker.observe(&client, Some(seq(9)), tail),
        Some(ClientState::Connected)
    );
    assert_eq!(tracker.get_client(&client).unwrap().last_ack_seq, seq(9));

    // Once the log moves past the cursor, the next activity makes it bootstrap again.
    assert_eq!(
        tracker.observe(&client, None, seq(12)),
        Some(ClientState::Bootstrapping)
    );
}

#[test]
fn observe_reactivates_inactive_clients_according_to_their_cursor() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let inside = ClientId::new("inside").unwrap();
    let behind = ClientId::new("behind").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&inside, Some(seq(8)), seq(1));
    tracker.register_client(&behind, Some(seq(2)), seq(1));
    tracker.get_client_mut(&inside).unwrap().state = ClientState::Dormant;
    tracker.get_client_mut(&behind).unwrap().state = ClientState::Disconnected;

    let tail = seq(6);
    assert_eq!(
        tracker.observe(&inside, None, tail),
        Some(ClientState::Connected)
    );
    assert_eq!(
        tracker.observe(&behind, None, tail),
        Some(ClientState::Bootstrapping)
    );
}

#[test]
fn activity_that_changes_state_is_persisted_on_flush() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, None, seq(10));
    tracker.observe(&client, Some(seq(9)), seq(10));
    tracker.persist_if_dirty().unwrap();

    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.is_connected(&client));
}

#[test]
fn roster_entry_with_invalid_client_id_is_skipped_and_the_rest_kept() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    durable::write_checked(
        &path,
        br#"[
            {"client_id": "alice", "state": "Connected", "last_ack_seq": 3},
            {"client_id": "", "state": "Connected", "last_ack_seq": 1},
            {"client_id": "bad\u0000id", "state": "Connected", "last_ack_seq": 1},
            {"client_id": "carol", "state": "NoSuchState", "last_ack_seq": 1},
            {"client_id": "bob", "state": "Disconnected", "last_ack_seq": 5}
        ]"#,
    )
    .unwrap();

    let tracker = ClientLeaseTracker::open_or_create(&path).unwrap();

    let alice = ClientId::new("alice").unwrap();
    let bob = ClientId::new("bob").unwrap();
    assert_eq!(tracker.client_counts().4, 2);
    assert_eq!(tracker.get_client(&alice).unwrap().last_ack_seq, seq(3));
    assert_eq!(tracker.get_client(&bob).unwrap().last_ack_seq, seq(5));
    assert!(tracker
        .get_client(&ClientId::new("carol").unwrap())
        .is_none());
}

#[test]
fn activity_of_an_unregistered_client_does_not_register_it() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let stranger = ClientId::new("stranger").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert_eq!(tracker.observe(&stranger, Some(seq(3)), seq(1)), None);

    assert!(tracker.get_client(&stranger).is_none());
    assert_eq!(tracker.min_connected_ack_seq(), None);
}

#[test]
fn disconnected_client_inside_the_log_stays_disconnected_by_default() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, Some(seq(5)), seq(1));
    set_inactive_for(&mut tracker, &client, 120);

    tracker.check_timeouts(Duration::from_secs(5), None, Duration::MAX, seq(1));
    tracker.check_timeouts(Duration::from_secs(5), None, Duration::MAX, seq(1));

    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Disconnected
    );
    assert_eq!(tracker.min_connected_ack_seq(), None);
}

#[test]
fn disconnected_client_becomes_dormant_once_behind_the_log() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();
    let lease = Duration::from_secs(5);

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, Some(seq(5)), seq(1));
    set_inactive_for(&mut tracker, &client, 6);

    tracker.check_timeouts(lease, None, Duration::MAX, seq(6));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Disconnected
    );
    tracker.check_timeouts(lease, None, Duration::MAX, seq(6));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Disconnected,
        "a cursor at tail - 1 can still catch up from the log"
    );

    tracker.check_timeouts(lease, None, Duration::MAX, seq(7));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Dormant
    );
}

#[test]
fn configured_dormancy_timeout_makes_a_disconnected_client_dormant() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();
    let lease = Duration::from_secs(5);
    let dormant_after = Some(Duration::from_secs(60));

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, Some(seq(5)), seq(1));
    set_inactive_for(&mut tracker, &client, 30);

    tracker.check_timeouts(lease, dormant_after, Duration::MAX, seq(1));
    tracker.check_timeouts(lease, dormant_after, Duration::MAX, seq(1));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Disconnected
    );

    set_inactive_for(&mut tracker, &client, 61);
    tracker.check_timeouts(lease, dormant_after, Duration::MAX, seq(1));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Dormant
    );
}

#[test]
fn uploader_is_the_connected_client_with_the_highest_cursor() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let ahead = ClientId::new("ahead").unwrap();
    let recent = ClientId::new("recent").unwrap();
    let older = ClientId::new("older").unwrap();
    let away = ClientId::new("away").unwrap();
    let newcomer = ClientId::new("newcomer").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&ahead, Some(seq(9)), seq(5));
    tracker.register_client(&recent, Some(seq(7)), seq(5));
    tracker.register_client(&older, Some(seq(7)), seq(5));
    tracker.register_client(&away, Some(seq(20)), seq(5));
    tracker.register_client(&newcomer, None, seq(5));
    tracker.get_client_mut(&away).unwrap().state = ClientState::Disconnected;
    set_inactive_for(&mut tracker, &older, 10);

    let mut excluded = HashSet::new();
    assert_eq!(tracker.pick_uploader(&excluded), Some(ahead.clone()));

    // Equal cursors: the most recently active one.
    excluded.insert(ahead.clone());
    assert_eq!(tracker.pick_uploader(&excluded), Some(recent.clone()));

    // Neither inactive nor bootstrapping clients are candidates.
    excluded.insert(recent.clone());
    excluded.insert(older.clone());
    assert_eq!(tracker.pick_uploader(&excluded), None);
}

/// A wall-clock time with the millisecond precision the roster stores.
fn wall_time_ms(ms: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(ms)
}

#[test]
fn snapshot_demand_is_persisted_on_flush_and_survives_reopening() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();
    let renewed_at = wall_time_ms(1_790_000_000_123);

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, None, seq(1));
    tracker.set_snapshot_demand(Some(renewed_at));
    assert_eq!(
        ClientLeaseTracker::open_or_create(&path)
            .unwrap()
            .snapshot_demand(),
        None,
        "the demand is only written by a flush"
    );

    tracker.persist_if_dirty().unwrap();
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert_eq!(reopened.snapshot_demand(), Some(renewed_at));
    assert!(reopened.get_client(&client).is_some());

    tracker.set_snapshot_demand(None);
    tracker.persist_if_dirty().unwrap();
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert_eq!(reopened.snapshot_demand(), None);
}

#[test]
fn roster_in_the_array_format_still_loads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    durable::write_checked(
        &path,
        br#"[{"client_id": "alice", "state": "Connected", "last_ack_seq": 3}]"#,
    )
    .unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    let alice = ClientId::new("alice").unwrap();
    assert_eq!(tracker.get_client(&alice).unwrap().last_ack_seq, seq(3));
    assert_eq!(tracker.snapshot_demand(), None);

    // The next save writes the current format, which loads the same clients.
    tracker.save().unwrap();
    assert_eq!(persisted_cursor(&path, &alice), Some(seq(3)));
}

#[test]
fn unreadable_snapshot_demand_keeps_the_clients() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    durable::write_checked(
        &path,
        br#"{
            "clients": [
                {"client_id": "alice", "state": "Connected", "last_ack_seq": 3},
                {"client_id": "", "state": "Connected", "last_ack_seq": 1}
            ],
            "snapshot_demand": {"renewed_at_unix_ms": "yesterday"}
        }"#,
    )
    .unwrap();

    let tracker = ClientLeaseTracker::open_or_create(&path).unwrap();

    assert_eq!(tracker.client_counts().4, 1);
    assert!(tracker
        .get_client(&ClientId::new("alice").unwrap())
        .is_some());
    assert_eq!(tracker.snapshot_demand(), None);
}

#[test]
fn snapshot_demand_renewals_within_the_granularity_do_not_dirty_the_roster() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let granularity = Duration::from_secs(60);
    let start = wall_time_ms(1_790_000_000_000);

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.renew_snapshot_demand(start, granularity);
    assert!(tracker.dirty, "turning the demand on is persisted");
    tracker.persist_if_dirty().unwrap();

    for secs in [1, 30, 59] {
        tracker.renew_snapshot_demand(start + Duration::from_secs(secs), granularity);
        assert!(!tracker.dirty, "renewal after {secs} s dirtied the roster");
    }
    assert_eq!(tracker.snapshot_demand(), Some(start));

    let later = start + granularity;
    tracker.renew_snapshot_demand(later, granularity);
    assert!(tracker.dirty);
    assert_eq!(tracker.snapshot_demand(), Some(later));
    tracker.persist_if_dirty().unwrap();

    // A stored renewal in the future (the clock moved backwards) is replaced.
    tracker.renew_snapshot_demand(start, granularity);
    assert_eq!(tracker.snapshot_demand(), Some(start));
    tracker.persist_if_dirty().unwrap();

    tracker.set_snapshot_demand(None);
    assert!(tracker.dirty, "turning the demand off is persisted");
}

/// How long ago, by the wall clock, the roster on disk says `client_id` was last active.
fn persisted_inactivity(path: &Path, client_id: &ClientId) -> Duration {
    let roster: serde_json::Value =
        serde_json::from_str(durable::verified_json(&fs::read(path).unwrap()).unwrap()).unwrap();
    let entry = roster["clients"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["client_id"] == client_id.as_str())
        .unwrap();
    let last_seen =
        UNIX_EPOCH + Duration::from_millis(entry["last_seen_unix_ms"].as_u64().unwrap());
    SystemTime::now()
        .duration_since(last_seen)
        .unwrap_or_default()
}

#[test]
fn inactivity_survives_reopening_the_roster() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();
    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, Some(seq(5)), seq(1));
    set_inactive_for(&mut tracker, &client, 100);
    tracker.save().unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    let inactive_for = tracker
        .get_client(&client)
        .unwrap()
        .last_heartbeat
        .elapsed();
    assert!(
        (Duration::from_secs(99)..Duration::from_secs(110)).contains(&inactive_for),
        "{inactive_for:?}"
    );
    // The lease and the dormancy timeout keep counting from the last activity.
    let (lease, dormant_after) = (Duration::from_secs(90), Some(Duration::from_secs(60)));
    tracker.check_timeouts(lease, dormant_after, Duration::MAX, seq(1));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Disconnected
    );
    tracker.check_timeouts(lease, dormant_after, Duration::MAX, seq(1));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Dormant
    );
}

#[test]
fn roster_without_last_activity_or_with_a_future_one_loads_clients_as_active() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let tomorrow = SystemTime::now() + Duration::from_secs(86_400);
    let tomorrow_ms = tomorrow.duration_since(UNIX_EPOCH).unwrap().as_millis();
    durable::write_checked(
        &path,
        format!(
            r#"{{"clients": [
                {{"client_id": "old", "state": "Connected", "last_ack_seq": 3}},
                {{"client_id": "future", "state": "Connected", "last_ack_seq": 3,
                  "last_seen_unix_ms": {tomorrow_ms}}}
            ], "snapshot_demand": null}}"#
        )
        .as_bytes(),
    )
    .unwrap();

    let tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    for name in ["old", "future"] {
        let entry = tracker.get_client(&ClientId::new(name).unwrap()).unwrap();
        assert_eq!(entry.last_ack_seq, seq(3));
        assert!(
            entry.last_heartbeat.elapsed() < Duration::from_secs(5),
            "{name}"
        );
    }
}

#[test]
fn activity_alone_is_written_only_when_the_room_stops() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();
    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, Some(seq(5)), seq(1));
    set_inactive_for(&mut tracker, &client, 100);
    tracker.save().unwrap();

    // A heartbeat that changes neither the state nor the cursor.
    tracker.observe(&client, None, seq(1));
    tracker.persist_if_dirty().unwrap();
    assert!(persisted_inactivity(&path, &client) >= Duration::from_secs(99));

    tracker.persist_if_dirty_or_active().unwrap();
    assert!(persisted_inactivity(&path, &client) < Duration::from_secs(5));
}

// Paused time: the Tokio clock runs ahead of the system's monotonic clock once the test
// advances it, so activity saved or restored on the system clock would come out wrong.
#[tokio::test(start_paused = true)]
async fn inactivity_survives_a_save_and_a_reopen_on_the_tokio_clock() {
    let lease = Duration::from_secs(90);
    tokio::time::advance(Duration::from_secs(3600)).await;
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();
    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, Some(seq(1)), seq(1));
    tokio::time::advance(Duration::from_secs(60)).await;
    tracker.save().unwrap();
    drop(tracker);

    // 60 s of the lease had passed, and the wall clock hardly moved since.
    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tokio::time::advance(Duration::from_secs(29)).await;
    tracker.check_timeouts(lease, None, Duration::MAX, seq(1));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Connected
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    tracker.check_timeouts(lease, None, Duration::MAX, seq(1));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Disconnected
    );
}

#[test]
fn activity_before_the_clock_can_reach_saturates() {
    let now = Instant::now();
    let earliest = instant_before(now, Duration::from_secs(u64::MAX / 4));
    assert!(earliest <= now);
    assert_eq!(
        instant_before(now, Duration::from_secs(1)),
        now - Duration::from_secs(1)
    );
}

#[test]
fn dormant_client_is_forgotten_after_its_ttl_from_a_compact_roster() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let (gone, active) = (
        ClientId::new("gone").unwrap(),
        ClientId::new("active").unwrap(),
    );
    let (lease, forget_after) = (Duration::from_secs(5), Duration::from_secs(60));

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    for client in [&gone, &active] {
        tracker.register_client(client, Some(seq(1)), seq(1));
    }
    // Disconnected, then Dormant (its cursor fell behind the log), and kept before its TTL.
    set_inactive_for(&mut tracker, &gone, 59);
    for _ in 0..3 {
        tracker.check_timeouts(lease, None, forget_after, seq(5));
    }
    assert_eq!(
        tracker.get_client(&gone).unwrap().state,
        ClientState::Dormant
    );

    set_inactive_for(&mut tracker, &gone, 61);
    tracker.check_timeouts(lease, None, forget_after, seq(5));
    tracker.persist_if_dirty().unwrap();
    assert!(tracker.get_client(&gone).is_none() && tracker.get_client(&active).is_some());

    // The roster on disk, one compact line, forgot it too.
    let saved = fs::read(&path).unwrap();
    let saved = durable::verified_json(&saved).unwrap();
    assert!(!saved.contains('\n'), "{saved}");
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.get_client(&gone).is_none() && reopened.get_client(&active).is_some());
}

#[test]
fn bootstrapping_clients_do_not_hold_back_pruning() {
    let dir = tempdir().unwrap();
    let mut tracker =
        ClientLeaseTracker::open_or_create(dir.path().join("meta_clients_room.json")).unwrap();
    let active = ClientId::new("active").unwrap();
    let newbie = ClientId::new("newbie").unwrap();
    tracker.register_client(&active, Some(seq(20)), seq(10));
    assert_eq!(
        tracker.register_client(&newbie, None, seq(10)),
        ClientState::Bootstrapping
    );
    assert_eq!(tracker.min_connected_ack_seq(), Some(seq(20)));
}
