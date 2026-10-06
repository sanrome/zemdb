use super::*;
use crate::durable;
use crate::fail_point;
use std::collections::HashSet;
use tempfile::tempdir;

fn seq(n: u64) -> SequenceNumber {
    SequenceNumber::new(n)
}

fn persisted_cursor(path: &Path, client_id: &ClientId) -> Option<SequenceNumber> {
    let roster: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
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
    fs::write(&path, b"[{\"client_id\": \"a\", \"sta").unwrap();

    let tracker = ClientLeaseTracker::open_or_create(&path).unwrap();

    assert_eq!(tracker.client_counts().4, 0);
}

#[test]
fn interrupted_roster_save_keeps_previous_roster() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let first = ClientId::new("first").unwrap();
    let second = ClientId::new("second").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&first, None, seq(1)).unwrap();

    fail_point::arm("write_atomic_before_rename", &path);
    assert!(tracker.register_client(&second, None, seq(1)).is_err());
    // A failed registration leaves no trace in memory either.
    assert!(!tracker.is_registered(&second));

    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.is_registered(&first));
    assert!(!reopened.is_registered(&second));
}

#[test]
fn failed_re_registration_keeps_the_previous_entry() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker
        .register_client(&client, Some(seq(5)), seq(1))
        .unwrap();

    // Re-registering with a cursor behind the log would make it Bootstrapping at 0.
    fail_point::arm("write_atomic_before_rename", &path);
    assert!(tracker.register_client(&client, None, seq(3)).is_err());

    let entry = tracker.get_client(&client).unwrap();
    assert_eq!(entry.state, ClientState::Connected);
    assert_eq!(entry.last_ack_seq, seq(5));
}

#[test]
fn failed_deregistration_keeps_the_client() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, None, seq(1)).unwrap();

    fail_point::arm("write_atomic_before_rename", &path);
    assert!(tracker.deregister_client(&client).is_err());
    assert!(tracker.is_registered(&client));
    assert!(ClientLeaseTracker::open_or_create(&path)
        .unwrap()
        .is_registered(&client));

    // Deregistering a client that is not registered writes nothing and succeeds.
    assert!(!tracker
        .deregister_client(&ClientId::new("other").unwrap())
        .unwrap());
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
    tracker.register_client(&client, None, seq(1)).unwrap();
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
    tracker.register_client(&client, None, seq(1)).unwrap();
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
    tracker
        .register_client(&client, Some(seq(3)), tail)
        .unwrap();
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
    tracker
        .register_client(&inside, Some(seq(8)), seq(1))
        .unwrap();
    tracker
        .register_client(&behind, Some(seq(2)), seq(1))
        .unwrap();
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
    tracker.register_client(&client, None, seq(10)).unwrap();
    tracker.observe(&client, Some(seq(9)), seq(10));
    tracker.persist_if_dirty().unwrap();

    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.is_connected(&client));
}

#[test]
fn roster_entry_with_invalid_client_id_is_skipped_and_the_rest_kept() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    fs::write(
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
    assert!(!tracker.is_registered(&ClientId::new("carol").unwrap()));
}

#[test]
fn activity_of_an_unregistered_client_does_not_register_it() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let stranger = ClientId::new("stranger").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert_eq!(tracker.observe(&stranger, Some(seq(3)), seq(1)), None);

    assert!(!tracker.is_registered(&stranger));
    assert_eq!(tracker.min_connected_ack_seq(), None);
}

#[test]
fn disconnected_client_inside_the_log_stays_disconnected_by_default() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client").unwrap();

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker
        .register_client(&client, Some(seq(5)), seq(1))
        .unwrap();
    tracker.get_client_mut(&client).unwrap().last_heartbeat =
        Instant::now() - Duration::from_secs(120);

    tracker.check_timeouts(Duration::from_secs(5), None, seq(1));
    tracker.check_timeouts(Duration::from_secs(5), None, seq(1));

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
    tracker
        .register_client(&client, Some(seq(5)), seq(1))
        .unwrap();
    tracker.get_client_mut(&client).unwrap().last_heartbeat =
        Instant::now() - Duration::from_secs(6);

    tracker.check_timeouts(lease, None, seq(6));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Disconnected
    );
    tracker.check_timeouts(lease, None, seq(6));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Disconnected,
        "a cursor at tail - 1 can still catch up from the log"
    );

    tracker.check_timeouts(lease, None, seq(7));
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
    tracker
        .register_client(&client, Some(seq(5)), seq(1))
        .unwrap();
    tracker.get_client_mut(&client).unwrap().last_heartbeat =
        Instant::now() - Duration::from_secs(30);

    tracker.check_timeouts(lease, dormant_after, seq(1));
    tracker.check_timeouts(lease, dormant_after, seq(1));
    assert_eq!(
        tracker.get_client(&client).unwrap().state,
        ClientState::Disconnected
    );

    tracker.get_client_mut(&client).unwrap().last_heartbeat =
        Instant::now() - Duration::from_secs(61);
    tracker.check_timeouts(lease, dormant_after, seq(1));
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
    tracker
        .register_client(&ahead, Some(seq(9)), seq(5))
        .unwrap();
    tracker
        .register_client(&recent, Some(seq(7)), seq(5))
        .unwrap();
    tracker
        .register_client(&older, Some(seq(7)), seq(5))
        .unwrap();
    tracker
        .register_client(&away, Some(seq(20)), seq(5))
        .unwrap();
    tracker.register_client(&newcomer, None, seq(5)).unwrap();
    tracker.get_client_mut(&away).unwrap().state = ClientState::Disconnected;
    tracker.get_client_mut(&older).unwrap().last_heartbeat =
        Instant::now() - Duration::from_secs(10);

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
    tracker.register_client(&client, None, seq(1)).unwrap();
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
    assert!(reopened.is_registered(&client));

    tracker.set_snapshot_demand(None);
    tracker.persist_if_dirty().unwrap();
    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert_eq!(reopened.snapshot_demand(), None);
}

#[test]
fn roster_in_the_array_format_still_loads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    fs::write(
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
    fs::write(
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
    assert!(tracker.is_registered(&ClientId::new("alice").unwrap()));
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
