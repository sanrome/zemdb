use super::*;
use crate::durable;
use crate::fail_point;
use tempfile::tempdir;

fn seq(n: u64) -> SequenceNumber {
    SequenceNumber::new(n)
}

fn persisted_cursor(path: &Path, client_id: &ClientId) -> Option<SequenceNumber> {
    let entries: Vec<ClientEntry> =
        serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
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
    let first = ClientId::new("first");
    let second = ClientId::new("second");

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&first, None, seq(1)).unwrap();

    fail_point::arm("write_atomic_before_rename", &path);
    assert!(tracker.register_client(&second, None, seq(1)).is_err());

    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.is_registered(&first));
    assert!(!reopened.is_registered(&second));
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
    let client = ClientId::new("client");

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, None, seq(1)).unwrap();
    tracker.record_ack(&client, seq(5));
    tracker.advance_cursor(&client, seq(7));

    assert_eq!(persisted_cursor(&path, &client), Some(seq(0)));

    tracker.persist_if_dirty().unwrap();
    assert_eq!(persisted_cursor(&path, &client), Some(seq(7)));
}

#[test]
fn advance_cursor_is_monotonic_and_keeps_state() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client");

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, None, seq(10)).unwrap();
    assert!(tracker.is_bootstrapping(&client));

    tracker.advance_cursor(&client, seq(4));
    tracker.advance_cursor(&client, seq(2));

    assert_eq!(tracker.get_client(&client).unwrap().last_ack_seq, seq(4));
    assert!(tracker.is_bootstrapping(&client));
}

#[test]
fn activity_that_changes_state_is_persisted_on_flush() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta_clients_room.json");
    let client = ClientId::new("client");

    let mut tracker = ClientLeaseTracker::open_or_create(&path).unwrap();
    tracker.register_client(&client, None, seq(10)).unwrap();
    tracker.record_activity(&client);
    tracker.persist_if_dirty().unwrap();

    let reopened = ClientLeaseTracker::open_or_create(&path).unwrap();
    assert!(reopened.is_connected(&client));
}
