use super::*;

const TTL: Duration = Duration::from_secs(600);
const TIMEOUT: Duration = Duration::from_secs(60);

fn client(name: &str) -> ClientId {
    ClientId::new(name).unwrap()
}

/// Connected clients in order of preference.
struct Room {
    connected: Vec<ClientId>,
}

impl Room {
    fn new(names: &[&str]) -> Self {
        Self {
            connected: names.iter().map(|n| client(n)).collect(),
        }
    }

    fn designate(
        &self,
        demand: &mut SnapshotDemand,
        now: Instant,
        upload_in_progress: bool,
    ) -> Option<ClientId> {
        demand.designate(
            now,
            upload_in_progress,
            |c| self.connected.contains(c),
            |excluded| {
                self.connected
                    .iter()
                    .find(|c| !excluded.contains(*c))
                    .cloned()
            },
        )
    }
}

#[test]
fn demand_turns_on_renews_and_expires() {
    let start = Instant::now();
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    assert!(!demand.is_active());
    assert!(!demand.expire(start + 2 * TTL));

    demand.renew(start);
    assert!(demand.is_active());
    assert!(!demand.expire(start + TTL / 2));

    // A renewal pushes the expiry back.
    demand.renew(start + TTL / 2);
    assert!(!demand.expire(start + TTL));
    assert!(demand.is_active());

    assert!(demand.expire(start + TTL / 2 + TTL));
    assert!(!demand.is_active());
}

#[test]
fn usable_snapshot_satisfies_the_demand() {
    let start = Instant::now();
    let room = Room::new(&["a"]);
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    demand.renew(start);
    assert_eq!(room.designate(&mut demand, start, false), Some(client("a")));

    demand.satisfy();
    assert!(!demand.is_active());
    assert_eq!(demand.designee(), None);
    assert_eq!(room.designate(&mut demand, start, false), None);
}

#[test]
fn nobody_is_designated_without_demand() {
    let room = Room::new(&["a"]);
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    assert_eq!(room.designate(&mut demand, Instant::now(), false), None);
    assert_eq!(demand.designee(), None);
}

#[test]
fn designee_keeps_its_role_until_it_times_out() {
    let start = Instant::now();
    let room = Room::new(&["a", "b"]);
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    demand.renew(start);

    assert_eq!(room.designate(&mut demand, start, false), Some(client("a")));
    // Not reported again while it keeps the role.
    assert_eq!(
        room.designate(&mut demand, start + TIMEOUT / 2, false),
        None
    );
    assert_eq!(demand.designee(), Some(&client("a")));

    assert_eq!(
        room.designate(&mut demand, start + TIMEOUT, false),
        Some(client("b"))
    );
    assert_eq!(demand.designee(), Some(&client("b")));
}

#[test]
fn designee_that_stops_being_connected_is_replaced() {
    let start = Instant::now();
    let mut room = Room::new(&["a", "b"]);
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    demand.renew(start);
    assert_eq!(room.designate(&mut demand, start, false), Some(client("a")));

    room.connected.remove(0);
    assert_eq!(
        room.designate(&mut demand, start + Duration::from_secs(1), false),
        Some(client("b"))
    );
}

#[test]
fn designation_does_not_change_while_an_upload_is_in_progress() {
    let start = Instant::now();
    let mut room = Room::new(&["a", "b"]);
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    demand.renew(start);
    assert_eq!(room.designate(&mut demand, start, false), Some(client("a")));

    // Timed out and disconnected, but an upload is under way.
    room.connected.remove(0);
    assert_eq!(room.designate(&mut demand, start + 2 * TIMEOUT, true), None);
    assert_eq!(demand.designee(), Some(&client("a")));

    // Once the upload is over without a usable snapshot, the role moves on.
    assert_eq!(
        room.designate(&mut demand, start + 2 * TIMEOUT, false),
        Some(client("b"))
    );
}

#[test]
fn replaced_designees_are_skipped_until_every_candidate_had_its_turn() {
    let start = Instant::now();
    let room = Room::new(&["a", "b"]);
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    demand.renew(start);

    assert_eq!(room.designate(&mut demand, start, false), Some(client("a")));
    let t1 = start + TIMEOUT;
    assert_eq!(room.designate(&mut demand, t1, false), Some(client("b")));
    // Both were excluded: the round starts over with the preferred client.
    let t2 = t1 + TIMEOUT;
    assert_eq!(room.designate(&mut demand, t2, false), Some(client("a")));
    let t3 = t2 + TIMEOUT;
    assert_eq!(room.designate(&mut demand, t3, false), Some(client("b")));
}

#[test]
fn single_candidate_is_designated_again_after_timing_out() {
    let start = Instant::now();
    let room = Room::new(&["a"]);
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    demand.renew(start);

    assert_eq!(room.designate(&mut demand, start, false), Some(client("a")));
    assert_eq!(
        room.designate(&mut demand, start + TIMEOUT, false),
        Some(client("a"))
    );
}

#[test]
fn demand_without_candidates_waits_for_one() {
    let start = Instant::now();
    let mut room = Room::new(&[]);
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    demand.renew(start);

    assert_eq!(room.designate(&mut demand, start, false), None);
    assert!(demand.is_active());
    assert_eq!(demand.designee(), None);

    room.connected.push(client("late"));
    assert_eq!(
        room.designate(&mut demand, start + Duration::from_secs(1), false),
        Some(client("late"))
    );
}

#[test]
fn a_new_episode_forgets_the_previous_exclusions() {
    let start = Instant::now();
    let room = Room::new(&["a", "b"]);
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);
    demand.renew(start);
    room.designate(&mut demand, start, false);
    assert_eq!(
        room.designate(&mut demand, start + TIMEOUT, false),
        Some(client("b"))
    );

    demand.satisfy();
    demand.renew(start + 2 * TIMEOUT);
    assert_eq!(
        room.designate(&mut demand, start + 2 * TIMEOUT, false),
        Some(client("a"))
    );
}

#[test]
fn persisted_demand_is_restored_with_its_remaining_lifetime() {
    let now = Instant::now();
    let wall_now = SystemTime::now();
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);

    assert!(demand.restore(wall_now - TTL / 2, wall_now, now));
    assert!(demand.is_active());
    assert_eq!(demand.designee(), None);
    assert!(!demand.expire(now + TTL / 2 - Duration::from_secs(1)));
    assert!(demand.expire(now + TTL / 2));
}

#[test]
fn persisted_demand_older_than_the_ttl_is_not_restored() {
    let now = Instant::now();
    let wall_now = SystemTime::now();
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);

    assert!(!demand.restore(wall_now - TTL, wall_now, now));
    assert!(!demand.is_active());
}

#[test]
fn persisted_renewal_in_the_future_counts_as_renewed_now() {
    let now = Instant::now();
    let wall_now = SystemTime::now();
    let mut demand = SnapshotDemand::new(TTL, TIMEOUT);

    // The wall clock moved backwards since the renewal: the demand still expires one TTL
    // from now instead of living until the clock catches up.
    assert!(demand.restore(wall_now + 100 * TTL, wall_now, now));
    assert!(!demand.expire(now + TTL - Duration::from_secs(1)));
    assert!(demand.expire(now + TTL));
}

#[test]
fn restored_demand_older_than_the_machine_uptime_keeps_its_remaining_lifetime() {
    // Ages far beyond any uptime: a monotonic clock cannot represent the renewal instant.
    let year = Duration::from_secs(365 * 24 * 60 * 60);
    let now = Instant::now();
    let wall_now = SystemTime::now();
    let mut demand = SnapshotDemand::new(20 * year, TIMEOUT);

    assert!(demand.restore(wall_now - 15 * year, wall_now, now));
    assert!(!demand.expire(now + 4 * year));
    assert!(demand.expire(now + 6 * year));
}

#[test]
fn persisted_renewal_granularity_is_a_hundredth_of_the_ttl_up_to_an_hour() {
    assert_eq!(
        persistence_granularity(Duration::from_secs(1000)),
        Duration::from_secs(10)
    );
    assert_eq!(
        persistence_granularity(Duration::from_secs(7 * 24 * 60 * 60)),
        Duration::from_secs(60 * 60)
    );
}
