//! The scenarios of the reference app, each against a server of its own, with the devices of the
//! friends reaching it over HTTP: every replica reaches the same standings; the replica rejects a
//! second vote, a vote to or from someone who is not a member and a title repeated; a person votes
//! from two devices; the members keep the creator from deleting the ranking; a new member catches
//! up from the snapshot of another; without network the standings read and a vote is `Offline`;
//! the standings watched change as the votes arrive; and the notes never travel in the clear.

use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;
use zemdb_client::{
    ClientConfig, ClientError, Freshness, HttpTransport, HttpTransportConfig, Rejection,
    RoomHandle, TokenProvider, TransportClient, Violation,
};
use zemdb_core::{
    decode_message, ClientId, ClientMessage, CorrelationId, RoomId, SequenceNumber, ServerMessage,
};
use zemdb_storage::snapshot::decode_snapshot_envelope;

use crate::app::*;

/// A scenario, which fails with what it found otherwise.
pub type Scenario = fn() -> BoxFuture<'static, Result<()>>;

/// Every scenario, in order, with what it shows.
pub const ALL: [(&str, Scenario); 8] = [
    ("Every replica reaches the same standings", || {
        same_standings().boxed()
    }),
    (
        "The replica rejects a second vote, a stranger and a title repeated",
        || rejections().boxed(),
    ),
    ("A person votes from two devices", || two_devices().boxed()),
    (
        "The members keep the creator from deleting the ranking",
        || restrict().boxed(),
    ),
    (
        "A new member catches up from the snapshot of another",
        || snapshot().boxed(),
    ),
    (
        "Without network, the standings read and a vote is offline",
        || offline().boxed(),
    ),
    ("The standings watched change as the votes arrive", || {
        watch().boxed()
    }),
    ("The notes never travel in the clear", || notes().boxed()),
];

/// A member of the friends: a device of the person, with the room of the friends open.
struct Member {
    device: Device,
    room: RoomHandle,
}

impl Member {
    /// A new device of `person` in `room_id`, whose worker catches the replica up with each
    /// heartbeat.
    async fn open(server: &Server, room_id: &RoomId, person: &str) -> Result<Self> {
        Self::with(server, room_id, person, |_| {}).await
    }

    /// A new device of `person` in `room_id` that catches up only when its app asks it to, or
    /// writes.
    async fn manual(server: &Server, room_id: &RoomId, person: &str) -> Result<Self> {
        let manual = |config: &mut ClientConfig| config.freshness = Freshness::Manual;
        Self::with(server, room_id, person, manual).await
    }

    /// A new device of `person` in `room_id`, with the configuration of a device that `change`
    /// changes.
    async fn with(
        server: &Server,
        room_id: &RoomId,
        person: &str,
        change: impl FnOnce(&mut ClientConfig),
    ) -> Result<Self> {
        let device = Device::with(server, person, change).await?;
        let room = device.room(room_id).await?;
        Ok(Self { device, room })
    }

    /// Joins `ranking` once invited to it: the replica catches up with it first.
    async fn join(&self, ranking: Uuid, display_name: &str) -> Result<()> {
        self.room.sync().await?;
        Ok(join(&self.room, ranking, display_name).await?)
    }

    /// Votes for `candidate` in `round`, once the replica caught up with it.
    async fn vote(&self, ranking: Uuid, round: Uuid, candidate: &str) -> Result<()> {
        self.room.sync().await?;
        Ok(vote(&self.room, ranking, round, candidate).await?)
    }

    /// Whether the last request of the room got a reply.
    async fn connected(&self) -> Result<Option<bool>> {
        let worker = self.room.status().await?.worker;
        Ok(worker.and_then(|worker| worker.connected))
    }
}

/// A server with the schema of the app, and the room of the friends, which keeps its log as
/// `retention` says.
async fn friends(retention: Retention) -> Result<(Server, RoomId)> {
    let server = Server::start().await?;
    let backend = server.backend();
    backend.register_schema().await?;
    let room_id = backend.create_room("friends", retention).await?;
    Ok((server, room_id))
}

/// A wait for something that the workers do on their own, of at most 30 s.
struct Waiting {
    what: &'static str,
    deadline: Instant,
}

impl Waiting {
    fn until(what: &'static str) -> Self {
        let deadline = Instant::now() + Duration::from_secs(30);
        Self { what, deadline }
    }

    /// Waits a little, or fails once the time is up.
    async fn tick(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(format!("Timed out waiting until {}", self.what).into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }
}

/// Waits until the workers of `members` catch their replicas up with `expected`, the standings of
/// `ranking`.
async fn reach(members: &[&Member], ranking: Uuid, expected: &Standings) -> Result<()> {
    let waiting = Waiting::until("every replica reaches the standings");
    for member in members {
        while standings(&member.room, ranking).await? != *expected {
            waiting.tick().await?;
        }
    }
    Ok(())
}

/// Waits until `stream` emits `expected`, and returns the values it emitted.
async fn emits(
    stream: &mut (impl Stream<Item = Result<Standings, ClientError>> + Unpin),
    expected: &Standings,
) -> Result<Vec<Standings>> {
    let deadline = Waiting::until("the stream emits the standings").deadline;
    let mut emitted = Vec::new();
    while emitted.last() != Some(expected) {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(standings)) => emitted.push(standings?),
            Ok(None) => return Err("The stream ended".into()),
            Err(_) => return Err(format!("Timed out after emitting {emitted:?}").into()),
        }
    }
    Ok(emitted)
}

/// A write that the precheck rejects for `violation`, with nothing sent.
fn prechecked(violation: Violation) -> ClientError {
    ClientError::PrecheckFailed(Rejection::Constraint(violation))
}

fn foreign_key(constraint: &str) -> Violation {
    Violation::ForeignKey {
        constraint: constraint.to_string(),
    }
}

/// Several members vote in a round, and the workers bring every replica to the same standings.
pub async fn same_standings() -> Result<()> {
    let (server, room_id) = friends(Retention::Default).await?;
    let ana = Member::open(&server, &room_id, "ana").await?;
    let bob = Member::open(&server, &room_id, "bob").await?;
    let carla = Member::open(&server, &room_id, "carla").await?;
    let dani = Member::open(&server, &room_id, "dani").await?;
    let ranking = create_ranking(&ana.room, "The Thursday barbecue").await?;
    ana.join(ranking, "Ana").await?;
    bob.join(ranking, "Bob").await?;
    carla.join(ranking, "Carla").await?;
    dani.join(ranking, "Dani").await?;
    let round = open_round(&ana.room, ranking, "Round 1", "Who brings the wine").await?;
    ana.vote(ranking, round, "bob").await?;
    bob.vote(ranking, round, "carla").await?;
    carla.vote(ranking, round, "bob").await?;
    dani.vote(ranking, round, "bob").await?;

    // Without asking: each worker catches its replica up with the heartbeats.
    let expected = standings_of([("bob", 3), ("carla", 1)]);
    reach(&[&ana, &bob, &carla, &dani], ranking, &expected).await?;
    println!("  every replica: {expected:?}");
    Ok(())
}

/// A second vote in the same round is rejected (`Duplicate`), and so are a vote for someone who
/// is not a member and one of someone who is not a member (`ForeignKey`), and a second round
/// with the title of another (`Unique`), which is of the first in the order of the log.
pub async fn rejections() -> Result<()> {
    let (server, room_id) = friends(Retention::Default).await?;
    let ana = Member::open(&server, &room_id, "ana").await?;
    let bob = Member::manual(&server, &room_id, "bob").await?;
    let carla = Member::open(&server, &room_id, "carla").await?;
    let eve = Member::open(&server, &room_id, "eve").await?;
    let ranking = create_ranking(&ana.room, "Truco").await?;
    ana.join(ranking, "Ana").await?;
    bob.join(ranking, "Bob").await?;
    carla.join(ranking, "Carla").await?;
    let round = open_round(&ana.room, ranking, "Round 1", "At Carla's").await?;
    bob.vote(ranking, round, "carla").await?;

    // With the replica in the state of the log, the precheck rejects each, and nothing is sent:
    // the server sequences nothing.
    let (_, head) = server.log(&room_id).await?;
    let second = vote(&bob.room, ranking, round, "ana").await;
    assert_eq!(second, Err(prechecked(Violation::Duplicate)));
    carla.room.sync().await?;
    let to_eve = vote(&carla.room, ranking, round, "eve").await;
    assert_eq!(to_eve, Err(prechecked(foreign_key("votes_candidate"))));
    eve.room.sync().await?;
    let of_eve = vote(&eve.room, ranking, round, "bob").await;
    assert_eq!(of_eve, Err(prechecked(foreign_key("votes_voter"))));
    let unique = Violation::Unique {
        constraint: "rounds_title".to_string(),
    };
    let again = open_round(&carla.room, ranking, "Round 1", "Again").await;
    assert_eq!(again, Err(prechecked(unique.clone())));
    assert_eq!(server.log(&room_id).await?.1, head);
    println!("  rejected before being sent: {second:?}, {to_eve:?}, {of_eve:?}, {again:?}");

    // A title is of the first in the order of the log: bob's replica has not seen carla's
    // second round, so his goes to the server, which sequences it after hers, and his replica
    // rejects it as every replica does.
    let carlas = open_round(&carla.room, ranking, "Round 2", "At Bob's").await?;
    let bobs = open_round(&bob.room, ranking, "Round 2", "At Ana's").await;
    assert_eq!(bobs, Err(ClientError::ConstraintViolation(unique)));
    assert_eq!(server.log(&room_id).await?.1, head + 2);
    let title = (String::from("Round 2"), Some(String::from("At Bob's")));
    assert_eq!(round_of(&bob.room, ranking, carlas).await?, Some(title));
    println!("  rejected by the replica in the order of the log: {bobs:?}");
    Ok(())
}

/// The same person votes from two devices, two client ids of the same owner: their replicas
/// converge, and the second vote is rejected (`Duplicate`).
pub async fn two_devices() -> Result<()> {
    let (server, room_id) = friends(Retention::Default).await?;
    let ana = Member::open(&server, &room_id, "ana").await?;
    let bob = Member::open(&server, &room_id, "bob").await?;
    let phone = Member::open(&server, &room_id, "carla").await?;
    let laptop = Member::manual(&server, &room_id, "carla").await?;
    let (phone_id, laptop_id) = (
        phone.device.client().client_id(),
        laptop.device.client().client_id(),
    );
    assert_ne!(phone_id, laptop_id);
    assert_eq!((phone_id.owner(), laptop_id.owner()), ("carla", "carla"));
    let ranking = create_ranking(&ana.room, "Paddle").await?;
    ana.join(ranking, "Ana").await?;
    bob.join(ranking, "Bob").await?;
    phone.join(ranking, "Carla").await?;
    let round = open_round(&ana.room, ranking, "Round 1", "Court 3").await?;
    // The laptop is a member as the phone is: they are the same person.
    laptop.room.sync().await?;

    // The laptop votes before catching up with the vote of the phone: the server sequences it
    // after the phone's, and the replica rejects it.
    phone.vote(ranking, round, "bob").await?;
    let second = vote(&laptop.room, ranking, round, "ana").await;
    assert_eq!(
        second,
        Err(ClientError::ConstraintViolation(Violation::Duplicate))
    );
    let expected = standings_of([("bob", 1)]);
    reach(&[&phone, &ana, &bob], ranking, &expected).await?;
    laptop.room.sync().await?;
    for device in [&phone, &laptop] {
        let voted = vote_of(&device.room, "carla", ranking, round).await?;
        assert_eq!(voted.as_deref(), Some("bob"));
        assert_eq!(standings(&device.room, ranking).await?, expected);
    }
    println!("  {phone_id} and {laptop_id}: {expected:?}; the second vote: {second:?}");
    Ok(())
}

/// The members that joined keep the creator from deleting the ranking: their rows of `members`
/// are of other owners (`Restrict`), and only they can delete them.
pub async fn restrict() -> Result<()> {
    let (server, room_id) = friends(Retention::Default).await?;
    let ana = Member::open(&server, &room_id, "ana").await?;
    let bob = Member::open(&server, &room_id, "bob").await?;
    let carla = Member::open(&server, &room_id, "carla").await?;
    let ranking = create_ranking(&ana.room, "Chess").await?;
    bob.join(ranking, "Bob").await?;
    carla.join(ranking, "Carla").await?;
    ana.room.sync().await?;
    let restrict = Violation::Restrict {
        constraint: "members_ranking".to_string(),
    };
    let deleted = delete_ranking(&ana.room, ranking).await;
    assert_eq!(deleted, Err(prechecked(restrict.clone())));

    // A ranking without members of others, she deletes.
    let alone = create_ranking(&ana.room, "Solitaire").await?;
    delete_ranking(&ana.room, alone).await?;

    // Once one leaves, the other still keeps it; once both left, she deletes it.
    bob.room.sync().await?;
    leave(&bob.room, ranking).await?;
    ana.room.sync().await?;
    let still = delete_ranking(&ana.room, ranking).await;
    assert_eq!(still, Err(prechecked(restrict)));
    carla.room.sync().await?;
    leave(&carla.room, ranking).await?;
    ana.room.sync().await?;
    delete_ranking(&ana.room, ranking).await?;
    println!("  with members: {deleted:?}; without them, deleted");
    Ok(())
}

/// Ana, Bob and Carla in a ranking with a round and their votes: a ranking, three members, two
/// rounds and six votes, twelve operations in all.
async fn played(server: &Server, room_id: &RoomId) -> Result<Played> {
    let ana = Member::open(server, room_id, "ana").await?;
    let bob = Member::open(server, room_id, "bob").await?;
    let carla = Member::open(server, room_id, "carla").await?;
    let ranking = create_ranking(&ana.room, "Mus").await?;
    ana.join(ranking, "Ana").await?;
    bob.join(ranking, "Bob").await?;
    carla.join(ranking, "Carla").await?;
    let first = open_round(&ana.room, ranking, "Round 1", FIRST_NOTES).await?;
    ana.vote(ranking, first, "bob").await?;
    bob.vote(ranking, first, "carla").await?;
    carla.vote(ranking, first, "bob").await?;
    let second = open_round(&bob.room, ranking, "Round 2", SECOND_NOTES).await?;
    ana.vote(ranking, second, "carla").await?;
    bob.vote(ranking, second, "carla").await?;
    carla.vote(ranking, second, "ana").await?;
    Ok(Played {
        members: [ana, bob, carla],
        ranking,
        rounds: [first, second],
        standings: standings_of([("carla", 3), ("bob", 2), ("ana", 1)]),
    })
}

/// The notes of the rounds of [`played`].
const FIRST_NOTES: &str = "The one who loses pays for dinner";
const SECOND_NOTES: &str = "Bob owes Carla a beer";

struct Played {
    members: [Member; 3],
    ranking: Uuid,
    rounds: [Uuid; 2],
    standings: Standings,
}

/// Once every member of `played` acknowledged the head, the server prunes the log of `room_id`;
/// then Dani, new, opens the room behind the log, the server asks a member for a snapshot, and
/// Dani's worker downloads it and catches up from it.
async fn welcome(server: &Server, room_id: &RoomId, played: &Played) -> Result<Member> {
    for member in &played.members {
        member.room.sync().await?;
    }
    let pruned = Waiting::until("the server prunes the log");
    while server.log(room_id).await?.0 <= 1 {
        pruned.tick().await?;
    }
    let (tail, head) = server.log(room_id).await?;
    assert_eq!(server.snapshot(room_id), None);
    println!("  the log keeps {tail} to {head}");
    let dani = Member::open(server, room_id, "dani").await?;
    let caught_up = Waiting::until("dani catches up");
    while standings(&dani.room, played.ranking).await? != played.standings {
        caught_up.tick().await?;
    }
    // The snapshot of a member at the head, since nobody wrote meanwhile.
    let snapshot = server.snapshot(room_id).ok_or("No snapshot")?;
    assert_eq!(snapshot, head);
    println!("  dani catches up from the snapshot at {snapshot}");
    Ok(dani)
}

/// A new member enters a ranking whose history is pruned, and catches up from a snapshot that
/// another member uploads.
pub async fn snapshot() -> Result<()> {
    let (server, room_id) = friends(Retention::Short).await?;
    let played = played(&server, &room_id).await?;
    let dani = welcome(&server, &room_id, &played).await?;
    let [first, second] = played.rounds;
    let notes = round_of(&dani.room, played.ranking, first).await?;
    assert_eq!(
        notes.and_then(|(_, notes)| notes).as_deref(),
        Some(FIRST_NOTES)
    );

    // Dani joins and votes, as any member.
    dani.join(played.ranking, "Dani").await?;
    dani.vote(played.ranking, second, "ana").await?;
    let expected = standings_of([("carla", 3), ("ana", 2), ("bob", 2)]);
    let [ana, bob, carla] = &played.members;
    reach(&[ana, bob, carla, &dani], played.ranking, &expected).await?;
    println!("  every replica: {expected:?}");
    Ok(())
}

/// Without network, the standings still read, and a vote is `Offline`, with nothing sent; once
/// the network is back, the vote goes, and the replica catches up with what the others did.
pub async fn offline() -> Result<()> {
    let (server, room_id) = friends(Retention::Default).await?;
    let ana = Member::open(&server, &room_id, "ana").await?;
    let bob = Member::open(&server, &room_id, "bob").await?;
    let carla = Member::open(&server, &room_id, "carla").await?;
    let ranking = create_ranking(&ana.room, "Pool").await?;
    ana.join(ranking, "Ana").await?;
    bob.join(ranking, "Bob").await?;
    carla.join(ranking, "Carla").await?;
    let round = open_round(&ana.room, ranking, "Round 1", "Table 2").await?;
    ana.vote(ranking, round, "carla").await?;
    carla.room.sync().await?;
    let before = standings(&carla.room, ranking).await?;
    assert_eq!(before, standings_of([("carla", 1)]));

    carla.device.network().cut();
    let down = Waiting::until("carla's worker finds no network");
    while carla.connected().await? != Some(false) {
        down.tick().await?;
    }
    // Bob votes meanwhile, which carla does not see.
    bob.vote(ranking, round, "ana").await?;
    assert_eq!(standings(&carla.room, ranking).await?, before);
    let (_, head) = server.log(&room_id).await?;
    let voted = vote(&carla.room, ranking, round, "bob").await;
    assert_eq!(voted, Err(ClientError::Offline(room_id.clone())));
    assert_eq!(server.log(&room_id).await?.1, head);
    println!("  without network: {before:?}, and the vote: {voted:?}");

    carla.device.network().restore();
    let up = Waiting::until("carla's worker reaches the server again");
    while carla.connected().await? != Some(true) {
        up.tick().await?;
    }
    vote(&carla.room, ranking, round, "bob").await?;
    let expected = standings_of([("ana", 1), ("bob", 1), ("carla", 1)]);
    reach(&[&ana, &bob, &carla], ranking, &expected).await?;
    println!("  with the network back: {expected:?}");
    Ok(())
}

/// A watch of the standings emits them as the votes arrive, and the last value is what a read
/// gives.
pub async fn watch() -> Result<()> {
    let (server, room_id) = friends(Retention::Default).await?;
    // Ana's heartbeats are almost a minute apart, so the votes reach her replica by the events of
    // the stream of her worker, which runs live while she watches.
    let rare = |config: &mut ClientConfig| config.heartbeat_interval = Duration::from_secs(59);
    let ana = Member::with(&server, &room_id, "ana", rare).await?;
    let bob = Member::open(&server, &room_id, "bob").await?;
    let carla = Member::open(&server, &room_id, "carla").await?;
    let dani = Member::open(&server, &room_id, "dani").await?;
    let ranking = create_ranking(&ana.room, "Darts").await?;
    ana.join(ranking, "Ana").await?;
    bob.join(ranking, "Bob").await?;
    carla.join(ranking, "Carla").await?;
    dani.join(ranking, "Dani").await?;
    let round = open_round(&ana.room, ranking, "Round 1", "Bring your own darts").await?;

    // While ana watches, her worker runs live, with the event stream of the room open.
    let mut watched = Box::pin(watch_standings(&ana.room, ranking)?);
    assert_eq!(emits(&mut watched, &Standings::new()).await?.len(), 1);
    let status = ana.room.status().await?;
    let live = status.worker.as_ref().map(|worker| worker.freshness);
    assert_eq!((status.watches, live), (1, Some(Freshness::Live)));
    let open = Waiting::until("ana's worker opens the event stream");
    while !ana
        .room
        .status()
        .await?
        .worker
        .is_some_and(|worker| worker.stream_open)
    {
        open.tick().await?;
    }

    let votes = [
        (&bob, "carla", standings_of([("carla", 1)])),
        (&carla, "bob", standings_of([("bob", 1), ("carla", 1)])),
        (&dani, "carla", standings_of([("carla", 2), ("bob", 1)])),
    ];
    for (member, candidate, expected) in votes {
        member.vote(ranking, round, candidate).await?;
        let emitted = emits(&mut watched, &expected).await?;
        println!("  ana sees {emitted:?}");
    }
    let last = standings_of([("carla", 2), ("bob", 1)]);
    assert_eq!(standings(&ana.room, ranking).await?, last);

    // Without the watch, the worker goes back to its mode.
    drop(watched);
    let status = ana.room.status().await?;
    let periodic = status.worker.map(|worker| worker.freshness);
    assert_eq!((status.watches, periodic), (0, Some(Freshness::Periodic)));
    Ok(())
}

/// The notes never travel in the clear: not in the operations that the server serves, which an
/// archivist without the key syncs, nor in the snapshot that a member uploads, decoded. The
/// members read them, and the snapshot holds what is not encrypted (the titles).
pub async fn notes() -> Result<()> {
    let (server, room_id) = friends(Retention::Short).await?;
    // The archivist registers first, so the server keeps the log for it.
    let archivist = Archivist::register(&server, &room_id).await?;
    let played = played(&server, &room_id).await?;
    let [_, bob, _] = &played.members;
    bob.room.sync().await?;
    let notes = round_of(&bob.room, played.ranking, played.rounds[0]).await?;
    assert_eq!(
        notes.and_then(|(_, notes)| notes).as_deref(),
        Some(FIRST_NOTES)
    );

    let (operations, frames) = archivist.operations().await?;
    assert_eq!(operations, server.log(&room_id).await?.1);
    for frame in &frames {
        assert!(!holds(frame, FIRST_NOTES.as_bytes()));
        assert!(!holds(frame, SECOND_NOTES.as_bytes()));
    }
    assert!(frames.iter().any(|frame| holds(frame, b"Round 1")));
    println!("  {operations} operations served, without the notes");

    archivist.leave().await?;
    let dani = welcome(&server, &room_id, &played).await?;
    let [first, second] = played.rounds;
    let notes = round_of(&dani.room, played.ranking, first).await?;
    assert_eq!(
        notes.and_then(|(_, notes)| notes).as_deref(),
        Some(FIRST_NOTES)
    );
    let notes = round_of(&dani.room, played.ranking, second).await?;
    assert_eq!(
        notes.and_then(|(_, notes)| notes).as_deref(),
        Some(SECOND_NOTES)
    );
    let snapshot = archivist.snapshot().await?;
    assert!(!holds(&snapshot, FIRST_NOTES.as_bytes()));
    assert!(!holds(&snapshot, SECOND_NOTES.as_bytes()));
    assert!(holds(&snapshot, b"Round 1") && holds(&snapshot, b"Round 2"));
    println!(
        "  a snapshot of {} bytes, without the notes",
        snapshot.len()
    );
    Ok(())
}

/// A client of the server without the key, that reads what the server serves through the
/// protocol, as a backup or an auditor of the server would: it registers, syncs every operation
/// of the log, and downloads the snapshot that the relay serves.
struct Archivist {
    transport: HttpTransport,
    client_id: ClientId,
    room_id: RoomId,
    token: String,
    incarnation: u128,
}

impl Archivist {
    async fn register(server: &Server, room_id: &RoomId) -> Result<Self> {
        let transport = HttpTransport::new(HttpTransportConfig::new(server.url()))?;
        let client_id = ClientId::new("archivist#backup")?;
        let token = server.backend().token(&client_id, room_id).await?;
        let mut archivist = Self {
            transport,
            client_id,
            room_id: room_id.clone(),
            token,
            incarnation: 0,
        };
        let registration = ClientMessage::RegisterClient {
            correlation_id: CorrelationId::new(1),
            room_id: room_id.clone(),
            client_id: archivist.client_id.clone(),
            auth_token: archivist.token.clone(),
            current_seq: None,
            known_incarnation: None,
        };
        match archivist.send(&registration).await?.0 {
            ServerMessage::Registered {
                room_incarnation, ..
            } => archivist.incarnation = room_incarnation,
            other => return Err(format!("Not registered: {other:?}").into()),
        }
        Ok(archivist)
    }

    /// The reply to `message`, and its frame.
    async fn send(&self, message: &ClientMessage) -> Result<(ServerMessage, Vec<u8>)> {
        let frame = self.transport.send(message, &self.token).await?;
        Ok((decode_message(&frame)?, frame))
    }

    /// How many operations the log holds from the first, and the frames that brought them.
    async fn operations(&self) -> Result<(u64, Vec<Vec<u8>>)> {
        let (mut cursor, mut frames) = (0, Vec::new());
        loop {
            let sync = ClientMessage::Sync {
                correlation_id: CorrelationId::new(2),
                room_id: self.room_id.clone(),
                client_id: self.client_id.clone(),
                from_seq: SequenceNumber::new(cursor),
                max_batch_size: 100,
            };
            let (ServerMessage::SyncBatch { ops, has_more, .. }, frame) = self.send(&sync).await?
            else {
                return Err("Not a batch".into());
            };
            for op in &ops {
                if op.seq.get() != cursor + 1 {
                    return Err(format!("A gap after {cursor}: {}", op.seq).into());
                }
                cursor = op.seq.get();
            }
            frames.push(frame);
            if !has_more {
                return Ok((cursor, frames));
            }
        }
    }

    /// Deregisters, so that the log is no longer kept for it.
    async fn leave(&self) -> Result<()> {
        let leave = ClientMessage::DeregisterClient {
            correlation_id: CorrelationId::new(3),
            room_id: self.room_id.clone(),
            client_id: self.client_id.clone(),
        };
        match self.send(&leave).await?.0 {
            ServerMessage::DeregisterAck { .. } => Ok(()),
            other => Err(format!("Not deregistered: {other:?}").into()),
        }
    }

    /// The snapshot that the relay serves, decoded: its envelope, without the compression.
    async fn snapshot(&self) -> Result<Vec<u8>> {
        let (mut snapshot, mut hash, mut index) = (Vec::new(), None, 0);
        loop {
            let request = ClientMessage::RequestSnapshotChunk {
                correlation_id: CorrelationId::new(4),
                room_id: self.room_id.clone(),
                chunk_index: index,
                chunk_size: 1 << 20,
                snapshot_hash: hash,
                room_incarnation: self.incarnation,
            };
            let (reply, _) = self.send(&request).await?;
            let ServerMessage::SnapshotChunk {
                total_chunks,
                snapshot_hash,
                data,
                ..
            } = reply
            else {
                return Err(format!("Not a chunk: {reply:?}").into());
            };
            snapshot.extend_from_slice(&data);
            (hash, index) = (Some(snapshot_hash), index + 1);
            if index == total_chunks {
                // Decompressing is work for a thread of its own, off the runtime.
                let decoded =
                    tokio::task::spawn_blocking(move || decode_snapshot_envelope(&snapshot));
                return Ok(decoded.await??);
            }
        }
    }
}
