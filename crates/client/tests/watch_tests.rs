//! `watch` and `watch_scan` of the facade against the server in process: a stream gives the
//! current value of its read and then the value each time it changes in the replica, after each
//! change of the applier, never for an operation the replica rejects, and ends once the room is
//! out of the client.

mod common;

/// The generator of the storage's randomized tests.
#[path = "../../storage/tests/common/rng.rs"]
#[allow(dead_code)]
mod rng;

use common::*;
use futures::StreamExt;
use rng::Rng;
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use zemdb_client::{
    ChangeStream, ClientConfig, ClientError, FileMetadata, Key, RoomHandle, Row, Scan, TableHandle,
    TransportError, ZemdbClient,
};
use zemdb_core::{
    ClientMessage, ColumnDef, CryptoEngine, DataType, Operation, OwnerIdentity, PrimaryKey, RoomId,
    RowBuilder, Schema, SchemaId, SequencedOperation, TableSchema, Value,
};
use zemdb_storage::{StorageEngine, StorageError};

// ---------------------------------------------------------------------------------------------
// The reference app

/// The ranking by votes of the reference app: in each round of a ranking, each member votes for
/// another, and each vote is a point.
fn ranking_schema() -> Schema {
    let rankings = TableSchema::builder("rankings")
        .table_id(0)
        .primary_key("ranking_id", DataType::Uuid)
        .column("creator", DataType::String)
        .column("name", DataType::String)
        .owner("creator", OwnerIdentity::ClientId);
    let members = TableSchema::builder("members")
        .table_id(1)
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("member_id", DataType::String)
        .column("display_name", DataType::String)
        .owner("member_id", OwnerIdentity::ClientId)
        .foreign_key("members_ranking", ["ranking_id"], "rankings");
    let rounds = TableSchema::builder("rounds")
        .table_id(2)
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("round_id", DataType::Uuid)
        .column("opened_by", DataType::String)
        .column("title", DataType::String)
        .nullable_column("notes", DataType::String)
        .encrypted_column("notes", DataType::String)
        .owner("opened_by", OwnerIdentity::ClientId)
        .foreign_key("rounds_ranking", ["ranking_id"], "rankings")
        .foreign_key("rounds_member", ["ranking_id", "opened_by"], "members")
        .unique("rounds_title", ["ranking_id", "title"]);
    let votes = TableSchema::builder("votes")
        .table_id(3)
        .primary_key("voter", DataType::String)
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("round_id", DataType::Uuid)
        .column("candidate", DataType::String)
        .owner("voter", OwnerIdentity::ClientId)
        .immutable()
        .foreign_key("votes_round", ["ranking_id", "round_id"], "rounds")
        .foreign_key("votes_voter", ["ranking_id", "voter"], "members")
        .foreign_key("votes_candidate", ["ranking_id", "candidate"], "members");
    Schema::builder()
        .table(rankings)
        .table(members)
        .table(rounds)
        .table(votes)
        .build()
}

fn ranking_id() -> SchemaId {
    SchemaId::new("ranking").unwrap()
}

fn uuid(n: u128) -> Value {
    Value::Uuid(n.to_be_bytes())
}

/// The id of the ranking of the tests.
fn ranking() -> Value {
    uuid(1)
}

/// The id of the round `round` of the ranking.
fn round_id(round: u128) -> Value {
    uuid(10 + round)
}

/// An insert into `table` of `row`, as bob commits it through the server.
fn insert(table: &str, row: RowBuilder) -> Operation {
    let insert = ranking_schema().to_operation_insert(table, &row.build(), 1000);
    insert.unwrap()
}

/// An update of the row of `key` of `table` with `fields`, as bob commits it.
fn update(table: &str, key: Vec<Value>, fields: &[(&str, Value)]) -> Operation {
    let fields: BTreeMap<_, _> = (fields.iter())
        .map(|(column, value)| (column.to_string(), value.clone()))
        .collect();
    let pk = PrimaryKey::composite(key);
    let update = ranking_schema().to_operation_update(table, pk, &fields, 1000);
    update.unwrap()
}

/// The member `person` named `name` joins the ranking.
fn joins(person: &str, name: &str) -> Operation {
    let row = RowBuilder::new()
        .set("ranking_id", ranking())
        .set("member_id", person)
        .set("display_name", name);
    insert("members", row)
}

/// The member `person` takes the name `name`.
fn renames(person: &str, name: &str) -> Operation {
    let key = vec![ranking(), person.into()];
    update("members", key, &[("display_name", name.into())])
}

/// Bob opens the round `round` with `title` and `notes`, as the bytes they are encrypted to.
fn bob_opens(round: u128, title: &str, notes: Value) -> Operation {
    let row = RowBuilder::new()
        .set("ranking_id", ranking())
        .set("round_id", round_id(round))
        .set("opened_by", "bob")
        .set("title", title)
        .set("notes", notes);
    insert("rounds", row)
}

/// Bob changes `fields` of his round `round`, as the owner he declares.
fn bob_changes(round: u128, fields: &[(&str, Value)]) -> Operation {
    let mut fields = fields.to_vec();
    fields.push(("opened_by", "bob".into()));
    update("rounds", vec![ranking(), round_id(round)], &fields)
}

/// Bob votes for `candidate` in the round `round`.
fn bob_votes(round: u128, candidate: &str) -> Operation {
    let row = RowBuilder::new()
        .set("voter", "bob")
        .set("ranking_id", ranking())
        .set("round_id", round_id(round))
        .set("candidate", candidate);
    insert("votes", row)
}

fn member_key(person: &str) -> Key {
    Key::new()
        .with("ranking_id", ranking())
        .with("member_id", person)
}

/// The row of `person` in `members`, named `name`, as a read gives it.
fn member(person: &str, name: &str) -> Row {
    Row::new()
        .with("ranking_id", ranking())
        .with("member_id", person)
        .with("display_name", name)
}

fn round_key(round: u128) -> Key {
    Key::new()
        .with("ranking_id", ranking())
        .with("round_id", round_id(round))
}

/// The row of the round `round`, as a read gives it.
fn round(round: u128, opened_by: &str, title: &str, notes: Option<&str>) -> Row {
    Row::new()
        .with("ranking_id", ranking())
        .with("round_id", round_id(round))
        .with("opened_by", opened_by)
        .with("title", title)
        .with("notes", notes.map_or(Value::Null, Value::from))
}

/// The round `round` as ana writes it, with `title` and `notes`.
fn ana_round(round: u128, title: &str, notes: &str) -> Row {
    Row::new()
        .with("ranking_id", ranking())
        .with("round_id", round_id(round))
        .with("title", title)
        .with("notes", notes)
}

/// The vote of ana for `candidate` in the round `round`, as she writes it.
fn ana_vote(round: u128, candidate: &str) -> Row {
    Row::new()
        .with("ranking_id", ranking())
        .with("round_id", round_id(round))
        .with("candidate", candidate)
}

// ---------------------------------------------------------------------------------------------
// Setup

/// A server with `room-a` of [`ranking_schema`], whose ranking bob created and joined as `Bob`.
async fn league(server: TestServer) -> (TestServer, RoomId) {
    let room_id = server
        .create_room_with("room-a", "ranking", ranking_schema())
        .await;
    server.commit_ops(&room_id, founded()).await;
    (server, room_id)
}

/// What bob commits to found the ranking: he creates it, and joins it as `Bob`.
fn founded() -> [Operation; 2] {
    let ranking = RowBuilder::new()
        .set("ranking_id", ranking())
        .set("creator", "bob")
        .set("name", "Amigos");
    [insert("rankings", ranking), joins("bob", "Bob")]
}

/// The facade of ana over `storage`, with her metadata in `dir`, a `CryptoEngine` of the tests
/// and the precheck.
async fn ana(
    dir: &Path,
    storage: impl StorageEngine + 'static,
    transport: &TestTransport,
) -> ZemdbClient {
    let metadata = FileMetadata::new(dir);
    let config = ClientConfig::new("ana");
    let crypto: Arc<dyn CryptoEngine> = TestCrypto::new();
    let tokens = TestTokens::new();
    let client = ZemdbClient::open(
        config,
        storage,
        metadata,
        transport.clone(),
        tokens,
        Some(crypto),
    );
    client.await.unwrap()
}

/// Opens `room_id` through `client`, with a token of the client.
async fn open(client: &ZemdbClient, room_id: &RoomId) -> RoomHandle {
    let token = token(&client.client_id(), room_id);
    client.room(room_id.clone(), token).await.unwrap()
}

/// Ana's room, synced with the server: bob's ranking, which ana joined as `Ana`.
async fn joined(client: &ZemdbClient, room_id: &RoomId) -> RoomHandle {
    let room = open(client, room_id).await;
    room.sync().await.unwrap();
    let members = room.table("members").unwrap();
    let ana = Row::new()
        .with("ranking_id", ranking())
        .with("display_name", "Ana");
    members.insert(&ana).await.unwrap();
    room
}

/// The next element of `stream`, which comes within 10 s.
async fn next<T>(stream: &mut ChangeStream<T>) -> Option<Result<T, ClientError>> {
    let next = tokio::time::timeout(Duration::from_secs(10), stream.next()).await;
    next.expect("The stream gave nothing")
}

/// Checks that `stream` gives nothing for a while.
async fn quiet<T: fmt::Debug>(stream: &mut ChangeStream<T>) {
    let next = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
    assert!(next.is_err(), "The stream gave {next:?}");
}

/// How many subscriptions `room` counts.
async fn watches(room: &RoomHandle) -> usize {
    room.status().await.unwrap().watches
}

fn io() -> StorageError {
    StorageError::Io(std::io::Error::other("The disk failed"))
}

// ---------------------------------------------------------------------------------------------
// Tests

#[tokio::test]
async fn a_watch_gives_the_current_value_and_then_each_change_of_its_own_writes_and_of_a_sync() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = ana(dir.path(), engine(dir.path()), &transport).await;
    let room = open(&client, &room_id).await;
    let members = room.table("members").unwrap();
    // The replica is at 0: bob is not there yet.
    let mut bob = members.watch(&member_key("bob"));
    assert_eq!(next(&mut bob).await, Some(Ok(None)));
    // The insert of another client, applied by a sync, and then his update.
    room.sync().await.unwrap();
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    server
        .commit_ops(&room_id, [renames("bob", "Roberto")])
        .await;
    room.sync().await.unwrap();
    let roberto = member("bob", "Roberto");
    assert_eq!(next(&mut bob).await, Some(Ok(Some(roberto.clone()))));

    // Its own insert, by the catch-up of its commit, on the row and on a scan of the table.
    let mut ana = members.watch(&member_key("ana"));
    let mut everyone = members.watch_scan(&Scan::new());
    assert_eq!(next(&mut ana).await, Some(Ok(None)));
    assert_eq!(next(&mut everyone).await, Some(Ok(vec![roberto.clone()])));
    let joined = Row::new()
        .with("ranking_id", ranking())
        .with("display_name", "Ana");
    members.insert(&joined).await.unwrap();
    assert_eq!(next(&mut ana).await, Some(Ok(Some(member("ana", "Ana")))));
    let both = vec![member("ana", "Ana"), roberto];
    assert_eq!(next(&mut everyone).await, Some(Ok(both)));
    // An encrypted column reads in the clear.
    let rounds = room.table("rounds").unwrap();
    let mut first = rounds.watch(&round_key(1));
    assert_eq!(next(&mut first).await, Some(Ok(None)));
    let opened = ana_round(1, "Ronda 1", "secret");
    rounds.insert(&opened).await.unwrap();
    let opened = round(1, "ana", "Ronda 1", Some("secret"));
    assert_eq!(next(&mut first).await, Some(Ok(Some(opened))));
    // Nothing else changed.
    quiet(&mut bob).await;
    quiet(&mut ana).await;
    quiet(&mut everyone).await;
    assert_eq!(watches(&room).await, 4);

    // A stream that waits in a task of its own, which nothing else wakes, is woken by the change.
    let waiting = tokio::spawn(async move {
        let next = bob.next().await;
        (bob, next)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    server.commit_ops(&room_id, [renames("bob", "Bob")]).await;
    room.sync().await.unwrap();
    let woken = tokio::time::timeout(Duration::from_secs(10), waiting).await;
    let (_bob, woken) = woken.expect("The waiting stream was not woken").unwrap();
    assert_eq!(woken, Some(Ok(Some(member("bob", "Bob")))));
}

#[tokio::test]
async fn a_rejected_operation_another_key_or_a_row_out_of_the_range_emits_nothing() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = ana(dir.path(), storage.clone(), &transport).await;
    let room = joined(&client, &room_id).await;
    let rounds = room.table("rounds").unwrap();
    rounds
        .insert(&ana_round(1, "Final", "first"))
        .await
        .unwrap();
    let mut second = rounds.watch(&round_key(2));
    assert_eq!(next(&mut second).await, Some(Ok(None)));
    let range = Scan::new().range(round_key(1)..=round_key(1));
    let mut first = rounds.watch_scan(&range);
    let ana_first = round(1, "ana", "Final", Some("first"));
    assert_eq!(next(&mut first).await, Some(Ok(vec![ana_first])));
    // The read of the row of the watch, if it reads again, is held.
    let not_read_again = |storage: &TestEngine<_>| {
        let held = storage.hold_next("get_by_id");
        async move {
            let mut held = held;
            let read = tokio::time::timeout(Duration::from_millis(100), held.replied()).await;
            assert!(read.is_err(), "The watch read its row again");
        }
    };

    // Bob opens the round 2 with the title of the round 1, which the replica rejects: the
    // watch of its key does not even read it again.
    let duplicate = bob_opens(2, "Final", Value::Null);
    server.commit_ops(&room_id, [duplicate]).await;
    room.sync().await.unwrap();
    let held = not_read_again(&storage);
    quiet(&mut second).await;
    held.await;
    assert_eq!(rounds.get(&round_key(2)).await, Ok(None));
    // Nor a change of another key.
    server
        .commit_ops(&room_id, [renames("bob", "Roberto")])
        .await;
    room.sync().await.unwrap();
    let held = not_read_again(&storage);
    quiet(&mut second).await;
    held.await;
    // A scan reads its table again, but a row out of its range changes nothing it emits; the
    // watch of another key of the table does not read it again.
    let third = bob_opens(3, "Semis", Value::Null);
    server.commit_ops(&room_id, [third]).await;
    room.sync().await.unwrap();
    quiet(&mut first).await;
    let held = not_read_again(&storage);
    quiet(&mut second).await;
    held.await;

    // A change of its key does.
    server
        .commit_ops(&room_id, [bob_opens(2, "Ronda 2", Value::Null)])
        .await;
    room.sync().await.unwrap();
    let bobs = round(2, "bob", "Ronda 2", None);
    assert_eq!(next(&mut second).await, Some(Ok(Some(bobs))));
    quiet(&mut first).await;
}

#[tokio::test]
async fn a_scan_of_the_votes_keeps_the_standings_while_votes_arrive() {
    let (server, room_id) = league(TestServer::new()).await;
    let rounds = (1..=4).map(|round| bob_opens(round, &format!("Ronda {round}"), Value::Null));
    server.commit_ops(&room_id, rounds).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = ana(dir.path(), engine(dir.path()), &transport).await;
    let room = joined(&client, &room_id).await;
    let votes = room.table("votes").unwrap();
    let ballots = Scan::new().columns(["candidate"]);
    // The points of each candidate.
    let standings = |rows: Vec<Row>| {
        let mut points = BTreeMap::new();
        for row in rows {
            let Some(Value::String(candidate)) = row.get("candidate") else {
                panic!("A vote without its candidate: {row:?}");
            };
            *points.entry(candidate.to_string()).or_insert(0) += 1;
        }
        points
    };
    let mut table = votes.watch_scan(&ballots);
    let mut seen = Vec::new();
    seen.push(standings(next(&mut table).await.unwrap().unwrap()));
    for round in 1..=4 {
        let candidate = ["ana", "bob"][usize::from(round % 2 == 0)];
        server
            .commit_ops(&room_id, [bob_votes(round, candidate)])
            .await;
        room.sync().await.unwrap();
        let now = standings(next(&mut table).await.unwrap().unwrap());
        assert_eq!(now, standings(votes.scan(&ballots).await.unwrap()));
        seen.push(now);
        votes.insert(&ana_vote(round, "bob")).await.unwrap();
        let now = standings(next(&mut table).await.unwrap().unwrap());
        assert_eq!(now, standings(votes.scan(&ballots).await.unwrap()));
        seen.push(now);
    }
    let points = |ana: usize, bob: usize| {
        let points = [("ana", ana), ("bob", bob)]
            .into_iter()
            .filter(|(_, p)| *p > 0);
        points
            .map(|(name, p)| (name.to_string(), p))
            .collect::<BTreeMap<_, _>>()
    };
    let expected = [
        points(0, 0),
        points(1, 0),
        points(1, 1),
        points(1, 2),
        points(1, 3),
        points(2, 3),
        points(2, 4),
        points(2, 5),
        points(2, 6),
    ];
    assert_eq!(seen, expected);
    quiet(&mut table).await;
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_installed_reads_the_watch_again() {
    let (server, room_id) = league(TestServer::pruning()).await;
    let renames = (1..=7).map(|n| renames("bob", &format!("Bob {n}")));
    let mut ops: Vec<SequencedOperation> = Vec::new();
    for (op, seq) in founded().into_iter().chain(renames).zip(1_u64..) {
        ops.push(SequencedOperation::new(seq, op));
    }
    server
        .commit_ops(&room_id, ops[2..].iter().map(|op| op.op.clone()))
        .await;
    server.prune(&room_id, 6).await;
    let snapshot = snapshot(&room_id, ranking_schema(), &ops).await;
    server.upload(&room_id, 9, snapshot).await;

    // A replica behind the log, with nothing, until the snapshot at the head of the room is
    // installed: it is the only change of the replica that the watch sees.
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = ana(dir.path(), engine(dir.path()), &transport).await;
    let room = open(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    assert_eq!(next(&mut bob).await, Some(Ok(None)));
    // The stream waits while the sync installs the snapshot: the notice comes once it is
    // installed.
    let (synced, got) = tokio::join!(room.sync(), next(&mut bob));
    synced.unwrap();
    assert_eq!(room.status().await.unwrap().head_seq.get(), 9);
    let installed = Some(member("bob", "Bob 7"));
    assert_eq!(got, Some(Ok(installed.clone())));
    assert_eq!(members.get(&member_key("bob")).await, Ok(installed));
    quiet(&mut bob).await;
}

#[tokio::test]
async fn a_batch_applied_while_the_first_read_of_a_watch_is_in_flight_reads_it_again() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = ana(dir.path(), storage.clone(), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    server
        .commit_ops(&room_id, [renames("bob", "Roberto")])
        .await;
    // The first read is held once the engine read the row, and the batch is applied meanwhile:
    // the subscription already looks at the row, which the batch marks.
    let mut bob = members.watch(&member_key("bob"));
    let mut held = storage.hold_next("get_by_id:read");
    let first = {
        let first = next(&mut bob);
        tokio::pin!(first);
        tokio::select! {
            next = &mut first => panic!("The read was not held: {next:?}"),
            () = held.replied() => {}
        }
        room.sync().await.unwrap();
        held.release();
        first.await
    };
    assert_eq!(first, Some(Ok(Some(member("bob", "Bob")))));
    let roberto = Some(member("bob", "Roberto"));
    assert_eq!(next(&mut bob).await, Some(Ok(roberto)));
}

#[tokio::test]
async fn an_error_that_takes_the_room_out_of_the_client_ends_the_stream() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = ana(dir.path(), storage.clone(), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    // The batch fails once applied, and the storage room does not open again.
    server
        .commit_ops(&room_id, [renames("bob", "Roberto")])
        .await;
    storage.fail_next("apply_batch:applied", io());
    storage.fail_next("open_room", io());
    let synced = room.sync().await;
    assert!(synced.is_err(), "{synced:?}");
    let closed = Err(ClientError::RoomClosed(room_id.clone()));
    assert_eq!(room.status().await, closed);
    assert_eq!(next(&mut bob).await, None);
}

#[tokio::test]
async fn a_watch_made_while_the_room_closes_ends_with_it() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = ana(dir.path(), storage.clone(), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    // Made with the close in progress, the replica already out of the client.
    let held = storage.hold_next("close_room");
    let during = async {
        let mut held = held;
        held.replied().await;
        let made = members.watch(&member_key("bob"));
        held.release();
        made
    };
    let (closed, mut made) = tokio::join!(room.close(), during);
    closed.unwrap();
    assert_eq!(next(&mut made).await, None);
}

#[tokio::test]
async fn a_change_that_panics_with_the_room_out_of_the_client_ends_its_streams() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = ana(dir.path(), storage.clone(), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    // The engine panics once it closed the room: the close is `Storage`, and the stream ends.
    storage.fail_next("close_room:panic", io());
    let closed = room.close().await;
    assert!(matches!(closed, Err(ClientError::Storage(_))), "{closed:?}");
    assert_eq!(next(&mut bob).await, None);
}

#[tokio::test]
async fn a_change_that_panics_with_the_room_in_the_client_reads_every_watch_again() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = ana(dir.path(), storage.clone(), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    // The engine panics once the batch is visible: the change ends without its notice.
    server
        .commit_ops(&room_id, [renames("bob", "Roberto")])
        .await;
    storage.fail_next("apply_batch:panic", io());
    let synced = room.sync().await;
    assert!(matches!(synced, Err(ClientError::Storage(_))), "{synced:?}");
    let roberto = Some(member("bob", "Roberto"));
    assert_eq!(members.get(&member_key("bob")).await, Ok(roberto.clone()));
    assert_eq!(next(&mut bob).await, Some(Ok(roberto)));
}

#[tokio::test]
async fn a_storage_room_opened_again_after_an_error_reads_the_watch_again_without_ending_it() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = ana(dir.path(), storage.clone(), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    // The engine applies the batch and then fails: the storage room is opened again from its
    // files, which hold the batch, and only the reopening notifies the watch.
    server
        .commit_ops(&room_id, [renames("bob", "Roberto")])
        .await;
    storage.fail_next("apply_batch:applied", io());
    // A watch made in the middle of the reopening, with the replica out of the client, does not
    // end: the reopening is a change in progress.
    let held = storage.hold_next("open_room");
    let during = async {
        let mut held = held;
        held.replied().await;
        let made = members.watch(&member_key("bob"));
        held.release();
        made
    };
    let (synced, mut made) = tokio::join!(room.sync(), during);
    assert!(matches!(synced, Err(ClientError::Storage(_))), "{synced:?}");
    let roberto = Some(member("bob", "Roberto"));
    assert_eq!(next(&mut bob).await, Some(Ok(roberto.clone())));
    assert_eq!(next(&mut made).await, Some(Ok(roberto.clone())));
    assert_eq!(members.get(&member_key("bob")).await, Ok(roberto));
    // The streams go on.
    server.commit_ops(&room_id, [renames("bob", "Bob")]).await;
    room.sync().await.unwrap();
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    assert_eq!(next(&mut made).await, Some(Ok(Some(member("bob", "Bob")))));
    assert_eq!(watches(&room).await, 2);
}

#[tokio::test]
async fn a_watch_made_while_a_batch_is_applied_reads_it_once_applied() {
    // With a subscription of the room when the batch starts, which takes its keys, and without.
    for watched in [true, false] {
        let (server, room_id) = league(TestServer::new()).await;
        let dir = tempdir().unwrap();
        let transport = server.transport();
        let storage = TestEngine::new(engine(dir.path()));
        let client = ana(dir.path(), storage.clone(), &transport).await;
        let room = joined(&client, &room_id).await;
        let members = room.table("members").unwrap();
        let _other = watched.then(|| members.watch_scan(&Scan::new()));
        server
            .commit_ops(&room_id, [renames("bob", "Roberto")])
            .await;
        // The batch is held before the engine applies it, and a watch reads the row meanwhile.
        let held = storage.hold_next("apply_batch");
        let during = async {
            let mut held = held;
            held.replied().await;
            let mut made = members.watch(&member_key("bob"));
            let first = next(&mut made).await;
            held.release();
            (made, first)
        };
        let (synced, (mut made, first)) = tokio::join!(room.sync(), during);
        synced.unwrap();
        assert_eq!(first, Some(Ok(Some(member("bob", "Bob")))));
        let roberto = Some(member("bob", "Roberto"));
        assert_eq!(next(&mut made).await, Some(Ok(roberto)), "{watched}");
    }
}

/// [`ranking_schema`] with `players` in place of `members`, with the same id and columns: the
/// room created again with it has no `members`.
fn players_schema() -> Schema {
    let rankings = TableSchema::builder("rankings")
        .table_id(0)
        .primary_key("ranking_id", DataType::Uuid)
        .column("creator", DataType::String)
        .column("name", DataType::String)
        .owner("creator", OwnerIdentity::ClientId);
    let players = TableSchema::builder("players")
        .table_id(1)
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("member_id", DataType::String)
        .column("display_name", DataType::String)
        .owner("member_id", OwnerIdentity::ClientId)
        .foreign_key("players_ranking", ["ranking_id"], "rankings");
    Schema::builder().table(rankings).table(players).build()
}

#[tokio::test]
async fn a_watch_whose_table_the_schema_no_longer_has_is_not_read_again_by_a_batch() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = ana(dir.path(), engine(dir.path()), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    // The room is created again without `members`: the watch is an `InvalidOperation`.
    let rooms = &server.state.room_manager;
    rooms.delete_room(&room_id).await.unwrap();
    server
        .create_room_with(room_id.as_str(), "players", players_schema())
        .await;
    room.sync().await.unwrap();
    let gone = next(&mut bob).await;
    assert!(
        matches!(gone, Some(Err(ClientError::InvalidOperation(_)))),
        "{gone:?}"
    );
    // A batch with the row of the same table id and key does not read it again.
    let schema = players_schema();
    let founded = RowBuilder::new()
        .set("ranking_id", ranking())
        .set("creator", "bob")
        .set("name", "Amigos")
        .build();
    let player = RowBuilder::new()
        .set("ranking_id", ranking())
        .set("member_id", "bob")
        .set("display_name", "Bob")
        .build();
    let ops = [
        schema
            .to_operation_insert("rankings", &founded, 1000)
            .unwrap(),
        schema
            .to_operation_insert("players", &player, 1000)
            .unwrap(),
    ];
    server.commit_ops(&room_id, ops).await;
    room.sync().await.unwrap();
    quiet(&mut bob).await;
    let players = room.table("players").unwrap();
    let bob_player = Some(member("bob", "Bob"));
    assert_eq!(players.get(&member_key("bob")).await, Ok(bob_player));
}

#[tokio::test]
async fn a_column_added_to_the_schema_appears_in_null_in_the_value_of_a_watch() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = ana(dir.path(), engine(dir.path()), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    let mut everyone = members.watch_scan(&Scan::new().columns(["member_id"]));
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    let ids = |ids: &[&str]| {
        let row = |id: &&str| Row::new().with("member_id", *id);
        ids.iter().map(row).collect::<Vec<_>>()
    };
    assert_eq!(next(&mut everyone).await, Some(Ok(ids(&["ana", "bob"]))));
    // The server adds `nickname`, and bob opens a round: the batch touches no member, and the
    // reload of the schema reads the watches again.
    let nickname = ColumnDef::new("nickname", DataType::String).nullable(true);
    let registry = &server.state.schema_registry;
    registry
        .add_column(&ranking_id(), "members", nickname)
        .unwrap();
    let rooms = &server.state.room_manager;
    rooms.reload_schema_for_rooms(&ranking_id()).await;
    server
        .commit_ops(&room_id, [bob_opens(1, "Ronda 1", Value::Null)])
        .await;
    room.sync().await.unwrap();
    let nicknamed = Some(member("bob", "Bob").with("nickname", Value::Null));
    assert_eq!(next(&mut bob).await, Some(Ok(nicknamed.clone())));
    assert_eq!(members.get(&member_key("bob")).await, Ok(nicknamed));
    // The scan names its columns: the same rows, which it does not emit again.
    quiet(&mut everyone).await;
}

#[tokio::test]
async fn a_watch_over_an_empty_replica_emits_once_it_opens_with_the_room_created_again() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = ana(dir.path(), engine(dir.path()), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    let epoch = room.status().await.unwrap().epoch;
    // The room is created again, where bob founds the ranking again as `Bob 2`; the replica is
    // discarded for it (`RoomReset`), and its registration without a cursor gets no reply.
    let rooms = &server.state.room_manager;
    rooms.delete_room(&room_id).await.unwrap();
    server
        .create_room_with(room_id.as_str(), "ranking", ranking_schema())
        .await;
    let [created, _] = founded();
    server
        .commit_ops(&room_id, [created, joins("bob", "Bob 2")])
        .await;
    let mut registrations = 0;
    transport.answer(move |message| {
        let ClientMessage::RegisterClient { .. } = message else {
            return None;
        };
        registrations += 1;
        (registrations > 1).then(|| Err(TransportError("Offline".to_string())))
    });
    let synced = room.sync().await;
    assert!(matches!(synced, Err(ClientError::Network(_))), "{synced:?}");
    assert_eq!(transport.registrations().last(), Some(&(None, None)));
    let closed = Err(ClientError::RoomClosed(room_id.clone()));
    assert_eq!(members.get(&member_key("bob")).await, closed);
    // While the replica is empty, nothing is emitted, and a watch made meanwhile waits.
    quiet(&mut bob).await;
    let mut late = members.watch(&member_key("bob"));
    quiet(&mut late).await;

    // Once registered, the replica opens with the room created again.
    transport.online();
    room.sync().await.unwrap();
    let again = Some(member("bob", "Bob 2"));
    assert_eq!(next(&mut bob).await, Some(Ok(again.clone())));
    assert_eq!(next(&mut late).await, Some(Ok(again.clone())));
    assert_eq!(members.get(&member_key("bob")).await, Ok(again));
    assert_eq!(watches(&room).await, 2);
    assert_eq!(room.status().await.unwrap().epoch, epoch + 1);
}

#[tokio::test]
async fn a_slow_consumer_gets_the_current_value_skipping_those_in_between() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = ana(dir.path(), engine(dir.path()), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    let mut everyone = members.watch_scan(&Scan::new());
    assert!(next(&mut bob).await.is_some());
    assert!(next(&mut everyone).await.is_some());
    // Thirty batches of one, which nobody reads.
    let renames = (1..=30).map(|n| renames("bob", &format!("Bob {n}")));
    server.commit_ops(&room_id, renames).await;
    transport.sync_batch_size(1);
    room.sync().await.unwrap();
    assert_eq!(watches(&room).await, 2);
    // The next element is the current value.
    let current = member("bob", "Bob 30");
    assert_eq!(next(&mut bob).await, Some(Ok(Some(current.clone()))));
    let both = vec![member("ana", "Ana"), current];
    assert_eq!(next(&mut everyone).await, Some(Ok(both)));
    quiet(&mut bob).await;
    quiet(&mut everyone).await;
    assert_eq!(watches(&room).await, 2);
}

#[tokio::test]
async fn a_stream_ends_once_the_room_closes_or_the_client_shuts_down() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = ana(dir.path(), engine(dir.path()), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    let mut everyone = members.watch_scan(&Scan::new());
    let mut waiting = members.watch(&member_key("bob"));
    assert!(next(&mut bob).await.is_some());
    assert!(next(&mut everyone).await.is_some());
    assert!(next(&mut waiting).await.is_some());
    assert_eq!(watches(&room).await, 3);
    // Dropping a stream drops its subscription.
    drop(everyone);
    assert_eq!(watches(&room).await, 2);
    // A stream that waits for a change, in another task than the one it waited in first, and
    // one that does not wait, end with the room.
    quiet(&mut waiting).await;
    let waited = tokio::spawn(async move { waiting.next().await.is_none() });
    tokio::task::yield_now().await;
    room.close().await.unwrap();
    assert_eq!(next(&mut bob).await, None);
    assert_eq!(next(&mut bob).await, None);
    let waited = tokio::time::timeout(Duration::from_secs(10), waited).await;
    assert!(waited.expect("The waiting stream did not end").unwrap());
    // A watch of a room out of the client ends at once.
    let mut closed = members.watch(&member_key("bob"));
    assert_eq!(next(&mut closed).await, None);

    // Opened again, a new watch reads it, and ends with the client.
    let room = open(&client, &room_id).await;
    assert_eq!(watches(&room).await, 0);
    let members = room.table("members").unwrap();
    let mut bob = members.watch(&member_key("bob"));
    assert_eq!(next(&mut bob).await, Some(Ok(Some(member("bob", "Bob")))));
    client.shutdown().await.unwrap();
    assert_eq!(next(&mut bob).await, None);
    let mut after = members.watch_scan(&Scan::new());
    assert_eq!(next(&mut after).await, None);
}

#[tokio::test]
async fn a_stream_dropped_at_any_await_of_its_read_leaves_nothing() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = ana(dir.path(), storage.clone(), &transport).await;
    let room = joined(&client, &room_id).await;
    let members = room.table("members").unwrap();
    for hold in ["get_by_id", "get_by_id:read"] {
        let mut stream = members.watch(&member_key("bob"));
        let mut held = storage.hold_next(hold);
        tokio::select! {
            next = stream.next() => panic!("The read was not held: {next:?}"),
            () = held.replied() => {}
        }
        assert_eq!(watches(&room).await, 1);
        drop(stream);
        assert_eq!(watches(&room).await, 0);
        drop(held);
    }
    // Waiting for a change.
    let mut stream = members.watch(&member_key("bob"));
    assert!(next(&mut stream).await.is_some());
    quiet(&mut stream).await;
    drop(stream);
    assert_eq!(watches(&room).await, 0);
    // The room goes on: a batch with nobody to notify, and a new watch.
    server
        .commit_ops(&room_id, [renames("bob", "Roberto")])
        .await;
    room.sync().await.unwrap();
    let mut bob = members.watch(&member_key("bob"));
    let roberto = Some(member("bob", "Roberto"));
    assert_eq!(next(&mut bob).await, Some(Ok(roberto)));
}

#[tokio::test]
async fn a_read_that_fails_is_an_element_and_the_next_value_is_compared_with_the_last_emitted() {
    let (server, room_id) = league(TestServer::new()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = ana(dir.path(), engine(dir.path()), &transport).await;
    let room = joined(&client, &room_id).await;
    let rounds = room.table("rounds").unwrap();
    // Bob opens the round 2 with notes that do not decrypt.
    let garbage = || Value::Bytes(vec![0xc1, 0x9a, 0x07].into());
    server
        .commit_ops(&room_id, [bob_opens(2, "Ronda 2", garbage())])
        .await;
    room.sync().await.unwrap();
    let mut second = rounds.watch(&round_key(2));
    let undecrypted = |next| matches!(next, Some(Err(ClientError::Decryption(_))));
    let first = next(&mut second).await;
    assert!(undecrypted(first.clone()), "{first:?}");
    // He clears them: the stream went on.
    let null = [("notes", Value::Null)];
    let commit = |op| server.commit_ops(&room_id, [op]);
    commit(bob_changes(2, &null)).await;
    room.sync().await.unwrap();
    let cleared = Some(round(2, "bob", "Ronda 2", None));
    assert_eq!(next(&mut second).await, Some(Ok(cleared.clone())));
    // Garbage again, and cleared again: the same value as the last one emitted, not emitted.
    commit(bob_changes(2, &[("notes", garbage())])).await;
    room.sync().await.unwrap();
    let failed = next(&mut second).await;
    assert!(undecrypted(failed.clone()), "{failed:?}");
    commit(bob_changes(2, &null)).await;
    room.sync().await.unwrap();
    quiet(&mut second).await;
    assert_eq!(rounds.get(&round_key(2)).await, Ok(cleared));
    commit(bob_changes(2, &[("title", "Final".into())])).await;
    room.sync().await.unwrap();
    let renamed = Some(round(2, "bob", "Final", None));
    assert_eq!(next(&mut second).await, Some(Ok(renamed)));
}

/// What a watch of the property reads, with its stream and the last value it gave.
enum Watched {
    Row(Key, ChangeStream<Option<Row>>, Option<Option<Row>>),
    Rows(Scan, ChangeStream<Vec<Row>>, Option<Vec<Row>>),
}

impl Watched {
    /// A watch at random of the rounds, the members or the votes.
    fn new(rng: &mut Rng, tables: &[TableHandle; 3]) -> Self {
        let [members, rounds, votes] = tables;
        let rounds_range = |rng: &mut Rng| {
            let (a, b) = (1 + u128::from(rng.below(6)), 1 + u128::from(rng.below(6)));
            Scan::new().range(round_key(a.min(b))..=round_key(a.max(b)))
        };
        match rng.below(6) {
            0 => {
                let key = round_key(1 + u128::from(rng.below(6)));
                Self::Row(key.clone(), rounds.watch(&key), None)
            }
            1 => {
                let key = member_key(["ana", "bob"][rng.below(2) as usize]);
                Self::Row(key.clone(), members.watch(&key), None)
            }
            2 => {
                let scan = rounds_range(rng);
                Self::Rows(scan.clone(), rounds.watch_scan(&scan), None)
            }
            3 => {
                let scan = rounds_range(rng).backward().limit(2);
                Self::Rows(scan.clone(), rounds.watch_scan(&scan), None)
            }
            4 => {
                let scan = Scan::new().columns(["candidate", "round_id"]);
                Self::Rows(scan.clone(), votes.watch_scan(&scan), None)
            }
            _ => {
                let scan = Scan::new();
                Self::Rows(scan.clone(), members.watch_scan(&scan), None)
            }
        }
    }

    /// Reads the stream for at most `wait`, keeping what it gives.
    async fn read(&mut self, wait: Duration) -> bool {
        let read = async {
            match self {
                Self::Row(_, stream, last) => match stream.next().await {
                    Some(Ok(value)) => *last = Some(value),
                    Some(Err(err)) => panic!("{err:?}"),
                    None => panic!("The stream ended"),
                },
                Self::Rows(_, stream, last) => match stream.next().await {
                    Some(Ok(value)) => *last = Some(value),
                    Some(Err(err)) => panic!("{err:?}"),
                    None => panic!("The stream ended"),
                },
            }
        };
        tokio::time::timeout(wait, read).await.is_ok()
    }

    /// Reads the stream until it gives nothing more, and checks that its last value is that of
    /// a direct read.
    async fn settled(&mut self, tables: &[TableHandle; 3]) {
        while self.read(Duration::from_millis(200)).await {}
        let [members, rounds, votes] = tables;
        match self {
            Self::Row(key, _, last) => {
                let table = match key.get("member_id") {
                    Some(_) => members,
                    None => rounds,
                };
                assert_eq!(last.clone(), Some(table.get(key).await.unwrap()), "{key:?}");
            }
            Self::Rows(scan, _, last) => {
                let table = match &scan.columns {
                    Some(_) => votes,
                    None if scan.start == std::ops::Bound::Unbounded => members,
                    None => rounds,
                };
                assert_eq!(
                    last.clone(),
                    Some(table.scan(scan).await.unwrap()),
                    "{scan:?}"
                );
            }
        }
    }
}

/// An operation of bob at random over six rounds, which the replica may reject: a round opened
/// with a title another has, a vote in a round that is not there, or a delete of a round with
/// votes.
fn bob_at_random(rng: &mut Rng) -> Operation {
    let round = 1 + u128::from(rng.below(6));
    let title = format!("Ronda {}", rng.below(4));
    match rng.below(5) {
        0 => renames("bob", &format!("Bob {}", rng.below(1000))),
        1 => bob_opens(round, &title, Value::Null),
        2 => bob_changes(round, &[("title", title.into())]),
        3 => bob_votes(round, ["ana", "bob"][rng.below(2) as usize]),
        _ => {
            let pk = PrimaryKey::composite(vec![ranking(), round_id(round)]);
            Operation::delete_with_owner(2, pk, "bob", 1000)
        }
    }
}

/// A write of ana at random, through the facade: the replica, or its precheck, may reject it.
async fn ana_at_random(rng: &mut Rng, tables: &[TableHandle; 3]) {
    let [members, rounds, votes] = tables;
    let round = 1 + u128::from(rng.below(6));
    let title = format!("Ronda {}", rng.below(4));
    let _ = match rng.below(5) {
        0 => {
            let name = Row::new().with("display_name", format!("Ana {}", rng.below(1000)));
            members.update(&member_key("ana"), &name).await
        }
        1 => {
            let notes = format!("notes {}", rng.below(1000));
            rounds
                .insert(&ana_round(round, &title, &notes))
                .await
                .map(drop)
        }
        2 => {
            let changes = Row::new().with("notes", format!("notes {}", rng.below(1000)));
            rounds.update(&round_key(round), &changes).await
        }
        3 => {
            let candidate = ["ana", "bob"][rng.below(2) as usize];
            votes.insert(&ana_vote(round, candidate)).await.map(drop)
        }
        _ => rounds.delete(&round_key(round)).await,
    };
}

/// The property over `seeds`, each with `steps` steps: random batches of bob, writes of ana
/// through the facade, syncs, watches dropped and made, and reads at random moments; once the
/// replica is still, the last value of each watch is that of a direct read.
async fn still(seeds: std::ops::RangeInclusive<u64>, steps: usize) {
    for seed in seeds {
        let mut rng = Rng::new(seed);
        let (server, room_id) = league(TestServer::new()).await;
        let dir = tempdir().unwrap();
        let transport = server.transport();
        let client = ana(dir.path(), engine(dir.path()), &transport).await;
        let room = joined(&client, &room_id).await;
        let tables = ["members", "rounds", "votes"].map(|name| room.table(name).unwrap());
        let mut watched: Vec<_> = (0..6).map(|_| Watched::new(&mut rng, &tables)).collect();
        for _ in 0..steps {
            match rng.below(10) {
                0..=2 => {
                    let ops = (0..=rng.below(3)).map(|_| bob_at_random(&mut rng));
                    server.commit_ops(&room_id, ops.collect::<Vec<_>>()).await;
                }
                3 | 4 => ana_at_random(&mut rng, &tables).await,
                5 | 6 => room.sync().await.unwrap(),
                7 => {
                    // A watch dropped, and another made.
                    let index = rng.below(watched.len() as u64) as usize;
                    watched[index] = Watched::new(&mut rng, &tables);
                }
                _ => {
                    // A read at a random moment, which may find nothing yet.
                    let index = rng.below(watched.len() as u64) as usize;
                    let wait = Duration::from_millis(rng.below(3));
                    watched[index].read(wait).await;
                }
            }
        }
        room.sync().await.unwrap();
        for watch in &mut watched {
            watch.settled(&tables).await;
        }
        assert_eq!(watches(&room).await, watched.len(), "seed {seed}");
    }
}

#[tokio::test]
async fn once_the_replica_is_still_the_last_value_of_each_watch_is_that_of_a_direct_read() {
    still(1..=3, 60).await;
}

#[tokio::test]
#[ignore = "long: run with --ignored"]
async fn once_the_replica_is_still_the_last_value_of_each_watch_is_that_of_a_direct_read_long() {
    still(1..=40, 300).await;
}

/// Waits until `last`, which a task keeps, is `direct`, for at most 10 s.
async fn becomes<T: PartialEq + fmt::Debug>(last: &Mutex<Option<T>>, direct: T, seed: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let direct = Some(direct);
    while *last.lock().unwrap() != direct {
        let late = tokio::time::Instant::now() >= deadline;
        assert!(
            !late,
            "seed {seed}: {:?} is not {direct:?}",
            last.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The property with the streams read as an application does, each in a task of its own on the
/// threads of the runtime, while the steps go on: once the replica is still, the last value of
/// each one is that of a direct read, and none ended.
async fn in_other_threads(seeds: std::ops::RangeInclusive<u64>, steps: usize) {
    enum Kept {
        Row(Key, Arc<Mutex<Option<Option<Row>>>>),
        Rows(Scan, Arc<Mutex<Option<Vec<Row>>>>),
    }
    /// Reads `stream` in a task of its own, keeping its last value in `last`.
    fn read<T: Send + 'static>(
        mut stream: ChangeStream<T>,
        last: &Arc<Mutex<Option<T>>>,
    ) -> tokio::task::JoinHandle<()> {
        let last = Arc::clone(last);
        tokio::spawn(async move {
            while let Some(value) = stream.next().await {
                *last.lock().unwrap() = Some(value.unwrap());
            }
        })
    }
    for seed in seeds {
        let mut rng = Rng::new(seed);
        let (server, room_id) = league(TestServer::new()).await;
        let dir = tempdir().unwrap();
        let transport = server.transport();
        let client = ana(dir.path(), engine(dir.path()), &transport).await;
        let room = joined(&client, &room_id).await;
        let tables = ["members", "rounds", "votes"].map(|name| room.table(name).unwrap());
        let (mut kept, mut tasks) = (Vec::new(), Vec::new());
        for _ in 0..10 {
            match Watched::new(&mut rng, &tables) {
                Watched::Row(key, stream, _) => {
                    let last = Arc::default();
                    tasks.push(read(stream, &last));
                    kept.push(Kept::Row(key, last));
                }
                Watched::Rows(scan, stream, _) => {
                    let last = Arc::default();
                    tasks.push(read(stream, &last));
                    kept.push(Kept::Rows(scan, last));
                }
            }
        }
        for _ in 0..steps {
            match rng.below(6) {
                0 | 1 => {
                    let ops = (0..=rng.below(3)).map(|_| bob_at_random(&mut rng));
                    server.commit_ops(&room_id, ops.collect::<Vec<_>>()).await;
                }
                2 | 3 => ana_at_random(&mut rng, &tables).await,
                _ => room.sync().await.unwrap(),
            }
        }
        room.sync().await.unwrap();
        let [members, rounds, votes] = &tables;
        for kept in &kept {
            match kept {
                Kept::Row(key, last) => {
                    let table = match key.get("member_id") {
                        Some(_) => members,
                        None => rounds,
                    };
                    becomes(last, table.get(key).await.unwrap(), seed).await;
                }
                Kept::Rows(scan, last) => {
                    let table = match &scan.columns {
                        Some(_) => votes,
                        None if scan.start == std::ops::Bound::Unbounded => members,
                        None => rounds,
                    };
                    becomes(last, table.scan(scan).await.unwrap(), seed).await;
                }
            }
        }
        for task in tasks {
            assert!(!task.is_finished(), "seed {seed}: a stream ended");
            task.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_read_in_tasks_of_other_threads_end_with_the_value_of_a_direct_read() {
    in_other_threads(1..=3, 80).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long: run with --ignored"]
async fn streams_read_in_tasks_of_other_threads_end_with_the_value_of_a_direct_read_long() {
    in_other_threads(1..=20, 300).await;
}
