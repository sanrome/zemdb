//! The precheck of the writes of the facade against the server in process: a write that the
//! replica, in its state, would reject is not sent; and what the precheck does when the replica is
//! behind the log, changes before the write is sequenced, or its storage fails.

mod common;

use common::*;
use std::future::Future;
use std::path::Path;
use tempfile::tempdir;
use zemdb_client::{
    ClientConfig, ClientError, FileMetadata, Key, Rejection, RoomHandle, Row, Violation,
    ZemdbClient,
};
use zemdb_core::{
    encode_message, ClientMessage, ClientMessageKind, DataType, Operation, OwnerIdentity, RoomId,
    RowBuilder, Schema, SchemaId, SequenceNumber, ServerMessage, TableSchema, ValidationError,
};
use zemdb_storage::{DiskStorageEngine, StorageEngine, StorageError};

/// A league of rankings:
/// - `rankings`, keyed by an int `ranking_id`, with its owner `creator` outside the key and a
///   unique string `name`;
/// - `members`, keyed by an int `ranking_id` and the owner column `member_id`, with a string
///   `display_name`, and a foreign key to `rankings`;
/// - `votes`, immutable, keyed by the owner column `voter` and an int `ranking_id`, with a string
///   `candidate`;
/// - `tags`, without an owner, keyed by a string `tag`, with the nullable string `color`.
fn league_schema() -> Schema {
    let rankings = TableSchema::builder("rankings")
        .primary_key("ranking_id", DataType::Int)
        .column("creator", DataType::String)
        .column("name", DataType::String)
        .owner("creator", OwnerIdentity::ClientId)
        .unique("rankings_name", ["name"]);
    let members = TableSchema::builder("members")
        .primary_key("ranking_id", DataType::Int)
        .primary_key("member_id", DataType::String)
        .column("display_name", DataType::String)
        .owner("member_id", OwnerIdentity::ClientId)
        .foreign_key("members_ranking", ["ranking_id"], "rankings");
    let votes = TableSchema::builder("votes")
        .primary_key("voter", DataType::String)
        .primary_key("ranking_id", DataType::Int)
        .column("candidate", DataType::String)
        .owner("voter", OwnerIdentity::ClientId)
        .immutable();
    let tags = TableSchema::builder("tags")
        .primary_key("tag", DataType::String)
        .nullable_column("color", DataType::String);
    Schema::builder()
        .table(rankings)
        .table(members)
        .table(votes)
        .table(tags)
        .build()
}

/// An insert into `table` of [`league_schema`] of `row`.
fn insert(table: &str, row: RowBuilder) -> Operation {
    let row = row.build();
    let insert = league_schema().to_operation_insert(table, &row, 1000);
    insert.unwrap()
}

/// A server with `room-a` of [`league_schema`], where bob created the ranking 1, `Bob's`, and
/// joined it.
async fn league() -> (TestServer, RoomId) {
    let server = TestServer::new();
    let room_id = server
        .create_room_with("room-a", "league", league_schema())
        .await;
    server
        .commit_ops(&room_id, [bobs_ranking(1, "Bob's")])
        .await;
    let member = RowBuilder::new()
        .set("ranking_id", 1)
        .set("member_id", "bob")
        .set("display_name", "Bob");
    server
        .commit_ops(&room_id, [insert("members", member)])
        .await;
    (server, room_id)
}

/// The insert of bob's ranking `id`, named `name`.
fn bobs_ranking(id: i64, name: &str) -> Operation {
    let ranking = RowBuilder::new()
        .set("ranking_id", id)
        .set("creator", "bob")
        .set("name", name);
    insert("rankings", ranking)
}

/// A facade of `person` over `storage`, with its metadata in `dir`, that prechecks its writes,
/// as it does by default, or does not, if `precheck` is false.
async fn facade(
    dir: &Path,
    storage: impl StorageEngine + 'static,
    transport: &TestTransport,
    person: &str,
    precheck: bool,
) -> ZemdbClient {
    let metadata = FileMetadata::new(dir);
    let mut config = ClientConfig::new(person);
    if !precheck {
        config.precheck = false;
    }
    let tokens = TestTokens::new();
    let client = ZemdbClient::open(config, storage, metadata, transport.clone(), tokens, None);
    client.await.unwrap()
}

/// Opens `room_id` through `client`, with a token of the client.
async fn open(client: &ZemdbClient, room_id: &RoomId) -> RoomHandle {
    let token = token(&client.client_id(), room_id);
    client.room(room_id.clone(), token).await.unwrap()
}

/// The number of commits sent through `transport`.
fn commits(transport: &TestTransport) -> usize {
    let sent = transport.sent().into_iter();
    sent.filter(|message| matches!(message, ClientMessage::Commit { .. }))
        .count()
}

/// Catches the replica of `room` up with the log: the catch-up of the insert of the tag `tag`,
/// which the precheck lets out, brings every sequence before it.
async fn catch_up(room: &RoomHandle, tag: &str) {
    let tag = Row::new().with("tag", tag);
    room.table("tags").unwrap().insert(&tag).await.unwrap();
}

async fn head(server: &TestServer, room_id: &RoomId) -> u64 {
    let bounds = server.state.room_manager.log_bounds(room_id).await;
    bounds.unwrap().head_seq.get()
}

fn ranking(id: i64, name: &str) -> Row {
    Row::new().with("ranking_id", id).with("name", name)
}

fn ranking_key(id: i64) -> Key {
    Key::new().with("ranking_id", id)
}

fn member(ranking: i64, name: &str) -> Row {
    Row::new()
        .with("ranking_id", ranking)
        .with("display_name", name)
}

fn renamed(name: &str) -> Row {
    Row::new().with("name", name)
}

/// The reasons the replica rejects the writes of [`rejected_writes`], in their order.
fn reasons() -> Vec<Rejection> {
    let constraint = |violation| Rejection::Constraint(violation);
    let name = |name: &str| name.to_string();
    vec![
        constraint(Violation::Unique {
            constraint: name("rankings_name"),
        }),
        constraint(Violation::ForeignKey {
            constraint: name("members_ranking"),
        }),
        constraint(Violation::Restrict {
            constraint: name("members_ranking"),
        }),
        constraint(Violation::Duplicate),
        constraint(Violation::NotOwner),
        constraint(Violation::NotOwner),
        constraint(Violation::NotOwner),
        Rejection::RowMissing,
    ]
}

/// Each write of ana through `room`, caught up, that the replica rejects, after the writes it
/// needs (her ranking 2, her membership of it and her vote in it): its result, and the commits it
/// sent through `transport`. Then the delete of a row that is not there, which changes nothing
/// and is not rejected.
async fn rejected_writes(
    room: &RoomHandle,
    transport: &TestTransport,
) -> Vec<(Result<(), ClientError>, usize)> {
    let rankings = room.table("rankings").unwrap();
    let members = room.table("members").unwrap();
    let votes = room.table("votes").unwrap();
    rankings.insert(&ranking(2, "Ana's")).await.unwrap();
    members.insert(&member(2, "Ana")).await.unwrap();
    let vote = Row::new().with("ranking_id", 2).with("candidate", "bob");
    votes.insert(&vote).await.unwrap();

    let mut rejected = Vec::new();
    let mut sent = commits(transport);
    let mut taken = |result: Result<(), ClientError>| {
        let now = commits(transport);
        rejected.push((result, now - sent));
        sent = now;
    };
    // Another ranking with the name of bob's.
    taken(rankings.insert(&ranking(3, "Bob's")).await.map(drop));
    // A member of a ranking that is not there.
    taken(members.insert(&member(9, "Ana")).await.map(drop));
    // The delete of a ranking with a member.
    taken(rankings.delete(&ranking_key(2)).await);
    // An insert of a key that is there, in an immutable table.
    let again = vote.with("candidate", "ana");
    taken(votes.insert(&again).await.map(drop));
    // A ranking of bob, whose owner is outside the key: replaced, updated or deleted.
    taken(rankings.insert(&ranking(1, "Mine")).await.map(drop));
    taken(rankings.update(&ranking_key(1), &renamed("Mine")).await);
    taken(rankings.delete(&ranking_key(1)).await);
    // An update of a row that is not there.
    taken(rankings.update(&ranking_key(7), &renamed("Lost")).await);

    // The delete of a row that is not there changes nothing: it goes out, and the replica takes
    // it.
    let sent = commits(transport);
    rankings.delete(&ranking_key(8)).await.unwrap();
    assert_eq!(commits(transport), sent + 1);
    rejected
}

#[tokio::test]
async fn a_write_that_the_replica_would_reject_is_precheck_failed_and_sends_nothing() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", true).await;
    let room = open(&client, &room_id).await;
    catch_up(&room, "red").await;

    let rejected = rejected_writes(&room, &transport).await;
    let expected: Vec<_> = (reasons().into_iter())
        .map(|reason| (Err(ClientError::PrecheckFailed(reason)), 0))
        .collect();
    assert_eq!(rejected, expected);
    // The log has bob's two rows, the tag, ana's three rows and the delete of a missing row.
    assert_eq!(head(&server, &room_id).await, 2 + 1 + 3 + 1);
    // Bob's ranking did not change.
    let bobs = ranking(1, "Bob's").with("creator", "bob");
    let rankings = room.table("rankings").unwrap();
    assert_eq!(rankings.get(&ranking_key(1)).await, Ok(Some(bobs)));
}

#[tokio::test]
async fn without_the_precheck_the_same_writes_go_out_and_the_replica_decides() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", false).await;
    let room = open(&client, &room_id).await;
    catch_up(&room, "red").await;

    let rejected = rejected_writes(&room, &transport).await;
    let decided = |reason| match reason {
        Rejection::Constraint(violation) => ClientError::ConstraintViolation(violation),
        Rejection::RowMissing => ClientError::RowMissing,
    };
    let expected: Vec<_> = (reasons().into_iter())
        .map(|reason| (Err(decided(reason)), 1))
        .collect();
    assert_eq!(rejected, expected);
    // Each rejected write took a sequence of its own.
    assert_eq!(head(&server, &room_id).await, 2 + 1 + 3 + 8 + 1);
}

#[tokio::test]
async fn a_replica_behind_the_log_stops_a_write_that_the_log_would_take_until_it_catches_up() {
    let (server, room_id) = league().await;
    let transport = server.transport();
    let (ana_dir, carol_dir) = (tempdir().unwrap(), tempdir().unwrap());
    let ana = facade(
        ana_dir.path(),
        engine(ana_dir.path()),
        &transport,
        "ana",
        true,
    );
    let carol = facade(
        carol_dir.path(),
        engine(carol_dir.path()),
        &transport,
        "carol",
        false,
    );
    let (ana, carol) = (ana.await, carol.await);
    // Each replica opens without the rows of the log: it registered, and did not sync.
    let (on_ana, on_carol) = (open(&ana, &room_id).await, open(&carol, &room_id).await);
    let members = on_ana.table("members").unwrap();
    let rankings = on_ana.table("rankings").unwrap();
    assert_eq!(rankings.get(&ranking_key(1)).await, Ok(None));

    // Ana's replica does not have bob's ranking yet, so it would reject her membership of it,
    // which the log takes: the precheck stops it, and nothing is sent.
    let sent = transport.sent().len();
    let foreign_key = Violation::ForeignKey {
        constraint: "members_ranking".to_string(),
    };
    let stopped = members.insert(&member(1, "Ana")).await;
    let failed = ClientError::PrecheckFailed(Rejection::Constraint(foreign_key));
    assert_eq!(stopped, Err(failed));
    assert_eq!(transport.sent().len(), sent);
    // Without the precheck, carol's replica, as far behind, sends hers, and the log takes it.
    let joined = on_carol.table("members").unwrap();
    joined.insert(&member(1, "Carol")).await.unwrap();

    // Once ana's replica catches up, her membership goes out, and the log takes it.
    catch_up(&on_ana, "red").await;
    let key = members.insert(&member(1, "Ana")).await.unwrap();
    let joined = member(1, "Ana").with("member_id", "ana");
    assert_eq!(members.get(&key).await, Ok(Some(joined)));
    assert_eq!(head(&server, &room_id).await, 2 + 1 + 1 + 1);
}

#[tokio::test]
async fn a_write_that_passes_the_precheck_ends_with_the_decision_of_the_replica() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", true).await;
    let room = open(&client, &room_id).await;
    catch_up(&room, "red").await;
    let rankings = room.table("rankings").unwrap();

    // Bob's ranking `Shared`, sequenced before ana's of the same name, is not in her replica yet:
    // her insert passes the precheck, goes out, and the replica rejects it on its sequence.
    server
        .commit_ops(&room_id, [bobs_ranking(5, "Shared")])
        .await;
    let sent = commits(&transport);
    let unique = Violation::Unique {
        constraint: "rankings_name".to_string(),
    };
    let inserted = rankings.insert(&ranking(6, "Shared")).await;
    assert_eq!(inserted, Err(ClientError::ConstraintViolation(unique)));
    assert_eq!(commits(&transport), sent + 1);
    assert_eq!(head(&server, &room_id).await, 2 + 1 + 1 + 1);
    assert_eq!(rankings.get(&ranking_key(6)).await, Ok(None));
}

#[tokio::test]
async fn the_errors_of_the_storage_in_the_precheck() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = facade(dir.path(), storage.clone(), &transport, "ana", true).await;
    let room = open(&client, &room_id).await;
    catch_up(&room, "red").await;
    let (rankings, tags) = (room.table("rankings").unwrap(), room.table("tags").unwrap());
    let blue = || Row::new().with("tag", "blue");

    // A storage room that does not take the operation (its schema changed since the plan) is
    // `InvalidOperation`, and one that is closed, `RoomClosed`: nothing is sent.
    let sent = transport.sent().len();
    let invalid = StorageError::SchemaValidation(ValidationError::TableNotFound("tags".into()));
    storage.fail_next("check_operation", invalid);
    let message = match tags.insert(&blue()).await {
        Err(ClientError::InvalidOperation(message)) => message,
        other => panic!("{other:?}"),
    };
    assert!(message.contains("does not take the operation"), "{message}");
    let no_table = StorageError::TableNotFound {
        room_id: room_id.clone(),
        table: "tags".to_string(),
    };
    storage.fail_next("check_operation", no_table);
    let message = match tags.insert(&blue()).await {
        Err(ClientError::InvalidOperation(message)) => message,
        other => panic!("{other:?}"),
    };
    assert!(message.contains("does not take the operation"), "{message}");
    storage.fail_next(
        "check_operation",
        StorageError::RoomNotFound(room_id.clone()),
    );
    let closed = Err(ClientError::RoomClosed(room_id.clone()));
    assert_eq!(tags.insert(&blue()).await.map(drop), closed);
    assert_eq!(transport.sent().len(), sent);

    // A write that the facade does not plan, or that does not fit, does not reach the precheck:
    // the error of the storage goes to the next one.
    storage.fail_next(
        "check_operation",
        StorageError::RoomNotFound(room_id.clone()),
    );
    let unknown = Row::new().with("tag", "blue").with("shade", "dark");
    let large = Row::new()
        .with("tag", "t".repeat(8 << 20))
        .with("color", "x".repeat(7 << 20));
    let rules = [
        (unknown, "has no column 'shade'"),
        (large, "needs a log record"),
    ];
    for (row, rule) in rules {
        let message = match tags.insert(&row).await {
            Err(ClientError::InvalidOperation(message)) => message,
            other => panic!("{other:?}"),
        };
        assert!(message.contains(rule), "{message}");
    }
    assert_eq!(tags.insert(&blue()).await.map(drop), closed);
    assert_eq!(transport.sent().len(), sent);

    // Any other error leaves the precheck without a decision: the write goes out, and the
    // replica decides, also to reject it.
    let io = StorageError::Io(std::io::Error::other("The disk failed"));
    storage.fail_next("check_operation", io);
    let unique = Violation::Unique {
        constraint: "rankings_name".to_string(),
    };
    let named = rankings.insert(&ranking(3, "Bob's")).await;
    assert_eq!(named, Err(ClientError::ConstraintViolation(unique)));
    let failed = StorageError::RoomFailed {
        room_id: room_id.clone(),
        reason: "A compaction failed".to_string(),
    };
    storage.fail_next("check_operation", failed);
    assert_eq!(
        tags.insert(&blue()).await,
        Ok(Key::new().with("tag", "blue"))
    );
    assert_eq!(commits(&transport), 1 + 2);
    assert_eq!(head(&server, &room_id).await, 2 + 1 + 2);
}

#[tokio::test]
async fn a_write_whose_storage_room_closes_during_its_precheck_is_room_closed() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = facade(dir.path(), storage.clone(), &transport, "ana", true).await;
    let room = open(&client, &room_id).await;
    let tags = room.table("tags").unwrap();
    let (red, blue) = (
        Row::new().with("tag", "red"),
        Row::new().with("tag", "blue"),
    );

    // The precheck of the insert of `red` waits while the catch-up of the insert of `blue` fails
    // to apply, and its storage room reopens (DC21): it reaches the storage room closed.
    let mut checking = storage.hold_next("check_operation");
    let held = tags.clone();
    let prechecked = tokio::spawn(async move { held.insert(&red).await });
    checking.replied().await;
    storage.fail_next(
        "apply_batch",
        StorageError::Io(std::io::Error::other("The disk failed")),
    );
    let mut opening = storage.hold_next("open_room");
    let sent = commits(&transport);
    let reopened = tags.clone();
    let written = tokio::spawn(async move { reopened.insert(&blue).await });
    opening.replied().await;
    checking.release();
    let prechecked = prechecked.await.unwrap();
    assert_eq!(prechecked, Err(ClientError::RoomClosed(room_id.clone())));
    opening.release();
    let written = written.await.unwrap();
    assert_eq!(written, Ok(Key::new().with("tag", "blue")));
    // Only the insert of `blue` went out.
    assert_eq!(commits(&transport), sent + 1);
    let red_key = Key::new().with("tag", "red");
    assert_eq!(tags.get(&red_key).await, Ok(None));
    assert_eq!(head(&server, &room_id).await, 2 + 1);
}

/// Ana's facade of `room_id` over a [`TestEngine`] and a transport, with her ranking 2 in the
/// replica, which then opens again without the server, unregistered, while the room is created
/// again with `schema`.
async fn reopened_before_the_room_is_created_again(
    server: &TestServer,
    room_id: &RoomId,
    dir: &Path,
    schema: Schema,
) -> (
    TestEngine<DiskStorageEngine>,
    TestTransport,
    ZemdbClient,
    RoomHandle,
) {
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir));
    let client = facade(dir, storage.clone(), &transport, "ana", true).await;
    let room = open(&client, room_id).await;
    let rankings = room.table("rankings").unwrap();
    rankings.insert(&ranking(2, "Ana's")).await.unwrap();
    room.close().await.unwrap();
    transport.offline();
    open(&client, room_id).await;
    let opened = room.status().await.unwrap();
    assert_eq!((opened.epoch, opened.registered), (1, false));
    let rooms = &server.state.room_manager;
    rooms.delete_room(room_id).await.unwrap();
    server
        .create_room_with(room_id.as_str(), "again", schema)
        .await;
    transport.online();
    (storage, transport, client, room)
}

/// What `write`, a write through `room`, gives when, while its precheck waits, the insert of a tag
/// registers the replica, which is discarded for `RoomReset` and opens with the room created
/// again: the precheck then reaches the storage room of the new replica. Checks that all that went
/// out meanwhile through `transport` are the two registrations of the insert of the tag.
async fn discarded_during_the_precheck(
    storage: &TestEngine<DiskStorageEngine>,
    transport: &TestTransport,
    room: &RoomHandle,
    write: impl Future<Output = Result<(), ClientError>>,
) -> Result<(), ClientError> {
    let sent = transport.sent().len();
    let mut checking = storage.hold_next("check_operation");
    let discarding = async {
        checking.replied().await;
        let tag = Row::new().with("tag", "red");
        let tagged = room.table("tags").unwrap().insert(&tag).await;
        assert_eq!(tagged, Err(ClientError::RoomClosed(room.room_id().clone())));
        assert_eq!(room.status().await, Ok(status(0, 2, true)));
        checking.release();
    };
    let (written, ()) = tokio::join!(write, discarding);
    let kinds: Vec<_> = (transport.sent().iter().skip(sent))
        .map(ClientMessage::kind)
        .collect();
    let register = ClientMessageKind::RegisterClient;
    assert_eq!(kinds, [register, register]);
    written
}

#[tokio::test]
async fn a_write_whose_replica_is_discarded_during_its_precheck_is_room_closed() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    // The room created again has a table of the id of `rankings` that does not take its insert.
    let scores = TableSchema::builder("scores")
        .primary_key("number", DataType::Int)
        .column("points", DataType::Int);
    let scores = Schema::builder().table(scores).build();
    let (storage, transport, _client, room) =
        reopened_before_the_room_is_created_again(&server, &room_id, dir.path(), scores).await;
    let rankings = room.table("rankings").unwrap();
    let insert = async { rankings.insert(&ranking(7, "Ana's")).await.map(drop) };
    let inserted = discarded_during_the_precheck(&storage, &transport, &room, insert).await;
    assert_eq!(inserted, Err(ClientError::RoomClosed(room_id.clone())));
    assert_eq!(head(&server, &room_id).await, 0);
}

#[tokio::test]
async fn a_write_whose_replica_is_discarded_during_its_precheck_does_not_take_its_decision() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    // The room created again, of the same schema, is empty: the new replica would reject the
    // update of ana's ranking 2, which the replica of the plan has.
    let (storage, transport, _client, room) =
        reopened_before_the_room_is_created_again(&server, &room_id, dir.path(), league_schema())
            .await;
    let rankings = room.table("rankings").unwrap();
    let (key, renamed) = (ranking_key(2), renamed("Ana's league"));
    let update = rankings.update(&key, &renamed);
    let updated = discarded_during_the_precheck(&storage, &transport, &room, update).await;
    assert_eq!(updated, Err(ClientError::RoomClosed(room_id.clone())));
    assert_eq!(head(&server, &room_id).await, 0);
}

#[tokio::test]
async fn a_write_to_a_room_that_fails_closed_is_failed_closed_without_the_precheck() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", true).await;
    let room = open(&client, &room_id).await;
    catch_up(&room, "red").await;
    let (rankings, members) = (
        room.table("rankings").unwrap(),
        room.table("members").unwrap(),
    );
    rankings.insert(&ranking(2, "Ana's")).await.unwrap();
    members.insert(&member(2, "Ana")).await.unwrap();
    room.close().await.unwrap();
    // The server answers the registration with a schema that does not evolve from the replica's.
    let incarnation = server.incarnation(&room_id).await;
    let scores = TableSchema::builder("scores")
        .primary_key("number", DataType::Int)
        .column("points", DataType::Int);
    let scores = Schema::builder().table(scores).build();
    transport.answer_next_registration(move |correlation_id, room_id| {
        let registered = ServerMessage::Registered {
            correlation_id,
            room_id: room_id.clone(),
            head_seq: SequenceNumber::new(5),
            tail_seq: SequenceNumber::new(0),
            schema_id: SchemaId::new("scores").unwrap(),
            schema: scores.clone(),
            active_snapshot_seq: None,
            room_incarnation: incarnation,
        };
        encode_message(&registered).unwrap()
    });
    let room = open(&client, &room_id).await;
    assert!(room.status().await.unwrap().failed_closed);

    // The replica no longer catches up: the writes that it would reject (an update of a row
    // that it lacks, the delete of a ranking with a member), and one that it would take, are
    // `FailedClosed`, and nothing is sent.
    let sent = transport.sent().len();
    let failed_closed = Err(ClientError::FailedClosed(room_id.clone()));
    let missing = rankings.update(&ranking_key(7), &renamed("Lost")).await;
    assert_eq!(missing, failed_closed);
    assert_eq!(rankings.delete(&ranking_key(2)).await, failed_closed);
    let tag = Row::new().with("tag", "blue");
    let tags = room.table("tags").unwrap();
    assert_eq!(tags.insert(&tag).await.map(drop), failed_closed);
    assert_eq!(transport.sent().len(), sent);
}
