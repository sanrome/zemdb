//! The writes of the facade against the server in process: the order of a write, with the owner
//! that the client fills, the validation by name before anything is sent, and the decisions of
//! the replica.

mod common;

use common::*;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use zemdb_client::{
    ClientConfig, ClientError, FileMetadata, Key, RoomHandle, RoomStatus, Row, Scan, Violation,
    ZemdbClient,
};
use zemdb_core::{
    encode_message, ClientMessage, ClientMessageKind, CryptoEngine, DataType, ErrorCode,
    NoOpCryptoEngine, Operation, OperationKind, OwnerIdentity, RoomId, RowBuilder, Schema,
    SchemaId, SequenceNumber, ServerMessage, TableSchema, Value,
};
use zemdb_storage::{StorageEngine, StorageError};

/// A league of rankings:
/// - `rankings`, keyed by an int `ranking_id`, with its owner `creator` outside the key and a
///   unique string `name`;
/// - `members`, keyed by an int `ranking_id` and the owner column `member_id`, with a string
///   `display_name`, and a foreign key to `rankings`;
/// - `votes`, immutable, keyed by the owner column `voter` and an int `ranking_id`, with a string
///   `candidate`;
/// - `rounds`, keyed by an int `round_id`, with a string `title` and the nullable encrypted
///   string `notes`;
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
    let rounds = TableSchema::builder("rounds")
        .primary_key("round_id", DataType::Int)
        .column("title", DataType::String)
        .nullable_column("notes", DataType::String)
        .encrypted_column("notes", DataType::String);
    let tags = TableSchema::builder("tags")
        .primary_key("tag", DataType::String)
        .nullable_column("color", DataType::String);
    Schema::builder()
        .table(rankings)
        .table(members)
        .table(votes)
        .table(rounds)
        .table(tags)
        .build()
}

/// Table `notes`, with the id of `rankings` and columns of its types, without an owner: an int key
/// `number`, and the strings `author` and `text`.
fn notes_schema() -> Schema {
    let notes = TableSchema::builder("notes")
        .primary_key("number", DataType::Int)
        .column("author", DataType::String)
        .column("text", DataType::String);
    Schema::builder().table(notes).build()
}

/// An insert into `table` of [`league_schema`] of `row`.
fn insert(table: &str, row: RowBuilder) -> Operation {
    let row = row.build();
    let insert = league_schema().to_operation_insert(table, &row, 1000);
    insert.unwrap()
}

/// A server with `room-a` of [`league_schema`], where bob created the ranking 1, `Bob's`, joined
/// it, and opened the round 1 with a ciphertext in `notes`.
async fn league() -> (TestServer, RoomId) {
    let server = TestServer::new();
    let room_id = server
        .create_room_with("room-a", "league", league_schema())
        .await;
    let ranking = RowBuilder::new()
        .set("ranking_id", 1)
        .set("creator", "bob")
        .set("name", "Bob's");
    let member = RowBuilder::new()
        .set("ranking_id", 1)
        .set("member_id", "bob")
        .set("display_name", "Bob");
    let round = RowBuilder::new()
        .set("round_id", 1)
        .set("title", "Kickoff")
        .set("notes", vec![0xc1_u8, 0x9a, 0x07]);
    let ops = [
        insert("rankings", ranking),
        insert("members", member),
        insert("rounds", round),
    ];
    server.commit_ops(&room_id, ops).await;
    (server, room_id)
}

/// A facade of `person` over `storage`, with its metadata in `dir`.
async fn facade(
    dir: &Path,
    storage: impl StorageEngine + 'static,
    transport: &TestTransport,
    person: &str,
    crypto: Option<Arc<dyn CryptoEngine>>,
) -> ZemdbClient {
    let metadata = FileMetadata::new(dir);
    let config = ClientConfig::new(person);
    let tokens = TestTokens::new();
    let client = ZemdbClient::open(config, storage, metadata, transport.clone(), tokens, crypto);
    client.await.unwrap()
}

/// Opens `room_id` through `client`, with a token of the client.
async fn open(client: &ZemdbClient, room_id: &RoomId) -> Result<RoomHandle, ClientError> {
    let token = token(&client.client_id(), room_id);
    client.room(room_id.clone(), token).await
}

/// The operations of the commits sent through `transport` after its first `from` messages.
fn committed(transport: &TestTransport, from: usize) -> Vec<Operation> {
    let sent = transport.sent().into_iter().skip(from);
    let op = |message| match message {
        ClientMessage::Commit { op, .. } => Some(op),
        _ => None,
    };
    sent.filter_map(op).collect()
}

/// The owner that `op` declares, for [`league_schema`].
fn declared_owner(op: &Operation) -> Option<Value> {
    let schema = league_schema();
    let table = schema.get_table_by_id(op.table_id()).unwrap();
    table.declared_owner(op).cloned()
}

fn ranking_key(ranking: i64) -> Key {
    Key::new().with("ranking_id", ranking)
}

fn member_key(ranking: i64, member: &str) -> Key {
    Key::new()
        .with("ranking_id", ranking)
        .with("member_id", member)
}

fn renamed(name: &str) -> Row {
    Row::new().with("name", name)
}

fn is_invalid<T: std::fmt::Debug>(result: Result<T, ClientError>) -> String {
    match result {
        Err(ClientError::InvalidOperation(message)) => message,
        other => panic!("Not InvalidOperation: {other:?}"),
    }
}

fn is_crypto<T: std::fmt::Debug>(result: Result<T, ClientError>) -> String {
    match result {
        Err(ClientError::Crypto(message)) => message,
        other => panic!("Not Crypto: {other:?}"),
    }
}

fn violation(violation: Violation) -> ClientError {
    ClientError::ConstraintViolation(violation)
}

/// The wall clock, in milliseconds.
fn now() -> u64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    u64::try_from(since.as_millis()).unwrap()
}

async fn head(server: &TestServer, room_id: &RoomId) -> u64 {
    let bounds = server.state.room_manager.log_bounds(room_id).await;
    bounds.unwrap().head_seq.get()
}

#[tokio::test]
async fn the_writes_go_out_with_the_owner_of_the_person_and_the_server_takes_them() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", None).await;
    assert_ne!(client.client_id().as_str(), "ana");
    let room = open(&client, &room_id).await.unwrap();
    let sent = transport.sent().len();
    let rankings = room.table("rankings").unwrap();
    let members = room.table("members").unwrap();
    let tags = room.table("tags").unwrap();
    let started = now();

    // With the owner outside the key: the row lacks it, and the key does not have it.
    let ranking = Row::new().with("ranking_id", 2).with("name", "Ana's");
    let ranking_two = rankings.insert(&ranking).await.unwrap();
    assert_eq!(ranking_two, ranking_key(2));
    let created = ranking.with("creator", "ana");
    assert_eq!(rankings.get(&ranking_two).await, Ok(Some(created.clone())));
    let league = renamed("Ana's league");
    rankings.update(&ranking_two, &league).await.unwrap();
    let created = created.with("name", "Ana's league");
    assert_eq!(rankings.get(&ranking_two).await, Ok(Some(created)));

    // With the owner in the key: the insert returns the key with it, which names the row for
    // reads and writes, and so does a key without it in a write.
    let member = Row::new().with("ranking_id", 2).with("display_name", "Ana");
    let ana = members.insert(&member).await.unwrap();
    assert_eq!(ana, member_key(2, "ana"));
    let joined = member.with("member_id", "ana");
    assert_eq!(members.get(&ana).await, Ok(Some(joined.clone())));
    let ann = Row::new().with("display_name", "Ann");
    members.update(&ana, &ann).await.unwrap();
    let without_owner = ranking_key(2);
    let annie = Row::new().with("display_name", "Annie");
    members.update(&without_owner, &annie).await.unwrap();
    let joined = joined.with("display_name", "Annie");
    assert_eq!(members.get(&ana).await, Ok(Some(joined)));
    members.delete(&without_owner).await.unwrap();
    assert_eq!(members.get(&ana).await, Ok(None));
    rankings.delete(&ranking_two).await.unwrap();
    assert_eq!(rankings.get(&ranking_two).await, Ok(None));

    // Without an owner: a column the row lacks that takes NULL gets it.
    let red = tags.insert(&Row::new().with("tag", "red")).await.unwrap();
    assert_eq!(red, Key::new().with("tag", "red"));
    let tag = Row::new().with("tag", "red").with("color", Value::Null);
    assert_eq!(tags.get(&red).await, Ok(Some(tag)));
    let color = Row::new().with("color", "#f00");
    tags.update(&red, &color).await.unwrap();
    tags.delete(&red).await.unwrap();

    // Each went out once, with the owner of the person in a table with an owner, and with none
    // in the table without one, at the time of the wall clock.
    let ops = committed(&transport, sent);
    let tables: Vec<_> = ops.iter().map(Operation::table_id).collect();
    assert_eq!(tables, [0, 0, 1, 1, 1, 1, 0, 4, 4, 4]);
    let ended = now();
    for op in &ops {
        let owner = (op.table_id() != 4).then(|| Value::from("ana"));
        assert_eq!(declared_owner(op), owner, "{op:?}");
        assert!((started..=ended).contains(&op.timestamp()), "{op:?}");
    }
    // The delete of `rankings` declares its owner, which is outside the key; the others, none.
    let delete_owners: Vec<_> = (ops.iter())
        .filter_map(|op| match op.kind() {
            OperationKind::Delete { owner } => Some(owner.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(delete_owners, [None, Some(Value::from("ana")), None]);
    // The server took them: its log has them after bob's three.
    assert_eq!(head(&server, &room_id).await, 3 + 10);
}

#[tokio::test]
async fn what_is_not_of_the_schema_or_gives_the_owner_is_invalid_and_sends_nothing() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", None).await;
    let room = open(&client, &room_id).await.unwrap();
    let sent = transport.sent().len();
    let rankings = room.table("rankings").unwrap();
    let members = room.table("members").unwrap();
    let votes = room.table("votes").unwrap();
    let tags = room.table("tags").unwrap();
    let secret = "secret-value";
    let member = Row::new().with("ranking_id", 1).with("display_name", "Ana");
    let mut messages = Vec::new();
    // Each is invalid for what the facade checks by name, before the operation that it would
    // send is checked as the server checks it, which would refuse some too.
    let mut invalid = |result: Result<(), ClientError>, why: &str| {
        let message = is_invalid(result);
        assert!(message.contains(why), "{message}");
        messages.push(message);
    };

    // The owner column in the row or in the changes, also with the owner of the client, or
    // another owner in the key.
    let given = member.clone().with("member_id", "ana");
    invalid(members.insert(&given).await.map(drop), "is its owner");
    let given = Row::new()
        .with("ranking_id", 5)
        .with("creator", "ana")
        .with("name", "Ana's");
    invalid(rankings.insert(&given).await.map(drop), "is its owner");
    let given = Row::new().with("creator", "ana");
    invalid(
        rankings.update(&ranking_key(1), &given).await,
        "is its owner",
    );
    let display = Row::new().with("display_name", "Ana");
    let other = member_key(1, secret);
    invalid(members.update(&other, &display).await, "another owner");
    invalid(members.delete(&member_key(1, "bob")).await, "another owner");

    // A column of the key in the changes, and no changes.
    let rekeyed = Row::new().with("ranking_id", 2);
    let ana = member_key(1, "ana");
    invalid(members.update(&ana, &rekeyed).await, "is of the key");
    let unchanged = rankings.update(&ranking_key(1), &Row::new()).await;
    invalid(unchanged, "changes no column");

    // A required column that the row lacks, a column that the table does not have, a value of
    // another type, and NULL where the column does not take it.
    let lacking = Row::new().with("ranking_id", 1);
    let lacks = "lacks the column 'display_name'";
    invalid(members.insert(&lacking).await.map(drop), lacks);
    let extra = member.clone().with("color", secret);
    invalid(members.insert(&extra).await.map(drop), "no column 'color'");
    let retyped = member.clone().with("ranking_id", secret);
    invalid(members.insert(&retyped).await.map(drop), "is of type");
    let null = member.clone().with("display_name", Value::Null);
    invalid(members.insert(&null).await.map(drop), "does not take NULL");
    for (change, why) in [
        (renamed(secret).with("color", "red"), "no column 'color'"),
        (Row::new().with("name", 7), "is of type"),
        (Row::new().with("name", Value::Null), "does not take NULL"),
    ] {
        invalid(rankings.update(&ranking_key(1), &change).await, why);
    }

    // A key with a column less or more, or with a value of another type.
    for (key, why) in [
        (Key::new(), "lacks 'ranking_id'"),
        (ranking_key(1).with("name", secret), "is not of the key"),
        (Key::new().with("ranking_id", secret), "is of type"),
        (
            Key::new().with("ranking_id", Value::Null),
            "does not take NULL",
        ),
    ] {
        invalid(rankings.update(&key, &renamed("New")).await, why);
        invalid(rankings.delete(&key).await, why);
    }
    let lacking = Key::new().with("member_id", "ana");
    invalid(members.delete(&lacking).await, "lacks 'ranking_id'");
    let retyped = member_key(1, "ana").with("member_id", 7);
    invalid(members.delete(&retyped).await, "is of type");

    // An update or a delete of an immutable table, which the server does not take.
    let candidate = Row::new().with("candidate", "bob");
    invalid(
        votes.update(&ranking_key(1), &candidate).await,
        "is immutable",
    );
    invalid(votes.delete(&ranking_key(1)).await, "is immutable");

    // An operation larger than the limit of the server: by its values alone, before it is
    // planned, in an insert and in an update, or once it is built.
    let huge = || "x".repeat(16 << 20);
    let values = "its values encode to more than";
    let large = Row::new().with("tag", "large").with("color", huge());
    invalid(tags.insert(&large).await.map(drop), values);
    let red = Key::new().with("tag", "red");
    let large = Row::new().with("color", huge());
    invalid(tags.update(&red, &large).await, values);
    // Its values fit in a message, and the key that the insert repeats does not.
    let long_tag = "t".repeat(8 << 20);
    let large = Row::new()
        .with("tag", long_tag)
        .with("color", "x".repeat(7 << 20));
    invalid(tags.insert(&large).await.map(drop), "needs a log record");

    // No message names a value, and nothing was sent.
    for message in &messages {
        assert!(!message.contains(secret), "{message}");
    }
    assert_eq!(transport.sent().len(), sent);
    assert_eq!(head(&server, &room_id).await, 3);
    assert_eq!(rankings.get(&ranking_key(5)).await, Ok(None));
}

#[tokio::test]
async fn until_the_client_encrypts_a_write_that_touches_an_encrypted_column_is_crypto() {
    let (server, room_id) = league().await;
    for crypto in [
        None,
        Some(Arc::new(NoOpCryptoEngine) as Arc<dyn CryptoEngine>),
    ] {
        let has_crypto = crypto.is_some();
        let dir = tempdir().unwrap();
        let transport = server.transport();
        let client = facade(dir.path(), engine(dir.path()), &transport, "ana", crypto).await;
        let room = open(&client, &room_id).await.unwrap();
        let sent = transport.sent().len();
        let rounds = room.table("rounds").unwrap();
        let round = |notes: Option<Value>| {
            let round = Row::new().with("round_id", 2).with("title", "Final");
            match notes {
                Some(notes) => round.with("notes", notes),
                None => round,
            }
        };

        // With its type, with NULL, or omitted, which is NULL: every insert into the table
        // touches it.
        let message = is_crypto(rounds.insert(&round(Some("secret".into()))).await);
        assert!(message.contains("'notes'"), "{message}");
        assert!(!message.contains("secret"), "{message}");
        let why = match has_crypto {
            true => "does not encrypt or decrypt columns yet",
            false => "has no CryptoEngine",
        };
        assert!(message.contains(why), "{message}");
        is_crypto(rounds.insert(&round(Some(Value::Null))).await);
        is_crypto(rounds.insert(&round(None)).await);
        let notes = |notes: Value| Row::new().with("notes", notes);
        let kickoff = Key::new().with("round_id", 1);
        is_crypto(rounds.update(&kickoff, &notes("secret".into())).await);
        is_crypto(rounds.update(&kickoff, &notes(Value::Null)).await);
        // With a value of another type, a ciphertext among them, it is invalid by name first.
        let bytes = || Value::from(vec![0xc1_u8, 0x9a]);
        is_invalid(rounds.insert(&round(Some(bytes()))).await);
        is_invalid(rounds.update(&kickoff, &notes(bytes())).await);
        let untitled = Row::new().with("round_id", 2).with("notes", "secret");
        is_invalid(rounds.insert(&untitled).await);
        assert_eq!(transport.sent().len(), sent);

        // A write that does not touch it goes out.
        let retitled = Row::new().with("title", "Opening");
        rounds.update(&kickoff, &retitled).await.unwrap();
        let clear = Scan::new().columns(["round_id", "title"]);
        let opening = Row::new().with("round_id", 1).with("title", "Opening");
        assert_eq!(rounds.scan(&clear).await, Ok(vec![opening]));
        let ops = committed(&transport, sent);
        assert_eq!(ops.len(), 1);
        client.shutdown().await.unwrap();
    }
    // And so does a delete.
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", None).await;
    let room = open(&client, &room_id).await.unwrap();
    let rounds = room.table("rounds").unwrap();
    let kickoff = Key::new().with("round_id", 1);
    rounds.delete(&kickoff).await.unwrap();
    let clear = Scan::new().columns(["round_id"]);
    assert_eq!(rounds.scan(&clear).await, Ok(vec![]));
}

#[tokio::test]
async fn the_decisions_of_the_replica_come_as_in_the_core() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", None).await;
    let room = open(&client, &room_id).await.unwrap();
    let rankings = room.table("rankings").unwrap();
    let members = room.table("members").unwrap();
    let votes = room.table("votes").unwrap();
    let ranking = |id: i64, name: &str| Row::new().with("ranking_id", id).with("name", name);
    let ana = |ranking: i64, name: &str| {
        Row::new()
            .with("ranking_id", ranking)
            .with("display_name", name)
    };
    let constraint = |name: &str| name.to_string();

    rankings.insert(&ranking(2, "Ana's")).await.unwrap();
    // Another ranking with the name of bob's.
    let unique = Violation::Unique {
        constraint: constraint("rankings_name"),
    };
    let named = rankings.insert(&ranking(3, "Bob's")).await;
    assert_eq!(named, Err(violation(unique)));
    // A member of a ranking that is not there.
    let foreign_key = Violation::ForeignKey {
        constraint: constraint("members_ranking"),
    };
    assert_eq!(
        members.insert(&ana(9, "Ana")).await,
        Err(violation(foreign_key))
    );
    // The delete of a ranking with a member.
    let key = members.insert(&ana(2, "Ana")).await.unwrap();
    let restrict = Violation::Restrict {
        constraint: constraint("members_ranking"),
    };
    let deleted = rankings.delete(&ranking_key(2)).await;
    assert_eq!(deleted, Err(violation(restrict)));
    // An insert of a key that is there replaces the row whole, but in an immutable table.
    members.insert(&ana(2, "Ann")).await.unwrap();
    let replaced = ana(2, "Ann").with("member_id", "ana");
    assert_eq!(members.get(&key).await, Ok(Some(replaced)));
    let vote = Row::new().with("ranking_id", 2).with("candidate", "bob");
    votes.insert(&vote).await.unwrap();
    let again = votes.insert(&vote.with("candidate", "ana")).await;
    assert_eq!(again, Err(violation(Violation::Duplicate)));
    // A ranking of bob, whose owner is outside the key: replaced, updated or deleted.
    let not_owner = || Some(violation(Violation::NotOwner));
    let replaced = rankings.insert(&ranking(1, "Mine")).await;
    assert_eq!(replaced.err(), not_owner());
    let renamed_one = rankings.update(&ranking_key(1), &renamed("Mine")).await;
    assert_eq!(renamed_one.err(), not_owner());
    assert_eq!(rankings.delete(&ranking_key(1)).await.err(), not_owner());
    // An update of a row that is not there; the delete of one changes nothing.
    let missing = rankings.update(&ranking_key(7), &renamed("Lost")).await;
    assert_eq!(missing, Err(ClientError::RowMissing));
    rankings.delete(&ranking_key(8)).await.unwrap();
    // Bob's rows did not change.
    let bobs = Row::new()
        .with("ranking_id", 1)
        .with("creator", "bob")
        .with("name", "Bob's");
    assert_eq!(rankings.get(&ranking_key(1)).await, Ok(Some(bobs)));
}

#[tokio::test]
async fn a_device_of_a_person_writes_the_rows_that_another_of_its_devices_wrote() {
    let (server, room_id) = league().await;
    let transport = server.transport();
    let (phone_dir, laptop_dir) = (tempdir().unwrap(), tempdir().unwrap());
    let phone = facade(
        phone_dir.path(),
        engine(phone_dir.path()),
        &transport,
        "ana",
        None,
    );
    let laptop = facade(
        laptop_dir.path(),
        engine(laptop_dir.path()),
        &transport,
        "ana",
        None,
    );
    let (phone, laptop) = (phone.await, laptop.await);
    assert_ne!(phone.client_id(), laptop.client_id());
    let on_phone = open(&phone, &room_id).await.unwrap();
    let ranking = Row::new().with("ranking_id", 2).with("name", "Ana's");
    let ranking = on_phone.table("rankings").unwrap().insert(&ranking).await;
    let member = Row::new().with("ranking_id", 2).with("display_name", "Ana");
    let member = on_phone.table("members").unwrap().insert(&member).await;
    let (ranking, member) = (ranking.unwrap(), member.unwrap());

    // The laptop changes and deletes them: the owner is the person, and the server and the
    // replica take them.
    let on_laptop = open(&laptop, &room_id).await.unwrap();
    let sent = transport.sent().len();
    let rankings = on_laptop.table("rankings").unwrap();
    let members = on_laptop.table("members").unwrap();
    rankings
        .update(&ranking, &renamed("Ana's league"))
        .await
        .unwrap();
    let ann = Row::new().with("display_name", "Ann");
    members.update(&member, &ann).await.unwrap();
    let joined = ann.with("ranking_id", 2).with("member_id", "ana");
    assert_eq!(members.get(&member).await, Ok(Some(joined)));
    members.delete(&member).await.unwrap();
    rankings.delete(&ranking).await.unwrap();
    assert_eq!(rankings.get(&ranking).await, Ok(None));
    let ops = committed(&transport, sent);
    assert_eq!(ops.len(), 4);
    for op in &ops {
        assert_eq!(declared_owner(op), Some(Value::from("ana")), "{op:?}");
    }
    assert_eq!(head(&server, &room_id).await, 3 + 2 + 4);
}

#[tokio::test]
async fn an_uncertain_write_is_an_unknown_outcome_and_is_not_repeated() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", None).await;
    let room = open(&client, &room_id).await.unwrap();
    let tags = room.table("tags").unwrap();
    let sent = transport.sent().len();
    // The reply to its commit is lost, and the attempt that follows is refused for good.
    transport.lose_next_reply(ClientMessageKind::Commit);
    transport.answer(move |message| {
        let ClientMessage::Commit { .. } = message else {
            return None;
        };
        let (correlation_id, room_id) = request(message);
        Some(Ok(error_reply(
            correlation_id,
            &room_id,
            ErrorCode::RoomNotFound,
        )))
    });
    let tag = Row::new().with("tag", "red");
    assert_eq!(tags.insert(&tag).await, Err(ClientError::OutcomeUnknown));
    // Every attempt carried the same mutation id, and the first reached the log, once.
    let attempts: Vec<_> = (transport.sent().into_iter().skip(sent))
        .filter_map(|message| match message {
            ClientMessage::Commit { mutation_id, .. } => Some(mutation_id),
            _ => None,
        })
        .collect();
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    assert!(attempts.iter().all(|id| *id == attempts[0]));
    assert_eq!(head(&server, &room_id).await, 3 + 1);
}

#[tokio::test]
async fn a_write_to_a_replica_that_is_not_open_is_room_closed() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = facade(dir.path(), storage.clone(), &transport, "ana", None).await;
    let room = open(&client, &room_id).await.unwrap();
    let tags = room.table("tags").unwrap();
    let red = Key::new().with("tag", "red");
    let closed = || Some(ClientError::RoomClosed(room_id.clone()));
    let writes_closed = || async {
        let sent = transport.sent().len();
        let tag = Row::new().with("tag", "red");
        assert_eq!(tags.insert(&tag).await.err(), closed());
        let color = Row::new().with("color", "#f00");
        assert_eq!(tags.update(&red, &color).await.err(), closed());
        assert_eq!(tags.delete(&red).await.err(), closed());
        assert_eq!(transport.sent().len(), sent);
    };

    // In the middle of the reopening of its storage room, after an error of the storage while
    // the catch-up of a write applies it: another write does not wait for it.
    storage.fail_next(
        "apply_batch",
        StorageError::Io(std::io::Error::other("The disk failed")),
    );
    let held = storage.hold_next("open_room");
    let reopening = async {
        let mut held = held;
        held.replied().await;
        let within = |writes| tokio::time::timeout(Duration::from_secs(10), writes);
        within(writes_closed()).await.expect("The write waited");
        held.release();
    };
    let tag = Row::new().with("tag", "blue");
    let (written, ()) = tokio::join!(tags.insert(&tag), reopening);
    assert_eq!(written, Ok(Key::new().with("tag", "blue")));
    let blue = Row::new().with("tag", "blue").with("color", Value::Null);
    let blue_key = Key::new().with("tag", "blue");
    assert_eq!(tags.get(&blue_key).await, Ok(Some(blue)));

    // Closed.
    room.close().await.unwrap();
    writes_closed().await;

    // Empty after a discard for `RoomReset`, until `Registered`: the room was created again, and
    // its registration without a cursor gets no reply.
    server.recreate_room(&room_id).await;
    let mut registrations = 0;
    transport.answer(move |message| {
        let ClientMessage::RegisterClient { .. } = message else {
            return None;
        };
        registrations += 1;
        (registrations > 1).then(|| Err(zemdb_client::TransportError("Offline".to_string())))
    });
    let opened = open(&client, &room_id).await;
    assert!(matches!(opened, Err(ClientError::Network(_))), "{opened:?}");
    writes_closed().await;

    // After a shutdown.
    transport.online();
    open(&client, &room_id).await.unwrap();
    client.shutdown().await.unwrap();
    writes_closed().await;
}

#[tokio::test]
async fn a_write_planned_before_a_discard_is_not_sent_to_the_room_created_again() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", None).await;
    let room = open(&client, &room_id).await.unwrap();
    let rankings = room.table("rankings").unwrap();
    // The replica (opened without metadata, so in the epoch 1, and not synced) opens again
    // without the server, unregistered, and the room is created again with a table of the id of
    // `rankings` that would take its insert.
    room.close().await.unwrap();
    transport.offline();
    open(&client, &room_id).await.unwrap();
    assert_eq!(room.status().await, Ok(status(0, 1, false)));
    let rooms = &server.state.room_manager;
    rooms.delete_room(&room_id).await.unwrap();
    server
        .create_room_with(room_id.as_str(), "notes", notes_schema())
        .await;
    transport.online();
    let sent = transport.sent().len();

    // The insert registers the replica, which is discarded for `RoomReset` and opens with the
    // room created again: it is not sent there. All that went out are its two registrations,
    // with the cursor and without it.
    let ranking = Row::new().with("ranking_id", 7).with("name", "Ana's");
    let inserted = rankings.insert(&ranking).await;
    assert_eq!(inserted, Err(ClientError::RoomClosed(room_id.clone())));
    let kinds: Vec<_> = (transport.sent().iter().skip(sent))
        .map(ClientMessage::kind)
        .collect();
    let register = ClientMessageKind::RegisterClient;
    assert_eq!(kinds, [register, register]);
    let registrations = transport.registrations();
    assert_eq!(registrations.last(), Some(&(None, None)));
    assert_eq!(room.status().await, Ok(status(0, 2, true)));
    assert_eq!(head(&server, &room_id).await, 0);
    // A new write resolves the table against the new schema, which has no `rankings`.
    is_invalid(rankings.insert(&ranking).await);
    let notes = room.table("notes").unwrap();
    assert_eq!(notes.scan(&Scan::new()).await, Ok(vec![]));
}

#[tokio::test]
async fn a_write_to_a_room_that_fails_closed_is_failed_closed() {
    let (server, room_id) = league().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", None).await;
    let room = open(&client, &room_id).await.unwrap();
    room.close().await.unwrap();
    // The server answers the registration with a schema that does not evolve from the replica's.
    let incarnation = server.incarnation(&room_id).await;
    transport.answer_next_registration(move |correlation_id, room_id| {
        let registered = ServerMessage::Registered {
            correlation_id,
            room_id: room_id.clone(),
            head_seq: SequenceNumber::new(3),
            tail_seq: SequenceNumber::new(0),
            schema_id: SchemaId::new("notes").unwrap(),
            schema: notes_schema(),
            active_snapshot_seq: None,
            room_incarnation: incarnation,
        };
        encode_message(&registered).unwrap()
    });
    let room = open(&client, &room_id).await.unwrap();
    let failed = RoomStatus {
        failed_closed: true,
        ..status(0, 1, false)
    };
    assert_eq!(room.status().await, Ok(failed));
    let sent = transport.sent().len();
    let tags = room.table("tags").unwrap();
    let tag = Row::new().with("tag", "red");
    let failed_closed = Some(ClientError::FailedClosed(room_id.clone()));
    assert_eq!(tags.insert(&tag).await.err(), failed_closed);
    let red = Key::new().with("tag", "red");
    assert_eq!(tags.delete(&red).await.err(), failed_closed);
    assert_eq!(transport.sent().len(), sent);
}
