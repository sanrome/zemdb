//! The facade of the client against the server in process: its handles, and the local reads of
//! the replica by the names of the columns.

mod common;

use common::*;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use zemdb_client::{
    ClientConfig, ClientError, Direction, FileMetadata, Key, RoomHandle, RoomStatus, Row, Scan,
    TransportError, Violation, ZemdbClient,
};
use zemdb_core::{
    encode_message, ClientMessage, ColumnDef, CryptoEngine, DataType, NoOpCryptoEngine, Operation,
    OwnerIdentity, RoomId, RowBuilder, Schema, SchemaId, SequenceNumber, ServerMessage,
    TableSchema, Value,
};
use zemdb_storage::{StorageEngine, StorageError};

fn rankings_id() -> SchemaId {
    SchemaId::new("rankings").unwrap()
}

/// Table `members`, keyed by an int `ranking_id` and the owner column `member_id`, with a string
/// `display_name`; and `rounds`, keyed by an int `round_id`, with a string `title` and the
/// nullable encrypted string `notes`.
fn rankings_schema() -> Schema {
    let members = TableSchema::builder("members")
        .primary_key("ranking_id", DataType::Int)
        .primary_key("member_id", DataType::String)
        .column("display_name", DataType::String)
        .owner("member_id", OwnerIdentity::ClientId);
    let rounds = TableSchema::builder("rounds")
        .primary_key("round_id", DataType::Int)
        .column("title", DataType::String)
        .nullable_column("notes", DataType::String)
        .encrypted_column("notes", DataType::String);
    Schema::builder().table(members).table(rounds).build()
}

/// [`rankings_schema`] with the nullable string column `nickname` in `members`: its version 2.
fn nicknamed_schema() -> Schema {
    let mut schema = rankings_schema();
    schema.add_column("members", nickname()).unwrap();
    schema
}

fn nickname() -> ColumnDef {
    ColumnDef::new("nickname", DataType::String).nullable(true)
}

/// [`rankings_schema`] with `display_name` an int, which does not evolve from it.
fn retyped_schema() -> Schema {
    let members = TableSchema::builder("members")
        .primary_key("ranking_id", DataType::Int)
        .primary_key("member_id", DataType::String)
        .column("display_name", DataType::Int)
        .owner("member_id", OwnerIdentity::ClientId);
    Schema::builder().table(members).build()
}

/// An insert into `members` of `schema` of `member` in `ranking`, with `nickname` if any.
fn join(schema: &Schema, ranking: i64, member: &str, nickname: Option<&str>) -> Operation {
    let mut row = RowBuilder::new()
        .set("ranking_id", ranking)
        .set("member_id", member)
        .set("display_name", member.to_uppercase());
    if let Some(nickname) = nickname {
        row = row.set("nickname", nickname);
    }
    schema
        .to_operation_insert("members", &row.build(), 1000)
        .unwrap()
}

fn key(ranking: i64, member: &str) -> Key {
    Key::new()
        .with("ranking_id", ranking)
        .with("member_id", member)
}

/// The row of `members` that [`join`] inserts, as a read with [`nicknamed_schema`] gives it.
fn member(ranking: i64, member: &str, nickname: Option<&str>) -> Row {
    Row::new()
        .with("ranking_id", ranking)
        .with("member_id", member)
        .with("display_name", member.to_uppercase())
        .with("nickname", nickname.map_or(Value::Null, Value::from))
}

/// The members of a ranked room, in the order of their keys: bob in the rankings 1 to 5, and ana
/// in 1 and 2; only bob in 5, written after `nickname` was added, has one.
fn members() -> Vec<Row> {
    let (ana, bob) = (|r| member(r, "ana", None), |r| member(r, "bob", None));
    let mut members = vec![ana(1), bob(1), ana(2), bob(2), bob(3), bob(4)];
    members.push(member(5, "bob", Some("B")));
    members
}

/// A server with `room-a` of [`rankings_schema`], and in `dir` the replica of ana, up to date with
/// it: bob joins the rankings 1 to 4 and ana 1 and 2, `members` gets `nickname`, bob joins 5 with
/// one, and opens the round 1 with a ciphertext in `notes`. The client that kept it is shut down.
async fn ranked() -> (TestServer, RoomId, TempDir) {
    let server = TestServer::new();
    let room_id = server
        .create_room_with("room-a", "rankings", rankings_schema())
        .await;
    let joins = (1..=4).map(|ranking| join(&rankings_schema(), ranking, "bob", None));
    server.commit_ops(&room_id, joins).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = open_client(dir.path(), engine(dir.path()), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    for ranking in 1..=2 {
        let joined = join(&rankings_schema(), ranking, "ana", None);
        client.write(room_id.clone(), joined).await.unwrap();
    }
    let registry = &server.state.schema_registry;
    registry
        .add_column(&rankings_id(), "members", nickname())
        .unwrap();
    let rooms = &server.state.room_manager;
    rooms.reload_schema_for_rooms(&rankings_id()).await;
    let joined = join(&nicknamed_schema(), 5, "bob", Some("B"));
    let round = RowBuilder::new()
        .set("round_id", 1)
        .set("title", "Kickoff")
        .set("notes", vec![0xc1_u8, 0x9a, 0x07])
        .build();
    let round = rankings_schema().to_operation_insert("rounds", &round, 1000);
    server.commit_ops(&room_id, [joined, round.unwrap()]).await;
    client.sync(room_id.clone()).await.unwrap();
    client.shutdown().await.unwrap();
    drop(client);
    (server, room_id, dir)
}

/// A facade of ana over `storage`, with its metadata in `dir`.
async fn facade(
    dir: &Path,
    storage: impl StorageEngine + 'static,
    transport: &TestTransport,
    crypto: Option<Arc<dyn CryptoEngine>>,
) -> ZemdbClient {
    let metadata = FileMetadata::new(dir);
    let config = ClientConfig::new("ana");
    let tokens = TestTokens::new();
    let client = ZemdbClient::open(config, storage, metadata, transport.clone(), tokens, crypto);
    client.await.unwrap()
}

/// Opens `room_id` through `client`, with a token of the client.
async fn open(client: &ZemdbClient, room_id: &RoomId) -> Result<RoomHandle, ClientError> {
    let token = token(&client.client_id(), room_id);
    client.room(room_id.clone(), token).await
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

#[tokio::test]
async fn the_rows_of_a_room_up_to_date_are_read_by_the_names_of_their_columns() {
    let (server, room_id, dir) = ranked().await;
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, None).await;
    let room = open(&client, &room_id).await.unwrap();
    assert_eq!(room.status().await, Ok(status(8, 0, true)));
    let members_table = room.table("members").unwrap();
    let sent = transport.sent().len();

    // A row of another member, with the owner column in its key; a short row, written before
    // `nickname` was added, has it in NULL.
    let bob = members_table.get(&key(1, "bob")).await;
    assert_eq!(bob, Ok(Some(member(1, "bob", None))));
    let nicknamed = members_table.get(&key(5, "bob")).await;
    assert_eq!(nicknamed, Ok(Some(member(5, "bob", Some("B")))));
    assert_eq!(members_table.get(&key(9, "bob")).await, Ok(None));

    let all = members();
    assert_eq!(members_table.scan(&Scan::new()).await, Ok(all.clone()));
    // A range with both bounds in it, and one that starts after its first key.
    let range = Scan::new().range(key(2, "ana")..=key(4, "bob"));
    assert_eq!(members_table.scan(&range).await, Ok(all[2..6].to_vec()));
    let after = Scan::new().range((Bound::Excluded(key(2, "ana")), Bound::Unbounded));
    assert_eq!(members_table.scan(&after).await, Ok(all[3..].to_vec()));
    let before = Scan::new().range(..key(2, "bob"));
    assert_eq!(members_table.scan(&before).await, Ok(all[..3].to_vec()));
    // Backward, with a limit.
    let last = range.clone().backward().limit(2);
    let last_two = vec![all[5].clone(), all[4].clone()];
    assert_eq!(members_table.scan(&last).await, Ok(last_two));
    assert_eq!(members_table.scan(&Scan::new().limit(0)).await, Ok(vec![]));
    // With the columns it names, also one a short row lacks.
    let named = Scan::new()
        .columns(["nickname", "member_id"])
        .limit(3)
        .backward();
    let named_row = |nickname: Value| {
        Row::new()
            .with("member_id", "bob")
            .with("nickname", nickname)
    };
    let named_rows = vec![
        named_row("B".into()),
        named_row(Value::Null),
        named_row(Value::Null),
    ];
    assert_eq!(members_table.scan(&named).await, Ok(named_rows));
    // A range whose start is after its end holds no row.
    let inverted = Scan::new().range(key(4, "bob")..=key(2, "ana"));
    assert_eq!(members_table.scan(&inverted).await, Ok(vec![]));

    // Another handle of the room, which is open, reads the same.
    let again = open(&client, &room_id).await.unwrap();
    let again = again.table("members").unwrap().scan(&Scan::new()).await;
    assert_eq!(again, Ok(all));
    // The reads sent nothing.
    assert_eq!(transport.sent().len(), sent);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn without_network_the_reads_give_what_the_replica_holds_and_send_nothing() {
    let (server, room_id, dir) = ranked().await;
    let transport = server.transport();
    transport.offline();
    let client = facade(dir.path(), engine(dir.path()), &transport, None).await;
    // A room with metadata opens unregistered, to be read.
    let room = open(&client, &room_id).await.unwrap();
    assert_eq!(room.status().await, Ok(status(8, 0, false)));
    let sent = transport.sent().len();
    let members_table = room.table("members").unwrap();
    let bob = members_table.get(&key(5, "bob")).await;
    assert_eq!(bob, Ok(Some(member(5, "bob", Some("B")))));
    let scan = Scan::new().range(key(2, "ana")..).limit(2);
    assert_eq!(
        members_table.scan(&scan).await,
        Ok(members()[2..4].to_vec())
    );
    assert_eq!(transport.sent().len(), sent);
}

#[tokio::test]
async fn a_room_that_fails_closed_as_it_opens_has_its_handle_to_be_read() {
    let (server, room_id, dir) = ranked().await;
    let transport = server.transport();
    let incarnation = server.incarnation(&room_id).await;
    transport.answer_next_registration(move |correlation_id, room_id| {
        let registered = ServerMessage::Registered {
            correlation_id,
            room_id: room_id.clone(),
            head_seq: SequenceNumber::new(8),
            tail_seq: SequenceNumber::new(0),
            schema_id: SchemaId::new("retyped").unwrap(),
            schema: retyped_schema(),
            active_snapshot_seq: None,
            room_incarnation: incarnation,
        };
        encode_message(&registered).unwrap()
    });
    let client = facade(dir.path(), engine(dir.path()), &transport, None).await;
    let room = open(&client, &room_id).await.unwrap();
    let failed = RoomStatus {
        failed_closed: true,
        ..status(8, 0, false)
    };
    assert_eq!(room.status().await, Ok(failed.clone()));
    // It reads the replica, with the schema of the replica.
    let members_table = room.table("members").unwrap();
    let bob = members_table.get(&key(5, "bob")).await;
    assert_eq!(bob, Ok(Some(member(5, "bob", Some("B")))));
    assert_eq!(members_table.scan(&Scan::new()).await, Ok(members()));
    // Another handle of it, too, without sending anything.
    let sent = transport.sent().len();
    let again = open(&client, &room_id).await.unwrap();
    let again = again.table("members").unwrap().get(&key(1, "ana")).await;
    assert_eq!(again, Ok(Some(member(1, "ana", None))));
    assert_eq!(transport.sent().len(), sent);
    assert_eq!(room.status().await, Ok(failed));
}

#[tokio::test]
async fn a_room_whose_replica_is_not_open_is_room_closed() {
    let (server, room_id, dir) = ranked().await;
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = facade(dir.path(), storage.clone(), &transport, None).await;
    let room = open(&client, &room_id).await.unwrap();
    let members_table = room.table("members").unwrap();
    let closed = || Some(ClientError::RoomClosed(room_id.clone()));
    let reads_closed = || async {
        assert_eq!(members_table.get(&key(1, "bob")).await.err(), closed());
        assert_eq!(members_table.scan(&Scan::new()).await.err(), closed());
        assert_eq!(room.table("members").err(), closed());
        assert_eq!(room.status().await.err(), closed());
    };

    // Closed.
    room.close().await.unwrap();
    reads_closed().await;

    // Whose open failed.
    let failed = StorageError::Io(std::io::Error::other("The disk failed"));
    storage.fail_next("open_room", failed);
    let opened = open(&client, &room_id).await;
    assert!(matches!(opened, Err(ClientError::Storage(_))), "{opened:?}");
    reads_closed().await;

    // An error of the open other than failing closed has no handle, also with the replica open:
    // its registration cannot read the head of the storage room.
    open(&client, &room_id).await.unwrap();
    room.close().await.unwrap();
    let failed = StorageError::Io(std::io::Error::other("The disk failed"));
    storage.fail_next("get_head_seq", failed);
    let opened = open(&client, &room_id).await;
    assert!(matches!(opened, Err(ClientError::Storage(_))), "{opened:?}");
    let bob = members_table.get(&key(1, "bob")).await;
    assert_eq!(bob, Ok(Some(member(1, "bob", None))));
    room.close().await.unwrap();

    // Open, with its storage room closed while a read goes on, as by the reopening after an
    // error: the read is `RoomClosed`; another error of the storage, `Storage`.
    open(&client, &room_id).await.unwrap();
    let not_found = || StorageError::RoomNotFound(room_id.clone());
    storage.fail_next("get_by_id", not_found());
    assert_eq!(members_table.get(&key(1, "bob")).await.err(), closed());
    storage.fail_next("scan_by_id", not_found());
    assert_eq!(members_table.scan(&Scan::new()).await.err(), closed());
    let io = || StorageError::Io(std::io::Error::other("The disk failed"));
    storage.fail_next("get_by_id", io());
    let failed_read = members_table.get(&key(1, "bob")).await;
    assert!(
        matches!(failed_read, Err(ClientError::Storage(_))),
        "{failed_read:?}"
    );
    storage.fail_next("scan_by_id", io());
    let failed_scan = members_table.scan(&Scan::new()).await;
    assert!(
        matches!(failed_scan, Err(ClientError::Storage(_))),
        "{failed_scan:?}"
    );
    assert_eq!(
        members_table.get(&key(1, "bob")).await,
        Ok(Some(member(1, "bob", None)))
    );

    // Empty after a discard for `RoomReset`, until `Registered`: the room was created again, and
    // its registration without a cursor gets no reply.
    room.close().await.unwrap();
    server.recreate_room(&room_id).await;
    let mut registrations = 0;
    transport.answer(move |message| {
        let ClientMessage::RegisterClient { .. } = message else {
            return None;
        };
        registrations += 1;
        (registrations > 1).then(|| Err(TransportError("Offline".to_string())))
    });
    let opened = open(&client, &room_id).await;
    assert!(matches!(opened, Err(ClientError::Network(_))), "{opened:?}");
    assert_eq!(transport.registrations().last(), Some(&(None, None)));
    reads_closed().await;
    // Once registered, it reads the room created again, empty, with its schema.
    transport.online();
    open(&client, &room_id).await.unwrap();
    assert_eq!(room.status().await, Ok(status(0, 1, true)));
    let tasks_table = room.table("tasks").unwrap();
    assert_eq!(tasks_table.scan(&Scan::new()).await, Ok(vec![]));
    let gone = members_table.get(&key(1, "bob")).await;
    is_invalid(gone);

    // After a shutdown, also to open it.
    client.shutdown().await.unwrap();
    assert_eq!(tasks_table.scan(&Scan::new()).await.err(), closed());
    assert_eq!(room.status().await.err(), closed());
    assert_eq!(open(&client, &room_id).await.err(), closed());
}

#[tokio::test]
async fn a_read_does_not_wait_for_a_change_of_the_replica_in_progress() {
    let (server, room_id, dir) = ranked().await;
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = facade(dir.path(), storage.clone(), &transport, None).await;
    let room = open(&client, &room_id).await.unwrap();
    let members_table = room.table("members").unwrap();
    room.close().await.unwrap();
    // The server adds a column, which the replica takes as it opens again: its storage room
    // reloads the schema under the applier, and the reload is held.
    let color = ColumnDef::new("color", DataType::String).nullable(true);
    let registry = &server.state.schema_registry;
    registry
        .add_column(&rankings_id(), "members", color)
        .unwrap();
    let rooms = &server.state.room_manager;
    rooms.reload_schema_for_rooms(&rankings_id()).await;
    let held = storage.hold_next("reload_schema");
    let read = async {
        let mut held = held;
        held.replied().await;
        let within = |read| tokio::time::timeout(Duration::from_secs(10), read);
        let bob = within(members_table.get(&key(5, "bob"))).await;
        held.release();
        bob.expect("The read waited for the reload")
    };
    let (opened, bob) = tokio::join!(open(&client, &room_id), read);
    opened.unwrap();
    // It read with the schema of the replica before the reload.
    assert_eq!(bob, Ok(Some(member(5, "bob", Some("B")))));
    let bob = members_table.get(&key(5, "bob")).await;
    let colored = member(5, "bob", Some("B")).with("color", Value::Null);
    assert_eq!(bob, Ok(Some(colored)));
}

#[tokio::test]
async fn a_discard_while_a_read_is_in_the_storage_room_makes_it_room_closed() {
    let (server, room_id, dir) = ranked().await;
    let transport = server.transport();
    let storage = TestEngine::new(engine(dir.path()));
    let client = facade(dir.path(), storage.clone(), &transport, None).await;
    let room = open(&client, &room_id).await.unwrap();
    let members_table = room.table("members").unwrap();
    let held = storage.hold_next("get_by_id");
    // Once the read took the schema, the room is created again and the replica discarded for
    // it: its storage room holds the new incarnation when the read reaches it.
    let discard = async {
        let mut held = held;
        held.replied().await;
        room.close().await.unwrap();
        server.recreate_room(&room_id).await;
        open(&client, &room_id).await.unwrap();
        held.release();
    };
    let bob = key(1, "bob");
    let (read, ()) = tokio::join!(members_table.get(&bob), discard);
    assert_eq!(read, Err(ClientError::RoomClosed(room_id.clone())));
    assert_eq!(room.status().await, Ok(status(0, 1, true)));
}

#[tokio::test]
async fn a_room_without_metadata_has_no_handle_without_the_server() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    transport.offline();
    let client = facade(dir.path(), engine(dir.path()), &transport, None).await;
    let opened = open(&client, &room_id).await;
    assert!(matches!(opened, Err(ClientError::Network(_))), "{opened:?}");
}

#[tokio::test]
async fn what_is_not_of_the_schema_is_invalid_and_an_encrypted_column_is_not_read() {
    let (server, room_id, dir) = ranked().await;
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, None).await;
    let room = open(&client, &room_id).await.unwrap();
    let sent = transport.sent().len();
    is_invalid(room.table("leagues"));
    let members_table = room.table("members").unwrap();

    // A key with a column less or more, of another table, with a value of another type, or with
    // NULL. No message names a value.
    let secret = "secret-value";
    let keys = [
        Key::new().with("ranking_id", 1),
        key(1, "bob").with("display_name", "BOB"),
        key(1, "bob").with("color", secret),
        Key::new()
            .with("ranking_id", secret)
            .with("member_id", "bob"),
        Key::new()
            .with("ranking_id", 1)
            .with("member_id", Value::Null),
        Key::new().with("ranking_id", 1.5).with("member_id", secret),
    ];
    for key in keys {
        let message = is_invalid(members_table.get(&key).await);
        assert!(!message.contains(secret), "{message}");
        let bounded = Scan::new().range(key.clone()..);
        let message = is_invalid(members_table.scan(&bounded).await);
        assert!(!message.contains(secret), "{message}");
        let bounded = Scan::new().range(..=key);
        is_invalid(members_table.scan(&bounded).await);
    }
    // A column that the table does not have, or twice.
    is_invalid(members_table.scan(&Scan::new().columns(["color"])).await);
    let twice = Scan::new().columns(["member_id", "nickname", "member_id"]);
    is_invalid(members_table.scan(&twice).await);

    // A read that gives the encrypted `notes` is `Crypto`, also with a `CryptoEngine`; one that
    // does not give it reads.
    let rounds = room.table("rounds").unwrap();
    let round = Key::new().with("round_id", 1);
    let message = is_crypto(rounds.get(&round).await);
    assert!(message.contains("'notes'"), "{message}");
    is_crypto(rounds.scan(&Scan::new()).await);
    is_crypto(rounds.scan(&Scan::new().columns(["title", "notes"])).await);
    let clear = Scan::new().columns(["round_id", "title"]);
    let kickoff = Row::new().with("round_id", 1).with("title", "Kickoff");
    assert_eq!(rounds.scan(&clear).await, Ok(vec![kickoff]));
    // What is not of the schema is invalid first, as a write validates by name before it
    // encrypts.
    is_invalid(rounds.get(&Key::new().with("round_id", "1")).await);
    is_invalid(rounds.scan(&Scan::new().columns(["notes", "color"])).await);
    let bounded = Scan::new().range(Key::new().with("round", 1)..);
    is_invalid(rounds.scan(&bounded).await);
    client.shutdown().await.unwrap();
    drop(client);

    let crypto: Arc<dyn CryptoEngine> = Arc::new(NoOpCryptoEngine);
    let client = facade(dir.path(), engine(dir.path()), &transport, Some(crypto)).await;
    let rounds = open(&client, &room_id)
        .await
        .unwrap()
        .table("rounds")
        .unwrap();
    is_crypto(rounds.get(&round).await);
    is_crypto(rounds.scan(&Scan::new().columns(["notes"])).await);
    // Nothing was sent but the registration of the second client.
    let sent_now = transport.sent();
    assert_eq!(sent_now.len(), sent + 1);
    let registration = sent_now.last().unwrap();
    assert!(matches!(registration, ClientMessage::RegisterClient { .. }));
}

/// `value` after a trip through JSON.
fn json_trip<T: serde::Serialize + serde::de::DeserializeOwned>(value: &T) -> T {
    serde_json::from_str(&serde_json::to_string(value).unwrap()).unwrap()
}

#[test]
fn the_data_of_the_facade_go_through_serde_and_back() {
    let row = Row::new()
        .with("null", Value::Null)
        .with("int", -7)
        .with("float", 2.5)
        .with("timestamp", Value::Timestamp(1_700_000_000_000))
        .with("string", "text")
        .with("bool", true)
        .with("bytes", vec![0u8, 1, 255])
        .with("uuid", [9u8; 16]);
    assert_eq!(json_trip(&row), row);
    let key = key(1, "bob");
    assert_eq!(json_trip(&key), key);
    let scans = [
        Scan::new(),
        Scan::new()
            .range(key.clone()..=key.clone())
            .backward()
            .limit(3),
        Scan::new()
            .range((Bound::Excluded(key.clone()), Bound::Excluded(key)))
            .columns(["member_id", "nickname"]),
    ];
    for scan in scans {
        assert_eq!(json_trip(&scan), scan);
    }
    assert_eq!(json_trip(&Direction::Backward), Direction::Backward);
    assert_eq!(json_trip(&status(3, 1, true)), status(3, 1, true));

    let room_id = RoomId::new("room-a").unwrap();
    let constraint = || "members_name".to_string();
    let errors = [
        ClientError::Config("config".into()),
        ClientError::Storage("storage".into()),
        ClientError::RoomClosed(room_id.clone()),
        ClientError::Network("network".into()),
        ClientError::FailedClosed(room_id),
        ClientError::ConstraintViolation(Violation::Unique {
            constraint: constraint(),
        }),
        ClientError::ConstraintViolation(Violation::ForeignKey {
            constraint: constraint(),
        }),
        ClientError::ConstraintViolation(Violation::Restrict {
            constraint: constraint(),
        }),
        ClientError::ConstraintViolation(Violation::Duplicate),
        ClientError::ConstraintViolation(Violation::NotOwner),
        ClientError::RowMissing,
        ClientError::OutcomeUnknown,
        ClientError::Validation("validation".into()),
        ClientError::InvalidOperation("invalid".into()),
        ClientError::Crypto("crypto".into()),
    ];
    for error in errors {
        assert_eq!(json_trip(&error), error);
    }
}
