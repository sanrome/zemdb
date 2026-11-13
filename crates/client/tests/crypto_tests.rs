//! The encrypted columns of the facade against the server in process: a value of an encrypted
//! column goes out, and is stored, only encrypted, with the additional data of its cell, and is
//! read in the clear; what does not decrypt, or does not decrypt to a value of the type of its
//! column, is `Decryption`.

mod common;

use bincode::Options;
use common::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use zemdb_client::{
    Client, ClientConfig, ClientError, FileMetadata, Key, MemoryMetadata, Rejection, RoomHandle,
    Row, Scan, Violation, ZemdbClient,
};
use zemdb_core::{
    ClientMessage, ClientMessageKind, ColumnDef, CryptoEngine, DataType, Operation, OperationKind,
    OwnerIdentity, PrimaryKey, RoomId, RowBuilder, Schema, SchemaId, TableSchema, Value,
};
use zemdb_storage::snapshot::decode_snapshot_envelope;
use zemdb_storage::{MemoryStorageEngine, ScanOptions, StorageEngine};

/// The schema `league` of the tests, after the reference app:
/// - `members` (table 0), keyed by an int `ranking_id` and its owner `member_id`, with a string
///   `display_name` and the nullable encrypted string `notes`;
/// - `rounds` (table 1), without an owner, keyed by the ints `ranking_id` and `round_id`, with a
///   string `title`, unique in its ranking, the nullable encrypted string `notes`, the nullable
///   encrypted int `rating` and the nullable encrypted string `summary`;
/// - `marks` (table 2), keyed by a float `mark`, with the encrypted string `note`, which does not
///   take `NULL`;
/// - `tags` (table 3), keyed by a string `tag`, without encrypted columns.
fn league_schema() -> Schema {
    let members = TableSchema::builder("members")
        .primary_key("ranking_id", DataType::Int)
        .primary_key("member_id", DataType::String)
        .column("display_name", DataType::String)
        .nullable_column("notes", DataType::String)
        .encrypted_column("notes", DataType::String)
        .owner("member_id", OwnerIdentity::ClientId);
    let rounds = TableSchema::builder("rounds")
        .primary_key("ranking_id", DataType::Int)
        .primary_key("round_id", DataType::Int)
        .column("title", DataType::String)
        .nullable_column("notes", DataType::String)
        .encrypted_column("notes", DataType::String)
        .nullable_column("rating", DataType::Int)
        .encrypted_column("rating", DataType::Int)
        .nullable_column("summary", DataType::String)
        .encrypted_column("summary", DataType::String)
        .unique("rounds_title", ["ranking_id", "title"]);
    let marks = TableSchema::builder("marks")
        .primary_key("mark", DataType::Float)
        .encrypted_column("note", DataType::String);
    let tags = TableSchema::builder("tags").primary_key("tag", DataType::String);
    Schema::builder()
        .table(members)
        .table(rounds)
        .table(marks)
        .table(tags)
        .build()
}

/// The positions of the columns of `rounds` in [`league_schema`].
const ROUNDS: u16 = 1;
const NOTES: u16 = 3;
const RATING: u16 = 4;

/// A server with `room_id` of [`league_schema`].
async fn league(room_id: &str) -> (TestServer, RoomId) {
    let server = TestServer::new();
    let room_id = server
        .create_room_with(room_id, "league", league_schema())
        .await;
    (server, room_id)
}

/// A facade of `person` over `storage`, with its metadata in `dir` and `crypto`, with the precheck.
async fn facade(
    dir: &Path,
    storage: impl StorageEngine + 'static,
    transport: &TestTransport,
    person: &str,
    crypto: Option<Arc<TestCrypto>>,
) -> ZemdbClient {
    let metadata = FileMetadata::new(dir);
    let config = ClientConfig::new(person);
    let tokens = TestTokens::new();
    let crypto = crypto.map(|crypto| crypto as Arc<dyn CryptoEngine>);
    let client = ZemdbClient::open(config, storage, metadata, transport.clone(), tokens, crypto);
    client.await.unwrap()
}

/// Opens `room_id` through `client`, with a token of the client.
async fn open(client: &ZemdbClient, room_id: &RoomId) -> RoomHandle {
    let token = token(&client.client_id(), room_id);
    client.room(room_id.clone(), token).await.unwrap()
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

/// Catches the replica of `room` up with the log, with the catch-up of the insert of `tag`.
async fn catch_up(room: &RoomHandle, tag: &str) {
    let tag = Row::new().with("tag", tag);
    room.table("tags").unwrap().insert(&tag).await.unwrap();
}

async fn head(server: &TestServer, room_id: &RoomId) -> u64 {
    let bounds = server.state.room_manager.log_bounds(room_id).await;
    bounds.unwrap().head_seq.get()
}

fn round_key(round: i64) -> Key {
    Key::new().with("ranking_id", 1).with("round_id", round)
}

fn round(round: i64, title: &str) -> Row {
    Row::new()
        .with("ranking_id", 1)
        .with("round_id", round)
        .with("title", title)
}

fn notes(notes: impl Into<Value>) -> Row {
    Row::new().with("notes", notes)
}

/// Whether `bytes` holds `needle`.
fn holds(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|window| window == needle)
}

/// The values of the encrypted columns that `op` carries, for [`league_schema`].
fn encrypted_values(op: &Operation) -> Vec<Value> {
    let schema = league_schema();
    let table = schema.get_table_by_id(op.table_id()).unwrap();
    let encrypted = |index: usize| table.columns()[index].is_encrypted();
    match op.kind() {
        OperationKind::Insert { row } => (row.values().iter().enumerate())
            .filter(|(index, _)| encrypted(*index))
            .map(|(_, value)| value.clone())
            .collect(),
        OperationKind::Update { updates } => (updates.iter())
            .filter(|update| encrypted(usize::from(update.column_idx())))
            .map(|update| update.value().clone())
            .collect(),
        OperationKind::Delete { .. } => Vec::new(),
    }
}

/// Checks that each value in `values` is a ciphertext that does not hold `secret`.
fn sealed(values: &[Value], secret: &[u8]) {
    for value in values {
        let Value::Bytes(bytes) = value else {
            panic!("Not a ciphertext: {value:?}");
        };
        assert!(!holds(bytes, secret), "{value:?}");
    }
}

/// The additional data of a cell, in the encoding that the facade fixes, as a member with the key
/// builds them: their version, and then the room id, the table id, the values of the primary key
/// and the position of the column, in the encoding of the protocol.
fn additional_data(room_id: &RoomId, table_id: u16, pk: &[Value], column_idx: u16) -> Vec<u8> {
    let mut data = vec![1];
    let fields = (room_id.as_str(), table_id, pk, column_idx);
    let encoding = bincode::DefaultOptions::new();
    encoding.serialize_into(&mut data, &fields).unwrap();
    data
}

fn encoded(value: &Value) -> Vec<u8> {
    bincode::DefaultOptions::new().serialize(value).unwrap()
}

/// An update of `column` of the round `round` of [`league_schema`] to `value`, as a member writes
/// it with the low-level API.
fn written(round: i64, column: &str, value: impl Into<Value>) -> Operation {
    let pk = PrimaryKey::composite([Value::from(1), Value::from(round)]);
    let fields = BTreeMap::from([(column.to_string(), value.into())]);
    let update = league_schema().to_operation_update("rounds", pk, &fields, 1000);
    update.unwrap()
}

fn is_crypto<T: std::fmt::Debug>(result: Result<T, ClientError>) -> String {
    match result {
        Err(ClientError::Crypto(message)) => message,
        other => panic!("Not Crypto: {other:?}"),
    }
}

fn is_decryption<T: std::fmt::Debug>(result: Result<T, ClientError>) -> String {
    match result {
        Err(ClientError::Decryption(message)) => message,
        other => panic!("Not Decryption: {other:?}"),
    }
}

fn is_invalid<T: std::fmt::Debug>(result: Result<T, ClientError>) -> String {
    match result {
        Err(ClientError::InvalidOperation(message)) => message,
        other => panic!("Not InvalidOperation: {other:?}"),
    }
}

#[tokio::test]
async fn encrypted_columns_go_out_and_are_stored_encrypted_and_read_in_the_clear() {
    let (server, room_id) = league("room-a").await;
    let dir = tempdir().unwrap();
    let (transport, storage, crypto) = (server.transport(), engine(dir.path()), TestCrypto::new());
    let client = facade(
        dir.path(),
        storage.clone(),
        &transport,
        "ana",
        Some(Arc::clone(&crypto)),
    )
    .await;
    let room = open(&client, &room_id).await;
    let (rounds, members) = (
        room.table("rounds").unwrap(),
        room.table("members").unwrap(),
    );

    // An insert and an update of `notes`; an insert that names none of the encrypted columns,
    // which are `NULL`, and an update to `NULL`; and a member of ana with notes.
    let kickoff = round(1, "Kickoff")
        .with("notes", "kickoff secret")
        .with("rating", 5);
    assert_eq!(rounds.insert(&kickoff).await, Ok(round_key(1)));
    rounds
        .update(&round_key(1), &notes("final secret"))
        .await
        .unwrap();
    rounds.insert(&round(2, "Final")).await.unwrap();
    rounds
        .update(&round_key(2), &notes(Value::Null))
        .await
        .unwrap();
    let member = Row::new()
        .with("ranking_id", 1)
        .with("display_name", "Ana")
        .with("notes", "member secret");
    members.insert(&member).await.unwrap();
    // Each value of an encrypted column, `NULL` too, was encrypted once.
    assert_eq!(crypto.encryptions(), 3 + 1 + 3 + 1 + 1);

    // Every commit carries a ciphertext in each encrypted column it writes, `NULL` too, and the
    // other columns in the clear, the owner among them; the server took them.
    let ops = committed(&transport, 0);
    assert_eq!(ops.len(), 5);
    for op in &ops {
        sealed(&encrypted_values(op), b"secret");
    }
    let OperationKind::Insert { row } = ops[0].kind() else {
        panic!("{:?}", ops[0]);
    };
    assert_eq!(row.get(2), Some(&Value::from("Kickoff")));
    let OperationKind::Insert { row } = ops[4].kind() else {
        panic!("{:?}", ops[4]);
    };
    assert_eq!(row.get(1), Some(&Value::from("ana")));
    assert_eq!(head(&server, &room_id).await, 5);

    // The storage holds the ciphertexts.
    for table_id in [0, ROUNDS] {
        let mut scan = storage
            .scan_by_id(&room_id, table_id, ScanOptions::default())
            .await
            .unwrap();
        let table = league_schema().get_table_by_id(table_id).unwrap().clone();
        while let Some(item) = futures::StreamExt::next(&mut scan).await {
            let (_, row) = item.unwrap();
            let values: Vec<_> = (row.values().iter().enumerate())
                .filter(|(index, _)| table.columns()[*index].is_encrypted())
                .map(|(_, value)| value.clone())
                .collect();
            assert_eq!(values.len(), [1, 3][usize::from(table_id)]);
            sealed(&values, b"secret");
        }
    }

    // The facade reads them in the clear, with their types.
    let final_round = round(1, "Kickoff")
        .with("notes", "final secret")
        .with("rating", 5)
        .with("summary", Value::Null);
    let second = round(2, "Final")
        .with("notes", Value::Null)
        .with("rating", Value::Null)
        .with("summary", Value::Null);
    assert_eq!(
        rounds.get(&round_key(1)).await,
        Ok(Some(final_round.clone()))
    );
    let all = Ok(vec![final_round, second]);
    assert_eq!(rounds.scan(&Scan::new()).await, all);
    let ratings = Scan::new().columns(["rating", "round_id"]);
    let rated = Row::new().with("rating", 5).with("round_id", 1);
    let unrated = Row::new().with("rating", Value::Null).with("round_id", 2);
    assert_eq!(rounds.scan(&ratings).await, Ok(vec![rated, unrated]));
    let ana = Key::new().with("ranking_id", 1).with("member_id", "ana");
    let member = member.with("member_id", "ana");
    assert_eq!(members.get(&ana).await, Ok(Some(member.clone())));

    // Another member, with the same key, reads what ana wrote, once its replica catches up.
    let bob_dir = tempdir().unwrap();
    let bob_transport = server.transport();
    let bob = facade(
        bob_dir.path(),
        MemoryStorageEngine::new(),
        &bob_transport,
        "bob",
        Some(TestCrypto::new()),
    );
    let bob = bob.await;
    let bob_room = open(&bob, &room_id).await;
    catch_up(&bob_room, "bob").await;
    let bob_members = bob_room.table("members").unwrap();
    assert_eq!(bob_members.get(&ana).await, Ok(Some(member)));
}

#[tokio::test(start_paused = true)]
async fn the_snapshot_that_the_designated_client_uploads_holds_the_ciphertexts() {
    let server = TestServer::pruning();
    let room_id = server
        .create_room_with("room-a", "league", league_schema())
        .await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(
        dir.path(),
        engine(dir.path()),
        &transport,
        "ana",
        Some(TestCrypto::new()),
    );
    let client = client.await;
    let rounds = open(&client, &room_id).await.table("rounds").unwrap();
    for id in 1..=9 {
        let row = round(id, &format!("Round {id}")).with("notes", format!("secret {id}"));
        rounds.insert(&row).await.unwrap();
    }
    server.prune(&room_id, 6).await;
    client.shutdown().await.unwrap();
    drop(client);

    // Ana's replica, through the core over the same files, is designated once a client registers
    // behind the log, and its heartbeat uploads the snapshot of its storage.
    let transport = server.transport();
    let ana = Client::open(
        ClientConfig::new("ana"),
        engine(dir.path()),
        FileMetadata::new(dir.path()),
        transport.clone(),
        TestTokens::new(),
    );
    let ana = ana.await.unwrap();
    open_room(&ana, &room_id).await.unwrap();
    let lagging = Client::open(
        ClientConfig::new("bob"),
        MemoryStorageEngine::new(),
        MemoryMetadata::default(),
        server.transport(),
        TestTokens::new(),
    );
    let lagging = lagging.await.unwrap();
    open_room(&lagging, &room_id).await.unwrap();
    ana.heartbeat(room_id.clone()).await.unwrap();

    let mut chunks: Vec<_> = (transport.sent().into_iter())
        .filter_map(|message| match message {
            ClientMessage::UploadSnapshotChunk {
                chunk_index, data, ..
            } => Some((chunk_index, data)),
            _ => None,
        })
        .collect();
    assert!(!chunks.is_empty());
    chunks.sort_by_key(|(index, _)| *index);
    let snapshot: Vec<u8> = chunks.into_iter().flat_map(|(_, data)| data).collect();
    // Decoded, without its compression: the titles are there in the clear, and no note is.
    let decoded = decode_snapshot_envelope(&snapshot).unwrap();
    assert!(holds(&decoded, b"Round 3"));
    assert!(!holds(&decoded, b"secret"));
}

#[tokio::test]
async fn a_ciphertext_copied_to_another_row_column_or_room_does_not_decrypt() {
    let (server, room_id) = league("room-a").await;
    let other_room = server
        .create_room_with("room-b", "league", league_schema())
        .await;
    let dir = tempdir().unwrap();
    let (transport, crypto) = (server.transport(), TestCrypto::new());
    let client = facade(
        dir.path(),
        engine(dir.path()),
        &transport,
        "ana",
        Some(crypto),
    )
    .await;
    let room = open(&client, &room_id).await;
    let rounds = room.table("rounds").unwrap();
    for (id, title) in [(1, "One"), (2, "Two"), (3, "Three")] {
        let row = round(id, title).with("notes", format!("secret {title}"));
        rounds.insert(&row).await.unwrap();
    }
    let other = open(&client, &other_room).await;
    let other_rounds = other.table("rounds").unwrap();
    let row = round(1, "One").with("notes", "secret b");
    other_rounds.insert(&row).await.unwrap();

    // A member copies the ciphertext of the notes of the round 1 to the notes of the round 2,
    // to the summary of the round 1, and to the notes of the round 1 of the other room, with the
    // same `CryptoEngine`, which uses the same key in every room.
    let first = committed(&transport, 0).remove(0);
    let ciphertext = encrypted_values(&first).remove(0);
    let copies = [
        written(2, "notes", ciphertext.clone()),
        written(1, "summary", ciphertext.clone()),
    ];
    server.commit_ops(&room_id, copies).await;
    let copy = written(1, "notes", ciphertext);
    server.commit_ops(&other_room, [copy]).await;
    catch_up(&room, "a").await;
    catch_up(&other, "b").await;

    let message = is_decryption(rounds.get(&round_key(2)).await);
    assert!(
        message.starts_with("The column 'notes' of 'rounds' does not decrypt"),
        "{message}"
    );
    assert!(!message.contains("secret"), "{message}");
    let message = is_decryption(rounds.get(&round_key(1)).await);
    assert!(
        message.starts_with("The column 'summary' of 'rounds'"),
        "{message}"
    );
    let message = is_decryption(other_rounds.get(&round_key(1)).await);
    assert!(
        message.starts_with("The column 'notes' of 'rounds'"),
        "{message}"
    );
    // A scan that gives one of them fails whole; one that does not name them reads the rest.
    is_decryption(rounds.scan(&Scan::new()).await);
    is_decryption(rounds.scan(&Scan::new().columns(["notes"])).await);
    let clear = Scan::new().columns(["round_id", "title"]);
    let titles: Vec<_> = (rounds.scan(&clear).await.unwrap().into_iter())
        .map(|row| row.get("title").cloned().unwrap())
        .collect();
    assert_eq!(titles, ["One", "Two", "Three"].map(Value::from));
    let third = Scan::new()
        .range(round_key(3)..)
        .columns(["notes", "rating"]);
    let three = Row::new()
        .with("notes", "secret Three")
        .with("rating", Value::Null);
    assert_eq!(rounds.scan(&third).await, Ok(vec![three]));
    // The cell keeps its own ciphertext: the notes of the round 1 decrypt.
    let noted = Scan::new().range(..=round_key(1)).columns(["notes"]);
    let one = Row::new().with("notes", "secret One");
    assert_eq!(rounds.scan(&noted).await, Ok(vec![one]));
}

#[tokio::test]
async fn a_plaintext_that_is_not_one_value_of_the_type_of_its_column_is_decryption() {
    let (server, room_id) = league("room-a").await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let crypto = Some(TestCrypto::new());
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", crypto).await;
    let room = open(&client, &room_id).await;
    let rounds = room.table("rounds").unwrap();
    for id in 1..=6 {
        rounds
            .insert(&round(id, &format!("Round {id}")))
            .await
            .unwrap();
    }

    // A member with the key writes, for each cell, with its additional data: a value of another
    // type, a value with a byte left over, a `String` that declares more bytes than the largest
    // length, nothing, and then a value of its type, and `NULL` in a column that takes it.
    let sealed = |round: i64, column: u16, plaintext: &[u8]| {
        let pk = [Value::from(1), Value::from(round)];
        let data = additional_data(&room_id, ROUNDS, &pk, column);
        Value::from(TestCrypto::seal(&data, plaintext))
    };
    let mut trailing = encoded(&Value::from("forged"));
    trailing.push(0);
    let mut huge = vec![4, 253];
    huge.extend(u64::MAX.to_le_bytes());
    let forged = [
        written(1, "notes", sealed(1, NOTES, &encoded(&Value::Int(7)))),
        written(2, "notes", sealed(2, NOTES, &trailing)),
        written(3, "notes", sealed(3, NOTES, &huge)),
        written(4, "notes", sealed(4, NOTES, &[])),
        written(
            5,
            "notes",
            sealed(5, NOTES, &encoded(&Value::from("forged"))),
        ),
        written(6, "rating", sealed(6, RATING, &encoded(&Value::Null))),
    ];
    server.commit_ops(&room_id, forged).await;
    catch_up(&room, "a").await;

    let message = is_decryption(rounds.get(&round_key(1)).await);
    let other_type =
        "The column 'notes' of 'rounds' does not decrypt to a value of its type, String";
    assert_eq!(message, other_type);
    for round in [2, 3, 4] {
        let message = is_decryption(rounds.get(&round_key(round)).await);
        assert_eq!(message, other_type);
    }
    // The additional data that the member built are those of the cell.
    let read = rounds.get(&round_key(5)).await.unwrap().unwrap();
    assert_eq!(read.get("notes"), Some(&Value::from("forged")));
    let read = rounds.get(&round_key(6)).await.unwrap().unwrap();
    assert_eq!(read.get("rating"), Some(&Value::Null));
}

#[tokio::test]
async fn a_key_with_minus_zero_names_the_cell_of_its_row() {
    let (server, room_id) = league("room-a").await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let crypto = Some(TestCrypto::new());
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", crypto).await;
    let marks = open(&client, &room_id).await.table("marks").unwrap();
    let (zero, minus_zero) = (Value::Float(0.0), Value::Float(-0.0));
    let row = Row::new().with("mark", zero.clone()).with("note", "zero");
    marks.insert(&row).await.unwrap();

    // An update that names the row with `-0.0`, which goes in its key as it is.
    let key = Key::new().with("mark", minus_zero.clone());
    let changes = Row::new().with("note", "changed");
    marks.update(&key, &changes).await.unwrap();
    let update = committed(&transport, 0).remove(1);
    let Some(Value::Float(sent)) = update.pk().get(0) else {
        panic!("{update:?}");
    };
    assert!(sent.is_sign_negative());
    let changed = Row::new()
        .with("mark", zero.clone())
        .with("note", "changed");
    for mark in [zero, minus_zero] {
        let key = Key::new().with("mark", mark);
        let read = marks.get(&key).await.unwrap().unwrap();
        assert_eq!(read.get("note"), changed.get("note"));
    }
    assert_eq!(marks.scan(&Scan::new()).await.unwrap().len(), 1);
}

#[tokio::test]
async fn without_a_crypto_engine_or_with_one_that_fails_an_encrypted_column_is_crypto() {
    let (server, room_id) = league("room-a").await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", None).await;
    let room = open(&client, &room_id).await;
    let rounds = room.table("rounds").unwrap();
    // A round of another client, without notes.
    let one = RowBuilder::new()
        .set("ranking_id", 1)
        .set("round_id", 1)
        .set("title", "One")
        .build();
    let one = league_schema().to_operation_insert("rounds", &one, 1000);
    server.commit_ops(&room_id, [one.unwrap()]).await;
    catch_up(&room, "a").await;
    let sent = transport.sent().len();

    // Every insert into a table with an encrypted column, an update of one, `NULL` too, and a
    // read that gives one.
    let message = is_crypto(
        rounds
            .insert(&round(1, "One").with("notes", "secret"))
            .await,
    );
    let no_engine =
        "The column 'notes' of 'rounds' is encrypted, and the client has no CryptoEngine";
    assert_eq!(message, no_engine);
    is_crypto(rounds.insert(&round(1, "One")).await);
    let message = is_crypto(rounds.update(&round_key(1), &notes(Value::Null)).await);
    assert_eq!(message, no_engine);
    let rated = Row::new().with("rating", 4);
    is_crypto(rounds.update(&round_key(1), &rated).await);
    is_crypto(rounds.get(&round_key(1)).await);
    // Also one of a row that is not there: it is planned against the schema.
    is_crypto(rounds.get(&round_key(9)).await);
    let rating = Scan::new().columns(["title", "rating"]);
    let message = is_crypto(rounds.scan(&rating).await);
    assert!(
        message.starts_with("The column 'rating' of 'rounds'"),
        "{message}"
    );
    // What is not valid by name is invalid first, without the value in the message.
    let typed = is_invalid(rounds.insert(&round(1, "One").with("notes", 4242)).await);
    assert!(!typed.contains("4242"), "{typed}");
    let bytes = round(1, "One").with("notes", b"secret".to_vec());
    let message = is_invalid(rounds.insert(&bytes).await);
    assert!(!message.contains("secret"), "{message}");
    assert_eq!(committed(&transport, sent), []);

    // A write and a read that do not touch one go.
    let retitled = Row::new().with("title", "Final");
    rounds.update(&round_key(1), &retitled).await.unwrap();
    catch_up(&room, "b").await;
    let clear = Scan::new().columns(["round_id", "title"]);
    let final_round = Row::new().with("round_id", 1).with("title", "Final");
    assert_eq!(rounds.scan(&clear).await, Ok(vec![final_round]));
    assert_eq!(committed(&transport, sent).len(), 2);
    client.shutdown().await.unwrap();
    drop(client);

    // An engine that fails to encrypt, with the plaintext in its message: `Crypto`, without the
    // message of the engine, and nothing sent.
    let crypto = TestCrypto::new();
    let client = facade(
        dir.path(),
        engine(dir.path()),
        &transport,
        "ana",
        Some(Arc::clone(&crypto)),
    );
    let client = client.await;
    let rounds = open(&client, &room_id).await.table("rounds").unwrap();
    let sent = transport.sent().len();
    crypto.fail_encrypt(true);
    let message = is_crypto(
        rounds
            .insert(&round(1, "One").with("notes", "secret"))
            .await,
    );
    let failed = "The column 'notes' of 'rounds' was not encrypted: the CryptoEngine failed to \
                  encrypt";
    assert_eq!(message, failed);
    is_crypto(rounds.update(&round_key(1), &notes("secret")).await);
    assert_eq!(committed(&transport, sent), []);
    crypto.fail_encrypt(false);
    rounds
        .insert(&round(1, "One").with("notes", "secret"))
        .await
        .unwrap();
    let read = rounds.get(&round_key(1)).await.unwrap().unwrap();
    assert_eq!(read.get("notes"), Some(&Value::from("secret")));
}

#[tokio::test]
async fn an_encrypted_column_added_later_reads_null_in_the_rows_written_before_it() {
    let diary = || {
        let entries = TableSchema::builder("entries")
            .primary_key("entry_id", DataType::Int)
            .column("title", DataType::String);
        Schema::builder().table(entries).build()
    };
    let server = TestServer::new();
    let room_id = server.create_room_with("room-a", "diary", diary()).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let crypto = Some(TestCrypto::new());
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", crypto).await;
    let room = open(&client, &room_id).await;
    let entry = |id: i64| Row::new().with("entry_id", id).with("title", "Entry");
    room.table("entries")
        .unwrap()
        .insert(&entry(1))
        .await
        .unwrap();

    // The schema adds the encrypted column `secret`, which the replica takes when it opens again.
    let secret = ColumnDef::new("secret", DataType::String)
        .nullable(true)
        .encrypted(true);
    let diary_id = SchemaId::new("diary").unwrap();
    let registry = &server.state.schema_registry;
    registry.add_column(&diary_id, "entries", secret).unwrap();
    let rooms = &server.state.room_manager;
    rooms.reload_schema_for_rooms(&diary_id).await;
    room.close().await.unwrap();
    let room = open(&client, &room_id).await;
    let entries = room.table("entries").unwrap();

    // The row written before it, shorter, reads it `NULL`; a new one encrypts it, also `NULL`.
    let read = entries.get(&Key::new().with("entry_id", 1)).await.unwrap();
    assert_eq!(read, Some(entry(1).with("secret", Value::Null)));
    let secrets = Scan::new().columns(["secret"]);
    let null = Row::new().with("secret", Value::Null);
    assert_eq!(entries.scan(&secrets).await, Ok(vec![null]));
    let sent = transport.sent().len();
    let changes = Row::new().with("secret", "later secret");
    let key = Key::new().with("entry_id", 1);
    entries.update(&key, &changes).await.unwrap();
    entries.insert(&entry(2)).await.unwrap();
    for op in committed(&transport, sent) {
        let values = match op.kind() {
            OperationKind::Update { updates } => vec![updates[0].value().clone()],
            OperationKind::Insert { row } => vec![row.get(2).cloned().unwrap()],
            OperationKind::Delete { .. } => panic!("{op:?}"),
        };
        sealed(&values, b"secret");
    }
    let all = vec![
        entry(1).with("secret", "later secret"),
        entry(2).with("secret", Value::Null),
    ];
    assert_eq!(entries.scan(&Scan::new()).await, Ok(all));
}

/// Opens the replica of `room` of `client` again without the server, unregistered, while the room
/// is created again on `server` with `schema`.
async fn created_again_while_unregistered(
    server: &TestServer,
    transport: &TestTransport,
    client: &ZemdbClient,
    room: &RoomHandle,
    schema: Schema,
) {
    let room_id = room.room_id();
    room.close().await.unwrap();
    transport.offline();
    open(client, room_id).await;
    let rooms = &server.state.room_manager;
    rooms.delete_room(room_id).await.unwrap();
    server
        .create_room_with(room_id.as_str(), "again", schema)
        .await;
    transport.online();
}

/// What `call`, a call through `room`, gives when, while `held` holds it, the insert of a tag
/// registers the replica, which is discarded for `RoomReset` and opens with the room created again.
/// Checks that all that went out meanwhile through `transport` are the two registrations of the
/// insert of the tag.
async fn discarded_meanwhile<T>(
    transport: &TestTransport,
    room: &RoomHandle,
    mut held: Held,
    call: impl std::future::Future<Output = T>,
) -> T {
    let sent = transport.sent().len();
    let discarding = async {
        held.replied().await;
        let tag = Row::new().with("tag", "red");
        let tagged = room.table("tags").unwrap().insert(&tag).await;
        assert_eq!(tagged, Err(ClientError::RoomClosed(room.room_id().clone())));
        assert_eq!(room.status().await, Ok(status(0, 2, true)));
        held.release();
    };
    let (called, ()) = tokio::join!(call, discarding);
    let kinds: Vec<_> = (transport.sent().iter().skip(sent))
        .map(ClientMessage::kind)
        .collect();
    let register = ClientMessageKind::RegisterClient;
    assert_eq!(kinds, [register, register]);
    called
}

#[tokio::test]
async fn a_write_whose_replica_is_discarded_while_it_encrypts_is_room_closed() {
    let (server, room_id) = league("room-a").await;
    let dir = tempdir().unwrap();
    let (transport, crypto) = (server.transport(), TestCrypto::new());
    let client = facade(
        dir.path(),
        engine(dir.path()),
        &transport,
        "ana",
        Some(Arc::clone(&crypto)),
    );
    let client = client.await;
    let room = open(&client, &room_id).await;
    // The room created again has a table of the id of `rounds` that does not take the insert.
    let scores = TableSchema::builder("scores")
        .primary_key("number", DataType::Int)
        .column("points", DataType::Int);
    let tags = TableSchema::builder("tags").primary_key("tag", DataType::String);
    let again = Schema::builder().table(tags).table(scores).build();
    created_again_while_unregistered(&server, &transport, &client, &room, again).await;

    // The replica is discarded while the insert encrypts.
    let encrypting = crypto.hold_next_encrypt();
    let rounds = room.table("rounds").unwrap();
    let kickoff = round(1, "Kickoff").with("notes", "secret");
    let insert = rounds.insert(&kickoff);
    let inserted = discarded_meanwhile(&transport, &room, encrypting, insert).await;
    assert_eq!(inserted, Err(ClientError::RoomClosed(room_id.clone())));
    assert_eq!(head(&server, &room_id).await, 0);
}

#[tokio::test]
async fn a_read_whose_replica_is_discarded_once_it_read_decrypts_nothing() {
    let (server, room_id) = league("room-a").await;
    let dir = tempdir().unwrap();
    let (transport, crypto) = (server.transport(), TestCrypto::new());
    let storage = TestEngine::new(engine(dir.path()));
    let client = facade(
        dir.path(),
        storage.clone(),
        &transport,
        "ana",
        Some(Arc::clone(&crypto)),
    );
    let client = client.await;
    let room = open(&client, &room_id).await;
    let rounds = room.table("rounds").unwrap();
    let kickoff = round(1, "Kickoff").with("notes", "secret");
    rounds.insert(&kickoff).await.unwrap();
    created_again_while_unregistered(&server, &transport, &client, &room, league_schema()).await;

    // The read takes the row of the round from the storage room, and the replica is discarded
    // before it decrypts: it is `RoomClosed`, and the engine decrypts nothing of the replica of
    // before.
    let reading = storage.hold_next("get_by_id:read");
    let key = round_key(1);
    let read = rounds.get(&key);
    let read = discarded_meanwhile(&transport, &room, reading, read).await;
    assert_eq!(read, Err(ClientError::RoomClosed(room_id.clone())));
    assert_eq!(crypto.decryptions(), 0);
}

#[tokio::test]
async fn an_encrypted_column_that_does_not_take_null_is_checked_by_name() {
    // Once encrypted, the server and the storage see only a ciphertext: the facade checks the type
    // in the clear and the nullability of an encrypted column by name, before it encrypts.
    let (server, room_id) = league("room-a").await;
    let dir = tempdir().unwrap();
    let (transport, crypto) = (server.transport(), TestCrypto::new());
    let client = facade(
        dir.path(),
        engine(dir.path()),
        &transport,
        "ana",
        Some(Arc::clone(&crypto)),
    );
    let marks = open(&client.await, &room_id).await.table("marks").unwrap();
    let sent = transport.sent().len();
    let mark = |note: Option<Value>| {
        let mark = Row::new().with("mark", 1.0);
        match note {
            Some(note) => mark.with("note", note),
            None => mark,
        }
    };
    let null = "The column 'note' of 'marks' does not take NULL";
    let typed = "The column 'note' of 'marks' is of type String, not Int";
    assert_eq!(
        is_invalid(marks.insert(&mark(Some(Value::Null))).await),
        null
    );
    let lacks = "The row of 'marks' lacks the column 'note', which does not take NULL";
    assert_eq!(is_invalid(marks.insert(&mark(None)).await), lacks);
    assert_eq!(
        is_invalid(marks.insert(&mark(Some(Value::from(7)))).await),
        typed
    );
    marks.insert(&mark(Some(Value::from("one")))).await.unwrap();
    let key = Key::new().with("mark", 1.0);
    let note = |note: Value| Row::new().with("note", note);
    assert_eq!(
        is_invalid(marks.update(&key, &note(Value::Null)).await),
        null
    );
    assert_eq!(
        is_invalid(marks.update(&key, &note(Value::from(7))).await),
        typed
    );
    // Only the valid insert was encrypted and sent.
    assert_eq!(committed(&transport, sent).len(), 1);
    assert_eq!(crypto.encryptions(), 1);
    let read = marks.get(&key).await.unwrap().unwrap();
    assert_eq!(read.get("note"), Some(&Value::from("one")));
}

#[tokio::test]
async fn the_precheck_decides_on_the_ciphertext() {
    let (server, room_id) = league("room-a").await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let crypto = Some(TestCrypto::new());
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", crypto).await;
    let rounds = open(&client, &room_id).await.table("rounds").unwrap();
    rounds
        .insert(&round(1, "Kickoff").with("notes", "secret"))
        .await
        .unwrap();
    // A round of the same title, with its notes encrypted: the replica would reject it.
    let sent = transport.sent().len();
    let again = round(2, "Kickoff").with("notes", "other secret");
    let unique = Violation::Unique {
        constraint: "rounds_title".to_string(),
    };
    let rejected = Err(ClientError::PrecheckFailed(Rejection::Constraint(unique)));
    assert_eq!(rounds.insert(&again).await, rejected);
    assert_eq!(committed(&transport, sent), []);
    assert_eq!(head(&server, &room_id).await, 1);
    let renamed = round(2, "Final").with("notes", "other secret");
    rounds.insert(&renamed).await.unwrap();
    assert_eq!(head(&server, &room_id).await, 2);
}

#[tokio::test]
async fn bytes_that_another_client_writes_without_the_facade_do_not_decrypt() {
    // The low-level API writes an encrypted column as it is given: bytes that are no ciphertext.
    let (server, room_id) = league("room-a").await;
    let ops = [league_schema()
        .to_operation_insert(
            "rounds",
            &RowBuilder::new()
                .set("ranking_id", 1)
                .set("round_id", 1)
                .set("title", "One")
                .set("notes", b"not a ciphertext".to_vec())
                .build(),
            1000,
        )
        .unwrap()];
    server.commit_ops(&room_id, ops).await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let crypto = Some(TestCrypto::new());
    let client = facade(dir.path(), engine(dir.path()), &transport, "ana", crypto).await;
    let room = open(&client, &room_id).await;
    catch_up(&room, "a").await;
    let rounds = room.table("rounds").unwrap();
    let message = is_decryption(rounds.get(&round_key(1)).await);
    assert!(
        message.contains("does not decrypt with its additional data"),
        "{message}"
    );
    assert!(!message.contains("not a ciphertext"), "{message}");
}
