//! The dispatcher of the runtime of a browser Worker, in native, over the facade against the
//! server in process: each command gives what its call of the facade gives, in a task of its
//! own, with its reply to its tab; the client opens once, and after its shutdown every command on
//! a room is `RoomClosed`; a tab that left releases its rooms; and every command and reply goes
//! through `bincode` unchanged.
#![allow(clippy::disallowed_methods)]

mod common;

/// The generator of the storage's randomized tests.
#[path = "../../storage/tests/common/rng.rs"]
#[allow(dead_code)]
mod rng;

use async_trait::async_trait;
use common::*;
use rng::Rng;
use std::collections::HashMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::mpsc;
use zemdb_client::dispatch::{
    Answer, Command, DispatchConfig, Dispatcher, Message, Platform, Reply, Request, RoomCall,
    Table, TableCall,
};
use zemdb_client::{
    ClientConfig, ClientError, Direction, FileMetadata, Freshness, Key, Rejection, RoomHandle,
    RoomStatus, Row, Scan, Violation, WorkerStatus, ZemdbClient,
};
use zemdb_core::{
    ClientId, ClientMessage, ClientMessageKind, DataType, ErrorCode, OwnerIdentity, RoomId,
    RowBuilder, Schema, SequenceNumber, TableSchema, Value,
};
use zemdb_storage::{DiskStorageEngine, StorageError};

/// The tab of the commands of a test with one tab.
const TAB: u64 = 7;

// ---------------------------------------------------------------------------------------------
// The page and the platform

/// The platform of the tests: a client of the facade over a disk engine and its metadata in the
/// directory of the configuration, through the transport to the server in process, with a token
/// provider that issues valid tokens, and without a `CryptoEngine`. Its heartbeats get no reply,
/// so that its worker sends nothing else (`TestTransport::hush`).
#[derive(Clone)]
struct TestPlatform {
    transport: TestTransport,
    /// The last client it opened, and its storage engine.
    opened: Arc<Mutex<Option<ZemdbClient>>>,
    engine: Arc<Mutex<Option<TestEngine<DiskStorageEngine>>>>,
    /// Held by the test while an open must wait.
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl TestPlatform {
    fn new(transport: &TestTransport) -> Self {
        Self {
            transport: transport.clone(),
            opened: Arc::default(),
            engine: Arc::default(),
            gate: Arc::default(),
        }
    }

    /// The storage engine of the last client it opened.
    fn engine(&self) -> TestEngine<DiskStorageEngine> {
        self.engine.lock().unwrap().clone().expect("Not opened")
    }

    /// The last client it opened.
    fn opened(&self) -> ZemdbClient {
        self.opened.lock().unwrap().clone().expect("Not opened")
    }
}

#[async_trait]
impl Platform for TestPlatform {
    async fn open(&self, config: DispatchConfig) -> Result<ZemdbClient, ClientError> {
        drop(self.gate.lock().await);
        let dir = Path::new(&config.directory);
        let storage = TestEngine::new(DiskStorageEngine::new(options(dir))?);
        *self.engine.lock().unwrap() = Some(storage.clone());
        let metadata = FileMetadata::new(dir);
        let (transport, tokens) = (self.transport.clone(), TestTokens::new());
        let client =
            ZemdbClient::open(config.client, storage, metadata, transport, tokens, None).await?;
        self.transport.hush(&client.client_id());
        *self.opened.lock().unwrap() = Some(client.clone());
        Ok(client)
    }
}

/// The page of a test: it sends the commands of its tabs to its dispatcher, and takes the replies
/// that the dispatcher publishes.
struct Page {
    dispatcher: Dispatcher,
    replies: mpsc::UnboundedReceiver<Reply>,
    /// The replies that came before the test asked for them, in the order they came.
    early: Vec<Reply>,
    last: u64,
}

impl Page {
    fn new(platform: &TestPlatform) -> Self {
        let (sender, replies) = mpsc::unbounded_channel();
        // The page may be gone once the test ends, with commands still in flight.
        let publish = move |Message::Reply(reply)| drop(sender.send(reply));
        Self {
            dispatcher: Dispatcher::new(platform.clone(), publish),
            replies,
            early: Vec::new(),
            last: 0,
        }
    }

    /// Sends `request` from `tab`, and returns the id of its command.
    fn send(&mut self, tab: u64, request: Request) -> u64 {
        self.last += 1;
        let id = self.last;
        self.dispatcher.dispatch(Command { tab, id, request });
        id
    }

    /// The next reply that comes, or the first that came early.
    async fn next(&mut self) -> Reply {
        if !self.early.is_empty() {
            return self.early.remove(0);
        }
        let reply = tokio::time::timeout(Duration::from_secs(30), self.replies.recv());
        reply.await.expect("No reply came").unwrap()
    }

    /// The reply to the command `id`, keeping the others that come before it.
    async fn reply(&mut self, id: u64) -> Reply {
        if let Some(index) = self.early.iter().position(|reply| reply.id == id) {
            return self.early.remove(index);
        }
        loop {
            let reply = tokio::time::timeout(Duration::from_secs(30), self.replies.recv());
            let reply = reply.await.expect("No reply came").unwrap();
            if reply.id == id {
                return reply;
            }
            self.early.push(reply);
        }
    }

    /// What `request` from `tab` gives, once its reply, to `tab`, comes.
    async fn call(&mut self, tab: u64, request: Request) -> Result<Answer, ClientError> {
        let id = self.send(tab, request);
        let reply = self.reply(id).await;
        assert_eq!(reply.tab, tab, "{reply:?}");
        reply.result
    }

    /// The ids of the replies that came, or come within `wait`, keeping them for later.
    async fn arrived(&mut self, wait: Duration) -> Vec<u64> {
        tokio::time::sleep(wait).await;
        while let Ok(reply) = self.replies.try_recv() {
            self.early.push(reply);
        }
        self.early.iter().map(|reply| reply.id).collect()
    }

    /// Whether no reply came, nor comes within 100 ms.
    async fn quiet(&mut self) -> bool {
        let next = tokio::time::timeout(Duration::from_millis(100), self.replies.recv());
        self.early.is_empty() && next.await.is_err()
    }
}

/// The configuration of a client of ana over `dir`, in `Manual`.
fn config(dir: &Path) -> DispatchConfig {
    let directory = dir.to_str().unwrap();
    DispatchConfig {
        client: manual("ana"),
        ..DispatchConfig::new("ana", "http://localhost:3000", directory)
    }
}

fn open(dir: &Path) -> Request {
    Request::Open(Box::new(config(dir)))
}

/// A page whose dispatcher opened the client of ana over `dir`, through a transport to `server`,
/// with its platform and the transport.
async fn opened(server: &TestServer, dir: &Path) -> (Page, TestPlatform, TestTransport) {
    let transport = server.transport();
    let platform = TestPlatform::new(&transport);
    let mut page = Page::new(&platform);
    let client_id = page.call(TAB, open(dir)).await;
    assert_eq!(
        client_id,
        Ok(Answer::ClientId(platform.opened().client_id()))
    );
    (page, platform, transport)
}

// ---------------------------------------------------------------------------------------------
// The room of the notes

/// Table `notes`, keyed by an int `number` and its owner `author`, with a unique string `text`.
fn notes_schema() -> Schema {
    let notes = TableSchema::builder("notes")
        .primary_key("number", DataType::Int)
        .primary_key("author", DataType::String)
        .column("text", DataType::String)
        .owner("author", OwnerIdentity::ClientId)
        .unique("notes_text", ["text"]);
    Schema::builder().table(notes).build()
}

/// A server with `room_id` of [`notes_schema`], where bob wrote the note 1, `Hello`.
async fn notes_in(server: &TestServer, room_id: &str) -> RoomId {
    let room_id = server
        .create_room_with(room_id, "notes", notes_schema())
        .await;
    let note = RowBuilder::new()
        .set("number", 1)
        .set("author", "bob")
        .set("text", "Hello");
    let insert = notes_schema().to_operation_insert("notes", &note.build(), 1000);
    server.commit_ops(&room_id, [insert.unwrap()]).await;
    room_id
}

async fn notes() -> (TestServer, RoomId) {
    let server = TestServer::new();
    let room_id = notes_in(&server, "room-a").await;
    (server, room_id)
}

fn notes_of(room_id: &RoomId) -> Table {
    Table {
        room_id: room_id.clone(),
        name: "notes".to_string(),
    }
}

fn key(number: i64, author: &str) -> Key {
    Key::new().with("number", number).with("author", author)
}

/// The note `number` of `author` with `text`, as a read gives it.
fn note(number: i64, author: &str, text: &str) -> Row {
    Row::new()
        .with("number", number)
        .with("author", author)
        .with("text", text)
}

/// A note of ana to insert, without its owner, which the client fills.
fn new_note(number: i64, text: &str) -> Row {
    Row::new().with("number", number).with("text", text)
}

fn on_notes(room_id: &RoomId, call: TableCall) -> Request {
    Request::Table(notes_of(room_id), call)
}

fn on_room(room_id: &RoomId, call: RoomCall) -> Request {
    Request::Room(room_id.clone(), call)
}

/// Opens `room_id` from `tab`, and catches its replica up with a sync, as a room in `Manual`
/// does.
async fn open_room(page: &mut Page, tab: u64, room_id: &RoomId) {
    for call in [RoomCall::Open, RoomCall::Sync] {
        assert_eq!(
            page.call(tab, on_room(room_id, call)).await,
            Ok(Answer::Done)
        );
    }
}

/// The head of the log of `room_id` in `server`.
async fn head(server: &TestServer, room_id: &RoomId) -> u64 {
    let bounds = server.state.room_manager.log_bounds(room_id).await;
    bounds.unwrap().head_seq.get()
}

// ---------------------------------------------------------------------------------------------
// The commands, against the facade

/// The facade of the test, with the handles of the rooms it opened, as an application keeps them.
struct Facade {
    client: ZemdbClient,
    handles: HashMap<RoomId, RoomHandle>,
}

impl Facade {
    /// What the facade gives for the call of `request`, with the handle of its room.
    async fn call(&mut self, request: Request) -> Result<Answer, ClientError> {
        let done = |()| Answer::Done;
        match request {
            Request::Room(room_id, RoomCall::Open) => {
                let room = self.client.room(room_id.clone()).await?;
                self.handles.insert(room_id, room);
                Ok(Answer::Done)
            }
            Request::Room(room_id, call) => {
                let room = &self.handles[&room_id];
                match call {
                    RoomCall::Open => unreachable!(),
                    RoomCall::Close => room.close().await.map(done),
                    RoomCall::Discard => room.discard().await.map(done),
                    RoomCall::Status => room.status().await.map(Answer::Status),
                    RoomCall::Sync => room.sync().await.map(done),
                    RoomCall::SetFreshness(freshness) => {
                        room.set_freshness(freshness);
                        Ok(Answer::Done)
                    }
                }
            }
            Request::Table(table, call) => {
                let table = self.handles[&table.room_id].table(&table.name)?;
                match call {
                    TableCall::Get(key) => table.get(&key).await.map(Answer::Row),
                    TableCall::Scan(scan) => table.scan(&scan).await.map(Answer::Rows),
                    TableCall::Insert(row) => table.insert(&row).await.map(Answer::Key),
                    TableCall::Update(key, changes) => table.update(&key, &changes).await.map(done),
                    TableCall::Delete(key) => table.delete(&key).await.map(done),
                }
            }
            Request::Shutdown => self.client.shutdown().await.map(done),
            request => unreachable!("{request:?}"),
        }
    }
}

#[tokio::test]
async fn every_command_gives_what_its_call_of_the_facade_gives() {
    // Two servers alike: one for the dispatcher, and one for the facade.
    let (server, room_id) = notes().await;
    let (twin, _) = notes().await;
    let absent = RoomId::new("room-z").unwrap();
    let dir = tempdir().unwrap();
    let (mut page, platform, dispatched_through) = opened(&server, dir.path()).await;
    let client_id = platform.opened().client_id();
    assert_eq!(
        page.call(TAB, Request::ClientId).await,
        Ok(Answer::ClientId(client_id))
    );

    let twin_dir = tempdir().unwrap();
    let metadata = FileMetadata::new(twin_dir.path());
    let (transport, tokens) = (twin.transport(), TestTokens::new());
    let storage = engine(twin_dir.path());
    let client = ZemdbClient::open(
        manual("ana"),
        storage,
        metadata,
        transport.clone(),
        tokens,
        None,
    );
    let client = client.await.unwrap();
    transport.hush(&client.client_id());
    let mut facade = Facade {
        client,
        handles: HashMap::new(),
    };

    let notes = |call| on_notes(&room_id, call);
    let room = |call| on_room(&room_id, call);
    let changes = |text: &str| Row::new().with("text", text);
    let script = [
        room(RoomCall::Open),
        // In `Manual`, the replica catches up with a sync.
        room(RoomCall::Sync),
        room(RoomCall::Status),
        notes(TableCall::Get(key(1, "bob"))),
        notes(TableCall::Get(key(7, "bob"))),
        notes(TableCall::Scan(Scan::new())),
        notes(TableCall::Insert(new_note(2, "World"))),
        // The precheck, on by default: a text that another note has, and a note that is not there.
        notes(TableCall::Insert(new_note(3, "Hello"))),
        notes(TableCall::Update(key(9, "ana"), changes("Nope"))),
        notes(TableCall::Update(key(2, "ana"), changes("Bye"))),
        notes(TableCall::Scan(
            Scan::new().backward().limit(1).columns(["text"]),
        )),
        notes(TableCall::Delete(key(2, "ana"))),
        notes(TableCall::Scan(Scan::new())),
        // What the schema does not take: a column, an update without changes, and a table.
        notes(TableCall::Insert(new_note(4, "Hi").with("mood", "glad"))),
        notes(TableCall::Update(key(1, "bob"), Row::new())),
        Request::Table(
            Table {
                room_id: room_id.clone(),
                name: "nope".to_string(),
            },
            TableCall::Get(Key::new()),
        ),
        room(RoomCall::Sync),
        room(RoomCall::SetFreshness(Freshness::Manual)),
        room(RoomCall::Status),
        room(RoomCall::Close),
        notes(TableCall::Get(key(1, "bob"))),
        room(RoomCall::Status),
        room(RoomCall::Sync),
        // The mode of a room that is closed is for its next open.
        room(RoomCall::SetFreshness(Freshness::Periodic)),
        room(RoomCall::Open),
        room(RoomCall::Status),
        notes(TableCall::Scan(Scan::new())),
        room(RoomCall::Discard),
        notes(TableCall::Get(key(1, "bob"))),
        // Opened again, as new.
        room(RoomCall::Open),
        room(RoomCall::Sync),
        room(RoomCall::Status),
        notes(TableCall::Scan(Scan::new())),
        // A room that the server does not have.
        on_room(&absent, RoomCall::Open),
        Request::Shutdown,
        room(RoomCall::Open),
        notes(TableCall::Get(key(1, "bob"))),
    ];
    // Each gives what the other gives, and sends what the other sends, in the same order (the
    // heartbeats get no reply, and are not logged).
    let kinds = |transport: &TestTransport| {
        let sent = transport.sent().into_iter();
        sent.map(|message| message.kind()).collect::<Vec<_>>()
    };
    let mut answers = Vec::new();
    for request in script {
        let dispatched = page.call(TAB, request.clone()).await;
        let called = facade.call(request.clone()).await;
        assert_eq!(dispatched, called, "{request:?}");
        let sent = kinds(&dispatched_through);
        assert_eq!(sent, kinds(&transport), "{request:?}");
        answers.push(dispatched);
    }
    assert!(kinds(&transport).contains(&ClientMessageKind::Ack));
    // What the script went through: rows, keys, statuses and errors of the facade.
    let error = |index: usize| answers[index].clone().unwrap_err();
    let status = |index: usize| match &answers[index] {
        Ok(Answer::Status(status)) => status.clone(),
        answer => panic!("{answer:?}"),
    };
    let freshness = |index: usize| status(index).worker.unwrap().freshness;
    assert_eq!(answers[3], Ok(Answer::Row(Some(note(1, "bob", "Hello")))));
    assert_eq!(answers[4], Ok(Answer::Row(None)));
    assert_eq!(answers[6], Ok(Answer::Key(key(2, "ana"))));
    let unique = Violation::Unique {
        constraint: "notes_text".to_string(),
    };
    let rejected = ClientError::PrecheckFailed(Rejection::Constraint(unique));
    assert_eq!(error(7), rejected);
    assert_eq!(error(8), ClientError::PrecheckFailed(Rejection::RowMissing));
    let last = Row::new().with("text", "Bye");
    assert_eq!(answers[10], Ok(Answer::Rows(vec![last])));
    let only_bob = Answer::Rows(vec![note(1, "bob", "Hello")]);
    assert_eq!(answers[12], Ok(only_bob.clone()));
    for index in 13..=15 {
        assert!(
            matches!(error(index), ClientError::InvalidOperation(_)),
            "{index}"
        );
    }
    assert_eq!(freshness(18), Freshness::Manual);
    assert_eq!(error(20), ClientError::RoomClosed(room_id.clone()));
    assert_eq!(error(21), ClientError::RoomClosed(room_id.clone()));
    assert_eq!(freshness(25), Freshness::Periodic);
    assert_eq!(error(28), ClientError::RoomClosed(room_id.clone()));
    assert!(status(31).epoch > status(2).epoch);
    assert_eq!(answers[32], Ok(only_bob));
    assert!(
        matches!(error(33), ClientError::Network(_)),
        "{:?}",
        answers[33]
    );
    assert_eq!(answers[34], Ok(Answer::Done));
    assert_eq!(error(35), ClientError::RoomClosed(room_id.clone()));
}

// ---------------------------------------------------------------------------------------------
// Concurrency, and the writes

#[tokio::test]
async fn a_write_that_waits_for_its_commit_ack_does_not_hold_back_a_read_of_its_room() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    let mut held = transport.hold_next_reply(ClientMessageKind::Commit);
    let write = page.send(
        TAB,
        on_notes(&room_id, TableCall::Insert(new_note(2, "World"))),
    );
    held.replied().await;
    // The read and the status, sent after the write, reply while the write waits, each with
    // the id of its command and to its tab.
    let read = page.send(TAB, on_notes(&room_id, TableCall::Get(key(1, "bob"))));
    let reply = page.next().await;
    assert_eq!((reply.tab, reply.id), (TAB, read), "{reply:?}");
    assert_eq!(reply.result, Ok(Answer::Row(Some(note(1, "bob", "Hello")))));
    let status = page.send(TAB, on_room(&room_id, RoomCall::Status));
    let reply = page.next().await;
    assert_eq!((reply.tab, reply.id), (TAB, status), "{reply:?}");
    let Ok(Answer::Status(status)) = reply.result else {
        panic!("{reply:?}");
    };
    assert_eq!(status.writes_in_flight, 1);
    assert!(page.quiet().await);
    // Then the write ends, with its reply.
    held.release();
    let reply = page.next().await;
    assert_eq!((reply.tab, reply.id), (TAB, write), "{reply:?}");
    assert_eq!(reply.result, Ok(Answer::Key(key(2, "ana"))));
}

/// Refuses every commit with `RoomNotFound`, without reaching the server.
fn refuse_commits(transport: &TestTransport) {
    transport.answer(move |message| {
        let ClientMessage::Commit { .. } = message else {
            return None;
        };
        let (correlation_id, room_id) = request(message);
        let code = ErrorCode::RoomNotFound;
        Some(Ok(error_reply(correlation_id, &room_id, code)))
    });
}

#[tokio::test]
async fn a_write_is_one_call_of_the_facade_with_one_mutation_id_also_with_retries() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    let notes = |call| on_notes(&room_id, call);

    // The reply to its first attempt is lost: the client retries it with the same mutation id,
    // and the server sequences it once.
    transport.lose_next_reply(ClientMessageKind::Commit);
    let before = commits(&transport).len();
    let written = page
        .call(TAB, notes(TableCall::Insert(new_note(2, "World"))))
        .await;
    assert_eq!(written, Ok(Answer::Key(key(2, "ana"))));
    let sent = &commits(&transport)[before..];
    assert_eq!(sent.len(), 2, "{sent:?}");
    assert_eq!(sent[0].0, sent[1].0);
    assert_eq!(head(&server, &room_id).await, 2);

    // The reply to its first attempt is lost, and the next attempt is refused for good: the
    // outcome is unknown, and nothing sends it again.
    transport.lose_next_reply(ClientMessageKind::Commit);
    refuse_commits(&transport);
    let before = commits(&transport).len();
    let written = page
        .call(TAB, notes(TableCall::Insert(new_note(3, "Again"))))
        .await;
    assert_eq!(written, Err(ClientError::OutcomeUnknown));
    tokio::time::sleep(Duration::from_millis(200)).await;
    let sent = &commits(&transport)[before..];
    assert_eq!(sent.len(), 2, "{sent:?}");
    assert_eq!(sent[0].0, sent[1].0);
    assert_eq!(head(&server, &room_id).await, 3);
    // An update and a delete, too: one commit each.
    transport.online();
    let before = commits(&transport).len();
    let changes = Row::new().with("text", "Bye");
    let updated = page
        .call(TAB, notes(TableCall::Update(key(2, "ana"), changes)))
        .await;
    assert_eq!(updated, Ok(Answer::Done));
    let deleted = page
        .call(TAB, notes(TableCall::Delete(key(2, "ana"))))
        .await;
    assert_eq!(deleted, Ok(Answer::Done));
    assert_eq!(commits(&transport).len(), before + 2);
    assert_eq!(head(&server, &room_id).await, 5);
}

#[tokio::test]
async fn a_discard_ends_a_write_of_the_dispatcher_in_flight_as_the_facade_ends_it() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    let mut held = transport.hold_next_reply(ClientMessageKind::Commit);
    let write = page.send(
        TAB,
        on_notes(&room_id, TableCall::Insert(new_note(2, "World"))),
    );
    held.replied().await;
    let discarded = page.call(TAB, on_room(&room_id, RoomCall::Discard)).await;
    assert_eq!(discarded, Ok(Answer::Done));
    // Its `CommitAck` comes after the discard: its outcome is unknown, and it reached the log,
    // once.
    held.release();
    assert_eq!(
        page.reply(write).await.result,
        Err(ClientError::OutcomeUnknown)
    );
    let read = page
        .call(TAB, on_notes(&room_id, TableCall::Get(key(1, "bob"))))
        .await;
    assert_eq!(read, Err(ClientError::RoomClosed(room_id.clone())));
    // Opened again, as new, the replica has it from the server.
    open_room(&mut page, TAB, &room_id).await;
    let read = page
        .call(TAB, on_notes(&room_id, TableCall::Get(key(2, "ana"))))
        .await;
    assert_eq!(read, Ok(Answer::Row(Some(note(2, "ana", "World")))));
    assert_eq!(commits(&transport).len(), 1);
}

// ---------------------------------------------------------------------------------------------
// The life cycle

/// Every command on `room_id`, with what the facade takes.
fn on_room_commands(room_id: &RoomId) -> Vec<Request> {
    let notes = |call| on_notes(room_id, call);
    let room = |call| on_room(room_id, call);
    // First, the one that the facade takes after its shutdown too.
    vec![
        room(RoomCall::SetFreshness(Freshness::Live)),
        room(RoomCall::Open),
        room(RoomCall::Close),
        room(RoomCall::Discard),
        room(RoomCall::Status),
        room(RoomCall::Sync),
        notes(TableCall::Get(key(1, "bob"))),
        notes(TableCall::Scan(Scan::new())),
        notes(TableCall::Insert(new_note(5, "Late"))),
        notes(TableCall::Update(
            key(1, "ana"),
            Row::new().with("text", "Late"),
        )),
        notes(TableCall::Delete(key(1, "ana"))),
    ]
}

#[tokio::test]
async fn after_shutdown_every_command_on_a_room_is_room_closed() {
    let (server, room_id) = notes().await;
    let other = notes_in(&server, "room-b").await;
    let dir = tempdir().unwrap();
    let (mut page, platform, transport) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    // A write in flight, whose reply is held, when the shutdown comes.
    let mut held = transport.hold_next_reply(ClientMessageKind::Commit);
    let write = page.send(
        TAB,
        on_notes(&room_id, TableCall::Insert(new_note(2, "World"))),
    );
    held.replied().await;
    let shutdown = page.send(TAB, Request::Shutdown);
    // Every command on a room that comes after it is `RoomClosed`, open or not, and nothing
    // reaches the client: nothing is sent.
    let sent = transport.sent().len();
    for room_id in [&room_id, &other] {
        for request in on_room_commands(room_id) {
            let closed = Err(ClientError::RoomClosed(room_id.clone()));
            assert_eq!(page.call(TAB, request.clone()).await, closed, "{request:?}");
        }
    }
    assert_eq!(transport.sent().len(), sent);
    // The shutdown ends, and the write in flight, whose `CommitAck` comes afterwards, ends as
    // the shutdown of the facade ends it.
    assert_eq!(page.reply(shutdown).await.result, Ok(Answer::Done));
    held.release();
    assert_eq!(
        page.reply(write).await.result,
        Err(ClientError::OutcomeUnknown)
    );
    let client = platform.opened();
    let room = client.room(room_id.clone()).await;
    assert_eq!(room.unwrap_err(), ClientError::RoomClosed(room_id.clone()));
    // The client does not open again, keeps its id, and shuts down again.
    let refused = ClientError::Config("the client opens once".to_string());
    assert_eq!(page.call(TAB, open(dir.path())).await, Err(refused));
    let client_id = Answer::ClientId(client.client_id());
    assert_eq!(page.call(TAB, Request::ClientId).await, Ok(client_id));
    assert_eq!(page.call(TAB, Request::Shutdown).await, Ok(Answer::Done));
    for request in on_room_commands(&room_id) {
        let closed = Err(ClientError::RoomClosed(room_id.clone()));
        assert_eq!(page.call(TAB, request.clone()).await, closed, "{request:?}");
    }
    assert_eq!(transport.sent().len(), sent);
}

#[tokio::test]
async fn the_client_opens_once_and_an_open_that_failed_may_be_tried_again() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let platform = TestPlatform::new(&transport);
    let mut page = Page::new(&platform);
    // Before the open, a command on a room is `RoomClosed`, and the client has no id.
    for request in on_room_commands(&room_id) {
        let closed = Err(ClientError::RoomClosed(room_id.clone()));
        assert_eq!(page.call(TAB, request.clone()).await, closed, "{request:?}");
    }
    let not_open = ClientError::Config("the client is not open yet".to_string());
    assert_eq!(page.call(TAB, Request::ClientId).await, Err(not_open));
    assert_eq!(page.call(TAB, Request::Left).await, Ok(Answer::Done));
    // An open whose configuration does not work fails as the facade's, and the next one opens.
    let mut config = config(dir.path());
    config.client.person = String::new();
    let failed = page.call(TAB, Request::Open(Box::new(config))).await;
    assert!(matches!(failed, Err(ClientError::Config(_))), "{failed:?}");
    assert!(platform.opened.lock().unwrap().is_none());
    // Two opens at once: the second is refused while the first is in flight.
    let gate = platform.gate.lock().await;
    let first = page.send(TAB, open(dir.path()));
    let refused = ClientError::Config("the client opens once".to_string());
    assert_eq!(page.call(TAB, open(dir.path())).await, Err(refused.clone()));
    drop(gate);
    let opened = page.reply(first).await.result;
    let client_id = Answer::ClientId(platform.opened().client_id());
    assert_eq!(opened, Ok(client_id.clone()));
    assert_eq!(page.call(TAB, Request::ClientId).await, Ok(client_id));
    // And once it is open.
    assert_eq!(page.call(TAB, open(dir.path())).await, Err(refused));
    open_room(&mut page, TAB, &room_id).await;
}

#[tokio::test]
async fn a_shutdown_that_comes_while_the_client_opens_shuts_down_the_client_it_opens() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let platform = TestPlatform::new(&transport);
    let mut page = Page::new(&platform);
    let gate = platform.gate.lock().await;
    let opening = page.send(TAB, open(dir.path()));
    let shutdown = page.send(TAB, Request::Shutdown);
    // The shutdown waits for the open.
    assert!(page.quiet().await);
    drop(gate);
    let opened = page.reply(opening).await.result;
    assert_eq!(opened, Ok(Answer::ClientId(platform.opened().client_id())));
    assert_eq!(page.reply(shutdown).await.result, Ok(Answer::Done));
    // The client it opened is shut down, and lets the directory go: another client opens it.
    let room = platform.opened().room(room_id.clone()).await;
    assert_eq!(room.unwrap_err(), ClientError::RoomClosed(room_id.clone()));
    let metadata = FileMetadata::new(dir.path());
    let other = ZemdbClient::open(
        ClientConfig::new("ana"),
        engine(dir.path()),
        metadata,
        transport.clone(),
        TestTokens::new(),
        None,
    );
    other.await.unwrap().shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------------------------
// The tabs

/// Whether `room_id` is open, as a status that another tab asks for tells.
async fn is_open(page: &mut Page, room_id: &RoomId) -> bool {
    let status = page.call(99, on_room(room_id, RoomCall::Status)).await;
    status.is_ok()
}

#[tokio::test]
async fn each_reply_goes_to_its_tab_and_a_tab_that_left_closes_only_the_rooms_no_other_opened() {
    let server = TestServer::new();
    let [a, b, c] = [
        notes_in(&server, "room-a").await,
        notes_in(&server, "room-b").await,
        notes_in(&server, "room-c").await,
    ];
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    let (one, two, three) = (1, 2, 3);
    for (tab, room_id) in [(one, &a), (one, &b), (two, &b), (two, &c)] {
        open_room(&mut page, tab, room_id).await;
    }
    // A read of each tab, sent together: each reply goes to the tab of its command.
    let read = |room_id| on_notes(room_id, TableCall::Get(key(1, "bob")));
    let ids = [page.send(one, read(&b)), page.send(two, read(&b))];
    for _ in 0..2 {
        let reply = page.next().await;
        let tab = if reply.id == ids[0] { one } else { two };
        assert_eq!(reply.tab, tab, "{reply:?}");
        assert_eq!(reply.result, Ok(Answer::Row(Some(note(1, "bob", "Hello")))));
    }

    // The first tab leaves: the room only it opened closes, and the one the second opened too
    // stays open, as do the second tab's.
    assert_eq!(page.call(one, Request::Left).await, Ok(Answer::Done));
    assert!(!is_open(&mut page, &a).await);
    assert!(is_open(&mut page, &b).await);
    assert!(is_open(&mut page, &c).await);
    assert_eq!(
        page.call(two, read(&b)).await,
        Ok(Answer::Row(Some(note(1, "bob", "Hello"))))
    );
    // A tab that never opened a room leaves: nothing closes.
    assert_eq!(page.call(three, Request::Left).await, Ok(Answer::Done));
    assert!(is_open(&mut page, &b).await);
    // The second leaves: its rooms close.
    assert_eq!(page.call(two, Request::Left).await, Ok(Answer::Done));
    assert!(!is_open(&mut page, &b).await);
    assert!(!is_open(&mut page, &c).await);

    // A room that a tab closed or discarded, and another opened again, is only of the one that
    // opened it again: it closes once that one leaves, whichever leaves first.
    for ended in [RoomCall::Close, RoomCall::Discard] {
        for first in [one, two] {
            let calls = [(one, RoomCall::Open), (one, ended), (two, RoomCall::Open)];
            for (tab, call) in calls {
                assert_eq!(page.call(tab, on_room(&a, call)).await, Ok(Answer::Done));
            }
            assert_eq!(page.call(first, Request::Left).await, Ok(Answer::Done));
            let open = is_open(&mut page, &a).await;
            assert_eq!(open, first == one, "{ended:?}, {first} left first");
            let second = one + two - first;
            assert_eq!(page.call(second, Request::Left).await, Ok(Answer::Done));
            assert!(
                !is_open(&mut page, &a).await,
                "{ended:?}, {first} left first"
            );
        }
    }
}

#[tokio::test]
async fn a_shutdown_before_the_open_is_done_and_no_open_follows_it() {
    let server = TestServer::new();
    let dir = tempdir().unwrap();
    let platform = TestPlatform::new(&server.transport());
    let mut page = Page::new(&platform);
    assert_eq!(page.call(TAB, Request::Shutdown).await, Ok(Answer::Done));
    let refused = ClientError::Config("the client opens once".to_string());
    assert_eq!(page.call(TAB, open(dir.path())).await, Err(refused));
    assert!(platform.opened.lock().unwrap().is_none());
}

/// Opens `room_id` from `tab`, and moves the head of its replica past the cursor that the server
/// took with a write, so that closing the room sends the last acknowledgement of its worker.
async fn written(page: &mut Page, tab: u64, room_id: &RoomId) {
    open_room(page, tab, room_id).await;
    let note = TableCall::Insert(new_note(2, "World"));
    let written = page.call(tab, on_notes(room_id, note)).await;
    assert_eq!(written, Ok(Answer::Key(key(2, "ana"))));
}

#[tokio::test]
async fn a_shutdown_replies_once_the_client_shut_down() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, platform, transport) = opened(&server, dir.path()).await;
    written(&mut page, TAB, &room_id).await;
    // The last acknowledgement of the worker, which the shutdown waits for, is held.
    let mut held = transport.hold_next_reply(ClientMessageKind::Ack);
    let shutdown = page.send(TAB, Request::Shutdown);
    held.replied().await;
    assert!(page.quiet().await);
    held.release();
    assert_eq!(page.reply(shutdown).await.result, Ok(Answer::Done));
    let room = platform.opened().room(room_id.clone()).await;
    assert_eq!(room.unwrap_err(), ClientError::RoomClosed(room_id.clone()));
}

/// Tab 1, which opened `room_id`, leaves while tab 2 opens it: once the close of tab 1 ends, the
/// open of tab 2, dispatched after it, opens the room, which stays open.
async fn left_while_another_tab_opens(
    page: &mut Page,
    transport: &TestTransport,
    room_id: &RoomId,
) {
    written(page, 1, room_id).await;
    // The close of tab 1 waits for the last acknowledgement of the worker, which is held.
    let mut held = transport.hold_next_reply(ClientMessageKind::Ack);
    let left = page.send(1, Request::Left);
    held.replied().await;
    let opening = page.send(2, on_room(room_id, RoomCall::Open));
    // The open waits for the close.
    assert!(page.quiet().await);
    held.release();
    assert_eq!(page.reply(left).await.result, Ok(Answer::Done));
    assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    let read = page
        .call(2, on_notes(room_id, TableCall::Get(key(2, "ana"))))
        .await;
    assert_eq!(read, Ok(Answer::Row(Some(note(2, "ana", "World")))));
    assert!(is_open(page, room_id).await);
}

#[tokio::test]
async fn a_tab_that_left_does_not_close_a_room_that_another_tab_opens_after_it() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    left_while_another_tab_opens(&mut page, &transport, &room_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tab_that_left_does_not_close_a_room_that_another_tab_opens_after_it_with_threads() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    left_while_another_tab_opens(&mut page, &transport, &room_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_calls_that_open_close_or_discard_a_room_run_in_the_order_they_were_dispatched() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    for ended in [RoomCall::Close, RoomCall::Discard] {
        for _ in 0..10 {
            // An open and a close or a discard sent together: the room ends closed, and the
            // tab does not hold it, so the tab that opens it next holds it alone.
            let ids = [
                page.send(1, on_room(&room_id, RoomCall::Open)),
                page.send(1, on_room(&room_id, ended)),
            ];
            for id in ids {
                assert_eq!(page.reply(id).await.result, Ok(Answer::Done), "{ended:?}");
            }
            assert!(!is_open(&mut page, &room_id).await, "{ended:?}");
            let opened = page.call(2, on_room(&room_id, RoomCall::Open)).await;
            assert_eq!(opened, Ok(Answer::Done));
            assert_eq!(page.call(2, Request::Left).await, Ok(Answer::Done));
            assert!(!is_open(&mut page, &room_id).await, "{ended:?}");
            // An open and the notice that its tab left, sent together: the room opens, and
            // then closes.
            let ids = [
                page.send(1, on_room(&room_id, RoomCall::Open)),
                page.send(1, Request::Left),
            ];
            for id in ids {
                assert_eq!(page.reply(id).await.result, Ok(Answer::Done), "{ended:?}");
            }
            assert!(!is_open(&mut page, &room_id).await, "{ended:?}");
        }
    }
}

/// The status of `room_id`, as another tab asks for it.
async fn status_of(page: &mut Page, room_id: &RoomId) -> RoomStatus {
    match page.call(99, on_room(room_id, RoomCall::Status)).await {
        Ok(Answer::Status(status)) => status,
        result => panic!("{result:?}"),
    }
}

#[tokio::test]
async fn an_open_that_failed_does_not_count_its_tab() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    // Without the network, a room without metadata does not open.
    transport.offline();
    let failed = page.call(1, on_room(&room_id, RoomCall::Open)).await;
    assert!(matches!(failed, Err(ClientError::Network(_))), "{failed:?}");
    transport.online();
    let opened = page.call(2, on_room(&room_id, RoomCall::Open)).await;
    assert_eq!(opened, Ok(Answer::Done));
    // The room is only of the tab whose open opened it.
    assert_eq!(page.call(2, Request::Left).await, Ok(Answer::Done));
    assert!(!is_open(&mut page, &room_id).await);
}

#[tokio::test]
async fn a_slow_open_does_not_hold_back_the_calls_of_other_rooms_through_a_tab_that_left() {
    let server = TestServer::new();
    let slow = notes_in(&server, "room-x").await;
    let mut others = Vec::new();
    for room in 0..8 {
        others.push(notes_in(&server, &format!("room-{room}")).await);
    }
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    for room_id in &others {
        open_room(&mut page, 1, room_id).await;
    }
    // The open of a room by tab 3 waits for its registration, whose reply is held.
    let mut held = transport.hold_next_reply(ClientMessageKind::RegisterClient);
    let opening = page.send(3, on_room(&slow, RoomCall::Open));
    held.replied().await;
    // Tab 9, which opened nothing, leaves, and tab 4 opens and closes the other rooms: none of
    // them waits for the slow open.
    let left = page.send(9, Request::Left);
    let mut calls = Vec::new();
    for room_id in &others {
        calls.push(page.send(4, on_room(room_id, RoomCall::Open)));
        calls.push(page.send(4, on_room(room_id, RoomCall::Close)));
    }
    let came = page.arrived(Duration::from_millis(500)).await;
    for id in &calls {
        assert!(came.contains(id), "{id} waited for the slow open");
    }
    held.release();
    assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    assert_eq!(page.reply(left).await.result, Ok(Answer::Done));
    for id in calls {
        assert_eq!(page.reply(id).await.result, Ok(Answer::Done));
    }
    assert!(is_open(&mut page, &slow).await);
}

#[tokio::test]
async fn a_tab_that_left_releases_each_of_its_rooms_without_waiting_for_a_slow_one() {
    let server = TestServer::new();
    let slow = notes_in(&server, "room-x").await;
    let mut others = Vec::new();
    for room in 0..8 {
        others.push(notes_in(&server, &format!("room-{room}")).await);
    }
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    // In some order of the rooms, the slow one comes before the others: a few rounds reach one.
    for round in 0..5 {
        for room_id in &others {
            open_room(&mut page, 1, room_id).await;
        }
        // Tab 1 opens one more room, whose registration waits, and leaves.
        let mut held = transport.hold_next_reply(ClientMessageKind::RegisterClient);
        let opening = page.send(1, on_room(&slow, RoomCall::Open));
        held.replied().await;
        let left = page.send(1, Request::Left);
        // Its other rooms close without waiting for the slow one: tab 2 opens them again.
        let opens = Vec::from_iter(others.iter().map(|room_id| {
            let open = on_room(room_id, RoomCall::Open);
            page.send(2, open)
        }));
        let came = page.arrived(Duration::from_millis(300)).await;
        for id in &opens {
            assert!(
                came.contains(id),
                "round {round}: {id} waited for the slow open"
            );
        }
        assert!(!came.contains(&left), "round {round}");
        held.release();
        assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
        assert_eq!(page.reply(left).await.result, Ok(Answer::Done));
        for id in opens {
            assert_eq!(page.reply(id).await.result, Ok(Answer::Done));
        }
        // The slow room closed with the notice, and the others are of tab 2.
        assert!(!is_open(&mut page, &slow).await, "round {round}");
        assert_eq!(page.call(2, Request::Left).await, Ok(Answer::Done));
        for room_id in &others {
            assert!(
                !is_open(&mut page, room_id).await,
                "round {round}: {room_id}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_tabs_that_leave_together_leave_their_rooms_to_the_tab_that_opens_them_next() {
    let server = TestServer::new();
    let mut rooms = Vec::new();
    for room in 0..40 {
        rooms.push(notes_in(&server, &format!("many-{room}")).await);
    }
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    for (index, room_id) in rooms.iter().enumerate() {
        let tab = 1 + index as u64 % 2;
        let opened = page.call(tab, on_room(room_id, RoomCall::Open)).await;
        assert_eq!(opened, Ok(Answer::Done));
    }
    // Both tabs leave, and a third tab opens and reads every room, all sent together.
    let mut ids = vec![page.send(1, Request::Left), page.send(2, Request::Left)];
    for room_id in &rooms {
        ids.push(page.send(3, on_room(room_id, RoomCall::Open)));
        ids.push(page.send(5, on_notes(room_id, TableCall::Get(key(1, "bob")))));
    }
    for id in ids {
        let reply = page.reply(id).await;
        let closed = matches!(reply.result, Err(ClientError::RoomClosed(_)));
        assert!(reply.result.is_ok() || closed, "{reply:?}");
    }
    // Every room is of the third tab, alone.
    for room_id in &rooms {
        assert!(is_open(&mut page, room_id).await, "{room_id}");
    }
    assert_eq!(page.call(3, Request::Left).await, Ok(Answer::Done));
    for room_id in &rooms {
        assert!(!is_open(&mut page, room_id).await, "{room_id}");
    }
}

#[tokio::test]
async fn a_tab_that_left_closes_its_room_before_an_open_sent_right_after_it() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    written(&mut page, 1, &room_id).await;
    let registered = transport.registrations().len();
    // The close of tab 1 waits for the last acknowledgement of the worker, which is held. The
    // notice and the open of tab 2 are sent together, without an `.await` between them.
    let mut held = transport.hold_next_reply(ClientMessageKind::Ack);
    let left = page.send(1, Request::Left);
    let opening = page.send(2, on_room(&room_id, RoomCall::Open));
    let closing = tokio::time::timeout(Duration::from_secs(5), held.replied()).await;
    assert!(closing.is_ok(), "the notice did not close the room");
    assert!(page.quiet().await);
    held.release();
    assert_eq!(page.reply(left).await.result, Ok(Answer::Done));
    assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    // The open came after the close: it opened the room again, which is of tab 2 alone.
    assert_eq!(transport.registrations().len(), registered + 1);
    assert!(is_open(&mut page, &room_id).await);
    assert_eq!(page.call(2, Request::Left).await, Ok(Answer::Done));
    assert!(!is_open(&mut page, &room_id).await);
}

#[tokio::test]
async fn a_tab_that_left_closes_a_room_that_an_open_of_its_own_still_in_flight_opens() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    // A room that no tab opened yet, whose registration waits.
    let mut held = transport.hold_next_reply(ClientMessageKind::RegisterClient);
    let opening = page.send(1, on_room(&room_id, RoomCall::Open));
    held.replied().await;
    let left = page.send(1, Request::Left);
    assert!(page.quiet().await);
    held.release();
    assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    assert_eq!(page.reply(left).await.result, Ok(Answer::Done));
    assert!(!is_open(&mut page, &room_id).await);
}

#[tokio::test]
async fn the_reads_writes_and_other_calls_of_a_room_do_not_wait_for_its_close() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    written(&mut page, 1, &room_id).await;
    // The close waits for the last acknowledgement of the worker, which is held.
    let mut held = transport.hold_next_reply(ClientMessageKind::Ack);
    let close = page.send(1, on_room(&room_id, RoomCall::Close));
    held.replied().await;
    let notes = |call| on_notes(&room_id, call);
    let calls = [
        page.send(2, notes(TableCall::Get(key(1, "bob")))),
        page.send(2, notes(TableCall::Scan(Scan::new()))),
        page.send(2, notes(TableCall::Insert(new_note(3, "Again")))),
        page.send(2, on_room(&room_id, RoomCall::Status)),
        page.send(
            2,
            on_room(&room_id, RoomCall::SetFreshness(Freshness::Manual)),
        ),
    ];
    let came = page.arrived(Duration::from_millis(300)).await;
    for id in calls {
        assert!(came.contains(&id), "{id} waited for the close");
        let reply = page.reply(id).await;
        assert!(reply.result.is_ok(), "{reply:?}");
    }
    assert!(!came.contains(&close));
    held.release();
    assert_eq!(page.reply(close).await.result, Ok(Answer::Done));
}

#[tokio::test]
async fn a_tab_that_left_closes_its_rooms_and_does_not_discard_them() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, transport) = opened(&server, dir.path()).await;
    written(&mut page, 1, &room_id).await;
    let before = status_of(&mut page, &room_id).await;
    assert_eq!(page.call(1, Request::Left).await, Ok(Answer::Done));
    assert!(!is_open(&mut page, &room_id).await);
    // Without the network, the room opens again from its metadata, with its rows and its
    // epoch: a discard would have lost them.
    transport.offline();
    let opened = page.call(2, on_room(&room_id, RoomCall::Open)).await;
    assert_eq!(opened, Ok(Answer::Done));
    let rows = page
        .call(2, on_notes(&room_id, TableCall::Scan(Scan::new())))
        .await;
    let notes = vec![note(1, "bob", "Hello"), note(2, "ana", "World")];
    assert_eq!(rows, Ok(Answer::Rows(notes)));
    assert_eq!(status_of(&mut page, &room_id).await.epoch, before.epoch);
}

#[tokio::test]
async fn a_tab_that_left_reports_a_close_that_failed_and_closes_the_others() {
    let server = TestServer::new();
    let [a, b, c] = [
        notes_in(&server, "room-a").await,
        notes_in(&server, "room-b").await,
        notes_in(&server, "room-c").await,
    ];
    let dir = tempdir().unwrap();
    let (mut page, platform, _) = opened(&server, dir.path()).await;
    for room_id in [&a, &b, &c] {
        open_room(&mut page, 1, room_id).await;
    }
    // A room that the client closed by another way does not fail the close.
    platform.opened().discard_room(c.clone()).await.unwrap();
    // A close fails: the tab reports it, and the others close all the same.
    let error = std::io::Error::other("Injected");
    platform
        .engine()
        .fail_next("close_room", StorageError::Io(error));
    let left = page.call(1, Request::Left).await;
    assert!(matches!(left, Err(ClientError::Storage(_))), "{left:?}");
    for room_id in [&a, &b, &c] {
        assert!(!is_open(&mut page, room_id).await, "{room_id}");
    }
    // Without a failed close, the tab that leaves gets `Done`, also with a room that the client
    // closed by another way.
    for room_id in [&a, &b] {
        open_room(&mut page, 1, room_id).await;
    }
    platform.opened().discard_room(b.clone()).await.unwrap();
    assert_eq!(page.call(1, Request::Left).await, Ok(Answer::Done));
    assert!(!is_open(&mut page, &a).await);
}

/// A platform whose first open panics.
#[derive(Clone)]
struct PanicsOnce {
    inner: TestPlatform,
    panicked: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl Platform for PanicsOnce {
    async fn open(&self, config: DispatchConfig) -> Result<ZemdbClient, ClientError> {
        if !self
            .panicked
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("The platform panics");
        }
        self.inner.open(config).await
    }
}

#[tokio::test]
async fn a_command_that_panics_has_no_reply_and_the_dispatcher_goes_on() {
    let server = TestServer::new();
    let dir = tempdir().unwrap();
    let inner = TestPlatform::new(&server.transport());
    let platform = PanicsOnce {
        inner: inner.clone(),
        panicked: Arc::default(),
    };
    let (sender, mut replies) = mpsc::unbounded_channel();
    let publish = move |Message::Reply(reply)| drop(sender.send(reply));
    let dispatcher = Dispatcher::new(platform, publish);
    let command = |id| Command {
        tab: TAB,
        id,
        request: open(dir.path()),
    };
    dispatcher.dispatch(command(1));
    let reply = tokio::time::timeout(Duration::from_millis(300), replies.recv()).await;
    assert!(reply.is_err(), "{reply:?}");
    // The open that panicked let the client be opened again.
    dispatcher.dispatch(command(2));
    let reply = tokio::time::timeout(Duration::from_secs(30), replies.recv()).await;
    let reply = reply.unwrap().unwrap();
    assert_eq!(reply.id, 2);
    assert_eq!(
        reply.result,
        Ok(Answer::ClientId(inner.opened().client_id()))
    );
}

// ---------------------------------------------------------------------------------------------
// The encoding

/// A value of each type, at random, with the edges of each.
fn value(rng: &mut Rng) -> Value {
    let int = |rng: &mut Rng| match rng.below(4) {
        0 => i64::MIN,
        1 => i64::MAX,
        2 => 0,
        _ => rng.next_u64() as i64,
    };
    match rng.below(8) {
        0 => Value::Null,
        1 => Value::Int(int(rng)),
        2 => Value::Float(match rng.below(6) {
            // A NaN with a payload, quiet or signaling, and of either sign.
            0 => f64::from_bits(0x7ff8_0000_dead_beef | (rng.next_u64() & (1 << 63))),
            1 => f64::from_bits(0x7ff0_0000_0000_0001 + rng.below(1 << 51)),
            2 => -0.0,
            3 => [
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::MIN_POSITIVE,
                f64::MAX,
            ][rng.below(4) as usize],
            _ => f64::from_bits(rng.next_u64()),
        }),
        3 => Value::Timestamp(int(rng)),
        4 => Value::String(text(rng).into()),
        5 => Value::Bool(rng.chance(50)),
        6 => Value::Bytes((0..rng.below(40)).map(|_| rng.next_u64() as u8).collect()),
        _ => Value::Uuid(rng.next_u64().to_le_bytes().repeat(2).try_into().unwrap()),
    }
}

/// A text of up to 12 characters, ASCII or not.
fn text(rng: &mut Rng) -> String {
    let chars = [
        'a', 'Z', '0', ' ', '#', 'ñ', 'é', '€', '中', '🦀', '\u{7f}', '"',
    ];
    (0..rng.below(13))
        .map(|_| chars[rng.below(chars.len() as u64) as usize])
        .collect()
}

fn columns<T: FromIterator<(String, Value)>>(rng: &mut Rng) -> T {
    (0..rng.below(5))
        .map(|column| (format!("{}{column}", text(rng)), value(rng)))
        .collect()
}

fn room_id(rng: &mut Rng) -> RoomId {
    let chars = b"abcxyz0189-_";
    let id: String = (0..1 + rng.below(64))
        .map(|_| chars[rng.below(chars.len() as u64) as usize] as char)
        .collect();
    RoomId::new(id).unwrap()
}

fn bound(rng: &mut Rng) -> Bound<Key> {
    match rng.below(3) {
        0 => Bound::Unbounded,
        1 => Bound::Included(columns(rng)),
        _ => Bound::Excluded(columns(rng)),
    }
}

fn scan(rng: &mut Rng) -> Scan {
    Scan {
        start: bound(rng),
        end: bound(rng),
        direction: [Direction::Forward, Direction::Backward][rng.below(2) as usize],
        limit: rng.chance(50).then(|| rng.next_u64() as usize),
        columns: rng
            .chance(50)
            .then(|| (0..rng.below(4)).map(|_| text(rng)).collect()),
    }
}

fn freshness(rng: &mut Rng) -> Freshness {
    [Freshness::Live, Freshness::Periodic, Freshness::Manual][rng.below(3) as usize]
}

fn duration(rng: &mut Rng) -> Duration {
    Duration::new(rng.next_u64(), rng.below(1_000_000_000) as u32)
}

fn dispatch_config(rng: &mut Rng) -> DispatchConfig {
    let client = ClientConfig {
        person: text(rng),
        write_timeout: duration(rng),
        backoff_base: duration(rng),
        backoff_max: duration(rng),
        max_snapshot_bytes: rng.next_u64(),
        precheck: rng.chance(50),
        freshness: freshness(rng),
        heartbeat_interval: duration(rng),
        retry_base: duration(rng),
        retry_max: duration(rng),
        events_retry: duration(rng),
    };
    DispatchConfig {
        client,
        server: text(rng),
        directory: text(rng),
        idle_timeout: duration(rng),
        max_streams: rng.next_u64() as u32,
        wait: rng.chance(50),
    }
}

fn table(rng: &mut Rng) -> Table {
    Table {
        room_id: room_id(rng),
        name: text(rng),
    }
}

/// Each request, with values at random.
fn requests(rng: &mut Rng) -> Vec<Request> {
    let mut requests = vec![
        Request::Open(Box::new(dispatch_config(rng))),
        Request::ClientId,
        Request::Shutdown,
        Request::Left,
    ];
    let calls = [
        RoomCall::Open,
        RoomCall::Close,
        RoomCall::Discard,
        RoomCall::Status,
        RoomCall::Sync,
        RoomCall::SetFreshness(freshness(rng)),
    ];
    requests.extend(calls.map(|call| Request::Room(room_id(rng), call)));
    let calls = [
        TableCall::Get(columns(rng)),
        TableCall::Scan(scan(rng)),
        TableCall::Insert(columns(rng)),
        TableCall::Update(columns(rng), columns(rng)),
        TableCall::Delete(columns(rng)),
    ];
    requests.extend(calls.map(|call| Request::Table(table(rng), call)));
    requests
}

/// Each error of the client, with values at random.
fn errors(rng: &mut Rng) -> Vec<ClientError> {
    let violations = [
        Violation::Unique {
            constraint: text(rng),
        },
        Violation::ForeignKey {
            constraint: text(rng),
        },
        Violation::Restrict {
            constraint: text(rng),
        },
        Violation::Duplicate,
        Violation::NotOwner,
    ];
    let mut errors = vec![
        ClientError::Config(text(rng)),
        ClientError::Storage(text(rng)),
        ClientError::RoomClosed(room_id(rng)),
        ClientError::Network(text(rng)),
        ClientError::FailedClosed(room_id(rng)),
        ClientError::RowMissing,
        ClientError::OutcomeUnknown,
        ClientError::Validation(text(rng)),
        ClientError::InvalidOperation(text(rng)),
        ClientError::Crypto(text(rng)),
        ClientError::Decryption(text(rng)),
        ClientError::PrecheckFailed(Rejection::RowMissing),
        ClientError::Offline(room_id(rng)),
    ];
    for violation in violations {
        errors.push(ClientError::ConstraintViolation(violation.clone()));
        errors.push(ClientError::PrecheckFailed(Rejection::Constraint(
            violation,
        )));
    }
    errors
}

/// Each answer, with values at random.
fn answers(rng: &mut Rng) -> Vec<Answer> {
    let worker = WorkerStatus {
        freshness: freshness(rng),
        effective: freshness(rng),
        stream_open: rng.chance(50),
        running: rng.chance(50),
        connected: [None, Some(false), Some(true)][rng.below(3) as usize],
        last_error: rng.chance(50).then(|| ClientError::Network(text(rng))),
    };
    let status = RoomStatus {
        head_seq: SequenceNumber::new(rng.next_u64()),
        epoch: rng.next_u64(),
        registered: rng.chance(50),
        failed_closed: rng.chance(50),
        writes_in_flight: rng.next_u64() as usize,
        retained: rng.next_u64() as usize,
        worker: rng.chance(50).then_some(worker),
        watches: rng.next_u64() as usize,
    };
    let person = text(rng).replace('\u{7f}', "");
    let client_id = ClientId::new(format!("p{person}#{}", rng.next_u64())).unwrap();
    vec![
        Answer::Done,
        Answer::ClientId(client_id),
        Answer::Status(status),
        Answer::Row(rng.chance(50).then(|| columns(rng))),
        Answer::Rows((0..rng.below(4)).map(|_| columns(rng)).collect()),
        Answer::Key(columns(rng)),
    ]
}

/// `value` decoded from its encoding with `options`, which encodes it again to the same bytes: a
/// float keeps its bits, also a NaN with a payload and `-0.0`.
fn round_trip<T, O>(value: &T, options: O)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    O: bincode::Options + Copy,
{
    let bytes = options.serialize(value).unwrap();
    let decoded: T = options.deserialize(&bytes).unwrap();
    assert_eq!(&decoded, value);
    assert_eq!(options.serialize(&decoded).unwrap(), bytes, "{value:?}");
}

#[test]
fn every_command_and_reply_goes_through_bincode_unchanged() {
    use bincode::Options;
    let mut rng = Rng::new(0x5eed_d15c);
    let fixint = bincode::DefaultOptions::new().with_fixint_encoding();
    let varint = bincode::DefaultOptions::new().with_varint_encoding();
    for _ in 0..200 {
        for request in requests(&mut rng) {
            let command = Command {
                tab: rng.next_u64(),
                id: rng.next_u64(),
                request,
            };
            round_trip(&command, fixint);
            round_trip(&command, varint);
        }
        let results =
            (answers(&mut rng).into_iter().map(Ok)).chain(errors(&mut rng).into_iter().map(Err));
        for result in results {
            let reply = Message::Reply(Reply {
                tab: rng.next_u64(),
                id: rng.next_u64(),
                result,
            });
            round_trip(&reply, fixint);
            round_trip(&reply, varint);
        }
    }
}

#[test]
fn the_values_at_random_hold_the_edges_of_each_type() {
    // The generator of the round trips reaches a NaN with a payload, `-0.0` and the edges of
    // `i64`, so the round trip goes through each.
    let mut rng = Rng::new(0x5eed_d15c);
    let values: Vec<Value> = (0..2000).map(|_| value(&mut rng)).collect();
    let float = |test: fn(f64) -> bool| {
        (values.iter()).any(|value| matches!(value, Value::Float(float) if test(*float)))
    };
    assert!(float(
        |float| float.is_nan() && float.to_bits() & 0xffff_ffff == 0xdead_beef
    ));
    assert!(float(|float| float == 0.0 && float.is_sign_negative()));
    for int in [i64::MIN, i64::MAX] {
        assert!(values.contains(&Value::Int(int)));
        assert!(values.contains(&Value::Timestamp(int)));
    }
}
