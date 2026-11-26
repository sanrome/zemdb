//! The dispatcher of the runtime of a browser Worker, in native, over the facade against the
//! server in process: each command gives what its call of the facade gives, in a task of its
//! own, with its reply to its tab; the client opens once, and after its shutdown every command on
//! a room is `RoomClosed`; a tab that left releases its rooms and ends its subscriptions; a
//! subscription has one value in flight until its tab confirms it; the tokens come from a tab of
//! the room; and every command, reply and notice goes through `bincode` unchanged.
#![allow(clippy::disallowed_methods)]

mod common;

/// The generator of the storage's randomized tests.
#[path = "../../storage/tests/common/rng.rs"]
#[allow(dead_code)]
mod rng;

use async_trait::async_trait;
use common::*;
use rng::Rng;
use std::collections::{HashMap, VecDeque};
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::time::Instant;
use zemdb_client::dispatch::{
    Answer, Command, DispatchConfig, Dispatcher, Message, Notice, PageTokens, Platform, Reply,
    Request, RoomCall, Table, TableCall, Watch,
};
use zemdb_client::{
    ClientConfig, ClientError, Direction, FileMetadata, Freshness, Key, Rejection, RoomHandle,
    RoomStatus, Row, Scan, Violation, WorkerStatus, ZemdbClient,
};
use zemdb_core::{
    ClientId, ClientMessage, ClientMessageKind, DataType, ErrorCode, OwnerIdentity, RoomId,
    RowBuilder, Schema, SequenceNumber, TableSchema, Value,
};
use zemdb_server::generate_client_token;
use zemdb_storage::{DiskStorageEngine, StorageError};

/// The tab of the commands of a test with one tab.
const TAB: u64 = 7;

// ---------------------------------------------------------------------------------------------
// The page and the platform

/// The platform of the tests: a client of the facade over a disk engine and its metadata in the
/// directory of the configuration, through the transport to the server in process, with a token
/// provider that issues valid tokens (or the one of the dispatcher, which asks the page), and
/// without a `CryptoEngine`. Its heartbeats get no reply, so that its worker sends nothing else
/// (`TestTransport::hush`).
#[derive(Clone)]
struct TestPlatform {
    transport: TestTransport,
    /// Whether the client takes its tokens from the page, through the dispatcher.
    asks_the_page: bool,
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
            asks_the_page: false,
            opened: Arc::default(),
            engine: Arc::default(),
            gate: Arc::default(),
        }
    }

    /// A platform whose client takes its tokens from the page.
    fn asking_the_page(transport: &TestTransport) -> Self {
        Self {
            asks_the_page: true,
            ..Self::new(transport)
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
    async fn open(
        &self,
        config: DispatchConfig,
        tokens: PageTokens,
    ) -> Result<ZemdbClient, ClientError> {
        drop(self.gate.lock().await);
        let dir = Path::new(&config.directory);
        let storage = TestEngine::new(DiskStorageEngine::new(options(dir))?);
        *self.engine.lock().unwrap() = Some(storage.clone());
        let metadata = FileMetadata::new(dir);
        let transport = self.transport.clone();
        let client = if self.asks_the_page {
            ZemdbClient::open(config.client, storage, metadata, transport, tokens, None).await?
        } else {
            let tokens = TestTokens::new();
            ZemdbClient::open(config.client, storage, metadata, transport, tokens, None).await?
        };
        self.transport.hush(&client.client_id());
        *self.opened.lock().unwrap() = Some(client.clone());
        Ok(client)
    }
}

/// The page of a test: it sends the commands of its tabs to its dispatcher, and takes the replies
/// and the notices that the dispatcher publishes.
struct Page {
    dispatcher: Arc<OnceLock<Dispatcher>>,
    messages: mpsc::UnboundedReceiver<Message>,
    /// Every message that came, in the order it came.
    log: Vec<Message>,
    /// The replies that came before the test asked for them, in the order they came.
    early: Vec<Reply>,
    /// The notices that came and the test did not take yet, in the order they came.
    notices: VecDeque<Notice>,
    last: u64,
    /// The longest it waits for a reply or a notice.
    wait: Duration,
}

impl Page {
    fn new(platform: &TestPlatform) -> Self {
        Self::with(platform, |_, _| {})
    }

    /// A page whose dispatcher runs `hook` with each message as it publishes it, before the page
    /// takes it: `hook` may dispatch a command.
    fn with(
        platform: &TestPlatform,
        hook: impl Fn(&Dispatcher, &Message) + Send + Sync + 'static,
    ) -> Self {
        let (sender, messages) = mpsc::unbounded_channel();
        let dispatcher = Arc::new(OnceLock::new());
        let hooked = Arc::downgrade(&dispatcher);
        let publish = move |message: Message| {
            let dispatcher = hooked.upgrade();
            if let Some(dispatcher) = dispatcher.as_ref().and_then(|once| once.get()) {
                hook(dispatcher, &message);
            }
            // The page may be gone once the test ends, with commands still in flight.
            drop(sender.send(message));
        };
        let set = dispatcher.set(Dispatcher::new(platform.clone(), publish));
        assert!(set.is_ok());
        Self {
            dispatcher,
            messages,
            log: Vec::new(),
            early: Vec::new(),
            notices: VecDeque::new(),
            last: 0,
            wait: Duration::from_secs(30),
        }
    }

    /// Sends `request` from `tab`, and returns the id of its command.
    fn send(&mut self, tab: u64, request: Request) -> u64 {
        self.last += 1;
        let id = self.last;
        let dispatcher = self.dispatcher.get().unwrap();
        dispatcher.dispatch(Command { tab, id, request });
        id
    }

    /// Keeps `message`, as a reply or a notice.
    fn keep(&mut self, message: Message) {
        self.log.push(message.clone());
        match message {
            Message::Reply(reply) => self.early.push(reply),
            Message::Notice(notice) => self.notices.push_back(notice),
        }
    }

    /// Keeps the next message that comes, within the wait of the page.
    async fn take(&mut self) {
        let message = tokio::time::timeout(self.wait, self.messages.recv());
        let message = message.await.expect("Nothing came").unwrap();
        self.keep(message);
    }

    /// The next reply that comes, or the first that came early.
    async fn next(&mut self) -> Reply {
        while self.early.is_empty() {
            self.take().await;
        }
        self.early.remove(0)
    }

    /// The reply to the command `id`, keeping the others that come before it.
    async fn reply(&mut self, id: u64) -> Reply {
        loop {
            if let Some(index) = self.early.iter().position(|reply| reply.id == id) {
                return self.early.remove(index);
            }
            self.take().await;
        }
    }

    /// The next notice that comes, or the first that came and was not taken.
    async fn notice(&mut self) -> Notice {
        loop {
            if let Some(notice) = self.notices.pop_front() {
                return notice;
            }
            self.take().await;
        }
    }

    /// What `request` from `tab` gives, once its reply, to `tab`, comes.
    async fn call(&mut self, tab: u64, request: Request) -> Result<Answer, ClientError> {
        let id = self.send(tab, request);
        let reply = self.reply(id).await;
        assert_eq!(reply.tab, tab, "{reply:?}");
        reply.result
    }

    /// Keeps what came, or comes within `wait`.
    async fn settle(&mut self, wait: Duration) {
        tokio::time::sleep(wait).await;
        while let Ok(message) = self.messages.try_recv() {
            self.keep(message);
        }
    }

    /// Whether the reply to the command `id` came already, keeping what came.
    fn has_replied(&mut self, id: u64) -> bool {
        while let Ok(message) = self.messages.try_recv() {
            self.keep(message);
        }
        self.early.iter().any(|reply| reply.id == id)
    }

    /// Whether no reply nor notice came, nor comes within 100 ms.
    async fn quiet(&mut self) -> bool {
        self.settle(Duration::from_millis(100)).await;
        self.early.is_empty() && self.notices.is_empty()
    }

    /// Whether no notice came, nor comes within 100 ms, keeping the replies that come.
    async fn no_notice(&mut self) -> bool {
        self.settle(Duration::from_millis(100)).await;
        self.notices.is_empty()
    }

    /// Where in the log the reply to the command `id` is.
    fn logged_reply(&self, id: u64) -> usize {
        let reply = |message: &Message| matches!(message, Message::Reply(reply) if reply.id == id);
        self.log.iter().position(reply).expect("No reply")
    }

    /// Where in the log `notice` is.
    fn logged_notice(&self, notice: &Notice) -> usize {
        let wanted = Message::Notice(notice.clone());
        self.log
            .iter()
            .position(|message| *message == wanted)
            .expect("No notice")
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

/// A page whose dispatcher opened the client of ana over `dir` with `platform`.
async fn opened_with(platform: &TestPlatform, dir: &Path) -> Page {
    let mut page = Page::new(platform);
    let client_id = page.call(TAB, open(dir)).await;
    assert_eq!(
        client_id,
        Ok(Answer::ClientId(platform.opened().client_id()))
    );
    page
}

/// A page whose dispatcher opened the client of ana over `dir`, through a transport to `server`,
/// with its platform and the transport.
async fn opened(server: &TestServer, dir: &Path) -> (Page, TestPlatform, TestTransport) {
    let transport = server.transport();
    let platform = TestPlatform::new(&transport);
    let page = opened_with(&platform, dir).await;
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
    // Each replies while the slow open is still held, and the notice waits for it.
    for id in calls {
        assert_eq!(page.reply(id).await.result, Ok(Answer::Done), "{id}");
    }
    assert!(!page.has_replied(left));
    held.release();
    assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    assert_eq!(page.reply(left).await.result, Ok(Answer::Done));
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
        // Each replies while the slow open is still held, and the notice waits for it.
        for id in opens {
            let reply = page.reply(id).await;
            assert_eq!(reply.result, Ok(Answer::Done), "round {round}: {id}");
        }
        assert!(!page.has_replied(left), "round {round}");
        held.release();
        assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
        assert_eq!(page.reply(left).await.result, Ok(Answer::Done));
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
    // Each replies while the close is still held.
    for id in calls {
        let reply = page.reply(id).await;
        assert!(reply.result.is_ok(), "{reply:?}");
    }
    assert!(!page.has_replied(close));
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
    async fn open(
        &self,
        config: DispatchConfig,
        tokens: PageTokens,
    ) -> Result<ZemdbClient, ClientError> {
        if !self
            .panicked
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("The platform panics");
        }
        self.inner.open(config, tokens).await
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
    let publish = move |message| {
        if let Message::Reply(reply) = message {
            drop(sender.send(reply));
        }
    };
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
// The subscriptions

fn watch(room_id: &RoomId, what: Watch) -> Request {
    Request::Watch(notes_of(room_id), what)
}

/// The id of the subscription that `result`, the reply to a `Watch`, gives.
fn subscription(result: Result<Answer, ClientError>) -> u64 {
    match result {
        Ok(Answer::Subscription(subscription)) => subscription,
        result => panic!("{result:?}"),
    }
}

/// Subscribes `tab` to `what` of the notes of `room_id`, and gives the id of the subscription.
async fn subscribe(page: &mut Page, tab: u64, room_id: &RoomId, what: Watch) -> u64 {
    subscription(page.call(tab, watch(room_id, what)).await)
}

/// The notice of `value`, of `subscription` of `tab`.
fn value_of(tab: u64, subscription: u64, value: Answer) -> Notice {
    Notice::Value {
        tab,
        subscription,
        value: Ok(value),
    }
}

fn ended(tab: u64, subscription: u64) -> Notice {
    Notice::Ended { tab, subscription }
}

async fn confirm(page: &mut Page, tab: u64, subscription: u64) {
    let confirmed = page.call(tab, Request::Confirm(subscription)).await;
    assert_eq!(confirmed, Ok(Answer::Done));
}

async fn unwatch(page: &mut Page, tab: u64, subscription: u64) {
    let unwatched = page.call(tab, Request::Unwatch(subscription)).await;
    assert_eq!(unwatched, Ok(Answer::Done));
}

/// Inserts the note `number` of ana, with `text`, from `tab`, and gives it as a read gives it.
async fn insert(page: &mut Page, tab: u64, room_id: &RoomId, number: i64, text: &str) -> Row {
    let row = TableCall::Insert(new_note(number, text));
    let written = page.call(tab, on_notes(room_id, row)).await;
    assert_eq!(written, Ok(Answer::Key(key(number, "ana"))));
    note(number, "ana", text)
}

/// How many subscriptions `room_id` has, as its status tells.
async fn watches(page: &mut Page, room_id: &RoomId) -> usize {
    status_of(page, room_id).await.watches
}

#[tokio::test]
async fn a_subscription_has_one_value_in_flight_until_its_tab_confirms_it_and_then_the_last() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    let bob = note(1, "bob", "Hello");
    // Its reply comes first, and then its first value: the current one.
    let id = page.send(TAB, watch(&room_id, Watch::Rows(Scan::new())));
    let all = subscription(page.reply(id).await.result);
    let first = value_of(TAB, all, Answer::Rows(vec![bob.clone()]));
    assert_eq!(page.notice().await, first);
    assert!(page.logged_reply(id) < page.logged_notice(&first));

    // Many changes without a confirmation: no value comes, and its stream keeps the last one,
    // which comes once the value in flight is confirmed.
    let mut rows = vec![bob.clone()];
    for number in 2..=6 {
        rows.push(insert(&mut page, TAB, &room_id, number, &format!("Note {number}")).await);
    }
    assert!(page.no_notice().await);
    assert_eq!(watches(&mut page, &room_id).await, 1);
    confirm(&mut page, TAB, all).await;
    let last = value_of(TAB, all, Answer::Rows(rows.clone()));
    assert_eq!(page.notice().await, last);
    // Confirmed with nothing changed, nothing comes; the next change comes once confirmed.
    confirm(&mut page, TAB, all).await;
    assert!(page.no_notice().await);
    rows.push(insert(&mut page, TAB, &room_id, 7, "Note 7").await);
    let last = value_of(TAB, all, Answer::Rows(rows.clone()));
    assert_eq!(page.notice().await, last);

    // `Unwatch` ends it, with its value in flight: nothing of it comes afterwards, and the room
    // has no subscription.
    unwatch(&mut page, TAB, all).await;
    assert_eq!(watches(&mut page, &room_id).await, 0);
    confirm(&mut page, TAB, all).await;
    insert(&mut page, TAB, &room_id, 8, "Note 8").await;
    assert!(page.no_notice().await);
    unwatch(&mut page, TAB, all).await;

    // The close of its room ends a subscription at once, and one with its value in flight once
    // it is confirmed; then it changes nothing.
    let watch_bob = Watch::Row(key(1, "bob"));
    let one = subscribe(&mut page, TAB, &room_id, watch_bob).await;
    let value = value_of(TAB, one, Answer::Row(Some(bob.clone())));
    assert_eq!(page.notice().await, value);
    confirm(&mut page, TAB, one).await;
    let two = subscribe(&mut page, TAB, &room_id, Watch::Rows(Scan::new().limit(1))).await;
    let value = value_of(TAB, two, Answer::Rows(vec![bob.clone()]));
    assert_eq!(page.notice().await, value);
    let closed = page.call(TAB, on_room(&room_id, RoomCall::Close)).await;
    assert_eq!(closed, Ok(Answer::Done));
    assert_eq!(page.notice().await, ended(TAB, one));
    assert!(page.no_notice().await);
    confirm(&mut page, TAB, two).await;
    assert_eq!(page.notice().await, ended(TAB, two));
    confirm(&mut page, TAB, two).await;
    unwatch(&mut page, TAB, two).await;
    assert!(page.no_notice().await);
    // Opened again, the room has none.
    open_room(&mut page, TAB, &room_id).await;
    assert_eq!(watches(&mut page, &room_id).await, 0);
}

#[tokio::test]
async fn a_confirmation_without_a_value_in_flight_is_not_kept_for_the_next_value() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    let all = subscribe(&mut page, TAB, &room_id, Watch::Rows(Scan::new())).await;
    let bob = note(1, "bob", "Hello");
    let first = value_of(TAB, all, Answer::Rows(vec![bob.clone()]));
    assert_eq!(page.notice().await, first);
    // Two confirmations: the second finds no value in flight.
    confirm(&mut page, TAB, all).await;
    confirm(&mut page, TAB, all).await;
    let world = insert(&mut page, TAB, &room_id, 2, "World").await;
    let next = value_of(TAB, all, Answer::Rows(vec![bob, world]));
    assert_eq!(page.notice().await, next);
    // That value waits for its own confirmation.
    insert(&mut page, TAB, &room_id, 3, "Again").await;
    assert!(page.no_notice().await);
}

#[tokio::test]
async fn an_unwatch_or_a_left_ends_a_subscription_without_a_value_in_flight() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    // Subscriptions of tabs 1 and 2, whose first values were confirmed: they wait for a change.
    let bob = Answer::Rows(vec![note(1, "bob", "Hello")]);
    let mut subscriptions = Vec::new();
    for tab in [1, 2] {
        let subscription = subscribe(&mut page, tab, &room_id, Watch::Rows(Scan::new())).await;
        let value = value_of(tab, subscription, bob.clone());
        assert_eq!(page.notice().await, value);
        confirm(&mut page, tab, subscription).await;
        subscriptions.push(subscription);
    }
    assert_eq!(watches(&mut page, &room_id).await, 2);
    // With no change in the room, the `Unwatch` of tab 1 and the notice that tab 2 left end them.
    unwatch(&mut page, 1, subscriptions[0]).await;
    assert_eq!(page.call(2, Request::Left).await, Ok(Answer::Done));
    assert_eq!(watches(&mut page, &room_id).await, 0);
    assert!(page.no_notice().await);
}

#[tokio::test]
async fn a_tab_that_leaves_during_the_shutdown_does_not_wait_for_its_close() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, platform, _) = opened(&server, dir.path()).await;
    open_room(&mut page, 1, &room_id).await;
    let all = subscribe(&mut page, 1, &room_id, Watch::Rows(Scan::new())).await;
    let bob = Answer::Rows(vec![note(1, "bob", "Hello")]);
    assert_eq!(page.notice().await, value_of(1, all, bob));
    // The shutdown closes the room, and its close in the storage engine is held.
    let mut closing = platform.engine().hold_next("close_room");
    let shutdown = page.send(TAB, Request::Shutdown);
    closing.replied().await;
    // The tab that holds the room leaves: the notice ends its subscription, and closes nothing,
    // so it does not wait for the close of the shutdown.
    assert_eq!(page.call(1, Request::Left).await, Ok(Answer::Done));
    assert!(!page.has_replied(shutdown));
    // Its subscription ended: confirmed now, its value in flight brings no end of its stream.
    confirm(&mut page, 1, all).await;
    closing.release();
    assert_eq!(page.reply(shutdown).await.result, Ok(Answer::Done));
    assert!(page.no_notice().await);
}

#[tokio::test]
async fn a_tab_that_left_during_the_shutdown_gets_no_request_of_a_token_of_it() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let platform = TestPlatform::asking_the_page(&transport);
    let mut page = opened_with(&platform, dir.path()).await;
    let client_id = platform.opened().client_id();
    // Tab 1 opens the room, and tab 2 opens it too, with a token that expired, which becomes the
    // token of the room.
    for (tab, given) in [
        (1, token(&client_id, &room_id)),
        (2, expired(&client_id, &room_id)),
    ] {
        let opening = page.send(tab, on_room(&room_id, RoomCall::Open));
        let (asked_tab, request, _) = asked(&mut page, &client_id).await;
        assert_eq!(asked_tab, tab);
        answer(&mut page, tab, request, &given).await;
        assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    }
    // Tab 2 writes, and the refusal of its commit for its token is held; the shutdown comes, with
    // its close of the room in the storage engine held; and tab 1 leaves.
    let mut refused = transport.hold_next_reply(ClientMessageKind::Commit);
    let late = TableCall::Insert(new_note(3, "Late"));
    let writing = page.send(2, on_notes(&room_id, late));
    refused.replied().await;
    let mut closing = platform.engine().hold_next("close_room");
    let shutdown = page.send(TAB, Request::Shutdown);
    closing.replied().await;
    assert_eq!(page.call(1, Request::Left).await, Ok(Answer::Done));
    // The refusal comes: the renewal of the write goes to tab 2, which still holds the room, and
    // not to tab 1, which left.
    refused.release();
    let (asked_tab, request, _) = asked(&mut page, &client_id).await;
    assert_eq!(asked_tab, 2);
    let unanswered = page.call(2, Request::Token(request, None)).await;
    assert_eq!(unanswered, Ok(Answer::Done));
    closing.release();
    assert_eq!(page.reply(shutdown).await.result, Ok(Answer::Done));
    let written = page.reply(writing).await.result;
    assert!(written.is_err(), "{written:?}");
}

#[tokio::test]
async fn a_watch_gives_what_the_facade_gives_for_a_room_or_a_table_it_cannot_read() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let platform = TestPlatform::new(&server.transport());
    let mut page = Page::new(&platform);
    let all = || watch(&room_id, Watch::Rows(Scan::new()));
    let closed = Err(ClientError::RoomClosed(room_id.clone()));
    // Before the open, and in a room that is not open, `RoomClosed`.
    assert_eq!(page.call(TAB, all()).await, closed);
    let opened = page.call(TAB, open(dir.path())).await;
    assert!(matches!(opened, Ok(Answer::ClientId(_))), "{opened:?}");
    assert_eq!(page.call(TAB, all()).await, closed);
    open_room(&mut page, TAB, &room_id).await;
    // A table that the schema does not have is `InvalidOperation`, and a key that the schema
    // does not take is a read that fails: a value of the subscription.
    let nope = Table {
        room_id: room_id.clone(),
        name: "nope".to_string(),
    };
    let watched = page.call(TAB, Request::Watch(nope, Watch::Rows(Scan::new())));
    let watched = watched.await;
    assert!(
        matches!(watched, Err(ClientError::InvalidOperation(_))),
        "{watched:?}"
    );
    let mood = Key::new().with("mood", "glad");
    let wrong = subscribe(&mut page, TAB, &room_id, Watch::Row(mood)).await;
    let notice = page.notice().await;
    let Notice::Value {
        tab: TAB,
        subscription,
        value: Err(ClientError::InvalidOperation(_)),
    } = notice
    else {
        panic!("{notice:?}");
    };
    assert_eq!(subscription, wrong);
    assert_eq!(watches(&mut page, &room_id).await, 1);
}

#[tokio::test]
async fn a_shutdown_ends_the_subscriptions_with_their_streams() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    let bob = note(1, "bob", "Hello");
    let confirmed = subscribe(&mut page, TAB, &room_id, Watch::Row(key(1, "bob"))).await;
    let value = value_of(TAB, confirmed, Answer::Row(Some(bob.clone())));
    assert_eq!(page.notice().await, value);
    confirm(&mut page, TAB, confirmed).await;
    let in_flight = subscribe(&mut page, TAB, &room_id, Watch::Rows(Scan::new())).await;
    let value = value_of(TAB, in_flight, Answer::Rows(vec![bob]));
    assert_eq!(page.notice().await, value);
    assert_eq!(page.call(TAB, Request::Shutdown).await, Ok(Answer::Done));
    assert_eq!(page.notice().await, ended(TAB, confirmed));
    assert!(page.no_notice().await);
    confirm(&mut page, TAB, in_flight).await;
    assert_eq!(page.notice().await, ended(TAB, in_flight));
    // Afterwards, a watch is `RoomClosed`.
    let watched = page
        .call(TAB, watch(&room_id, Watch::Rows(Scan::new())))
        .await;
    assert_eq!(watched, Err(ClientError::RoomClosed(room_id.clone())));
}

#[tokio::test]
async fn a_discard_on_request_ends_the_subscriptions_of_its_room() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    let bob = note(1, "bob", "Hello");
    let world = insert(&mut page, TAB, &room_id, 2, "World").await;
    let one = subscribe(&mut page, TAB, &room_id, Watch::Row(key(2, "ana"))).await;
    let value = value_of(TAB, one, Answer::Row(Some(world.clone())));
    assert_eq!(page.notice().await, value);
    confirm(&mut page, TAB, one).await;
    let two = subscribe(&mut page, 2, &room_id, Watch::Rows(Scan::new())).await;
    let value = value_of(2, two, Answer::Rows(vec![bob.clone(), world]));
    assert_eq!(page.notice().await, value);
    // Another tab discards the room: each subscription ends, the one with its value in flight
    // once it is confirmed.
    let discarded = page.call(3, on_room(&room_id, RoomCall::Discard)).await;
    assert_eq!(discarded, Ok(Answer::Done));
    assert_eq!(page.notice().await, ended(TAB, one));
    assert!(page.no_notice().await);
    confirm(&mut page, 2, two).await;
    assert_eq!(page.notice().await, ended(2, two));
    // Opened again, as new, the room has none, and a new one reads the replica of the server.
    open_room(&mut page, TAB, &room_id).await;
    assert_eq!(watches(&mut page, &room_id).await, 0);
    let three = subscribe(&mut page, TAB, &room_id, Watch::Rows(Scan::new())).await;
    let rows = vec![bob, note(2, "ana", "World")];
    assert_eq!(
        page.notice().await,
        value_of(TAB, three, Answer::Rows(rows))
    );
}

/// The subscription of `notice`, a value.
fn subscription_of(notice: &Notice) -> u64 {
    match notice {
        Notice::Value { subscription, .. } => *subscription,
        notice => panic!("{notice:?}"),
    }
}

#[tokio::test]
async fn each_value_goes_to_the_tab_of_its_subscription_and_a_tab_that_left_ends_only_its_own() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    let (one, two, three) = (1, 2, 3);
    open_room(&mut page, one, &room_id).await;
    open_room(&mut page, two, &room_id).await;
    // Tabs 1 and 2 watch the same row, and tab 3, which did not open the room, every row.
    let ana = Watch::Row(key(2, "ana"));
    let first = subscribe(&mut page, one, &room_id, ana.clone()).await;
    assert_eq!(page.notice().await, value_of(one, first, Answer::Row(None)));
    let second = subscribe(&mut page, two, &room_id, ana).await;
    assert_eq!(
        page.notice().await,
        value_of(two, second, Answer::Row(None))
    );
    let third = subscribe(&mut page, three, &room_id, Watch::Rows(Scan::new())).await;
    let bob = note(1, "bob", "Hello");
    let value = value_of(three, third, Answer::Rows(vec![bob.clone()]));
    assert_eq!(page.notice().await, value);
    assert!(first != second && second != third && first != third);
    for (tab, subscription) in [(one, first), (two, second), (three, third)] {
        confirm(&mut page, tab, subscription).await;
    }
    // A change: each subscription gets its value, at its own tab.
    let world = insert(&mut page, two, &room_id, 2, "World").await;
    let mut values = Vec::new();
    for _ in 0..3 {
        values.push(page.notice().await);
    }
    values.sort_by_key(subscription_of);
    let row = Answer::Row(Some(world.clone()));
    let mut expected = vec![
        value_of(one, first, row.clone()),
        value_of(two, second, row),
        value_of(three, third, Answer::Rows(vec![bob.clone(), world])),
    ];
    expected.sort_by_key(subscription_of);
    assert_eq!(values, expected);
    // The confirmation and the `Unwatch` of a subscription of another tab change nothing.
    confirm(&mut page, two, first).await;
    unwatch(&mut page, two, first).await;
    let bye = Row::new().with("text", "Bye");
    let updated = page
        .call(
            two,
            on_notes(&room_id, TableCall::Update(key(2, "ana"), bye)),
        )
        .await;
    assert_eq!(updated, Ok(Answer::Done));
    assert!(page.no_notice().await);
    assert_eq!(watches(&mut page, &room_id).await, 3);

    // Tab 1 leaves: its subscription ends, and those of the others do not. The room stays open:
    // tab 2 opened it too.
    assert_eq!(page.call(one, Request::Left).await, Ok(Answer::Done));
    assert_eq!(watches(&mut page, &room_id).await, 2);
    confirm(&mut page, one, first).await;
    confirm(&mut page, two, second).await;
    let bye = note(2, "ana", "Bye");
    let value = value_of(two, second, Answer::Row(Some(bye.clone())));
    assert_eq!(page.notice().await, value);
    // Tab 3 leaves, with its value in flight: its subscription ends too, though it did not open
    // the room.
    assert_eq!(page.call(three, Request::Left).await, Ok(Answer::Done));
    assert_eq!(watches(&mut page, &room_id).await, 1);
    confirm(&mut page, three, third).await;
    assert!(page.no_notice().await);
    // Tab 2 leaves: the room closes, and its subscription ends with nothing to tab 2.
    assert_eq!(page.call(two, Request::Left).await, Ok(Answer::Done));
    assert!(!is_open(&mut page, &room_id).await);
    assert!(page.no_notice().await);
}

#[tokio::test]
async fn in_periodic_the_worker_is_live_while_the_room_has_a_subscription_of_the_dispatcher() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, _, _) = opened(&server, dir.path()).await;
    let periodic = RoomCall::SetFreshness(Freshness::Periodic);
    assert_eq!(
        page.call(TAB, on_room(&room_id, periodic)).await,
        Ok(Answer::Done)
    );
    open_room(&mut page, TAB, &room_id).await;
    async fn mode(page: &mut Page, room_id: &RoomId) -> Freshness {
        status_of(page, room_id).await.worker.unwrap().freshness
    }
    assert_eq!(mode(&mut page, &room_id).await, Freshness::Periodic);
    // The first subscription asks the worker for `Live`; ending one of two leaves it in `Live`,
    // and ending the last, with the tab that left, asks for `Periodic`.
    let first = subscribe(&mut page, 1, &room_id, Watch::Rows(Scan::new())).await;
    assert_eq!(subscription_of(&page.notice().await), first);
    assert_eq!(mode(&mut page, &room_id).await, Freshness::Live);
    let second = subscribe(&mut page, 2, &room_id, Watch::Row(key(1, "bob"))).await;
    assert_eq!(subscription_of(&page.notice().await), second);
    unwatch(&mut page, 1, first).await;
    assert_eq!(mode(&mut page, &room_id).await, Freshness::Live);
    assert_eq!(page.call(2, Request::Left).await, Ok(Answer::Done));
    assert_eq!(mode(&mut page, &room_id).await, Freshness::Periodic);
    // One that ends with its room, which opens again in `Periodic`.
    let third = subscribe(&mut page, 1, &room_id, Watch::Rows(Scan::new())).await;
    assert_eq!(subscription_of(&page.notice().await), third);
    assert_eq!(mode(&mut page, &room_id).await, Freshness::Live);
    confirm(&mut page, 1, third).await;
    let closed = page.call(TAB, on_room(&room_id, RoomCall::Close)).await;
    assert_eq!(closed, Ok(Answer::Done));
    assert_eq!(page.notice().await, ended(1, third));
    open_room(&mut page, TAB, &room_id).await;
    assert_eq!(mode(&mut page, &room_id).await, Freshness::Periodic);
}

#[tokio::test]
async fn dropping_the_dispatcher_ends_its_subscriptions() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let (mut page, platform, _) = opened(&server, dir.path()).await;
    open_room(&mut page, TAB, &room_id).await;
    let confirmed = subscribe(&mut page, TAB, &room_id, Watch::Row(key(1, "bob"))).await;
    page.notice().await;
    confirm(&mut page, TAB, confirmed).await;
    subscribe(&mut page, TAB, &room_id, Watch::Rows(Scan::new())).await;
    page.notice().await;
    assert_eq!(watches(&mut page, &room_id).await, 2);
    drop(page);
    let room = platform.opened().room(room_id.clone()).await.unwrap();
    let start = Instant::now();
    while room.status().await.unwrap().watches > 0 {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "The subscriptions stay"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_that_answers_from_inside_what_publishes_gets_its_tokens_and_the_last_value() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let platform = TestPlatform::asking_the_page(&server.transport());
    // The page confirms each value, and answers each request of a token, from inside the function
    // that publishes it.
    let hook = |dispatcher: &Dispatcher, message: &Message| {
        let (tab, request) = match message {
            Message::Notice(Notice::Value {
                tab, subscription, ..
            }) => (*tab, Request::Confirm(*subscription)),
            Message::Notice(Notice::Token {
                tab,
                request,
                client_id,
                room_id,
            }) => (
                *tab,
                Request::Token(*request, Some(token(client_id, room_id))),
            ),
            _ => return,
        };
        let id = 0;
        dispatcher.dispatch(Command { tab, id, request });
    };
    let mut page = Page::with(&platform, hook);
    let opened = page.call(TAB, open(dir.path())).await;
    assert!(matches!(opened, Ok(Answer::ClientId(_))), "{opened:?}");
    open_room(&mut page, TAB, &room_id).await;
    let all = subscribe(&mut page, TAB, &room_id, Watch::Rows(Scan::new())).await;
    // Writes one after another, while the values come and are confirmed.
    let mut rows = vec![note(1, "bob", "Hello")];
    for number in 2..=40 {
        rows.push(insert(&mut page, TAB, &room_id, number, &format!("Note {number}")).await);
    }
    // Once the replica is still, the last value is the last state, and each value is a state
    // after the one before.
    let last = value_of(TAB, all, Answer::Rows(rows));
    while page.notices.back() != Some(&last) {
        page.take().await;
    }
    let counts = page.notices.iter().map(|notice| match notice {
        Notice::Value {
            tab: TAB,
            subscription,
            value: Ok(Answer::Rows(rows)),
        } if *subscription == all => rows.len(),
        Notice::Token { .. } => 0,
        notice => panic!("{notice:?}"),
    });
    let counts = Vec::from_iter(counts.filter(|count| *count > 0));
    assert!(
        counts.windows(2).all(|pair| pair[0] < pair[1]),
        "{counts:?}"
    );
    assert_eq!(counts.last(), Some(&40));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_page_gets_the_reply_to_a_watch_first_and_nothing_after_its_unwatch_or_its_left() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let platform = TestPlatform::new(&server.transport());
    // The page takes a while with the reply to a watch, longer than with a value, which it tells.
    let (publishing, mut published) = mpsc::unbounded_channel();
    let hook = move |_: &Dispatcher, message: &Message| match message {
        Message::Reply(Reply {
            result: Ok(Answer::Subscription(_)),
            ..
        }) => std::thread::sleep(Duration::from_millis(300)),
        Message::Notice(Notice::Value { subscription, .. }) => {
            let _ = publishing.send(*subscription);
            std::thread::sleep(Duration::from_millis(100));
        }
        _ => {}
    };
    let mut page = Page::with(&platform, hook);
    let opened = page.call(TAB, open(dir.path())).await;
    assert!(matches!(opened, Ok(Answer::ClientId(_))), "{opened:?}");
    open_room(&mut page, TAB, &room_id).await;
    // How the tab ends the subscription: with `Unwatch` (`None`), with the notice that it left, or
    // with two of those at once.
    let rounds = [
        (TAB, vec![None]),
        (2, vec![Some(Request::Left)]),
        (3, vec![Some(Request::Left)]),
        (4, vec![None, Some(Request::Left)]),
        (5, vec![None, None]),
        (6, vec![Some(Request::Left), Some(Request::Left)]),
        (8, vec![Some(Request::Left), None]),
    ];
    for (tab, endings) in rounds {
        // The first value comes once the page took the reply.
        let id = page.send(tab, watch(&room_id, Watch::Rows(Scan::new())));
        let subscription = subscription(page.reply(id).await.result);
        // The tab ends it while the page takes its value: the value comes before each reply.
        let publishing = tokio::time::timeout(Duration::from_secs(30), published.recv());
        assert_eq!(publishing.await.unwrap(), Some(subscription));
        let ended = Vec::from_iter(endings.into_iter().map(|ending| {
            let ending = ending.unwrap_or(Request::Unwatch(subscription));
            page.send(tab, ending)
        }));
        for id in &ended {
            assert_eq!(page.reply(*id).await.result, Ok(Answer::Done), "{tab}");
        }
        // Once they replied, the subscription and its stream are out of the room.
        assert_eq!(watches(&mut page, &room_id).await, 0, "{tab}");
        page.settle(Duration::from_millis(200)).await;
        let rows = Answer::Rows(vec![note(1, "bob", "Hello")]);
        let value = page.logged_notice(&value_of(tab, subscription, rows));
        assert!(page.logged_reply(id) < value, "{tab}");
        for id in ended {
            assert!(value < page.logged_reply(id), "{tab}: {id}");
        }
        page.notices.clear();
    }
    assert_eq!(watches(&mut page, &room_id).await, 0);
}

#[tokio::test]
async fn a_page_that_unwatches_from_inside_the_reply_to_its_watch_ends_the_subscription() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let platform = TestPlatform::new(&server.transport());
    // Tab 2 ends each of its subscriptions as soon as it takes the reply that gives its id.
    let hook = |dispatcher: &Dispatcher, message: &Message| {
        if let Message::Reply(Reply {
            tab: 2,
            result: Ok(Answer::Subscription(subscription)),
            ..
        }) = message
        {
            let request = Request::Unwatch(*subscription);
            let id = 1000;
            dispatcher.dispatch(Command {
                tab: 2,
                id,
                request,
            });
        }
    };
    let mut page = Page::with(&platform, hook);
    let opened = page.call(TAB, open(dir.path())).await;
    assert!(matches!(opened, Ok(Answer::ClientId(_))), "{opened:?}");
    open_room(&mut page, TAB, &room_id).await;
    subscribe(&mut page, 2, &room_id, Watch::Rows(Scan::new())).await;
    assert_eq!(page.reply(1000).await.result, Ok(Answer::Done));
    insert(&mut page, TAB, &room_id, 2, "World").await;
    assert!(page.no_notice().await);
    assert_eq!(watches(&mut page, &room_id).await, 0);
}

// ---------------------------------------------------------------------------------------------
// The tokens

/// The tab of the next request of a token that comes, and its id and its room, which is of the
/// client.
async fn asked(page: &mut Page, client: &ClientId) -> (u64, u64, RoomId) {
    match page.notice().await {
        Notice::Token {
            tab,
            request,
            client_id,
            room_id,
        } if client_id == *client => (tab, request, room_id),
        notice => panic!("{notice:?}"),
    }
}

/// Answers the request `request` from `tab` with `token`.
async fn answer(page: &mut Page, tab: u64, request: u64, token: &str) {
    let answered = page.call(tab, Request::Token(request, Some(token.to_string())));
    assert_eq!(answered.await, Ok(Answer::Done));
}

/// A page whose dispatcher opened over `dir` the client of ana, which takes its tokens from the
/// page, through a transport to `server`, with its transport and its client id.
async fn asking(server: &TestServer, dir: &Path) -> (Page, TestTransport, ClientId) {
    let transport = server.transport();
    let platform = TestPlatform::asking_the_page(&transport);
    let mut page = opened_with(&platform, dir).await;
    // The waits of the client, on the paused clock.
    page.wait = Duration::from_secs(3600);
    (page, transport, platform.opened().client_id())
}

/// A token of `client_id` for `room_id` that already expired, in the format of the server.
fn expired(client_id: &ClientId, room_id: &RoomId) -> String {
    generate_client_token(client_id, room_id, Duration::ZERO, AUTH_SECRET)
}

#[tokio::test(start_paused = true)]
async fn with_the_token_of_its_tab_a_room_opens_and_without_one_in_time_it_opens_as_without_one() {
    let (server, room_id) = notes().await;
    let other = notes_in(&server, "room-b").await;
    let dir = tempdir().unwrap();
    let (mut page, transport, client_id) = asking(&server, dir.path()).await;
    // The open asks its tab, and the room opens with the token that the tab gives, which its
    // registration carries.
    let opening = page.send(1, on_room(&room_id, RoomCall::Open));
    let (tab, request, asked_for) = asked(&mut page, &client_id).await;
    assert_eq!((tab, &asked_for), (1, &room_id));
    let given = token(&client_id, &room_id);
    answer(&mut page, 1, request, &given).await;
    assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    assert!(status_of(&mut page, &room_id).await.registered);
    let registered = (ClientMessageKind::RegisterClient, given);
    assert_eq!(transport.bearers(), [registered]);

    // Without an answer within the 60 s of the client, the open goes on as without a token: its
    // registration goes without one, and is refused for it, which asks the tab once more.
    let start = Instant::now();
    let opening = page.send(1, on_room(&other, RoomCall::Open));
    let (tab, first, asked_for) = asked(&mut page, &client_id).await;
    assert_eq!((tab, &asked_for), (1, &other));
    let (tab, second, asked_for) = asked(&mut page, &client_id).await;
    assert_eq!((tab, &asked_for), (1, &other));
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    assert_ne!(first, second);
    // An answer that comes late changes nothing, and a room without metadata does not open.
    answer(&mut page, 1, first, &token(&client_id, &other)).await;
    let opened = page.reply(opening).await.result;
    let waited = start.elapsed();
    let sent = transport.bearers()[1..].to_vec();
    assert_eq!(sent, [(ClientMessageKind::RegisterClient, String::new())]);
    // As with a token provider that does not answer, in the facade.
    let twin_dir = tempdir().unwrap();
    let (twin, tokens) = (server.transport(), TestTokens::issuing(Issue::Hang));
    let metadata = FileMetadata::new(twin_dir.path());
    let storage = engine(twin_dir.path());
    let facade = ZemdbClient::open(manual("ana"), storage, metadata, twin.clone(), tokens, None);
    let facade = facade.await.unwrap();
    twin.hush(&facade.client_id());
    let start = Instant::now();
    let room = facade.room(other.clone()).await;
    assert_eq!(opened, room.map(|_| Answer::Done));
    assert!(matches!(opened, Err(ClientError::Network(_))), "{opened:?}");
    assert_eq!(start.elapsed(), waited);
    assert_eq!(twin.bearers(), sent);
}

#[tokio::test(start_paused = true)]
async fn a_request_of_a_token_goes_to_the_tab_that_opens_its_room_or_else_to_one_that_holds_it() {
    let server = TestServer::new();
    let [a, b, c, d] = [
        notes_in(&server, "room-a").await,
        notes_in(&server, "room-b").await,
        notes_in(&server, "room-c").await,
        notes_in(&server, "room-d").await,
    ];
    let dir = tempdir().unwrap();
    let (mut page, transport, client_id) = asking(&server, dir.path()).await;
    // Each open asks its own tab, and the answer of another tab changes nothing: tab 1 opens A,
    // and tab 2 opens B, and A too.
    for (tab, room_id) in [(1, &a), (2, &b), (2, &a)] {
        let opening = page.send(tab, on_room(room_id, RoomCall::Open));
        let (asked_tab, request, asked_for) = asked(&mut page, &client_id).await;
        assert_eq!((asked_tab, asked_for), (tab, room_id.clone()));
        let given = token(&client_id, room_id);
        answer(&mut page, 3 - tab, request, &given).await;
        assert!(page.quiet().await, "{room_id}");
        answer(&mut page, tab, request, &given).await;
        assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    }
    // A renewal out of an open goes to the first of the tabs that opened the room: tab 2 opens A
    // again with a token that expired, which becomes its token, and its write is refused for it.
    let opening = page.send(2, on_room(&a, RoomCall::Open));
    let (asked_tab, request, _) = asked(&mut page, &client_id).await;
    assert_eq!(asked_tab, 2);
    answer(&mut page, 2, request, &expired(&client_id, &a)).await;
    assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    let row = TableCall::Insert(new_note(2, "World"));
    let writing = page.send(2, on_notes(&a, row));
    let (asked_tab, request, asked_for) = asked(&mut page, &client_id).await;
    assert_eq!((asked_tab, asked_for), (1, a.clone()));
    answer(&mut page, 1, request, &token(&client_id, &a)).await;
    let written = page.reply(writing).await.result;
    assert_eq!(written, Ok(Answer::Key(key(2, "ana"))));

    // Two requests to the same tab at once, answered in the other order: each answer goes to its
    // request, and each room registers with its own token.
    let opening = [
        page.send(1, on_room(&c, RoomCall::Open)),
        page.send(1, on_room(&d, RoomCall::Open)),
    ];
    let mut requests = HashMap::new();
    for _ in 0..2 {
        let (asked_tab, request, asked_for) = asked(&mut page, &client_id).await;
        assert_eq!(asked_tab, 1);
        requests.insert(asked_for, request);
    }
    let (for_c, for_d) = (token(&client_id, &c), token(&client_id, &d));
    answer(&mut page, 1, requests[&d], &for_d).await;
    answer(&mut page, 1, requests[&c], &for_c).await;
    for id in opening {
        assert_eq!(page.reply(id).await.result, Ok(Answer::Done));
    }
    let bearers = transport.bearers();
    for given in [for_c, for_d] {
        let registered = (ClientMessageKind::RegisterClient, given);
        assert!(bearers.contains(&registered), "{bearers:?}");
    }
    assert!(page.no_notice().await);
}

#[tokio::test(start_paused = true)]
async fn a_tab_that_left_ends_the_requests_of_a_token_to_it_and_gets_no_more() {
    let server = TestServer::new();
    let [a, e] = [
        notes_in(&server, "room-a").await,
        notes_in(&server, "room-e").await,
    ];
    let dir = tempdir().unwrap();
    let (mut page, _, client_id) = asking(&server, dir.path()).await;
    for tab in [1, 2] {
        let opening = page.send(tab, on_room(&a, RoomCall::Open));
        let (_, request, _) = asked(&mut page, &client_id).await;
        answer(&mut page, tab, request, &token(&client_id, &a)).await;
        assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    }
    // Tab 1 opens E, whose request waits, and leaves: the request ends at once, and the open goes
    // on without a token; its registration, refused for it, has no tab left to ask, and E,
    // without metadata, does not open.
    let start = Instant::now();
    let opening = page.send(1, on_room(&e, RoomCall::Open));
    let (asked_tab, _, asked_for) = asked(&mut page, &client_id).await;
    assert_eq!((asked_tab, &asked_for), (1, &e));
    let left = page.send(1, Request::Left);
    let opened = page.reply(opening).await.result;
    assert!(matches!(opened, Err(ClientError::Network(_))), "{opened:?}");
    assert_eq!(page.reply(left).await.result, Ok(Answer::Done));
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );
    assert!(page.no_notice().await);
    // A stays open, of tab 2, and a renewal of its token goes to tab 2.
    let opening = page.send(2, on_room(&a, RoomCall::Open));
    let (asked_tab, request, _) = asked(&mut page, &client_id).await;
    assert_eq!(asked_tab, 2);
    answer(&mut page, 2, request, &expired(&client_id, &a)).await;
    assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    let row = TableCall::Insert(new_note(2, "World"));
    let writing = page.send(2, on_notes(&a, row));
    let (asked_tab, request, _) = asked(&mut page, &client_id).await;
    assert_eq!(asked_tab, 2);
    answer(&mut page, 2, request, &token(&client_id, &a)).await;
    let written = page.reply(writing).await.result;
    assert_eq!(written, Ok(Answer::Key(key(2, "ana"))));
    // Once its notice replied, tab 1 gets requests again: it opens A again, which tab 2 holds, and
    // the request of its open goes to it.
    let opening = page.send(1, on_room(&a, RoomCall::Open));
    let (asked_tab, request, _) = asked(&mut page, &client_id).await;
    assert_eq!(asked_tab, 1);
    answer(&mut page, 1, request, &token(&client_id, &a)).await;
    assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
}

#[tokio::test(start_paused = true)]
async fn the_renewal_of_an_open_goes_to_its_tab_while_an_open_of_another_tab_waits_for_its_turn() {
    let server = TestServer::new();
    let b = notes_in(&server, "room-b").await;
    let dir = tempdir().unwrap();
    let (mut page, _, client_id) = asking(&server, dir.path()).await;
    // Tab 1 opens B, and does not answer its request; tab 2 opens B meanwhile, after it.
    let start = Instant::now();
    let first = page.send(1, on_room(&b, RoomCall::Open));
    let (asked_tab, _, _) = asked(&mut page, &client_id).await;
    assert_eq!(asked_tab, 1);
    let second = page.send(2, on_room(&b, RoomCall::Open));
    // At 60 s, the registration of the first goes without a token, and its renewal asks tab 1,
    // whose open is still the one in the turn of the room.
    let (asked_tab, request, _) = asked(&mut page, &client_id).await;
    assert_eq!(asked_tab, 1);
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    answer(&mut page, 1, request, &token(&client_id, &b)).await;
    assert_eq!(page.reply(first).await.result, Ok(Answer::Done));
    // Then the open of tab 2 takes its turn, and asks tab 2.
    let (asked_tab, request, _) = asked(&mut page, &client_id).await;
    assert_eq!(asked_tab, 2);
    answer(&mut page, 2, request, &token(&client_id, &b)).await;
    assert_eq!(page.reply(second).await.result, Ok(Answer::Done));
}

#[tokio::test]
async fn a_request_of_a_token_during_the_shutdown_goes_to_a_tab_of_its_room() {
    let (server, room_id) = notes().await;
    let dir = tempdir().unwrap();
    let transport = server.transport();
    let platform = TestPlatform::asking_the_page(&transport);
    let mut page = opened_with(&platform, dir.path()).await;
    let client_id = platform.opened().client_id();
    // Tab 1 opens the room, and opens it again with a token that expired, which becomes its
    // token.
    for given in [token(&client_id, &room_id), expired(&client_id, &room_id)] {
        let opening = page.send(1, on_room(&room_id, RoomCall::Open));
        let (asked_tab, request, _) = asked(&mut page, &client_id).await;
        assert_eq!(asked_tab, 1);
        answer(&mut page, 1, request, &given).await;
        assert_eq!(page.reply(opening).await.result, Ok(Answer::Done));
    }
    // A write, whose commit is refused for its token, with the refusal held; and the shutdown,
    // whose close of the room in the storage engine is held.
    let mut refused = transport.hold_next_reply(ClientMessageKind::Commit);
    let late = TableCall::Insert(new_note(3, "Late"));
    let writing = page.send(1, on_notes(&room_id, late));
    refused.replied().await;
    let mut closing = platform.engine().hold_next("close_room");
    let shutdown = page.send(1, Request::Shutdown);
    closing.replied().await;
    // The refusal comes during the shutdown: the renewal of the write asks tab 1, which held the
    // room when the shutdown came.
    refused.release();
    let (asked_tab, request, asked_for) = asked(&mut page, &client_id).await;
    assert_eq!((asked_tab, asked_for), (1, room_id.clone()));
    let unanswered = page.call(1, Request::Token(request, None)).await;
    assert_eq!(unanswered, Ok(Answer::Done));
    closing.release();
    assert_eq!(page.reply(shutdown).await.result, Ok(Answer::Done));
    let written = page.reply(writing).await.result;
    assert!(written.is_err(), "{written:?}");
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
    requests.extend([
        Request::Watch(table(rng), Watch::Row(columns(rng))),
        Request::Watch(table(rng), Watch::Rows(scan(rng))),
        Request::Unwatch(rng.next_u64()),
        Request::Confirm(rng.next_u64()),
        Request::Token(rng.next_u64(), rng.chance(50).then(|| text(rng))),
    ]);
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
    vec![
        Answer::Done,
        Answer::ClientId(client_id(rng)),
        Answer::Status(status),
        Answer::Row(rng.chance(50).then(|| columns(rng))),
        Answer::Rows((0..rng.below(4)).map(|_| columns(rng)).collect()),
        Answer::Key(columns(rng)),
        Answer::Subscription(rng.next_u64()),
    ]
}

fn client_id(rng: &mut Rng) -> ClientId {
    let person = text(rng).replace('\u{7f}', "");
    ClientId::new(format!("p{person}#{}", rng.next_u64())).unwrap()
}

/// Each notice, with values at random: a value of each answer and each error.
fn notices(rng: &mut Rng) -> Vec<Notice> {
    let results = (answers(rng).into_iter().map(Ok)).chain(errors(rng).into_iter().map(Err));
    let mut notices = Vec::from_iter(results.map(|value| Notice::Value {
        tab: rng.next_u64(),
        subscription: rng.next_u64(),
        value,
    }));
    notices.push(Notice::Ended {
        tab: rng.next_u64(),
        subscription: rng.next_u64(),
    });
    notices.push(Notice::Token {
        tab: rng.next_u64(),
        request: rng.next_u64(),
        client_id: client_id(rng),
        room_id: room_id(rng),
    });
    notices
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
fn every_command_reply_and_notice_goes_through_bincode_unchanged() {
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
        for notice in notices(&mut rng) {
            let notice = Message::Notice(notice);
            round_trip(&notice, fixint);
            round_trip(&notice, varint);
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
