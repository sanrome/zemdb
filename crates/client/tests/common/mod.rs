//! Fixtures shared by the integration tests: the server in process, with a transport to it that
//! logs what is sent and answers on demand, a file layer that logs what is done to the files and
//! fails on demand, a storage engine that fails on demand, and a client over a disk engine.

// Every file in `tests/` is a crate of its own that uses only part of this module.
#![allow(dead_code)]

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::Request;
use axum::Router;
use bytes::Bytes;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::sync::oneshot;
use tower::ServiceExt;
use zemdb_client::{
    endpoint, Client, ClientConfig, ClientError, FileMetadata, RoomStatus, TransportClient,
    TransportError,
};
use zemdb_core::*;
use zemdb_server::{
    build_router, generate_client_token, AppState, RoomManager, SchemaRegistry, ServerConfig,
    SnapshotRelay,
};
use zemdb_storage::files::{NativeFile, NativeFiles, RoomFile, RoomFiles};
use zemdb_storage::{
    ApplyReport, DiskStorageEngine, DiskStorageOptions, RowStream, ScanOptions, StorageEngine,
    StorageError,
};

pub const AUTH_SECRET: &str = "test_cluster_secret_key_1234567890";
pub const ADMIN_SECRET: &str = "test_admin_secret_key_12345678901";

/// Table `tasks`: an int key `id` and a string `title`.
pub fn tasks_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    Schema::builder().table(table).build()
}

/// An insert of row `id` into `tasks` of [`tasks_schema`].
pub fn insert_task(id: i64) -> Operation {
    let row = RowBuilder::new().set("id", id).set("title", "task").build();
    tasks_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap()
}

/// An update of the title of row `id` of `tasks` of [`tasks_schema`].
pub fn update_task(id: i64) -> Operation {
    let fields = std::collections::BTreeMap::from([("title".to_string(), Value::from("done"))]);
    tasks_schema()
        .to_operation_update("tasks", PrimaryKey::single(id), &fields, 1000)
        .unwrap()
}

/// A client token of `client_id` for `room_id`, valid for 300 s.
pub fn token(client_id: &ClientId, room_id: &RoomId) -> String {
    generate_client_token(client_id, room_id, Duration::from_secs(300), AUTH_SECRET)
}

/// The options of a disk engine over `dir` that compacts only when asked to.
pub fn options(dir: &Path) -> DiskStorageOptions {
    DiskStorageOptions::new(dir).auto_compact(false)
}

pub fn engine(dir: &Path) -> DiskStorageEngine {
    DiskStorageEngine::new(options(dir)).unwrap()
}

/// A client of ana over `storage`, with its metadata in `dir`.
pub async fn open_client(
    dir: &Path,
    storage: impl StorageEngine + 'static,
    transport: &TestTransport,
) -> Client {
    let metadata = FileMetadata::new(dir);
    let client = Client::open(
        ClientConfig::new("ana"),
        storage,
        metadata,
        transport.clone(),
    );
    client.await.unwrap()
}

/// Opens `room_id` in `client`, with a token of the client.
pub async fn open_room(client: &Client, room_id: &RoomId) -> Result<(), ClientError> {
    let token = token(&client.client_id(), room_id);
    client.open_room(room_id.clone(), token).await
}

pub fn status(head_seq: u64, epoch: u64, registered: bool) -> RoomStatus {
    RoomStatus {
        head_seq: SequenceNumber::new(head_seq),
        epoch,
        registered,
        failed_closed: false,
        writes_in_flight: 0,
        retained: 0,
    }
}

/// Which of the tasks 1 to 9 `room_id` of `storage` holds.
pub async fn tasks(storage: &impl StorageEngine, room_id: &RoomId) -> Vec<i64> {
    let mut tasks = Vec::new();
    for id in 1..=9 {
        let row = storage.get(room_id, "tasks", &PrimaryKey::single(id)).await;
        if row.unwrap().is_some() {
            tasks.push(id);
        }
    }
    tasks
}

/// A server with `room-a`, and a client over `storage` with its metadata in `dir`, with the room
/// open.
pub async fn setup<S: StorageEngine + Clone + 'static>(
    dir: &Path,
    storage: S,
) -> (TestServer, RoomId, TestTransport, S, Client) {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let transport = server.transport();
    let client = open_client(dir, storage.clone(), &transport).await;
    open_room(&client, &room_id).await.unwrap();
    (server, room_id, transport, storage, client)
}

/// The correlation id and the room of `message`, a sync, an ack or a commit.
pub fn request(message: &ClientMessage) -> (CorrelationId, RoomId) {
    match message {
        ClientMessage::Sync {
            correlation_id,
            room_id,
            ..
        }
        | ClientMessage::Commit {
            correlation_id,
            room_id,
            ..
        }
        | ClientMessage::Ack {
            correlation_id,
            room_id,
            ..
        } => (*correlation_id, room_id.clone()),
        message => panic!("{message:?}"),
    }
}

/// Answers the next message of `kind`, a sync, an ack or a commit, with what `reply` gives for its
/// correlation id and room.
pub fn answer_next(
    transport: &TestTransport,
    kind: ClientMessageKind,
    reply: impl FnOnce(CorrelationId, &RoomId) -> Vec<u8> + Send + 'static,
) {
    transport.answer_next(kind, move |message| {
        let (correlation_id, room_id) = request(message);
        reply(correlation_id, &room_id)
    });
}

/// The syncs sent through `transport`, with their `from_seq`, and the acks, with their
/// `ack_seq`, in order.
pub fn requests(transport: &TestTransport) -> Vec<(&'static str, u64)> {
    let request = |message: ClientMessage| match message {
        ClientMessage::Sync { from_seq, .. } => Some(("sync", from_seq.get())),
        ClientMessage::Ack { ack_seq, .. } => Some(("ack", ack_seq.get())),
        _ => None,
    };
    transport.sent().into_iter().filter_map(request).collect()
}

/// The commits sent through `transport`, with their mutation id and their `last_ack_seq`, in
/// order.
pub fn commits(transport: &TestTransport) -> Vec<(MutationId, u64)> {
    let commit = |message: ClientMessage| match message {
        ClientMessage::Commit {
            mutation_id,
            last_ack_seq,
            ..
        } => Some((mutation_id, last_ack_seq.get())),
        _ => None,
    };
    transport.sent().into_iter().filter_map(commit).collect()
}

/// How many deregistrations went through `transport`.
pub fn deregistrations(transport: &TestTransport) -> usize {
    let sent = transport.sent();
    let deregistration =
        |message: &&ClientMessage| matches!(message, ClientMessage::DeregisterClient { .. });
    sent.iter().filter(deregistration).count()
}

/// The pieces of a server keeping its data in a temporary directory, built as the server's tests
/// build them (without the lock of the data directory and the expiry sweeper of `main`), with the
/// test secrets.
pub struct TestServer {
    pub state: AppState,
    _dir: TempDir,
}

impl TestServer {
    pub fn new() -> Self {
        let dir = tempdir().unwrap();
        let config = Arc::new(ServerConfig {
            data_dir: dir.path().join("data"),
            auth_secret: AUTH_SECRET.to_string(),
            admin_secret: ADMIN_SECRET.to_string(),
            ..ServerConfig::default()
        });
        let registry = Arc::new(SchemaRegistry::new(config.data_dir.join("schemas")).unwrap());
        let relay = Arc::new(
            SnapshotRelay::new(
                config.data_dir.join("snapshots"),
                Duration::from_secs(config.snapshot_ttl_secs),
                config.max_snapshot_bytes,
                Duration::from_secs(config.snapshot_upload_max_secs),
            )
            .unwrap(),
        );
        let manager = Arc::new(RoomManager::new(
            Arc::clone(&config),
            Arc::clone(&registry),
            Arc::clone(&relay),
        ));
        Self {
            state: AppState::new(config, registry, manager, relay),
            _dir: dir,
        }
    }

    /// Creates `room_id` with [`tasks_schema`], registered as `tasks` if it is not yet.
    pub async fn create_room(&self, room_id: &str) -> RoomId {
        self.create_room_with(room_id, "tasks", tasks_schema())
            .await
    }

    /// Creates `room_id` with `schema`, registered as `schema_id` if it is not yet.
    pub async fn create_room_with(&self, room_id: &str, schema_id: &str, schema: Schema) -> RoomId {
        let schema_id = SchemaId::new(schema_id).unwrap();
        if self.state.schema_registry.get_schema(&schema_id).is_none() {
            self.state
                .schema_registry
                .register_schema(schema_id.clone(), schema)
                .unwrap();
        }
        let room_id = RoomId::new(room_id).unwrap();
        self.state
            .room_manager
            .create_room(room_id.clone(), schema_id, None)
            .await
            .unwrap();
        room_id
    }

    /// Deletes `room_id` and creates it again, with another incarnation.
    pub async fn recreate_room(&self, room_id: &RoomId) {
        self.state.room_manager.delete_room(room_id).await.unwrap();
        self.create_room(room_id.as_str()).await;
    }

    /// The incarnation of `room_id`.
    pub async fn incarnation(&self, room_id: &RoomId) -> u128 {
        let metadata = self.state.room_manager.room_metadata(room_id).await;
        metadata.unwrap().room_incarnation
    }

    /// Commits an insert of each of `ids` into `room_id`, as a client of its own, and returns
    /// them with the sequences they got.
    pub async fn commit(
        &self,
        room_id: &RoomId,
        ids: impl IntoIterator<Item = i64>,
    ) -> Vec<SequencedOperation> {
        self.commit_ops(room_id, ids.into_iter().map(insert_task))
            .await
    }

    /// Commits each of `ops` to `room_id`, as a client of its own, and returns them with the
    /// sequences they got.
    pub async fn commit_ops(
        &self,
        room_id: &RoomId,
        ops: impl IntoIterator<Item = Operation>,
    ) -> Vec<SequencedOperation> {
        let transport = self.transport();
        let client_id = ClientId::new("bob#writer").unwrap();
        let token = token(&client_id, room_id);
        let correlation_id = CorrelationId::new(1);
        let send = |message: ClientMessage| {
            let (transport, token) = (transport.clone(), token.clone());
            async move {
                let reply = transport.send(&message, &token).await.unwrap();
                decode_message::<ServerMessage>(&reply).unwrap()
            }
        };
        let registered = send(ClientMessage::RegisterClient {
            correlation_id,
            room_id: room_id.clone(),
            client_id: client_id.clone(),
            auth_token: token.clone(),
            current_seq: None,
            known_incarnation: None,
        })
        .await;
        assert!(matches!(registered, ServerMessage::Registered { .. }));
        let mut sequenced = Vec::new();
        for op in ops {
            let reply = send(ClientMessage::Commit {
                correlation_id,
                room_id: room_id.clone(),
                client_id: client_id.clone(),
                mutation_id: MutationId::from(uuid::Uuid::new_v4().as_u128()),
                last_ack_seq: SequenceNumber::new(0),
                op: op.clone(),
            })
            .await;
            let ServerMessage::CommitAck { assigned_seq, .. } = reply else {
                panic!("{reply:?}");
            };
            sequenced.push(SequencedOperation::new(assigned_seq, op));
        }
        sequenced
    }

    /// A transport to this server's router, called as a `tower` service.
    pub fn transport(&self) -> TestTransport {
        TestTransport {
            router: build_router(self.state.clone()),
            sent: Arc::default(),
            answer: Arc::default(),
            delay: Arc::default(),
            delay_next: Arc::default(),
            hold: Arc::default(),
            lose: Arc::default(),
            sync_batch_size: Arc::default(),
        }
    }
}

/// The encoding of an error with `code` for the request `correlation_id` of `room_id`.
pub fn error_reply(correlation_id: CorrelationId, room_id: &RoomId, code: ErrorCode) -> Vec<u8> {
    encode_message(&ServerMessage::Error {
        correlation_id: Some(correlation_id),
        room_id: Some(room_id.clone()),
        code,
        message: "Fabricated".to_string(),
    })
    .unwrap()
}

/// What a [`TestTransport`] answers a message with instead of the server, if anything.
type Answer = dyn FnMut(&ClientMessage) -> Option<Result<Vec<u8>, TransportError>> + Send;

/// The kind of the messages whose reply a [`TestTransport`] holds, with the ends of a [`Held`].
type Hold = (
    ClientMessageKind,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
);

/// The reply to a message that a [`TestTransport`] holds until it is released.
pub struct Held {
    replied: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

impl Held {
    /// Waits until the server replied to the message.
    pub async fn replied(&mut self) {
        let replied = tokio::time::timeout(Duration::from_secs(10), &mut self.replied).await;
        replied.expect("no reply came").unwrap();
    }

    /// Lets the reply reach the client.
    pub fn release(self) {
        self.release.send(()).unwrap();
    }
}

/// A transport to a [`TestServer`], which logs the messages it sends and can answer them itself.
/// Each request runs in a task of its own, as over a network, where the server goes on with a
/// request whose client stopped waiting.
#[derive(Clone)]
pub struct TestTransport {
    router: Router,
    sent: Arc<Mutex<Vec<ClientMessage>>>,
    answer: Arc<Mutex<Option<Box<Answer>>>>,
    delay: Arc<Mutex<Duration>>,
    delay_next: Arc<Mutex<Option<(ClientMessageKind, Duration)>>>,
    hold: Arc<Mutex<Vec<Hold>>>,
    lose: Arc<Mutex<Option<ClientMessageKind>>>,
    sync_batch_size: Arc<Mutex<Option<u32>>>,
}

impl TestTransport {
    /// The messages sent so far, in order.
    pub fn sent(&self) -> Vec<ClientMessage> {
        self.sent.lock().unwrap().clone()
    }

    /// The registrations sent so far, as their cursor and incarnation.
    pub fn registrations(&self) -> Vec<(Option<u64>, Option<u128>)> {
        let registration = |message: ClientMessage| match message {
            ClientMessage::RegisterClient {
                current_seq,
                known_incarnation,
                ..
            } => Some((current_seq.map(|seq| seq.get()), known_incarnation)),
            _ => None,
        };
        self.sent().into_iter().filter_map(registration).collect()
    }

    /// Gets no response to any message, which does not reach the server.
    pub fn offline(&self) {
        self.answer(|_| Some(Err(TransportError("Offline".to_string()))));
    }

    /// Makes every message reach the server `delay` after it is sent.
    pub fn slow(&self, delay: Duration) {
        *self.delay.lock().unwrap() = delay;
    }

    /// Makes the next message of `kind` reach the server `delay` after it is sent.
    pub fn slow_next(&self, kind: ClientMessageKind, delay: Duration) {
        *self.delay_next.lock().unwrap() = Some((kind, delay));
    }

    /// Lets every message reach the server.
    pub fn online(&self) {
        *self.answer.lock().unwrap() = None;
    }

    /// Makes every sync ask for at most `size` operations.
    pub fn sync_batch_size(&self, size: u32) {
        *self.sync_batch_size.lock().unwrap() = Some(size);
    }

    /// Holds the reply to the next message of `kind`, from the server or answered: the message
    /// reaches the server at once, and its reply reaches the client once the [`Held`] is
    /// released.
    pub fn hold_next_reply(&self, kind: ClientMessageKind) -> Held {
        let (replied_tx, replied) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        self.hold
            .lock()
            .unwrap()
            .push((kind, replied_tx, release_rx));
        Held { replied, release }
    }

    /// Loses the reply to the next message of `kind`: the message reaches the server, and the
    /// client gets no response. That message is neither held nor answered.
    pub fn lose_next_reply(&self, kind: ClientMessageKind) {
        *self.lose.lock().unwrap() = Some(kind);
    }

    /// Answers the next message of `kind` with the bytes `reply` gives for it, without reaching
    /// the server.
    pub fn answer_next(
        &self,
        kind: ClientMessageKind,
        reply: impl FnOnce(&ClientMessage) -> Vec<u8> + Send + 'static,
    ) {
        let mut reply = Some(reply);
        self.answer(move |message| match message.kind() == kind {
            true => reply.take().map(|reply| Ok(reply(message))),
            false => None,
        });
    }

    /// Answers every message of `kind` with the bytes `reply` gives for it, without reaching the
    /// server.
    pub fn answer_every(
        &self,
        kind: ClientMessageKind,
        mut reply: impl FnMut(&ClientMessage) -> Vec<u8> + Send + 'static,
    ) {
        self.answer(move |message| (message.kind() == kind).then(|| Ok(reply(message))));
    }

    /// Answers the next registration with the bytes `reply` gives for its correlation id and
    /// room, without reaching the server.
    pub fn answer_next_registration(
        &self,
        reply: impl FnOnce(CorrelationId, &RoomId) -> Vec<u8> + Send + 'static,
    ) {
        let mut reply = Some(reply);
        self.answer(move |message| match message {
            ClientMessage::RegisterClient {
                correlation_id,
                room_id,
                ..
            } => reply
                .take()
                .map(|reply| Ok(reply(*correlation_id, room_id))),
            _ => None,
        });
    }

    /// Answers each message that `answer` gives a reply for with it, without reaching the
    /// server, until another answer replaces it.
    pub fn answer(
        &self,
        answer: impl FnMut(&ClientMessage) -> Option<Result<Vec<u8>, TransportError>> + Send + 'static,
    ) {
        *self.answer.lock().unwrap() = Some(Box::new(answer));
    }
}

impl fmt::Debug for TestTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestTransport").finish_non_exhaustive()
    }
}

#[async_trait]
impl TransportClient for TestTransport {
    async fn send(&self, message: &ClientMessage, token: &str) -> Result<Vec<u8>, TransportError> {
        self.sent.lock().unwrap().push(message.clone());
        let lost = (self.lose.lock().unwrap()).take_if(|kind| *kind == message.kind());
        if lost.is_some() {
            self.server_reply(message, token).await?;
            return Err(TransportError("Lost".to_string()));
        }
        let held = {
            let mut hold = self.hold.lock().unwrap();
            let index = hold.iter().position(|(kind, ..)| *kind == message.kind());
            index.map(|index| hold.remove(index))
        };
        let reply = self.reply(message, token).await;
        // A dropped hold releases the reply.
        if let Some((_, replied, release)) = held {
            let _ = replied.send(());
            let _ = release.await;
        }
        reply
    }
}

impl TestTransport {
    /// The reply to `message`: the answer to it, if any, or the server's.
    async fn reply(&self, message: &ClientMessage, token: &str) -> Result<Vec<u8>, TransportError> {
        let answered = self
            .answer
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|answer| answer(message));
        if let Some(answer) = answered {
            return answer;
        }
        self.server_reply(message, token).await
    }

    /// The server's reply to `message`.
    async fn server_reply(
        &self,
        message: &ClientMessage,
        token: &str,
    ) -> Result<Vec<u8>, TransportError> {
        let mut message = message.clone();
        let size = *self.sync_batch_size.lock().unwrap();
        if let (ClientMessage::Sync { max_batch_size, .. }, Some(size)) = (&mut message, size) {
            *max_batch_size = size;
        }
        let request = Request::post(endpoint(&message))
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(encode_message(&message).unwrap()))
            .unwrap();
        let (router, mut delay) = (self.router.clone(), *self.delay.lock().unwrap());
        let next = (self.delay_next.lock().unwrap()).take_if(|(kind, _)| *kind == message.kind());
        delay += next.map_or(Duration::ZERO, |(_, next)| next);
        let response = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let response = router.oneshot(request).await.unwrap();
            to_bytes(response.into_body(), usize::MAX).await
        });
        let no_response = |err: &dyn std::fmt::Display| TransportError(err.to_string());
        let body = response.await.map_err(|err| no_response(&err))?;
        Ok(body.map_err(|err| no_response(&err))?.to_vec())
    }
}

/// The file system of the process through a layer that logs what is done to each file (by its
/// name, without its reads), and where one operation on one file can fail with an I/O error (not
/// a missing file) and a write can be slow.
#[derive(Debug, Clone, Default)]
pub struct TestFiles {
    failing: Option<(&'static str, &'static str)>,
    fail_next: Arc<Mutex<Option<(&'static str, &'static str)>>>,
    next_write_delay: Arc<Mutex<Option<Duration>>>,
    log: Arc<Mutex<Vec<String>>>,
}

impl TestFiles {
    /// A layer where `operation` (a method of `RoomFiles` or of `RoomFile`) fails on the file
    /// named `name`: for a rename by path, the file it would replace.
    pub fn failing(operation: &'static str, name: &'static str) -> Self {
        Self {
            failing: Some((operation, name)),
            ..Self::default()
        }
    }

    /// Makes the next `operation` on the file named `name` fail as in [`failing`](Self::failing).
    pub fn fail_next(&self, operation: &'static str, name: &'static str) {
        *self.fail_next.lock().unwrap() = Some((operation, name));
    }

    /// Makes the next write land `delay` after it is called, even if its caller stopped
    /// waiting, as a write handed to a slow disk.
    pub fn slow(&self, delay: Duration) {
        *self.next_write_delay.lock().unwrap() = Some(delay);
    }

    /// What was done to the files so far, in order, as `operation name`.
    pub fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// Waits until an entry that starts with `entry` is in the log.
    pub async fn logged(&self, entry: &str) {
        while !self.log().iter().any(|logged| logged.starts_with(entry)) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Logs `operation` on the files `names` unless it is a read, and fails if it is the failing
    /// one on the last of them.
    fn run(&self, operation: &'static str, names: &[&str]) -> io::Result<()> {
        if !matches!(operation, "size" | "read_at") {
            self.push(format!("{operation} {}", names.join(" ")));
        }
        let this = names.last().map(|name| (operation, *name));
        let mut fail_next = self.fail_next.lock().unwrap();
        let once = fail_next.is_some() && *fail_next == this;
        if once {
            *fail_next = None;
        }
        if once || (self.failing.is_some() && self.failing == this) {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "injected"));
        }
        Ok(())
    }

    fn push(&self, entry: String) {
        self.log.lock().unwrap().push(entry);
    }

    fn file(&self, path: &Path, file: NativeFile) -> TestFile {
        TestFile {
            file: Arc::new(file),
            name: name(path).to_string(),
            files: self.clone(),
        }
    }
}

fn name(path: &Path) -> &str {
    path.file_name().and_then(|name| name.to_str()).unwrap()
}

#[async_trait]
impl RoomFiles for TestFiles {
    type File = TestFile;

    async fn open_locked(&self, path: &Path) -> io::Result<Option<TestFile>> {
        self.run("open_locked", &[name(path)])?;
        let file = NativeFiles.open_locked(path).await?;
        Ok(file.map(|file| self.file(path, file)))
    }

    async fn create_locked(&self, path: &Path) -> io::Result<Option<TestFile>> {
        self.run("create_locked", &[name(path)])?;
        let file = NativeFiles.create_locked(path).await?;
        Ok(file.map(|file| self.file(path, file)))
    }

    async fn create(&self, path: &Path) -> io::Result<TestFile> {
        self.run("create", &[name(path)])?;
        Ok(self.file(path, NativeFiles.create(path).await?))
    }

    async fn open_read(&self, path: &Path) -> io::Result<TestFile> {
        self.run("open_read", &[name(path)])?;
        Ok(self.file(path, NativeFiles.open_read(path).await?))
    }

    async fn exists(&self, path: &Path) -> io::Result<bool> {
        NativeFiles.exists(path).await
    }

    async fn list(&self, dir: &Path, prefix: &str) -> io::Result<Vec<PathBuf>> {
        NativeFiles.list(dir, prefix).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.run("rename", &[name(from), name(to)])?;
        NativeFiles.rename(from, to).await
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        self.run("remove", &[name(path)])?;
        NativeFiles.remove(path).await
    }

    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.run("create_dir_all", &[name(path)])?;
        NativeFiles.create_dir_all(path).await
    }

    async fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.run("sync_dir", &[name(dir)])?;
        NativeFiles.sync_dir(dir).await
    }
}

/// A file opened by [`TestFiles`], which logs what is done to it, and when it is closed.
#[derive(Debug)]
pub struct TestFile {
    file: Arc<NativeFile>,
    name: String,
    files: TestFiles,
}

impl Drop for TestFile {
    fn drop(&mut self) {
        self.files.push(format!("close {}", self.name));
    }
}

#[async_trait]
impl RoomFile for TestFile {
    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.files.run("read_at", &[&self.name])?;
        self.file.read_at(offset, len).await
    }

    async fn write_at(&self, offset: u64, bytes: Vec<u8>) -> io::Result<()> {
        self.files.run("write_at", &[&self.name])?;
        let Some(delay) = self.files.next_write_delay.lock().unwrap().take() else {
            return self.file.write_at(offset, bytes).await;
        };
        let file = Arc::clone(&self.file);
        let write = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            file.write_at(offset, bytes).await
        });
        write.await.unwrap()
    }

    async fn size(&self) -> io::Result<u64> {
        self.files.run("size", &[&self.name])?;
        self.file.size().await
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        self.files.run("set_len", &[&self.name])?;
        self.file.set_len(len).await
    }

    async fn sync_data(&self) -> io::Result<()> {
        self.files.run("sync_data", &[&self.name])?;
        self.file.sync_data().await
    }

    async fn sync_all(&self) -> io::Result<()> {
        self.files.run("sync_all", &[&self.name])?;
        self.file.sync_all().await
    }

    async fn rename(&mut self, to: &Path) -> io::Result<()> {
        self.files.run("rename", &[&self.name])?;
        let file = Arc::get_mut(&mut self.file).expect("no write in flight");
        file.rename(to).await
    }
}

/// What a [`TestEngine`] runs before its next `reload_schema`.
type BeforeReload = Box<dyn FnOnce() + Send>;

/// A storage engine that wraps `E` and fails on demand: [`fail_next`](Self::fail_next) makes the
/// next call of a method return an error. A close that fails still closes the room, as a disk
/// engine's does when its last sync fails.
#[derive(Clone)]
pub struct TestEngine<E> {
    inner: E,
    failing: Arc<Mutex<Vec<(&'static str, StorageError)>>>,
    slow_open: Arc<Mutex<Option<Duration>>>,
    before_reload: Arc<Mutex<Option<BeforeReload>>>,
}

impl<E> TestEngine<E> {
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            failing: Arc::default(),
            slow_open: Arc::default(),
            before_reload: Arc::default(),
        }
    }

    /// Runs `before` when `reload_schema` is called next, before anything else.
    pub fn before_next_reload(&self, before: impl FnOnce() + Send + 'static) {
        *self.before_reload.lock().unwrap() = Some(Box::new(before));
    }

    /// Makes the next call of `method` (a method of `StorageEngine`) return `error`.
    pub fn fail_next(&self, method: &'static str, error: StorageError) {
        self.failing.lock().unwrap().push((method, error));
    }

    /// Makes the next call of `open_room` start `delay` after it is called.
    pub fn slow_open(&self, delay: Duration) {
        *self.slow_open.lock().unwrap() = Some(delay);
    }

    fn check(&self, method: &str) -> Result<(), StorageError> {
        let mut failing = self.failing.lock().unwrap();
        match failing.iter().position(|(failing, _)| *failing == method) {
            Some(index) => Err(failing.remove(index).1),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl<E: StorageEngine> StorageEngine for TestEngine<E> {
    async fn open_room(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError> {
        let delay = self.slow_open.lock().unwrap().take();
        tokio::time::sleep(delay.unwrap_or_default()).await;
        self.check("open_room")?;
        self.inner.open_room(room_id, schema).await
    }

    async fn reload_schema(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError> {
        let before = self.before_reload.lock().unwrap().take();
        if let Some(before) = before {
            before();
        }
        self.check("reload_schema")?;
        self.inner.reload_schema(room_id, schema).await
    }

    async fn close_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let closed = self.inner.close_room(room_id).await;
        self.check("close_room")?;
        closed
    }

    async fn delete_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        self.check("delete_room")?;
        self.inner.delete_room(room_id).await
    }

    async fn apply_batch(
        &self,
        room_id: &RoomId,
        ops: Vec<SequencedOperation>,
    ) -> Result<ApplyReport, StorageError> {
        self.check("apply_batch")?;
        self.inner.apply_batch(room_id, ops).await
    }

    async fn get(
        &self,
        room_id: &RoomId,
        table: &str,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        self.check("get")?;
        self.inner.get(room_id, table, pk).await
    }

    async fn get_by_id(
        &self,
        room_id: &RoomId,
        table_id: u16,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        self.check("get_by_id")?;
        self.inner.get_by_id(room_id, table_id, pk).await
    }

    async fn scan<'a>(
        &'a self,
        room_id: &RoomId,
        table: &str,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        self.check("scan")?;
        self.inner.scan(room_id, table, options).await
    }

    async fn scan_by_id<'a>(
        &'a self,
        room_id: &RoomId,
        table_id: u16,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        self.check("scan_by_id")?;
        self.inner.scan_by_id(room_id, table_id, options).await
    }

    async fn get_head_seq(&self, room_id: &RoomId) -> Result<SequenceNumber, StorageError> {
        self.check("get_head_seq")?;
        self.inner.get_head_seq(room_id).await
    }

    async fn create_snapshot(&self, room_id: &RoomId) -> Result<Vec<u8>, StorageError> {
        self.check("create_snapshot")?;
        self.inner.create_snapshot(room_id).await
    }

    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: Bytes,
        expected_head_seq: SequenceNumber,
    ) -> Result<SequenceNumber, StorageError> {
        self.check("apply_snapshot")?;
        let applied = self
            .inner
            .apply_snapshot(room_id, schema, snapshot, expected_head_seq);
        applied.await
    }
}
