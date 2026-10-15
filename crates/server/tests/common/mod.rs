//! Fixtures shared by the integration tests: the pieces of a server over a temporary
//! directory, a server serving them over HTTP, the requests of the binary protocol, and
//! commands sent straight to a room actor.

// Every file in `tests/` is a crate of its own that uses only part of this module.
#![allow(dead_code)]

use futures::{Stream, StreamExt};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, StatusCode};
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use zemdb_core::*;
use zemdb_server::{
    build_router, generate_client_token, serve_until_shutdown, AppState, CommitResponse,
    RegisterResponse, RoomCommand, RoomLifecyclePolicy, RoomManager, RoomMetrics, SchemaRegistry,
    ServerConfig, ServerError, SnapshotRelay, SyncBatchResponse,
};

pub const AUTH_SECRET: &str = "test_cluster_secret_key_1234567890";
pub const ADMIN_SECRET: &str = "test_admin_secret_key_12345678901";

/// Table `tasks`: an int key `id`, a string `title` and a bool `completed`.
pub fn tasks_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String)
        .column("completed", DataType::Bool);
    Schema::builder().table(table).build()
}

/// An insert of row `id` into `tasks` of [`tasks_schema`].
pub fn make_test_op(id: i64) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("title", "task")
        .set("completed", false)
        .build();
    tasks_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap()
}

/// A raw snapshot envelope of `total_len` bytes whose body is `fill` repeated.
pub fn envelope(total_len: usize, fill: u8) -> Vec<u8> {
    let body = vec![fill; total_len - SNAPSHOT_HEADER_LEN];
    let header =
        SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, body.len() as u32, &body);
    let mut out = header.to_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

/// Sequence numbers of `ops`, in order.
pub fn seqs(ops: &[SequencedOperation]) -> Vec<u64> {
    ops.iter().map(|op| op.seq.get()).collect()
}

/// A client token for `client_id` in `room_id`, valid for 300 s.
pub fn token(client_id: &ClientId, room_id: &RoomId) -> String {
    generate_client_token(client_id, room_id, Duration::from_secs(300), AUTH_SECRET)
}

pub fn register_msg(room_id: &RoomId, client_id: &ClientId, auth_token: &str) -> ClientMessage {
    ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        auth_token: auth_token.to_string(),
        current_seq: None,
        known_incarnation: None,
    }
}

/// An encoded `Heartbeat` from `client_id` in `room_id`.
pub fn heartbeat(client_id: &str, room_id: &str) -> Vec<u8> {
    encode_message(&ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(2),
        room_id: RoomId::new(room_id).unwrap(),
        client_id: ClientId::new(client_id).unwrap(),
    })
    .unwrap()
}

/// The pieces of a server, built as `main` builds them.
pub struct Parts {
    pub dir: TempDir,
    pub data_dir: PathBuf,
    pub config: Arc<ServerConfig>,
    pub schema_registry: Arc<SchemaRegistry>,
    pub relay: Arc<SnapshotRelay>,
    pub room_manager: Arc<RoomManager>,
}

/// Builds the pieces of a server keeping its data in `data/` below a new temporary directory,
/// with the test secrets and a snapshot TTL of 60 s, as changed by `configure`.
pub fn build(configure: impl FnOnce(&mut ServerConfig)) -> Parts {
    build_with_policy(configure, None)
}

/// Like [`build`], with `policy` as the default lifecycle policy of the rooms, if given.
pub fn build_with_policy(
    configure: impl FnOnce(&mut ServerConfig),
    policy: Option<RoomLifecyclePolicy>,
) -> Parts {
    let dir = tempdir().unwrap();
    let mut config = ServerConfig {
        data_dir: dir.path().join("data"),
        auth_secret: AUTH_SECRET.to_string(),
        admin_secret: ADMIN_SECRET.to_string(),
        snapshot_ttl_secs: 60,
        ..ServerConfig::default()
    };
    configure(&mut config);
    let config = Arc::new(config);
    let data_dir = config.data_dir.clone();
    let schema_registry = Arc::new(SchemaRegistry::new(data_dir.join("schemas")).unwrap());
    let relay = new_relay(&config);
    let mut room_manager = RoomManager::new(
        Arc::clone(&config),
        Arc::clone(&schema_registry),
        Arc::clone(&relay),
    );
    if let Some(policy) = policy {
        room_manager = room_manager.with_default_policy(policy);
    }
    Parts {
        dir,
        data_dir,
        config,
        schema_registry,
        relay,
        room_manager: Arc::new(room_manager),
    }
}

fn new_relay(config: &ServerConfig) -> Arc<SnapshotRelay> {
    Arc::new(
        SnapshotRelay::new(
            config.data_dir.join("snapshots"),
            Duration::from_secs(config.snapshot_ttl_secs),
            config.max_snapshot_bytes,
        )
        .unwrap(),
    )
}

impl Parts {
    pub fn register_schema(&self, schema_id: &str, schema: Schema) -> SchemaId {
        let schema_id = SchemaId::new(schema_id).unwrap();
        self.schema_registry
            .register_schema(schema_id.clone(), schema)
            .unwrap();
        schema_id
    }

    /// Creates `room_id` with the schema registered as `schema_id` and no lifecycle overrides.
    pub async fn create_room(&self, room_id: &str, schema_id: &SchemaId) -> RoomId {
        let room_id = RoomId::new(room_id).unwrap();
        self.room_manager
            .create_room(room_id.clone(), schema_id.clone(), None)
            .await
            .unwrap();
        room_id
    }

    /// Registers [`tasks_schema`] as `schema_id` and creates `room_id` with it.
    pub async fn room_with_schema(&self, schema_id: &str, room_id: &str) -> (SchemaId, RoomId) {
        let schema_id = self.register_schema(schema_id, tasks_schema());
        let room_id = self.create_room(room_id, &schema_id).await;
        (schema_id, room_id)
    }

    /// A room manager over the same directory, with a relay of its own: what a restarted
    /// server builds.
    pub fn restarted_manager(&self) -> RoomManager {
        RoomManager::new(
            Arc::clone(&self.config),
            Arc::clone(&self.schema_registry),
            new_relay(&self.config),
        )
    }

    pub fn state(&self) -> AppState {
        AppState::new(
            Arc::clone(&self.config),
            Arc::clone(&self.schema_registry),
            Arc::clone(&self.room_manager),
            Arc::clone(&self.relay),
        )
    }

    /// Serves these pieces with `serve_until_shutdown` until the returned sender fires, with
    /// a shutdown grace period of 10 s.
    pub async fn serve_until_stopped(
        &self,
    ) -> (
        SocketAddr,
        oneshot::Sender<()>,
        JoinHandle<Result<(), ServerError>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(serve_until_shutdown(
            listener,
            self.state(),
            async move {
                stopped.await.ok();
            },
            Duration::from_secs(10),
        ));
        (addr, stop, server)
    }
}

/// A server serving [`Parts`] with `axum::serve` on an ephemeral port.
pub struct TestServer {
    pub base_url: String,
    pub client: reqwest::Client,
    parts: Parts,
}

impl Deref for TestServer {
    type Target = Parts;

    fn deref(&self) -> &Parts {
        &self.parts
    }
}

impl TestServer {
    /// Serves the pieces that [`build`] makes with `configure`.
    pub async fn start(configure: impl FnOnce(&mut ServerConfig)) -> Self {
        let parts = build(configure);
        let app = build_router(parts.state());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            base_url: format!("http://{addr}"),
            client: reqwest::Client::new(),
            parts,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// A request to the control plane authenticated with the admin secret.
    pub fn admin(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, self.url(path))
            .bearer_auth(&self.config.admin_secret)
    }

    /// Posts `body` to `/rooms/{path}`, with `bearer` as the bearer token if given.
    pub async fn post(&self, path: &str, bearer: Option<&str>, body: Vec<u8>) -> reqwest::Response {
        let mut req = self
            .client
            .post(self.url(&format!("/rooms/{path}")))
            .body(body);
        if let Some(token) = bearer {
            req = req.bearer_auth(token);
        }
        req.send().await.unwrap()
    }

    /// Posts `msg`, encoded as a binary frame, to `/rooms/{path}`.
    pub async fn post_frame(
        &self,
        path: &str,
        bearer: Option<&str>,
        msg: &ClientMessage,
    ) -> reqwest::Response {
        let mut req = self
            .client
            .post(self.url(&format!("/rooms/{path}")))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(encode_message(msg).unwrap());
        if let Some(token) = bearer {
            req = req.bearer_auth(token);
        }
        req.send().await.unwrap()
    }

    /// Posts `msg` to the data plane `endpoint` of `room_id` and decodes the reply.
    pub async fn post_message(
        &self,
        room_id: &RoomId,
        endpoint: &str,
        token: &str,
        msg: &ClientMessage,
    ) -> (StatusCode, ServerMessage) {
        let resp = self
            .post_frame(&format!("{room_id}/{endpoint}"), Some(token), msg)
            .await;
        let status = resp.status();
        let body = resp.bytes().await.unwrap();
        (status, decode_message(&body).unwrap())
    }

    /// Registers `client_id` in `room_id` over HTTP, with its token only in the payload,
    /// checks that it succeeds, and returns the token with the server's reply.
    pub async fn register(
        &self,
        room_id: &RoomId,
        client_id: &ClientId,
    ) -> (String, ServerMessage) {
        let token = token(client_id, room_id);
        let msg = register_msg(room_id, client_id, &token);
        let resp = self
            .post_frame(&format!("{room_id}/register"), None, &msg)
            .await;
        let status = resp.status();
        let reply: ServerMessage = decode_message(&resp.bytes().await.unwrap()).unwrap();
        assert_eq!(status, StatusCode::OK, "{reply:?}");
        (token, reply)
    }

    /// Opens the event stream of `room_id`, with `token` in the `Authorization` header.
    pub async fn events(&self, room_id: &RoomId, token: &str) -> reqwest::Response {
        self.client
            .get(self.url(&format!("/rooms/{room_id}/events")))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
    }
}

/// Asserts a binary `ServerMessage::Error` frame with the given status and code.
pub async fn assert_binary_error(resp: reqwest::Response, status: StatusCode, code: ErrorCode) {
    assert_eq!(resp.status(), status);
    assert_eq!(
        resp.headers()["content-type"],
        "application/octet-stream",
        "Data Plane errors, like every error response of the binary API, are binary frames"
    );
    let body = resp.bytes().await.unwrap();
    match decode_message::<ServerMessage>(&body) {
        Ok(ServerMessage::Error { code: actual, .. }) => assert_eq!(actual, code),
        other => panic!("expected a binary error frame, got {other:?}"),
    }
}

/// Reads an SSE byte stream until the received text contains `needle`, returning all of it.
pub async fn read_sse_until(
    stream: &mut (impl Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin),
    needle: &str,
) -> String {
    let mut text = String::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !text.contains(needle) {
            let chunk = stream.next().await.expect("SSE stream closed").unwrap();
            text.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await
    .unwrap_or_else(|_| panic!("SSE never sent {needle:?}; received: {text}"));
    text
}

/// Asserts a JSON error body with the given status and `code`.
pub async fn assert_json_error(resp: reqwest::Response, status: StatusCode, code: &str) {
    assert_eq!(resp.status(), status);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], code, "{body}");
}

/// A room spawned with `get_or_spawn` on the pieces of a server.
pub struct TestRoom {
    pub schema: Schema,
    pub schema_id: SchemaId,
    pub room_id: RoomId,
    pub sender: mpsc::Sender<RoomCommand>,
    parts: Parts,
}

impl Deref for TestRoom {
    type Target = Parts;

    fn deref(&self) -> &Parts {
        &self.parts
    }
}

impl TestRoom {
    /// Spawns room `test-room` with [`tasks_schema`], on a manager whose default lifecycle
    /// policy is `policy`, if given.
    pub async fn spawn(policy: Option<RoomLifecyclePolicy>) -> Self {
        Self::spawn_with(|_| {}, policy).await
    }

    /// Like [`TestRoom::spawn`], with the configuration changed by `configure`.
    pub async fn spawn_with(
        configure: impl FnOnce(&mut ServerConfig),
        policy: Option<RoomLifecyclePolicy>,
    ) -> Self {
        let parts = build_with_policy(configure, policy);
        let schema = tasks_schema();
        let schema_id = parts.register_schema("tasks-schema", schema.clone());
        let room_id = RoomId::new("test-room").unwrap();
        let sender = parts
            .room_manager
            .get_or_spawn(&room_id, Some(&schema_id))
            .await
            .unwrap();
        Self {
            schema,
            schema_id,
            room_id,
            sender,
            parts,
        }
    }

    pub fn segments_dir(&self) -> PathBuf {
        self.data_dir
            .join("rooms")
            .join(self.room_id.as_str())
            .join("segments")
    }
}

/// Sends the command that `cmd` builds around a reply channel and waits for the reply.
pub async fn ask<T>(
    tx: &mpsc::Sender<RoomCommand>,
    cmd: impl FnOnce(oneshot::Sender<Result<T, ServerError>>) -> RoomCommand,
) -> Result<T, ServerError> {
    let (reply, rx) = oneshot::channel();
    tx.send(cmd(reply)).await.unwrap();
    rx.await.unwrap()
}

/// Registers `client_id` without a cursor.
pub async fn register(tx: &mpsc::Sender<RoomCommand>, client_id: &ClientId) -> RegisterResponse {
    ask(tx, |reply| RoomCommand::RegisterClient {
        client_id: client_id.clone(),
        current_seq: None,
        known_incarnation: None,
        reply,
    })
    .await
    .unwrap()
}

pub async fn commit(
    tx: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    mutation_id: MutationId,
    last_ack_seq: u64,
    op: Operation,
) -> Result<CommitResponse, ServerError> {
    ask(tx, |reply| RoomCommand::Commit {
        client_id: client_id.clone(),
        mutation_id,
        last_ack_seq: SequenceNumber::new(last_ack_seq),
        op,
        reply,
    })
    .await
}

pub async fn sync(
    tx: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    from_seq: u64,
    max_batch_size: u32,
) -> Result<SyncBatchResponse, ServerError> {
    ask(tx, |reply| RoomCommand::Sync {
        client_id: client_id.clone(),
        from_seq: SequenceNumber::new(from_seq),
        max_batch_size,
        reply,
    })
    .await
}

pub async fn ack(
    tx: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
    ack_seq: u64,
) -> Result<SequenceNumber, ServerError> {
    ask(tx, |reply| RoomCommand::Ack {
        client_id: client_id.clone(),
        ack_seq: SequenceNumber::new(ack_seq),
        reply,
    })
    .await
}

/// The confirmed cursor of `client_id`, `None` if it is not registered.
pub async fn cursor(
    tx: &mpsc::Sender<RoomCommand>,
    client_id: &ClientId,
) -> Option<SequenceNumber> {
    ask(tx, |reply| RoomCommand::GetClientCursor {
        client_id: client_id.clone(),
        reply,
    })
    .await
    .unwrap()
}

pub async fn metrics(tx: &mpsc::Sender<RoomCommand>) -> RoomMetrics {
    ask(tx, |reply| RoomCommand::GetMetrics { reply })
        .await
        .unwrap()
}
