//! The ranking by votes, the reference app: friends share rankings, open rounds in them, and in
//! each round each member votes for another, and each vote is a point.
//!
//! Here are its schema; the server it runs against, in process on a local socket; its backend,
//! which provisions the rooms and issues the tokens; the key with which the members encrypt the
//! notes of the rounds; the devices of the members, each a client of the facade over a replica on
//! disk, which reaches the server through a network that can be cut; and what the app does, over
//! the facade.

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use reqwest::header::CONTENT_TYPE;
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;
use zemdb_client::{
    ClientConfig, ClientError, FileMetadata, HttpTransport, HttpTransportConfig, Key, RoomHandle,
    Row, Scan, TokenError, TokenProvider, ZemdbClient,
};
use zemdb_core::{
    ClientId, CryptoEngine, CryptoError, DataType, OwnerIdentity, RoomId, Schema, TableSchema,
    Value,
};
use zemdb_server::{
    generate_client_token, serve_until_shutdown, AppState, IoThreads, RoomManager, SchemaRegistry,
    ServerConfig, SnapshotRelay, SHUTDOWN_GRACE_PERIOD,
};
use zemdb_storage::{DiskStorageEngine, DiskStorageOptions};

/// The errors of the example: those of the SDK, of the server, of the network and of its checks.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The id under which the backend registers [`schema`].
pub const SCHEMA_ID: &str = "ranking";

/// The schema of the app. The owners are persons, the part of a client id before `#`, so that
/// the devices of a person write the same rows; the SDK fills them.
///
/// - `rankings(ranking_id, creator, name)`, of its creator.
/// - `members(ranking_id, member_id, display_name)`, of the member: each joins by themselves,
///   a ranking that exists.
/// - `rounds(ranking_id, round_id, opened_by, title, notes)`, of the member of the ranking who
///   opens it, with a title unique in the ranking and the notes encrypted.
/// - `votes(voter, ranking_id, round_id, candidate)`, of the voter, immutable: one per member in
///   a round of the ranking, for a member of the ranking.
///
/// The foreign keys to `members` share `ranking_id`, so a vote does not mix the round of a ranking
/// with a member of another; and those of `rounds` and of the voter include the owner column of
/// both tables. Since votes are immutable, a member who voted or got votes cannot leave, a round
/// with votes cannot be deleted, and a ranking with members of others cannot be deleted either
/// (`Restrict`). That nobody votes for themselves is a rule of the app, not of the schema.
pub fn schema() -> Schema {
    let rankings = TableSchema::builder("rankings")
        .primary_key("ranking_id", DataType::Uuid)
        .column("creator", DataType::String)
        .column("name", DataType::String)
        .owner("creator", OwnerIdentity::ClientId);
    let members = TableSchema::builder("members")
        .primary_key("ranking_id", DataType::Uuid)
        .primary_key("member_id", DataType::String)
        .column("display_name", DataType::String)
        .owner("member_id", OwnerIdentity::ClientId)
        .foreign_key("members_ranking", ["ranking_id"], "rankings");
    let rounds = TableSchema::builder("rounds")
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

// ---------------------------------------------------------------------------------------------
// The server and the backend

/// The ZemDB server, in process, with the pieces that its binary builds and its default
/// configuration, on a socket of `127.0.0.1` with a port that the system chooses, and with its
/// data in a temporary directory, without the lock of the binary on it.
///
/// Dropped, it gets the signal to stop, and its directory goes with it, without waiting for it
/// to stop: the example drops it once done with it, and its runtime then stops its tasks. An
/// error that stops it is printed.
pub struct Server {
    addr: SocketAddr,
    state: AppState,
    _stop: oneshot::Sender<()>,
    _dir: TempDir,
}

impl Server {
    /// Starts a server with the defaults of its configuration and secrets of its own.
    pub async fn start() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let secret = || format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let config = ServerConfig {
            data_dir: dir.path().join("data"),
            auth_secret: secret(),
            admin_secret: secret(),
            ..ServerConfig::default()
        };
        config.validate()?;
        let config = Arc::new(config);
        let registry = Arc::new(SchemaRegistry::new(config.data_dir.join("schemas"))?);
        let relay = Arc::new(SnapshotRelay::new(
            config.data_dir.join("snapshots"),
            Duration::from_secs(config.snapshot_ttl_secs),
            config.max_snapshot_bytes,
            Duration::from_secs(config.snapshot_upload_max_secs),
        )?);
        relay.spawn_expiry_sweeper();
        let manager = Arc::new(RoomManager::new(
            Arc::clone(&config),
            Arc::clone(&registry),
            Arc::clone(&relay),
            IoThreads::new(config.io_threads),
        ));
        let state = AppState::new(config, registry, manager, relay);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (stop, stopped) = oneshot::channel::<()>();
        let stopped = async move {
            let _ = stopped.await;
        };
        let served = serve_until_shutdown(listener, state.clone(), stopped, SHUTDOWN_GRACE_PERIOD);
        tokio::spawn(async move {
            if let Err(err) = served.await {
                eprintln!("The server stopped: {err}");
            }
        });
        Ok(Self {
            addr,
            state,
            _stop: stop,
            _dir: dir,
        })
    }

    /// The URL of the server.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The backend of the app, which knows the secrets of the server.
    pub fn backend(&self) -> Backend {
        let config = &self.state.config;
        Backend {
            url: self.url(),
            auth_secret: config.auth_secret.as_str().into(),
            admin_secret: config.admin_secret.as_str().into(),
            http: reqwest::Client::new(),
        }
    }

    /// The oldest sequence that the log of `room_id` retains, and its head, as the operator of
    /// the server sees them.
    pub async fn log(&self, room_id: &RoomId) -> Result<(u64, u64)> {
        let bounds = self.state.room_manager.log_bounds(room_id).await?;
        Ok((bounds.tail_seq.get(), bounds.head_seq.get()))
    }

    /// The sequence of the snapshot that the relay serves for `room_id`, if it has one.
    pub fn snapshot(&self, room_id: &RoomId) -> Option<u64> {
        let relay = &self.state.snapshot_relay;
        relay.active_snapshot_seq(room_id).map(|seq| seq.get())
    }
}

/// How much of its log a room keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    /// As the server does by default: segments of 1000 operations, so the few of the example
    /// are never pruned.
    Default,
    /// A segment of 5 operations, which the room deletes once every member that is connected
    /// acknowledged it: its log is pruned soon, and a new member catches up from a snapshot.
    Short,
}

/// The backend of the app, which the app trusts and the server knows by its secrets: it
/// provisions the schema and the rooms through the Control Plane of the server, with the admin
/// secret, and issues the token of each device for each room, with the secret of the cluster, as
/// the token provider of each client.
///
/// A real backend runs apart, gives a token only to a person allowed in the room, and decides
/// how long it lasts; this one gives one for ten minutes to every device that asks. And a real
/// device asks its backend for each token over the network, with a token provider that only
/// knows how to reach it: it never has the secrets of the server, which here the backend keeps
/// in the process of the example, next to the devices.
#[derive(Clone)]
pub struct Backend {
    url: String,
    auth_secret: Arc<str>,
    admin_secret: Arc<str>,
    http: reqwest::Client,
}

impl Backend {
    /// Registers [`schema`] as [`SCHEMA_ID`].
    pub async fn register_schema(&self) -> Result<()> {
        let body = serde_json::json!({ "schema_id": SCHEMA_ID, "schema": schema() });
        self.admin("schemas", body).await
    }

    /// Creates `room_id` with the schema of the app, which keeps its log as `retention` says.
    pub async fn create_room(&self, room_id: &str, retention: Retention) -> Result<RoomId> {
        let lifecycle = match retention {
            Retention::Default => serde_json::json!({}),
            Retention::Short => serde_json::json!({ "ram_max_ops": 5 }),
        };
        let body = serde_json::json!({
            "room_id": room_id,
            "schema_id": SCHEMA_ID,
            "lifecycle": lifecycle,
        });
        self.admin("rooms", body).await?;
        Ok(RoomId::new(room_id)?)
    }

    /// Posts `body` to `path` of the Control Plane.
    async fn admin(&self, path: &str, body: serde_json::Value) -> Result<()> {
        let request = self.http.post(format!("{}/admin/{path}", self.url));
        let response = request
            .bearer_auth(&*self.admin_secret)
            .header(CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&body)?)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await?;
            return Err(format!("POST /admin/{path}: {status}: {text}").into());
        }
        Ok(())
    }
}

#[async_trait]
impl TokenProvider for Backend {
    async fn token(&self, client_id: &ClientId, room_id: &RoomId) -> Result<String, TokenError> {
        let ttl = Duration::from_secs(600);
        Ok(generate_client_token(
            client_id,
            room_id,
            ttl,
            &self.auth_secret,
        ))
    }
}

impl fmt::Debug for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------------------------
// The key of the notes

/// The length of the nonce of a ciphertext of a [`NotesKey`].
const NONCE_LEN: usize = 16;

/// The length of the tag of a ciphertext of a [`NotesKey`].
const TAG_LEN: usize = 32;

/// The key with which the members encrypt the notes of the rounds, which they share among
/// themselves, outside the server (with the invitation to the room, say), and which the server
/// never has.
///
/// An authenticated encryption over BLAKE3, for the example only: an app brings a vetted AEAD
/// (AES-GCM, XChaCha20-Poly1305) and its own management of the keys. A ciphertext is a nonce of
/// 16 bytes (those of a UUID v4, 122 of their bits at random), the plaintext masked with the
/// keyed stream of BLAKE3 for that nonce, and a tag of the additional data, the nonce and the
/// masked plaintext, with a key of its own: decrypting fails unless the tag is that of the
/// additional data it is given, which the SDK makes of the room, the table, the key of the row
/// and the column, so a ciphertext copied to another cell does not decrypt.
#[derive(Clone)]
pub struct NotesKey {
    stream: [u8; 32],
    tag: [u8; 32],
}

impl NotesKey {
    /// The key of `secret`.
    pub fn new(secret: &[u8]) -> Self {
        Self {
            stream: blake3::derive_key("zemdb ranking example 2026-11 notes stream", secret),
            tag: blake3::derive_key("zemdb ranking example 2026-11 notes tag", secret),
        }
    }

    /// The key that the friends of the example share.
    pub fn of_friends() -> Self {
        Self::new(b"the secret that came with the invitation")
    }

    /// `bytes` masked with the stream of `nonce`, which unmasks them too.
    fn mask(&self, nonce: &[u8], bytes: &[u8]) -> Vec<u8> {
        let mut stream = vec![0; bytes.len()];
        let mut hasher = blake3::Hasher::new_keyed(&self.stream);
        hasher.update(nonce).finalize_xof().fill(&mut stream);
        (bytes.iter().zip(stream))
            .map(|(byte, mask)| byte ^ mask)
            .collect()
    }

    fn tag(&self, aad: &[u8], nonce: &[u8], masked: &[u8]) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new_keyed(&self.tag);
        let aad_len = u64::try_from(aad.len()).unwrap_or(u64::MAX);
        hasher.update(&aad_len.to_le_bytes());
        hasher.update(aad).update(nonce).update(masked);
        hasher.finalize()
    }
}

#[async_trait]
impl CryptoEngine for NotesKey {
    async fn encrypt(
        &self,
        _room_id: &RoomId,
        aad: &[u8],
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        let nonce = *Uuid::new_v4().as_bytes();
        let masked = self.mask(&nonce, plaintext);
        let tag = self.tag(aad, &nonce, &masked);
        Ok([&nonce[..], &masked, tag.as_bytes()].concat())
    }

    async fn decrypt(
        &self,
        _room_id: &RoomId,
        aad: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        let Some(masked_len) = ciphertext.len().checked_sub(NONCE_LEN + TAG_LEN) else {
            return Err(CryptoError::Decryption("Too short".to_string()));
        };
        let (nonce, rest) = ciphertext.split_at(NONCE_LEN);
        let (masked, tag) = rest.split_at(masked_len);
        // In constant time.
        if !self.tag(aad, nonce, masked).eq(tag) {
            return Err(CryptoError::Decryption("Not authentic".to_string()));
        }
        Ok(self.mask(nonce, masked))
    }
}

impl fmt::Debug for NotesKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotesKey").finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------------------------
// The devices and their network

/// A device of a person: a client of the facade over a replica on disk, with its metadata next
/// to it, that reaches the server through a network of its own, with the backend as its token
/// provider and the key of the friends for the notes.
pub struct Device {
    client: ZemdbClient,
    network: Network,
    _dir: TempDir,
}

impl Device {
    /// A new device of `person`, with the configuration of the example (a heartbeat every
    /// 200 ms, and retries within a second, for a local server) that `change` changes.
    pub async fn with(
        server: &Server,
        person: &str,
        change: impl FnOnce(&mut ClientConfig),
    ) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let network = Network::to(server.addr).await?;
        let replica = DiskStorageOptions::new(dir.path().join("replica"));
        let storage = DiskStorageEngine::new(replica)?;
        let metadata = FileMetadata::new(dir.path().join("metadata"));
        let transport = HttpTransport::new(HttpTransportConfig::new(network.url()))?;
        let mut config = ClientConfig {
            heartbeat_interval: Duration::from_millis(200),
            retry_base: Duration::from_millis(200),
            retry_max: Duration::from_secs(1),
            ..ClientConfig::new(person)
        };
        change(&mut config);
        let key: Arc<dyn CryptoEngine> = Arc::new(NotesKey::of_friends());
        let client = ZemdbClient::open(
            config,
            storage,
            metadata,
            transport,
            server.backend(),
            Some(key),
        );
        Ok(Self {
            client: client.await?,
            network,
            _dir: dir,
        })
    }

    /// The client of the device.
    pub fn client(&self) -> &ZemdbClient {
        &self.client
    }

    /// The network of the device.
    pub fn network(&self) -> &Network {
        &self.network
    }

    /// Opens `room_id`, and starts its worker.
    pub async fn room(&self, room_id: &RoomId) -> Result<RoomHandle, ClientError> {
        self.client.room(room_id.clone()).await
    }
}

/// The network between a device and the server: a front on a socket of its own that forwards
/// each connection to the server, until it is cut. Cut, it closes the connections it forwards,
/// and every new one at once, as a device without network (in airplane mode, say) finds it:
/// each request fails at once, instead of waiting for a reply that never comes, as on a network
/// that drops what it is sent.
pub struct Network {
    addr: SocketAddr,
    /// Whether it is cut, and the connections it forwards, under one lock.
    state: Arc<Mutex<Links>>,
    accepting: JoinHandle<()>,
}

#[derive(Default)]
struct Links {
    cut: bool,
    forwarded: Vec<JoinHandle<()>>,
}

impl Network {
    async fn to(server: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let state: Arc<Mutex<Links>> = Arc::default();
        let links = Arc::clone(&state);
        let accepting = tokio::spawn(async move {
            while let Ok((mut device, _)) = listener.accept().await {
                let mut links = links.lock().unwrap_or_else(PoisonError::into_inner);
                if links.cut {
                    // Closed at once.
                    continue;
                }
                let forward = tokio::spawn(async move {
                    if let Ok(mut server) = TcpStream::connect(server).await {
                        let _ = tokio::io::copy_bidirectional(&mut device, &mut server).await;
                    }
                });
                links.forwarded.retain(|forward| !forward.is_finished());
                links.forwarded.push(forward);
            }
        });
        Ok(Self {
            addr,
            state,
            accepting,
        })
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Cuts the network.
    pub fn cut(&self) {
        let mut links = self.links();
        links.cut = true;
        for forward in links.forwarded.drain(..) {
            forward.abort();
        }
    }

    /// Brings the network back.
    pub fn restore(&self) {
        self.links().cut = false;
    }

    fn links(&self) -> std::sync::MutexGuard<'_, Links> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        self.accepting.abort();
        self.cut();
    }
}

// ---------------------------------------------------------------------------------------------
// What the app does

/// The value of the id `id` of a ranking or a round.
fn id(id: Uuid) -> Value {
    Value::from(*id.as_bytes())
}

/// Creates a ranking named `name`, of the person of the client, and returns its id. Its creator
/// is not a member until they join it.
pub async fn create_ranking(room: &RoomHandle, name: &str) -> Result<Uuid, ClientError> {
    let ranking = Uuid::new_v4();
    let row = Row::new()
        .with("ranking_id", id(ranking))
        .with("name", name);
    room.table("rankings")?.insert(&row).await?;
    Ok(ranking)
}

/// Deletes `ranking`, of the person of the client.
pub async fn delete_ranking(room: &RoomHandle, ranking: Uuid) -> Result<(), ClientError> {
    let key = Key::new().with("ranking_id", id(ranking));
    room.table("rankings")?.delete(&key).await
}

/// Joins `ranking` as `display_name`. The replica has to hold the ranking: a member invited to it
/// catches up first ([`RoomHandle::sync`]).
pub async fn join(room: &RoomHandle, ranking: Uuid, display_name: &str) -> Result<(), ClientError> {
    let row = Row::new()
        .with("ranking_id", id(ranking))
        .with("display_name", display_name);
    room.table("members")?.insert(&row).await.map(drop)
}

/// Leaves `ranking`.
pub async fn leave(room: &RoomHandle, ranking: Uuid) -> Result<(), ClientError> {
    let key = Key::new().with("ranking_id", id(ranking));
    room.table("members")?.delete(&key).await
}

/// Opens a round of `ranking` titled `title`, with `notes` that only the members read, and
/// returns its id.
pub async fn open_round(
    room: &RoomHandle,
    ranking: Uuid,
    title: &str,
    notes: &str,
) -> Result<Uuid, ClientError> {
    let round = Uuid::new_v4();
    let row = Row::new()
        .with("ranking_id", id(ranking))
        .with("round_id", id(round))
        .with("title", title)
        .with("notes", notes);
    room.table("rounds")?.insert(&row).await?;
    Ok(round)
}

/// The title and the notes of `round` of `ranking`, in the replica, with the notes decrypted, or
/// none if they are `NULL`.
pub async fn round_of(
    room: &RoomHandle,
    ranking: Uuid,
    round: Uuid,
) -> Result<Option<(String, Option<String>)>, ClientError> {
    let key = Key::new()
        .with("ranking_id", id(ranking))
        .with("round_id", id(round));
    let row = room.table("rounds")?.get(&key).await?;
    let text = |row: &Row, column| row.get(column).and_then(Value::as_str).map(str::to_string);
    Ok(row.and_then(|row| Some((text(&row, "title")?, text(&row, "notes")))))
}

/// Votes for `candidate` in `round` of `ranking`.
pub async fn vote(
    room: &RoomHandle,
    ranking: Uuid,
    round: Uuid,
    candidate: &str,
) -> Result<(), ClientError> {
    let row = Row::new()
        .with("ranking_id", id(ranking))
        .with("round_id", id(round))
        .with("candidate", candidate);
    room.table("votes")?.insert(&row).await.map(drop)
}

/// Whom `voter` voted for in `round` of `ranking`, in the replica.
pub async fn vote_of(
    room: &RoomHandle,
    voter: &str,
    ranking: Uuid,
    round: Uuid,
) -> Result<Option<String>, ClientError> {
    let key = Key::new()
        .with("voter", voter)
        .with("ranking_id", id(ranking))
        .with("round_id", id(round));
    let vote = room.table("votes")?.get(&key).await?;
    let candidate = vote.as_ref().and_then(|vote| vote.get("candidate"));
    Ok(candidate.and_then(Value::as_str).map(str::to_string))
}

/// The points of each member that got votes in a ranking, one per vote, from the most, and in
/// the order of their names on a tie.
pub type Standings = Vec<(String, u64)>;

/// The standings of `ranking`, which the replica counts from the votes of every round.
pub async fn standings(room: &RoomHandle, ranking: Uuid) -> Result<Standings, ClientError> {
    let votes = room.table("votes")?.scan(&votes()).await?;
    Ok(tally(&votes, ranking))
}

/// The standings of `ranking`, and then again each time the votes change: a stream of the votes
/// of the room ([`TableHandle::watch_scan`](zemdb_client::TableHandle::watch_scan)), which the
/// replica counts each time.
pub fn watch_standings(
    room: &RoomHandle,
    ranking: Uuid,
) -> Result<impl Stream<Item = Result<Standings, ClientError>>, ClientError> {
    let votes = room.table("votes")?.watch_scan(&votes());
    Ok(votes.map(move |votes| votes.map(|votes| tally(&votes, ranking))))
}

/// The votes of the room, with the columns that the standings count.
fn votes() -> Scan {
    Scan::new().columns(["ranking_id", "candidate"])
}

fn tally(votes: &[Row], ranking: Uuid) -> Standings {
    let ranking = id(ranking);
    let mut points = BTreeMap::<String, u64>::new();
    for vote in votes {
        if vote.get("ranking_id") != Some(&ranking) {
            continue;
        }
        if let Some(candidate) = vote.get("candidate").and_then(Value::as_str) {
            *points.entry(candidate.to_string()).or_default() += 1;
        }
    }
    let mut standings: Standings = points.into_iter().collect();
    standings.sort_by(|(a, a_points), (b, b_points)| b_points.cmp(a_points).then(a.cmp(b)));
    standings
}

/// The standings of `points`, as [`standings`] gives them.
pub fn standings_of<const N: usize>(points: [(&str, u64); N]) -> Standings {
    let mut standings: Standings = (points.into_iter())
        .map(|(member, points)| (member.to_string(), points))
        .collect();
    standings.sort_by(|(a, a_points), (b, b_points)| b_points.cmp(a_points).then(a.cmp(b)));
    standings
}

/// Whether `bytes` hold `needle`.
pub fn holds(bytes: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || bytes.windows(needle.len()).any(|window| window == needle)
}
