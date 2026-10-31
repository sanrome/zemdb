//! Evaluation 2: many rooms, each with clients that write at a human pace and catch up with
//! what the others write, as the SDK's `sync` does (`/sync` until caught up, then `Ack`).
//!
//! ```text
//! zemdb-perf rooms --addr HOST:PORT --rooms R --clients C --period T [--sse 1] [--pool 512]
//!                  [--warmup S] [--secs S] [--server-pid PID] [--disk vda]
//! ```
//!
//! Each client writes on average every `T` seconds at exponential intervals, without waiting for
//! its previous write (open loop; `--period 0`: no writes). With `--sse 1` each client keeps its
//! room's event stream open on a connection of its own, and every `head_advanced` above its
//! cursor wakes its sync task. Commits, syncs and acks of every client go through a shared pool
//! of keep-alive connections, as behind a proxy. Each row carries its author and the time it was
//! sent, so the propagation latency (from the scheduled send to each other client applying it)
//! needs no shared clock.

use crate::{context_switches, cpu_ticks, insert_with, memory_kib, schema, CLOCK_TICKS_PER_SEC};
use crate::SCHEMA_ID;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::{self, SendRequest};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch, Notify, Semaphore};
use tokio::task::JoinSet;
use zemdb_core::*;
use zemdb_server::api::control_plane::{CreateRoomRequest, CreateSchemaRequest, RoomDetails};
use zemdb_server::generate_client_token;

type Reply = Result<(StatusCode, Bytes), String>;

struct Job {
    method: Method,
    path: Arc<str>,
    auth: Option<Arc<str>>,
    content_type: &'static str,
    body: Bytes,
    enqueued: Instant,
    reply: oneshot::Sender<Reply>,
}

/// A fixed set of keep-alive connections taking requests from one queue. A connection the
/// server closed (idle for longer than its header timeout) is reopened, and a request that
/// failed on it is sent once more on a new one.
#[derive(Clone)]
struct Pool {
    tx: mpsc::Sender<Job>,
    retries: Arc<AtomicU64>,
    /// While set, how long each request waited in the queue for a free connection.
    recording: Arc<AtomicBool>,
    waits: Arc<Mutex<Vec<u32>>>,
}

impl Pool {
    fn start(addr: &str, connections: usize) -> Self {
        let (tx, rx) = mpsc::channel(1 << 17);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let retries = Arc::new(AtomicU64::new(0));
        let recording = Arc::new(AtomicBool::new(false));
        let waits = Arc::new(Mutex::new(Vec::new()));
        for _ in 0..connections {
            tokio::spawn(pool_worker(
                addr.to_string(),
                Arc::clone(&rx),
                Arc::clone(&retries),
                Arc::clone(&recording),
                Arc::clone(&waits),
            ));
        }
        Self {
            tx,
            retries,
            recording,
            waits,
        }
    }

    async fn send(
        &self,
        method: Method,
        path: Arc<str>,
        auth: Option<Arc<str>>,
        content_type: &'static str,
        body: Vec<u8>,
    ) -> Reply {
        let (reply, rx) = oneshot::channel();
        let job = Job {
            method,
            path,
            auth,
            content_type,
            body: Bytes::from(body),
            enqueued: Instant::now(),
            reply,
        };
        self.tx.send(job).await.map_err(|_| "pool closed".to_string())?;
        rx.await.map_err(|_| "pool dropped the request".to_string())?
    }

    async fn post(&self, path: &Arc<str>, auth: &Arc<str>, body: Vec<u8>) -> Reply {
        let (path, auth) = (Arc::clone(path), Some(Arc::clone(auth)));
        self.send(Method::POST, path, auth, "application/octet-stream", body)
            .await
    }
}

async fn try_connect(addr: &str) -> Result<SendRequest<Full<Bytes>>, String> {
    let stream = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
    stream.set_nodelay(true).map_err(|e| e.to_string())?;
    let (sender, conn) = http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|e| e.to_string())?;
    tokio::spawn(conn);
    Ok(sender)
}

async fn pool_worker(
    addr: String,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Job>>>,
    retries: Arc<AtomicU64>,
    recording: Arc<AtomicBool>,
    waits: Arc<Mutex<Vec<u32>>>,
) {
    let mut sender: Option<SendRequest<Full<Bytes>>> = None;
    loop {
        let job = match rx.lock().await.recv().await {
            Some(job) => job,
            None => return,
        };
        if recording.load(Ordering::Relaxed) {
            waits.lock().unwrap().push(micros(job.enqueued.elapsed()));
        }
        let mut result = Err(String::new());
        for attempt in 0..2 {
            if sender.as_ref().map_or(true, SendRequest::is_closed) {
                match try_connect(&addr).await {
                    Ok(new) => sender = Some(new),
                    Err(err) => {
                        result = Err(err);
                        continue;
                    }
                }
            }
            match send_on(sender.as_mut().unwrap(), &job).await {
                Ok(reply) => {
                    result = Ok(reply);
                    break;
                }
                Err(err) => {
                    sender = None;
                    if attempt == 0 {
                        retries.fetch_add(1, Ordering::Relaxed);
                    }
                    result = Err(err);
                }
            }
        }
        let _ = job.reply.send(result);
    }
}

async fn send_on(sender: &mut SendRequest<Full<Bytes>>, job: &Job) -> Reply {
    sender.ready().await.map_err(|e| e.to_string())?;
    let mut req = Request::builder()
        .method(job.method.clone())
        .uri(&*job.path)
        .header("host", "zemdb")
        .header("content-type", job.content_type);
    if let Some(auth) = &job.auth {
        req = req.header("authorization", &**auth);
    }
    let resp = sender
        .send_request(req.body(Full::new(job.body.clone())).unwrap())
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status();
    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| e.to_string())?
        .to_bytes();
    Ok((status, body))
}

/// xorshift64*: enough for the intervals between writes.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn unit(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }

    fn exponential(&mut self, mean: f64) -> f64 {
        -(1.0 - self.unit()).ln() * mean
    }
}

struct Room {
    id: RoomId,
    commit: Arc<str>,
    sync: Arc<str>,
    ack: Arc<str>,
    events: String,
}

struct Client {
    index: i64,
    room: Arc<Room>,
    client_id: ClientId,
    token: String,
    auth: Arc<str>,
    /// The highest sequence applied.
    cursor: Mutex<u64>,
    /// The highest head the client heard of.
    known_head: AtomicU64,
    notify: Notify,
}

#[derive(Default)]
struct Counters {
    commits_ok: AtomicU64,
    commit_errors: AtomicU64,
    syncs: AtomicU64,
    acks: AtomicU64,
    sync_errors: AtomicU64,
    sse_events: AtomicU64,
    sse_errors: AtomicU64,
    has_more: AtomicU64,
    gaps: AtomicU64,
    unavailable: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> [u64; 10] {
        [
            &self.commits_ok,
            &self.commit_errors,
            &self.syncs,
            &self.acks,
            &self.sync_errors,
            &self.sse_events,
            &self.sse_errors,
            &self.has_more,
            &self.gaps,
            &self.unavailable,
        ]
        .map(|c| c.load(Ordering::Relaxed))
    }
}

struct Shared {
    pool: Pool,
    schema: Schema,
    epoch: Instant,
    nonce: u128,
    window: OnceLock<(Instant, Instant)>,
    next_id: AtomicU64,
    correlation: AtomicU64,
    counters: Counters,
    commit_latency: Mutex<Vec<u32>>,
    propagation: Mutex<Vec<u32>>,
    send_lag: Mutex<Vec<u32>>,
    first_error: Mutex<Option<String>>,
}

impl Shared {
    fn in_window(&self, at: Instant) -> bool {
        self.window
            .get()
            .is_some_and(|&(start, end)| at >= start && at < end)
    }

    fn now_us(&self) -> i64 {
        self.epoch.elapsed().as_micros() as i64
    }

    fn correlation_id(&self) -> CorrelationId {
        CorrelationId::new(self.correlation.fetch_add(1, Ordering::Relaxed))
    }

    fn error(&self, what: &str, detail: impl std::fmt::Debug) {
        let mut first = self.first_error.lock().unwrap();
        if first.is_none() {
            let mut text = format!("{what}: {detail:?}");
            text.truncate(400);
            *first = Some(text);
        }
    }
}

fn micros(d: Duration) -> u32 {
    u32::try_from(d.as_micros()).unwrap_or(u32::MAX)
}

/// Applies the operations above the client's cursor, recording the propagation latency of
/// those written by other clients.
fn apply(shared: &Shared, client: &Client, ops: &[SequencedOperation]) {
    let now = Instant::now();
    let now_us = shared.now_us();
    let in_window = shared.in_window(now);
    let mut samples = Vec::new();
    {
        let mut cursor = client.cursor.lock().unwrap();
        for op in ops {
            let seq = op.seq.get();
            if seq <= *cursor {
                continue;
            }
            if seq != *cursor + 1 {
                shared.counters.gaps.fetch_add(1, Ordering::Relaxed);
            }
            *cursor = seq;
            if let OperationKind::Insert { row } = op.op.kind() {
                if let (Some(Value::Int(author)), Some(Value::Int(sent))) = (row.get(2), row.get(4)) {
                    if *author != client.index && in_window {
                        samples.push(u32::try_from(now_us - sent).unwrap_or(u32::MAX));
                    }
                }
            }
        }
    }
    if !samples.is_empty() {
        shared.propagation.lock().unwrap().extend(samples);
    }
}

async fn commit_one(shared: Arc<Shared>, client: Arc<Client>, scheduled: Instant) {
    let sent = Instant::now();
    let lag = sent.saturating_duration_since(scheduled);
    let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
    let op = insert_with(&shared.schema, id as i64, client.index, shared.now_us());
    let last_ack = *client.cursor.lock().unwrap();
    let msg = ClientMessage::Commit {
        correlation_id: shared.correlation_id(),
        room_id: client.room.id.clone(),
        client_id: client.client_id.clone(),
        mutation_id: MutationId::from_u128(shared.nonce | u128::from(id)),
        last_ack_seq: SequenceNumber::new(last_ack),
        op,
    };
    let reply = shared
        .pool
        .post(&client.room.commit, &client.auth, encode_message(&msg).unwrap())
        .await;
    let done = Instant::now();
    match reply.map(|(status, body)| (status, decode_message::<ServerMessage>(&body))) {
        Ok((StatusCode::OK, Ok(ServerMessage::CommitAck { catchup_ops, has_more, .. }))) => {
            shared.counters.commits_ok.fetch_add(1, Ordering::Relaxed);
            if shared.in_window(done) {
                shared.commit_latency.lock().unwrap().push(micros(done - sent));
                shared.send_lag.lock().unwrap().push(micros(lag));
            }
            apply(&shared, &client, &catchup_ops);
            if has_more {
                shared.counters.has_more.fetch_add(1, Ordering::Relaxed);
                client.notify.notify_one();
            }
        }
        other => {
            shared.counters.commit_errors.fetch_add(1, Ordering::Relaxed);
            shared.error("commit", other);
        }
    }
}

/// Writes at exponential intervals of mean `period` from a random offset, without waiting for
/// the previous write.
async fn writer(shared: Arc<Shared>, client: Arc<Client>, period: f64, start: Instant, end: Instant) {
    let mut rng = Rng::new(client.index as u64 + 1);
    let mut next = start + Duration::from_secs_f64(rng.unit() * period);
    while next < end {
        tokio::time::sleep_until(next.into()).await;
        tokio::spawn(commit_one(Arc::clone(&shared), Arc::clone(&client), next));
        next += Duration::from_secs_f64(rng.exponential(period));
    }
}

/// On every wake-up, syncs until caught up with the highest head heard of, then acks.
async fn syncer(shared: Arc<Shared>, client: Arc<Client>, mut stop: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            () = client.notify.notified() => {}
            _ = stop.changed() => return,
        }
        let mut backoff = Duration::from_millis(20);
        loop {
            let from = *client.cursor.lock().unwrap();
            if client.known_head.load(Ordering::Acquire) <= from {
                break;
            }
            let msg = ClientMessage::Sync {
                correlation_id: shared.correlation_id(),
                room_id: client.room.id.clone(),
                client_id: client.client_id.clone(),
                from_seq: SequenceNumber::new(from),
                max_batch_size: u32::MAX,
            };
            let reply = shared
                .pool
                .post(&client.room.sync, &client.auth, encode_message(&msg).unwrap())
                .await;
            match reply.map(|(status, body)| (status, decode_message::<ServerMessage>(&body))) {
                Ok((StatusCode::OK, Ok(ServerMessage::SyncBatch { head_seq, ops, has_more, .. }))) => {
                    shared.counters.syncs.fetch_add(1, Ordering::Relaxed);
                    client.known_head.fetch_max(head_seq.get(), Ordering::AcqRel);
                    apply(&shared, &client, &ops);
                    if has_more {
                        continue;
                    }
                }
                other => {
                    retry_later(&shared, "sync", other, &mut backoff).await;
                    continue;
                }
            }
            let ack_seq = *client.cursor.lock().unwrap();
            let msg = ClientMessage::Ack {
                correlation_id: shared.correlation_id(),
                room_id: client.room.id.clone(),
                client_id: client.client_id.clone(),
                ack_seq: SequenceNumber::new(ack_seq),
            };
            let reply = shared
                .pool
                .post(&client.room.ack, &client.auth, encode_message(&msg).unwrap())
                .await;
            match reply.map(|(status, body)| (status, decode_message::<ServerMessage>(&body))) {
                Ok((StatusCode::OK, Ok(ServerMessage::AckConfirmed { head_seq, .. }))) => {
                    shared.counters.acks.fetch_add(1, Ordering::Relaxed);
                    client.known_head.fetch_max(head_seq.get(), Ordering::AcqRel);
                }
                other => {
                    retry_later(&shared, "ack", other, &mut backoff).await;
                    continue;
                }
            }
        }
    }
}

/// Counts a failed sync or ack and waits before retrying, doubling the wait up to 1 s, as the
/// SDK backs off.
async fn retry_later(
    shared: &Shared,
    what: &str,
    reply: Result<(StatusCode, Result<ServerMessage, DecodeError>), String>,
    backoff: &mut Duration,
) {
    shared.counters.sync_errors.fetch_add(1, Ordering::Relaxed);
    if matches!(
        &reply,
        Ok((_, Ok(ServerMessage::Error { code: ErrorCode::Unavailable, .. })))
    ) {
        shared.counters.unavailable.fetch_add(1, Ordering::Relaxed);
    }
    shared.error(what, reply);
    tokio::time::sleep(*backoff).await;
    *backoff = (*backoff * 2).min(Duration::from_secs(1));
}

/// Keeps the client's event stream open, waking its sync task on every head above its cursor.
async fn sse_reader(
    shared: Arc<Shared>,
    client: Arc<Client>,
    addr: String,
    connected: oneshot::Sender<bool>,
) {
    let fail = |shared: &Shared, what: &str, detail: String| {
        shared.counters.sse_errors.fetch_add(1, Ordering::Relaxed);
        shared.error(what, detail);
    };
    let mut sender = match try_connect(&addr).await {
        Ok(sender) => sender,
        Err(err) => {
            fail(&shared, "sse connect", err);
            let _ = connected.send(false);
            return;
        }
    };
    let req = Request::get(&client.room.events)
        .header("host", "zemdb")
        .header("authorization", &*client.auth)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = match sender.send_request(req).await {
        Ok(resp) if resp.status() == StatusCode::OK => resp,
        Ok(resp) => {
            fail(&shared, "sse status", resp.status().to_string());
            let _ = connected.send(false);
            return;
        }
        Err(err) => {
            fail(&shared, "sse request", err.to_string());
            let _ = connected.send(false);
            return;
        }
    };
    let _ = connected.send(true);
    let mut body = resp.into_body();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else {
            fail(&shared, "sse body", "stream error".to_string());
            return;
        };
        let Ok(data) = frame.into_data() else { continue };
        buf.extend_from_slice(&data);
        while let Some(end) = buf.windows(2).position(|w| w == b"\n\n") {
            let block = String::from_utf8_lossy(&buf[..end]).into_owned();
            buf.drain(..end + 2);
            let mut event = "";
            let mut payload = "";
            for line in block.lines() {
                if let Some(v) = line.strip_prefix("event:") {
                    event = v.trim();
                } else if let Some(v) = line.strip_prefix("data:") {
                    payload = v.trim();
                }
            }
            if event == "head_advanced" {
                shared.counters.sse_events.fetch_add(1, Ordering::Relaxed);
                if let Ok(seq) = payload.parse::<u64>() {
                    client.known_head.fetch_max(seq, Ordering::AcqRel);
                    if seq > *client.cursor.lock().unwrap() {
                        client.notify.notify_one();
                    }
                }
            }
        }
    }
    drop(sender);
}

/// Writes, flush requests and milliseconds busy of `disk`, from `/proc/diskstats`.
fn disk_stats(disk: &str) -> [u64; 3] {
    let stats = std::fs::read_to_string("/proc/diskstats").unwrap_or_default();
    stats
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>())
        .find(|f| f.get(2) == Some(&disk))
        .map(|f| {
            let n = |i: usize| f.get(i).and_then(|v| v.parse().ok()).unwrap_or(0);
            [n(7), n(18), n(12)]
        })
        .unwrap_or([0; 3])
}

fn count_entries(path: String) -> usize {
    std::fs::read_dir(path).map(|d| d.count()).unwrap_or(0)
}

fn percentiles(mut v: Vec<u32>) -> serde_json::Value {
    v.sort_unstable();
    let at = |p: f64| {
        v.get(((v.len() as f64 * p) as usize).min(v.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    serde_json::json!({
        "n": v.len(),
        "p50_us": at(0.50),
        "p99_us": at(0.99),
        "p999_us": at(0.999),
        "max_us": v.last().copied().unwrap_or(0),
    })
}

pub async fn rooms_mode(args: &HashMap<String, String>) {
    let arg = |name: &str, default: &str| args.get(name).cloned().unwrap_or(default.to_string());
    let addr = arg("addr", "127.0.0.1:18080");
    let rooms: usize = arg("rooms", "1").parse().unwrap();
    let clients: usize = arg("clients", "1").parse().unwrap();
    let period: f64 = arg("period", "0").parse().unwrap();
    let sse = arg("sse", "0") == "1";
    let pool_size: usize = arg("pool", "512").parse().unwrap();
    let warmup = Duration::from_secs(arg("warmup", "15").parse().unwrap());
    let secs = Duration::from_secs(arg("secs", "60").parse().unwrap());
    let server_pid: u32 = arg("server-pid", &std::process::id().to_string()).parse().unwrap();
    let disk = arg("disk", "vda");
    let auth_secret = std::env::var("ZEMDB_AUTH_SECRET").unwrap();
    let admin: Arc<str> = format!("Bearer {}", std::env::var("ZEMDB_ADMIN_SECRET").unwrap()).into();

    let pool = Pool::start(&addr, pool_size);
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() << 64;
    let shared = Arc::new(Shared {
        pool: pool.clone(),
        schema: schema(),
        epoch: Instant::now(),
        nonce,
        window: OnceLock::new(),
        next_id: AtomicU64::new(0),
        correlation: AtomicU64::new(1),
        counters: Counters::default(),
        commit_latency: Mutex::new(Vec::new()),
        propagation: Mutex::new(Vec::new()),
        send_lag: Mutex::new(Vec::new()),
        first_error: Mutex::new(None),
    });

    let setup_start = Instant::now();
    let schema_id = SchemaId::new(SCHEMA_ID).unwrap();
    let body = serde_json::to_vec(&CreateSchemaRequest {
        schema_id: schema_id.clone(),
        schema: schema(),
    })
    .unwrap();
    let (status, body) = pool
        .send(Method::POST, "/admin/schemas".into(), Some(Arc::clone(&admin)), "application/json", body)
        .await
        .unwrap();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::CONFLICT,
        "create schema: {status} {body:?}"
    );

    let room_list: Vec<Arc<Room>> = (0..rooms)
        .map(|r| {
            let id = RoomId::new(format!("e2-r{r}")).unwrap();
            Arc::new(Room {
                commit: format!("/rooms/{id}/commit").into(),
                sync: format!("/rooms/{id}/sync").into(),
                ack: format!("/rooms/{id}/ack").into(),
                events: format!("/rooms/{id}/events"),
                id,
            })
        })
        .collect();
    let mut set = JoinSet::new();
    for room in &room_list {
        let body = serde_json::to_vec(&CreateRoomRequest {
            room_id: room.id.clone(),
            schema_id: schema_id.clone(),
            lifecycle: None,
        })
        .unwrap();
        let (pool, admin) = (pool.clone(), Arc::clone(&admin));
        set.spawn(async move {
            pool.send(Method::POST, "/admin/rooms".into(), Some(admin), "application/json", body)
                .await
        });
    }
    while let Some(created) = set.join_next().await {
        let (status, body) = created.unwrap().unwrap();
        assert!(status.is_success(), "create room: {status} {body:?}");
    }

    let mut client_list: Vec<Arc<Client>> = Vec::with_capacity(rooms * clients);
    for (r, room) in room_list.iter().enumerate() {
        for c in 0..clients {
            let client_id = ClientId::new(format!("perf#r{r}c{c}")).unwrap();
            let token =
                generate_client_token(&client_id, &room.id, Duration::from_secs(7200), &auth_secret);
            client_list.push(Arc::new(Client {
                index: (r * clients + c) as i64,
                auth: format!("Bearer {token}").into(),
                token,
                room: Arc::clone(room),
                client_id,
                cursor: Mutex::new(0),
                known_head: AtomicU64::new(0),
                notify: Notify::new(),
            }));
        }
    }
    let mut set = JoinSet::new();
    for client in &client_list {
        let msg = ClientMessage::RegisterClient {
            correlation_id: shared.correlation_id(),
            room_id: client.room.id.clone(),
            client_id: client.client_id.clone(),
            auth_token: client.token.clone(),
            current_seq: None,
            known_incarnation: None,
        };
        let path: Arc<str> = format!("/rooms/{}/register", client.room.id).into();
        let pool = pool.clone();
        set.spawn(async move {
            pool.send(Method::POST, path, None, "application/octet-stream", encode_message(&msg).unwrap())
                .await
        });
    }
    while let Some(registered) = set.join_next().await {
        let (status, body) = registered.unwrap().unwrap();
        assert_eq!(status, StatusCode::OK, "register: {body:?}");
    }

    let mut sse_tasks = Vec::new();
    let mut sse_open = 0;
    if sse {
        let gate = Arc::new(Semaphore::new(256));
        let mut waits = Vec::new();
        for client in &client_list {
            let permit = Arc::clone(&gate).acquire_owned().await.unwrap();
            let (tx, rx) = oneshot::channel();
            sse_tasks.push(tokio::spawn(sse_reader(
                Arc::clone(&shared),
                Arc::clone(client),
                addr.clone(),
                tx,
            )));
            waits.push(tokio::spawn(async move {
                let ok = rx.await.unwrap_or(false);
                drop(permit);
                ok
            }));
        }
        for wait in waits {
            sse_open += usize::from(wait.await.unwrap());
        }
    }
    let setup_secs = setup_start.elapsed().as_secs_f64();

    let load_start = Instant::now();
    let window_start = load_start + warmup;
    let window_end = window_start + secs;
    shared.window.set((window_start, window_end)).unwrap();
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut syncers = Vec::new();
    let mut writers = Vec::new();
    for client in &client_list {
        syncers.push(tokio::spawn(syncer(
            Arc::clone(&shared),
            Arc::clone(client),
            stop_rx.clone(),
        )));
        if period > 0.0 {
            writers.push(tokio::spawn(writer(
                Arc::clone(&shared),
                Arc::clone(client),
                period,
                load_start,
                window_end,
            )));
        }
    }

    let own_pid = std::process::id();
    tokio::time::sleep_until(window_start.into()).await;
    pool.recording.store(true, Ordering::Relaxed);
    let before = (
        cpu_ticks(server_pid),
        cpu_ticks(own_pid),
        context_switches(server_pid),
        disk_stats(&disk),
        shared.counters.snapshot(),
        pool.retries.load(Ordering::Relaxed),
    );
    tokio::time::sleep_until(window_end.into()).await;
    pool.recording.store(false, Ordering::Relaxed);
    let after = (
        cpu_ticks(server_pid),
        cpu_ticks(own_pid),
        context_switches(server_pid),
        disk_stats(&disk),
        shared.counters.snapshot(),
        pool.retries.load(Ordering::Relaxed),
    );
    let (rss_kib, hwm_kib) = memory_kib(server_pid);
    let threads = count_entries(format!("/proc/{server_pid}/task"));
    let fds = count_entries(format!("/proc/{server_pid}/fd"));

    for writer in writers {
        writer.await.unwrap();
    }
    // Lets the commits and syncs in flight finish before checking the heads.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut heads = HashMap::new();
    for room in &room_list {
        let (status, body) = pool
            .send(
                Method::GET,
                format!("/admin/rooms/{}", room.id).into(),
                Some(Arc::clone(&admin)),
                "application/json",
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        let details: RoomDetails = serde_json::from_slice(&body).unwrap();
        heads.insert(room.id.clone(), details.metrics.head_seq.get());
    }
    let head_total: u64 = heads.values().sum();
    let behind = client_list
        .iter()
        .filter(|c| *c.cursor.lock().unwrap() < heads[&c.room.id])
        .count();
    stop_tx.send(true).unwrap();
    for task in syncers {
        task.await.unwrap();
    }
    for task in sse_tasks {
        task.abort();
    }

    let window = secs.as_secs_f64();
    let rate = |i: usize| (after.4[i] - before.4[i]) as f64 / window;
    let cpu = |b: u64, a: u64| (a - b) as f64 / CLOCK_TICKS_PER_SEC / window * 100.0;
    let commits_in_window = (after.4[0] - before.4[0]).max(1);
    let totals = shared.counters.snapshot();
    let out = serde_json::json!({
        "mode": "rooms",
        "rooms": rooms,
        "clients_per_room": clients,
        "period_secs": period,
        "sse": sse,
        "sse_open": sse_open,
        "pool": pool_size,
        "secs": window,
        "setup_secs": setup_secs,
        "commits_per_sec": rate(0),
        "syncs_per_sec": rate(2),
        "acks_per_sec": rate(3),
        "sse_events_per_sec": rate(5),
        "commit_latency": percentiles(std::mem::take(&mut *shared.commit_latency.lock().unwrap())),
        "propagation": percentiles(std::mem::take(&mut *shared.propagation.lock().unwrap())),
        "send_lag": percentiles(std::mem::take(&mut *shared.send_lag.lock().unwrap())),
        "pool_wait": percentiles(std::mem::take(&mut *pool.waits.lock().unwrap())),
        "commit_errors": totals[1],
        "sync_errors": totals[4],
        "sse_errors": totals[6],
        "has_more": totals[7],
        "gaps": totals[8],
        "unavailable": totals[9],
        "retries": after.5 - before.5,
        "first_error": *shared.first_error.lock().unwrap(),
        "acked_total": totals[0],
        "head_total": head_total,
        "head_ok": head_total == totals[0],
        "clients_behind_at_end": behind,
        "server_cpu_pct": cpu(before.0, after.0),
        "gen_cpu_pct": cpu(before.1, after.1),
        "server_ctx_switches_per_commit": (after.2 - before.2) as f64 / commits_in_window as f64,
        "server_rss_kib": rss_kib,
        "server_hwm_kib": hwm_kib,
        "server_threads": threads,
        "server_fds": fds,
        "disk_writes_per_sec": (after.3[0] - before.3[0]) as f64 / window,
        "disk_flushes_per_sec": (after.3[1] - before.3[1]) as f64 / window,
        "disk_busy_pct": (after.3[2] - before.3[2]) as f64 / (window * 10.0),
    });
    println!("{out}");
}
