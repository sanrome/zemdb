//! Load generator of the performance evaluations. Spike code, not merged to `main`: see
//! `docs/proposals/2026-10-evaluaciones-performance.md`.
//!
//! ```text
//! zemdb-perf http   --addr HOST:PORT --room ROOM --k K [--warmup S] [--secs S] [--server-pid PID]
//! zemdb-perf inproc --data-dir DIR --k K [--warmup S] [--secs S]
//! ```
//!
//! `http` creates the schema and the room in a running server (secrets from `ZEMDB_AUTH_SECRET`
//! and `ZEMDB_ADMIN_SECRET`), registers one client and commits with `k` requests in flight,
//! each worker over its own HTTP/1.1 keep-alive connection. `inproc` builds the server's room
//! manager in this process, as `main` does, and sends the same commits to the room actor,
//! without HTTP. Both print one JSON line with the results.

mod rooms;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use zemdb_core::*;
use zemdb_server::api::control_plane::{CreateRoomRequest, CreateSchemaRequest, RoomDetails};
use zemdb_server::{
    generate_client_token, RoomCommand, RoomManager, SchemaRegistry, ServerConfig, SnapshotRelay,
};

const SCHEMA_ID: &str = "e1-schema";
const TABLE: &str = "items";
const CLIENT_ID: &str = "perf#e1";
const TEXT: &str = concat!(
    "0123456789", "0123456789", "0123456789", "0123456789", "0123456789",
    "0123456789", "0123456789", "0123456789", "0123456789", "0123456789",
);

/// Table `items`: an int key, a text of 100 bytes, an int, a bool and an int timestamp.
fn schema() -> Schema {
    let table = TableSchema::builder(TABLE)
        .primary_key("id", DataType::Int)
        .column("text", DataType::String)
        .column("count", DataType::Int)
        .column("done", DataType::Bool)
        .column("ts", DataType::Int);
    Schema::builder().table(table).build()
}

fn insert(schema: &Schema, id: i64) -> Operation {
    insert_with(schema, id, id, 1_700_000_000_000)
}

/// An insert of row `id` with `count` and `ts` given: Evaluation 2 stores the author there and
/// the time the row was sent.
fn insert_with(schema: &Schema, id: i64, count: i64, ts: i64) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("text", TEXT)
        .set("count", count)
        .set("done", false)
        .set("ts", ts)
        .build();
    schema.to_operation_insert(TABLE, &row, 1000).unwrap()
}

/// One way of sending a commit: returns the assigned sequence, or what went wrong.
trait Committer: Send + 'static {
    fn commit(
        &mut self,
        mutation_id: MutationId,
        last_ack_seq: u64,
        op: Operation,
    ) -> impl Future<Output = Result<u64, String>> + Send;
}

struct HttpCommitter {
    sender: http1::SendRequest<Full<Bytes>>,
    path: String,
    auth: String,
    room_id: RoomId,
    client_id: ClientId,
    next_correlation: u64,
}

impl Committer for HttpCommitter {
    async fn commit(
        &mut self,
        mutation_id: MutationId,
        last_ack_seq: u64,
        op: Operation,
    ) -> Result<u64, String> {
        self.next_correlation += 1;
        let msg = ClientMessage::Commit {
            correlation_id: CorrelationId::new(self.next_correlation),
            room_id: self.room_id.clone(),
            client_id: self.client_id.clone(),
            mutation_id,
            last_ack_seq: SequenceNumber::new(last_ack_seq),
            op,
        };
        let body = encode_message(&msg).map_err(|e| e.to_string())?;
        self.sender.ready().await.map_err(|e| e.to_string())?;
        let req = Request::post(&self.path)
            .header("host", "zemdb")
            .header("authorization", &self.auth)
            .header("content-type", "application/octet-stream")
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        let resp = self.sender.send_request(req).await.map_err(|e| e.to_string())?;
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .to_bytes();
        match decode_message::<ServerMessage>(&bytes) {
            Ok(ServerMessage::CommitAck { assigned_seq, .. }) if status == StatusCode::OK => {
                Ok(assigned_seq.get())
            }
            other => Err(format!("{status}: {other:?}")),
        }
    }
}

struct InprocCommitter {
    manager: Arc<RoomManager>,
    room_id: RoomId,
    client_id: ClientId,
}

impl Committer for InprocCommitter {
    async fn commit(
        &mut self,
        mutation_id: MutationId,
        last_ack_seq: u64,
        op: Operation,
    ) -> Result<u64, String> {
        let result = self
            .manager
            .ask(&self.room_id, |reply| RoomCommand::Commit {
                client_id: self.client_id.clone(),
                mutation_id,
                last_ack_seq: SequenceNumber::new(last_ack_seq),
                op,
                reply,
            })
            .await;
        match result {
            Ok(Ok(resp)) => Ok(resp.assigned_seq.get()),
            Ok(Err(e)) | Err(e) => Err(e.to_string()),
        }
    }
}

#[derive(Default)]
struct Shared {
    next_id: AtomicI64,
    max_seen: AtomicU64,
}

#[derive(Default)]
struct WorkerResult {
    latencies_us: Vec<u32>,
    acked: u64,
    errors: u64,
    first_error: Option<String>,
    /// Commits that took 10 ms or more, or failed: (ms since the start, ms it took, failed).
    stalls: Vec<(u64, u64, bool)>,
}

/// Commits until `end`, one at a time, recording the latency of the commits that complete in
/// `[warmup_end, end)`. Each commit reports as its cursor the highest sequence any worker has
/// seen acknowledged.
async fn worker<C: Committer>(
    mut committer: C,
    shared: Arc<Shared>,
    schema: Arc<Schema>,
    run_start: Instant,
    warmup_end: Instant,
    end: Instant,
) -> WorkerResult {
    let mut result = WorkerResult::default();
    loop {
        let start = Instant::now();
        if start >= end {
            return result;
        }
        let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
        let last_ack = shared.max_seen.load(Ordering::Acquire);
        let op = insert(&schema, id);
        let mutation_id = MutationId::from_u128((1 << 64) | id as u128);
        let outcome = committer.commit(mutation_id, last_ack, op).await;
        let done = Instant::now();
        if done - start >= Duration::from_millis(10) || outcome.is_err() {
            result.stalls.push((
                (start - run_start).as_millis() as u64,
                (done - start).as_millis() as u64,
                outcome.is_err(),
            ));
        }
        match outcome {
            Ok(seq) => {
                shared.max_seen.fetch_max(seq, Ordering::AcqRel);
                result.acked += 1;
                if done >= warmup_end && done < end {
                    let us = (done - start).as_micros();
                    result.latencies_us.push(u32::try_from(us).unwrap_or(u32::MAX));
                }
            }
            Err(err) => {
                result.errors += 1;
                result.first_error.get_or_insert(err);
            }
        }
    }
}

/// CPU time (user + system) of `pid`, in clock ticks.
fn cpu_ticks(pid: u32) -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let fields: Vec<&str> = stat.rsplit_once(") ").unwrap().1.split(' ').collect();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}

/// Context switches (voluntary and not) of every thread of `pid`.
fn context_switches(pid: u32) -> u64 {
    std::fs::read_dir(format!("/proc/{pid}/task"))
        .unwrap()
        .filter_map(|task| std::fs::read_to_string(task.ok()?.path().join("status")).ok())
        .flat_map(|status| {
            status
                .lines()
                .filter(|line| line.contains("ctxt_switches:"))
                .filter_map(|line| line.split_whitespace().nth(1)?.parse::<u64>().ok())
                .collect::<Vec<_>>()
        })
        .sum()
}

/// `VmRSS` and `VmHWM` of `pid`, in KiB.
fn memory_kib(pid: u32) -> (u64, u64) {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let field = |name: &str| {
        status
            .lines()
            .find(|line| line.starts_with(name))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

const CLOCK_TICKS_PER_SEC: f64 = 100.0;

struct Run {
    k: usize,
    warmup: Duration,
    secs: Duration,
    server_pid: u32,
}

/// Runs the workers and prints the results, with `head_seq` the head of the room afterwards.
async fn run<C: Committer>(
    mode: &str,
    run: &Run,
    committers: Vec<C>,
    head_seq: impl Future<Output = u64>,
) {
    let shared = Arc::new(Shared::default());
    let schema = Arc::new(schema());
    let start = Instant::now();
    let warmup_end = start + run.warmup;
    let end = warmup_end + run.secs;
    let tasks: Vec<_> = committers
        .into_iter()
        .map(|c| {
            tokio::spawn(worker(
                c,
                Arc::clone(&shared),
                Arc::clone(&schema),
                start,
                warmup_end,
                end,
            ))
        })
        .collect();

    let own_pid = std::process::id();
    tokio::time::sleep_until(warmup_end.into()).await;
    let (server_before, own_before) = (cpu_ticks(run.server_pid), cpu_ticks(own_pid));
    let switches_before = context_switches(run.server_pid);
    tokio::time::sleep_until(end.into()).await;
    let (server_after, own_after) = (cpu_ticks(run.server_pid), cpu_ticks(own_pid));
    let switches = context_switches(run.server_pid) - switches_before;
    let (rss_kib, hwm_kib) = memory_kib(run.server_pid);

    let mut latencies = Vec::new();
    let (mut acked, mut errors, mut first_error) = (0, 0, None);
    let mut stalls = Vec::new();
    for task in tasks {
        let r = task.await.unwrap();
        latencies.extend(r.latencies_us);
        acked += r.acked;
        errors += r.errors;
        first_error = first_error.or(r.first_error);
        stalls.extend(r.stalls);
    }
    stalls.sort_unstable();
    latencies.sort_unstable();
    let head = head_seq.await;

    let secs = run.secs.as_secs_f64();
    let pct = |p: f64| {
        latencies
            .get(((latencies.len() as f64 * p) as usize).min(latencies.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    let mean = latencies.iter().map(|&v| u64::from(v)).sum::<u64>() as f64
        / latencies.len().max(1) as f64;
    let cpu = |before: u64, after: u64| (after - before) as f64 / CLOCK_TICKS_PER_SEC / secs * 100.0;
    let out = serde_json::json!({
        "mode": mode,
        "k": run.k,
        "secs": secs,
        "commits": latencies.len(),
        "commits_per_sec": latencies.len() as f64 / secs,
        "mean_us": mean,
        "p50_us": pct(0.50),
        "p99_us": pct(0.99),
        "p999_us": pct(0.999),
        "max_us": latencies.last().copied().unwrap_or(0),
        "errors": errors,
        "first_error": first_error,
        "acked_total": acked,
        "head_seq": head,
        "head_ok": head == acked,
        "server_cpu_pct": cpu(server_before, server_after),
        "gen_cpu_pct": cpu(own_before, own_after),
        "server_rss_kib": rss_kib,
        "server_hwm_kib": hwm_kib,
        "server_ctx_switches_per_commit": switches as f64 / latencies.len().max(1) as f64,
        "stalls": stalls,
    });
    println!("{out}");
}

/// Sends one request on a connection of its own and returns the status and the body.
async fn request(
    addr: &str,
    method: &str,
    path: &str,
    auth: Option<&str>,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Bytes) {
    let mut sender = connect(addr).await;
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "zemdb")
        .header("content-type", content_type);
    if let Some(auth) = auth {
        req = req.header("authorization", auth);
    }
    let resp = sender
        .send_request(req.body(Full::new(Bytes::from(body))).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    (status, resp.into_body().collect().await.unwrap().to_bytes())
}

async fn connect(addr: &str) -> http1::SendRequest<Full<Bytes>> {
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (sender, conn) = http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(conn);
    sender
}

async fn http_mode(args: &HashMap<String, String>, run_args: Run) {
    let addr = args["addr"].clone();
    let room_id = RoomId::new(args["room"].as_str()).unwrap();
    let client_id = ClientId::new(CLIENT_ID).unwrap();
    let schema_id = SchemaId::new(SCHEMA_ID).unwrap();
    let auth_secret = std::env::var("ZEMDB_AUTH_SECRET").unwrap();
    let admin = format!("Bearer {}", std::env::var("ZEMDB_ADMIN_SECRET").unwrap());

    let create_schema = CreateSchemaRequest {
        schema_id: schema_id.clone(),
        schema: schema(),
    };
    let (status, body) = request(
        &addr,
        "POST",
        "/admin/schemas",
        Some(&admin),
        "application/json",
        serde_json::to_vec(&create_schema).unwrap(),
    )
    .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::CONFLICT,
        "create schema: {status} {body:?}"
    );
    let create_room = CreateRoomRequest {
        room_id: room_id.clone(),
        schema_id,
        lifecycle: None,
    };
    let (status, body) = request(
        &addr,
        "POST",
        "/admin/rooms",
        Some(&admin),
        "application/json",
        serde_json::to_vec(&create_room).unwrap(),
    )
    .await;
    assert!(status.is_success(), "create room: {status} {body:?}");

    let token = generate_client_token(
        &client_id,
        &room_id,
        Duration::from_secs(3600),
        &auth_secret,
    );
    let register = ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        auth_token: token.clone(),
        current_seq: None,
        known_incarnation: None,
    };
    let (status, body) = request(
        &addr,
        "POST",
        &format!("/rooms/{room_id}/register"),
        None,
        "application/octet-stream",
        encode_message(&register).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register: {body:?}");

    let mut committers = Vec::new();
    for _ in 0..run_args.k {
        committers.push(HttpCommitter {
            sender: connect(&addr).await,
            path: format!("/rooms/{room_id}/commit"),
            auth: format!("Bearer {token}"),
            room_id: room_id.clone(),
            client_id: client_id.clone(),
            next_correlation: 1,
        });
    }
    let head = async {
        let (status, body) = request(
            &addr,
            "GET",
            &format!("/admin/rooms/{room_id}"),
            Some(&admin),
            "application/json",
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let details: RoomDetails = serde_json::from_slice(&body).unwrap();
        details.metrics.head_seq.get()
    };
    run("http", &run_args, committers, head).await;
}

async fn inproc_mode(args: &HashMap<String, String>, mut run_args: Run) {
    let config = Arc::new(ServerConfig {
        data_dir: PathBuf::from(&args["data-dir"]),
        auth_secret: "perf_auth_secret_0123456789_0123456789".to_string(),
        admin_secret: "perf_admin_secret_0123456789_012345678".to_string(),
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
        relay,
    ));
    let schema_id = SchemaId::new(SCHEMA_ID).unwrap();
    registry
        .register_schema(schema_id.clone(), schema())
        .unwrap();
    let room_id = RoomId::new("e1-inproc").unwrap();
    manager
        .create_room(room_id.clone(), schema_id, None)
        .await
        .unwrap();
    let client_id = ClientId::new(CLIENT_ID).unwrap();
    manager
        .ask(&room_id, |reply| RoomCommand::RegisterClient {
            client_id: client_id.clone(),
            current_seq: None,
            known_incarnation: None,
            reply,
        })
        .await
        .unwrap()
        .unwrap();

    let committers = (0..run_args.k)
        .map(|_| InprocCommitter {
            manager: Arc::clone(&manager),
            room_id: room_id.clone(),
            client_id: client_id.clone(),
        })
        .collect();
    run_args.server_pid = std::process::id();
    let head = async {
        manager
            .ask(&room_id, |reply| RoomCommand::GetMetrics { reply })
            .await
            .unwrap()
            .unwrap()
            .head_seq
            .get()
    };
    run("inproc", &run_args, committers, head).await;
    manager.shutdown_all().await;
}

fn main() {
    let mut argv = std::env::args().skip(1);
    let mode = argv.next().expect("mode: http or inproc");
    let rest: Vec<String> = argv.collect();
    let args: HashMap<String, String> = rest
        .chunks(2)
        .map(|pair| (pair[0].trim_start_matches("--").to_string(), pair[1].clone()))
        .collect();
    let num = |name: &str, default: u64| args.get(name).map_or(default, |v| v.parse().unwrap());
    let run_args = Run {
        k: num("k", 1) as usize,
        warmup: Duration::from_secs(num("warmup", 5)),
        secs: Duration::from_secs(num("secs", 30)),
        server_pid: num("server-pid", u64::from(std::process::id())) as u32,
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        match mode.as_str() {
            "http" => http_mode(&args, run_args).await,
            "inproc" => inproc_mode(&args, run_args).await,
            "rooms" => rooms::rooms_mode(&args).await,
            other => panic!("unknown mode {other}"),
        }
    });
}
