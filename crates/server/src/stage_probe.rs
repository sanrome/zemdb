//! Diagnostic probes of the stages of a commit (`tools/perf/diagnostics/write-stages.patch`).
//! Not part of `main`: applied on a local copy to measure, then discarded.
//!
//! Each stage has a histogram of nanoseconds (8 buckets per power of two), with its count and
//! sum. `reset` clears them and `dump` returns them as JSON. The switches come from the
//! environment, read once:
//! - `ZEMDB_PERF_STAGES`: the probes record (in the server binary, also the file that SIGUSR2
//!   writes the dump to, after SIGUSR1 resets);
//! - `ZEMDB_PERF_NO_FSYNC`: no `fsync` anywhere (the OS writes the pages back when it wants);
//! - `ZEMDB_PERF_NO_WAL`: a commit writes nothing to `active.wal` (memory only);
//! - `ZEMDB_PERF_INLINE_IO`: the file operations of a room run on the worker, not the pool.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

macro_rules! stages {
    ($($name:ident),* $(,)?) => {
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy)]
        #[repr(usize)]
        pub enum Stage { $($name),* }
        const NAMES: &[&str] = &[$(stringify!($name)),*];
    };
}

stages! {
    // The connection (HTTP/1.1, one request in flight per connection).
    sock_read,       // the read syscall that brought the request
    http_parse,      // read done -> the service is called (hyper parses the head)
    route,           // service called -> the first extractor starts (router, layers)
    auth,            // the path room and the token
    body_read,       // the body, collected
    decode,          // decode_client_message
    handler_pre,     // decoded -> the command is built
    // The room.
    to_actor,        // command built -> the actor takes it (send, wake, queue)
    sequence,        // decide: the checks of one commit
    io_dispatch,     // group sequenced -> its write starts on an I/O thread
    wal_encode,
    wal_write,
    wal_fsync,
    hot_append,      // the RAM buffer and its window
    seal,            // the segment sealed after a group, when it is
    io_return,       // write done -> the actor goes on
    publish,         // dedup window, head, SSE announcement
    answer,          // lease, reservation, catch-up of one commit
    // Back.
    to_handler,      // reply sent -> the handler goes on
    encode,          // the ack, encoded, and its response
    axum_out,        // handler done -> the service has the response
    http_write_prep, // service done -> the write syscall starts (hyper encodes the head)
    sock_write,      // the write syscall
    // Totals.
    actor_total,     // command built -> reply received
    server_total,    // read syscall starts -> write syscall done
    // Values, not times.
    group_commits,
    catchup_ops,
    request_bytes,
    response_bytes,
    wal_bytes,
}

const BUCKETS: usize = 496;

struct Hist {
    count: AtomicU64,
    sum: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
#[allow(clippy::declare_interior_mutable_const)]
const EMPTY: Hist = Hist {
    count: ZERO,
    sum: ZERO,
    buckets: [ZERO; BUCKETS],
};
static HISTS: [Hist; 64] = [EMPTY; 64];

fn flag(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|v| !v.is_empty() && v != "0")
}

/// Whether the probes record.
pub fn on() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| flag("ZEMDB_PERF_STAGES"))
}

pub fn no_fsync() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| flag("ZEMDB_PERF_NO_FSYNC"))
}

pub fn no_wal() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| flag("ZEMDB_PERF_NO_WAL"))
}

pub fn inline_io() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| flag("ZEMDB_PERF_INLINE_IO"))
}

fn bucket(v: u64) -> usize {
    if v < 8 {
        return v as usize;
    }
    let e = 63 - v.leading_zeros() as usize;
    8 + (e - 3) * 8 + ((v >> (e - 3)) & 7) as usize
}

/// Records the value `v` for `stage`.
pub fn value(stage: Stage, v: u64) {
    if !on() {
        return;
    }
    let h = &HISTS[stage as usize];
    h.count.fetch_add(1, Relaxed);
    h.sum.fetch_add(v, Relaxed);
    h.buckets[bucket(v)].fetch_add(1, Relaxed);
}

/// Records `d` for `stage`.
pub fn record(stage: Stage, d: Duration) {
    value(stage, d.as_nanos() as u64);
}

/// Records for `stage` the time since `*last`, and moves `*last` to now.
pub fn lap(stage: Stage, last: &mut Instant) {
    let now = Instant::now();
    record(stage, now.saturating_duration_since(*last));
    *last = now;
}

/// Nanoseconds of `at` since the first call, never 0: what the connection keeps in an atomic.
pub fn nanos(at: Instant) -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = *EPOCH.get_or_init(|| at.checked_sub(Duration::from_secs(1)).unwrap_or(at));
    (at.saturating_duration_since(epoch).as_nanos() as u64).max(1)
}

thread_local! {
    static HANDOFF: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Leaves `at` for the code that runs next on this thread, in the same poll.
pub fn hand(at: Instant) {
    HANDOFF.with(|c| c.set(Some(at)));
}

/// Takes what [`hand`] left, or now.
pub fn take() -> Instant {
    HANDOFF
        .with(Cell::take)
        .unwrap_or_else(Instant::now)
}

/// The time the service was called, in the request's extensions.
#[derive(Clone, Copy)]
pub struct Called(pub Instant);

/// The time the handler ended, in the response's extensions.
#[derive(Clone, Copy)]
pub struct Handled(pub Instant);

pub fn reset() {
    for h in &HISTS {
        h.count.store(0, Relaxed);
        h.sum.store(0, Relaxed);
        for b in &h.buckets {
            b.store(0, Relaxed);
        }
    }
}

/// `{"stage": {"count": n, "sum": n, "buckets": [[index, count], ...]}, ...}`
pub fn dump() -> String {
    let mut out = String::from("{");
    for (i, name) in NAMES.iter().enumerate() {
        let h = &HISTS[i];
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "\"{name}\":{{\"count\":{},\"sum\":{},\"buckets\":[",
            h.count.load(Relaxed),
            h.sum.load(Relaxed)
        ));
        let mut first = true;
        for (b, n) in h.buckets.iter().enumerate() {
            let n = n.load(Relaxed);
            if n > 0 {
                if !first {
                    out.push(',');
                }
                first = false;
                out.push_str(&format!("[{b},{n}]"));
            }
        }
        out.push_str("]}");
    }
    out.push('}');
    out
}

/// In the server binary: SIGUSR1 resets the probes and SIGUSR2 writes their dump to the file
/// `ZEMDB_PERF_STAGES` names.
#[cfg(unix)]
pub fn spawn_signal_listener() {
    use tokio::signal::unix::{signal, SignalKind};
    let Some(path) = std::env::var_os("ZEMDB_PERF_STAGES").filter(|v| !v.is_empty()) else {
        return;
    };
    tokio::spawn(async move {
        let mut usr1 = signal(SignalKind::user_defined1()).unwrap();
        let mut usr2 = signal(SignalKind::user_defined2()).unwrap();
        loop {
            tokio::select! {
                _ = usr1.recv() => reset(),
                _ = usr2.recv() => {
                    let tmp = std::path::PathBuf::from(format!("{}.tmp", path.to_string_lossy()));
                    std::fs::write(&tmp, dump()).unwrap();
                    std::fs::rename(&tmp, &path).unwrap();
                }
            }
        }
    });
}
