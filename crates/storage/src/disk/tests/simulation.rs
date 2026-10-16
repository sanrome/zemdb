//! Randomized simulations of the disk engine, checked against a `MemoryStorageEngine` that
//! applies the same confirmed changes.
//!
//! A scenario applies random batches (some with a sequence gap or an unknown table), compactions
//! (some failing at a fail point), snapshots ahead of, at or behind the room, and reopens. The
//! simulations crash it, or fail it, at every fail or pause point it reaches in turn, read it
//! while it runs, or damage its WAL at random. Seeds are fixed, so a failure names the seed (and
//! the point) that reproduces it.
//!
//! The short runs take a few seconds in debug builds. The long ones are ignored by default; run
//! them with `cargo test -p zemdb-storage --release -- --ignored`. The environment variables
//! `SIM_SEED`, `SIM_SEEDS` and `SIM_STEPS` override the first seed, the number of seeds and the
//! steps of a scenario.

use super::support::{engine_at, file_contents, Rng};
use crate::disk::compactor::compacting_wal_path;
use crate::fail_point::{self, CrashAction};
use crate::{DiskStorageEngine, DiskStorageOptions, MemoryStorageEngine, ScanOptions};
use crate::{StorageEngine, StorageError};
use futures::StreamExt;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::JoinSet;
use zemdb_core::{
    decode_wal_batch_from_slice, encode_wal_batch, ColumnUpdate, CompactRow, DataType, Operation,
    PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation, TableSchema, Value,
    WalBatchDecodeResult, BATCH_HEADER_SIZE,
};

/// Runs of a simulation in flight at once.
const PARALLEL_RUNS: usize = 8;

/// `default`, or the value of the environment variable `name` if it holds a number.
fn setting<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// `count` seeds from `first`, or as many as `SIM_SEEDS` from `SIM_SEED`.
fn seeds(first: u64, count: u64) -> Range<u64> {
    let first = setting("SIM_SEED", first);
    first..first + setting("SIM_SEEDS", count)
}

/// Fails the test with the first of `failures`, if any, out of `runs`.
fn assert_no_failures(failures: &[String], runs: usize) {
    assert!(
        failures.is_empty(),
        "{} of {runs} runs failed:\n{}",
        failures.len(),
        failures[..failures.len().min(20)].join("\n")
    );
}

// ---------------------------------------------------------------------------------------------
// The model

const USERS: u16 = 0;
const ITEMS: u16 = 1;

/// `users`, keyed by an integer, with a name; `items`, keyed by an integer, with bytes that
/// sometimes hold a complete WAL frame, as any client can write.
fn schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(USERS)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String);
    let items = TableSchema::builder("items")
        .table_id(ITEMS)
        .primary_key("id", DataType::Int)
        .column("data", DataType::Bytes);
    Schema::builder().table(users).table(items).build()
}

fn room() -> RoomId {
    RoomId::new("sim").unwrap()
}

/// A change the room confirmed.
#[derive(Clone)]
enum Event {
    Batch(Vec<SequencedOperation>),
    /// A snapshot and its head.
    Snapshot(Vec<u8>, u64),
}

/// `events` followed by `event`.
fn with(events: &[Event], event: Event) -> Vec<Event> {
    let mut events = events.to_vec();
    events.push(event);
    events
}

/// The head of the room and every row of its tables, in order.
type Dump = (u64, Vec<String>);

async fn dump<E: StorageEngine>(engine: &E) -> Dump {
    let head = engine.get_head_seq(&room()).await.unwrap().get();
    let mut rows = Vec::new();
    for table in [USERS, ITEMS] {
        let mut scan = engine
            .scan_by_id(&room(), table, ScanOptions::default())
            .await
            .unwrap();
        while let Some(item) = scan.next().await {
            let (pk, row) = item.unwrap();
            rows.push(format!("{table}:{pk:?}={row:?}"));
        }
    }
    (head, rows)
}

/// A memory engine whose room holds `events`.
async fn oracle_engine(events: &[Event]) -> MemoryStorageEngine {
    let engine = MemoryStorageEngine::new();
    engine.open_room(&room(), schema()).await.unwrap();
    for event in events {
        match event {
            Event::Batch(ops) => {
                engine.apply_batch(&room(), ops.clone()).await.unwrap();
            }
            Event::Snapshot(bytes, head) => {
                let (bytes, head) = (bytes.clone().into(), SequenceNumber::from(*head));
                engine
                    .apply_snapshot(&room(), schema(), bytes, head)
                    .await
                    .unwrap();
            }
        }
    }
    engine
}

/// The room after `events`.
async fn oracle(events: &[Event]) -> Dump {
    dump(&oracle_engine(events).await).await
}

/// The head of the room after `events`.
fn head_of(events: &[Event]) -> u64 {
    events.iter().fold(0, |head, event| match event {
        Event::Batch(ops) => ops.last().map_or(head, |op| op.seq.get()),
        Event::Snapshot(_, seq) => *seq,
    })
}

/// A value for the second column of `table`: a short or (rarely) a long name for `users`;
/// random bytes or a complete WAL frame for `items`.
fn random_value(rng: &mut Rng, table: u16) -> Value {
    if table == USERS {
        let len = if rng.chance(10) {
            2000 + rng.below(3000)
        } else {
            1 + rng.below(30)
        };
        let name: String = (0..len)
            .map(|i| (b'a' + ((i + rng.below(26)) % 26) as u8) as char)
            .collect();
        Value::String(name.into())
    } else if rng.chance(30) {
        let key = PrimaryKey::single(rng.below(50) as i64);
        let frame =
            SequencedOperation::new(1 + rng.below(1_000_000), Operation::delete(USERS, key, 1));
        Value::Bytes(encode_wal_batch(&[frame], None).unwrap().into())
    } else {
        let len = rng.below(64);
        let bytes: Vec<u8> = (0..len).map(|_| rng.below(256) as u8).collect();
        Value::Bytes(bytes.into())
    }
}

/// An insert, update or delete of one of a few keys of either table, at `seq`.
fn random_op(rng: &mut Rng, seq: u64) -> SequencedOperation {
    let table = if rng.chance(50) { USERS } else { ITEMS };
    let key = rng.below(12) as i64;
    let pk = PrimaryKey::single(key);
    let roll = rng.below(100);
    let op = if roll < 60 {
        let row = CompactRow::new(vec![Value::Int(key), random_value(rng, table)]);
        Operation::insert(table, pk, row, seq)
    } else if roll < 85 {
        let update = ColumnUpdate::new(1, random_value(rng, table));
        Operation::update(table, pk, vec![update], seq)
    } else {
        Operation::delete(table, pk, seq)
    };
    SequencedOperation::new(seq, op)
}

/// One to four operations that continue `head`.
fn random_batch(rng: &mut Rng, head: u64) -> Vec<SequencedOperation> {
    let len = 1 + rng.below(4);
    (0..len).map(|i| random_op(rng, head + 1 + i)).collect()
}

/// A snapshot of the room after `events` and `extra` more random batches, and its head.
async fn snapshot_ahead(rng: &mut Rng, events: &[Event], extra: u64) -> (Vec<u8>, u64) {
    let engine = oracle_engine(events).await;
    let mut head = head_of(events);
    for _ in 0..extra {
        let batch = random_batch(rng, head);
        head = batch.last().unwrap().seq.get();
        engine.apply_batch(&room(), batch).await.unwrap();
    }
    (engine.create_snapshot(&room()).await.unwrap(), head)
}

/// One step of a scenario.
#[derive(Clone)]
enum Step {
    Batch(Vec<SequencedOperation>),
    /// A batch one past the next sequence number: `SequenceMismatch`.
    GapBatch(Vec<SequencedOperation>),
    /// A batch whose last operation names a table the schema lacks: rejected whole.
    InvalidBatch(Vec<SequencedOperation>),
    Compact,
    /// A compaction that fails at this fail point.
    FailedCompact(&'static str),
    /// A snapshot ahead of the room, and its head.
    SnapshotAhead(Vec<u8>, u64),
    /// A snapshot of a prefix of the confirmed changes: at the room's head it changes nothing,
    /// behind it it is `SnapshotBehind`.
    SnapshotNotAhead(Vec<u8>, u64),
    /// A snapshot ahead of the room, applied with a head that is not its own:
    /// `SnapshotHeadMismatch`.
    SnapshotWrongHead(Vec<u8>, u64),
    /// Closes the room and opens it in a new engine.
    Reopen,
}

impl Step {
    fn describe(&self) -> String {
        match self {
            Step::Batch(ops) => {
                let (first, last) = (ops[0].seq.get(), ops.last().unwrap().seq.get());
                format!("batch {first}..={last}")
            }
            Step::GapBatch(_) => "batch with a gap".into(),
            Step::InvalidBatch(_) => "invalid batch".into(),
            Step::Compact => "compaction".into(),
            Step::FailedCompact(point) => format!("compaction failing at {point}"),
            Step::SnapshotAhead(_, seq) => format!("snapshot at {seq}"),
            Step::SnapshotNotAhead(_, seq) => format!("snapshot not ahead at {seq}"),
            Step::SnapshotWrongHead(_, seq) => format!("snapshot at {seq} with a wrong head"),
            Step::Reopen => "reopen".into(),
        }
    }
}

/// The fail points where a compaction of a scenario fails.
const COMPACTION_FAILURES: [&str; 5] = [
    "compaction.phase2",
    "snapshot.stage_write",
    "compaction.phase3_rename",
    "compaction.phase3_sync",
    "compaction.cleanup",
];

/// The file of the room that the fail point `point` of a compaction is armed for.
fn compaction_fail_path(engine: &DiskStorageEngine, point: &str) -> PathBuf {
    if point == "compaction.phase2" || point == "snapshot.stage_write" {
        engine.snap_file_path(&room())
    } else {
        engine.wal_file_path(&room())
    }
}

/// The next step of a scenario whose room holds `events`.
async fn random_step(rng: &mut Rng, events: &[Event]) -> Step {
    let head = head_of(events);
    let roll = rng.below(100);
    if roll < 50 {
        Step::Batch(random_batch(rng, head))
    } else if roll < 54 {
        let gap = random_batch(rng, head + 1);
        Step::GapBatch(gap)
    } else if roll < 57 {
        let mut batch = random_batch(rng, head);
        let last = batch.last_mut().unwrap();
        let unknown_table = Operation::delete(9, PrimaryKey::single(1i64), 1);
        *last = SequencedOperation::new(last.seq.get(), unknown_table);
        Step::InvalidBatch(batch)
    } else if roll < 66 {
        Step::Compact
    } else if roll < 75 {
        Step::FailedCompact(COMPACTION_FAILURES[rng.below(5) as usize])
    } else if roll < 84 {
        let extra = 1 + rng.below(3);
        let (bytes, seq) = snapshot_ahead(rng, events, extra).await;
        Step::SnapshotAhead(bytes, seq)
    } else if roll < 88 {
        let prefix = rng.below(events.len() as u64 + 1) as usize;
        let (bytes, seq) = snapshot_ahead(rng, &events[..prefix], 0).await;
        Step::SnapshotNotAhead(bytes, seq)
    } else if roll < 90 {
        let extra = 1 + rng.below(3);
        let (bytes, seq) = snapshot_ahead(rng, events, extra).await;
        Step::SnapshotWrongHead(bytes, seq)
    } else {
        Step::Reopen
    }
}

/// Runs `step` (anything but a reopen) in a task of its own, so that a panic injected at a panic
/// point fails that task instead of the simulation, and returns the head it reports. `Err` holds
/// the panic.
async fn run_step(
    engine: &DiskStorageEngine,
    step: &Step,
) -> Result<Result<u64, StorageError>, String> {
    let (engine, room, step) = (engine.clone(), room(), step.clone());
    let task = tokio::spawn(async move {
        let head = match step {
            Step::Batch(ops) | Step::GapBatch(ops) | Step::InvalidBatch(ops) => {
                engine.apply_batch(&room, ops).await?
            }
            Step::Compact => {
                engine.compact_room(&room).await?;
                return Ok(0);
            }
            Step::FailedCompact(point) => {
                let path = compaction_fail_path(&engine, point);
                fail_point::arm(point, &path);
                let result = engine.compact_room(&room).await;
                fail_point::disarm(point, &path);
                result?;
                return Ok(0);
            }
            Step::SnapshotAhead(bytes, seq) | Step::SnapshotNotAhead(bytes, seq) => {
                let head = SequenceNumber::from(seq);
                engine
                    .apply_snapshot(&room, schema(), bytes.into(), head)
                    .await?
            }
            Step::SnapshotWrongHead(bytes, seq) => {
                let head = SequenceNumber::from(seq + 1);
                engine
                    .apply_snapshot(&room, schema(), bytes.into(), head)
                    .await?
            }
            Step::Reopen => unreachable!("a reopen is not run as a task"),
        };
        Ok(head.get())
    });
    task.await.map_err(|e| format!("the step panicked: {e}"))
}

/// Checks `result`, the outcome of `step` on a room at `head` when nothing failed, and returns
/// the change it confirmed, if any.
fn check_outcome(
    step: &Step,
    head: u64,
    result: &Result<u64, StorageError>,
) -> Result<Option<Event>, String> {
    match (step, result) {
        (Step::Batch(ops), Ok(new_head)) if *new_head == ops.last().unwrap().seq.get() => {
            Ok(Some(Event::Batch(ops.clone())))
        }
        (Step::SnapshotAhead(bytes, seq), Ok(new_head)) if new_head == seq => {
            Ok(Some(Event::Snapshot(bytes.clone(), *seq)))
        }
        (Step::Compact, Ok(_))
        | (Step::FailedCompact(_), Err(_))
        | (Step::GapBatch(_), Err(StorageError::SequenceMismatch { .. }))
        | (Step::InvalidBatch(_), Err(StorageError::TableNotFound { .. }))
        | (Step::InvalidBatch(_), Err(StorageError::SchemaValidation(_)))
        | (Step::SnapshotWrongHead(..), Err(StorageError::SnapshotHeadMismatch { .. })) => Ok(None),
        (Step::SnapshotNotAhead(_, seq), Ok(new_head)) if *seq == head && *new_head == head => {
            Ok(None)
        }
        (Step::SnapshotNotAhead(_, seq), Err(StorageError::SnapshotBehind { .. }))
            if *seq < head =>
        {
            Ok(None)
        }
        (step, result) => Err(format!(
            "unexpected {result:?} for the {} on a room at {head}",
            step.describe()
        )),
    }
}

/// Checks that the room of `engine` holds exactly `events`.
async fn check_memory(engine: &DiskStorageEngine, events: &[Event]) -> Result<(), String> {
    let (got, want) = (dump(engine).await, oracle(events).await);
    if got != want {
        return Err(format!(
            "memory holds head {} with {} rows, the confirmed changes head {} with {} rows",
            got.0,
            got.1.len(),
            want.0,
            want.1.len()
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// A crash at every point
//
// Copying the files of an open room reads files its engine holds locked, which Windows refuses,
// so this simulation runs on Unix only.

/// A copy of the room's files at one point of a scenario: what the disk would hold if the
/// process crashed there.
#[cfg(unix)]
struct CrashImage {
    /// The number of the point, counting from 1.
    point: usize,
    dir: PathBuf,
    /// Where the scenario was, for failure messages.
    place: String,
    /// The confirmed changes the copy may hold: those before the step it was taken in and, if
    /// that step confirmed one, after it.
    candidates: Vec<Vec<Event>>,
}

/// Runs the scenario of `seed` for `steps` steps in `data`, under a crash plan that copies the
/// room's files at every point the scenario reaches into `images`, checking after every step
/// that the room holds exactly the confirmed changes. Returns the copies.
#[cfg(unix)]
async fn run_with_crash_images(
    data: &Path,
    images: &Path,
    seed: u64,
    steps: usize,
) -> Result<Vec<CrashImage>, String> {
    fail_point::arm_crash_plan(data, CrashAction::CopyAtEveryPoint(images.to_path_buf()));
    // The points each step reached, where, and the confirmed changes before and after it.
    let mut log: Vec<(usize, String, Vec<Vec<Event>>)> = Vec::new();
    let run = async {
        let mut rng = Rng::new(seed);
        let mut engine = engine_at(data);
        engine
            .open_room(&room(), schema())
            .await
            .map_err(|e| format!("initial open: {e}"))?;
        log.push((
            fail_point::crash_plan_points(data),
            "initial open".into(),
            vec![Vec::new()],
        ));
        let mut events = Vec::new();
        for step_no in 0..steps {
            let step = random_step(&mut rng, &events).await;
            let before = events.clone();
            if let Step::Reopen = step {
                engine
                    .close_room(&room())
                    .await
                    .map_err(|e| format!("close: {e}"))?;
                engine = engine_at(data);
                engine
                    .open_room(&room(), schema())
                    .await
                    .map_err(|e| format!("step {step_no}: reopen: {e}"))?;
            } else {
                let result = run_step(&engine, &step).await?;
                let confirmed = check_outcome(&step, head_of(&events), &result)
                    .map_err(|e| format!("step {step_no}: {e}"))?;
                events.extend(confirmed);
            }
            check_memory(&engine, &events)
                .await
                .map_err(|e| format!("step {step_no}: {e}"))?;
            let mut candidates = vec![before];
            if events.len() > candidates[0].len() {
                candidates.push(events.clone());
            }
            let place = format!("step {step_no} ({})", step.describe());
            log.push((fail_point::crash_plan_points(data), place, candidates));
        }
        Ok::<_, String>(engine)
    }
    .await;
    let points = fail_point::disarm_crash_plan(data);
    let _ = run?.close_room(&room()).await;

    let mut crash_images = Vec::new();
    let mut steps = log.into_iter();
    let mut current = steps.next();
    for (index, name) in points.into_iter().enumerate() {
        let point = index + 1;
        while current.as_ref().is_some_and(|(last, _, _)| point > *last) {
            current = steps.next();
        }
        let (_, place, candidates) = current.as_ref().expect("a point after the last step");
        crash_images.push(CrashImage {
            point,
            dir: images.join(point.to_string()),
            place: format!("{place} at {name} (point {point})"),
            candidates: candidates.clone(),
        });
    }
    Ok(crash_images)
}

/// Opens `image`, after crashing its recovery as well at the `recovery_crash`-th point if given.
/// The recovered room must hold one of the image's candidates, keep taking writes, and keep them
/// across a compaction and a reopen.
#[cfg(unix)]
async fn check_crash_image(
    image: CrashImage,
    recovery_crash: Option<usize>,
    seed: u64,
) -> Result<(), String> {
    let place = &image.place;
    let mut recovered_dir = image.dir.clone();
    if let Some(at) = recovery_crash {
        let second = image
            .dir
            .with_file_name(format!("{}-recovery", image.point));
        let action = CrashAction::CopyAt {
            at,
            to: second.clone(),
        };
        fail_point::arm_crash_plan(&image.dir, action);
        let first = engine_at(&image.dir);
        let opened = first.open_room(&room(), schema()).await;
        let points = fail_point::disarm_crash_plan(&image.dir);
        opened.map_err(|e| format!("{place}: recovery failed: {e}"))?;
        first.close_room(&room()).await.unwrap();
        if points.len() >= at {
            recovered_dir = second;
        }
    }

    let recovered = engine_at(&recovered_dir);
    recovered
        .open_room(&room(), schema())
        .await
        .map_err(|e| format!("{place}: recovery failed: {e}"))?;
    let got = dump(&recovered).await;
    let mut matched = None;
    for candidate in &image.candidates {
        if oracle(candidate).await == got {
            matched = Some(candidate);
        }
    }
    let Some(confirmed) = matched else {
        let heads: Vec<u64> = image.candidates.iter().map(|c| head_of(c)).collect();
        return Err(format!(
            "{place}: recovered head {} with {} rows, expected one of the heads {heads:?}",
            got.0,
            got.1.len()
        ));
    };

    // The recovered room takes a write and keeps it across a compaction and a reopen.
    let mut rng = Rng::new(seed ^ image.point as u64);
    let batch = random_batch(&mut rng, got.0);
    let confirmed = with(confirmed, Event::Batch(batch.clone()));
    let write = recovered.apply_batch(&room(), batch).await;
    write.map_err(|e| format!("{place}: write after recovery: {e}"))?;
    let compaction = recovered.compact_room(&room()).await;
    compaction.map_err(|e| format!("{place}: compaction after recovery: {e}"))?;
    recovered.close_room(&room()).await.unwrap();
    let reopened = engine_at(&recovered_dir);
    reopened
        .open_room(&room(), schema())
        .await
        .map_err(|e| format!("{place}: reopen after recovery: {e}"))?;
    if dump(&reopened).await != oracle(&confirmed).await {
        return Err(format!("{place}: lost the write made after recovery"));
    }
    Ok(())
}

/// Crashes the scenario of each of `seeds` at every point it reaches, and half of the time
/// crashes the recovery of the copy as well.
#[cfg(unix)]
async fn crash_at_every_point(seeds: Range<u64>, steps: usize) {
    let mut failures = Vec::new();
    let mut runs = 0;
    for seed in seeds {
        let tmp = tempfile::tempdir().unwrap();
        let (data, images) = (tmp.path().join("data"), tmp.path().join("images"));
        let crash_images = match run_with_crash_images(&data, &images, seed, steps).await {
            Ok(crash_images) => crash_images,
            Err(e) => {
                failures.push(format!("seed {seed}: {e}"));
                continue;
            }
        };
        let mut recovery_rng = Rng::new(seed.wrapping_mul(7919));
        let mut set = JoinSet::new();
        for image in crash_images {
            let recovery_crash = recovery_rng
                .chance(50)
                .then(|| 1 + recovery_rng.below(14) as usize);
            runs += 1;
            while set.len() >= PARALLEL_RUNS {
                collect_run(set.join_next().await, &mut failures);
            }
            set.spawn(async move {
                check_crash_image(image, recovery_crash, seed)
                    .await
                    .map_err(|e| format!("seed {seed}, recovery crash {recovery_crash:?}: {e}"))
            });
        }
        while let Some(joined) = set.join_next().await {
            collect_run(Some(joined), &mut failures);
        }
    }
    assert_no_failures(&failures, runs);
}

/// Records the outcome of a run joined from a `JoinSet`.
fn collect_run(
    joined: Option<Result<Result<(), String>, tokio::task::JoinError>>,
    failures: &mut Vec<String>,
) {
    match joined {
        Some(Ok(Ok(()))) | None => {}
        Some(Ok(Err(failure))) => failures.push(failure),
        Some(Err(e)) => failures.push(format!("a run panicked: {e}")),
    }
}

/// A crash at any point of any operation, and at any point of the recovery that follows it,
/// leaves files that recover a state the room confirmed: the one before the operation or, if
/// the operation confirmed a change, the one after it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_at_any_point_recovers_a_confirmed_state() {
    crash_at_every_point(seeds(45, 1), setting("SIM_STEPS", 4)).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn crash_at_any_point_recovers_a_confirmed_state_over_many_scenarios() {
    crash_at_every_point(seeds(1, 6), setting("SIM_STEPS", 30)).await;
}

// ---------------------------------------------------------------------------------------------
// An injected failure at every point

/// Closes the room and opens it in a new engine, once more if the open fails (only the plan's
/// single injected failure can make it fail), and returns the one of `candidates` the room
/// recovered.
async fn reopen_and_match(
    engine: &mut DiskStorageEngine,
    data: &Path,
    candidates: &[Vec<Event>],
) -> Result<Vec<Event>, String> {
    let _ = engine.close_room(&room()).await;
    *engine = engine_at(data);
    if engine.open_room(&room(), schema()).await.is_err() {
        *engine = engine_at(data);
        let opened = engine.open_room(&room(), schema()).await;
        opened.map_err(|e| format!("reopen failed twice: {e}"))?;
    }
    let got = dump(engine).await;
    for candidate in candidates {
        if oracle(candidate).await == got {
            return Ok(candidate.clone());
        }
    }
    let heads: Vec<u64> = candidates.iter().map(|c| head_of(c)).collect();
    Err(format!(
        "the room reopened at head {} with {} rows, expected one of the heads {heads:?}",
        got.0,
        got.1.len()
    ))
}

/// Runs the scenario of `seed` for `steps` steps in `data`, under a crash plan that fails its
/// `target`-th point. A step that fails is resolved by reopening the room, which must recover
/// the confirmed changes before it or, for a change that failed, after it; a failed compaction
/// changes nothing, so the room is reopened only half of the time. Any other step must have its
/// usual outcome and leave the room holding exactly the confirmed changes. Returns those changes.
async fn run_with_failure(
    data: &Path,
    seed: u64,
    steps: usize,
    target: usize,
) -> Result<Vec<Event>, String> {
    let failure_injected = || target > 0 && fail_point::crash_plan_points(data) >= target;
    let mut rng = Rng::new(seed);
    let mut engine = engine_at(data);
    let mut events = Vec::new();
    if engine.open_room(&room(), schema()).await.is_err() {
        reopen_and_match(&mut engine, data, &[Vec::new()]).await?;
    }
    for step_no in 0..steps {
        let step = random_step(&mut rng, &events).await;
        let injected_before = failure_injected();
        if let Step::Reopen = step {
            let candidates = [events.clone()];
            events = reopen_and_match(&mut engine, data, &candidates)
                .await
                .map_err(|e| format!("step {step_no}: reopen: {e}"))?;
            continue;
        }
        let head = head_of(&events);
        let result = run_step(&engine, &step).await;
        let injected_now = !injected_before && failure_injected();
        let outcome = match &result {
            Ok(result) => check_outcome(&step, head, result),
            Err(panic) => Err(panic.clone()),
        };
        let room_failed = matches!(result, Ok(Err(StorageError::RoomFailed { .. })));
        let reopen_with = match outcome {
            Ok(confirmed) => {
                events.extend(confirmed);
                None
            }
            // Refused before anything was written: the room failed at an earlier step.
            Err(_) if room_failed => Some(vec![events.clone()]),
            // A change that failed may or may not have reached the disk.
            Err(_) if injected_now => match &step {
                Step::Batch(ops) => {
                    let after = with(&events, Event::Batch(ops.clone()));
                    Some(vec![events.clone(), after])
                }
                Step::SnapshotAhead(bytes, seq) => {
                    let after = with(&events, Event::Snapshot(bytes.clone(), *seq));
                    Some(vec![events.clone(), after])
                }
                Step::Compact | Step::FailedCompact(_) => {
                    rng.chance(50).then(|| vec![events.clone()])
                }
                // A refusal that the failure turned into an I/O error.
                _ => Some(vec![events.clone()]),
            },
            Err(e) => return Err(format!("step {step_no}: {e}")),
        };
        let checked = match reopen_with {
            Some(candidates) => reopen_and_match(&mut engine, data, &candidates)
                .await
                .map(|recovered| events = recovered),
            None => check_memory(&engine, &events).await,
        };
        checked.map_err(|e| format!("step {step_no} ({}): {e}", step.describe()))?;
    }
    Ok(events)
}

/// Runs the scenario of `seed` with a crash plan that fails its `target`-th point (0 fails
/// none), then reopens the room, which must hold exactly the confirmed changes. Returns the
/// number of points the scenario reached.
async fn failure_run(seed: u64, steps: usize, target: usize) -> Result<usize, String> {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    fail_point::arm_crash_plan(&data, CrashAction::FailAt(target));
    let run = run_with_failure(&data, seed, steps, target).await;
    let points = fail_point::disarm_crash_plan(&data);
    let failed_at = target.checked_sub(1).and_then(|i| points.get(i));
    let place = |e: String| format!("{e} (failure at {failed_at:?}, point {target})");
    let events = run.map_err(place)?;
    let mut engine = engine_at(&data);
    reopen_and_match(&mut engine, &data, &[events])
        .await
        .map_err(|e| place(format!("final reopen: {e}")))?;
    Ok(points.len())
}

/// Fails the scenario of each of `seeds` at every point it reaches, one run per point.
async fn fail_at_every_point(seeds: Range<u64>, steps: usize) {
    let mut failures = Vec::new();
    let mut runs = 0;
    for seed in seeds {
        let points = match failure_run(seed, steps, 0).await {
            Ok(points) => points,
            Err(e) => {
                failures.push(format!("seed {seed} without a failure: {e}"));
                continue;
            }
        };
        let mut set = JoinSet::new();
        for target in 1..=points {
            runs += 1;
            while set.len() >= PARALLEL_RUNS {
                collect_run(set.join_next().await, &mut failures);
            }
            set.spawn(async move {
                failure_run(seed, steps, target)
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("seed {seed}: {e}"))
            });
        }
        while let Some(joined) = set.join_next().await {
            collect_run(Some(joined), &mut failures);
        }
    }
    assert_no_failures(&failures, runs);
}

/// An I/O error (or a panic, at a panic point) at any point of any operation never loses a
/// confirmed change, and never confirms one that the reopened room does not hold: a failed
/// operation is resolved by reopening the room, and every other one keeps its usual outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failure_at_any_point_never_loses_a_confirmed_change() {
    fail_at_every_point(seeds(127, 1), setting("SIM_STEPS", 5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn failure_at_any_point_never_loses_a_confirmed_change_over_many_scenarios() {
    fail_at_every_point(seeds(100, 6), setting("SIM_STEPS", 30)).await;
}

// ---------------------------------------------------------------------------------------------
// Readers during writes, compactions and snapshots

/// For each of `seeds`, one task applies `steps` random batches and snapshots to a room that
/// compacts on its own after every few hundred bytes, another compacts it in a loop, and three
/// read it in a loop. Every scan of `users` must return the table after some prefix of the
/// changes, never an earlier prefix than the scan before it; the room must end, and reopen, with
/// every change.
async fn readers_see_a_prefix(seeds: Range<u64>, steps: usize) {
    for seed in seeds {
        let mut rng = Rng::new(seed);
        let mut events = Vec::new();
        for _ in 0..steps {
            let event = if rng.chance(8) {
                let extra = 1 + rng.below(3);
                let (bytes, seq) = snapshot_ahead(&mut rng, &events, extra).await;
                Event::Snapshot(bytes, seq)
            } else {
                Event::Batch(random_batch(&mut rng, head_of(&events)))
            };
            events.push(event);
        }
        // The rows of `users` after each prefix of the changes.
        let mut prefixes = Vec::new();
        for len in 0..=events.len() {
            let (_, rows) = oracle(&events[..len]).await;
            let users: Vec<String> = rows
                .into_iter()
                .filter(|row| row.starts_with(&format!("{USERS}:")))
                .collect();
            prefixes.push(users);
        }
        let prefixes = Arc::new(prefixes);

        let dir = tempfile::tempdir().unwrap();
        let options = DiskStorageOptions::new(dir.path())
            .min_compaction_bytes(512)
            .compaction_ratio(0.5);
        let engine = DiskStorageEngine::new(options).unwrap();
        engine.open_room(&room(), schema()).await.unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let mut readers = Vec::new();
        for _ in 0..3 {
            let (engine, done, prefixes) =
                (engine.clone(), Arc::clone(&done), Arc::clone(&prefixes));
            readers.push(tokio::spawn(async move {
                let (mut last, mut scans) = (0, 0);
                while !done.load(Ordering::Acquire) {
                    let mut scan = engine
                        .scan_by_id(&room(), USERS, ScanOptions::default())
                        .await
                        .unwrap();
                    let mut rows = Vec::new();
                    while let Some(item) = scan.next().await {
                        let (pk, row) = item.unwrap();
                        rows.push(format!("{USERS}:{pk:?}={row:?}"));
                        tokio::task::yield_now().await;
                    }
                    last = (last..prefixes.len())
                        .find(|&len| prefixes[len] == rows)
                        .ok_or_else(|| format!("a scan matches no prefix of {last} or more"))?;
                    scans += 1;
                }
                Ok::<_, String>(scans)
            }));
        }
        let compactor = {
            let (engine, done) = (engine.clone(), Arc::clone(&done));
            tokio::spawn(async move {
                while !done.load(Ordering::Acquire) {
                    // A compaction already running makes this one return at once.
                    engine.compact_room(&room()).await.unwrap();
                    tokio::task::yield_now().await;
                }
            })
        };
        for event in &events {
            match event {
                Event::Batch(ops) => {
                    engine.apply_batch(&room(), ops.clone()).await.unwrap();
                }
                Event::Snapshot(bytes, seq) => {
                    let (bytes, head) = (bytes.clone().into(), SequenceNumber::from(*seq));
                    engine
                        .apply_snapshot(&room(), schema(), bytes, head)
                        .await
                        .unwrap();
                }
            }
        }
        done.store(true, Ordering::Release);
        compactor.await.unwrap();
        for reader in readers {
            let scans = reader
                .await
                .unwrap()
                .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
            assert!(scans > 0, "seed {seed}: a reader never scanned");
        }
        let want = oracle(&events).await;
        assert!(dump(&engine).await == want, "seed {seed}: final state");
        engine.close_room(&room()).await.unwrap();
        let reopened = engine_at(dir.path());
        reopened.open_room(&room(), schema()).await.unwrap();
        assert!(dump(&reopened).await == want, "seed {seed}: reopened state");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn readers_see_a_prefix_during_writes_compactions_and_snapshots() {
    readers_see_a_prefix(seeds(9000, 1), setting("SIM_STEPS", 20)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn readers_see_a_prefix_during_writes_compactions_and_snapshots_over_many_scenarios() {
    readers_see_a_prefix(seeds(9000, 20), setting("SIM_STEPS", 60)).await;
}

// ---------------------------------------------------------------------------------------------
// A damaged WAL

/// Start, end and last sequence number of each frame of `wal`.
fn frames_of(wal: &[u8]) -> Vec<(usize, usize, u64)> {
    let mut frames = Vec::new();
    let mut start = 0;
    while start < wal.len() {
        match decode_wal_batch_from_slice(&wal[start..]).unwrap() {
            WalBatchDecodeResult::Ok {
                ops,
                bytes_consumed,
                ..
            } => {
                let last = ops.last().map_or(0, |op| op.seq.get());
                frames.push((start, start + bytes_consumed, last));
                start += bytes_consumed;
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    frames
}

/// One way of damaging a WAL.
#[derive(Debug, Clone, Copy)]
enum Damage {
    /// Cut short at an offset.
    Truncated,
    /// Zeroed from an offset to the end.
    ZeroedToTheEnd,
    /// One bit flipped.
    FlippedBit,
    /// Up to 40 bytes zeroed.
    ZeroedRange,
}

impl Damage {
    /// Whether it reaches the end of the file, as a write cut short by a crash does.
    fn reaches_the_end(self) -> bool {
        matches!(self, Damage::Truncated | Damage::ZeroedToTheEnd)
    }
}

/// Damages a copy of `base`, a closed room whose WAL holds the batches `events` in the frames
/// `frames`, in `trial`, and opens it. The open must either recover exactly the batches before
/// the first damaged one, and truncate the WAL there, which it may only do if no intact frame
/// follows the damage; or report `WalCorruption` without changing any file, which it must do if
/// an intact frame follows the damage. Damage that reaches the end of the file is never
/// corruption, and neither is damage in the payload of the last frame; a damaged header of the
/// last frame may be (it hides where the frame ends, so a frame stored in its value can pass for
/// a later one).
///
/// If `rotated`, the damaged file is instead the room's `.wal.compacting`, whose records the
/// active WAL continues: any damage must be `WalCorruption`, without changing any file.
async fn check_damaged_wal(
    rng: &mut Rng,
    base: &Path,
    trial: &Path,
    events: &[Event],
    frames: &[(usize, usize, u64)],
    rotated: bool,
) -> Result<(), String> {
    let engine = engine_at(trial);
    let wal_path = damaged_file(&engine, rotated);
    fail_point::copy_files(base, trial);
    let wal = std::fs::read(&wal_path).unwrap();
    let damage = match rng.below(4) {
        0 => Damage::Truncated,
        1 => Damage::ZeroedToTheEnd,
        2 => Damage::FlippedBit,
        _ => Damage::ZeroedRange,
    };
    let at = rng.below(wal.len() as u64) as usize;
    let mut damaged = wal.clone();
    match damage {
        Damage::Truncated => damaged.truncate(at),
        Damage::ZeroedToTheEnd => damaged[at..].fill(0),
        Damage::FlippedBit => damaged[at] ^= 1 << rng.below(8),
        Damage::ZeroedRange => {
            let end = (at + 1 + rng.below(40) as usize).min(wal.len());
            damaged[at..end].fill(0);
        }
    }
    // The first and last bytes that changed: zeroes written over zeroes change nothing.
    let changed = |i: &usize| damaged.get(*i) != wal.get(*i);
    let Some(first) = (0..wal.len()).find(changed) else {
        return Ok(());
    };
    let last = if damage.reaches_the_end() {
        wal.len() - 1
    } else {
        (0..wal.len()).rev().find(changed).unwrap()
    };
    std::fs::write(&wal_path, &damaged).unwrap();
    let before = file_contents(trial);

    let damaged_frame = frames.iter().position(|&(_, end, _)| first < end).unwrap();
    let intact_frame_follows = frames.iter().any(|&(start, _, _)| start > last);
    let header_damaged = first < frames[damaged_frame].0 + BATCH_HEADER_SIZE;
    let described = format!(
        "{damage:?} at {first} (frame {damaged_frame} of {})",
        frames.len()
    );

    match engine.open_room(&room(), schema()).await {
        Ok(()) => {
            if intact_frame_follows || rotated {
                return Err(format!(
                    "{described}: accepted, with intact frames after it"
                ));
            }
            if dump(&engine).await != oracle(&events[..damaged_frame]).await {
                return Err(format!(
                    "{described}: recovered something else than the frames before it"
                ));
            }
            engine.close_room(&room()).await.unwrap();
            let len = std::fs::metadata(&wal_path).unwrap().len() as usize;
            if len != frames[damaged_frame].0 {
                return Err(format!(
                    "{described}: WAL truncated to {len} bytes, not to the start of the frame, {}",
                    frames[damaged_frame].0
                ));
            }
        }
        Err(StorageError::WalCorruption(_)) => {
            if file_contents(trial) != before {
                return Err(format!(
                    "{described}: corruption reported after changing files"
                ));
            }
            let in_the_last_frame = damaged_frame == frames.len() - 1;
            if !rotated && (damage.reaches_the_end() || (in_the_last_frame && !header_damaged)) {
                return Err(format!("{described}: reported as corruption"));
            }
        }
        Err(e) => return Err(format!("{described}: unexpected error {e}")),
    }
    Ok(())
}

/// The WAL of the room in `engine`, or its `.wal.compacting` if `rotated`.
fn damaged_file(engine: &DiskStorageEngine, rotated: bool) -> PathBuf {
    let wal_path = engine.wal_file_path(&room());
    if rotated {
        compacting_wal_path(&wal_path)
    } else {
        wal_path
    }
}

/// For each of `seeds`, builds a room from 2 to 9 random batches and checks `damages` damaged
/// copies of its WAL (see [`check_damaged_wal`]). If `rotated`, a failed compaction leaves those
/// batches in `.wal.compacting` and one more batch goes to the active WAL.
async fn damaged_wals_recover_a_prefix_or_fail_untouched(
    seeds: Range<u64>,
    damages: u64,
    rotated: bool,
) {
    let mut failures = Vec::new();
    let mut runs = 0;
    for seed in seeds {
        let mut rng = Rng::new(seed);
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().join("base");
        let engine = engine_at(&base);
        engine.open_room(&room(), schema()).await.unwrap();
        let mut events = Vec::new();
        for _ in 0..2 + rng.below(8) {
            let batch = random_batch(&mut rng, head_of(&events));
            engine.apply_batch(&room(), batch.clone()).await.unwrap();
            events.push(Event::Batch(batch));
        }
        if rotated {
            fail_point::arm("compaction.phase2", &engine.snap_file_path(&room()));
            assert!(engine.compact_room(&room()).await.is_err());
            let batch = random_batch(&mut rng, head_of(&events));
            engine.apply_batch(&room(), batch).await.unwrap();
        }
        engine.close_room(&room()).await.unwrap();
        let frames = frames_of(&std::fs::read(damaged_file(&engine, rotated)).unwrap());
        assert_eq!(frames.len(), events.len());

        for damage in 0..damages {
            runs += 1;
            let trial = tmp.path().join(format!("trial-{damage}"));
            let checked =
                check_damaged_wal(&mut rng, &base, &trial, &events, &frames, rotated).await;
            if let Err(e) = checked {
                failures.push(format!("seed {seed} damage {damage}: {e}"));
            }
        }
    }
    assert_no_failures(&failures, runs);
}

#[tokio::test]
async fn damaged_wal_recovers_a_prefix_or_fails_untouched() {
    damaged_wals_recover_a_prefix_or_fail_untouched(seeds(1000, 2), 8, false).await;
}

#[tokio::test]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn damaged_wal_recovers_a_prefix_or_fails_untouched_over_many_wals() {
    damaged_wals_recover_a_prefix_or_fail_untouched(seeds(1000, 100), 20, false).await;
}

#[tokio::test]
async fn damaged_rotated_wal_fails_untouched() {
    damaged_wals_recover_a_prefix_or_fail_untouched(seeds(2000, 2), 8, true).await;
}

#[tokio::test]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn damaged_rotated_wal_fails_untouched_over_many_wals() {
    damaged_wals_recover_a_prefix_or_fail_untouched(seeds(2000, 100), 20, true).await;
}
