//! The suites `engine` and `rooms`: the disk engine of the storage
//! (`zemdb_storage::DiskStorageEngine`) on the file layer over the OPFS (`OpfsFiles`), the real
//! engine on the real layer, under `harness/engine-{run}/`, each scenario in a data directory of
//! its own:
//! - a room that applies batches, with its automatic compactions, open across a reload;
//! - the Worker terminated in the middle of a stream of batches, of compactions, of snapshot
//!   installations and of deletions, and the room that reopens at a prefix of its log;
//! - snapshots of the native engine (with `zstd`) and of its own (with `ruzstd`), across a reload;
//! - a second page that opens a room that the first one has open, which is `RoomLocked`;
//! - operations dropped by their callers, which run to their end;
//! - and, in the suite `rooms`, how long opening tens of rooms takes, noted for the run.
//!
//! A step *in flight* leaves a loop of operations running in the Worker, which records how far it
//! got in a file of the harness outside the data directory: after each step that the engine
//! acknowledged, 8 bytes written at once and flushed, which a cut leaves whole. It returns once the
//! loop ran a few steps, so the cut never falls before the loop started; the page then terminates
//! the Worker, wherever the loop is. The next step reopens the room, in a new Worker, checks it
//! against that record, whatever the point of the cut, and notes where it fell. The loop ends only
//! if a step of it fails: then it records so, and the step that started it fails if it sees it,
//! and so does the next step, so the cut never falls after its end unseen.
//!
//! Each step fails if it does not end within [`LIMIT_MS`], so that an operation that hangs fails
//! it there.

use crate::files::{check, entries};
use crate::js::{failed, now, page, sleep, within};
use crate::opfs::{file_handle, harness_dir, open, read_all, write_all};
use crate::server::{bytes, fetch, text};
use futures::FutureExt;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{FileSystemDirectoryHandle, FileSystemReadWriteOptions, FileSystemSyncAccessHandle};
use zemdb_core::{
    CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequencedOperation, TableSchema,
    Value,
};
use zemdb_storage::files::{FileTaken, OpfsFiles, RoomFile, RoomFiles};
use zemdb_storage::format::{FileHeader, HEADER_SIZE};
use zemdb_storage::snapshot::{SnapshotCompression, SnapshotEnvelopeHeader};
use zemdb_storage::{
    DiskStorageEngine, DiskStorageOptions, MemoryStorageEngine, StorageEngine, StorageError,
};

/// The longest a step of the suite `engine` may take.
const LIMIT_MS: i32 = 20_000;

/// The longest a step of the suite `rooms` may take: Firefox lists a directory of the OPFS much
/// more slowly than Chromium.
const ROOMS_LIMIT_MS: i32 = 55_000;

/// How long a step waits for the files of a Worker that ended (a reload, or a terminated Worker),
/// which the browser releases on its own time, in milliseconds.
const RELEASE_WAIT_MS: f64 = 5000.0;

/// How many steps a loop in flight runs before the step that started it returns.
const STARTED: u64 = 3;

thread_local! {
    /// The engines that a step keeps open, with their rooms, until the Worker ends or a later
    /// step releases them.
    static HELD: RefCell<Vec<DiskStorageEngine>> = const { RefCell::new(Vec::new()) };
}

/// The data directory of the scenario `name` of the run.
fn scenario(run: &str, name: &str) -> PathBuf {
    PathBuf::from(format!("harness/engine-{run}/{name}"))
}

/// An engine on the OPFS in `dir` that compacts only when asked to.
fn engine_at(dir: &Path) -> Result<DiskStorageEngine, JsError> {
    let options = DiskStorageOptions::new(dir).auto_compact(false);
    DiskStorageEngine::new(options).map_err(storage("DiskStorageEngine::new"))
}

/// The schema of the rooms of the suite, and of the snapshots of the test server
/// (`server/src/main.rs`): a table `users` (id 0) with an integer key `id` and a string `name`.
fn schema() -> Schema {
    let users = TableSchema::builder("users")
        .table_id(0)
        .primary_key("id", DataType::Int)
        .column("name", DataType::String);
    Schema::builder().table(users).build()
}

/// The schema of the suite with a column `column` of `data_type` after the two of [`schema`],
/// nullable: one that evolves from it, if the column is new.
fn schema_with(column: &str, data_type: DataType) -> Schema {
    let users = TableSchema::builder("users")
        .table_id(0)
        .primary_key("id", DataType::Int)
        .column(
            "name",
            if column == "name" {
                data_type
            } else {
                DataType::String
            },
        );
    let users = if column == "name" {
        users
    } else {
        users.nullable_column(column, data_type)
    };
    Schema::builder().table(users).build()
}

/// The row of the key `key`: "user {key}", as the test server makes it.
fn row(key: u64) -> CompactRow {
    CompactRow::new(vec![
        Value::Int(key as i64),
        Value::String(format!("user {key}").into()),
    ])
}

/// A batch that inserts the row of each key of `keys`, each at the sequence of its key.
fn batch(keys: RangeInclusive<u64>) -> Vec<SequencedOperation> {
    batch_of(keys, row)
}

/// A batch that inserts `row_of(key)` for each key of `keys`, each at the sequence of its key.
fn batch_of(
    keys: RangeInclusive<u64>,
    row_of: impl Fn(u64) -> CompactRow,
) -> Vec<SequencedOperation> {
    keys.map(|key| {
        let op = Operation::insert(0, PrimaryKey::single(key as i64), row_of(key), 100);
        SequencedOperation::new(key, op)
    })
    .collect()
}

/// The row of the key `key` that the cycle `cycle` of `deletes` writes, which no other cycle
/// writes.
fn cycle_row(key: u64, cycle: u64) -> CompactRow {
    CompactRow::new(vec![
        Value::Int(key as i64),
        Value::String(format!("user {key} of the cycle {cycle}").into()),
    ])
}

fn room_id(name: &str) -> Result<RoomId, JsError> {
    RoomId::new(name).map_err(|err| JsError::new(&err.to_string()))
}

/// Makes an error of the engine the failure of `what`.
fn storage(what: &str) -> impl FnOnce(StorageError) -> JsError + '_ {
    move |err| JsError::new(&format!("{what}: {err}"))
}

/// Prints `what` as a note of the run, which the runner shows (`run.mjs`).
fn note(what: &str) {
    web_sys::console::log_1(&format!("harness note: {what}").into());
}

/// Whether `err` comes from a file that another handle has: one of a Worker that ended, which
/// the browser has not released yet.
fn taken(err: &StorageError) -> bool {
    match err {
        StorageError::RoomLocked(_) => true,
        StorageError::Io(err) => FileTaken::is(err),
        _ => false,
    }
}

/// Opens `room`, retrying while the files of a Worker that ended are still taken.
async fn open_released(engine: &DiskStorageEngine, room: &RoomId) -> Result<(), JsError> {
    let deadline = now()? + RELEASE_WAIT_MS;
    loop {
        match engine.open_room(room, schema()).await {
            Ok(()) => return Ok(()),
            Err(err) if taken(&err) && now()? < deadline => sleep(20).await?,
            Err(err) => return Err(storage("open_room")(err)),
        }
    }
}

/// Fails unless `room` is at `head`, with the row of each key up to it as [`row`] makes it, and
/// no row past it.
async fn expect_rows(engine: &DiskStorageEngine, room: &RoomId, head: u64) -> Result<(), JsError> {
    expect_rows_of(engine, room, head, row).await
}

/// [`expect_rows`], with the row of each key as `row_of` makes it.
async fn expect_rows_of(
    engine: &DiskStorageEngine,
    room: &RoomId,
    head: u64,
    row_of: impl Fn(u64) -> CompactRow,
) -> Result<(), JsError> {
    let found = engine.get_head_seq(room).await;
    let found = found.map_err(storage("get_head_seq"))?.get();
    check(found == head, || {
        format!("{room} is at {found}, not {head}")
    })?;
    for key in 1..=head + 1 {
        let got = engine
            .get(room, "users", &PrimaryKey::single(key as i64))
            .await;
        let got = got.map_err(storage("get"))?;
        let expected = (key <= head).then(|| row_of(key));
        check(got == expected, || {
            format!("{room} at {head} has {got:?} under the key {key}, not {expected:?}")
        })?;
    }
    Ok(())
}

/// The directory `dir`, relative to the root of the OPFS, through the browser.
async fn browser_dir(dir: &Path) -> Result<FileSystemDirectoryHandle, JsError> {
    let storage = crate::js::scope()?.navigator().storage();
    let root = JsFuture::from(storage.get_directory())
        .await
        .map_err(failed("navigator.storage.getDirectory"))?;
    let mut handle: FileSystemDirectoryHandle = root.unchecked_into();
    for name in dir.iter().filter_map(|name| name.to_str()) {
        let next = JsFuture::from(handle.get_directory_handle(name))
            .await
            .map_err(failed(&format!("getDirectoryHandle({name})")))?;
        handle = next.unchecked_into();
    }
    Ok(handle)
}

/// The entries of `dir`, with their generations, through the browser, as one line.
async fn entries_of(dir: &Path) -> Result<String, JsError> {
    let found = entries(&browser_dir(dir).await?).await?;
    Ok(found.into_iter().collect::<Vec<_>>().join(", "))
}

/// Fails unless the names of the files of `dir`, without their generations, are those of the
/// room `room` and nothing else: what a recovery leaves.
async fn expect_clean(dir: &Path, room: &str) -> Result<(), JsError> {
    let listed = OpfsFiles.list(dir, "").await.map_err(layer("list"))?;
    let names: Vec<String> = listed
        .iter()
        .filter_map(|path| Some(path.file_name()?.to_str()?.to_owned()))
        .collect();
    let expected = ["lock", "snap", "wal"].map(|kind| format!("room_{room}.{kind}"));
    check(names == expected, || {
        format!("{} holds {names:?}, not {expected:?}", dir.display())
    })
}

fn layer(what: &str) -> impl FnOnce(std::io::Error) -> JsError + '_ {
    move |err| JsError::new(&format!("{what}: {err}"))
}

/// The header of the snapshot of `room` in `dir`, read through the layer.
async fn snapshot_header(dir: &Path, room: &str) -> Result<FileHeader, JsError> {
    let file = OpfsFiles
        .open_read(&dir.join(format!("room_{room}.snap")))
        .await;
    let read = file
        .map_err(layer("open_read"))?
        .read_at(0, HEADER_SIZE)
        .await;
    let read: [u8; HEADER_SIZE] = read
        .map_err(layer("read_at"))?
        .try_into()
        .map_err(|_| JsError::new("a header of another length"))?;
    FileHeader::decode(&read).map_err(storage("the header of the snapshot"))
}

/// How far a loop in flight got: 8 bytes at the start of the file `progress-{name}` of the run,
/// outside the data directories. Dropped, which happens only when its loop ends (a Worker that is
/// terminated drops nothing), it records [`ENDED`].
struct Progress(FileSystemSyncAccessHandle);

/// What a loop in flight records when it ends, which it does only if a step of it failed or it
/// returned: the step that reads it then fails.
const ENDED: u64 = u64::MAX;

impl Drop for Progress {
    fn drop(&mut self) {
        // If it cannot record, the step that reads the record finds the last step instead, and
        // the step that started the loop saw it fail (`in_flight`).
        drop(self.record(ENDED));
        self.0.close();
    }
}

impl Progress {
    async fn dir(run: &str) -> Result<FileSystemDirectoryHandle, JsError> {
        let options = web_sys::FileSystemGetDirectoryOptions::new();
        options.set_create(true);
        let next = harness_dir()
            .await?
            .get_directory_handle_with_options(&format!("engine-{run}"), &options);
        let dir = JsFuture::from(next)
            .await
            .map_err(failed("getDirectoryHandle"))?;
        Ok(dir.unchecked_into())
    }

    /// The record of the loop `name`, which starts at `value`.
    async fn start(run: &str, name: &str, value: u64) -> Result<Self, JsError> {
        let entry = format!("progress-{name}");
        let file = file_handle(&Self::dir(run).await?, &entry, true)
            .await
            .map_err(failed("getFileHandle"))?;
        let handle = open(&file)
            .await
            .map_err(failed("createSyncAccessHandle"))?;
        write_all(&handle, &value.to_le_bytes())?;
        Ok(Self(handle))
    }

    /// Records `value` with one write, and flushes it.
    fn record(&self, value: u64) -> Result<(), JsError> {
        let options = FileSystemReadWriteOptions::new();
        options.set_at(0.0);
        let written = self
            .0
            .write_with_u8_array_and_options(&value.to_le_bytes(), &options)
            .map_err(failed("write"))?;
        check(written == 8.0, || {
            format!("the record wrote {written} bytes")
        })?;
        self.0.flush().map_err(failed("flush"))
    }

    /// What the loop `name` recorded last, once the Worker that ran it released the file.
    async fn read(run: &str, name: &str) -> Result<u64, JsError> {
        let entry = format!("progress-{name}");
        let file = file_handle(&Self::dir(run).await?, &entry, false)
            .await
            .map_err(failed("getFileHandle"))?;
        let deadline = now()? + RELEASE_WAIT_MS;
        let handle = loop {
            match open(&file).await {
                Ok(handle) => break handle,
                Err(_) if now()? < deadline => sleep(20).await?,
                Err(err) => return Err(failed("the record is still taken")(err)),
            }
        };
        let bytes = read_all(&handle);
        handle.close();
        let bytes: [u8; 8] = bytes?.try_into().map_err(|bytes: Vec<u8>| {
            JsError::new(&format!("a record of {} bytes", bytes.len()))
        })?;
        let recorded = u64::from_le_bytes(bytes);
        check(recorded != ENDED, || {
            format!(
                "the loop {name} ended before the Worker was terminated: a step of it failed, or \
                 it returned"
            )
        })?;
        Ok(recorded)
    }
}

/// What a loop in flight shares with the step that started it: how many steps it ran, and the
/// error of one that failed, which ends it.
#[derive(Default)]
struct Flight {
    steps: Cell<u64>,
    failed: RefCell<Option<String>>,
}

/// Runs `step` over and over, until the Worker ends, in a task of the event loop of the Worker,
/// recording in `progress` what each one gives, with a turn of the event loop after each `burst`
/// of them; once it ran [`STARTED`] of them, and then for a random part of the time they took,
/// so that the cut that follows falls at a random point of a later one, gives back, or gives the
/// error of one that failed, also during that time. A step that fails ends the loop, which
/// records [`ENDED`] as it drops `progress`.
async fn in_flight<S, F>(progress: Progress, burst: u64, mut step: S) -> Result<(), JsError>
where
    S: FnMut() -> F + 'static,
    F: Future<Output = Result<u64, JsError>> + 'static,
{
    let flight = Rc::new(Flight::default());
    let shared = Rc::clone(&flight);
    let started = now()?;
    wasm_bindgen_futures::spawn_local(async move {
        loop {
            let done = match step().await {
                Ok(value) => progress.record(value),
                Err(err) => Err(err),
            };
            if let Err(err) = done {
                let err = JsValue::from(err);
                *shared.failed.borrow_mut() = Some(crate::js::describe(&err));
                return;
            }
            shared.steps.set(shared.steps.get() + 1);
            if shared.steps.get() % burst == 0 {
                if let Err(err) = sleep(0).await {
                    let err = JsValue::from(err);
                    *shared.failed.borrow_mut() = Some(crate::js::describe(&err));
                    return;
                }
            }
        }
    });
    let failed = |flight: &Flight| match flight.failed.borrow().clone() {
        Some(err) => Err(JsError::new(&format!("the loop in flight failed: {err}"))),
        None => Ok(()),
    };
    loop {
        failed(&flight)?;
        let steps = flight.steps.get();
        if steps >= STARTED {
            let took = now()? - started;
            sleep((js_sys::Math::random() * took) as i32).await?;
            return failed(&flight);
        }
        sleep(1).await?;
    }
}

/// A room, `kept`, that applies batches with its automatic compactions, which nobody awaits, and
/// that stays open across the reload that follows, with the engine in the Worker.
#[wasm_bindgen]
pub async fn engine_write(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_write", write(&run)).await
}

/// The rows of `kept`, in batches of 5.
const KEPT: u64 = 1000;

async fn write(run: &str) -> Result<(), JsError> {
    let dir = scenario(run, "kept");
    let options = DiskStorageOptions::new(&dir)
        .min_compaction_bytes(4096)
        .compaction_ratio(0.5);
    let engine = DiskStorageEngine::new(options).map_err(storage("DiskStorageEngine::new"))?;
    let room = room_id("kept")?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    for first in (1..=KEPT).step_by(5) {
        let applied = engine.apply_batch(&room, batch(first..=first + 4)).await;
        applied.map_err(storage("apply_batch"))?;
        // The compactions it launched run in the turns of the event loop.
        if first % 50 == 1 {
            sleep(0).await?;
        }
    }
    expect_rows(&engine, &room, KEPT).await?;
    // An automatic compaction ran to its end, with nobody awaiting it.
    let deadline = now()? + 5000.0;
    while snapshot_header(&dir, "kept").await?.snapshot_seq == 0 {
        check(now()? < deadline, || "no automatic compaction ran".into())?;
        sleep(20).await?;
    }
    HELD.with(|held| held.borrow_mut().push(engine));
    Ok(())
}

/// After the page reloaded, with `kept` open in the Worker that the reload ended: it reopens with
/// every row, and a snapshot that an automatic compaction wrote; and it goes on.
#[wasm_bindgen]
pub async fn engine_after_reload(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_after_reload", after_reload(&run)).await
}

async fn after_reload(run: &str) -> Result<(), JsError> {
    let dir = scenario(run, "kept");
    let room = room_id("kept")?;
    let engine = engine_at(&dir)?;
    open_released(&engine, &room).await?;
    expect_rows(&engine, &room, KEPT).await?;
    let header = snapshot_header(&dir, "kept").await?;
    check(header.snapshot_seq > 0, || {
        "the snapshot is that of a new room: no automatic compaction ran".into()
    })?;
    let applied = engine.apply_batch(&room, batch(KEPT + 1..=KEPT + 10)).await;
    applied.map_err(storage("apply_batch"))?;
    // A schema that does not evolve from that of the room is refused, and one that does is
    // taken, which the room reopens with.
    let refused = engine
        .reload_schema(&room, schema_with("name", DataType::Int))
        .await;
    check(
        matches!(refused, Err(StorageError::SchemaNotEvolving { .. })),
        || format!("a reload to a schema that does not evolve gave {refused:?}"),
    )?;
    let evolved = schema_with("email", DataType::String);
    let reloaded = engine.reload_schema(&room, evolved.clone()).await;
    reloaded.map_err(storage("reload_schema"))?;
    engine
        .compact_room(&room)
        .await
        .map_err(storage("compact_room"))?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    let engine = engine_at(&dir)?;
    engine
        .open_room(&room, evolved)
        .await
        .map_err(storage("open_room"))?;
    expect_rows(&engine, &room, KEPT + 10).await?;
    let header = snapshot_header(&dir, "kept").await?;
    check(header.snapshot_seq == KEPT + 10, || {
        format!("the snapshot is at {}", header.snapshot_seq)
    })?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    expect_clean(&dir, "kept").await
}

/// The rows of `batches` before its loop, and of each batch of the loop.
const BATCHES_BEFORE: u64 = 200;
const BATCH_ROWS: u64 = 50;

/// A room, `batches`, with a loop of batches in flight, which the page cuts.
#[wasm_bindgen]
pub async fn engine_batches_in_flight(run: String) -> Result<(), JsError> {
    within(
        LIMIT_MS,
        "engine_batches_in_flight",
        batches_in_flight(&run),
    )
    .await
}

async fn batches_in_flight(run: &str) -> Result<(), JsError> {
    let engine = engine_at(&scenario(run, "batches"))?;
    let room = room_id("batches")?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    for first in (1..=BATCHES_BEFORE).step_by(20) {
        let applied = engine.apply_batch(&room, batch(first..=first + 19)).await;
        applied.map_err(storage("apply_batch"))?;
    }
    let progress = Progress::start(run, "batches", BATCHES_BEFORE).await?;
    let next = Rc::new(Cell::new(BATCHES_BEFORE + 1));
    in_flight(progress, 1, move || {
        let (engine, room, next) = (engine.clone(), room.clone(), Rc::clone(&next));
        async move {
            let first = next.get();
            let last = first + BATCH_ROWS - 1;
            let applied = engine.apply_batch(&room, batch(first..=last)).await;
            applied.map_err(storage("apply_batch"))?;
            next.set(last + 1);
            // A batch is one turn of the event loop (its write, its flush and its application),
            // where Chromium does not end a Worker: a turn here lets the cut fall once the batch
            // is in the WAL and before its acknowledgement is recorded.
            sleep(0).await?;
            Ok(last)
        }
    })
    .await
}

/// After the Worker was terminated with batches in flight: `batches` reopens with every batch
/// acknowledged, and at most the one in flight, whole; and it goes on.
#[wasm_bindgen]
pub async fn engine_batches_cut(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_batches_cut", batches_cut(&run)).await
}

async fn batches_cut(run: &str) -> Result<(), JsError> {
    let acknowledged = Progress::read(run, "batches").await?;
    check(
        acknowledged >= BATCHES_BEFORE + STARTED * BATCH_ROWS,
        || format!("the loop recorded {acknowledged}: it had not started"),
    )?;
    let dir = scenario(run, "batches");
    let room = room_id("batches")?;
    let engine = engine_at(&dir)?;
    open_released(&engine, &room).await?;
    let head = engine
        .get_head_seq(&room)
        .await
        .map_err(storage("get_head_seq"))?;
    let head = head.get();
    check(
        head == acknowledged || head == acknowledged + BATCH_ROWS,
        || format!("{acknowledged} rows were acknowledged, and the room reopens at {head}"),
    )?;
    expect_rows(&engine, &room, head).await?;
    let fell = if head > acknowledged {
        "once a batch was in the WAL and before its acknowledgement was recorded"
    } else {
        "between two batches, or in one before its write"
    };
    note(&format!(
        "engine_batches_cut: the cut fell {fell}: {acknowledged} rows acknowledged, {head} in the \
         room"
    ));
    let applied = engine.apply_batch(&room, batch(head + 1..=head + 5)).await;
    applied.map_err(storage("apply_batch"))?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    let engine = engine_at(&dir)?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    expect_rows(&engine, &room, head + 5).await?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    expect_clean(&dir, "batches").await
}

/// The rows of `compactions` and of `snapshots` before their loops, so that a compaction or the
/// installation of a snapshot takes a while.
const LARGE: u64 = 2000;

/// Applies the rows `1..=LARGE` to `room` of `engine`, in batches of 100.
async fn fill(engine: &impl StorageEngine, room: &RoomId) -> Result<(), JsError> {
    for first in (1..=LARGE).step_by(100) {
        let applied = engine.apply_batch(room, batch(first..=first + 99)).await;
        applied.map_err(storage("apply_batch"))?;
    }
    Ok(())
}

/// A room, `compactions`, with a loop in flight of a batch and a compaction, which the page cuts.
#[wasm_bindgen]
pub async fn engine_compactions_in_flight(run: String) -> Result<(), JsError> {
    within(
        LIMIT_MS,
        "engine_compactions_in_flight",
        compactions_in_flight(&run),
    )
    .await
}

async fn compactions_in_flight(run: &str) -> Result<(), JsError> {
    let engine = engine_at(&scenario(run, "compactions"))?;
    let room = room_id("compactions")?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    fill(&engine, &room).await?;
    engine
        .compact_room(&room)
        .await
        .map_err(storage("compact_room"))?;
    let progress = Progress::start(run, "compactions", LARGE).await?;
    let next = Rc::new(Cell::new(LARGE + 1));
    // The acknowledged row is recorded as the next step starts, before its compaction.
    let compacting = Rc::new(Cell::new(false));
    in_flight(progress, 1, move || {
        let (engine, room) = (engine.clone(), room.clone());
        let (next, compacting) = (Rc::clone(&next), Rc::clone(&compacting));
        async move {
            if compacting.replace(true) {
                engine
                    .compact_room(&room)
                    .await
                    .map_err(storage("compact_room"))?;
            }
            let key = next.get();
            let applied = engine.apply_batch(&room, batch(key..=key)).await;
            applied.map_err(storage("apply_batch"))?;
            next.set(key + 1);
            Ok(key)
        }
    })
    .await
}

/// Where the entries that a cut left in a data directory, separated by `, `, say it fell.
fn where_it_fell(left: &str, room: &str) -> &'static str {
    let names: Vec<&str> = left
        .split(", ")
        .map(|entry| entry.split('~').next().unwrap_or(entry))
        .collect();
    let generations = ["lock", "snap", "wal"].iter().any(|kind| {
        let name = format!("room_{room}.{kind}");
        names.iter().filter(|found| **found == name).count() > 1
    });
    if left.contains(".snap.tmp.") {
        "while a snapshot was staged"
    } else if left.contains(".wal.compacting") {
        "with the WAL rotated, before the cleanup"
    } else if generations {
        "with a replacement whose old generation stayed"
    } else {
        "where it left nothing behind"
    }
}

/// After the Worker was terminated with compactions in flight: `compactions` reopens with every
/// row acknowledged, and at most the one in flight; its recovery removes what the cut left; and
/// it goes on.
#[wasm_bindgen]
pub async fn engine_compactions_cut(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_compactions_cut", compactions_cut(&run)).await
}

async fn compactions_cut(run: &str) -> Result<(), JsError> {
    let acknowledged = Progress::read(run, "compactions").await?;
    check(acknowledged >= LARGE + STARTED, || {
        format!("the loop recorded {acknowledged}: it had not started")
    })?;
    let dir = scenario(run, "compactions");
    let left = entries_of(&dir).await?;
    let room = room_id("compactions")?;
    let engine = engine_at(&dir)?;
    open_released(&engine, &room).await?;
    let head = engine
        .get_head_seq(&room)
        .await
        .map_err(storage("get_head_seq"))?;
    let head = head.get();
    check(head == acknowledged || head == acknowledged + 1, || {
        format!("{acknowledged} rows were acknowledged, and the room reopens at {head}")
    })?;
    expect_rows(&engine, &room, head).await?;
    note(&format!(
        "engine_compactions_cut: the cut fell {}: {acknowledged} rows acknowledged, {head} in the \
         room; it left {left}",
        where_it_fell(&left, "compactions")
    ));
    engine
        .compact_room(&room)
        .await
        .map_err(storage("compact_room"))?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    let engine = engine_at(&dir)?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    expect_rows(&engine, &room, head).await?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    expect_clean(&dir, "compactions").await
}

/// A room, `snapshots`, with a loop in flight that installs snapshots of a memory engine, each one
/// row ahead of the room, which the page cuts.
#[wasm_bindgen]
pub async fn engine_snapshots_in_flight(run: String) -> Result<(), JsError> {
    within(
        LIMIT_MS,
        "engine_snapshots_in_flight",
        snapshots_in_flight(&run),
    )
    .await
}

async fn snapshots_in_flight(run: &str) -> Result<(), JsError> {
    let engine = engine_at(&scenario(run, "snapshots"))?;
    let room = room_id("snapshots")?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    fill(&engine, &room).await?;
    let source = MemoryStorageEngine::new();
    source
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    fill(&source, &room).await?;
    let progress = Progress::start(run, "snapshots", LARGE).await?;
    let next = Rc::new(Cell::new(LARGE + 1));
    in_flight(progress, 1, move || {
        let (engine, source, room) = (engine.clone(), source.clone(), room.clone());
        let next = Rc::clone(&next);
        async move {
            let head = next.get();
            let applied = source.apply_batch(&room, batch(head..=head)).await;
            applied.map_err(storage("apply_batch"))?;
            let snapshot = source.create_snapshot(&room).await;
            let snapshot = snapshot.map_err(storage("create_snapshot"))?;
            let installed = engine
                .apply_snapshot(&room, schema(), snapshot.into(), head.into())
                .await;
            installed.map_err(storage("apply_snapshot"))?;
            next.set(head + 1);
            Ok(head)
        }
    })
    .await
}

/// After the Worker was terminated with snapshot installations in flight: `snapshots` reopens
/// with the last snapshot installed, or the one in flight, whole; and it goes on.
#[wasm_bindgen]
pub async fn engine_snapshots_cut(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_snapshots_cut", snapshots_cut(&run)).await
}

async fn snapshots_cut(run: &str) -> Result<(), JsError> {
    let installed = Progress::read(run, "snapshots").await?;
    check(installed >= LARGE + STARTED, || {
        format!("the loop recorded {installed}: it had not started")
    })?;
    let dir = scenario(run, "snapshots");
    let left = entries_of(&dir).await?;
    let room = room_id("snapshots")?;
    let engine = engine_at(&dir)?;
    open_released(&engine, &room).await?;
    let head = engine
        .get_head_seq(&room)
        .await
        .map_err(storage("get_head_seq"))?;
    let head = head.get();
    check(head == installed || head == installed + 1, || {
        format!("the snapshot at {installed} was installed, and the room reopens at {head}")
    })?;
    expect_rows(&engine, &room, head).await?;
    let fell = if head > installed {
        "once the snapshot in flight was installed and before it was recorded"
    } else {
        where_it_fell(&left, "snapshots")
    };
    note(&format!(
        "engine_snapshots_cut: the cut fell {fell}: the snapshot at {installed} installed, the room \
         at {head}; it left {left}"
    ));
    let applied = engine.apply_batch(&room, batch(head + 1..=head + 1)).await;
    applied.map_err(storage("apply_batch"))?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    let engine = engine_at(&dir)?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    expect_rows(&engine, &room, head + 1).await?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    expect_clean(&dir, "snapshots").await
}

/// The rows of each cycle of `deletes`, in batches of [`DELETE_BATCH`].
const DELETE_ROWS: u64 = 80;
const DELETE_BATCH: u64 = 20;

/// The phases of a cycle of `deletes`, recorded with the cycle as `cycle * 4 + phase`.
const WRITING: u64 = 0;
const DELETING: u64 = 1;
const DELETED: u64 = 2;

/// A room, `deletes`, with a loop in flight of cycles that write the room, compact it, close it
/// and delete it, which the page cuts.
#[wasm_bindgen]
pub async fn engine_deletes_in_flight(run: String) -> Result<(), JsError> {
    within(
        LIMIT_MS,
        "engine_deletes_in_flight",
        deletes_in_flight(&run),
    )
    .await
}

async fn deletes_in_flight(run: &str) -> Result<(), JsError> {
    let engine = engine_at(&scenario(run, "deletes"))?;
    let room = room_id("deletes")?;
    let progress = Progress::start(run, "deletes", WRITING).await?;
    let next = Rc::new(Cell::new(WRITING));
    in_flight(progress, 1, move || {
        let (engine, room, next) = (engine.clone(), room.clone(), Rc::clone(&next));
        async move {
            let recorded = next.get();
            let (cycle, phase) = (recorded / 4, recorded % 4);
            let following = match phase {
                WRITING => {
                    let opened = engine.open_room(&room, schema()).await;
                    opened.map_err(storage("open_room"))?;
                    for first in (1..=DELETE_ROWS).step_by(DELETE_BATCH as usize) {
                        let last = first + DELETE_BATCH - 1;
                        let rows = batch_of(first..=last, |key| cycle_row(key, cycle));
                        let applied = engine.apply_batch(&room, rows).await;
                        applied.map_err(storage("apply_batch"))?;
                        if last == DELETE_ROWS - DELETE_BATCH {
                            let compacted = engine.compact_room(&room).await;
                            compacted.map_err(storage("compact_room"))?;
                        }
                    }
                    engine
                        .close_room(&room)
                        .await
                        .map_err(storage("close_room"))?;
                    cycle * 4 + DELETING
                }
                _ => {
                    engine
                        .delete_room(&room)
                        .await
                        .map_err(storage("delete_room"))?;
                    (cycle + 1) * 4 + WRITING
                }
            };
            next.set(following);
            // What the loop goes on to: deleting, or the next cycle once it deleted.
            Ok(if following % 4 == WRITING {
                cycle * 4 + DELETED
            } else {
                following
            })
        }
    })
    .await
}

/// After the Worker was terminated with deletions in flight: `deletes` reopens at a prefix of the
/// log of the cycle that the record says, with the rows of that cycle, or empty, and deleting it
/// again leaves it empty. With the cut in a deletion, the prefixes are those that each removal
/// leaves (all of it, the snapshot alone, or nothing); once the cycle deleted the room, they are
/// those of the writing of the next cycle.
#[wasm_bindgen]
pub async fn engine_deletes_cut(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_deletes_cut", deletes_cut(&run)).await
}

async fn deletes_cut(run: &str) -> Result<(), JsError> {
    let recorded = Progress::read(run, "deletes").await?;
    let (cycle, phase) = (recorded / 4, recorded % 4);
    // Each cycle records twice: once closed, and once deleted.
    let steps = match phase {
        DELETING => cycle * 2 + 1,
        DELETED => cycle * 2 + 2,
        _ => 0,
    };
    check(steps >= STARTED, || {
        format!("the loop recorded {recorded}, after {steps} steps: it had not started")
    })?;
    // The heads that the cut may leave, and the cycle whose rows the room holds.
    let (heads, rows_of) = match phase {
        DELETING => (vec![DELETE_ROWS, DELETE_ROWS - DELETE_BATCH, 0], cycle),
        _ => (
            (0..=DELETE_ROWS).step_by(DELETE_BATCH as usize).collect(),
            cycle + 1,
        ),
    };
    let dir = scenario(run, "deletes");
    let left = entries_of(&dir).await?;
    let room = room_id("deletes")?;
    let engine = engine_at(&dir)?;
    open_released(&engine, &room).await?;
    let head = engine
        .get_head_seq(&room)
        .await
        .map_err(storage("get_head_seq"))?;
    let head = head.get();
    check(heads.contains(&head), || {
        format!("with {recorded} recorded, the room reopens at {head}, which is none of {heads:?}")
    })?;
    expect_rows_of(&engine, &room, head, |key| cycle_row(key, rows_of)).await?;
    let phase = match phase {
        DELETING => "deleting the room, once closed",
        _ => "writing the room again, once deleted",
    };
    note(&format!(
        "engine_deletes_cut: the cut fell in the cycle {cycle}, {phase}, and the room reopens at \
         {head} of {DELETE_ROWS}; it left {left}"
    ));
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    engine
        .delete_room(&room)
        .await
        .map_err(storage("delete_room"))?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    expect_rows(&engine, &room, 0).await?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    expect_clean(&dir, "deletes").await
}

/// The rows of the native snapshots of the test server.
const NATIVE_ROWS: u64 = 500;
const NATIVE_AHEAD: u64 = 800;

/// The snapshot with `rows` rows of a room of the native disk engine, from the test server.
async fn native_snapshot(rows: u64) -> Result<Vec<u8>, JsError> {
    let body = format!("{{\"rows\": {rows}}}");
    let response = fetch(
        "POST",
        "/harness/snapshot",
        &[("content-type", "application/json")],
        Some(body.as_bytes()),
    )
    .await
    .map_err(failed("/harness/snapshot"))?;
    let status = response.status();
    let body = bytes(&response).await?;
    check(status == 200, || {
        format!(
            "/harness/snapshot: HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        )
    })?;
    Ok(body)
}

/// Fails unless `snapshot` is an envelope compressed with Zstandard.
fn expect_zstd(snapshot: &[u8], what: &str) -> Result<(), JsError> {
    let header = SnapshotEnvelopeHeader::parse(snapshot)
        .map_err(|err| JsError::new(&format!("{what}: {err}")))?;
    check(header.compression() == SnapshotCompression::Zstd, || {
        format!("{what} is not compressed with Zstandard")
    })
}

/// `apply_snapshot` of a snapshot of the native engine, compressed with `zstd`, into a new room,
/// and of one of its own, compressed with `ruzstd`, which the native engine decodes too, into
/// another; then of a native one ahead of a room with rows. The rooms stay open across the reload
/// that follows.
#[wasm_bindgen]
pub async fn engine_snapshots(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_snapshots", snapshots(&run)).await
}

async fn snapshots(run: &str) -> Result<(), JsError> {
    let engine = engine_at(&scenario(run, "installed"))?;
    let (native, own) = (room_id("native")?, room_id("own")?);
    for room in [&native, &own] {
        let opened = engine.open_room(room, schema()).await;
        opened.map_err(storage("open_room"))?;
    }
    let snapshot = native_snapshot(NATIVE_ROWS).await?;
    expect_zstd(&snapshot, "the native snapshot")?;
    let installed = engine
        .apply_snapshot(&native, schema(), snapshot.into(), NATIVE_ROWS.into())
        .await;
    installed.map_err(storage("apply_snapshot of the native snapshot"))?;
    expect_rows(&engine, &native, NATIVE_ROWS).await?;

    let snapshot = engine.create_snapshot(&native).await;
    let snapshot = snapshot.map_err(storage("create_snapshot"))?;
    expect_zstd(&snapshot, "the snapshot of the browser")?;
    let response = fetch(
        "POST",
        &format!("/harness/snapshot/check?head={NATIVE_ROWS}"),
        &[("content-type", "application/octet-stream")],
        Some(&snapshot),
    )
    .await
    .map_err(failed("/harness/snapshot/check"))?;
    let status = response.status();
    let answer = text(&response).await?;
    check(
        status == 200 && answer == format!("{NATIVE_ROWS} of {NATIVE_ROWS}"),
        || format!("the native engine read the snapshot of the browser: HTTP {status}: {answer}"),
    )?;
    let installed = engine
        .apply_snapshot(&own, schema(), snapshot.into(), NATIVE_ROWS.into())
        .await;
    installed.map_err(storage("apply_snapshot of the snapshot of the browser"))?;
    expect_rows(&engine, &own, NATIVE_ROWS).await?;

    let snapshot = native_snapshot(NATIVE_AHEAD).await?;
    let installed = engine
        .apply_snapshot(&native, schema(), snapshot.into(), NATIVE_AHEAD.into())
        .await;
    installed.map_err(storage("apply_snapshot of a native snapshot ahead"))?;
    expect_rows(&engine, &native, NATIVE_AHEAD).await?;
    HELD.with(|held| held.borrow_mut().push(engine));
    Ok(())
}

/// After the page reloaded with them open: the rooms of `engine_snapshots` reopen with their
/// snapshots, from the files the engine wrote, and go on.
#[wasm_bindgen]
pub async fn engine_snapshots_after_reload(run: String) -> Result<(), JsError> {
    within(
        LIMIT_MS,
        "engine_snapshots_after_reload",
        snapshots_after_reload(&run),
    )
    .await
}

async fn snapshots_after_reload(run: &str) -> Result<(), JsError> {
    let dir = scenario(run, "installed");
    let engine = engine_at(&dir)?;
    for (name, head) in [("native", NATIVE_AHEAD), ("own", NATIVE_ROWS)] {
        let room = room_id(name)?;
        open_released(&engine, &room).await?;
        expect_rows(&engine, &room, head).await?;
        let header = snapshot_header(&dir, name).await?;
        check(header.snapshot_seq == head, || {
            format!(
                "the snapshot of {name} is at {}, not {head}",
                header.snapshot_seq
            )
        })?;
        let applied = engine.apply_batch(&room, batch(head + 1..=head + 1)).await;
        applied.map_err(storage("apply_batch"))?;
        engine
            .close_room(&room)
            .await
            .map_err(storage("close_room"))?;
        let reopened = engine.open_room(&room, schema()).await;
        reopened.map_err(storage("open_room"))?;
        expect_rows(&engine, &room, head + 1).await?;
    }
    Ok(())
}

/// The file of the run that names the load of the page that holds `locked`.
fn holder(run: &str) -> PathBuf {
    PathBuf::from(format!("harness/engine-{run}/locked.holder"))
}

/// In the first page: the room `locked`, open with three rows, which the engine keeps open.
#[wasm_bindgen]
pub async fn engine_hold(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_hold", hold(&run)).await
}

async fn hold(run: &str) -> Result<(), JsError> {
    let engine = engine_at(&scenario(run, "locked"))?;
    let room = room_id("locked")?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    let applied = engine.apply_batch(&room, batch(1..=3)).await;
    applied.map_err(storage("apply_batch"))?;
    let file = OpfsFiles
        .create(&holder(run))
        .await
        .map_err(layer("create"))?;
    let written = file.write_at(0, page()?.into_bytes()).await;
    written.map_err(layer("write_at"))?;
    HELD.with(|held| held.borrow_mut().push(engine));
    Ok(())
}

/// In the second page, while the first one has `locked` open: opening it, and deleting it, is
/// `RoomLocked`, and changes no entry of its directory.
#[wasm_bindgen]
pub async fn engine_locked(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_locked", locked(&run)).await
}

async fn locked(run: &str) -> Result<(), JsError> {
    let file = OpfsFiles
        .open_read(&holder(run))
        .await
        .map_err(layer("open_read"))?;
    let size = file.size().await.map_err(layer("size"))?;
    let read = file
        .read_at(0, size as usize)
        .await
        .map_err(layer("read_at"))?;
    check(read != page()?.into_bytes(), || {
        "engine_locked runs in the page that has the room open, not in a second page".into()
    })?;
    let dir = scenario(run, "locked");
    let before = entries_of(&dir).await?;
    let engine = engine_at(&dir)?;
    let room = room_id("locked")?;
    let opened = engine.open_room(&room, schema()).await;
    check(matches!(opened, Err(StorageError::RoomLocked(_))), || {
        format!("open_room in a second page gave {opened:?}, not RoomLocked")
    })?;
    let deleted = engine.delete_room(&room).await;
    check(matches!(deleted, Err(StorageError::RoomLocked(_))), || {
        format!("delete_room in a second page gave {deleted:?}, not RoomLocked")
    })?;
    let after = entries_of(&dir).await?;
    check(after == before, || {
        format!("the second page changed the room: {before} before, {after} after")
    })
}

/// In the first page: a row more in `locked`, which the second page did not stop, and the room
/// closed.
#[wasm_bindgen]
pub async fn engine_release(_run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_release", release()).await
}

async fn release() -> Result<(), JsError> {
    let held = HELD.with(|held| held.borrow_mut().pop());
    let engine = held.ok_or_else(|| JsError::new("engine_hold kept no engine"))?;
    let room = room_id("locked")?;
    let applied = engine.apply_batch(&room, batch(4..=4)).await;
    applied.map_err(storage("apply_batch"))?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))
}

/// In the second page, once the first one closed `locked`: it opens, with its four rows.
#[wasm_bindgen]
pub async fn engine_free(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_free", free(&run)).await
}

async fn free(run: &str) -> Result<(), JsError> {
    let engine = engine_at(&scenario(run, "locked"))?;
    let room = room_id("locked")?;
    open_released(&engine, &room).await?;
    expect_rows(&engine, &room, 4).await?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))
}

/// Polls `operation` once, so that it starts, and drops it: `None` if it did not end there.
fn dropped<T>(operation: impl Future<Output = T>) -> Option<T> {
    operation.now_or_never()
}

/// Operations dropped by their callers once they started, which run to their end in their tasks:
/// `open_room` (another open finds the room open), `compact_room` and `apply_snapshot` (closing
/// waits for them, and the room reopens with what they wrote) and `delete_room` (an open waits for
/// it, and finds the room empty).
#[wasm_bindgen]
pub async fn engine_dropped(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "engine_dropped", drop_operations(&run)).await
}

async fn drop_operations(run: &str) -> Result<(), JsError> {
    let dir = scenario(run, "dropped");
    let engine = engine_at(&dir)?;
    let room = room_id("dropped")?;
    check(dropped(engine.open_room(&room, schema())).is_none(), || {
        "open_room ended at its first poll".into()
    })?;
    let again = engine.open_room(&room, schema()).await;
    check(
        matches!(again, Err(StorageError::RoomAlreadyOpen(_))),
        || format!("an open after a dropped one gave {again:?}, not RoomAlreadyOpen"),
    )?;
    let applied = engine.apply_batch(&room, batch(1..=10)).await;
    applied.map_err(storage("apply_batch"))?;

    check(dropped(engine.compact_room(&room)).is_none(), || {
        "compact_room ended at its first poll".into()
    })?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    let header = snapshot_header(&dir, "dropped").await?;
    check(header.snapshot_seq == 10, || {
        format!(
            "a dropped compaction left the snapshot at {}",
            header.snapshot_seq
        )
    })?;

    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    let snapshot = native_snapshot(20).await?;
    let installed = engine.apply_snapshot(&room, schema(), snapshot.into(), 20.into());
    check(dropped(installed).is_none(), || {
        "apply_snapshot ended at its first poll".into()
    })?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    expect_rows(&engine, &room, 20).await?;

    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    check(dropped(engine.delete_room(&room)).is_none(), || {
        "delete_room ended at its first poll".into()
    })?;
    engine
        .open_room(&room, schema())
        .await
        .map_err(storage("open_room"))?;
    expect_rows(&engine, &room, 0).await?;
    engine
        .close_room(&room)
        .await
        .map_err(storage("close_room"))?;
    expect_clean(&dir, "dropped").await
}

/// How long opening the rooms of a step of the suite `rooms` may take, in each of its two passes,
/// before the step stops opening and notes how far it got, in milliseconds: so that a slow
/// browser gives its number instead of failing the step at [`ROOMS_LIMIT_MS`].
const ROOMS_BUDGET_MS: f64 = 20_000.0;

/// Opens `rooms` rooms one after another in a directory of their own, the first time, closes them,
/// and opens them again with another engine, and notes how long each pass took: every operation
/// of the layer lists the directory, with the entries of all the rooms (three each).
async fn open_rooms(run: &str, rooms: usize) -> Result<(), JsError> {
    let dir = scenario(run, &format!("rooms-{rooms}"));
    let ids = (0..rooms)
        .map(|room| room_id(&format!("r{room}")))
        .collect::<Result<Vec<_>, _>>()?;
    let mut passes = Vec::new();
    let mut opened = rooms;
    let mut entries = 0;
    for _ in 0..2 {
        let engine = engine_at(&dir)?;
        let started = now()?;
        let mut done = 0;
        for room in &ids[..opened] {
            if now()? - started > ROOMS_BUDGET_MS {
                break;
            }
            let open = engine.open_room(room, schema()).await;
            open.map_err(storage("open_room"))?;
            done += 1;
        }
        let took = now()? - started;
        for room in &ids[..done] {
            engine
                .close_room(room)
                .await
                .map_err(storage("close_room"))?;
        }
        let stopped = if done < opened {
            ", when its time ran out"
        } else {
            ""
        };
        passes.push(format!(
            "{done} in {took:.0} ms ({:.1} ms a room{stopped})",
            took / done.max(1) as f64
        ));
        if passes.len() == 1 {
            // The rooms that the first pass created, three entries each.
            entries = done * 3;
        }
        opened = done;
    }
    note(&format!(
        "opening {rooms} rooms over the OPFS, in a directory of their own: {} the first time, and \
         {} again, with {} entries in the directory",
        passes[0], passes[1], entries
    ));
    Ok(())
}

/// How long opening 10 rooms takes over the OPFS, noted for the run.
#[wasm_bindgen]
pub async fn engine_rooms_10(run: String) -> Result<(), JsError> {
    within(ROOMS_LIMIT_MS, "engine_rooms_10", open_rooms(&run, 10)).await
}

/// How long opening 30 rooms takes over the OPFS, noted for the run.
#[wasm_bindgen]
pub async fn engine_rooms_30(run: String) -> Result<(), JsError> {
    within(ROOMS_LIMIT_MS, "engine_rooms_30", open_rooms(&run, 30)).await
}

/// How long opening 60 rooms takes over the OPFS, noted for the run.
#[wasm_bindgen]
pub async fn engine_rooms_60(run: String) -> Result<(), JsError> {
    within(ROOMS_LIMIT_MS, "engine_rooms_60", open_rooms(&run, 60)).await
}
