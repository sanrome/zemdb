pub mod compactor;
pub mod format;
pub mod recovery;
pub mod wal;

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use zemdb_core::{CompactRow, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation};

use crate::disk::compactor::{
    compact_room_cow, install_applied_snapshot, rename_staged_snapshot, stage_snapshot,
};
use crate::disk::recovery::recover_room;
use crate::disk::wal::WalWriter;
use crate::engine::{batched_scan, ApplyReport, RowStream, StorageEngine};
use crate::error::StorageError;
use crate::fail_point;
use crate::files::{NativeFiles, RoomFile, RoomFiles};
use crate::memory::state::{
    apply_operation, decode_received_snapshot, resolve_table_id, validate_batch, RoomData, TableRef,
};
use crate::memory::RoomSnapshotRef;
use crate::options::ScanOptions;
use crate::snapshot::DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES;
use crate::sys::run_blocking;

/// Options to configure `DiskStorageEngine`.
#[derive(Debug, Clone)]
pub struct DiskStorageOptions {
    /// Directory where `room_{id}.snap` and `room_{id}.wal` files are stored.
    pub data_dir: PathBuf,
    /// Ratio of WAL size to snapshot size that triggers automatic compaction (default: 3.0). It
    /// must be a finite number above 0.
    pub compaction_ratio: f64,
    /// Minimum WAL size in bytes before automatic compaction can trigger (default: 64 KB).
    pub min_compaction_bytes: u64,
    /// Zstandard compression level for base snapshots (default: 3), within
    /// `zstd::compression_level_range()`.
    pub zstd_level: i32,
    /// Whether automatic compaction is enabled during write batches (default: true).
    pub auto_compact: bool,
    /// Largest decompressed payload accepted by `apply_snapshot`, in bytes (default:
    /// [`DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES`], 2 GiB).
    pub max_snapshot_uncompressed_bytes: u64,
}

impl DiskStorageOptions {
    /// Creates default options for storing files in the given directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            compaction_ratio: 3.0,
            min_compaction_bytes: 64 * 1024,
            zstd_level: 3,
            auto_compact: true,
            max_snapshot_uncompressed_bytes: DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
        }
    }

    /// Sets the compaction ratio (WAL size / snapshot size threshold).
    pub fn compaction_ratio(mut self, ratio: f64) -> Self {
        self.compaction_ratio = ratio;
        self
    }

    /// Sets the minimum WAL bytes threshold for compaction.
    pub fn min_compaction_bytes(mut self, bytes: u64) -> Self {
        self.min_compaction_bytes = bytes;
        self
    }

    /// Sets the Zstandard compression level.
    pub fn zstd_level(mut self, level: i32) -> Self {
        self.zstd_level = level;
        self
    }

    /// Enables or disables automatic compaction during writes.
    pub fn auto_compact(mut self, enable: bool) -> Self {
        self.auto_compact = enable;
        self
    }

    /// Sets the largest decompressed snapshot payload accepted by `apply_snapshot`.
    pub fn max_snapshot_uncompressed_bytes(mut self, bytes: u64) -> Self {
        self.max_snapshot_uncompressed_bytes = bytes;
        self
    }
}

/// The append-only WAL of an open room. Lives behind the room's WAL mutex.
#[derive(Debug)]
pub(crate) struct DiskWal<F: RoomFiles> {
    room_id: RoomId,
    /// The WAL's handle, with its exclusive lock. Batches are appended at `len`.
    pub(crate) file: F::File,
    pub(crate) len: u64,
    /// Set when an I/O failure leaves the files in a state only recovery can resolve, such as
    /// a WAL write whose durability is unknown. Writes and compactions are then refused.
    failure: Option<String>,
}

impl<F: RoomFiles> DiskWal<F> {
    /// Marks the room as failed; it must be closed and reopened to recover from disk.
    pub(crate) fn mark_failed(&mut self, reason: String) {
        tracing::error!(room_id = %self.room_id, reason = %reason, "Room marked as failed");
        self.failure = Some(reason);
    }

    /// Marks the room as failed if `result` is an error, with `context` before the error in the
    /// reason, and returns `result` unchanged.
    pub(crate) fn fail_on_err<T>(
        &mut self,
        context: &str,
        result: Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        if let Err(err) = &result {
            self.mark_failed(format!("{context}: {err}"));
        }
        result
    }

    /// Returns an error if the room was marked as failed.
    pub(crate) fn ensure_usable(&self) -> Result<(), StorageError> {
        match &self.failure {
            Some(reason) => Err(StorageError::RoomFailed {
                room_id: self.room_id.clone(),
                reason: reason.clone(),
            }),
            None => Ok(()),
        }
    }
}

/// A change to the room's files in progress, made under the WAL mutex: a batch from its first
/// WAL byte until it is applied in memory, a snapshot from its rename until it is applied in
/// memory, or a WAL rotation, absorption or truncation from its first step to its last.
///
/// File operations complete even if the future awaiting them is dropped (see
/// [`RoomFile`]), so a change abandoned halfway (an error the change does not handle, a panic, or
/// the caller dropping the future) can leave the WAL handle, the files and the in-memory state
/// disagreeing, and a later write would land in the wrong file, at the wrong offset, or be
/// validated against a stale head. The room is therefore marked as failed when the guard drops
/// without `complete`.
pub(crate) struct PendingWrite<'a, F: RoomFiles> {
    wal: &'a mut DiskWal<F>,
    completed: bool,
}

impl<'a, F: RoomFiles> PendingWrite<'a, F> {
    pub(crate) fn begin(wal: &'a mut DiskWal<F>) -> Self {
        Self {
            wal,
            completed: false,
        }
    }

    pub(crate) fn wal(&mut self) -> &mut DiskWal<F> {
        self.wal
    }

    /// The change reached a consistent end: it is complete, or it failed in a way it handled
    /// itself (undoing it or marking the room as failed).
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl<F: RoomFiles> Drop for PendingWrite<'_, F> {
    fn drop(&mut self) {
        if !self.completed && self.wal.failure.is_none() {
            self.wal
                .mark_failed("A WAL write was interrupted before it completed".to_string());
        }
    }
}

/// In-memory state of an open room, read by queries. Lives behind the room's state lock.
#[derive(Debug)]
pub(crate) struct DiskRoomState {
    pub(crate) schema: Schema,
    pub(crate) head_seq: SequenceNumber,
    pub(crate) snapshot_seq: SequenceNumber,
    pub(crate) snapshot_len: u64,
    pub(crate) data: RoomData,
}

/// An open room on disk.
///
/// Three locks, always taken in this order:
///
/// 0. `compaction`, held by a compaction for its whole run, by `apply_snapshot` and by
///    `close_room`, so that they exclude each other. It belongs to this room instance: an
///    operation that waited for it while the room was closed (and maybe reopened) finds out
///    when it gets it, and gives up instead of working on the reopened room.
/// 1. `wal`, a mutex held by every change to the active WAL or to the room's contents: a batch
///    holds it while it is validated, appended, synced and applied in memory; compaction holds
///    it while it rotates the WAL; `apply_snapshot` while it replaces the files and the state.
///    Changes are therefore serialized, and reach memory in WAL order. (Compaction's later
///    phases touch only the snapshot and `.wal.compacting`, which the compaction lock guards.)
/// 2. `state`, a read-write lock over the in-memory state. Writers take it only to read the
///    head for validation and, once their change is durable, to apply it. Readers take it
///    shared and never wait for an `fsync`; they only see durable changes, applied in WAL
///    order, so what they observe is always a prefix of the WAL.
#[derive(Debug)]
pub(crate) struct DiskRoom<F: RoomFiles> {
    /// Handle of `room_{id}.lock`, whose exclusive lock keeps any other engine, in this process
    /// or another, from opening the room (see [`lock_room_files`]). It is released when the room
    /// is dropped.
    _lock: F::File,
    /// The engine's data directory, through which the room's files are opened, renamed and
    /// removed.
    pub(crate) files: Arc<F>,
    pub(crate) room_id: RoomId,
    pub(crate) snap_path: PathBuf,
    pub(crate) wal_path: PathBuf,
    pub(crate) compaction: Arc<Mutex<()>>,
    pub(crate) wal: Mutex<DiskWal<F>>,
    pub(crate) state: RwLock<DiskRoomState>,
    /// Set while a compaction runs. Atomic so that a guard can release it on drop without
    /// awaiting a lock.
    pub(crate) is_compacting: Arc<AtomicBool>,
}

/// Disk storage engine. Each room is stored in two files:
/// - `room_{id}.snap`: the base snapshot, behind a 64-byte `ZEM1` header, compressed with
///   Zstandard and checksummed.
/// - `room_{id}.wal`: the append-only WAL of batches since that snapshot, each framed with
///   `0xBA7C` and a CRC32.
///
/// The room's files are opened and locked, its WAL written, read, truncated and synced, and its
/// directory changed through the file layer `F` (see [`crate::files`]), by default the file
/// system of the process.
///
/// See `DiskRoom` for the locking of an open room and `compactor::compact_room_cow` for how
/// the WAL is folded into a new snapshot.
#[derive(Debug)]
pub struct DiskStorageEngine<F: RoomFiles = NativeFiles> {
    options: DiskStorageOptions,
    files: Arc<F>,
    rooms: Arc<RwLock<HashMap<RoomId, Arc<DiskRoom<F>>>>>,
}

impl<F: RoomFiles> Clone for DiskStorageEngine<F> {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            files: Arc::clone(&self.files),
            rooms: Arc::clone(&self.rooms),
        }
    }
}

impl DiskStorageEngine {
    /// Creates an engine with `options` on the file system of the process (see
    /// [`Self::with_files`]).
    pub fn new(options: DiskStorageOptions) -> Result<Self, StorageError> {
        Self::with_files(options, NativeFiles)
    }
}

impl<F: RoomFiles> DiskStorageEngine<F> {
    /// Creates an engine with `options` whose files go through `files`, or `InvalidOptions` if
    /// `compaction_ratio` is not a finite number above 0 (with NaN, 0 or a negative ratio, every
    /// batch would compact once the WAL reaches `min_compaction_bytes`) or `zstd_level` is
    /// outside the levels Zstandard accepts.
    pub fn with_files(options: DiskStorageOptions, files: F) -> Result<Self, StorageError> {
        let ratio = options.compaction_ratio;
        if !(ratio.is_finite() && ratio > 0.0) {
            return Err(StorageError::InvalidOptions(format!(
                "compaction_ratio must be a finite number above 0, got {ratio}"
            )));
        }
        let levels = zstd::compression_level_range();
        if !levels.contains(&options.zstd_level) {
            return Err(StorageError::InvalidOptions(format!(
                "zstd_level must be between {} and {}, got {}",
                levels.start(),
                levels.end(),
                options.zstd_level
            )));
        }
        Ok(Self {
            options,
            files: Arc::new(files),
            rooms: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Computes the filesystem path for a room's base snapshot file (`room_{id}.snap`).
    pub fn snap_file_path(&self, room_id: &RoomId) -> PathBuf {
        self.options.data_dir.join(format!("room_{}.snap", room_id))
    }

    /// Computes the filesystem path for a room's lock file (`room_{id}.lock`).
    pub fn lock_file_path(&self, room_id: &RoomId) -> PathBuf {
        self.options.data_dir.join(format!("room_{}.lock", room_id))
    }

    /// Computes the filesystem path for a room's append-only WAL file (`room_{id}.wal`).
    pub fn wal_file_path(&self, room_id: &RoomId) -> PathBuf {
        self.options.data_dir.join(format!("room_{}.wal", room_id))
    }

    /// Point lookup behind `get` and `get_by_id`: resolves the table and reads the row under
    /// a single read lock of the room's state.
    async fn get_row(
        &self,
        room_id: &RoomId,
        table: TableRef<'_>,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        let room = self.get_room(room_id).await?;
        let state = room.state.read().await;
        let table_id = resolve_table_id(room_id, &state.schema, table)?;
        Ok(state
            .data
            .tables()
            .get(&table_id)
            .and_then(|t| t.get(pk).cloned()))
    }

    /// Scan behind `scan` and `scan_by_id`: resolves the table and takes its rows under a
    /// single read lock of the room's state.
    async fn scan_table<'a>(
        &'a self,
        room_id: &RoomId,
        table: TableRef<'_>,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        let room = self.get_room(room_id).await?;
        let table_data = {
            let state = room.state.read().await;
            let table_id = resolve_table_id(room_id, &state.schema, table)?;
            // An O(1) clone that shares the table's nodes; writes during the scan copy only the
            // nodes they change.
            state
                .data
                .tables()
                .get(&table_id)
                .cloned()
                .unwrap_or_default()
        };

        Ok(batched_scan(table_data, options))
    }

    pub(crate) async fn get_room(
        &self,
        room_id: &RoomId,
    ) -> Result<Arc<DiskRoom<F>>, StorageError> {
        let rooms = self.rooms.read().await;
        rooms
            .get(room_id)
            .map(Arc::clone)
            .ok_or_else(|| StorageError::RoomNotFound(room_id.clone()))
    }

    /// The compaction lock of the open room `room_id`.
    async fn compaction_lock(&self, room_id: &RoomId) -> Result<Arc<Mutex<()>>, StorageError> {
        Ok(Arc::clone(&self.get_room(room_id).await?.compaction))
    }

    /// Waits for `lock`, the compaction lock of `room_id` returned by [`Self::compaction_lock`],
    /// and returns the room it belongs to. If that room was closed meanwhile (even if it was
    /// reopened since, with a lock of its own), returns `RoomNotFound`.
    async fn lock_compaction(
        &self,
        room_id: &RoomId,
        lock: Arc<Mutex<()>>,
    ) -> Result<(Arc<DiskRoom<F>>, OwnedMutexGuard<()>), StorageError> {
        let guard = lock.lock_owned().await;
        let room = self.get_room(room_id).await?;
        if !Arc::ptr_eq(&room.compaction, OwnedMutexGuard::mutex(&guard)) {
            return Err(StorageError::RoomNotFound(room_id.clone()));
        }
        Ok((room, guard))
    }

    /// Explicitly triggers snapshot compaction and WAL truncation for a room.
    ///
    /// The compaction runs in its own task: if the caller stops waiting, it still completes, so
    /// the room's files are never left halfway through a step.
    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    pub async fn compact_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let lock = self.compaction_lock(room_id).await?;
        let (room, guard) = self.lock_compaction(room_id, lock).await?;
        let options = self.options.clone();
        tokio::spawn(async move {
            let _guard = guard;
            compact_room_cow(room, &options).await
        })
        .await
        .map_err(|e| StorageError::Other(format!("Compaction task failed: {e}")))?
    }
}

/// Takes the locks of a room being opened and opens its WAL, creating it if needed.
///
/// The first lock is on `room_{id}.lock`, taken before any other file of the room is touched.
/// That file is never renamed or removed, so no other engine can open the room at any moment
/// while it is open, not even while a compaction has moved the WAL aside and its replacement
/// does not exist yet. The WAL keeps its own exclusive lock as well, on its handle: a file
/// operation of the layer still running after its caller stopped waiting keeps the file and its
/// lock open after the room is dropped (for example by an engine dropped without closing the
/// room), so the room cannot be opened again until a write still landing in the WAL has
/// finished.
async fn lock_room_files<F: RoomFiles>(
    files: &F,
    lock_path: &Path,
    wal_path: &Path,
    room_id: &RoomId,
) -> Result<(F::File, F::File), StorageError> {
    let open_locked = |path| async move {
        files
            .open_locked(path)
            .await?
            .ok_or_else(|| StorageError::RoomLocked(room_id.clone()))
    };
    let lock = open_locked(lock_path).await?;
    Ok((lock, open_locked(wal_path).await?))
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<F: RoomFiles> StorageEngine for DiskStorageEngine<F> {
    /// Opens `room_id`, recovering it from its files (see `recover_room`).
    ///
    /// Once it has the engine's rooms, it runs in its own task, which keeps the room's locks
    /// until its last file operation: if the caller stops waiting, the room still opens, and no
    /// other open takes the room while a file operation of the recovery is in flight.
    #[tracing::instrument(skip(self, schema), fields(room_id = %room_id))]
    async fn open_room(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError> {
        let mut rooms = Arc::clone(&self.rooms).write_owned().await;
        if rooms.contains_key(room_id) {
            return Err(StorageError::RoomAlreadyOpen(room_id.clone()));
        }

        let (room_id, data_dir) = (room_id.clone(), self.options.data_dir.clone());
        let (snap_path, wal_path) = (self.snap_file_path(&room_id), self.wal_file_path(&room_id));
        let lock_path = self.lock_file_path(&room_id);
        let zstd_level = self.options.zstd_level;
        let files = Arc::clone(&self.files);
        tokio::spawn(async move {
            files.create_dir_all(&data_dir).await?;
            let (lock, wal_file) =
                lock_room_files(&*files, &lock_path, &wal_path, &room_id).await?;
            // An earlier open may have created the directory, or the room's files, without a
            // successful sync: their entries are synced before anything in them is served.
            for dir in [Some(data_dir.as_path()), data_dir.parent()]
                .into_iter()
                .flatten()
            {
                fail_point::io_result("sync_dir", dir, files.sync_dir(dir).await)?;
            }

            let recovered = recover_room(
                &*files, &room_id, &snap_path, &wal_path, &schema, wal_file, zstd_level,
            )
            .await?;

            let room = DiskRoom {
                _lock: lock,
                files,
                room_id: room_id.clone(),
                snap_path,
                wal_path,
                wal: Mutex::new(DiskWal {
                    room_id: room_id.clone(),
                    file: recovered.wal_file,
                    len: recovered.wal_len,
                    failure: None,
                }),
                state: RwLock::new(DiskRoomState {
                    schema,
                    head_seq: recovered.head_seq,
                    snapshot_seq: recovered.snapshot_seq,
                    snapshot_len: recovered.snapshot_len,
                    data: recovered.data,
                }),
                is_compacting: Arc::new(AtomicBool::new(false)),
                compaction: Arc::new(Mutex::new(())),
            };

            rooms.insert(room_id.clone(), Arc::new(room));

            tracing::info!(room_id = %room_id, "Opened disk room (dual-file .snap + .wal)");
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Other(format!("Open task failed: {e}")))?
    }

    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    async fn close_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let lock = self.compaction_lock(room_id).await?;
        // Waits for a compaction or snapshot in progress to finish.
        let (room, _compaction_guard) = self.lock_compaction(room_id, lock).await?;

        // Still registered: closing needs the compaction lock, which this call holds.
        self.rooms.write().await.remove(room_id);
        // Waits for a write in progress to finish. The sync also waits for a write whose caller
        // stopped waiting, and returns its error if it failed.
        let wal = room.wal.lock().await;
        wal.file.sync_all().await?;
        tracing::info!(room_id = %room_id, "Closed disk room");
        Ok(())
    }

    /// Changes the room's schema (see the trait) under the WAL mutex, so no batch, snapshot
    /// install or WAL rotation runs meanwhile. A room opened with a schema narrower than the one
    /// its files were written under keeps the values of the columns it lacks, so the indexes of
    /// the new constraints are built from the rows and the rows checked against them, on the
    /// blocking pool; readers keep reading the old state meanwhile. Only the schema and the rows
    /// are published, under the state lock: the last phase of a compaction records the
    /// snapshot's sequence and size under that lock without the WAL mutex. Nothing touches the
    /// files, so a reload dropped by its caller changes nothing.
    #[tracing::instrument(skip(self, schema), fields(room_id = %room_id))]
    async fn reload_schema(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError> {
        let room = self.get_room(room_id).await?;
        let wal = room.wal.lock().await;
        wal.ensure_usable()?;
        let (older, data) = {
            let state = room.state.read().await;
            if state.schema == schema {
                return Ok(());
            }
            if !schema.evolves_from(&state.schema) {
                return Err(StorageError::SchemaNotEvolving {
                    room_id: room_id.clone(),
                });
            }
            (state.schema.clone(), state.data.clone())
        };

        let fail_key = room.wal_path.clone();
        let (schema, data) = run_blocking(move || {
            fail_point::pause_blocking("reload_schema.build", &fail_key);
            let data = data.evolved(&schema, true);
            data.check_added(&older, &schema)?;
            Ok::<_, StorageError>((schema, data))
        })
        .await?;
        {
            let mut state = room.state.write().await;
            state.schema = schema;
            state.data = data;
        }
        drop(wal);
        tracing::info!(room_id = %room_id, "Reloaded the schema of a disk room");
        Ok(())
    }

    #[tracing::instrument(skip(self, ops), fields(room_id = %room_id, ops_count = ops.len()))]
    async fn apply_batch(
        &self,
        room_id: &RoomId,
        ops: Vec<SequencedOperation>,
    ) -> Result<ApplyReport, StorageError> {
        let room = self.get_room(room_id).await?;
        // Held until the batch is applied in memory, so batches reach memory in WAL order and
        // the head read below is that of the last batch in the WAL.
        let mut wal = room.wal.lock().await;
        wal.ensure_usable()?;

        // Validate against the schema and the head.
        {
            let state = room.state.read().await;
            // An empty batch changes nothing; writing it would only add an empty frame to the WAL.
            if ops.is_empty() {
                return Ok(ApplyReport::new(state.head_seq, Vec::new()));
            }
            validate_batch(room_id, &state.schema, state.head_seq, &ops)?;
        }

        let wal_batch_bytes = WalWriter::encode_batch(&ops)?;
        let batch_len = wal_batch_bytes.len() as u64;

        // Write to the append-only WAL file and fsync data, without holding the state lock, so
        // that readers do not wait for the sync. A failure here leaves an unknown amount of the
        // batch on disk, and a failed fsync cannot be retried safely, so the room stops
        // accepting writes until recovery replays the WAL from disk.
        let mut pending = PendingWrite::begin(&mut wal);
        let write_result = async {
            let wal = pending.wal();
            crate::fail_point::read_only_handle(
                "apply_batch.write",
                &room.wal_path,
                &*room.files,
                &mut wal.file,
                &room.wal_path,
            )
            .await;
            wal.file.write_at(wal.len, wal_batch_bytes).await?;
            crate::fail_point::check("apply_batch.sync", &room.wal_path)?;
            crate::fail_point::pause("apply_batch.sync", &room.wal_path).await;
            wal.file.sync_data().await?;
            Ok::<(), StorageError>(())
        }
        .await;
        pending
            .wal()
            .fail_on_err("WAL append failed", write_result)?;
        pending.wal().len += batch_len;

        // Apply operations to O(1) clones of the in-memory tables and their indexes, then
        // publish them at once: readers see all of the batch or none of it, and never tables and
        // indexes of different moments, even if applying it panics. Nothing else changes them
        // meanwhile, since that needs the WAL mutex.
        let (data, head_seq, rejected) = {
            let state = room.state.read().await;
            let mut data = state.data.clone();
            let mut head_seq = state.head_seq;
            let mut rejected = Vec::new();
            for SequencedOperation { seq, op } in ops {
                if let Some(reason) = apply_operation(&mut data, &state.schema, op) {
                    rejected.push((seq, reason));
                }
                crate::fail_point::panic_point("apply_batch.apply", &room.wal_path);
                head_seq = head_seq.max(seq);
            }
            (data, head_seq, rejected)
        };
        let snapshot_len = {
            let mut state = room.state.write().await;
            state.data = data;
            state.head_seq = head_seq;
            state.snapshot_len
        };
        pending.complete();

        if self.options.auto_compact
            && !room.is_compacting.load(Ordering::Acquire)
            && wal.len >= self.options.min_compaction_bytes
            && wal.len >= (snapshot_len as f64 * self.options.compaction_ratio) as u64
        {
            if let Ok(compaction_guard) = Arc::clone(&room.compaction).try_lock_owned() {
                let room = Arc::clone(&room);
                let options_clone = self.options.clone();
                tokio::spawn(async move {
                    let _guard = compaction_guard;
                    if let Err(e) = compact_room_cow(room, &options_clone).await {
                        tracing::error!(error = %e, "Background auto-compaction failed");
                    }
                });
            }
        }

        tracing::debug!(room_id = %room_id, head_seq = head_seq.get(), "Applied batch to disk room");
        Ok(ApplyReport::new(head_seq, rejected))
    }

    async fn get(
        &self,
        room_id: &RoomId,
        table: &str,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        self.get_row(room_id, TableRef::Name(table), pk).await
    }

    async fn get_by_id(
        &self,
        room_id: &RoomId,
        table_id: u16,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        self.get_row(room_id, TableRef::Id(table_id), pk).await
    }

    async fn scan<'a>(
        &'a self,
        room_id: &RoomId,
        table: &str,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        self.scan_table(room_id, TableRef::Name(table), options)
            .await
    }

    async fn scan_by_id<'a>(
        &'a self,
        room_id: &RoomId,
        table_id: u16,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        self.scan_table(room_id, TableRef::Id(table_id), options)
            .await
    }

    async fn get_head_seq(&self, room_id: &RoomId) -> Result<SequenceNumber, StorageError> {
        let room = self.get_room(room_id).await?;
        let state = room.state.read().await;
        Ok(state.head_seq)
    }

    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    async fn create_snapshot(&self, room_id: &RoomId) -> Result<Vec<u8>, StorageError> {
        let room = self.get_room(room_id).await?;
        // O(1) clones of the tables: serialization and compression run without the lock.
        let (head_seq, tables) = {
            let state = room.state.read().await;
            (state.head_seq, state.data.tables().clone())
        };

        let zstd_level = self.options.zstd_level;
        run_blocking(move || {
            let raw = bincode::serialize(&RoomSnapshotRef {
                head_seq,
                tables: &tables,
            })
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
            crate::snapshot::encode_snapshot_envelope(&raw, true, zstd_level)
        })
        .await
    }

    /// Replaces the room's contents with `snapshot` (see the trait for its checks and the rules
    /// on its head sequence).
    ///
    /// The snapshot is decoded and checked on the blocking pool. It is then serialized,
    /// compressed and written to a temporary file under the room's compaction lock only, so
    /// writers keep going meanwhile; the head sequence is checked again under the WAL mutex
    /// before the file is installed. The snapshot is made durable before memory changes, so
    /// readers keep seeing the previous state until the new one is on disk. See
    /// `install_applied_snapshot` for the crash windows of replacing the files.
    ///
    /// Once it has the compaction lock, it runs in its own task, like `compact_room`: if the
    /// caller stops waiting, it still completes, so no file operation is left in flight when the
    /// room is closed (closing waits for the lock).
    #[tracing::instrument(skip(self, schema, snapshot), fields(room_id = %room_id, snapshot_size = snapshot.len()))]
    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: Bytes,
        expected_head_seq: SequenceNumber,
    ) -> Result<SequenceNumber, StorageError> {
        // Checked before decoding, so that both engines report an unopened room the same way.
        self.get_room(room_id).await?;

        let max_uncompressed = self.options.max_snapshot_uncompressed_bytes;
        let fail_key = self.wal_file_path(room_id);
        let (schema, (snapshot_head, data)) = run_blocking(move || {
            crate::fail_point::pause_blocking("apply_snapshot.decode", &fail_key);
            let received =
                decode_received_snapshot(&snapshot, max_uncompressed, &schema, expected_head_seq)?;
            Ok::<_, StorageError>((schema, received))
        })
        .await?;

        let lock = self.compaction_lock(room_id).await?;
        crate::fail_point::pause("apply_snapshot.lock", &self.wal_file_path(room_id)).await;
        let (room, compaction_guard) = self.lock_compaction(room_id, lock).await?;
        let zstd_level = self.options.zstd_level;
        tokio::spawn(async move {
            let _compaction_guard = compaction_guard;
            // A first check spares staging a snapshot that cannot be applied. Writers may still
            // advance the head until the WAL mutex is taken.
            let current = room.state.read().await.head_seq;
            if let Some(outcome) = snapshot_head_outcome(current, snapshot_head) {
                return outcome;
            }

            // Nothing visible changes until the staged snapshot is renamed into place.
            let staged = stage_snapshot(
                &room.snap_path,
                snapshot_head,
                data.tables(),
                zstd_level,
                &room.room_id,
            )
            .await?;
            crate::fail_point::pause("apply_snapshot.staged", &room.wal_path).await;

            let mut wal = room.wal.lock().await;
            let current = room.state.read().await.head_seq;
            let refusal = match wal.ensure_usable() {
                Err(err) => Some(Err(err)),
                Ok(()) => snapshot_head_outcome(current, snapshot_head),
            };
            if let Some(outcome) = refusal {
                staged.discard(&*room.files).await;
                return outcome;
            }

            // Armed before the rename: an installed snapshot whose WAL was not emptied would have
            // later batches replayed on top of it, so an error or a panic from here on marks the
            // room as failed. A failed rename changes nothing, so it leaves the room usable.
            let mut pending = PendingWrite::begin(&mut wal);
            if let Err(err) = rename_staged_snapshot(&room, &staged).await {
                pending.complete();
                staged.discard(&*room.files).await;
                return Err(err);
            }
            let installed = install_applied_snapshot(&room, pending.wal()).await;
            pending
                .wal()
                .fail_on_err("Persisting an applied snapshot failed", installed)?;
            crate::fail_point::panic_point("apply_snapshot.installed", &room.wal_path);

            {
                let mut state = room.state.write().await;
                state.schema = schema;
                state.head_seq = snapshot_head;
                state.data = data;
                state.snapshot_seq = snapshot_head;
                state.snapshot_len = staged.compressed_len;
            }
            pending.complete();

            tracing::info!(room_id = %room.room_id, head_seq = %snapshot_head, "Applied snapshot to disk room");
            Ok(snapshot_head)
        })
        .await
        .map_err(|e| StorageError::Other(format!("Snapshot task failed: {e}")))?
    }
}

/// What applying a snapshot at `snapshot` to a room at `current` comes to, if not a replacement:
/// a snapshot behind the room is rejected, one at its head changes nothing.
fn snapshot_head_outcome(
    current: SequenceNumber,
    snapshot: SequenceNumber,
) -> Option<Result<SequenceNumber, StorageError>> {
    if snapshot < current {
        return Some(Err(StorageError::SnapshotBehind { current, snapshot }));
    }
    (snapshot == current).then_some(Ok(current))
}

#[cfg(test)]
#[path = "../tests/disk.rs"]
mod tests;
