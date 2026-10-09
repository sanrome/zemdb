pub mod compactor;
pub mod format;
pub mod recovery;
pub mod wal;

use async_trait::async_trait;
use fs2::FileExt;
use std::collections::{HashMap, VecDeque};
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::fs::{create_dir_all, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use zemdb_core::{CompactRow, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation};

use crate::disk::compactor::{
    compact_room_cow, install_applied_snapshot, rename_staged_snapshot, stage_snapshot,
};
use crate::disk::recovery::recover_room;
use crate::disk::wal::WalWriter;
use crate::engine::{apply_scan_transforms, RowStream, StorageEngine};
use crate::error::StorageError;
use crate::memory::state::{apply_operation, validate_batch};
use crate::memory::{RoomSnapshotPayload, RoomSnapshotRef, Table};
use crate::options::{KeyRange, ScanDirection, ScanOptions};
use crate::snapshot::DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES;

/// Options to configure `DiskStorageEngine`.
#[derive(Debug, Clone)]
pub struct DiskStorageOptions {
    /// Directory where `room_{id}.snap` and `room_{id}.wal` files are stored.
    pub data_dir: PathBuf,
    /// Ratio of WAL size to snapshot size that triggers automatic compaction (default: 3.0).
    pub compaction_ratio: f64,
    /// Minimum WAL size in bytes before automatic compaction can trigger (default: 64 KB).
    pub min_compaction_bytes: u64,
    /// Zstandard compression level for base snapshots (default: 3).
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

    /// Sets the Zstandard compression level (1-22).
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
pub(crate) struct DiskWal {
    room_id: RoomId,
    pub(crate) file: File,
    pub(crate) len: u64,
    /// Set when an I/O failure leaves the files in a state only recovery can resolve, such as
    /// a WAL write whose durability is unknown. Writes and compactions are then refused.
    failure: Option<String>,
}

impl DiskWal {
    /// Marks the room as failed; it must be closed and reopened to recover from disk.
    pub(crate) fn mark_failed(&mut self, reason: String) {
        tracing::error!(room_id = %self.room_id, reason = %reason, "Room marked as failed");
        self.failure = Some(reason);
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
/// File operations run on the blocking pool and complete even if the future awaiting them is
/// dropped, so a change abandoned halfway (an error the change does not handle, a panic, or
/// the caller dropping the future) can leave the WAL handle, the files and the in-memory state
/// disagreeing, and a later write would land in the wrong file, at the wrong offset, or be
/// validated against a stale head. The room is therefore marked as failed when the guard drops
/// without `complete`.
pub(crate) struct PendingWrite<'a> {
    wal: &'a mut DiskWal,
    completed: bool,
}

impl<'a> PendingWrite<'a> {
    pub(crate) fn begin(wal: &'a mut DiskWal) -> Self {
        Self {
            wal,
            completed: false,
        }
    }

    pub(crate) fn wal(&mut self) -> &mut DiskWal {
        self.wal
    }

    /// Marks the room as failed with a specific reason.
    pub(crate) fn fail(self, reason: String) {
        self.wal.mark_failed(reason);
    }

    /// The change reached a consistent end: it is complete, or it failed in a way it handled
    /// itself (undoing it or marking the room as failed).
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for PendingWrite<'_> {
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
    pub(crate) tables: HashMap<u16, Table>,
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
pub(crate) struct DiskRoom {
    pub(crate) room_id: RoomId,
    pub(crate) snap_path: PathBuf,
    pub(crate) wal_path: PathBuf,
    pub(crate) compaction: Arc<Mutex<()>>,
    pub(crate) wal: Mutex<DiskWal>,
    pub(crate) state: RwLock<DiskRoomState>,
    /// Set while a compaction runs. Atomic so that a guard can release it on drop without
    /// awaiting a lock.
    pub(crate) is_compacting: Arc<AtomicBool>,
}

/// High-performance, crash-resilient disk storage engine for ZemDB.
///
/// Implements a Dual-File architecture per room:
/// - `room_{id}.snap`: Base consolidated snapshot with 64-byte `ZEM1` header,
///   compressed with Zstandard and checksummed.
/// - `room_{id}.wal`: Append-only Write-Ahead Log (WAL) of delta batches enqueued
///   with framing `0xBA7C` and per-batch CRC32.
/// - True CoW compaction: Background snapshot generation replaces `room_{id}.snap`
///   atomically without blocking incoming WAL writes, rotating to `wal.compacting`.
/// - Fast in-memory tables (`imbl::OrdMap`, see [`Table`]) whose O(1) clones isolate scans and
///   compactions from concurrent writes.
/// - WAL appends and syncs under a per-room WAL mutex, separate from the lock readers take
///   (see [`DiskRoom`]).
/// - Non-blocking batched cursor scans (64 items per yield) reading frozen table snapshots.
#[derive(Debug, Clone)]
pub struct DiskStorageEngine {
    options: DiskStorageOptions,
    rooms: Arc<RwLock<HashMap<RoomId, Arc<DiskRoom>>>>,
}

impl DiskStorageEngine {
    /// Creates a new `DiskStorageEngine` with the given configuration options.
    pub fn new(options: DiskStorageOptions) -> Self {
        Self {
            options,
            rooms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Computes the filesystem path for a room's base snapshot file (`room_{id}.snap`).
    pub fn snap_file_path(&self, room_id: &RoomId) -> PathBuf {
        self.options.data_dir.join(format!("room_{}.snap", room_id))
    }

    /// Computes the filesystem path for a room's append-only WAL file (`room_{id}.wal`).
    pub fn wal_file_path(&self, room_id: &RoomId) -> PathBuf {
        self.options.data_dir.join(format!("room_{}.wal", room_id))
    }

    /// Compatibility helper returning the room's WAL file path.
    pub fn room_file_path(&self, room_id: &RoomId) -> PathBuf {
        self.wal_file_path(room_id)
    }

    /// Fast lookup of an open room handle.
    pub(crate) async fn get_room(&self, room_id: &RoomId) -> Result<Arc<DiskRoom>, StorageError> {
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
    ) -> Result<(Arc<DiskRoom>, OwnedMutexGuard<()>), StorageError> {
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

struct DiskScanState {
    table_data: Table,
    range: KeyRange,
    direction: ScanDirection,
    projection: Option<Vec<u16>>,
    remaining_limit: Option<usize>,
    cursor: Option<PrimaryKey>,
    exhausted: bool,
    buffer: VecDeque<Result<(PrimaryKey, CompactRow), StorageError>>,
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StorageEngine for DiskStorageEngine {
    #[tracing::instrument(skip(self, schema), fields(room_id = %room_id))]
    async fn open_room(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError> {
        let mut rooms = self.rooms.write().await;
        if rooms.contains_key(room_id) {
            return Err(StorageError::RoomAlreadyOpen(room_id.clone()));
        }

        create_dir_all(&self.options.data_dir).await?;
        let snap_path = self.snap_file_path(room_id);
        let wal_path = self.wal_file_path(room_id);

        let std_wal_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&wal_path)?;

        std_wal_file
            .try_lock_exclusive()
            .map_err(|_| StorageError::RoomLocked(room_id.clone()))?;

        let recovered = recover_room(
            room_id,
            &snap_path,
            &wal_path,
            &schema,
            std_wal_file,
            self.options.zstd_level,
        )
        .await?;

        let room = DiskRoom {
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
                tables: recovered.tables,
            }),
            is_compacting: Arc::new(AtomicBool::new(false)),
            compaction: Arc::new(Mutex::new(())),
        };

        rooms.insert(room_id.clone(), Arc::new(room));

        tracing::info!(room_id = %room_id, "Opened disk room (dual-file .snap + .wal)");
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    async fn close_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let lock = self.compaction_lock(room_id).await?;
        // Waits for a compaction or snapshot in progress to finish.
        let (room, _compaction_guard) = self.lock_compaction(room_id, lock).await?;

        // Still registered: closing needs the compaction lock, which this call holds.
        self.rooms.write().await.remove(room_id);
        // Waits for a write in progress to finish.
        let wal = room.wal.lock().await;
        wal.file.sync_all().await?;
        tracing::info!(room_id = %room_id, "Closed disk room");
        Ok(())
    }

    #[tracing::instrument(skip(self, ops), fields(room_id = %room_id, ops_count = ops.len()))]
    async fn apply_batch(
        &self,
        room_id: &RoomId,
        ops: Vec<SequencedOperation>,
    ) -> Result<SequenceNumber, StorageError> {
        let room = self.get_room(room_id).await?;
        // Held until the batch is applied in memory, so batches reach memory in WAL order and
        // the head read below is that of the last batch in the WAL.
        let mut wal = room.wal.lock().await;
        wal.ensure_usable()?;

        // 1. Validate operations against room schema and strict monotonic sequence
        {
            let state = room.state.read().await;
            // An empty batch changes nothing; writing it would only add an empty frame to the WAL.
            if ops.is_empty() {
                return Ok(state.head_seq);
            }
            validate_batch(room_id, &state.schema, state.head_seq, &ops)?;
        }

        // 2. Encode WAL records into framed byte buffer
        let wal_batch_bytes = WalWriter::encode_batch(&ops)?;

        // 3. Write to append-only WAL file and fsync data, without holding the state lock, so
        // that readers do not wait for the sync. A failure here leaves an unknown amount of the
        // batch on disk, and a failed fsync cannot be retried safely, so the room stops
        // accepting writes until recovery replays the WAL from disk.
        let mut pending = PendingWrite::begin(&mut wal);
        let write_result = async {
            let wal = pending.wal();
            wal.file.write_all(&wal_batch_bytes).await?;
            crate::fail_point::check("apply_batch.sync", &room.wal_path)?;
            crate::fail_point::pause("apply_batch.sync", &room.wal_path).await;
            wal.file.sync_data().await?;
            Ok::<(), StorageError>(())
        }
        .await;
        if let Err(err) = write_result {
            pending.fail(format!("WAL append failed: {err}"));
            return Err(err);
        }
        pending.wal().len += wal_batch_bytes.len() as u64;

        // 4. Apply operations to O(1) clones of the in-memory tables, then publish them at once:
        // readers see all of the batch or none of it, even if applying it panics. Nothing else
        // changes the tables meanwhile, since that needs the WAL mutex.
        let (tables, head_seq) = {
            let state = room.state.read().await;
            let mut tables = state.tables.clone();
            let mut head_seq = state.head_seq;
            for SequencedOperation { seq, op } in ops {
                apply_operation(&mut tables, &state.schema, op);
                crate::fail_point::panic_point("apply_batch.apply", &room.wal_path);
                head_seq = head_seq.max(seq);
            }
            (tables, head_seq)
        };
        let snapshot_len = {
            let mut state = room.state.write().await;
            state.tables = tables;
            state.head_seq = head_seq;
            state.snapshot_len
        };
        pending.complete();

        // 5. Check if compaction threshold is triggered
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
        Ok(head_seq)
    }

    async fn get(
        &self,
        room_id: &RoomId,
        table: &str,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        let room = self.get_room(room_id).await?;
        let table_id = {
            let state = room.state.read().await;
            state
                .schema
                .get_table_id(table)
                .ok_or_else(|| StorageError::TableNotFound {
                    room_id: room_id.clone(),
                    table: table.to_string(),
                })?
        };

        self.get_by_id(room_id, table_id, pk).await
    }

    async fn get_by_id(
        &self,
        room_id: &RoomId,
        table_id: u16,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        let room = self.get_room(room_id).await?;
        let state = room.state.read().await;

        if !state.schema.has_table_by_id(table_id) {
            return Err(StorageError::TableNotFound {
                room_id: room_id.clone(),
                table: format!("id:{}", table_id),
            });
        }

        let row = state.tables.get(&table_id).and_then(|t| t.get(pk).cloned());
        Ok(row)
    }

    async fn scan<'a>(
        &'a self,
        room_id: &RoomId,
        table: &str,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        let room = self.get_room(room_id).await?;
        let table_id = {
            let state = room.state.read().await;
            state
                .schema
                .get_table_id(table)
                .ok_or_else(|| StorageError::TableNotFound {
                    room_id: room_id.clone(),
                    table: table.to_string(),
                })?
        };

        self.scan_by_id(room_id, table_id, options).await
    }

    async fn scan_by_id<'a>(
        &'a self,
        room_id: &RoomId,
        table_id: u16,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        let room = self.get_room(room_id).await?;
        let table_data = {
            let state = room.state.read().await;
            if !state.schema.has_table_by_id(table_id) {
                return Err(StorageError::TableNotFound {
                    room_id: room_id.clone(),
                    table: format!("id:{}", table_id),
                });
            }
            // An O(1) clone that shares the table's nodes; writes during the scan copy only the
            // nodes they change.
            state.tables.get(&table_id).cloned().unwrap_or_default()
        };

        let state = DiskScanState {
            table_data,
            range: options.range,
            direction: options.direction,
            projection: options.projection,
            remaining_limit: options.limit,
            cursor: None,
            exhausted: false,
            buffer: VecDeque::new(),
        };

        let stream = futures::stream::unfold(state, |mut state| async move {
            if let Some(item) = state.buffer.pop_front() {
                return Some((item, state));
            }

            if state.exhausted || state.remaining_limit == Some(0) {
                return None;
            }

            const BATCH_SIZE: usize = 64;
            let batch_limit = match state.remaining_limit {
                Some(limit) => limit.min(BATCH_SIZE),
                None => BATCH_SIZE,
            };

            let batch_items: Vec<Result<(PrimaryKey, CompactRow), StorageError>> = match state
                .direction
            {
                ScanDirection::Forward => {
                    let (start_bound, end_bound) = match &state.cursor {
                        Some(cur) => (std::ops::Bound::Excluded(cur), state.range.end_bound()),
                        None => (state.range.start_bound(), state.range.end_bound()),
                    };
                    let iter = state
                        .table_data
                        .range::<_, PrimaryKey>((start_bound, end_bound));
                    apply_scan_transforms(iter, state.projection.clone(), Some(batch_limit))
                        .collect()
                }
                ScanDirection::Backward => {
                    let (start_bound, end_bound) = match &state.cursor {
                        Some(cur) => (state.range.start_bound(), std::ops::Bound::Excluded(cur)),
                        None => (state.range.start_bound(), state.range.end_bound()),
                    };
                    let iter = state
                        .table_data
                        .range::<_, PrimaryKey>((start_bound, end_bound))
                        .rev();
                    apply_scan_transforms(iter, state.projection.clone(), Some(batch_limit))
                        .collect()
                }
            };

            let count = batch_items.len();
            if count == 0 {
                return None;
            }

            if count < batch_limit {
                state.exhausted = true;
            }

            if let Some(Ok((last_pk, _))) = batch_items.last() {
                state.cursor = Some(last_pk.clone());
            }

            if let Some(rem) = state.remaining_limit.as_mut() {
                *rem = rem.saturating_sub(count);
                if *rem == 0 {
                    state.exhausted = true;
                }
            }

            state.buffer.extend(batch_items);
            state.buffer.pop_front().map(|item| (item, state))
        });

        Ok(Box::pin(stream))
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
            (state.head_seq, state.tables.clone())
        };

        let zstd_level = self.options.zstd_level;
        tokio::task::spawn_blocking(move || {
            let raw = bincode::serialize(&RoomSnapshotRef {
                head_seq,
                tables: &tables,
            })
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
            crate::snapshot::encode_snapshot_envelope(&raw, true, zstd_level)
        })
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
    }

    /// Replaces the room's contents with `snapshot` (see the trait for the rules on its head
    /// sequence).
    ///
    /// The snapshot is serialized, compressed and written to a temporary file under the room's
    /// compaction lock only, so writers keep going meanwhile; the head sequence is checked
    /// again under the WAL mutex before the file is installed. The snapshot is made durable
    /// before memory changes, so readers keep seeing the previous state until the new one is on
    /// disk. See [`install_applied_snapshot`] for the crash windows of replacing the files.
    #[tracing::instrument(skip(self, schema, snapshot), fields(room_id = %room_id, snapshot_size = snapshot.len()))]
    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: &[u8],
    ) -> Result<SequenceNumber, StorageError> {
        // Checked before decoding, so that both engines report an unopened room the same way.
        self.get_room(room_id).await?;

        let snapshot_vec = snapshot.to_vec();
        let max_uncompressed = self.options.max_snapshot_uncompressed_bytes;
        let decompressed = tokio::task::spawn_blocking(move || {
            crate::snapshot::decode_snapshot_envelope_with_limit(&snapshot_vec, max_uncompressed)
        })
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
        .map_err(|e| StorageError::SnapshotCorruption(format!("Snapshot decode failed: {e}")))?;

        let mut payload: RoomSnapshotPayload = bincode::deserialize(&decompressed)
            .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;
        payload.fill_missing_tables(&schema);

        let lock = self.compaction_lock(room_id).await?;
        crate::fail_point::pause("apply_snapshot.lock", &self.wal_file_path(room_id)).await;
        let (room, _compaction_guard) = self.lock_compaction(room_id, lock).await?;

        // A first check spares staging a snapshot that cannot be applied. Writers may still
        // advance the head until the WAL mutex is taken.
        let current = room.state.read().await.head_seq;
        if let Some(outcome) = snapshot_head_outcome(current, payload.head_seq) {
            return outcome;
        }

        // Nothing visible changes until the staged snapshot is renamed into place.
        let staged = stage_snapshot(
            &room.snap_path,
            payload.head_seq,
            &payload.tables,
            self.options.zstd_level,
            room_id,
        )
        .await?;
        crate::fail_point::pause("apply_snapshot.staged", &room.wal_path).await;

        let mut wal = room.wal.lock().await;
        let current = room.state.read().await.head_seq;
        let checked = wal
            .ensure_usable()
            .map(|()| snapshot_head_outcome(current, payload.head_seq));
        match checked {
            Ok(None) => {}
            Ok(Some(outcome)) => {
                staged.discard().await;
                return outcome;
            }
            Err(err) => {
                staged.discard().await;
                return Err(err);
            }
        }

        // Armed before the rename, which completes on the blocking pool even if this future is
        // dropped: an installed snapshot whose WAL was not emptied would have later batches
        // replayed on top of it. A failed rename changes nothing, so it leaves the room usable.
        let mut pending = PendingWrite::begin(&mut wal);
        if let Err(err) = rename_staged_snapshot(&room, &staged).await {
            pending.complete();
            staged.discard().await;
            return Err(err);
        }
        if let Err(err) = install_applied_snapshot(&room, pending.wal()).await {
            pending.fail(format!("Persisting an applied snapshot failed: {err}"));
            return Err(err);
        }

        {
            let mut state = room.state.write().await;
            state.schema = schema;
            state.head_seq = payload.head_seq;
            state.tables = payload.tables;
            state.snapshot_seq = payload.head_seq;
            state.snapshot_len = staged.compressed_len;
        }
        pending.complete();

        tracing::info!(room_id = %room_id, head_seq = %payload.head_seq, "Applied snapshot to disk room");
        Ok(payload.head_seq)
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
