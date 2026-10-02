pub mod compactor;
pub mod format;
pub mod recovery;
pub mod wal;

use async_trait::async_trait;
use fs2::FileExt;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{create_dir_all, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use zemdb_core::{
    CompactRow, OperationKind, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation,
    Value,
};

use crate::disk::compactor::{compact_room_cow, compact_room_internal};
use crate::disk::recovery::recover_room;
use crate::disk::wal::WalWriter;
use crate::engine::{apply_scan_transforms, RowStream, StorageEngine};
use crate::error::StorageError;
use crate::memory::{RoomSnapshotPayload, RoomSnapshotRef};
use crate::options::{KeyRange, ScanDirection, ScanOptions};

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
}

/// Internal state of an open database room on disk in the Dual-File architecture.
#[derive(Debug)]
pub struct DiskRoomState {
    pub schema: Schema,
    pub head_seq: SequenceNumber,
    pub snapshot_seq: SequenceNumber,
    pub tables: HashMap<u16, Arc<BTreeMap<PrimaryKey, CompactRow>>>,
    pub wal_file: File,
    pub snap_path: PathBuf,
    pub wal_path: PathBuf,
    pub snapshot_len: u64,
    pub wal_len: u64,
    pub is_compacting: bool,
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
/// - Fast in-memory index/tables (`BTreeMap`) stored in `Arc` references for zero-lock scan isolation.
/// - Non-blocking batched cursor scans (64 items per yield) reading frozen table snapshots.
#[derive(Debug, Clone)]
pub struct DiskStorageEngine {
    options: DiskStorageOptions,
    rooms: Arc<RwLock<HashMap<RoomId, Arc<RwLock<DiskRoomState>>>>>,
    compaction_locks: Arc<dashmap::DashMap<RoomId, Arc<tokio::sync::Mutex<()>>>>,
}

impl DiskStorageEngine {
    /// Creates a new `DiskStorageEngine` with the given configuration options.
    pub fn new(options: DiskStorageOptions) -> Self {
        Self {
            options,
            rooms: Arc::new(RwLock::new(HashMap::new())),
            compaction_locks: Arc::new(dashmap::DashMap::new()),
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
    pub async fn get_room(
        &self,
        room_id: &RoomId,
    ) -> Result<Arc<RwLock<DiskRoomState>>, StorageError> {
        let rooms = self.rooms.read().await;
        rooms
            .get(room_id)
            .map(Arc::clone)
            .ok_or_else(|| StorageError::RoomNotFound(room_id.clone()))
    }

    fn get_compaction_lock(&self, room_id: &RoomId) -> Arc<tokio::sync::Mutex<()>> {
        self.compaction_locks
            .entry(room_id.clone())
            .or_default()
            .clone()
    }

    /// Explicitly triggers snapshot compaction and WAL truncation for a room.
    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    pub async fn compact_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let room_arc = self.get_room(room_id).await?;
        let compaction_lock = self.get_compaction_lock(room_id);
        let _guard = compaction_lock.lock().await;
        compact_room_cow(room_arc, &self.options).await
    }
}

struct DiskScanState {
    table_data: Arc<BTreeMap<PrimaryKey, CompactRow>>,
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

        let recovered = recover_room(room_id, &snap_path, &wal_path, &schema, std_wal_file).await?;

        let room_state = DiskRoomState {
            schema,
            head_seq: recovered.head_seq,
            snapshot_seq: recovered.snapshot_seq,
            tables: recovered.tables,
            wal_file: recovered.wal_file,
            snap_path,
            wal_path,
            snapshot_len: recovered.snapshot_len,
            wal_len: recovered.wal_len,
            is_compacting: false,
        };

        rooms.insert(room_id.clone(), Arc::new(RwLock::new(room_state)));

        tracing::info!(room_id = %room_id, "Opened disk room (dual-file .snap + .wal)");
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    async fn close_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let compaction_lock = self.get_compaction_lock(room_id);
        let _compaction_guard = compaction_lock.lock().await;

        let mut rooms = self.rooms.write().await;
        if let Some(room_arc) = rooms.remove(room_id) {
            drop(rooms);
            let room = room_arc.write().await;
            room.wal_file.sync_all().await?;
            self.compaction_locks.remove(room_id);
            tracing::info!(room_id = %room_id, "Closed disk room");
            Ok(())
        } else {
            Err(StorageError::RoomNotFound(room_id.clone()))
        }
    }

    #[tracing::instrument(skip(self, ops), fields(room_id = %room_id, ops_count = ops.len()))]
    async fn apply_batch(
        &self,
        room_id: &RoomId,
        ops: Vec<SequencedOperation>,
    ) -> Result<SequenceNumber, StorageError> {
        let room_arc = self.get_room(room_id).await?;
        let mut room = room_arc.write().await;

        // 1. Validate operations against room schema and strict monotonic sequence
        for (expected_seq, op) in (room.head_seq.get() + 1..).zip(ops.iter()) {
            if !room.schema.has_table_by_id(op.op.table_id) {
                return Err(StorageError::TableNotFound {
                    room_id: room_id.clone(),
                    table: format!("id:{}", op.op.table_id),
                });
            }

            if op.seq.get() != expected_seq {
                return Err(StorageError::SequenceMismatch {
                    expected: SequenceNumber::from(expected_seq),
                    actual: op.seq,
                });
            }

            room.schema.validate_operation(&op.op)?;
        }

        // 2. Encode WAL records into framed byte buffer
        let wal_batch_bytes = WalWriter::encode_batch(&ops)?;

        // 3. Write to append-only WAL file and fsync data
        room.wal_file.write_all(&wal_batch_bytes).await?;
        room.wal_file.sync_data().await?;
        room.wal_len += wal_batch_bytes.len() as u64;

        // 4. Apply operations to in-memory tables
        {
            let DiskRoomState {
                ref schema,
                ref mut head_seq,
                ref mut tables,
                ..
            } = *room;

            for SequencedOperation { seq, op } in ops {
                let table_arc = tables.entry(op.table_id).or_default();
                let table_map = Arc::make_mut(table_arc);

                match op.kind {
                    OperationKind::Insert { row } => {
                        table_map.insert(op.pk, row);
                    }
                    OperationKind::Update { updates } => {
                        if let Some(existing) = table_map.get_mut(&op.pk) {
                            let target_len = schema
                                .get_table_by_id(op.table_id)
                                .map(|t| t.columns().len())
                                .unwrap_or(0);
                            for col_up in updates {
                                let idx = col_up.column_idx as usize;
                                let min_len = target_len.max(idx + 1);
                                if existing.len() < min_len {
                                    existing.resize(min_len, Value::Null);
                                }
                                existing[idx] = col_up.value;
                            }
                        }
                    }
                    OperationKind::Delete => {
                        table_map.remove(&op.pk);
                    }
                }

                if seq > *head_seq {
                    *head_seq = seq;
                }
            }
        }

        // 5. Check if compaction threshold is triggered
        if self.options.auto_compact
            && !room.is_compacting
            && room.wal_len >= self.options.min_compaction_bytes
            && room.wal_len >= (room.snapshot_len as f64 * self.options.compaction_ratio) as u64
        {
            let compaction_lock = self.get_compaction_lock(room_id);
            if let Ok(compaction_guard) = compaction_lock.try_lock_owned() {
                let room_arc_clone = Arc::clone(&room_arc);
                let options_clone = self.options.clone();
                tokio::spawn(async move {
                    let _guard = compaction_guard;
                    if let Err(e) = compact_room_cow(room_arc_clone, &options_clone).await {
                        tracing::error!(error = %e, "Background auto-compaction failed");
                    }
                });
            }
        }

        tracing::debug!(room_id = %room_id, head_seq = room.head_seq.get(), "Applied batch to disk room");
        Ok(room.head_seq)
    }

    async fn get(
        &self,
        room_id: &RoomId,
        table: &str,
        pk: &PrimaryKey,
    ) -> Result<Option<CompactRow>, StorageError> {
        let room_arc = self.get_room(room_id).await?;
        let table_id = {
            let room = room_arc.read().await;
            room.schema
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
        let room_arc = self.get_room(room_id).await?;
        let room = room_arc.read().await;

        if !room.schema.has_table_by_id(table_id) {
            return Err(StorageError::TableNotFound {
                room_id: room_id.clone(),
                table: format!("id:{}", table_id),
            });
        }

        let row = room.tables.get(&table_id).and_then(|t| t.get(pk).cloned());
        Ok(row)
    }

    async fn scan<'a>(
        &'a self,
        room_id: &RoomId,
        table: &str,
        options: ScanOptions,
    ) -> Result<RowStream<'a>, StorageError> {
        let room_arc = self.get_room(room_id).await?;
        let table_id = {
            let room = room_arc.read().await;
            room.schema
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
        let room_arc = self.get_room(room_id).await?;
        let table_data = {
            let room = room_arc.read().await;
            if !room.schema.has_table_by_id(table_id) {
                return Err(StorageError::TableNotFound {
                    room_id: room_id.clone(),
                    table: format!("id:{}", table_id),
                });
            }
            room.tables
                .get(&table_id)
                .cloned()
                .unwrap_or_else(|| Arc::new(BTreeMap::new()))
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
                    let iter = state.table_data.range((start_bound, end_bound));
                    apply_scan_transforms(iter, state.projection.clone(), Some(batch_limit))
                        .collect()
                }
                ScanDirection::Backward => {
                    let (start_bound, end_bound) = match &state.cursor {
                        Some(cur) => (state.range.start_bound(), std::ops::Bound::Excluded(cur)),
                        None => (state.range.start_bound(), state.range.end_bound()),
                    };
                    let iter = state.table_data.range((start_bound, end_bound)).rev();
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
        let room_arc = self.get_room(room_id).await?;
        let room = room_arc.read().await;
        Ok(room.head_seq)
    }

    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    async fn create_snapshot(&self, room_id: &RoomId) -> Result<Vec<u8>, StorageError> {
        let room_arc = self.get_room(room_id).await?;
        let room = room_arc.read().await;

        let payload = RoomSnapshotRef {
            head_seq: room.head_seq,
            tables: &room.tables,
        };

        let raw =
            bincode::serialize(&payload).map_err(|e| StorageError::Serialization(e.to_string()))?;

        let zstd_level = self.options.zstd_level;
        tokio::task::spawn_blocking(move || {
            crate::snapshot::encode_snapshot_envelope(&raw, true, zstd_level)
        })
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
    }

    #[tracing::instrument(skip(self, schema, snapshot), fields(room_id = %room_id, snapshot_size = snapshot.len()))]
    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: &[u8],
    ) -> Result<SequenceNumber, StorageError> {
        let snapshot_vec = snapshot.to_vec();
        let decompressed = tokio::task::spawn_blocking(move || {
            crate::snapshot::decode_snapshot_envelope(&snapshot_vec)
        })
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
        .map_err(|e| StorageError::SnapshotCorruption(format!("Snapshot decode failed: {e}")))?;

        let mut payload: RoomSnapshotPayload = bincode::deserialize(&decompressed)
            .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;

        for table_id in schema.tables_by_id.keys() {
            payload.tables.entry(*table_id).or_default();
        }

        let room_arc = self.get_room(room_id).await?;
        let compaction_lock = self.get_compaction_lock(room_id);
        let _guard = compaction_lock.lock().await;

        let mut room = room_arc.write().await;

        room.schema = schema;
        room.head_seq = payload.head_seq;
        room.tables = payload.tables;

        // Perform atomic snapshot rewrite on disk
        compact_room_internal(&mut room, &self.options).await?;

        Ok(room.head_seq)
    }
}
