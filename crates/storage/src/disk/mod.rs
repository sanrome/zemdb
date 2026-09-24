pub mod compactor;
pub mod format;
pub mod recovery;
pub mod wal;

use async_trait::async_trait;
use rimdb_core::{
    CompactRow, OperationKind, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation,
};
use fs2::FileExt;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{create_dir_all, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::disk::compactor::{compact_room_internal, RoomSnapshotPayload};
use crate::disk::format::FileHeader;
use crate::disk::recovery::recover_room;
use crate::disk::wal::WalWriter;
use crate::engine::{apply_scan_transforms, RowStream, StorageEngine};
use crate::error::StorageError;
use crate::options::{ScanDirection, ScanOptions};
use crate::sys::sync_dir;

/// Options to configure `DiskStorageEngine`.
#[derive(Debug, Clone)]
pub struct DiskStorageOptions {
    /// Directory where `room_{id}.rimdb` files are stored.
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

/// Internal state of an open database room on disk.
#[derive(Debug)]
pub struct DiskRoomState {
    pub schema: Schema,
    pub head_seq: SequenceNumber,
    pub snapshot_seq: SequenceNumber,
    pub tables: HashMap<u16, BTreeMap<PrimaryKey, CompactRow>>,
    pub file: File,
    pub file_path: PathBuf,
    pub snapshot_len: u64,
    pub wal_len: u64,
}

/// High-performance, crash-resilient disk storage engine for RimDB.
///
/// Implements a file-per-room architecture (`room_{id}.rimdb`) featuring:
/// - Fixed 64-byte binary header with magic bytes `RIM1` and CRC32 verification.
/// - Base consolidated snapshot compressed with Zstandard (`zstd`).
/// - Append-only Write-Ahead Log (WAL) with per-record CRC32 checksums.
/// - Fast in-memory index/tables (`BTreeMap`) reconstructed via startup Replay.
/// - Torn-write detection and recovery at file EOF.
/// - Automatic or on-demand background snapshot compaction and log truncation.
#[derive(Debug, Clone)]
pub struct DiskStorageEngine {
    options: DiskStorageOptions,
    rooms: Arc<RwLock<HashMap<RoomId, Arc<RwLock<DiskRoomState>>>>>,
}

impl DiskStorageEngine {
    /// Creates a new `DiskStorageEngine` with the given configuration options.
    pub fn new(options: DiskStorageOptions) -> Self {
        Self {
            options,
            rooms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Computes the filesystem path for a room file.
    pub fn room_file_path(&self, room_id: &RoomId) -> PathBuf {
        self.options.data_dir.join(format!("room_{}.rimdb", room_id))
    }

    /// Fast lookup of an open room handle.
    pub async fn get_room(&self, room_id: &RoomId) -> Result<Arc<RwLock<DiskRoomState>>, StorageError> {
        let rooms = self.rooms.read().await;
        rooms
            .get(room_id)
            .map(Arc::clone)
            .ok_or_else(|| StorageError::RoomNotFound(room_id.clone()))
    }

    /// Explicitly triggers compaction and WAL truncation for a room.
    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    pub async fn compact_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let room_arc = self.get_room(room_id).await?;
        let mut room = room_arc.write().await;
        compact_room_internal(&mut room, &self.options).await
    }
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
        let file_path = self.room_file_path(room_id);

        let room_state = if Path::new(&file_path).exists() {
            let recovered = recover_room(room_id, &file_path, &schema).await?;

            DiskRoomState {
                schema,
                head_seq: recovered.head_seq,
                snapshot_seq: recovered.snapshot_seq,
                tables: recovered.tables,
                file: recovered.file,
                file_path,
                snapshot_len: recovered.snapshot_len,
                wal_len: recovered.wal_len,
            }
        } else {
            // New room file: initialize with 64B header and OS-level exclusive file lock
            let std_file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&file_path)?;

            std_file
                .try_lock_exclusive()
                .map_err(|_| StorageError::RoomLocked(room_id.clone()))?;

            let mut file = File::from_std(std_file);

            let header = FileHeader::new(0, 0, 0);
            file.write_all(&header.encode()).await?;
            file.sync_all().await?;

            if let Some(parent) = file_path.parent() {
                sync_dir(parent)?;
            }

            let mut tables = HashMap::new();
            for table_id in schema.tables_by_id.keys() {
                tables.insert(*table_id, BTreeMap::new());
            }

            DiskRoomState {
                schema,
                head_seq: SequenceNumber::from(0u64),
                snapshot_seq: SequenceNumber::from(0u64),
                tables,
                file,
                file_path,
                snapshot_len: 0,
                wal_len: 0,
            }
        };

        rooms.insert(room_id.clone(), Arc::new(RwLock::new(room_state)));

        tracing::info!(room_id = %room_id, "Opened disk room");
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(room_id = %room_id))]
    async fn close_room(&self, room_id: &RoomId) -> Result<(), StorageError> {
        let mut rooms = self.rooms.write().await;
        if let Some(room_arc) = rooms.remove(room_id) {
            let room = room_arc.write().await;
            room.file.sync_all().await?;
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
        }

        // 2. Encode WAL records into byte buffer
        let wal_batch_bytes = WalWriter::encode_batch(&ops)?;

        // 3. Write to disk and fsync data
        room.file.write_all(&wal_batch_bytes).await?;
        room.file.sync_data().await?;
        room.wal_len += wal_batch_bytes.len() as u64;

        // 4. Apply operations to in-memory tables
        {
            let DiskRoomState {
                ref mut head_seq,
                ref mut tables,
                ..
            } = *room;

            for SequencedOperation { seq, op } in ops {
                let table_map = tables.entry(op.table_id).or_default();

                match op.kind {
                    OperationKind::Insert { row } => {
                        table_map.insert(op.pk, row);
                    }
                    OperationKind::Update { updates } => {
                        if let Some(existing) = table_map.get_mut(&op.pk) {
                            for col_up in updates {
                                let idx = col_up.column_idx as usize;
                                if idx < existing.values.len() {
                                    existing.values[idx] = col_up.value;
                                }
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
            && room.wal_len >= self.options.min_compaction_bytes
            && room.wal_len >= (room.snapshot_len as f64 * self.options.compaction_ratio) as u64
        {
            compact_room_internal(&mut room, &self.options).await?;
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
        let room = room_arc.read().await;

        let table_id = room.schema.get_table_id(table).ok_or_else(|| StorageError::TableNotFound {
            room_id: room_id.clone(),
            table: table.to_string(),
        })?;

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
            room.schema.get_table_id(table).ok_or_else(|| StorageError::TableNotFound {
                room_id: room_id.clone(),
                table: table.to_string(),
            })?
        };

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            let room = room_arc.read().await;
            let empty = BTreeMap::new();
            let table_data = room.tables.get(&table_id).unwrap_or(&empty);

            let items = match options.direction {
                ScanDirection::Forward => {
                    let iter = table_data.range(options.range);
                    apply_scan_transforms(iter, options.projection, options.limit)
                }
                ScanDirection::Backward => {
                    let iter = table_data.range(options.range).rev();
                    apply_scan_transforms(iter, options.projection, options.limit)
                }
            };

            for item in items {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });

        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
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

        let tables = room
            .tables
            .iter()
            .map(|(&id, data)| {
                (
                    id,
                    data.iter().map(|(pk, r)| (pk.clone(), r.clone())).collect(),
                )
            })
            .collect();

        let payload = RoomSnapshotPayload {
            head_seq: room.head_seq,
            tables,
        };

        let raw = bincode::serialize(&payload)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let zstd_level = self.options.zstd_level;
        tokio::task::spawn_blocking(move || zstd::encode_all(&raw[..], zstd_level))
            .await
            .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
            .map_err(|e| StorageError::Other(format!("Zstd snapshot compression failed: {e}")))
    }

    #[tracing::instrument(skip(self, schema, snapshot), fields(room_id = %room_id, snapshot_size = snapshot.len()))]
    async fn apply_snapshot(
        &self,
        room_id: &RoomId,
        schema: Schema,
        snapshot: &[u8],
    ) -> Result<SequenceNumber, StorageError> {
        let snapshot_vec = snapshot.to_vec();
        let decompressed = tokio::task::spawn_blocking(move || zstd::decode_all(&snapshot_vec[..]))
            .await
            .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
            .map_err(|e| StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}")))?;

        let payload: RoomSnapshotPayload = bincode::deserialize(&decompressed)
            .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;

        let mut tables = HashMap::new();
        for (table_id, rows) in payload.tables {
            let mut map = BTreeMap::new();
            for (pk, row) in rows {
                map.insert(pk, row);
            }
            tables.insert(table_id, map);
        }

        let room_arc = self.get_room(room_id).await?;
        let mut room = room_arc.write().await;

        room.schema = schema;
        room.head_seq = payload.head_seq;
        room.tables = tables;

        // Perform atomic snapshot rewrite on disk
        compact_room_internal(&mut room, &self.options).await?;

        Ok(room.head_seq)
    }
}
