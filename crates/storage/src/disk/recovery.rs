use std::collections::{BTreeMap, HashMap};
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};

use rimdb_core::{
    CompactRow, OperationKind, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation,
    Value, BATCH_HEADER_SIZE, BATCH_MAGIC, MAX_MESSAGE_SIZE,
};

pub use crate::disk::format::replay_wal_records;
use crate::disk::format::{FileHeader, HEADER_SIZE};
use crate::error::StorageError;
use crate::memory::RoomSnapshotPayload;
use crate::sys::sync_dir;

/// Recovery result containing the reconstructed in-memory state and the open WAL file handle.
#[derive(Debug)]
pub struct RecoveredRoom {
    pub wal_file: tokio::fs::File,
    pub head_seq: SequenceNumber,
    pub snapshot_seq: SequenceNumber,
    pub snapshot_len: u64,
    pub wal_len: u64,
    pub tables: HashMap<u16, BTreeMap<PrimaryKey, CompactRow>>,
}

/// Replays a room from disk following the Dual-File architecture:
/// 1. Reads the immutable base snapshot from `snap_path` (`room_{id}.snap`),
///    reconstructing base tables and base `head_seq`.
/// 2. Streams and replays all framed WAL batches from `wal_path` (`room_{id}.wal`) using a 64 KB `BufReader`,
///    applying mutations on top of tables and advancing `head_seq`.
/// 3. Detects and truncates any incomplete torn write at WAL EOF in-place.
#[tracing::instrument(skip(schema, wal_file_std), fields(room_id = %room_id, snap_path = ?snap_path, wal_path = ?wal_path))]
pub async fn recover_room(
    room_id: &RoomId,
    snap_path: &Path,
    wal_path: &Path,
    schema: &Schema,
    wal_file_std: std::fs::File,
) -> Result<RecoveredRoom, StorageError> {
    let mut tables = HashMap::new();
    for table_id in schema.tables_by_id.keys() {
        tables.insert(*table_id, BTreeMap::new());
    }

    let mut snapshot_seq = SequenceNumber::from(0u64);
    let mut head_seq = SequenceNumber::from(0u64);
    let mut snapshot_len = 0u64;

    // 1. Recover base snapshot from snap_path if it exists
    if snap_path.exists() {
        let snap_std = std::fs::OpenOptions::new().read(true).open(snap_path)?;
        let snap_file = tokio::fs::File::from_std(snap_std);
        let snap_meta = snap_file.metadata().await?;
        let snap_file_len = snap_meta.len();

        if snap_file_len < HEADER_SIZE as u64 {
            return Err(StorageError::WalCorruption(format!(
                "Snapshot file size {snap_file_len} is smaller than minimum header size {HEADER_SIZE}"
            )));
        }

        let mut snap_reader = BufReader::with_capacity(64 * 1024, snap_file);
        let mut header_bytes = [0u8; HEADER_SIZE];
        snap_reader.read_exact(&mut header_bytes).await.map_err(|e| {
            StorageError::WalCorruption(format!("Failed to read snapshot file header: {e}"))
        })?;

        let header = FileHeader::decode(&header_bytes)?;
        snapshot_len = header.snapshot_compressed_len;
        snapshot_seq = SequenceNumber::from(header.snapshot_seq);
        head_seq = SequenceNumber::from(header.head_seq);

        if snapshot_len > 0 {
            let expected_total = HEADER_SIZE as u64 + snapshot_len;
            if snap_file_len < expected_total {
                return Err(StorageError::SnapshotCorruption(format!(
                    "Snapshot file truncated: expected length {expected_total}, actual {snap_file_len}"
                )));
            }

            let mut compressed_snap = vec![0u8; snapshot_len as usize];
            snap_reader.read_exact(&mut compressed_snap).await.map_err(|e| {
                StorageError::SnapshotCorruption(format!("Failed to read snapshot bytes: {e}"))
            })?;

            let decompressed = tokio::task::spawn_blocking(move || {
                zstd::decode_all(&compressed_snap[..])
            })
            .await
            .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
            .map_err(|e| StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}")))?;

            let mut payload: RoomSnapshotPayload = bincode::deserialize(&decompressed)
                .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;

            snapshot_seq = payload.head_seq;
            head_seq = payload.head_seq;

            for table_id in schema.tables_by_id.keys() {
                payload.tables.entry(*table_id).or_default();
            }
            tables = payload.tables;
        }
    } else {
        // Create initial empty snapshot
        let header = FileHeader::new(0, 0, 0);
        let mut snap_file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(snap_path)
            .await?;
        snap_file.write_all(&header.encode()).await?;
        snap_file.sync_all().await?;
        if let Some(parent) = snap_path.parent() {
            sync_dir(parent)?;
        }
    }

    // 2. Replay append-only WAL batches from wal_path
    let mut wal_file = tokio::fs::File::from_std(wal_file_std);
    let wal_meta = wal_file.metadata().await?;
    let wal_file_len = wal_meta.len();

    let mut valid_wal_bytes: usize = 0;
    let mut torn_write: Option<String> = None;

    if wal_file_len > 0 {
        wal_file.seek(SeekFrom::Start(0)).await?;
        let mut reader = BufReader::with_capacity(64 * 1024, &mut wal_file);

        loop {
            let mut header_buf = [0u8; BATCH_HEADER_SIZE];
            let mut header_read = 0;
            while header_read < BATCH_HEADER_SIZE {
                let n = reader.read(&mut header_buf[header_read..]).await?;
                if n == 0 {
                    break;
                }
                header_read += n;
            }

            if header_read == 0 {
                // Clean EOF
                break;
            }

            if header_read < BATCH_HEADER_SIZE {
                torn_write = Some(format!(
                    "Incomplete WAL batch header: available {header_read} bytes, expected at least {BATCH_HEADER_SIZE}"
                ));
                break;
            }

            let magic = [header_buf[0], header_buf[1]];
            if magic != BATCH_MAGIC {
                // Check if unparsed tail is completely zero-filled (e.g. from power cut or crash on thin-provisioned/pre-allocated storage)
                if header_buf.iter().all(|&b| b == 0) {
                    let mut rest = Vec::new();
                    reader.read_to_end(&mut rest).await?;
                    if rest.iter().all(|&b| b == 0) {
                        torn_write = Some(format!(
                            "Zero-filled tail detected at EOF (length: {} bytes)",
                            header_read + rest.len()
                        ));
                        break;
                    }
                }

                return Err(StorageError::WalCorruption(format!(
                    "Invalid WAL batch magic: expected {:?}, got {:?}",
                    BATCH_MAGIC, magic
                )));
            }

            let batch_len = u32::from_le_bytes(header_buf[2..6].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(header_buf[6..10].try_into().unwrap());
            let ops_count = u32::from_le_bytes(header_buf[10..14].try_into().unwrap()) as usize;

            if batch_len as u64 > MAX_MESSAGE_SIZE {
                return Err(StorageError::WalCorruption(format!(
                    "WAL batch length {batch_len} exceeds MAX_MESSAGE_SIZE {MAX_MESSAGE_SIZE}"
                )));
            }

            let total_expected_batch_len = match BATCH_HEADER_SIZE.checked_add(batch_len) {
                Some(len) => len,
                None => {
                    return Err(StorageError::WalCorruption(
                        "WAL batch length caused integer overflow".to_string(),
                    ));
                }
            };

            let mut payload = vec![0u8; batch_len];
            let mut payload_read = 0;
            while payload_read < batch_len {
                let n = reader.read(&mut payload[payload_read..]).await?;
                if n == 0 {
                    break;
                }
                payload_read += n;
            }

            if payload_read < batch_len {
                torn_write = Some(format!(
                    "Truncated WAL batch payload: available {payload_read} bytes, expected {batch_len}"
                ));
                break;
            }

            let actual_crc = crc32fast::hash(&payload);
            if actual_crc != expected_crc {
                // If there are no subsequent valid batch headers remaining, treat CRC mismatch as a torn write at EOF
                let mut peek_buf = [0u8; BATCH_HEADER_SIZE];
                let peek_bytes = reader.read(&mut peek_buf).await?;
                let has_subsequent_valid_batch = if peek_bytes >= 2 {
                    peek_buf[0..2] == BATCH_MAGIC
                } else {
                    false
                };

                if !has_subsequent_valid_batch {
                    torn_write = Some(format!(
                        "WAL batch CRC32 mismatch at EOF: expected {expected_crc}, actual {actual_crc}"
                    ));
                    break;
                }

                return Err(StorageError::WalCorruption(format!(
                    "WAL batch CRC32 mismatch: expected {expected_crc}, actual {actual_crc}"
                )));
            }

            let ops = match bincode::deserialize::<rimdb_core::protocol::wal_frame::WalBatchPayload>(&payload) {
                Ok(batch) => batch.ops,
                Err(_) => bincode::deserialize::<Vec<SequencedOperation>>(&payload).map_err(|e| {
                    StorageError::WalCorruption(format!("Failed to deserialize WAL batch operations: {e}"))
                })?,
            };

            if ops.len() != ops_count {
                return Err(StorageError::WalCorruption(format!(
                    "WAL batch ops count mismatch: expected {ops_count}, got {}",
                    ops.len()
                )));
            }

            for op in ops {
                // Skip deltas already consolidated in the base snapshot
                if op.seq <= snapshot_seq {
                    continue;
                }

                if schema.has_table_by_id(op.op.table_id) {
                    let table_map = tables.entry(op.op.table_id).or_default();

                    match op.op.kind {
                        OperationKind::Insert { row } => {
                            table_map.insert(op.op.pk, row);
                        }
                        OperationKind::Update { updates } => {
                            if let Some(existing) = table_map.get_mut(&op.op.pk) {
                                let target_len = schema
                                    .get_table_by_id(op.op.table_id)
                                    .map(|t| t.columns.len())
                                    .unwrap_or(0);
                                for col_up in updates {
                                    let idx = col_up.column_idx as usize;
                                    let min_len = target_len.max(idx + 1);
                                    if existing.values.len() < min_len {
                                        existing.values.resize(min_len, Value::Null);
                                    }
                                    existing.values[idx] = col_up.value;
                                }
                            }
                        }
                        OperationKind::Delete => {
                            table_map.remove(&op.op.pk);
                        }
                    }
                }

                if op.seq > head_seq {
                    head_seq = op.seq;
                }
            }

            valid_wal_bytes += total_expected_batch_len;
        }
    }

    // 3. Truncate torn write at WAL EOF if detected
    if let Some(ref reason) = torn_write {
        tracing::warn!(
            room_id = %room_id,
            reason = %reason,
            "Torn write detected at WAL EOF, truncating damaged bytes to {valid_wal_bytes}"
        );
        wal_file.set_len(valid_wal_bytes as u64).await?;
        wal_file.sync_all().await?;
    }

    wal_file.seek(SeekFrom::End(0)).await?;

    tracing::info!(
        room_id = %room_id,
        head_seq = %head_seq,
        wal_len = valid_wal_bytes,
        "Recovered dual-file room from disk"
    );

    Ok(RecoveredRoom {
        wal_file,
        head_seq,
        snapshot_seq,
        snapshot_len,
        wal_len: valid_wal_bytes as u64,
        tables,
    })
}
