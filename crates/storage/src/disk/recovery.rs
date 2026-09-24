use std::collections::{BTreeMap, HashMap};
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};

use fs2::FileExt;

use rimdb_core::{
    CompactRow, OperationKind, PrimaryKey, RoomId, Schema, SequenceNumber, SequencedOperation,
    MAX_MESSAGE_SIZE,
};

pub use crate::disk::format::replay_wal_records;
use crate::disk::compactor::RoomSnapshotPayload;
use crate::disk::format::{FileHeader, BATCH_HEADER_SIZE, BATCH_MAGIC, HEADER_SIZE};
use crate::error::StorageError;

/// Recovery result containing the reconstructed in-memory state and file handle.
#[derive(Debug)]
pub struct RecoveredRoom {
    pub file: tokio::fs::File,
    pub head_seq: SequenceNumber,
    pub snapshot_seq: SequenceNumber,
    pub snapshot_len: u64,
    pub wal_len: u64,
    pub tables: HashMap<u16, BTreeMap<PrimaryKey, CompactRow>>,
}

/// Replays a room file from disk using a streaming 64 KB BufReader,
/// acquiring an exclusive OS file lock, unpacking base snapshot with spawn_blocking,
/// replaying framed WAL batches, and recovering from torn writes by truncating the file if necessary.
#[tracing::instrument(skip(schema), fields(room_id = %room_id, file_path = ?file_path))]
pub async fn recover_room(
    room_id: &RoomId,
    file_path: &Path,
    schema: &Schema,
) -> Result<RecoveredRoom, StorageError> {
    let std_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)?;

    std_file
        .try_lock_exclusive()
        .map_err(|_| StorageError::RoomLocked(room_id.clone()))?;

    let file = tokio::fs::File::from_std(std_file);

    let file_metadata = file.metadata().await?;
    let file_len = file_metadata.len();

    if file_len < HEADER_SIZE as u64 {
        return Err(StorageError::WalCorruption(format!(
            "File size {file_len} is smaller than minimum header size {HEADER_SIZE}"
        )));
    }

    let mut reader = BufReader::with_capacity(64 * 1024, file);

    let mut header_bytes = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header_bytes).await.map_err(|e| {
        StorageError::WalCorruption(format!("Failed to read file header: {e}"))
    })?;

    let header = FileHeader::decode(&header_bytes)?;

    let snapshot_len = header.snapshot_compressed_len;
    let mut snapshot_seq = SequenceNumber::from(header.snapshot_seq);
    let mut head_seq = SequenceNumber::from(header.head_seq);

    let mut tables = HashMap::new();
    for table_id in schema.tables_by_id.keys() {
        tables.insert(*table_id, BTreeMap::new());
    }

    // Restore base snapshot if present
    if snapshot_len > 0 {
        let expected_min_len = HEADER_SIZE as u64 + snapshot_len;
        if file_len < expected_min_len {
            return Err(StorageError::SnapshotCorruption(format!(
                "File truncated: expected snapshot to end at byte {expected_min_len}, file size is {file_len}"
            )));
        }

        let mut compressed_snap = vec![0u8; snapshot_len as usize];
        reader.read_exact(&mut compressed_snap).await.map_err(|e| {
            StorageError::SnapshotCorruption(format!("Failed to read snapshot bytes: {e}"))
        })?;

        let decompressed = tokio::task::spawn_blocking(move || {
            zstd::decode_all(&compressed_snap[..])
        })
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
        .map_err(|e| StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}")))?;

        let payload: RoomSnapshotPayload = bincode::deserialize(&decompressed)
            .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;

        snapshot_seq = payload.head_seq;
        head_seq = payload.head_seq;

        for (table_id, rows) in payload.tables {
            let mut map = BTreeMap::new();
            for (pk, row) in rows {
                map.insert(pk, row);
            }
            tables.insert(table_id, map);
        }
    }

    // WAL Replay in streaming with framed atomic batches (0xBA7C)
    let mut valid_wal_bytes: usize = 0;
    let mut torn_write: Option<String> = None;

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
            return Err(StorageError::WalCorruption(format!(
                "WAL batch CRC32 mismatch: expected {expected_crc}, actual {actual_crc}"
            )));
        }

        let ops: Vec<SequencedOperation> = bincode::deserialize(&payload).map_err(|e| {
            StorageError::WalCorruption(format!("Failed to deserialize WAL batch operations: {e}"))
        })?;

        if ops.len() != ops_count {
            return Err(StorageError::WalCorruption(format!(
                "WAL batch ops count mismatch: expected {ops_count}, got {}",
                ops.len()
            )));
        }

        for op in ops {
            if schema.has_table_by_id(op.op.table_id) {
                let table_map = tables.entry(op.op.table_id).or_default();

                match op.op.kind {
                    OperationKind::Insert { row } => {
                        table_map.insert(op.op.pk, row);
                    }
                    OperationKind::Update { updates } => {
                        if let Some(existing) = table_map.get_mut(&op.op.pk) {
                            for col_up in updates {
                                let idx = col_up.column_idx as usize;
                                if idx < existing.values.len() {
                                    existing.values[idx] = col_up.value;
                                }
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

    let mut file = reader.into_inner();
    let wal_start = HEADER_SIZE + snapshot_len as usize;

    // If a torn-write was detected at EOF, truncate damaged trailing bytes and update header
    if let Some(ref reason) = torn_write {
        tracing::warn!(
            room_id = %room_id,
            reason = %reason,
            "Torn write detected at WAL EOF, truncating damaged bytes"
        );
        let valid_file_len = (wal_start + valid_wal_bytes) as u64;
        file.set_len(valid_file_len).await?;

        let updated_hdr = FileHeader::new(*snapshot_seq, *head_seq, snapshot_len);
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&updated_hdr.encode()).await?;
        file.sync_all().await?;
    }

    file.seek(SeekFrom::End(0)).await?;

    tracing::info!(
        room_id = %room_id,
        head_seq = %head_seq,
        wal_len = valid_wal_bytes,
        "Recovered room from disk"
    );

    Ok(RecoveredRoom {
        file,
        head_seq,
        snapshot_seq,
        snapshot_len,
        wal_len: valid_wal_bytes as u64,
        tables,
    })
}
