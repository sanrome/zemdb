use fs2::FileExt;
use rimdb_core::{CompactRow, PrimaryKey, RoomId, SequenceNumber};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::SeekFrom;
use tokio::fs::{rename, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::disk::format::FileHeader;
use crate::disk::{DiskRoomState, DiskStorageOptions};
use crate::error::StorageError;
use crate::sys::sync_dir;

/// In-memory snapshot payload serialized with bincode and compressed with zstd.
#[derive(Debug, Serialize, Deserialize)]
pub struct RoomSnapshotPayload {
    pub head_seq: SequenceNumber,
    pub tables: HashMap<u16, Vec<(PrimaryKey, CompactRow)>>,
}

/// Internal compaction implementation:
/// Serializes current in-memory tables, compresses with Zstandard, writes to a temporary file,
/// syncs to disk, atomically renames over the original file, invokes `sync_dir`, and reopens in append mode.
#[tracing::instrument(skip(room, options), fields(file_path = ?room.file_path, head_seq = %room.head_seq, wal_len = room.wal_len))]
pub async fn compact_room_internal(
    room: &mut DiskRoomState,
    options: &DiskStorageOptions,
) -> Result<(), StorageError> {
    let payload: RoomSnapshotPayload = RoomSnapshotPayload {
        head_seq: room.head_seq,
        tables: room
            .tables
            .iter()
            .map(|(&id, data)| {
                (
                    id,
                    data.iter().map(|(pk, r)| (pk.clone(), r.clone())).collect(),
                )
            })
            .collect(),
    };

    let serialized = bincode::serialize(&payload)
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    let zstd_level = options.zstd_level;
    let compressed = tokio::task::spawn_blocking(move || {
        zstd::encode_all(&serialized[..], zstd_level)
    })
    .await
    .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
    .map_err(|e| StorageError::Other(format!("Zstd compression failed: {e}")))?;

    let tmp_path = room.file_path.with_extension("rimdb.tmp");

    // Write temporary file with new header and compressed snapshot block
    let new_header = FileHeader::new(*room.head_seq, *room.head_seq, compressed.len() as u64);
    let mut tmp_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .await?;

    tmp_file.write_all(&new_header.encode()).await?;
    tmp_file.write_all(&compressed).await?;
    tmp_file.sync_all().await?;
    drop(tmp_file);

    // Atomically replace target file
    rename(&tmp_path, &room.file_path).await?;

    // POSIX directory entry synchronization
    if let Some(parent) = room.file_path.parent() {
        sync_dir(parent)?;
    }

    // Reopen target file in read/write/append mode and acquire OS file lock
    let std_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&room.file_path)?;
    std_file
        .try_lock_exclusive()
        .map_err(|_| StorageError::RoomLocked(RoomId::new(
            room.file_path.file_stem().and_then(|s| s.to_str()).unwrap_or("unknown")
        )))?;
    let mut new_file = File::from_std(std_file);
    new_file.seek(SeekFrom::End(0)).await?;

    room.file = new_file;
    room.snapshot_seq = room.head_seq;
    room.snapshot_len = compressed.len() as u64;
    room.wal_len = 0;

    tracing::info!(
        file_path = ?room.file_path,
        snapshot_len = compressed.len(),
        head_seq = %room.head_seq,
        "Compacted room snapshot successfully"
    );

    Ok(())
}
