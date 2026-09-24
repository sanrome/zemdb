use fs2::FileExt;
use rimdb_core::RoomId;
use std::io::SeekFrom;
use tokio::fs::{rename, File};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::disk::format::FileHeader;
use crate::disk::{DiskRoomState, DiskStorageOptions};
use crate::error::StorageError;
use crate::memory::RoomSnapshotRef;
use crate::sys::sync_dir;

/// Internal compaction implementation for the Dual-File architecture:
/// 1. Serializes current in-memory tables directly by reference and compresses with Zstandard.
/// 2. Writes to temporary snapshot file `room_{id}.snap.tmp`.
/// 3. Acquires exclusive kernel `flock` on `tmp_path` before rename to close the race window (Point H3).
/// 4. Atomically renames `room_{id}.snap.tmp` over `room_{id}.snap` and calls `sync_dir`.
/// 5. Truncates `room_{id}.wal` in-place to 0 bytes and seeks to start, leaving writers unaffected.
#[tracing::instrument(skip(room, options), fields(snap_path = ?room.snap_path, head_seq = %room.head_seq, wal_len = room.wal_len))]
pub async fn compact_room_internal(
    room: &mut DiskRoomState,
    options: &DiskStorageOptions,
) -> Result<(), StorageError> {
    let payload = RoomSnapshotRef {
        head_seq: room.head_seq,
        tables: &room.tables,
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

    let tmp_path = room.snap_path.with_extension("snap.tmp");

    // Write temporary snapshot file with header and compressed payload
    let new_header = FileHeader::new(room.head_seq.get(), room.head_seq.get(), compressed.len() as u64);

    // Acquire exclusive flock on tmp_path before rename to eliminate race window (Point H3)
    let std_tmp = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    std_tmp
        .try_lock_exclusive()
        .map_err(|_| StorageError::RoomLocked(RoomId::new(
            room.snap_path.file_stem().and_then(|s| s.to_str()).unwrap_or("unknown")
        )))?;

    let mut tmp_file = File::from_std(std_tmp);
    tmp_file.write_all(&new_header.encode()).await?;
    tmp_file.write_all(&compressed).await?;
    tmp_file.sync_all().await?;

    // Atomically replace snapshot file
    rename(&tmp_path, &room.snap_path).await?;

    // POSIX directory entry synchronization
    if let Some(parent) = room.snap_path.parent() {
        sync_dir(parent)?;
    }

    // In-place truncation of the append-only WAL file (.wal)
    room.wal_file.set_len(0).await?;
    room.wal_file.seek(SeekFrom::Start(0)).await?;
    room.wal_file.sync_all().await?;

    room.snapshot_seq = room.head_seq;
    room.snapshot_len = compressed.len() as u64;
    room.wal_len = 0;

    tracing::info!(
        snap_path = ?room.snap_path,
        snapshot_len = compressed.len(),
        head_seq = %room.head_seq,
        "Compacted dual-file room snapshot and truncated WAL successfully"
    );

    Ok(())
}
