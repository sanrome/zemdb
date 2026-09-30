use fs2::FileExt;
use rimdb_core::{RoomId, SequenceNumber};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{rename, File};
use tokio::io::AsyncSeekExt;
use tokio::sync::RwLock;

use crate::disk::format::FileHeader;
use crate::disk::{DiskRoomState, DiskStorageOptions};
use crate::error::StorageError;
use crate::memory::{RoomSnapshotPayload, RoomSnapshotRef};
use crate::sys::sync_dir;

/// Output of the background snapshot compression worker.
struct CompactionWorkerOutput {
    tmp_path: PathBuf,
    cut_seq: SequenceNumber,
    compressed_len: u64,
}

/// Executes Copy-on-Write compaction using a three-phase protocol:
///
/// 1. Phase 1 (Brief write lock, < 1ms):
///    Flushes current WAL writes, rotates `room_{id}.wal` to `room_{id}.wal.compacting`,
///    opens a fresh `room_{id}.wal` file for incoming concurrent writes, clones immutable table references
///    (`Arc<BTreeMap>`), records the cut sequence number `cut_seq`, and releases the room lock.
///
/// 2. Phase 2 (Unconstrained background I/O via blocking worker):
///    Serializes the captured snapshot payload, compresses with Zstandard, computes CRC32 of the compressed
///    payload, writes to a unique temporary file `room_{id}.snap.tmp.{uuid}` using `create_new(true)`, and fsyncs.
///
/// 3. Phase 3 (Brief write lock, < 1ms):
///    Atomically renames `snap.tmp.{uuid}` over `room_{id}.snap`, removes `room_{id}.wal.compacting`,
///    updates `snapshot_seq` to `cut_seq`, and marks compaction finished.
///    Concurrent writes accumulated in the fresh `room_{id}.wal` remain completely intact.
#[tracing::instrument(skip(room_arc, options))]
pub async fn compact_room_cow(
    room_arc: Arc<RwLock<DiskRoomState>>,
    options: &DiskStorageOptions,
) -> Result<(), StorageError> {
    // Phase 1: Preparation and WAL rotation under exclusive write lock
    let (cut_seq, snapshot_tables, snap_path, _wal_path, zstd_level, room_id) = {
        let mut room = room_arc.write().await;
        if room.is_compacting {
            return Ok(());
        }
        room.is_compacting = true;

        // Flush pending WAL writes
        room.wal_file.sync_all().await?;

        let wal_compacting_path = room.wal_path.with_extension("wal.compacting");

        // Rotate room.wal to room.wal.compacting
        tokio::fs::rename(&room.wal_path, &wal_compacting_path).await?;

        // Open fresh room.wal for concurrent writes
        let std_new_wal = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&room.wal_path)?;
        let room_id = RoomId::new(
            room.snap_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown"),
        );
        std_new_wal
            .try_lock_exclusive()
            .map_err(|_| StorageError::RoomLocked(room_id.clone()))?;
        room.wal_file = File::from_std(std_new_wal);
        room.wal_len = 0;

        (
            room.head_seq,
            room.tables.clone(),
            room.snap_path.clone(),
            room.wal_path.clone(),
            options.zstd_level,
            room_id,
        )
    }; // Lock released immediately

    // Phase 2: Heavy serialization, compression and disk I/O in blocking worker thread
    let worker_res =
        tokio::task::spawn_blocking(move || -> Result<CompactionWorkerOutput, StorageError> {
            let payload = RoomSnapshotPayload {
                head_seq: cut_seq,
                tables: snapshot_tables,
            };

            let serialized = bincode::serialize(&payload)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let compressed = zstd::encode_all(&serialized[..], zstd_level)
                .map_err(|e| StorageError::Other(format!("Zstd compression failed: {e}")))?;

            let payload_crc32 = crc32fast::hash(&compressed);
            let uuid_str = uuid::Uuid::new_v4().to_string();
            let tmp_path = snap_path.with_extension(format!("snap.tmp.{}", uuid_str));

            let header = FileHeader::new(
                cut_seq.get(),
                cut_seq.get(),
                compressed.len() as u64,
                payload_crc32,
            );

            let std_tmp = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path)?;
            std_tmp
                .try_lock_exclusive()
                .map_err(|_| StorageError::RoomLocked(room_id))?;

            use std::io::Write;
            let mut writer = std::io::BufWriter::new(std_tmp);
            writer.write_all(&header.encode())?;
            writer.write_all(&compressed)?;
            writer.flush()?;
            let std_file = writer
                .into_inner()
                .map_err(|e| StorageError::Other(e.to_string()))?;
            std_file.sync_all()?;

            if let Some(parent) = tmp_path.parent() {
                sync_dir(parent)?;
            }

            Ok(CompactionWorkerOutput {
                tmp_path,
                cut_seq,
                compressed_len: compressed.len() as u64,
            })
        })
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))?;

    let output = match worker_res {
        Ok(out) => out,
        Err(err) => {
            let mut room = room_arc.write().await;
            room.is_compacting = false;
            return Err(err);
        }
    };

    // Phase 3: Final atomic replacement and cleanup under brief write lock
    {
        let mut room = room_arc.write().await;

        // Atomically replace base snapshot
        rename(&output.tmp_path, &room.snap_path).await?;

        if let Some(parent) = room.snap_path.parent() {
            let _ = sync_dir(parent);
        }

        // Safely remove wal.compacting
        let wal_compacting_path = room.wal_path.with_extension("wal.compacting");
        let _ = tokio::fs::remove_file(&wal_compacting_path).await;

        room.snapshot_seq = output.cut_seq;
        room.snapshot_len = output.compressed_len;
        room.is_compacting = false;

        tracing::info!(
            snap_path = ?room.snap_path,
            snapshot_len = room.snapshot_len,
            snapshot_seq = %room.snapshot_seq,
            "Compacted room snapshot via Copy-on-Write successfully"
        );
    }

    Ok(())
}

/// Synchronous compaction helper for direct room modifications (e.g. applying external snapshots).
///
/// Serializes current in-memory tables directly by reference and compresses with Zstandard.
/// Writes to unique temporary snapshot file `snap.tmp.{uuid}`, acquires exclusive `flock`,
/// atomically renames over `snap_path`, and truncates the active WAL file.
pub async fn write_snapshot_and_truncate_wal(
    room: &mut DiskRoomState,
    options: &DiskStorageOptions,
) -> Result<(), StorageError> {
    let payload = RoomSnapshotRef {
        head_seq: room.head_seq,
        tables: &room.tables,
    };

    let serialized =
        bincode::serialize(&payload).map_err(|e| StorageError::Serialization(e.to_string()))?;

    let zstd_level = options.zstd_level;
    let compressed =
        tokio::task::spawn_blocking(move || zstd::encode_all(&serialized[..], zstd_level))
            .await
            .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
            .map_err(|e| StorageError::Other(format!("Zstd compression failed: {e}")))?;

    let payload_crc32 = crc32fast::hash(&compressed);
    let uuid_str = uuid::Uuid::new_v4().to_string();
    let tmp_path = room
        .snap_path
        .with_extension(format!("snap.tmp.{}", uuid_str));

    let new_header = FileHeader::new(
        room.head_seq.get(),
        room.head_seq.get(),
        compressed.len() as u64,
        payload_crc32,
    );

    let std_tmp = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp_path)?;
    std_tmp.try_lock_exclusive().map_err(|_| {
        StorageError::RoomLocked(RoomId::new(
            room.snap_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown"),
        ))
    })?;

    use std::io::Write;
    let mut writer = std::io::BufWriter::new(std_tmp);
    writer.write_all(&new_header.encode())?;
    writer.write_all(&compressed)?;
    writer.flush()?;
    let std_file = writer
        .into_inner()
        .map_err(|e| StorageError::Other(e.to_string()))?;
    std_file.sync_all()?;

    // Atomically replace snapshot file
    rename(&tmp_path, &room.snap_path).await?;

    // Directory entry synchronization
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
        "Snapshot written and WAL truncated successfully"
    );

    Ok(())
}

/// Backward compatibility alias.
pub use write_snapshot_and_truncate_wal as compact_room_internal;
