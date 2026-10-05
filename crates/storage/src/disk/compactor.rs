use fs2::FileExt;
use std::collections::{BTreeMap, HashMap};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::fs::{rename, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;
use zemdb_core::{CompactRow, PrimaryKey, RoomId, SequenceNumber};

use crate::disk::format::FileHeader;
use crate::disk::{DiskRoomState, DiskStorageOptions};
use crate::error::StorageError;
use crate::fail_point;
use crate::memory::{RoomSnapshotPayload, RoomSnapshotRef};
use crate::sys::sync_dir;

/// Path of the WAL segment rotated out by an in-progress (or interrupted) compaction.
pub(crate) fn compacting_wal_path(wal_path: &Path) -> PathBuf {
    wal_path.with_extension("wal.compacting")
}

/// Holds a room's compaction flag and releases it when dropped, including on early returns
/// through `?` and on panics.
struct CompactionFlag(Arc<AtomicBool>);

impl CompactionFlag {
    /// Acquires the flag, or returns `None` if another compaction already holds it.
    fn try_acquire(flag: &Arc<AtomicBool>) -> Option<Self> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| Self(Arc::clone(flag)))
    }
}

impl Drop for CompactionFlag {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// A snapshot written and synced to a temporary file, ready to be renamed into place.
struct StagedSnapshot {
    tmp_path: PathBuf,
    compressed_len: u64,
}

/// Executes Copy-on-Write compaction using a three-phase protocol:
///
/// 1. Phase 1 (brief write lock):
///    Moves the current WAL contents into `room_{id}.wal.compacting` and leaves an empty
///    `room_{id}.wal` for incoming concurrent writes. Normally this is a rename plus a fresh
///    WAL file. If a previous compaction failed and left `.wal.compacting` behind, the current
///    WAL is appended to it instead, so its records are never overwritten. Captures the cut
///    sequence and immutable table references, then releases the room lock.
///
/// 2. Phase 2 (background blocking worker):
///    Serializes and compresses the captured tables and writes them to a unique, synced
///    temporary snapshot file.
///
/// 3. Phase 3 (brief write lock):
///    Atomically renames the temporary file over `room_{id}.snap`, then removes
///    `room_{id}.wal.compacting`, whose records the new snapshot now contains.
///
/// If any phase fails, `.wal.compacting` stays on disk: recovery folds it back on the next
/// open, and the next compaction absorbs it.
#[tracing::instrument(skip(room_arc, options))]
pub async fn compact_room_cow(
    room_arc: Arc<RwLock<DiskRoomState>>,
    options: &DiskStorageOptions,
) -> Result<(), StorageError> {
    // Phase 1: Preparation and WAL rotation under exclusive write lock
    let (_flag, cut_seq, snapshot_tables, snap_path, wal_compacting_path, room_id) = {
        let mut room = room_arc.write().await;
        room.ensure_usable()?;
        let Some(flag) = CompactionFlag::try_acquire(&room.is_compacting) else {
            return Ok(());
        };

        room.wal_file.sync_all().await?;

        let room_id = room.room_id.clone();
        let wal_compacting_path = compacting_wal_path(&room.wal_path);

        if tokio::fs::try_exists(&wal_compacting_path).await? {
            absorb_wal_into_compacting(&mut room, &wal_compacting_path).await?;
        } else {
            rotate_wal_to_compacting(&mut room, &wal_compacting_path, &room_id).await?;
        }

        (
            flag,
            room.head_seq,
            room.tables.clone(),
            room.snap_path.clone(),
            wal_compacting_path,
            room_id,
        )
    }; // Lock released immediately

    // Phase 2: Heavy serialization, compression and disk I/O in blocking worker thread
    let zstd_level = options.zstd_level;
    let staged = {
        let snap_path = snap_path.clone();
        tokio::task::spawn_blocking(move || -> Result<StagedSnapshot, StorageError> {
            fail_point::check("compaction.phase2", &snap_path)?;
            let payload = RoomSnapshotPayload {
                head_seq: cut_seq,
                tables: snapshot_tables,
            };
            let serialized = bincode::serialize(&payload)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            stage_snapshot_blocking(&snap_path, cut_seq, &serialized, zstd_level, &room_id)
        })
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))??
    };

    // Phase 3: Final atomic replacement and cleanup under brief write lock
    let mut room = room_arc.write().await;

    rename(&staged.tmp_path, &snap_path).await?;
    room.snapshot_seq = cut_seq;
    room.snapshot_len = staged.compressed_len;
    sync_parent(&snap_path)?;

    remove_if_exists(&wal_compacting_path).await?;
    sync_parent(&wal_compacting_path)?;

    tracing::info!(
        snap_path = ?room.snap_path,
        snapshot_len = room.snapshot_len,
        snapshot_seq = %room.snapshot_seq,
        "Compacted room snapshot via Copy-on-Write successfully"
    );

    Ok(())
}

/// Moves the active WAL to `.wal.compacting` and installs a fresh, locked, empty WAL.
async fn rotate_wal_to_compacting(
    room: &mut DiskRoomState,
    wal_compacting_path: &Path,
    room_id: &RoomId,
) -> Result<(), StorageError> {
    rename(&room.wal_path, wal_compacting_path).await?;

    match open_fresh_wal(&room.wal_path, room_id) {
        Ok(new_wal) => {
            room.wal_file = File::from_std(new_wal);
            room.wal_len = 0;
        }
        Err(err) => {
            // The open handle still points at the renamed file. Move it back so that
            // subsequent writes keep landing in the file recovery reads as the active WAL.
            if let Err(rollback_err) = roll_back_rotation(&room.wal_path, wal_compacting_path).await
            {
                // Writes would now land in `.wal.compacting`, and a later compaction would
                // absorb that file into itself. Only recovery can sort this out safely.
                room.mark_failed(format!(
                    "WAL rotation failed ({err}) and could not be rolled back ({rollback_err})"
                ));
            }
            return Err(err);
        }
    }

    sync_parent(&room.wal_path)
}

async fn roll_back_rotation(
    wal_path: &Path,
    wal_compacting_path: &Path,
) -> Result<(), StorageError> {
    fail_point::check("compaction.rotate_rollback", wal_path)?;
    rename(wal_compacting_path, wal_path).await?;
    sync_parent(wal_path)
}

fn open_fresh_wal(wal_path: &Path, room_id: &RoomId) -> Result<std::fs::File, StorageError> {
    fail_point::check("compaction.rotate_open", wal_path)?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(wal_path)?;
    file.try_lock_exclusive()
        .map_err(|_| StorageError::RoomLocked(room_id.clone()))?;
    Ok(file)
}

/// Appends the active WAL to a `.wal.compacting` left by a failed compaction, then empties
/// the active WAL.
///
/// The append is synced before the active WAL is truncated, so a crash in between only
/// duplicates records, which recovery skips. The active WAL is read through its own locked
/// handle (a second handle cannot read a file locked on Windows), and that handle and its lock
/// are kept. If the append fails, `.wal.compacting` is truncated back to its previous length so
/// that a partially written batch never sits in front of records appended later.
async fn absorb_wal_into_compacting(
    room: &mut DiskRoomState,
    wal_compacting_path: &Path,
) -> Result<(), StorageError> {
    let mut wal_bytes = Vec::new();
    let read_result = async {
        room.wal_file.seek(SeekFrom::Start(0)).await?;
        room.wal_file.read_to_end(&mut wal_bytes).await?;
        Ok::<(), StorageError>(())
    }
    .await;
    if let Err(err) = read_result {
        // The handle position is unknown; appending from there could overwrite records.
        room.mark_failed(format!("Reading the active WAL failed: {err}"));
        return Err(err);
    }

    if !wal_bytes.is_empty() {
        append_synced(wal_compacting_path, &wal_bytes, room).await?;
    }

    fail_point::check("compaction.absorb", &room.wal_path)?;

    room.wal_file.set_len(0).await?;
    room.wal_file.seek(SeekFrom::Start(0)).await?;
    room.wal_file.sync_all().await?;
    room.wal_len = 0;
    Ok(())
}

/// Appends `bytes` to `path` and syncs it, restoring the previous length if anything fails.
async fn append_synced(
    path: &Path,
    bytes: &[u8],
    room: &mut DiskRoomState,
) -> Result<(), StorageError> {
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .await?;
    let original_len = file.metadata().await?.len();

    let append_result = async {
        let (first, rest) = bytes.split_at(bytes.len() / 2);
        file.write_all(first).await?;
        fail_point::check("compaction.absorb_write", &room.wal_path)?;
        file.write_all(rest).await?;
        file.sync_all().await?;
        Ok::<(), StorageError>(())
    }
    .await;

    if let Err(err) = append_result {
        let restore_result = async {
            file.set_len(original_len).await?;
            file.sync_all().await?;
            Ok::<(), StorageError>(())
        }
        .await;
        if let Err(restore_err) = restore_result {
            room.mark_failed(format!(
                "Appending to {path:?} failed ({err}) and its length could not be restored \
                 ({restore_err})"
            ));
        }
        return Err(err);
    }
    Ok(())
}

/// Compresses an already serialized snapshot payload and writes it, with its header, to a
/// unique temporary file next to `snap_path`, synced to disk.
fn stage_snapshot_blocking(
    snap_path: &Path,
    head_seq: SequenceNumber,
    serialized: &[u8],
    zstd_level: i32,
    room_id: &RoomId,
) -> Result<StagedSnapshot, StorageError> {
    use std::io::Write;

    let compressed = zstd::encode_all(serialized, zstd_level)
        .map_err(|e| StorageError::Other(format!("Zstd compression failed: {e}")))?;

    let payload_crc32 = crc32fast::hash(&compressed);
    let uuid_str = uuid::Uuid::new_v4().to_string();
    let tmp_path = snap_path.with_extension(format!("snap.tmp.{}", uuid_str));

    let header = FileHeader::new(
        head_seq.get(),
        head_seq.get(),
        compressed.len() as u64,
        payload_crc32,
    );

    let std_tmp = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp_path)?;
    std_tmp
        .try_lock_exclusive()
        .map_err(|_| StorageError::RoomLocked(room_id.clone()))?;

    let mut writer = std::io::BufWriter::new(std_tmp);
    writer.write_all(&header.encode())?;
    writer.write_all(&compressed)?;
    writer.flush()?;
    let std_file = writer
        .into_inner()
        .map_err(|e| StorageError::Other(e.to_string()))?;
    std_file.sync_all()?;

    Ok(StagedSnapshot {
        tmp_path,
        compressed_len: compressed.len() as u64,
    })
}

/// Atomically replaces `snap_path` with a snapshot of `tables` at `head_seq`.
///
/// Writes a synced temporary file, renames it over the destination and syncs the directory.
/// Returns the compressed payload length.
pub(crate) async fn write_snapshot_file(
    snap_path: &Path,
    head_seq: SequenceNumber,
    tables: &HashMap<u16, Arc<BTreeMap<PrimaryKey, CompactRow>>>,
    zstd_level: i32,
    room_id: &RoomId,
) -> Result<u64, StorageError> {
    let serialized = bincode::serialize(&RoomSnapshotRef { head_seq, tables })
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    let staged = {
        let snap_path = snap_path.to_path_buf();
        let room_id = room_id.clone();
        tokio::task::spawn_blocking(move || {
            stage_snapshot_blocking(&snap_path, head_seq, &serialized, zstd_level, &room_id)
        })
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))??
    };

    rename(&staged.tmp_path, snap_path).await?;
    sync_parent(snap_path)?;
    Ok(staged.compressed_len)
}

/// Synchronous compaction helper for direct room modifications (e.g. applying external snapshots).
///
/// Writes a snapshot of the current in-memory tables atomically, then truncates the active WAL
/// and removes any leftover `.wal.compacting`, all of whose records the snapshot now contains.
pub async fn write_snapshot_and_truncate_wal(
    room: &mut DiskRoomState,
    options: &DiskStorageOptions,
) -> Result<(), StorageError> {
    let room_id = room.room_id.clone();
    let compressed_len = write_snapshot_file(
        &room.snap_path,
        room.head_seq,
        &room.tables,
        options.zstd_level,
        &room_id,
    )
    .await?;
    room.snapshot_seq = room.head_seq;
    room.snapshot_len = compressed_len;

    // In-place truncation of the append-only WAL file (.wal)
    room.wal_file.set_len(0).await?;
    room.wal_file.seek(SeekFrom::Start(0)).await?;
    room.wal_file.sync_all().await?;
    room.wal_len = 0;

    let wal_compacting_path = compacting_wal_path(&room.wal_path);
    remove_if_exists(&wal_compacting_path).await?;
    sync_parent(&wal_compacting_path)?;

    tracing::info!(
        snap_path = ?room.snap_path,
        snapshot_len = compressed_len,
        head_seq = %room.head_seq,
        "Snapshot written and WAL truncated successfully"
    );

    Ok(())
}

/// Backward compatibility alias.
pub use write_snapshot_and_truncate_wal as compact_room_internal;

pub(crate) async fn remove_if_exists(path: &Path) -> Result<(), StorageError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn sync_parent(path: &Path) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/compactor.rs"]
mod tests;
