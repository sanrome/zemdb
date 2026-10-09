use fs2::FileExt;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::fs::{rename, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use zemdb_core::{RoomId, SequenceNumber};

use crate::disk::format::FileHeader;
use crate::disk::{DiskRoom, DiskStorageOptions, DiskWal, PendingWrite};
use crate::error::StorageError;
use crate::fail_point;
use crate::memory::{RoomSnapshotPayload, RoomSnapshotRef, Table};
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
pub(crate) struct StagedSnapshot {
    tmp_path: PathBuf,
    pub(crate) compressed_len: u64,
}

impl StagedSnapshot {
    /// Removes the temporary file of a snapshot that will not be installed. A file left
    /// behind (because this fails, or because the caller was dropped before) is removed when
    /// the room is next opened.
    pub(crate) async fn discard(self) {
        if let Err(err) = remove_if_exists(&self.tmp_path).await {
            tracing::warn!(path = ?self.tmp_path, error = %err, "Could not remove a staged snapshot");
        }
    }
}

/// Executes Copy-on-Write compaction using a three-phase protocol:
///
/// 1. Phase 1 (WAL mutex):
///    Moves the current WAL contents into `room_{id}.wal.compacting` and leaves an empty
///    `room_{id}.wal` for incoming concurrent writes. Normally this is a rename plus a fresh
///    WAL file. If a previous compaction failed and left `.wal.compacting` behind, the current
///    WAL is appended to it instead, so its records are never overwritten. Holding the WAL
///    mutex keeps writers out while the WAL moves, and guarantees that every batch in the
///    rotated segment is already applied in memory. Captures the cut sequence and O(1) clones
///    of the tables under a brief shared lock of the state; readers are never blocked.
///
/// 2. Phase 2 (background blocking worker):
///    Serializes and compresses the captured tables and writes them to a unique, synced
///    temporary snapshot file.
///
/// 3. Phase 3 (no room lock for the I/O):
///    Atomically renames the temporary file over `room_{id}.snap`, then removes
///    `room_{id}.wal.compacting`, whose records the new snapshot now contains. Writers only
///    touch the active WAL and the engine's compaction lock keeps out other compactions and
///    `apply_snapshot`, so these files are not shared; the state lock is taken briefly to
///    record the new snapshot's sequence and size.
///
/// If any phase fails, `.wal.compacting` stays on disk: recovery folds it back on the next
/// open, and the next compaction absorbs it.
#[tracing::instrument(skip(room, options), fields(room_id = %room.room_id))]
pub(crate) async fn compact_room_cow(
    room: Arc<DiskRoom>,
    options: &DiskStorageOptions,
) -> Result<(), StorageError> {
    let wal_compacting_path = compacting_wal_path(&room.wal_path);

    // Phase 1: WAL rotation under the WAL mutex
    let (_flag, cut_seq, snapshot_tables) = {
        let mut wal = room.wal.lock().await;
        wal.ensure_usable()?;
        let Some(flag) = CompactionFlag::try_acquire(&room.is_compacting) else {
            return Ok(());
        };

        // A failed sync leaves the durability of earlier writes unknown, and cannot be retried.
        let sync_result = async {
            fail_point::check("compaction.sync", &room.wal_path)?;
            wal.file.sync_all().await?;
            Ok::<(), StorageError>(())
        }
        .await;
        if let Err(err) = sync_result {
            wal.mark_failed(format!("WAL sync before compaction failed: {err}"));
            return Err(err);
        }

        fail_point::pause("compaction.rotate", &room.wal_path).await;
        let absorb = tokio::fs::try_exists(&wal_compacting_path).await?;
        // Both paths handle their own errors (undoing the step or marking the room as failed);
        // the guard covers a rotation abandoned halfway, which neither can see.
        let mut change = PendingWrite::begin(&mut wal);
        let moved = if absorb {
            absorb_wal_into_compacting(&room, change.wal(), &wal_compacting_path).await
        } else {
            rotate_wal_to_compacting(&room, change.wal(), &wal_compacting_path).await
        };
        change.complete();
        moved?;

        let state = room.state.read().await;
        (flag, state.head_seq, state.tables.clone())
    }; // Locks released immediately
    fail_point::pause("compaction.staging", &room.wal_path).await;

    // Phase 2: Heavy serialization, compression and disk I/O in blocking worker thread
    let zstd_level = options.zstd_level;
    let staged = {
        let snap_path = room.snap_path.clone();
        let room_id = room.room_id.clone();
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

    // Phase 3: Final atomic replacement and cleanup
    if let Err(err) = rename(&staged.tmp_path, &room.snap_path).await {
        staged.discard().await;
        return Err(err.into());
    }
    sync_parent(&room.snap_path).await?;
    {
        let mut state = room.state.write().await;
        state.snapshot_seq = cut_seq;
        state.snapshot_len = staged.compressed_len;
    }

    remove_if_exists(&wal_compacting_path).await?;
    sync_parent(&wal_compacting_path).await?;

    tracing::info!(
        snap_path = ?room.snap_path,
        snapshot_len = staged.compressed_len,
        snapshot_seq = %cut_seq,
        "Compacted room snapshot via Copy-on-Write successfully"
    );

    Ok(())
}

/// Moves the active WAL to `.wal.compacting` and installs a fresh, locked, empty WAL.
async fn rotate_wal_to_compacting(
    room: &DiskRoom,
    wal: &mut DiskWal,
    wal_compacting_path: &Path,
) -> Result<(), StorageError> {
    rename(&room.wal_path, wal_compacting_path).await?;
    fail_point::pause("compaction.rotate_renamed", &room.wal_path).await;

    match open_fresh_wal(&room.wal_path, &room.room_id) {
        Ok(new_wal) => {
            wal.file = File::from_std(new_wal);
            wal.len = 0;
        }
        Err(err) => {
            // The open handle still points at the renamed file. Move it back so that
            // subsequent writes keep landing in the file recovery reads as the active WAL.
            if let Err(rollback_err) = roll_back_rotation(&room.wal_path, wal_compacting_path).await
            {
                // Writes would now land in `.wal.compacting`, and a later compaction would
                // absorb that file into itself. Only recovery can sort this out safely.
                wal.mark_failed(format!(
                    "WAL rotation failed ({err}) and could not be rolled back ({rollback_err})"
                ));
            }
            return Err(err);
        }
    }

    // Writes now go to the fresh WAL, whose directory entry must be durable before any of them
    // is acknowledged; a failed directory sync cannot be retried safely.
    let sync_result = async {
        fail_point::check("compaction.rotate_sync_dir", &room.wal_path)?;
        sync_parent(&room.wal_path).await
    }
    .await;
    if let Err(err) = sync_result {
        wal.mark_failed(format!(
            "Syncing the directory after rotating the WAL failed: {err}"
        ));
        return Err(err);
    }
    Ok(())
}

async fn roll_back_rotation(
    wal_path: &Path,
    wal_compacting_path: &Path,
) -> Result<(), StorageError> {
    fail_point::check("compaction.rotate_rollback", wal_path)?;
    rename(wal_compacting_path, wal_path).await?;
    sync_parent(wal_path).await
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
    room: &DiskRoom,
    wal: &mut DiskWal,
    wal_compacting_path: &Path,
) -> Result<(), StorageError> {
    let mut wal_bytes = Vec::new();
    let read_result = async {
        wal.file.seek(SeekFrom::Start(0)).await?;
        wal.file.read_to_end(&mut wal_bytes).await?;
        Ok::<(), StorageError>(())
    }
    .await;
    if let Err(err) = read_result {
        // The handle position is unknown; appending from there could overwrite records.
        wal.mark_failed(format!("Reading the active WAL failed: {err}"));
        return Err(err);
    }

    if !wal_bytes.is_empty() {
        append_synced(wal_compacting_path, &wal_bytes, &room.wal_path, wal).await?;
    }

    fail_point::check("compaction.absorb", &room.wal_path)?;

    if let Err(err) = truncate_wal(wal, &room.wal_path).await {
        // The handle's length and position are unknown; appending from there could overwrite
        // or misplace records.
        wal.mark_failed(format!("Emptying the active WAL failed: {err}"));
        return Err(err);
    }
    Ok(())
}

/// Empties the active WAL through its own locked handle and syncs it.
async fn truncate_wal(wal: &mut DiskWal, wal_path: &Path) -> Result<(), StorageError> {
    wal.file.set_len(0).await?;
    fail_point::pause("wal.truncate", wal_path).await;
    wal.file.seek(SeekFrom::Start(0)).await?;
    wal.file.sync_all().await?;
    wal.len = 0;
    Ok(())
}

/// Appends `bytes` to `path` and syncs it, restoring the previous length if anything fails.
///
/// The file is opened for writing and positioned at its end rather than in append mode: on
/// Windows an append-only handle lacks the write access that truncating it back requires.
async fn append_synced(
    path: &Path,
    bytes: &[u8],
    wal_path: &Path,
    wal: &mut DiskWal,
) -> Result<(), StorageError> {
    let mut file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
    let original_len = file.seek(SeekFrom::End(0)).await?;

    let append_result = async {
        let (first, rest) = bytes.split_at(bytes.len() / 2);
        file.write_all(first).await?;
        fail_point::check("compaction.absorb_write", wal_path)?;
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
            wal.mark_failed(format!(
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

    let written = (|| {
        std_tmp
            .try_lock_exclusive()
            .map_err(|_| StorageError::RoomLocked(room_id.clone()))?;
        fail_point::check("snapshot.stage_write", snap_path)?;
        let mut writer = std::io::BufWriter::new(&std_tmp);
        writer.write_all(&header.encode())?;
        writer.write_all(&compressed)?;
        writer.flush()?;
        drop(writer);
        std_tmp.sync_all()?;
        Ok::<(), StorageError>(())
    })();
    if let Err(err) = written {
        drop(std_tmp);
        if let Err(remove_err) = std::fs::remove_file(&tmp_path) {
            tracing::warn!(path = ?tmp_path, error = %remove_err, "Could not remove a staged snapshot");
        }
        return Err(err);
    }

    Ok(StagedSnapshot {
        tmp_path,
        compressed_len: compressed.len() as u64,
    })
}

/// Serializes a snapshot of `tables` at `head_seq` and writes it to a synced temporary file
/// next to `snap_path`. Nothing visible changes until the file is renamed into place.
pub(crate) async fn stage_snapshot(
    snap_path: &Path,
    head_seq: SequenceNumber,
    tables: &HashMap<u16, Table>,
    zstd_level: i32,
    room_id: &RoomId,
) -> Result<StagedSnapshot, StorageError> {
    let snap_path = snap_path.to_path_buf();
    let room_id = room_id.clone();
    // O(1) clones of the tables, serialized on the blocking pool with the compression.
    let tables = tables.clone();
    tokio::task::spawn_blocking(move || {
        let serialized = bincode::serialize(&RoomSnapshotRef {
            head_seq,
            tables: &tables,
        })
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
        stage_snapshot_blocking(&snap_path, head_seq, &serialized, zstd_level, &room_id)
    })
    .await
    .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
}

/// Atomically replaces `snap_path` with a snapshot of `tables` at `head_seq`.
///
/// Writes a synced temporary file, renames it over the destination and syncs the directory.
/// Returns the compressed payload length.
pub(crate) async fn write_snapshot_file(
    snap_path: &Path,
    head_seq: SequenceNumber,
    tables: &HashMap<u16, Table>,
    zstd_level: i32,
    room_id: &RoomId,
) -> Result<u64, StorageError> {
    let staged = stage_snapshot(snap_path, head_seq, tables, zstd_level, room_id).await?;
    rename(&staged.tmp_path, snap_path).await?;
    sync_parent(snap_path).await?;
    Ok(staged.compressed_len)
}

/// Renames a staged snapshot that replaces the whole room (see `apply_snapshot`) over
/// `room_{id}.snap`. Must run under the room's WAL mutex, followed by
/// [`install_applied_snapshot`]. A failed rename changes nothing on disk.
pub(crate) async fn rename_staged_snapshot(
    room: &DiskRoom,
    staged: &StagedSnapshot,
) -> Result<(), StorageError> {
    fail_point::check("apply_snapshot.rename", &room.wal_path)?;
    rename(&staged.tmp_path, &room.snap_path).await?;
    Ok(())
}

/// Completes the installation of a snapshot renamed by [`rename_staged_snapshot`]: syncs the
/// directory, then empties the active WAL and removes any `.wal.compacting`, all of whose
/// records the snapshot supersedes. Must run under the room's WAL mutex.
///
/// The snapshot is ahead of every record in either WAL file, which is what makes each crash
/// window safe:
/// - Before the rename: the previous snapshot and WALs are intact, and recovery removes the
///   staged temporary file.
/// - After the rename, before the WAL is emptied (or before `.wal.compacting` is removed):
///   recovery loads the new snapshot and skips every remaining record, all of them at or below
///   its sequence.
///
/// A snapshot behind the room would break the second window (records between the two
/// sequence numbers would be replayed on top of it), which is why `apply_snapshot` rejects it.
pub(crate) async fn install_applied_snapshot(
    room: &DiskRoom,
    wal: &mut DiskWal,
) -> Result<(), StorageError> {
    sync_parent(&room.snap_path).await?;
    fail_point::check("apply_snapshot.truncate", &room.wal_path)?;

    truncate_wal(wal, &room.wal_path).await?;

    let wal_compacting_path = compacting_wal_path(&room.wal_path);
    remove_if_exists(&wal_compacting_path).await?;
    sync_parent(&wal_compacting_path).await?;

    tracing::info!(
        snap_path = ?room.snap_path,
        "Snapshot installed and WAL truncated successfully"
    );

    Ok(())
}

pub(crate) async fn remove_if_exists(path: &Path) -> Result<(), StorageError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Syncs the directory holding `path`, so that a rename, creation or removal in it is
/// durable. The directory sync blocks, so it runs on Tokio's blocking pool, like the
/// `tokio::fs` operations around it, instead of stalling a worker thread.
pub(crate) async fn sync_parent(path: &Path) -> Result<(), StorageError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let parent = parent.to_path_buf();
    tokio::task::spawn_blocking(move || sync_dir(&parent))
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))??;
    Ok(())
}

#[cfg(test)]
#[path = "tests/compactor.rs"]
mod tests;
