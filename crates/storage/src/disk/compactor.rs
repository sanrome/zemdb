use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use zemdb_core::{RoomId, SequenceNumber};

use crate::disk::format::{FileHeader, HEADER_SIZE};
use crate::disk::{DiskRoom, DiskStorageOptions, DiskWal, PendingWrite};
use crate::error::StorageError;
use crate::fail_point;
use crate::files::{RoomFile, RoomFiles};
use crate::memory::{RoomSnapshotRef, Table};
use crate::sys::run_blocking;

/// Path of the WAL segment rotated out by an in-progress (or interrupted) compaction.
pub(crate) fn compacting_wal_path(wal_path: &Path) -> PathBuf {
    wal_path.with_extension("wal.compacting")
}

/// Path of the copy of `.wal.compacting` and the active WAL that an absorption renames over
/// `.wal.compacting`.
pub(crate) fn absorbing_wal_path(wal_path: &Path) -> PathBuf {
    wal_path.with_extension("wal.compacting.tmp")
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
    /// behind (because this fails, or because of a crash) is removed when
    /// the room is next opened.
    pub(crate) async fn discard(self, files: &impl RoomFiles) {
        if let Err(err) = remove_if_exists(files, &self.tmp_path).await {
            tracing::warn!(path = ?self.tmp_path, error = %err, "Could not remove a staged snapshot");
        }
    }
}

/// Executes Copy-on-Write compaction using a three-phase protocol:
///
/// 1. Phase 1 (WAL mutex):
///    Moves the current WAL contents into `room_{id}.wal.compacting` and leaves an empty
///    `room_{id}.wal` for incoming concurrent writes. Normally this is a rename plus a fresh
///    WAL file. If a previous compaction failed and left `.wal.compacting` behind, a copy of it
///    with the current WAL appended replaces it instead, so its records are never overwritten
///    (see `absorb_wal_into_compacting`). Holding the WAL mutex keeps writers out while the WAL
///    moves, and guarantees that every batch in the rotated segment is already applied in
///    memory. Captures the cut sequence and O(1) clones
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
/// If any phase fails before the cleanup, `.wal.compacting` stays on disk: recovery folds it
/// back on the next open, and the next compaction absorbs it. If the directory sync after the
/// snapshot rename fails, `room_{id}.snap` on disk is already the new one while the in-memory
/// `snapshot_seq` and `snapshot_len` keep their old values; that is benign, since they only feed
/// the auto-compaction threshold and `.wal.compacting` is kept. If the cleanup fails,
/// `.wal.compacting` may already be gone, or may come back after a crash; either way its records
/// are in the new snapshot and recovery skips them.
#[tracing::instrument(skip(room, options), fields(room_id = %room.room_id))]
pub(crate) async fn compact_room_cow<F: RoomFiles>(
    room: Arc<DiskRoom<F>>,
    options: &DiskStorageOptions,
) -> Result<(), StorageError> {
    let files = &*room.files;
    let wal_compacting_path = compacting_wal_path(&room.wal_path);

    // Phase 1
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
        wal.fail_on_err("WAL sync before compaction failed", sync_result)?;

        fail_point::pause("compaction.rotate", &room.wal_path).await;
        let absorb = files.exists(&wal_compacting_path).await?;
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
        (flag, state.head_seq, state.data.tables().clone())
    }; // WAL mutex and state lock released here; the flag lives until the end
    fail_point::pause("compaction.staging", &room.wal_path).await;

    // Phase 2
    fail_point::check("compaction.phase2", &room.snap_path)?;
    let staged = stage_snapshot(
        files,
        &room.snap_path,
        cut_seq,
        &snapshot_tables,
        options.zstd_level,
        &room.room_id,
    )
    .await?;
    // Dropped before phase 3, so that it stops keeping alive the nodes that writes replace.
    drop(snapshot_tables);

    // Phase 3
    let renamed = async {
        fail_point::check("compaction.phase3_rename", &room.wal_path)?;
        files.rename(&staged.tmp_path, &room.snap_path).await?;
        Ok::<(), StorageError>(())
    }
    .await;
    if let Err(err) = renamed {
        staged.discard(files).await;
        return Err(err);
    }
    fail_point::panic_point("compaction.renamed", &room.wal_path);
    fail_point::check("compaction.phase3_sync", &room.wal_path)?;
    sync_parent(files, &room.snap_path).await?;
    {
        let mut state = room.state.write().await;
        state.snapshot_seq = cut_seq;
        state.snapshot_len = staged.compressed_len;
    }

    fail_point::check("compaction.cleanup", &room.wal_path)?;
    remove_if_exists(files, &wal_compacting_path).await?;
    sync_parent(files, &wal_compacting_path).await?;

    tracing::info!(
        snap_path = ?room.snap_path,
        snapshot_len = staged.compressed_len,
        snapshot_seq = %cut_seq,
        "Compacted room snapshot via Copy-on-Write successfully"
    );

    Ok(())
}

/// Moves the active WAL to `.wal.compacting` and installs a fresh, locked, empty WAL.
async fn rotate_wal_to_compacting<F: RoomFiles>(
    room: &DiskRoom<F>,
    wal: &mut DiskWal<F>,
    wal_compacting_path: &Path,
) -> Result<(), StorageError> {
    let files = &*room.files;
    // The only file renamed with its handle open, so through its handle, which follows it: a
    // failed rename leaves the WAL where it was.
    wal.file.rename(wal_compacting_path).await?;
    fail_point::pause("compaction.rotate_renamed", &room.wal_path).await;
    fail_point::panic_point("compaction.rotated", &room.wal_path);

    match open_fresh_wal(files, &room.wal_path, &room.room_id).await {
        Ok(new_wal) => {
            wal.file = new_wal;
            wal.len = 0;
        }
        Err(err) => {
            // The open handle still points at the renamed file. Move it back so that
            // subsequent writes keep landing in the file recovery reads as the active WAL.
            if let Err(rollback_err) = roll_back_rotation(files, wal, &room.wal_path).await {
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
        sync_parent(files, &room.wal_path).await
    }
    .await;
    wal.fail_on_err(
        "Syncing the directory after rotating the WAL failed",
        sync_result,
    )
}

/// Renames the rotated WAL, still open in `wal`, back to `wal_path`.
async fn roll_back_rotation<F: RoomFiles>(
    files: &F,
    wal: &mut DiskWal<F>,
    wal_path: &Path,
) -> Result<(), StorageError> {
    fail_point::check("compaction.rotate_rollback", wal_path)?;
    wal.file.rename(wal_path).await?;
    sync_parent(files, wal_path).await
}

/// Creates and locks the WAL that replaces the one just renamed. It must not exist: a file
/// found there is never truncated or adopted.
async fn open_fresh_wal<F: RoomFiles>(
    files: &F,
    wal_path: &Path,
    room_id: &RoomId,
) -> Result<F::File, StorageError> {
    fail_point::check("compaction.rotate_open", wal_path)?;
    files
        .create_locked(wal_path)
        .await?
        .ok_or_else(|| StorageError::RoomLocked(room_id.clone()))
}

/// Absorbs the active WAL into a `.wal.compacting` left by a failed compaction, then empties
/// the active WAL.
///
/// `.wal.compacting` is never written in place: a copy of it followed by the active WAL is
/// written to a temporary file, synced and renamed over it, and the directory is synced. Only then
/// is the active WAL emptied, so a crash at any step leaves `.wal.compacting` whole, as it was or
/// with the active WAL appended, and every record in it or in the active WAL (recovery skips the
/// duplicates and removes the temporary file). The active WAL is read through its own locked
/// handle (a second handle cannot read a file locked on Windows), and that handle and its lock
/// are kept.
async fn absorb_wal_into_compacting<F: RoomFiles>(
    room: &DiskRoom<F>,
    wal: &mut DiskWal<F>,
    wal_compacting_path: &Path,
) -> Result<(), StorageError> {
    let files = &*room.files;
    let read_result = async {
        let len = wal.file.size().await?;
        let len = usize::try_from(len).map_err(|_| {
            StorageError::Other(format!(
                "The active WAL is too large to absorb: {len} bytes"
            ))
        })?;
        Ok::<_, StorageError>(wal.file.read_at(0, len).await?)
    }
    .await;
    // A WAL that cannot be read is not written to again: only recovery, which reads it from
    // the start, can tell which of its records are whole.
    let wal_bytes = wal.fail_on_err("Reading the active WAL failed", read_result)?;

    if !wal_bytes.is_empty() {
        replace_with_absorbed_copy(files, wal_compacting_path, wal_bytes, &room.wal_path).await?;
        // The rename may not be durable, and a failed directory sync cannot be retried: emptying
        // the active WAL could lose its records.
        let synced = async {
            fail_point::check("compaction.absorb_sync_dir", &room.wal_path)?;
            sync_parent(files, wal_compacting_path).await
        }
        .await;
        wal.fail_on_err(
            "Syncing the directory after absorbing the WAL failed",
            synced,
        )?;
    }

    fail_point::check("compaction.absorb", &room.wal_path)?;

    // On failure the WAL's length is unknown; appending at `len` could overwrite or misplace
    // records.
    let truncated = truncate_wal(wal, &room.wal_path).await;
    wal.fail_on_err("Emptying the active WAL failed", truncated)
}

/// Empties the active WAL through its own locked handle and syncs it.
async fn truncate_wal<F: RoomFiles>(
    wal: &mut DiskWal<F>,
    wal_path: &Path,
) -> Result<(), StorageError> {
    wal.file.set_len(0).await?;
    fail_point::pause("wal.truncate", wal_path).await;
    wal.file.sync_all().await?;
    wal.len = 0;
    Ok(())
}

/// Bytes an absorption copies from `.wal.compacting` at a time.
const COPY_CHUNK: usize = 1024 * 1024;

/// Writes `compacting_path` followed by `wal_bytes` to `absorbing_wal_path`, syncs it and
/// renames it over `compacting_path`. On failure nothing visible changed: the temporary file is
/// removed, and one left behind by a crash is removed when the room is next opened.
async fn replace_with_absorbed_copy(
    files: &impl RoomFiles,
    compacting_path: &Path,
    wal_bytes: Vec<u8>,
    wal_path: &Path,
) -> Result<(), StorageError> {
    let tmp_path = absorbing_wal_path(wal_path);
    // `.wal.compacting` is closed once copied and the copy before its rename (neither may be
    // open then); on an error, each is closed when the block ends, before the removal.
    let replaced = async {
        let tmp = files.create(&tmp_path).await?;
        let copied = {
            let compacting = files.open_read(compacting_path).await?;
            let len = compacting.size().await?;
            let mut copied = 0;
            while copied < len {
                let chunk = (len - copied).min(COPY_CHUNK as u64) as usize;
                tmp.write_at(copied, compacting.read_at(copied, chunk).await?)
                    .await?;
                copied += chunk as u64;
            }
            copied
        };
        fail_point::check("compaction.absorb_write", wal_path)?;
        tmp.write_at(copied, wal_bytes).await?;
        tmp.sync_all().await?;
        drop(tmp);
        let renamed = files.rename(&tmp_path, compacting_path).await;
        fail_point::io_result("compaction.absorb_rename", wal_path, renamed)?;
        Ok::<(), StorageError>(())
    }
    .await;
    if replaced.is_err() {
        match files.remove(&tmp_path).await {
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
                tracing::warn!(path = ?tmp_path, error = %err, "Could not remove an absorbed WAL copy");
            }
            _ => {}
        }
    }
    replaced
}

/// Writes `header` followed by `payload` to a unique temporary file next to `snap_path`, created
/// with its lock, and syncs it. On failure the temporary file is removed; one left behind by a
/// crash is removed when the room is next opened.
async fn stage_file(
    files: &impl RoomFiles,
    snap_path: &Path,
    header: &FileHeader,
    payload: Vec<u8>,
    room_id: &RoomId,
) -> Result<StagedSnapshot, StorageError> {
    let tmp_path = snap_path.with_extension(format!("snap.tmp.{}", uuid::Uuid::new_v4()));
    let compressed_len = payload.len() as u64;

    let tmp = files.create_locked(&tmp_path).await?;
    // The handle is closed when the block ends, before the removal.
    let written = async {
        let tmp = tmp.ok_or_else(|| StorageError::RoomLocked(room_id.clone()))?;
        fail_point::check("snapshot.stage_write", snap_path)?;
        tmp.write_at(0, header.encode().to_vec()).await?;
        tmp.write_at(HEADER_SIZE as u64, payload).await?;
        tmp.sync_all().await?;
        Ok::<(), StorageError>(())
    }
    .await;
    if let Err(err) = written {
        if let Err(remove_err) = files.remove(&tmp_path).await {
            tracing::warn!(path = ?tmp_path, error = %remove_err, "Could not remove a staged snapshot");
        }
        return Err(err);
    }

    Ok(StagedSnapshot {
        tmp_path,
        compressed_len,
    })
}

/// Serializes a snapshot of `tables` at `head_seq` and writes it to a synced temporary file
/// next to `snap_path`. Nothing visible changes until the file is renamed into place.
pub(crate) async fn stage_snapshot(
    files: &impl RoomFiles,
    snap_path: &Path,
    head_seq: SequenceNumber,
    tables: &HashMap<u16, Table>,
    zstd_level: i32,
    room_id: &RoomId,
) -> Result<StagedSnapshot, StorageError> {
    // O(1) clones of the tables, serialized on the blocking pool with the compression.
    let tables = tables.clone();
    let (header, compressed) = run_blocking(move || {
        let serialized = bincode::serialize(&RoomSnapshotRef {
            head_seq,
            tables: &tables,
        })
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
        let compressed = crate::snapshot::compress_zstd(&serialized, zstd_level)?;
        let header = FileHeader::new(
            head_seq.get(),
            head_seq.get(),
            compressed.len() as u64,
            crc32fast::hash(&compressed),
        );
        Ok::<_, StorageError>((header, compressed))
    })
    .await?;
    stage_file(files, snap_path, &header, compressed, room_id).await
}

/// Atomically replaces `snap_path` with a snapshot of `tables` at `head_seq`.
///
/// Writes a synced temporary file, renames it over the destination and syncs the directory.
/// Returns the compressed payload length.
pub(crate) async fn write_snapshot_file(
    files: &impl RoomFiles,
    snap_path: &Path,
    head_seq: SequenceNumber,
    tables: &HashMap<u16, Table>,
    zstd_level: i32,
    room_id: &RoomId,
) -> Result<u64, StorageError> {
    let staged = stage_snapshot(files, snap_path, head_seq, tables, zstd_level, room_id).await?;
    install_staged(files, snap_path, staged).await
}

/// Creates the empty snapshot of a new room (a header without payload) at `snap_path`, through
/// a synced temporary file renamed into place, so that a failure or a crash never leaves a
/// partial `room_{id}.snap` that would keep the room from opening.
pub(crate) async fn write_empty_snapshot_file(
    files: &impl RoomFiles,
    snap_path: &Path,
    room_id: &RoomId,
) -> Result<(), StorageError> {
    let staged = stage_file(
        files,
        snap_path,
        &FileHeader::new(0, 0, 0, 0),
        Vec::new(),
        room_id,
    )
    .await?;
    install_staged(files, snap_path, staged).await?;
    Ok(())
}

/// Renames a staged snapshot over `snap_path` and syncs the directory. Returns the compressed
/// payload length. If the rename fails, the staged file is removed.
async fn install_staged(
    files: &impl RoomFiles,
    snap_path: &Path,
    staged: StagedSnapshot,
) -> Result<u64, StorageError> {
    fail_point::pause("snapshot.install", snap_path).await;
    if let Err(err) = files.rename(&staged.tmp_path, snap_path).await {
        staged.discard(files).await;
        return Err(err.into());
    }
    sync_parent(files, snap_path).await?;
    Ok(staged.compressed_len)
}

/// Renames a staged snapshot that replaces the whole room (see `apply_snapshot`) over
/// `room_{id}.snap`. Must run under the room's WAL mutex, followed by
/// [`install_applied_snapshot`]. A failed rename changes nothing on disk.
pub(crate) async fn rename_staged_snapshot<F: RoomFiles>(
    room: &DiskRoom<F>,
    staged: &StagedSnapshot,
) -> Result<(), StorageError> {
    fail_point::check("apply_snapshot.rename", &room.wal_path)?;
    let key = room.wal_path.clone();
    run_blocking(move || {
        fail_point::pause_blocking("apply_snapshot.rename", &key);
        Ok::<_, StorageError>(())
    })
    .await?;
    Ok(room.files.rename(&staged.tmp_path, &room.snap_path).await?)
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
pub(crate) async fn install_applied_snapshot<F: RoomFiles>(
    room: &DiskRoom<F>,
    wal: &mut DiskWal<F>,
) -> Result<(), StorageError> {
    let files = &*room.files;
    sync_parent(files, &room.snap_path).await?;
    fail_point::check("apply_snapshot.truncate", &room.wal_path)?;

    truncate_wal(wal, &room.wal_path).await?;

    let wal_compacting_path = compacting_wal_path(&room.wal_path);
    remove_if_exists(files, &wal_compacting_path).await?;
    sync_parent(files, &wal_compacting_path).await?;

    tracing::info!(
        snap_path = ?room.snap_path,
        "Snapshot installed and WAL truncated successfully"
    );

    Ok(())
}

/// Removes the file `path`, if there is one.
pub(crate) async fn remove_if_exists(
    files: &impl RoomFiles,
    path: &Path,
) -> Result<(), StorageError> {
    match files.remove(path).await {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

/// Syncs the directory holding `path`, so that a rename, creation or removal in it is
/// durable.
pub(crate) async fn sync_parent(files: &impl RoomFiles, path: &Path) -> Result<(), StorageError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    Ok(files.sync_dir(parent).await?)
}

#[cfg(test)]
#[path = "tests/compactor.rs"]
mod tests;
