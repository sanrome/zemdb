use std::path::Path;

use zemdb_core::{
    decode_batch_payload, parse_batch_header, BatchPayloadDecode, FrameAnomaly, FrameSearch,
    FrameSearchStep, RoomId, Schema, SequenceNumber, BATCH_HEADER_SIZE,
};

use crate::disk::compactor::{
    absorbing_wal_path, compacting_wal_path, remove_if_exists, sync_parent,
    write_empty_snapshot_file, write_snapshot_file,
};
use crate::disk::format::{FileHeader, HEADER_SIZE};
use crate::error::StorageError;
use crate::fail_point;
use crate::files::{RoomFile, RoomFiles};
use crate::memory::state::{apply_operation, RoomData, RoomSnapshotPayload};
use crate::sys::run_blocking;

/// Recovery result containing the reconstructed in-memory state and the open WAL file handle.
#[derive(Debug)]
pub(crate) struct RecoveredRoom<W> {
    pub wal_file: W,
    pub head_seq: SequenceNumber,
    pub snapshot_seq: SequenceNumber,
    pub snapshot_len: u64,
    pub wal_len: u64,
    pub data: RoomData,
}

struct WalReplayOutcome {
    valid_bytes: u64,
    /// Why the replay stopped before the end of the file, at a frame that could not be read and
    /// has no valid frame after it: a torn write.
    torn_write: Option<String>,
    applied_count: usize,
}

/// Bytes a replay reads from a WAL at a time, at least.
const REPLAY_WINDOW: usize = 64 * 1024;

/// Reads a file of the layer from the start, a window of at least [`REPLAY_WINDOW`] bytes at a
/// time, so that the replay of a WAL of small frames does not take a file operation per frame.
struct WindowedReader<'a, W> {
    file: &'a W,
    file_len: u64,
    window: Vec<u8>,
    window_start: u64,
}

impl<'a, W: RoomFile> WindowedReader<'a, W> {
    fn new(file: &'a W, file_len: u64) -> Self {
        Self {
            file,
            file_len,
            window: Vec::new(),
            window_start: 0,
        }
    }

    /// The `len` bytes at `offset`, which end at or before `file_len`.
    async fn read(&mut self, offset: u64, len: usize) -> std::io::Result<&[u8]> {
        let window_end = self.window_start + self.window.len() as u64;
        if offset < self.window_start || offset + len as u64 > window_end {
            let wanted = (len.max(REPLAY_WINDOW) as u64).min(self.file_len - offset);
            self.window = self.file.read_at(offset, wanted as usize).await?;
            self.window_start = offset;
        }
        let start = (offset - self.window_start) as usize;
        Ok(&self.window[start..start + len])
    }
}

/// Finds the first complete valid frame of `file` that starts at or after `from` and ends at or
/// before `end` (see [`find_valid_frame`]), reading what the search asks for with `read_at`: a
/// window, or a chunk of a payload, at a time.
///
/// [`find_valid_frame`]: zemdb_core::find_valid_frame
async fn find_valid_frame_in(
    file: &impl RoomFile,
    from: u64,
    end: u64,
) -> std::io::Result<Option<u64>> {
    let mut search = FrameSearch::new(from, end);
    loop {
        match search.step() {
            FrameSearchStep::Read { offset, len } => search.feed(&file.read_at(offset, len).await?),
            FrameSearchStep::Done(found) => return Ok(found),
        }
    }
}

/// Classifies an anomaly found at `frame_start` of `file` (see [`FrameAnomaly`]), searching
/// the rest of the file for a valid frame. Returns the reason of a torn write, or the
/// corruption as an error.
async fn classify_anomaly(
    file: &impl RoomFile,
    frame_start: u64,
    file_len: u64,
    anomaly: FrameAnomaly,
) -> Result<String, StorageError> {
    let from = frame_start.saturating_add(anomaly.search_offset());
    let following = find_valid_frame_in(file, from, file_len).await?;
    Ok(anomaly.classify(following)?)
}

/// Replays the framed batches of one WAL file on top of `data`, advancing `head_seq`.
///
/// The file is read a window at a time rather than all at once, and each frame is decoded with
/// the primitives of `zemdb_core::protocol::wal_frame`, so a torn write at the end of the file is
/// told apart from corruption exactly as `decode_wal_batch_from_slice` does: a frame that
/// cannot be read ends the valid part of the file, unless a valid frame follows it, which is
/// corruption.
///
/// Records at or below `head_seq` are already reflected in the state (from the snapshot or an
/// earlier file) and are skipped: crash windows during compaction can leave the same records
/// in more than one file. A record that does not continue the sequence exactly is a gap, which
/// means lost data, and is reported as corruption. A record of a table that `schema` does not
/// have is `SchemaMismatch`: skipping it would lose it for good at the next compaction. Each
/// record is decided as when its batch was applied, and the decision is discarded: rejected
/// records are consumed too.
async fn replay_wal_file(
    room_id: &RoomId,
    file: &impl RoomFile,
    schema: &Schema,
    head_seq: &mut SequenceNumber,
    data: &mut RoomData,
) -> Result<WalReplayOutcome, StorageError> {
    let mut valid_bytes: u64 = 0;
    let mut applied_count: usize = 0;

    let file_len = file.size().await?;
    let mut reader = WindowedReader::new(file, file_len);

    let anomaly = loop {
        let available = file_len - valid_bytes;
        if available == 0 {
            break None;
        }
        if available < BATCH_HEADER_SIZE as u64 {
            break Some(FrameAnomaly::IncompleteHeader {
                available: available as usize,
            });
        }

        let header_buf = reader.read(valid_bytes, BATCH_HEADER_SIZE).await?;
        let header_buf = header_buf
            .try_into()
            .expect("the reader returns the length asked for");
        let header = match parse_batch_header(header_buf) {
            Ok(header) => header,
            Err(invalid) => break Some(FrameAnomaly::InvalidHeader(invalid)),
        };

        // Checked before reading so that a frame cut short does not allocate its whole length.
        if available < header.frame_len() as u64 {
            break Some(FrameAnomaly::TruncatedPayload {
                available,
                frame_len: header.frame_len(),
            });
        }

        let payload = reader
            .read(valid_bytes + BATCH_HEADER_SIZE as u64, header.payload_len())
            .await?;

        let ops = match decode_batch_payload(&header, payload)? {
            BatchPayloadDecode::Batch { ops, .. } => ops,
            BatchPayloadDecode::ChecksumMismatch { expected, actual } => {
                break Some(FrameAnomaly::ChecksumMismatch {
                    expected,
                    actual,
                    frame_len: header.frame_len(),
                });
            }
        };

        for op in ops {
            if op.seq <= *head_seq {
                continue;
            }
            if op.seq.get() != head_seq.get() + 1 {
                return Err(StorageError::WalCorruption(format!(
                    "WAL sequence gap: expected {}, found {}",
                    head_seq.get() + 1,
                    op.seq.get()
                )));
            }

            ensure_known_table(room_id, schema, op.op.table_id())?;
            apply_operation(data, schema, op.op);
            *head_seq = op.seq;
            applied_count += 1;
        }

        valid_bytes += header.frame_len() as u64;
    };

    let torn_write = match anomaly {
        Some(anomaly) => Some(classify_anomaly(file, valid_bytes, file_len, anomaly).await?),
        None => None,
    };

    Ok(WalReplayOutcome {
        valid_bytes,
        torn_write,
        applied_count,
    })
}

/// A room's base snapshot, as loaded by `load_snapshot`.
struct BaseSnapshot {
    data: RoomData,
    snapshot_seq: SequenceNumber,
    head_seq: SequenceNumber,
    snapshot_len: u64,
}

/// `SchemaMismatch` unless `schema` has the table `table_id`, which holds data of the room.
fn ensure_known_table(
    room_id: &RoomId,
    schema: &Schema,
    table_id: u16,
) -> Result<(), StorageError> {
    if schema.has_table_by_id(table_id) {
        return Ok(());
    }
    Err(StorageError::SchemaMismatch {
        room_id: room_id.clone(),
        table_id,
    })
}

/// Loads the base snapshot from `snap_path` (`room_{id}.snap`), verifying the header and the
/// CRC32 of the compressed payload before decompressing it. The file's length is checked
/// before each read, so an error reading the header or the payload is `Io`, not a damaged
/// snapshot. A table with rows that `schema` does not have is `SchemaMismatch`; an empty one is
/// dropped (a snapshot holds every table of the schema it was compacted with, empty ones
/// included). The indexes of the constraints of `schema` are then built from its rows, and the
/// rows checked against those constraints, on the blocking pool: rows that break one are
/// `SnapshotConstraints`. Every error comes before any file that holds data of the room
/// changes.
///
/// A new room has none: an empty one is staged in a temporary file and renamed into place, so a
/// failure or crash never leaves a partial snapshot behind.
async fn load_snapshot(
    files: &impl RoomFiles,
    room_id: &RoomId,
    snap_path: &Path,
    schema: &Schema,
) -> Result<BaseSnapshot, StorageError> {
    let mut base = BaseSnapshot {
        data: RoomData::empty(schema),
        snapshot_seq: SequenceNumber::from(0u64),
        head_seq: SequenceNumber::from(0u64),
        snapshot_len: 0,
    };

    // An error checking for the snapshot is not a missing snapshot: creating an empty one in its
    // place would lose the room's contents.
    let snap_exists = fail_point::io_result(
        "recovery.snapshot_exists",
        snap_path,
        files.exists(snap_path).await,
    )?;
    if !snap_exists {
        // The rename would replace a snapshot, but there is none: `exists` said so, and the
        // room's lock keeps out any other engine that could create one.
        write_empty_snapshot_file(files, snap_path, room_id).await?;
        return Ok(base);
    }

    // Closed when the payload is read, before anything renames a snapshot over it.
    let snap_file = files.open_read(snap_path).await?;
    let snap_file_len = snap_file.size().await?;

    if snap_file_len < HEADER_SIZE as u64 {
        return Err(StorageError::WalCorruption(format!(
            "Snapshot file size {snap_file_len} is smaller than minimum header size {HEADER_SIZE}"
        )));
    }

    let read = snap_file.read_at(0, HEADER_SIZE).await;
    let header_bytes = fail_point::io_result("recovery.snapshot_header", snap_path, read)?;
    let header_bytes: [u8; HEADER_SIZE] = header_bytes.try_into().map_err(|_| {
        std::io::Error::other("the file layer read a snapshot header of another length")
    })?;

    let header = FileHeader::decode(&header_bytes)?;
    base.snapshot_len = header.snapshot_compressed_len;
    base.snapshot_seq = SequenceNumber::from(header.snapshot_seq);
    base.head_seq = SequenceNumber::from(header.head_seq);
    if base.snapshot_len == 0 {
        return Ok(base);
    }

    let expected_total = HEADER_SIZE as u64 + base.snapshot_len;
    if snap_file_len < expected_total {
        return Err(StorageError::SnapshotCorruption(format!(
            "Snapshot file truncated: expected length {expected_total}, actual {snap_file_len}"
        )));
    }

    let read = snap_file
        .read_at(HEADER_SIZE as u64, base.snapshot_len as usize)
        .await;
    let compressed_snap = fail_point::io_result("recovery.snapshot_body", snap_path, read)?;
    drop(snap_file);

    // Validate compressed snapshot payload CRC32 before decompression
    let actual_payload_crc = crc32fast::hash(&compressed_snap);
    if header.snapshot_payload_crc32 != 0 && header.snapshot_payload_crc32 != actual_payload_crc {
        return Err(StorageError::SnapshotCorruption(format!(
            "Snapshot payload CRC32 mismatch: expected {}, got {}",
            header.snapshot_payload_crc32, actual_payload_crc
        )));
    }

    // Decompressed and decoded together on the blocking pool. The file was written by this
    // engine from a validated state, and its CRC matched, so its rows are not validated again
    // and its size is not limited. They are still checked against the constraints of `schema`
    // below, which may not be those it was written under.
    let mut payload: RoomSnapshotPayload =
        run_blocking(move || crate::snapshot::deserialize_zstd(&compressed_snap, u64::MAX, None))
            .await?;

    for (&table_id, table) in &payload.tables {
        if !table.is_empty() {
            ensure_known_table(room_id, schema, table_id)?;
        }
    }
    payload
        .tables
        .retain(|table_id, _| schema.has_table_by_id(*table_id));
    payload.fill_missing_tables(schema);
    let schema = schema.clone();
    let data = run_blocking(move || RoomData::build_checked(&schema, payload.tables)).await?;
    Ok(BaseSnapshot {
        data,
        snapshot_seq: payload.head_seq,
        head_seq: payload.head_seq,
        snapshot_len: base.snapshot_len,
    })
}

/// Removes the temporary snapshot files next to `snap_path` (`room_{id}.snap.tmp.*`) left by
/// interrupted compactions and snapshot installs.
///
/// The cleanup is best effort: if the directory cannot be listed, the files stay until a later
/// open removes them. Failing to remove a file that was found is an error.
async fn remove_staged_snapshots(
    files: &impl RoomFiles,
    snap_path: &Path,
) -> Result<(), StorageError> {
    let Some(parent) = snap_path.parent() else {
        return Ok(());
    };
    let snap_file_name = snap_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    let tmp_prefix = format!("{snap_file_name}.tmp.");
    let Ok(staged) = files.list(parent, &tmp_prefix).await else {
        return Ok(());
    };
    for path in staged {
        remove_if_exists(files, &path).await?;
    }
    Ok(())
}

/// Replays the records of a rotated WAL (`room_{id}.wal.compacting`) left by an interrupted
/// compaction beyond the state recovered so far, and returns whether it held any such record.
async fn replay_compacting_wal(
    files: &impl RoomFiles,
    room_id: &RoomId,
    wal_compacting_path: &Path,
    schema: &Schema,
    head_seq: &mut SequenceNumber,
    data: &mut RoomData,
) -> Result<bool, StorageError> {
    let compacting_file = files.open_read(wal_compacting_path).await?;
    let outcome = replay_wal_file(room_id, &compacting_file, schema, head_seq, data).await?;
    if let Some(reason) = outcome.torn_write {
        tracing::warn!(
            room_id = %room_id,
            reason = %reason,
            "Torn write at the end of the rotated WAL, which is removed once folded"
        );
    }
    Ok(outcome.applied_count > 0)
}

/// Recovers a room from its files:
/// 1. Loads the base snapshot (`room_{id}.snap`), or creates an empty one for a new room, and
///    removes the temporary snapshot files and the copy of an interrupted absorption
///    (`room_{id}.wal.compacting.tmp`) left behind.
/// 2. If a pre-crash rotated WAL (`room_{id}.wal.compacting`) exists, replays the records it
///    holds beyond the snapshot.
/// 3. Reads and replays all framed WAL batches from `wal_path` (`room_{id}.wal`),
///    applying mutations on top of tables and advancing `head_seq`, and truncates any
///    incomplete torn write at WAL EOF.
/// 4. Folds `.wal.compacting` away without ever rewriting a file in place: if it held live
///    records, writes a fresh snapshot of the recovered state through the atomic path, then
///    empties the active WAL; only then removes `.wal.compacting`. A crash at any step leaves
///    files that a later recovery replays to the same state.
#[tracing::instrument(skip(files, schema, wal_file), fields(room_id = %room_id, snap_path = ?snap_path, wal_path = ?wal_path))]
pub(crate) async fn recover_room<F: RoomFiles>(
    files: &F,
    room_id: &RoomId,
    snap_path: &Path,
    wal_path: &Path,
    schema: &Schema,
    wal_file: F::File,
    zstd_level: i32,
) -> Result<RecoveredRoom<F::File>, StorageError> {
    // 1. Load the base snapshot
    let BaseSnapshot {
        mut data,
        mut snapshot_seq,
        mut head_seq,
        mut snapshot_len,
    } = load_snapshot(files, room_id, snap_path, schema).await?;
    fail_point::pause("recovery.snapshot_loaded", wal_path).await;
    remove_staged_snapshots(files, snap_path).await?;
    // Its records are all in `.wal.compacting` and the active WAL, which it never replaced.
    remove_if_exists(files, &absorbing_wal_path(wal_path)).await?;

    // 2. Replay a pre-crash rotated WAL (`room_{id}.wal.compacting`), if any
    let wal_compacting_path = compacting_wal_path(wal_path);
    let compacting_exists = files.exists(&wal_compacting_path).await?;
    let compacting_has_live_records = compacting_exists
        && replay_compacting_wal(
            files,
            room_id,
            &wal_compacting_path,
            schema,
            &mut head_seq,
            &mut data,
        )
        .await?;

    // 3. Replay append-only WAL batches from wal_path
    let outcome = replay_wal_file(room_id, &wal_file, schema, &mut head_seq, &mut data).await?;
    let mut valid_wal_bytes = outcome.valid_bytes;

    // Truncate torn write at WAL EOF if detected
    if let Some(ref reason) = outcome.torn_write {
        tracing::warn!(
            room_id = %room_id,
            reason = %reason,
            "Torn write detected at WAL EOF, truncating damaged bytes to {valid_wal_bytes}"
        );
        wal_file.set_len(valid_wal_bytes).await?;
    }
    // An earlier open may have written records whose sync failed: none is served before this
    // process syncs them. What `.wal.compacting` holds is synced below, in the folded snapshot.
    fail_point::check("recovery.wal_sync", wal_path)?;
    wal_file.sync_all().await?;
    fail_point::panic_point("recovery.replayed", wal_path);

    // 4. Fold the rotated WAL into a fresh snapshot, then remove it
    if compacting_exists {
        if compacting_has_live_records {
            snapshot_len = write_snapshot_file(
                files,
                snap_path,
                head_seq,
                data.tables(),
                zstd_level,
                room_id,
            )
            .await?;
            snapshot_seq = head_seq;
            fail_point::check("recovery.fold", wal_path)?;

            // The snapshot now holds every record of the active WAL as well.
            wal_file.set_len(0).await?;
            wal_file.sync_all().await?;
            valid_wal_bytes = 0;
        }

        fail_point::check("recovery.fold_cleanup", wal_path)?;
        remove_if_exists(files, &wal_compacting_path).await?;
        sync_parent(files, &wal_compacting_path).await?;
    }

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
        wal_len: valid_wal_bytes,
        data,
    })
}

#[cfg(test)]
#[path = "tests/recovery.rs"]
mod tests;
