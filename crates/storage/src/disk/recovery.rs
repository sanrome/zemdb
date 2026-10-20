use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader};

use zemdb_core::{
    decode_batch_payload, find_valid_frame, parse_batch_header, BatchPayloadDecode, FrameAnomaly,
    RoomId, Schema, SequenceNumber, BATCH_HEADER_SIZE,
};

use crate::disk::compactor::{
    absorbing_wal_path, compacting_wal_path, remove_if_exists, sync_file, sync_parent,
    write_empty_snapshot_file, write_snapshot_file, SyncKind,
};
use crate::disk::format::{FileHeader, HEADER_SIZE};
use crate::error::StorageError;
use crate::fail_point;
use crate::memory::state::{apply_operation, RoomData, RoomSnapshotPayload};
use crate::sys::run_blocking;

/// Recovery result containing the reconstructed in-memory state and the open WAL file handle.
#[derive(Debug)]
pub(crate) struct RecoveredRoom {
    pub wal_file: tokio::fs::File,
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

/// Reads until `buf` is full or the reader reaches its end, and returns the bytes read. A single
/// `read` may return fewer bytes than are left (for example at the edge of a buffer), so a short
/// read alone does not mean the end of the file.
async fn read_full<R: AsyncRead + Unpin>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

/// Classifies an anomaly found at `frame_start` of `file` (see [`FrameAnomaly`]), searching
/// the rest of the file for a valid frame on a blocking thread. Returns the reason of a torn
/// write, or the corruption as an error.
async fn classify_anomaly(
    file: &tokio::fs::File,
    frame_start: u64,
    file_len: u64,
    anomaly: FrameAnomaly,
) -> Result<String, StorageError> {
    let from = frame_start.saturating_add(anomaly.search_offset());
    let following = if file_len.saturating_sub(from) < BATCH_HEADER_SIZE as u64 {
        None
    } else {
        // The clone shares the file position, which the caller sets again before any other use.
        let mut search_file = file.try_clone().await?.into_std().await;
        run_blocking(move || find_valid_frame(&mut search_file, from, file_len)).await?
    };
    Ok(anomaly.classify(following)?)
}

/// Replays the framed batches of one WAL file on top of `data`, advancing `head_seq`.
///
/// The file is streamed rather than read into memory, and each frame is decoded with the
/// primitives of `zemdb_core::protocol::wal_frame`, so a torn write at the end of the file is
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
    file: &mut tokio::fs::File,
    schema: &Schema,
    head_seq: &mut SequenceNumber,
    data: &mut RoomData,
) -> Result<WalReplayOutcome, StorageError> {
    let mut valid_bytes: u64 = 0;
    let mut applied_count: usize = 0;

    let file_len = file.metadata().await?.len();
    file.seek(SeekFrom::Start(0)).await?;
    let mut reader = BufReader::with_capacity(64 * 1024, &mut *file);

    let anomaly = loop {
        let mut header_buf = [0u8; BATCH_HEADER_SIZE];
        let header_read = read_full(&mut reader, &mut header_buf).await?;
        if header_read == 0 {
            break None;
        }
        if header_read < BATCH_HEADER_SIZE {
            break Some(FrameAnomaly::IncompleteHeader {
                available: header_read,
            });
        }

        let header = match parse_batch_header(&header_buf) {
            Ok(header) => header,
            Err(invalid) => break Some(FrameAnomaly::InvalidHeader(invalid)),
        };

        // Checked before reading so that a frame cut short does not allocate its whole length.
        let available = file_len - valid_bytes;
        if available < header.frame_len() as u64 {
            break Some(FrameAnomaly::TruncatedPayload {
                available,
                frame_len: header.frame_len(),
            });
        }

        let mut payload = vec![0u8; header.payload_len()];
        reader.read_exact(&mut payload).await?;

        let ops = match decode_batch_payload(&header, &payload)? {
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
/// CRC32 of the compressed payload before decompressing it. A table with rows that `schema`
/// does not have is `SchemaMismatch`; an empty one is dropped (a snapshot holds every table of
/// the schema it was compacted with, empty ones included). The indexes of the constraints of
/// `schema` are then built from its rows, on the blocking pool.
///
/// A new room has none: an empty one is staged in a temporary file and renamed into place, so a
/// failure or crash never leaves a partial snapshot behind.
async fn load_snapshot(
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
        tokio::fs::try_exists(snap_path).await,
    )?;
    if !snap_exists {
        // The rename would replace a snapshot, but there is none: `try_exists` said so, and the
        // room's lock keeps out any other engine that could create one.
        write_empty_snapshot_file(snap_path, room_id).await?;
        return Ok(base);
    }

    let snap_file = tokio::fs::File::from_std(std::fs::File::open(snap_path)?);
    let snap_file_len = snap_file.metadata().await?.len();

    if snap_file_len < HEADER_SIZE as u64 {
        return Err(StorageError::WalCorruption(format!(
            "Snapshot file size {snap_file_len} is smaller than minimum header size {HEADER_SIZE}"
        )));
    }

    let mut snap_reader = BufReader::with_capacity(64 * 1024, snap_file);
    let mut header_bytes = [0u8; HEADER_SIZE];
    snap_reader
        .read_exact(&mut header_bytes)
        .await
        .map_err(|e| {
            StorageError::WalCorruption(format!("Failed to read snapshot file header: {e}"))
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

    let mut compressed_snap = vec![0u8; base.snapshot_len as usize];
    snap_reader
        .read_exact(&mut compressed_snap)
        .await
        .map_err(|e| {
            StorageError::SnapshotCorruption(format!("Failed to read snapshot bytes: {e}"))
        })?;

    // Validate compressed snapshot payload CRC32 before decompression
    let actual_payload_crc = crc32fast::hash(&compressed_snap);
    if header.snapshot_payload_crc32 != 0 && header.snapshot_payload_crc32 != actual_payload_crc {
        return Err(StorageError::SnapshotCorruption(format!(
            "Snapshot payload CRC32 mismatch: expected {}, got {}",
            header.snapshot_payload_crc32, actual_payload_crc
        )));
    }

    // Decompressed and decoded together on the blocking pool. The file was written by this
    // engine from a validated state, and its CRC matched, so its rows are not checked again and
    // its size is not limited.
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
    let data =
        run_blocking(move || Ok::<_, StorageError>(RoomData::build(&schema, payload.tables)))
            .await?;
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
async fn remove_staged_snapshots(snap_path: &Path) -> Result<(), StorageError> {
    let Some(parent) = snap_path.parent() else {
        return Ok(());
    };
    let Ok(mut entries) = tokio::fs::read_dir(parent).await else {
        return Ok(());
    };
    let snap_file_name = snap_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    let tmp_prefix = format!("{snap_file_name}.tmp.");
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Ok(name) = entry.file_name().into_string() {
            if name.starts_with(&tmp_prefix) {
                remove_if_exists(&entry.path()).await?;
            }
        }
    }
    Ok(())
}

/// Replays the records of a rotated WAL (`room_{id}.wal.compacting`) left by an interrupted
/// compaction beyond the state recovered so far, and returns whether it held any such record.
async fn replay_compacting_wal(
    room_id: &RoomId,
    wal_compacting_path: &Path,
    schema: &Schema,
    head_seq: &mut SequenceNumber,
    data: &mut RoomData,
) -> Result<bool, StorageError> {
    let mut compacting_file = tokio::fs::File::from_std(std::fs::File::open(wal_compacting_path)?);
    let outcome = replay_wal_file(room_id, &mut compacting_file, schema, head_seq, data).await?;
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
/// 3. Streams and replays all framed WAL batches from `wal_path` (`room_{id}.wal`),
///    applying mutations on top of tables and advancing `head_seq`, and truncates any
///    incomplete torn write at WAL EOF.
/// 4. Folds `.wal.compacting` away without ever rewriting a file in place: if it held live
///    records, writes a fresh snapshot of the recovered state through the atomic path, then
///    empties the active WAL; only then removes `.wal.compacting`. A crash at any step leaves
///    files that a later recovery replays to the same state.
#[tracing::instrument(skip(schema, wal_file_std), fields(room_id = %room_id, snap_path = ?snap_path, wal_path = ?wal_path))]
pub(crate) async fn recover_room(
    room_id: &RoomId,
    snap_path: &Path,
    wal_path: &Path,
    schema: &Schema,
    wal_file_std: std::fs::File,
    zstd_level: i32,
) -> Result<RecoveredRoom, StorageError> {
    // 1. Load the base snapshot
    let BaseSnapshot {
        mut data,
        mut snapshot_seq,
        mut head_seq,
        mut snapshot_len,
    } = load_snapshot(room_id, snap_path, schema).await?;
    fail_point::pause("recovery.snapshot_loaded", wal_path).await;
    remove_staged_snapshots(snap_path).await?;
    // Its records are all in `.wal.compacting` and the active WAL, which it never replaced.
    remove_if_exists(&absorbing_wal_path(wal_path)).await?;

    // 2. Replay a pre-crash rotated WAL (`room_{id}.wal.compacting`), if any
    let wal_compacting_path = compacting_wal_path(wal_path);
    let compacting_exists = tokio::fs::try_exists(&wal_compacting_path).await?;
    let compacting_has_live_records = compacting_exists
        && replay_compacting_wal(
            room_id,
            &wal_compacting_path,
            schema,
            &mut head_seq,
            &mut data,
        )
        .await?;

    // 3. Replay append-only WAL batches from wal_path
    let mut wal_file = tokio::fs::File::from_std(wal_file_std);
    let outcome = replay_wal_file(room_id, &mut wal_file, schema, &mut head_seq, &mut data).await?;
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
    sync_file(&mut wal_file, SyncKind::All).await?;
    fail_point::panic_point("recovery.replayed", wal_path);

    // 4. Fold the rotated WAL into a fresh snapshot, then remove it
    if compacting_exists {
        if compacting_has_live_records {
            snapshot_len =
                write_snapshot_file(snap_path, head_seq, data.tables(), zstd_level, room_id)
                    .await?;
            snapshot_seq = head_seq;
            fail_point::check("recovery.fold", wal_path)?;

            // The snapshot now holds every record of the active WAL as well.
            wal_file.set_len(0).await?;
            sync_file(&mut wal_file, SyncKind::All).await?;
            valid_wal_bytes = 0;
        }

        fail_point::check("recovery.fold_cleanup", wal_path)?;
        remove_if_exists(&wal_compacting_path).await?;
        sync_parent(&wal_compacting_path).await?;
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
        wal_len: valid_wal_bytes,
        data,
    })
}

#[cfg(test)]
#[path = "tests/recovery.rs"]
mod tests;
