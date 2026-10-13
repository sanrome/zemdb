use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader};

use zemdb_core::{
    decode_batch_payload, find_valid_frame, parse_batch_header, BatchPayloadDecode, FrameAnomaly,
    RoomId, Schema, SequenceNumber, WalFrameError, BATCH_HEADER_SIZE,
};

use crate::disk::compactor::{
    compacting_wal_path, remove_if_exists, sync_file, sync_parent, write_empty_snapshot_file,
    write_snapshot_file, SyncKind,
};
pub use crate::disk::format::replay_wal_records;
use crate::disk::format::{FileHeader, HEADER_SIZE};
use crate::error::StorageError;
use crate::fail_point;
use crate::memory::state::{apply_operation, empty_tables, RoomSnapshotPayload};
use crate::memory::Table;
use crate::sys::run_blocking;

/// Recovery result containing the reconstructed in-memory state and the open WAL file handle.
#[derive(Debug)]
pub struct RecoveredRoom {
    pub wal_file: tokio::fs::File,
    pub head_seq: SequenceNumber,
    pub snapshot_seq: SequenceNumber,
    pub snapshot_len: u64,
    pub wal_len: u64,
    pub tables: HashMap<u16, Table>,
}

struct WalReplayOutcome {
    valid_bytes: u64,
    end: ReplayEnd,
    applied_count: usize,
}

/// Where the replay of a WAL file stopped.
enum ReplayEnd {
    /// At the end of the file.
    Clean,
    /// At a frame that could not be read and has no valid frame after it.
    TornWrite(String),
    /// At a frame that could not be read and has a valid frame after it, at `following`:
    /// corruption, unless the records after it are recovered from elsewhere (see
    /// `recover_room`).
    FollowedByFrame {
        corruption: WalFrameError,
        following: u64,
    },
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
/// the rest of the file for a valid frame on a blocking thread.
async fn classify_anomaly(
    file: &tokio::fs::File,
    frame_start: u64,
    file_len: u64,
    anomaly: FrameAnomaly,
) -> Result<ReplayEnd, StorageError> {
    let from = frame_start.saturating_add(anomaly.search_offset());
    let following = if file_len.saturating_sub(from) < BATCH_HEADER_SIZE as u64 {
        None
    } else {
        // The clone shares the file position, which the caller sets again before any other use.
        let mut search_file = file.try_clone().await?.into_std().await;
        run_blocking(move || find_valid_frame(&mut search_file, from, file_len)).await?
    };
    Ok(match (anomaly.classify(following), following) {
        (Err(corruption), Some(following)) => ReplayEnd::FollowedByFrame {
            corruption,
            following,
        },
        (Err(corruption), None) => return Err(corruption.into()),
        (Ok(reason), _) => ReplayEnd::TornWrite(reason),
    })
}

/// Highest sequence number in the valid frames of `file` from `from` to `end`, skipping any
/// damage between them as [`find_valid_frame`] does. A frame taken as valid without checking
/// its payload yields `u64::MAX`: its records are unknown, so nothing can cover them.
fn highest_seq_in_valid_frames(
    file: &mut std::fs::File,
    mut from: u64,
    end: u64,
) -> Result<Option<SequenceNumber>, StorageError> {
    use std::io::{Read, Seek};

    let mut highest = None;
    while let Some(position) = find_valid_frame(file, from, end)? {
        let mut header = [0u8; BATCH_HEADER_SIZE];
        file.seek(SeekFrom::Start(position))?;
        file.read_exact(&mut header)?;
        let header = parse_batch_header(&header)
            .map_err(|e| StorageError::WalCorruption(format!("Invalid WAL batch header: {e}")))?;
        let mut payload = vec![0u8; header.payload_len()];
        file.read_exact(&mut payload)?;
        let frame_highest = match decode_batch_payload(&header, &payload)? {
            BatchPayloadDecode::Batch { ops, .. } => ops.iter().map(|op| op.seq).max(),
            BatchPayloadDecode::ChecksumMismatch { .. } => Some(SequenceNumber::from(u64::MAX)),
        };
        highest = highest.max(frame_highest);
        from = position + header.frame_len() as u64;
    }
    Ok(highest)
}

/// Replays the framed batches of one WAL file on top of `tables`, advancing `head_seq`.
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
/// means lost data, and is reported as corruption.
async fn replay_wal_file(
    file: &mut tokio::fs::File,
    schema: &Schema,
    head_seq: &mut SequenceNumber,
    tables: &mut HashMap<u16, Table>,
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

            if schema.has_table_by_id(op.op.table_id()) {
                apply_operation(tables, schema, op.op);
            }

            *head_seq = op.seq;
            applied_count += 1;
        }

        valid_bytes += header.frame_len() as u64;
    };

    let end = match anomaly {
        Some(anomaly) => classify_anomaly(file, valid_bytes, file_len, anomaly).await?,
        None => ReplayEnd::Clean,
    };

    Ok(WalReplayOutcome {
        valid_bytes,
        end,
        applied_count,
    })
}

/// Replays a room from disk following the Dual-File architecture:
/// 1. Reads the immutable base snapshot from `snap_path` (`room_{id}.snap`),
///    verifying header and compressed payload CRC32 checksums before decompressing. A new
///    room has none: an empty one is staged in a temporary file and renamed into place, so a
///    failure or crash never leaves a partial snapshot behind.
/// 2. If a pre-crash rotated WAL (`room_{id}.wal.compacting`) exists, replays the records
///    it holds beyond the snapshot. A damaged frame there followed by valid ones is accepted
///    only if the active WAL holds the records after it, as a crash while absorbing leaves.
/// 3. Streams and replays all framed WAL batches from `wal_path` (`room_{id}.wal`),
///    applying mutations on top of tables and advancing `head_seq`, and truncates any
///    incomplete torn write at WAL EOF.
/// 4. Folds `.wal.compacting` away without ever rewriting a file in place: if it held live
///    records, writes a fresh snapshot of the recovered state through the atomic path, then
///    empties the active WAL; only then removes `.wal.compacting`. A crash at any step leaves
///    files that a later recovery replays to the same state.
#[tracing::instrument(skip(schema, wal_file_std), fields(room_id = %room_id, snap_path = ?snap_path, wal_path = ?wal_path))]
pub async fn recover_room(
    room_id: &RoomId,
    snap_path: &Path,
    wal_path: &Path,
    schema: &Schema,
    wal_file_std: std::fs::File,
    zstd_level: i32,
) -> Result<RecoveredRoom, StorageError> {
    let mut tables = empty_tables(schema);

    let mut snapshot_seq = SequenceNumber::from(0u64);
    let mut head_seq = SequenceNumber::from(0u64);
    let mut snapshot_len = 0u64;

    // 1. Recover base snapshot from snap_path if it exists. An error checking for it is not a
    // missing snapshot: creating an empty one in its place would lose the room's contents.
    let snap_exists = fail_point::io_result(
        "recovery.snapshot_exists",
        snap_path,
        tokio::fs::try_exists(snap_path).await,
    )?;
    if snap_exists {
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
            snap_reader
                .read_exact(&mut compressed_snap)
                .await
                .map_err(|e| {
                    StorageError::SnapshotCorruption(format!("Failed to read snapshot bytes: {e}"))
                })?;

            // Validate compressed snapshot payload CRC32 before decompression
            let actual_payload_crc = crc32fast::hash(&compressed_snap);
            if header.snapshot_payload_crc32 != 0
                && header.snapshot_payload_crc32 != actual_payload_crc
            {
                return Err(StorageError::SnapshotCorruption(format!(
                    "Snapshot payload CRC32 mismatch: expected {}, got {}",
                    header.snapshot_payload_crc32, actual_payload_crc
                )));
            }

            // Decompressed and decoded together on the blocking pool. The file was written by
            // this engine from a validated state, and its CRC matched, so its rows are not
            // checked again and its size is not limited.
            let mut payload: RoomSnapshotPayload = run_blocking(move || {
                crate::snapshot::deserialize_zstd(&compressed_snap, u64::MAX, None)
            })
            .await?;

            snapshot_seq = payload.head_seq;
            head_seq = payload.head_seq;

            payload.fill_missing_tables(schema);
            tables = payload.tables;
        }
    } else {
        // Create the initial empty snapshot. The rename would replace a snapshot, but there is
        // none: `try_exists` said so, and the room's lock keeps out any other engine that
        // could create one.
        write_empty_snapshot_file(snap_path, room_id).await?;
    }

    // Clean up any lingering temporary snapshot files from interrupted compactions
    if let Some(parent) = snap_path.parent() {
        if let Ok(mut entries) = tokio::fs::read_dir(parent).await {
            let snap_file_name = snap_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            let tmp_prefix = format!("{}.tmp.", snap_file_name);
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(name) = entry.file_name().into_string() {
                    if name.starts_with(&tmp_prefix) {
                        remove_if_exists(&entry.path()).await?;
                    }
                }
            }
        }
    }

    // 2. Replay a pre-crash rotated WAL (`room_{id}.wal.compacting`), if any
    let wal_compacting_path = compacting_wal_path(wal_path);
    let compacting_exists = tokio::fs::try_exists(&wal_compacting_path).await?;
    let mut compacting_has_live_records = false;
    let mut unconfirmed_compacting_tail = None;

    if compacting_exists {
        let mut compacting_file =
            tokio::fs::File::from_std(std::fs::File::open(&wal_compacting_path)?);
        let outcome =
            replay_wal_file(&mut compacting_file, schema, &mut head_seq, &mut tables).await?;
        compacting_has_live_records = outcome.applied_count > 0;
        match outcome.end {
            ReplayEnd::Clean => {}
            ReplayEnd::TornWrite(reason) => tracing::warn!(
                room_id = %room_id,
                reason = %reason,
                "Torn write at the end of the rotated WAL, which is removed once folded"
            ),
            // Absorbing the active WAL appends all of it with a single sync and only empties it
            // afterwards, so a crash before that sync can leave a damaged frame followed by
            // valid ones, all of whose records are still in the active WAL. A crash while
            // folding them leaves the same file, with its records in the snapshot instead. The
            // damage is accepted only if the recovered state ends up covering every record of
            // the valid frames after it.
            ReplayEnd::FollowedByFrame {
                corruption,
                following,
            } => {
                let end = compacting_file.metadata().await?.len();
                let mut search_file = compacting_file.try_clone().await?.into_std().await;
                let highest = run_blocking(move || {
                    highest_seq_in_valid_frames(&mut search_file, following, end)
                })
                .await?;
                unconfirmed_compacting_tail = Some((corruption, highest));
            }
        }
    }

    // 3. Replay append-only WAL batches from wal_path
    let mut wal_file = tokio::fs::File::from_std(wal_file_std);
    let outcome = replay_wal_file(&mut wal_file, schema, &mut head_seq, &mut tables).await?;
    let torn_write = match outcome.end {
        ReplayEnd::Clean => None,
        ReplayEnd::TornWrite(reason) => Some(reason),
        ReplayEnd::FollowedByFrame { corruption, .. } => return Err(corruption.into()),
    };

    // The replay rejects a gap, so reaching the highest record after the damage means every
    // record up to it, the damaged ones included, came from the snapshot or the active WAL.
    if let Some((corruption, highest)) = unconfirmed_compacting_tail {
        if highest.is_some_and(|highest| highest > head_seq) {
            return Err(corruption.into());
        }
        tracing::warn!(
            room_id = %room_id,
            reason = %corruption,
            "Damaged tail of the rotated WAL is covered by the snapshot and the active WAL"
        );
    }

    let mut valid_wal_bytes = outcome.valid_bytes;

    // Truncate torn write at WAL EOF if detected
    if let Some(ref reason) = torn_write {
        tracing::warn!(
            room_id = %room_id,
            reason = %reason,
            "Torn write detected at WAL EOF, truncating damaged bytes to {valid_wal_bytes}"
        );
        wal_file.set_len(valid_wal_bytes).await?;
        sync_file(&mut wal_file, SyncKind::All).await?;
    }

    // 4. Fold the rotated WAL into a fresh snapshot, then remove it
    if compacting_exists {
        if compacting_has_live_records {
            snapshot_len =
                write_snapshot_file(snap_path, head_seq, &tables, zstd_level, room_id).await?;
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
        tables,
    })
}

#[cfg(test)]
#[path = "tests/recovery.rs"]
mod tests;
