use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader};

use zemdb_core::{
    classify_checksum_mismatch, classify_zeroed_header, decode_batch_payload, parse_batch_header,
    BatchPayloadDecode, RoomId, Schema, SequenceNumber, BATCH_HEADER_SIZE,
};

use crate::disk::compactor::{
    compacting_wal_path, remove_if_exists, sync_file, sync_parent, write_empty_snapshot_file,
    write_snapshot_file, SyncKind,
};
pub use crate::disk::format::replay_wal_records;
use crate::disk::format::{FileHeader, HEADER_SIZE};
use crate::error::StorageError;
use crate::fail_point;
use crate::memory::state::{apply_operation, empty_tables};
use crate::memory::{RoomSnapshotPayload, Table};

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

/// Reads the reader to its end, returning whether every byte was zero and how many there were.
async fn rest_is_zero<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<(bool, u64)> {
    let mut chunk = [0u8; 8 * 1024];
    let mut all_zero = true;
    let mut len = 0u64;
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            return Ok((all_zero, len));
        }
        all_zero &= chunk[..n].iter().all(|&b| b == 0);
        len += n as u64;
    }
}

/// Replays the framed batches of one WAL file on top of `tables`, advancing `head_seq`.
///
/// The file is streamed rather than read into memory, and each frame is decoded with the
/// primitives of `zemdb_core::protocol::wal_frame`, so a torn write at the end of the file is
/// told apart from corruption exactly as `decode_wal_batch_from_slice` does: an incomplete
/// frame, a zero-filled tail, or a checksum mismatch not followed by a complete frame header
/// ends the valid part of the file; anything else is corruption.
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
    let mut torn_write: Option<String> = None;
    let mut applied_count: usize = 0;

    file.seek(SeekFrom::Start(0)).await?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);

    loop {
        let mut header_buf = [0u8; BATCH_HEADER_SIZE];
        let header_read = read_full(&mut reader, &mut header_buf).await?;
        if header_read == 0 {
            break;
        }
        if header_read < BATCH_HEADER_SIZE {
            torn_write = Some(format!(
                "Incomplete WAL batch header: available {header_read} bytes, expected at least {BATCH_HEADER_SIZE}"
            ));
            break;
        }

        let Some(header) = parse_batch_header(&header_buf)? else {
            let (tail_is_zero, rest_len) = rest_is_zero(&mut reader).await?;
            torn_write = Some(classify_zeroed_header(
                tail_is_zero,
                BATCH_HEADER_SIZE as u64 + rest_len,
            )?);
            break;
        };

        let mut payload = vec![0u8; header.payload_len()];
        let payload_read = read_full(&mut reader, &mut payload).await?;
        if payload_read < payload.len() {
            torn_write = Some(format!(
                "Truncated WAL batch payload: available {payload_read} bytes, expected {}",
                payload.len()
            ));
            break;
        }

        let ops = match decode_batch_payload(&header, &payload)? {
            BatchPayloadDecode::Batch { ops, .. } => ops,
            BatchPayloadDecode::ChecksumMismatch { expected, actual } => {
                let mut following = [0u8; BATCH_HEADER_SIZE];
                let following_len = read_full(&mut reader, &mut following).await?;
                torn_write = Some(classify_checksum_mismatch(
                    expected,
                    actual,
                    &following[..following_len],
                )?);
                break;
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
    }

    Ok(WalReplayOutcome {
        valid_bytes,
        torn_write,
        applied_count,
    })
}

/// Replays a room from disk following the Dual-File architecture:
/// 1. Reads the immutable base snapshot from `snap_path` (`room_{id}.snap`),
///    verifying header and compressed payload CRC32 checksums before decompressing. A new
///    room has none: an empty one is staged in a temporary file and renamed into place, so a
///    failure or crash never leaves a partial snapshot behind.
/// 2. If a pre-crash rotated WAL (`room_{id}.wal.compacting`) exists, replays the records
///    it holds beyond the snapshot.
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
        let snap_std = std::fs::OpenOptions::new().read(true).open(snap_path)?;
        let snap_file = tokio::fs::File::from_std(snap_std);
        let snap_meta = snap_file.metadata().await?;
        let snap_file_len = snap_meta.len();

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

            let decompressed =
                tokio::task::spawn_blocking(move || zstd::decode_all(&compressed_snap[..]))
                    .await
                    .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
                    .map_err(|e| {
                        StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}"))
                    })?;

            let mut payload: RoomSnapshotPayload = bincode::deserialize(&decompressed)
                .map_err(|e| StorageError::SnapshotCorruption(e.to_string()))?;

            snapshot_seq = payload.head_seq;
            head_seq = payload.head_seq;

            payload.fill_missing_tables(schema);
            tables = payload.tables;
        }
    } else {
        // Create the initial empty snapshot. The rename would replace a snapshot, but there is
        // none: `try_exists` said so, and the room's exclusive WAL lock keeps out any other
        // engine that could create one.
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

    if compacting_exists {
        let std_compacting = std::fs::OpenOptions::new()
            .read(true)
            .open(&wal_compacting_path)?;
        let mut compacting_file = tokio::fs::File::from_std(std_compacting);
        let outcome =
            replay_wal_file(&mut compacting_file, schema, &mut head_seq, &mut tables).await?;
        compacting_has_live_records = outcome.applied_count > 0;
    }

    // 3. Replay append-only WAL batches from wal_path
    let mut wal_file = tokio::fs::File::from_std(wal_file_std);
    let outcome = replay_wal_file(&mut wal_file, schema, &mut head_seq, &mut tables).await?;

    let mut valid_wal_bytes = outcome.valid_bytes;

    // Truncate torn write at WAL EOF if detected
    if let Some(ref reason) = outcome.torn_write {
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
