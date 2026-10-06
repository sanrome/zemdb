//! Snapshot uploads: in one request, or in chunks written to a partial file. Both end by
//! promoting a verified file and installing it as the room's active snapshot.

use bytes::Bytes;
use dashmap::DashMap;
use std::future::Future;
use std::path::PathBuf;
use tokio::time::Instant;
use tracing::{info, warn};
use zemdb_core::id::{RoomId, SequenceNumber};
use zemdb_core::protocol::messages::ServerMessage;
use zemdb_core::protocol::snapshot_envelope::validate_snapshot_envelope;

use super::files::{
    promote, remove_file_if_exists, run_blocking, snapshot_file_name, sync_file, upload_file_name,
    verify_snapshot_file, write_at, write_new_file, VerifyError, PART_EXTENSION, TMP_EXTENSION,
};
use super::types::{
    LogBounds, SnapshotChunkUpload, Uploader, MAX_CHUNK_BYTES, MAX_PENDING_SINGLE_UPLOADS,
    MIN_CHUNK_BYTES, UPLOAD_IDLE_TIMEOUT,
};
use super::{SnapshotRelay, StagedSnapshot};
use crate::error::ServerError;

/// Chunk layout of a multipart upload: every chunk has `chunk_len` bytes except the last,
/// which holds the exact remainder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UploadLayout {
    total_bytes: u64,
    total_chunks: u32,
    chunk_len: u64,
}

impl UploadLayout {
    fn new(
        total_bytes: u64,
        total_chunks: u32,
        max_snapshot_bytes: u64,
    ) -> Result<Self, ServerError> {
        if total_chunks == 0 {
            return Err(ServerError::BadRequest(
                "total_chunks must be greater than 0".to_string(),
            ));
        }
        if total_bytes == 0 {
            return Err(ServerError::BadRequest(
                "total_bytes must be greater than 0".to_string(),
            ));
        }
        if total_bytes > max_snapshot_bytes {
            return Err(ServerError::BadRequest(format!(
                "Snapshot of {total_bytes} bytes exceeds the maximum of {max_snapshot_bytes} bytes"
            )));
        }
        let chunk_len = total_bytes.div_ceil(u64::from(total_chunks));
        if chunk_len > u64::from(MAX_CHUNK_BYTES) {
            return Err(ServerError::BadRequest(format!(
                "Upload chunks of {chunk_len} bytes exceed the maximum of {MAX_CHUNK_BYTES} bytes"
            )));
        }
        if total_chunks > 1 && chunk_len < u64::from(MIN_CHUNK_BYTES) {
            return Err(ServerError::BadRequest(format!(
                "Upload chunks of {chunk_len} bytes are below the minimum of {MIN_CHUNK_BYTES} \
                 bytes; upload smaller snapshots in fewer chunks"
            )));
        }
        if chunk_len * u64::from(total_chunks - 1) >= total_bytes {
            return Err(ServerError::BadRequest(format!(
                "{total_chunks} chunks of {chunk_len} bytes leave the last chunk of a \
                 {total_bytes}-byte snapshot empty"
            )));
        }
        Ok(Self {
            total_bytes,
            total_chunks,
            chunk_len,
        })
    }

    fn offset(&self, chunk_index: u32) -> u64 {
        u64::from(chunk_index) * self.chunk_len
    }

    fn len_of(&self, chunk_index: u32) -> u64 {
        if chunk_index + 1 == self.total_chunks {
            self.total_bytes - self.offset(chunk_index)
        } else {
            self.chunk_len
        }
    }
}

/// A multipart upload in progress. Its chunks are written to `path` at their offsets.
#[derive(Debug)]
pub(super) struct UploadSession {
    pub(super) head_seq: SequenceNumber,
    uploader: Uploader,
    layout: UploadLayout,
    snapshot_hash: [u8; 32],
    received: Vec<bool>,
    received_count: u32,
    last_chunk_at: Instant,
    pub(super) path: PathBuf,
}

impl UploadSession {
    /// Whether a new upload by `uploader` at this session's sequence, with other parameters,
    /// may replace it: the snapshot worker always may, and so may the session's own uploader.
    fn yields_to(&self, uploader: &Uploader) -> bool {
        *uploader == Uploader::Admin || self.uploader == *uploader
    }

    pub(super) fn is_idle(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_chunk_at) >= UPLOAD_IDLE_TIMEOUT
    }
}

/// Counts one single-request upload of a room for as long as it lives.
struct PendingUpload<'a> {
    pending: &'a DashMap<RoomId, usize>,
    room_id: RoomId,
}

impl<'a> PendingUpload<'a> {
    fn acquire(pending: &'a DashMap<RoomId, usize>, room_id: &RoomId) -> Result<Self, ServerError> {
        let mut count = pending.entry(room_id.clone()).or_insert(0);
        if *count >= MAX_PENDING_SINGLE_UPLOADS {
            return Err(ServerError::RateLimited);
        }
        *count += 1;
        Ok(Self {
            pending,
            room_id: room_id.clone(),
        })
    }
}

impl Drop for PendingUpload<'_> {
    fn drop(&mut self) {
        if let Some(mut count) = self.pending.get_mut(&self.room_id) {
            *count = count.saturating_sub(1);
        }
        self.pending
            .remove_if(&self.room_id, |_, count| *count == 0);
    }
}

impl SnapshotRelay {
    /// Stages a complete snapshot uploaded in one request and returns its BLAKE3 hash.
    ///
    /// `log_bounds` resolves to the room's retained log range; it is awaited after the request
    /// is tied to the room's current state, so a room deleted meanwhile is reported as not
    /// found instead of receiving a snapshot.
    pub async fn stage_snapshot(
        &self,
        room_id: &RoomId,
        head_seq: SequenceNumber,
        data: Bytes,
        log_bounds: impl Future<Output = Result<LogBounds, ServerError>>,
    ) -> Result<[u8; 32], ServerError> {
        check_seq_positive(head_seq)?;
        if data.len() as u64 > self.max_snapshot_bytes {
            return Err(ServerError::BadRequest(format!(
                "Snapshot of {} bytes exceeds the maximum of {} bytes",
                data.len(),
                self.max_snapshot_bytes
            )));
        }

        let _pending = PendingUpload::acquire(&self.pending_single_uploads, room_id)?;
        let slot = self.slot(room_id);
        let bounds = log_bounds.await?;
        bounds.check(head_seq)?;

        // Header, checksum and hash are computed before taking the room lock.
        let validated = data.clone();
        let snapshot_hash = run_blocking(move || {
            validate_snapshot_envelope(&validated)
                .map_err(|e| ServerError::BadRequest(format!("Invalid snapshot envelope: {e}")))?;
            Ok(ServerMessage::compute_snapshot_hash(&validated))
        })
        .await?;

        let mut guard = slot.lock().await;
        if guard.purged {
            return Err(room_deleted(room_id));
        }
        self.expire_locked(room_id, &mut guard).await;
        if self.is_active(room_id, head_seq, &snapshot_hash) {
            // A retry of an upload that already succeeded.
            return Ok(snapshot_hash);
        }
        self.check_newer_than_active(room_id, head_seq)?;

        let tmp_path = self
            .uploads_dir
            .join(upload_file_name(room_id, head_seq, TMP_EXTENSION));
        let final_path =
            self.snapshots_dir
                .join(snapshot_file_name(room_id, head_seq, &snapshot_hash));
        let total_bytes = data.len() as u64;
        let (uploads_dir, snapshots_dir) = (self.uploads_dir.clone(), self.snapshots_dir.clone());
        let promoted_path = final_path.clone();
        run_blocking(move || {
            let result = write_new_file(&tmp_path, &data)
                .and_then(|()| promote(&tmp_path, &promoted_path, &uploads_dir, &snapshots_dir));
            if result.is_err() {
                if let Err(err) = remove_file_if_exists(&tmp_path) {
                    warn!(path = ?tmp_path, error = %err, "Failed to delete snapshot upload after an error");
                }
            }
            result.map_err(ServerError::from)
        })
        .await?;

        self.install(
            room_id,
            &mut guard,
            StagedSnapshot {
                head_seq,
                total_bytes,
                snapshot_hash,
                staged_at: Instant::now(),
                path: final_path,
            },
        )
        .await;
        Ok(snapshot_hash)
    }

    /// Stages one chunk of a multipart upload. Returns `true` when the chunk completed the
    /// upload and the snapshot became the room's active snapshot.
    ///
    /// A room holds at most one upload: a chunk for a higher sequence replaces the upload in
    /// progress, one for a lower sequence (or for the same sequence with different
    /// parameters) is rejected. `log_bounds` is awaited for every chunk; the bounds are checked
    /// when the upload starts and again when it completes.
    pub async fn stage_chunk(
        &self,
        chunk: SnapshotChunkUpload,
        log_bounds: impl Future<Output = Result<LogBounds, ServerError>>,
    ) -> Result<bool, ServerError> {
        let SnapshotChunkUpload {
            room_id,
            uploader,
            head_seq,
            chunk_index,
            total_chunks,
            total_bytes,
            snapshot_hash,
            data,
        } = chunk;

        check_seq_positive(head_seq)?;
        let layout = UploadLayout::new(total_bytes, total_chunks, self.max_snapshot_bytes)?;
        if chunk_index >= total_chunks {
            return Err(ServerError::BadRequest(format!(
                "chunk_index {chunk_index} out of bounds for total_chunks {total_chunks}"
            )));
        }
        let expected_len = layout.len_of(chunk_index);
        if data.len() as u64 != expected_len {
            return Err(ServerError::BadRequest(format!(
                "Chunk {chunk_index} has {} bytes; expected exactly {expected_len}",
                data.len()
            )));
        }

        let slot = self.slot(&room_id);
        let bounds = log_bounds.await?;

        let mut guard = slot.lock().await;
        if guard.purged {
            return Err(room_deleted(&room_id));
        }
        self.expire_locked(&room_id, &mut guard).await;

        if self.is_active(&room_id, head_seq, &snapshot_hash) {
            // A chunk resent after the upload completed (its acknowledgment was lost).
            return Ok(true);
        }

        let continues = match guard.upload.as_ref() {
            Some(current)
                if current.head_seq == head_seq
                    && current.layout == layout
                    && current.snapshot_hash == snapshot_hash =>
            {
                true
            }
            Some(current) if current.head_seq > head_seq => {
                return Err(ServerError::SnapshotSuperseded(format!(
                    "An upload for the newer sequence {} of room {room_id} is in progress",
                    current.head_seq
                )));
            }
            Some(current) if current.head_seq == head_seq && !current.yields_to(&uploader) => {
                return Err(ServerError::SnapshotSuperseded(format!(
                    "Another upload for sequence {head_seq} of room {room_id} is in progress"
                )));
            }
            _ => false,
        };

        if !continues {
            bounds.check(head_seq)?;
            self.check_newer_than_active(&room_id, head_seq)?;
            if let Some(replaced) = guard.upload.take() {
                info!(room = %room_id, old_seq = %replaced.head_seq, new_seq = %head_seq,
                    "Replacing snapshot upload in progress");
                self.remove_files(vec![replaced.path]).await;
            }
            guard.upload = Some(UploadSession {
                head_seq,
                uploader,
                layout,
                snapshot_hash,
                received: vec![false; total_chunks as usize],
                received_count: 0,
                last_chunk_at: Instant::now(),
                path: self
                    .uploads_dir
                    .join(upload_file_name(&room_id, head_seq, PART_EXTENSION)),
            });
        }

        let Some(session) = guard.upload.as_mut() else {
            return Err(ServerError::Internal("Upload session vanished".to_string()));
        };
        let part_path = session.path.clone();
        let offset = layout.offset(chunk_index);
        run_blocking(move || write_at(&part_path, offset, &data).map_err(ServerError::from))
            .await?;

        if !session.received[chunk_index as usize] {
            session.received[chunk_index as usize] = true;
            session.received_count += 1;
        }
        session.last_chunk_at = Instant::now();
        if session.received_count < total_chunks {
            return Ok(false);
        }

        // Last chunk: the session ends here, whatever the outcome.
        let Some(session) = guard.upload.take() else {
            return Err(ServerError::Internal("Upload session vanished".to_string()));
        };
        let acceptable = bounds
            .check(head_seq)
            .and_then(|()| self.check_newer_than_active(&room_id, head_seq));
        if let Err(err) = acceptable {
            self.remove_files(vec![session.path]).await;
            return Err(err);
        }

        let final_path =
            self.snapshots_dir
                .join(snapshot_file_name(&room_id, head_seq, &snapshot_hash));
        let part_path = session.path.clone();
        let (uploads_dir, snapshots_dir) = (self.uploads_dir.clone(), self.snapshots_dir.clone());
        let promoted_path = final_path.clone();
        let outcome = run_blocking(move || {
            // The chunks were written without syncing; sync the whole file once, then read it
            // back to check it.
            let verified = sync_file(&part_path)
                .map_err(VerifyError::Io)
                .and_then(|()| verify_snapshot_file(&part_path, &snapshot_hash));
            let result = match verified {
                Ok(_) => promote(&part_path, &promoted_path, &uploads_dir, &snapshots_dir)
                    .map_err(ServerError::from),
                Err(VerifyError::Io(err)) => Err(ServerError::from(err)),
                Err(VerifyError::Invalid(msg)) => Err(ServerError::BadRequest(msg)),
            };
            if result.is_err() {
                if let Err(err) = remove_file_if_exists(&part_path) {
                    warn!(path = ?part_path, error = %err, "Failed to delete rejected snapshot upload");
                }
            }
            result
        })
        .await;
        outcome?;

        self.install(
            &room_id,
            &mut guard,
            StagedSnapshot {
                head_seq,
                total_bytes,
                snapshot_hash,
                staged_at: Instant::now(),
                path: final_path,
            },
        )
        .await;
        Ok(true)
    }

    /// Whether an upload for the room is under way: a single-request upload being received or
    /// staged, or a multipart upload that has not gone idle.
    ///
    /// Reads memory without waiting, so the room actor can call it. A room whose lock is held
    /// at that moment counts as uploading: the lock is only held briefly, and callers only
    /// postpone a decision on it.
    pub fn has_upload_in_progress(&self, room_id: &RoomId) -> bool {
        if self
            .pending_single_uploads
            .get(room_id)
            .is_some_and(|count| *count > 0)
        {
            return true;
        }
        let Some(slot) = self
            .slots
            .get(room_id)
            .map(|slot| std::sync::Arc::clone(slot.value()))
        else {
            return false;
        };
        let in_progress = match slot.try_lock() {
            Ok(guard) => guard
                .upload
                .as_ref()
                .is_some_and(|upload| !upload.is_idle(Instant::now())),
            Err(_) => true,
        };
        in_progress
    }

    /// Whether the room's active snapshot is exactly `seq` with `hash`.
    fn is_active(&self, room_id: &RoomId, seq: SequenceNumber, hash: &[u8; 32]) -> bool {
        self.active(room_id)
            .is_some_and(|a| a.head_seq == seq && a.snapshot_hash == *hash)
    }

    /// Rejects `seq` unless it is newer than the room's active snapshot. Requires the room lock.
    fn check_newer_than_active(
        &self,
        room_id: &RoomId,
        seq: SequenceNumber,
    ) -> Result<(), ServerError> {
        match self.active(room_id) {
            Some(active) if seq <= active.head_seq => Err(ServerError::SnapshotSuperseded(format!(
                "Snapshot sequence {seq} is not newer than the active snapshot ({}) of room {room_id}",
                active.head_seq
            ))),
            _ => Ok(()),
        }
    }
}

fn check_seq_positive(seq: SequenceNumber) -> Result<(), ServerError> {
    if seq.get() == 0 {
        return Err(ServerError::BadRequest(
            "Snapshot sequence must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

fn room_deleted(room_id: &RoomId) -> ServerError {
    ServerError::RoomNotFound(format!("Room {room_id} was deleted"))
}

#[cfg(test)]
#[path = "tests/upload.rs"]
mod tests;
