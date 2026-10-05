//! Snapshot downloads: chunks read from the active snapshot's file, anchored by hash.

use bytes::Bytes;
use std::future::Future;
use tracing::info;
use zemdb_core::id::{CorrelationId, RoomId};
use zemdb_core::protocol::messages::ServerMessage;

use super::files::{read_range, run_blocking};
use super::types::{LogBounds, MAX_CHUNK_BYTES, MIN_CHUNK_BYTES};
use super::{SnapshotRelay, StagedSnapshot};
use crate::error::ServerError;

impl SnapshotRelay {
    /// Retrieves one chunk of the active snapshot, read from its file.
    ///
    /// `chunk_size` is clamped to [`MIN_CHUNK_BYTES`]..=[`MAX_CHUNK_BYTES`]. Only the first
    /// chunk may be requested without `snapshot_hash`; that request also checks, through
    /// `log_bounds`, that the room log can still continue from the snapshot, and drops a
    /// snapshot that fell below the retained range. With `snapshot_hash`, the request is
    /// anchored to that snapshot: if it is no longer the active one the result is
    /// `SnapshotSuperseded`.
    pub async fn get_chunk(
        &self,
        correlation_id: CorrelationId,
        room_id: &RoomId,
        chunk_index: u32,
        chunk_size: u32,
        snapshot_hash: Option<[u8; 32]>,
        log_bounds: impl Future<Output = Result<LogBounds, ServerError>>,
    ) -> Result<ServerMessage, ServerError> {
        if chunk_index > 0 && snapshot_hash.is_none() {
            return Err(ServerError::BadRequest(
                "snapshot_hash is required after the first chunk; take it from the reply to chunk 0"
                    .to_string(),
            ));
        }
        let chunk_size = u64::from(chunk_size.clamp(MIN_CHUNK_BYTES, MAX_CHUNK_BYTES));
        let staged = match (self.active(room_id), snapshot_hash) {
            (Some(staged), Some(anchor)) if staged.snapshot_hash != anchor => {
                return Err(superseded(room_id, &staged))
            }
            (Some(staged), _) => staged,
            (None, Some(_)) => {
                return Err(ServerError::SnapshotSuperseded(format!(
                    "The requested snapshot of room {room_id} is no longer staged"
                )))
            }
            (None, None) => return Err(no_snapshot(room_id)),
        };

        if snapshot_hash.is_none() {
            let bounds = log_bounds.await?;
            if !bounds.admits(staged.head_seq) {
                self.drop_stale(room_id, &staged).await;
                return Err(no_snapshot(room_id));
            }
        }

        let total_chunks = u32::try_from(staged.total_bytes.div_ceil(chunk_size))
            .map_err(|_| ServerError::Internal("Snapshot has too many chunks".to_string()))?;
        if chunk_index >= total_chunks {
            return Err(ServerError::BadRequest(format!(
                "Requested chunk index {chunk_index} exceeds total chunks {total_chunks}"
            )));
        }

        let offset = u64::from(chunk_index) * chunk_size;
        let len = chunk_size.min(staged.total_bytes - offset);
        let data = self.read_chunk(room_id, &staged, offset, len).await?;

        Ok(ServerMessage::SnapshotChunk {
            correlation_id,
            room_id: room_id.clone(),
            snapshot_head_seq: staged.head_seq,
            chunk_index,
            total_chunks,
            total_bytes: staged.total_bytes,
            snapshot_hash: staged.snapshot_hash,
            data: Bytes::from(data),
        })
    }

    /// Reads a range of `staged`'s file. If the read fails and `staged` is no longer the active
    /// snapshot (replaced or expired meanwhile), the result is `SnapshotSuperseded`; otherwise
    /// it is the I/O error.
    async fn read_chunk(
        &self,
        room_id: &RoomId,
        staged: &StagedSnapshot,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, ServerError> {
        let path = staged.path.clone();
        match run_blocking(move || Ok(read_range(&path, offset, len))).await? {
            Ok(data) => Ok(data),
            Err(err) => match self.active(room_id) {
                Some(current) if current.snapshot_hash == staged.snapshot_hash => Err(err.into()),
                _ => Err(ServerError::SnapshotSuperseded(format!(
                    "The snapshot of room {room_id} was replaced during the download"
                ))),
            },
        }
    }

    /// Drops `stale` if it is still the room's active snapshot, deleting its file.
    async fn drop_stale(&self, room_id: &RoomId, stale: &StagedSnapshot) {
        let slot = self.slot(room_id);
        let guard = slot.lock().await;
        if guard.purged {
            return;
        }
        let removed = self
            .snapshots
            .remove_if(room_id, |_, s| s.snapshot_hash == stale.snapshot_hash);
        if let Some((_, removed)) = removed {
            info!(room = %room_id, seq = %removed.head_seq,
                "Dropping snapshot that fell below the retained log range");
            self.remove_files(vec![removed.path]).await;
        }
    }
}

fn no_snapshot(room_id: &RoomId) -> ServerError {
    ServerError::RoomNotFound(format!("No usable staged snapshot for room {room_id}"))
}

fn superseded(room_id: &RoomId, active: &StagedSnapshot) -> ServerError {
    ServerError::SnapshotSuperseded(format!(
        "The requested snapshot of room {room_id} was replaced by the snapshot at sequence {} \
         (hash {}); restart the download from chunk 0",
        active.head_seq,
        blake3::Hash::from(active.snapshot_hash).to_hex()
    ))
}

#[cfg(test)]
#[path = "tests/download.rs"]
mod tests;
