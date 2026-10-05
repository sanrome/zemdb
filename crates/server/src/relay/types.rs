//! Public types and limits of the snapshot relay.

use bytes::Bytes;
use std::time::Duration;
use zemdb_core::id::{ClientId, RoomId, SequenceNumber};

use crate::error::ServerError;

/// Smallest download chunk served; smaller requested sizes are raised to it. Also the smallest
/// chunk of a multipart upload made of more than one chunk.
pub const MIN_CHUNK_BYTES: u32 = 64 * 1024;

/// Largest download chunk served, and largest chunk of a multipart upload.
pub const MAX_CHUNK_BYTES: u32 = 4 * 1024 * 1024;

/// A multipart upload that receives no chunk for this long is discarded.
pub const UPLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// Interval between background sweeps of expired snapshots and idle uploads.
pub const EXPIRY_SWEEP_PERIOD: Duration = Duration::from_secs(30);

/// Single-request uploads of one room that may wait at the same time. Each holds its whole
/// body in memory while it waits for the room lock.
pub const MAX_PENDING_SINGLE_UPLOADS: usize = 2;

/// Retained range of a room's log, as reported by its actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogBounds {
    /// Oldest retained sequence number.
    pub tail_seq: SequenceNumber,
    /// Highest committed sequence number.
    pub head_seq: SequenceNumber,
}

impl LogBounds {
    /// Whether a client restoring a snapshot at `seq` can catch up from the retained log:
    /// `tail - 1 <= seq <= head`.
    pub(super) fn admits(&self, seq: SequenceNumber) -> bool {
        seq.get().saturating_add(1) >= self.tail_seq.get() && seq <= self.head_seq
    }

    pub(super) fn check(&self, seq: SequenceNumber) -> Result<(), ServerError> {
        if self.admits(seq) {
            Ok(())
        } else {
            Err(ServerError::BadRequest(format!(
                "Snapshot sequence {seq} is outside the range the room log can continue from \
                 (tail {}, head {})",
                self.tail_seq, self.head_seq
            )))
        }
    }
}

/// Who is uploading a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Uploader {
    /// The holder of the admin secret (the automated snapshot worker).
    Admin,
    /// A room member, authenticated by its client token.
    Client(ClientId),
}

/// Parameters for staging a snapshot chunk in the relay.
#[derive(Debug, Clone)]
pub struct SnapshotChunkUpload {
    pub room_id: RoomId,
    pub uploader: Uploader,
    pub head_seq: SequenceNumber,
    pub chunk_index: u32,
    pub total_chunks: u32,
    pub total_bytes: u64,
    pub snapshot_hash: [u8; 32],
    pub data: Bytes,
}
