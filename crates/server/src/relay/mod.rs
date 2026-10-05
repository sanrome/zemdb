//! Snapshot relay: keeps the latest room snapshot uploaded by a client (or an automated
//! snapshot worker) and serves it in chunks to clients that bootstrap from it.
//!
//! Snapshots live only on disk. The relay keeps metadata in memory (sequence, size, BLAKE3
//! hash, file path) and every download chunk is read from the file on demand, off the async
//! runtime. Snapshot files are written atomically and durably; a replaced snapshot file is
//! deleted only once its successor is durable.
//!
//! Acceptance rules for a snapshot at sequence `S` of a room whose log retains `tail..=head`:
//! `tail - 1 <= S <= head` (a client restoring it can catch up from the log),
//! `S` strictly greater than the active snapshot's sequence, and a valid `ZMSN` envelope
//! header whose CRC matches the body. The relay cannot check what the snapshot contains: the
//! client validates the structure when applying it.
//!
//! All changes to a room's snapshot or upload session are serialized by a per-room lock, so
//! concurrent uploads can neither delete each other's files nor move the active snapshot
//! backwards.

mod download;
mod files;
mod lifecycle;
mod recovery;
mod types;
mod upload;

pub use types::{
    LogBounds, SnapshotChunkUpload, Uploader, EXPIRY_SWEEP_PERIOD, MAX_CHUNK_BYTES,
    MAX_PENDING_SINGLE_UPLOADS, MIN_CHUNK_BYTES, UPLOAD_IDLE_TIMEOUT,
};

use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::warn;
use zemdb_core::id::{RoomId, SequenceNumber};

use crate::durable;
use files::{remove_file_if_exists, run_blocking, UPLOADS_DIR};
use upload::UploadSession;

/// Metadata of the active snapshot of a room. The bytes stay in the file.
#[derive(Debug, Clone)]
struct StagedSnapshot {
    head_seq: SequenceNumber,
    total_bytes: u64,
    snapshot_hash: [u8; 32],
    staged_at: Instant,
    path: PathBuf,
}

impl StagedSnapshot {
    fn is_expired(&self, now: Instant, ttl: Duration) -> bool {
        now.saturating_duration_since(self.staged_at) >= ttl
    }
}

/// Per-room state guarded by the room lock.
#[derive(Debug, Default)]
struct RoomSlot {
    /// The single multipart upload allowed per room.
    upload: Option<UploadSession>,
    /// Set when the room is deleted. A request that obtained this slot before the deletion
    /// must not stage anything once it gets the lock.
    purged: bool,
}

/// Ephemeral relay facilitating state-transfer chunks between peers or cold snapshots and bootstrapping clients.
#[derive(Debug)]
pub struct SnapshotRelay {
    /// Active snapshot of each room. Only changed while holding the room's slot lock.
    snapshots: DashMap<RoomId, StagedSnapshot>,
    slots: DashMap<RoomId, Arc<Mutex<RoomSlot>>>,
    snapshots_dir: PathBuf,
    uploads_dir: PathBuf,
    ttl: Duration,
    max_snapshot_bytes: u64,
    /// Single-request uploads in progress per room, bounded by [`MAX_PENDING_SINGLE_UPLOADS`].
    pending_single_uploads: DashMap<RoomId, usize>,
}

impl SnapshotRelay {
    /// Opens the relay on `snapshots_dir`, creating it if needed.
    ///
    /// Startup recovery deletes every partial upload, and for each room keeps only the
    /// highest-sequence snapshot file that is within `ttl` and intact (valid envelope and a
    /// content hash matching its file name); every other snapshot file is deleted.
    pub fn new(
        snapshots_dir: impl AsRef<Path>,
        ttl: Duration,
        max_snapshot_bytes: u64,
    ) -> std::io::Result<Self> {
        let snapshots_dir = snapshots_dir.as_ref().to_path_buf();
        let uploads_dir = snapshots_dir.join(UPLOADS_DIR);
        durable::create_dir_all_synced(&uploads_dir)?;

        let relay = Self {
            snapshots: DashMap::new(),
            slots: DashMap::new(),
            snapshots_dir,
            uploads_dir,
            ttl,
            max_snapshot_bytes,
            pending_single_uploads: DashMap::new(),
        };
        relay.clear_uploads()?;
        relay.recover_snapshots()?;
        Ok(relay)
    }

    /// Returns the active snapshot sequence number for a room if one is currently staged and valid.
    ///
    /// Reads memory only, so the room actor can call it.
    pub fn active_snapshot_seq(&self, room_id: &RoomId) -> Option<SequenceNumber> {
        self.active(room_id).map(|s| s.head_seq)
    }

    /// Unexpired active snapshot of a room. The map guard is released before returning.
    fn active(&self, room_id: &RoomId) -> Option<StagedSnapshot> {
        let staged = self.snapshots.get(room_id)?.clone();
        (!staged.is_expired(Instant::now(), self.ttl)).then_some(staged)
    }

    fn slot(&self, room_id: &RoomId) -> Arc<Mutex<RoomSlot>> {
        Arc::clone(&self.slots.entry(room_id.clone()).or_default())
    }

    /// Makes a durable snapshot file the room's active snapshot, then deletes the files it
    /// makes obsolete: the previous snapshot and an upload for a sequence not above it.
    /// Requires the room lock.
    async fn install(&self, room_id: &RoomId, slot: &mut RoomSlot, staged: StagedSnapshot) {
        let mut obsolete = Vec::new();
        if slot
            .upload
            .as_ref()
            .is_some_and(|u| u.head_seq <= staged.head_seq)
        {
            if let Some(upload) = slot.upload.take() {
                obsolete.push(upload.path);
            }
        }
        let new_path = staged.path.clone();
        if let Some(previous) = self.snapshots.insert(room_id.clone(), staged) {
            if previous.path != new_path {
                obsolete.push(previous.path);
            }
        }
        self.remove_files(obsolete).await;
    }

    /// Deletes files off the async runtime. A failure is logged: startup recovery removes
    /// whatever is left.
    async fn remove_files(&self, paths: Vec<PathBuf>) {
        if paths.is_empty() {
            return;
        }
        let result = run_blocking(move || {
            let mut dirs = Vec::new();
            for path in &paths {
                if let Err(err) = remove_file_if_exists(path) {
                    warn!(path = ?path, error = %err, "Failed to delete snapshot relay file");
                }
                if let Some(dir) = path.parent() {
                    if !dirs.iter().any(|d: &PathBuf| d == dir) {
                        dirs.push(dir.to_path_buf());
                    }
                }
            }
            for dir in dirs {
                if let Err(err) = durable::sync_dir(&dir) {
                    warn!(dir = ?dir, error = %err, "Failed to sync snapshots directory");
                }
            }
            Ok(())
        })
        .await;
        if let Err(err) = result {
            warn!(error = %err, "Snapshot relay file cleanup task failed");
        }
    }
}

#[cfg(test)]
#[path = "tests/common.rs"]
mod test_common;

#[cfg(test)]
#[path = "../tests/relay.rs"]
mod tests;
