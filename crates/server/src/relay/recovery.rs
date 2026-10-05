//! Startup recovery of the snapshot relay: discard partial uploads, keep the newest valid
//! snapshot of each room.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::time::Instant;
use tracing::{info, warn};
use zemdb_core::id::{RoomId, SequenceNumber};

use super::files::{
    parse_snapshot_file_name, remove_file_if_exists, verify_snapshot_file, VerifyError,
    TMP_EXTENSION,
};
use super::{SnapshotRelay, StagedSnapshot};
use crate::durable;

/// Snapshot file found at startup: sequence, hash from its name, path, and age.
type RecoveredFile = (SequenceNumber, [u8; 32], PathBuf, Duration);

impl SnapshotRelay {
    /// Deletes every partial upload left by a previous run.
    pub(super) fn clear_uploads(&self) -> std::io::Result<()> {
        let mut removed = false;
        for entry in fs::read_dir(&self.uploads_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                fs::remove_dir_all(entry.path())?;
            } else {
                fs::remove_file(entry.path())?;
            }
            removed = true;
        }
        if removed {
            durable::sync_dir(&self.uploads_dir)?;
        }
        Ok(())
    }

    /// Indexes the newest valid snapshot of each room and deletes every other snapshot file.
    pub(super) fn recover_snapshots(&self) -> std::io::Result<()> {
        let now = SystemTime::now();
        let mut candidates: HashMap<RoomId, Vec<RecoveredFile>> = HashMap::new();
        let mut doomed = Vec::new();

        for entry in fs::read_dir(&self.snapshots_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                continue;
            }
            let path = entry.path();
            let name = entry.file_name();
            let Some((room_id, seq, hash)) = name.to_str().and_then(parse_snapshot_file_name)
            else {
                if path.extension().is_some_and(|ext| ext == TMP_EXTENSION) {
                    doomed.push(path);
                } else {
                    warn!(path = ?path, "Ignoring unrecognized file in the snapshots directory");
                }
                continue;
            };
            let modified = entry.metadata()?.modified().unwrap_or(now);
            let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
            if age >= self.ttl {
                doomed.push(path);
                continue;
            }
            candidates
                .entry(room_id)
                .or_default()
                .push((seq, hash, path, age));
        }

        let started = Instant::now();
        for (room_id, mut files) in candidates {
            files.sort_by_key(|file| std::cmp::Reverse(file.0));
            // Set once the newest usable file is found, or once a file could not be read: an
            // older snapshot must not become active while a newer one may still exist.
            let mut settled = false;
            for (seq, hash, path, age) in files {
                if settled {
                    doomed.push(path);
                    continue;
                }
                match verify_snapshot_file(&path, &hash) {
                    Ok(total_bytes) => {
                        info!(room = %room_id, seq = %seq, "Recovered staged snapshot");
                        self.snapshots.insert(
                            room_id.clone(),
                            StagedSnapshot {
                                head_seq: seq,
                                total_bytes,
                                snapshot_hash: hash,
                                staged_at: started.checked_sub(age).unwrap_or(started),
                                path,
                            },
                        );
                        settled = true;
                    }
                    Err(VerifyError::Invalid(msg)) => {
                        warn!(path = ?path, error = %msg, "Discarding invalid snapshot file");
                        doomed.push(path);
                    }
                    Err(VerifyError::Io(err)) => {
                        // Not proof that the file is bad: keep it for a later start, but do not
                        // serve it, nor anything older.
                        warn!(path = ?path, error = %err, "Could not read snapshot file; leaving it in place");
                        settled = true;
                    }
                }
            }
        }

        // Deleting is housekeeping: a file left behind is retried at the next start, and the
        // newest valid snapshot always wins anyway.
        if !doomed.is_empty() {
            for path in &doomed {
                if let Err(err) = remove_file_if_exists(path) {
                    warn!(path = ?path, error = %err, "Failed to delete stale snapshot file");
                }
            }
            if let Err(err) = durable::sync_dir(&self.snapshots_dir) {
                warn!(error = %err, "Failed to sync the snapshots directory");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/recovery.rs"]
mod tests;
