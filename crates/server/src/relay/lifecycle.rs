//! Snapshot relay lifecycle: room deletion, expiry of snapshots and idle uploads, and the
//! background sweeper.

use std::collections::BTreeSet;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::info;
use zemdb_core::id::RoomId;

use super::files::{
    parse_snapshot_file_name, parse_upload_file_name, remove_room_files, run_blocking,
};
use super::types::EXPIRY_SWEEP_PERIOD;
use super::{RoomSlot, SnapshotRelay};
use crate::error::ServerError;
use crate::fail_point;

impl SnapshotRelay {
    /// Deletes everything the relay holds for a room: its active snapshot and its upload in
    /// progress, in memory and on disk. Requests that were already under way for the room
    /// fail with `RoomNotFound` instead of staging anything afterwards.
    ///
    /// The room gets a new slot, held until the files are gone even if the caller is dropped:
    /// a request that comes after the purge waits for it.
    pub async fn purge_room(&self, room_id: &RoomId) -> Result<(), ServerError> {
        let slot = self.slot(room_id);
        let mut guard = slot.lock().await;
        guard.purged = true;
        guard.upload = None;
        self.snapshots.remove(room_id);
        let next = Arc::<Mutex<RoomSlot>>::default();
        let held = Arc::clone(&next)
            .try_lock_owned()
            .expect("a new slot is free");
        self.slots.insert(room_id.clone(), next);

        // The directories are scanned rather than trusting the paths in memory, so that a
        // retry after a failed deletion still finds every file of the room.
        let room = room_id.clone();
        let (uploads_dir, snapshots_dir) = (self.uploads_dir.clone(), self.snapshots_dir.clone());
        run_blocking(move || {
            fail_point::hook("relay_purge_room_files", &snapshots_dir);
            remove_room_files(&snapshots_dir, &room, |name| {
                parse_snapshot_file_name(name).map(|(r, _, _)| r)
            })?;
            remove_room_files(&uploads_dir, &room, |name| {
                parse_upload_file_name(name).map(|(r, _)| r)
            })?;
            drop(held);
            Ok(())
        })
        .await
    }

    /// Deletes expired snapshots and idle uploads of every room, in memory and on disk.
    pub async fn cleanup_expired(&self) {
        let now = Instant::now();
        let rooms: BTreeSet<RoomId> = self
            .snapshots
            .iter()
            .filter(|s| s.value().is_expired(now, self.ttl))
            .map(|s| s.key().clone())
            .chain(self.slots.iter().map(|s| s.key().clone()))
            .collect();

        for room_id in rooms {
            let slot = self.slot(&room_id);
            let mut guard = slot.lock().await;
            if !guard.purged {
                self.expire_locked(&room_id, &mut guard).await;
            }
        }

        // Forget the locks of rooms with nothing in progress or remembered. A lock that some
        // request holds a reference to is kept, so every request of a room shares one lock.
        self.slots.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || slot
                    .try_lock()
                    .map(|guard| guard.upload.is_some() || guard.expired.is_some())
                    .unwrap_or(true)
        });
    }

    /// Spawns a background task that calls [`cleanup_expired`](Self::cleanup_expired) every
    /// [`EXPIRY_SWEEP_PERIOD`]. The task ends once the relay is dropped.
    pub fn spawn_expiry_sweeper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let relay: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(EXPIRY_SWEEP_PERIOD);
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(relay) = relay.upgrade() else { break };
                relay.cleanup_expired().await;
            }
        })
    }

    /// Drops the room's expired snapshot and idle upload, and forgets an idle upload's
    /// deadline once its uploader may start over.
    pub(super) async fn expire_locked(&self, room_id: &RoomId, slot: &mut RoomSlot) {
        let now = Instant::now();
        let mut doomed = Vec::new();
        if let Some((_, expired)) = self
            .snapshots
            .remove_if(room_id, |_, s| s.is_expired(now, self.ttl))
        {
            doomed.push(expired.path);
        }
        slot.expired.take_if(|e| now >= e.forget_at);
        if let Some(idle) = slot.upload.take_if(|u| u.is_idle(now)) {
            info!(room = %room_id, seq = %idle.head_seq, "Discarding idle snapshot upload");
            slot.expired = Some(idle.expired());
            doomed.push(idle.path);
        }
        self.remove_files(doomed, ()).await;
    }
}

#[cfg(test)]
#[path = "tests/lifecycle.rs"]
mod tests;
