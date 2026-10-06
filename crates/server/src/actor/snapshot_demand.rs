//! Snapshot demand of a room: whether some client needs a base snapshot that the relay does
//! not have, and which client is asked to upload one.
//!
//! The room actor owns this state. Whether the demand is on, and when it was last renewed,
//! is persisted in the client roster (as wall-clock time) and restored when the room reopens;
//! the designation is kept in memory only and recomputed after a restart.

use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime};
use zemdb_core::id::ClientId;

/// How long a designated client has to start uploading before another one is designated.
pub(crate) const DESIGNATION_TIMEOUT: Duration = Duration::from_secs(60);

/// How stale the persisted renewal time of a demand may get before it is rewritten:
/// `min(ttl / 100, 1 hour)`. After a restart, a restored demand may therefore expire up to
/// this much earlier than it would have without the restart.
pub(crate) fn persistence_granularity(ttl: Duration) -> Duration {
    (ttl / 100).min(Duration::from_secs(60 * 60))
}

/// Demand for a room snapshot and the client designated to upload it.
///
/// The demand is on while some client needs a snapshot and no usable one is available. Every
/// new request renews it; without renewals it expires after `ttl`. While it is on, exactly one
/// `Connected` client at a time is designated to upload. A designee that has not started an
/// upload within `designation_timeout`, or that stops being `Connected`, is replaced and
/// excluded for the rest of the demand episode. Designations never change while an upload is
/// in progress.
#[derive(Debug)]
pub(crate) struct SnapshotDemand {
    ttl: Duration,
    designation_timeout: Duration,
    active: Option<ActiveDemand>,
}

#[derive(Debug)]
struct ActiveDemand {
    /// When the demand turns off unless it is renewed first.
    expires_at: Instant,
    designee: Option<Designation>,
    /// Clients replaced as designee during this episode, not designated again unless every
    /// candidate has been.
    excluded: HashSet<ClientId>,
}

#[derive(Debug)]
struct Designation {
    client_id: ClientId,
    since: Instant,
}

impl SnapshotDemand {
    pub(crate) fn new(ttl: Duration, designation_timeout: Duration) -> Self {
        Self {
            ttl,
            designation_timeout,
            active: None,
        }
    }

    /// Whether some client is waiting for a snapshot.
    pub(crate) fn is_active(&self) -> bool {
        self.active.is_some()
    }

    /// The client currently asked to upload a snapshot, if any.
    pub(crate) fn designee(&self) -> Option<&ClientId> {
        self.active
            .as_ref()
            .and_then(|demand| demand.designee.as_ref())
            .map(|designation| &designation.client_id)
    }

    /// Turns the demand on, or renews it if it is already on: it lasts one TTL from `now`.
    pub(crate) fn renew(&mut self, now: Instant) {
        self.extend_until(now + self.ttl);
    }

    fn extend_until(&mut self, expires_at: Instant) {
        match self.active.as_mut() {
            Some(demand) => demand.expires_at = expires_at,
            None => {
                self.active = Some(ActiveDemand {
                    expires_at,
                    designee: None,
                    excluded: HashSet::new(),
                })
            }
        }
    }

    /// Restores a demand persisted before a restart, last renewed at the wall-clock time
    /// `renewed_at`. Returns false, leaving the demand off, if it had already expired. A
    /// renewal time in the future (the clock moved backwards) counts as renewed at
    /// `wall_now`, so the demand still expires one TTL later.
    pub(crate) fn restore(
        &mut self,
        renewed_at: SystemTime,
        wall_now: SystemTime,
        now: Instant,
    ) -> bool {
        let age = wall_now
            .duration_since(renewed_at)
            .unwrap_or(Duration::ZERO);
        let Some(remaining) = self.ttl.checked_sub(age).filter(|r| !r.is_zero()) else {
            return false;
        };
        // The deadline is computed forwards: on some platforms an `Instant` cannot go back
        // further than the machine's boot, so the renewal instant itself is not representable.
        self.extend_until(now + remaining);
        true
    }

    /// Turns the demand off: a usable snapshot is available.
    pub(crate) fn satisfy(&mut self) {
        self.active = None;
    }

    /// Turns the demand off if it was not renewed within the TTL. Returns true if it expired.
    pub(crate) fn expire(&mut self, now: Instant) -> bool {
        let expired = self
            .active
            .as_ref()
            .is_some_and(|demand| now >= demand.expires_at);
        if expired {
            self.active = None;
        }
        expired
    }

    /// Keeps a client designated while the demand is on, replacing a designee that timed out
    /// or is no longer connected. `is_connected` tells whether a client is still `Connected`;
    /// `pick` chooses among the `Connected` clients outside the given exclusion set. Nothing
    /// changes while `upload_in_progress`.
    ///
    /// Returns the client designated by this call, if any; a designee that keeps its role is
    /// not returned again.
    pub(crate) fn designate(
        &mut self,
        now: Instant,
        upload_in_progress: bool,
        is_connected: impl Fn(&ClientId) -> bool,
        pick: impl Fn(&HashSet<ClientId>) -> Option<ClientId>,
    ) -> Option<ClientId> {
        let demand = self.active.as_mut()?;
        if upload_in_progress {
            return None;
        }

        if let Some(current) = &demand.designee {
            let timed_out =
                now.saturating_duration_since(current.since) >= self.designation_timeout;
            if !timed_out && is_connected(&current.client_id) {
                return None;
            }
            demand.excluded.insert(current.client_id.clone());
            demand.designee = None;
        }

        let chosen = match pick(&demand.excluded) {
            Some(client_id) => Some(client_id),
            None if !demand.excluded.is_empty() => {
                // Every candidate already had its turn: start a new round.
                demand.excluded.clear();
                pick(&demand.excluded)
            }
            None => None,
        }?;
        demand.designee = Some(Designation {
            client_id: chosen.clone(),
            since: now,
        });
        Some(chosen)
    }
}

#[cfg(test)]
#[path = "tests/snapshot_demand.rs"]
mod tests;
