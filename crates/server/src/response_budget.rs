//! The budget of response bytes in flight (`max_inflight_response_bytes`), and the part of it
//! each client may hold (`max_inflight_response_bytes_per_client`).

use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zemdb_core::id::RoomId;
use zemdb_core::protocol::codec::MAX_FRAME_SIZE;
use zemdb_core::protocol::limits::MAX_RESPONSE_OPS_BYTES;

use crate::error::ServerError;
use crate::relay::Uploader;

/// The error of a response without room in the budget: `Unavailable` (503), to be retried.
pub(crate) fn no_room() -> ServerError {
    ServerError::Unavailable("Too many response bytes in flight".to_string())
}

/// What a response takes besides its operations: the frame header and, at most, the rest of a
/// `CommitAck` or `SyncBatch` (`RESPONSE_ENVELOPE_ALLOWANCE`), which also covers the rest of a
/// `Registered` or `Schema` besides its schema.
pub(crate) const RESPONSE_OVERHEAD: u64 = MAX_FRAME_SIZE as u64 - MAX_RESPONSE_OPS_BYTES;

/// What a read of the log grows the reservation of its response by at a time, at least
/// ([`ReadReservation`]).
pub(crate) const READ_RESERVATION_CHUNK: u64 = 256 * 1024;

/// Who holds part of the budget of response bytes in flight: a client of a room, or the admin
/// secret in a room's relay.
pub(crate) type BudgetHolder = (RoomId, Uploader);

/// The budget of response bytes in flight: `max_inflight_response_bytes` in all, of which each
/// holder may take `max_inflight_response_bytes_per_client`.
#[derive(Debug)]
pub(crate) struct InflightBudget {
    total: Arc<Semaphore>,
    held: Mutex<HashMap<BudgetHolder, u64>>,
    per_holder: u64,
    /// How many times reservations took from the budget, or tried to.
    #[cfg(test)]
    takes: AtomicUsize,
}

impl InflightBudget {
    pub(crate) fn new(total: u64, per_holder: u64) -> Self {
        let total = usize::try_from(total)
            .unwrap_or(usize::MAX)
            .min(Semaphore::MAX_PERMITS);
        Self {
            total: Arc::new(Semaphore::new(total)),
            held: Mutex::default(),
            per_holder,
            #[cfg(test)]
            takes: AtomicUsize::new(0),
        }
    }

    /// Reserves a frame, the most any response takes, for `holder`: for a response whose size
    /// is only known once it is read, such as a snapshot chunk.
    pub(crate) fn reserve_frame(self: &Arc<Self>, holder: BudgetHolder) -> Option<Reservation> {
        self.reserve(holder, MAX_FRAME_SIZE as u64)
    }

    /// Reserves `bytes` for `holder`, if both its part and the whole budget have room.
    pub(crate) fn reserve(
        self: &Arc<Self>,
        holder: BudgetHolder,
        bytes: u64,
    ) -> Option<Reservation> {
        let permit = self.take(&holder, bytes)?;
        Some(Reservation {
            budget: Arc::clone(self),
            holder,
            permit,
        })
    }

    /// Takes `bytes` from `holder`'s part and from the whole budget, if both have room.
    fn take(&self, holder: &BudgetHolder, bytes: u64) -> Option<OwnedSemaphorePermit> {
        #[cfg(test)]
        self.takes.fetch_add(1, Ordering::Relaxed);
        let permits = u32::try_from(bytes).ok()?;
        let permit = Arc::clone(&self.total)
            .try_acquire_many_owned(permits)
            .ok()?;
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        // A reservation that grows finds its holder's part already there: no copy of the
        // holder.
        match held.get_mut(holder) {
            Some(part) if *part + bytes > self.per_holder => return None,
            Some(part) => *part += bytes,
            None if bytes > self.per_holder => return None,
            None => {
                held.insert(holder.clone(), bytes);
            }
        }
        Some(permit)
    }

    /// The bytes held now: by the parts of all holders, and missing from the whole budget.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> (u64, usize) {
        let held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        (held.values().sum(), self.total.available_permits())
    }

    /// How many times reservations took from the budget, or tried to.
    #[cfg(test)]
    pub(crate) fn takes(&self) -> usize {
        self.takes.load(Ordering::Relaxed)
    }
}

/// Bytes of the budget reserved for one response, returned when dropped.
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<InflightBudget>,
    holder: BudgetHolder,
    permit: OwnedSemaphorePermit,
}

impl Reservation {
    /// Grows the reservation by `bytes`, if both the holder's part and the whole budget have
    /// room, and returns whether it did. A reservation never holds more than a frame, what a
    /// response reserved before it was read: past that it grows for free, so that a response
    /// always fits in the smallest part and the smallest budget accepted (a frame each).
    pub(crate) fn grow(&mut self, bytes: u64) -> bool {
        self.grow_by(bytes).is_some()
    }

    /// Grows the reservation as [`grow`](Self::grow) does, and returns by how much: less than
    /// `bytes` past a frame, and `None` without room.
    fn grow_by(&mut self, bytes: u64) -> Option<u64> {
        let held = self.permit.num_permits() as u64;
        let bytes = bytes.min((MAX_FRAME_SIZE as u64).saturating_sub(held));
        if bytes == 0 {
            return Some(0);
        }
        let more = self.budget.take(&self.holder, bytes)?;
        self.permit.merge(more);
        Some(bytes)
    }

    /// The reservation, for a read of the log that builds its response
    /// ([`ReadReservation`]).
    pub(crate) fn for_read(&mut self) -> ReadReservation<'_> {
        ReadReservation {
            reservation: self,
            ahead: 0,
            by_chunks: true,
        }
    }

    /// Returns what the reservation holds beyond `bytes`.
    pub(crate) fn shrink_to(&mut self, bytes: u64) {
        let extra = self.permit.num_permits().saturating_sub(bytes as usize);
        if let Some(extra) = self.permit.split(extra) {
            self.release(extra.num_permits());
        }
    }

    /// Returns `bytes` of what the reservation holds.
    fn give_back(&mut self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if let Some(extra) = self.permit.split(bytes as usize) {
            self.release(extra.num_permits());
        }
    }

    fn release(&self, bytes: usize) {
        let mut held = self
            .budget
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(part) = held.get_mut(&self.holder) {
            *part -= bytes as u64;
            if *part == 0 {
                held.remove(&self.holder);
            }
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.release(self.permit.num_permits());
    }
}

/// The reservation of a response, as a read of the log grows it by what each operation it
/// admits takes in the response: by a chunk ([`READ_RESERVATION_CHUNK`]) when what it reserved
/// ahead runs out, so that the read takes from the budget once while its operations fit in a
/// chunk, and by what an operation lacks if that is more.
///
/// A chunk without room in the client's part or in the whole budget does not refuse the
/// operation: the reservation grows by what it lacks, and so for the rest of the read, which
/// then ends at the same operation as growing once per operation. What no operation took goes
/// back to the budget when the read ends, whichever way, so that the response keeps what its
/// operations take.
#[derive(Debug)]
pub(crate) struct ReadReservation<'r> {
    reservation: &'r mut Reservation,
    /// What the reservation grew by that no operation took yet.
    ahead: u64,
    /// Whether the read still grows the reservation by chunks.
    by_chunks: bool,
}

impl ReadReservation<'_> {
    /// Takes `bytes` for an operation of the read, growing the reservation if what it reserved
    /// ahead falls short, and returns whether it had room.
    pub(crate) fn take(&mut self, bytes: u64) -> bool {
        if let Some(ahead) = self.ahead.checked_sub(bytes) {
            self.ahead = ahead;
            return true;
        }
        let lacking = bytes - self.ahead;
        let mut grown = None;
        if self.by_chunks && lacking < READ_RESERVATION_CHUNK {
            grown = self.reservation.grow_by(READ_RESERVATION_CHUNK);
            self.by_chunks = grown.is_some();
        }
        let Some(grown) = grown.or_else(|| self.reservation.grow_by(lacking)) else {
            return false;
        };
        // Past a frame the reservation grows by less, and for free: only what it grew is ahead.
        self.ahead = (self.ahead + grown).saturating_sub(bytes);
        true
    }
}

impl Drop for ReadReservation<'_> {
    fn drop(&mut self) {
        self.reservation.give_back(self.ahead);
    }
}

#[cfg(test)]
#[path = "tests/response_budget.rs"]
mod tests;
