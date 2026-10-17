//! The budget of response bytes in flight (`max_inflight_response_bytes`), and the part of it
//! each client may hold (`max_inflight_response_bytes_per_client`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zemdb_core::id::RoomId;
use zemdb_core::protocol::codec::MAX_FRAME_SIZE;

use crate::error::ServerError;
use crate::relay::Uploader;

/// The error of a response without room in the budget: `Unavailable` (503), to be retried.
pub(crate) fn no_room() -> ServerError {
    ServerError::Unavailable("Too many response bytes in flight".to_string())
}

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
        }
    }

    /// Reserves a frame, the most any response takes, for `holder`: before a response is
    /// materialized, so that what its reads and its encoding take is within the budget.
    pub(crate) fn reserve_frame(self: &Arc<Self>, holder: BudgetHolder) -> Option<Reservation> {
        self.reserve(holder, MAX_FRAME_SIZE as u32)
    }

    /// Reserves `bytes` for `holder`, if both its part and the whole budget have room.
    pub(crate) fn reserve(
        self: &Arc<Self>,
        holder: BudgetHolder,
        bytes: u32,
    ) -> Option<Reservation> {
        let permit = Arc::clone(&self.total).try_acquire_many_owned(bytes).ok()?;
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let part = held.get(&holder).copied().unwrap_or(0) + u64::from(bytes);
        if part > self.per_holder {
            return None;
        }
        held.insert(holder.clone(), part);
        Some(Reservation {
            budget: Arc::clone(self),
            holder,
            permit,
        })
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
    /// Returns what the reservation holds beyond `bytes`.
    pub(crate) fn shrink_to(&mut self, bytes: u64) {
        let extra = self.permit.num_permits().saturating_sub(bytes as usize);
        if let Some(extra) = self.permit.split(extra) {
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
