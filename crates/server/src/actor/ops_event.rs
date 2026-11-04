//! The SSE event `ops`: the operations of a commit, or of a group of commits, that became
//! durable, encoded once and shared by every stream of the room; and the total of the bytes
//! such events retain in the server (`max_sse_ops_bytes`).

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zemdb_core::id::{RoomId, SchemaId, SequenceNumber};
use zemdb_core::protocol::codec::{encode_message, encoded_len};
use zemdb_core::protocol::messages::{SequencedOperation, ServerMessage};

/// Largest frame an `ops` event carries, before base64 (about 5.5 KiB in the event). Operations
/// whose frame is larger are announced with `head_advanced`, and clients read them with a sync.
pub(crate) const MAX_OPS_EVENT_FRAME: u64 = 4 * 1024;

/// What an event retains besides its base64: the block shared by its copies and the headers of
/// its two allocations, rounded up.
const EVENT_OVERHEAD: usize = 128;

/// An `ops` event: a `ServerMessage::OpsEvent` frame in base64, built once and shared by the
/// streams of the room. It holds its bytes of the server's total of retained events until its
/// last copy is dropped: when every stream of the room read it, or its channel dropped it.
#[derive(Clone)]
pub struct OpsEvent(Arc<Retained>);

struct Retained {
    head_seq: SequenceNumber,
    data: Box<str>,
    /// The bytes of `data`, taken from the server's total and returned when dropped.
    _bytes: OwnedSemaphorePermit,
}

impl OpsEvent {
    /// The head the event's operations reach.
    pub fn head_seq(&self) -> SequenceNumber {
        self.0.head_seq
    }

    /// The data of the event: the frame, in base64.
    pub fn data(&self) -> &str {
        &self.0.data
    }

    /// The bytes the event holds of the server's total.
    #[cfg(test)]
    pub(crate) fn retained(&self) -> usize {
        self.0.data.len() + EVENT_OVERHEAD
    }
}

impl PartialEq for OpsEvent {
    fn eq(&self, other: &Self) -> bool {
        self.head_seq() == other.head_seq() && self.data() == other.data()
    }
}

impl Eq for OpsEvent {}

impl fmt::Debug for OpsEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpsEvent")
            .field("head_seq", &self.head_seq())
            .field("data", &self.data())
            .finish()
    }
}

/// The bytes of `ops` events that the event channels of all rooms retain: at most
/// `max_sse_ops_bytes`, counted as they are held: the base64 of each event, plus
/// [`EVENT_OVERHEAD`].
#[derive(Debug)]
pub(crate) struct OpsEventBudget {
    bytes: Arc<Semaphore>,
    /// The total, for tests to tell what the retained events hold.
    #[cfg(test)]
    max: usize,
}

impl OpsEventBudget {
    pub(crate) fn new(max_bytes: u64) -> Self {
        let max = usize::try_from(max_bytes)
            .unwrap_or(usize::MAX)
            .min(Semaphore::MAX_PERMITS);
        Self {
            bytes: Arc::new(Semaphore::new(max)),
            #[cfg(test)]
            max,
        }
    }

    /// The `ops` event of `ops`, the new operations of a durable commit or group of commits of
    /// `room_id` in its incarnation `room_incarnation`, checked with the schema `schema_id` at
    /// `schema_version`. `None`, for the
    /// caller to announce them with `head_advanced`, if there are none, if their frame is
    /// larger than [`MAX_OPS_EVENT_FRAME`], or if what the total has left cannot hold it.
    pub(crate) fn event<'a>(
        &self,
        room_id: &RoomId,
        room_incarnation: u128,
        schema_id: &SchemaId,
        schema_version: u64,
        ops: impl Iterator<Item = &'a SequencedOperation>,
    ) -> Option<OpsEvent> {
        // The operations alone take less than their frame: past the cap, none is copied and
        // the rest are not measured.
        let mut ops_len = 0;
        let mut carried = Vec::new();
        for op in ops {
            ops_len += encoded_len(op).ok()?;
            if ops_len > MAX_OPS_EVENT_FRAME {
                return None;
            }
            carried.push(op.clone());
        }
        let head_seq = carried.last()?.seq;
        // Nor is the frame encoded if the total cannot hold even the operations alone.
        let least = base64::encoded_len(ops_len as usize, true)? + EVENT_OVERHEAD;
        if self.bytes.available_permits() < least {
            return None;
        }
        let frame = encode_message(&ServerMessage::OpsEvent {
            room_id: room_id.clone(),
            room_incarnation,
            head_seq,
            ops: carried,
            schema_version,
            schema_id: schema_id.clone(),
        })
        .ok()?;
        if frame.len() as u64 > MAX_OPS_EVENT_FRAME {
            return None;
        }
        let retained = base64::encoded_len(frame.len(), true)? + EVENT_OVERHEAD;
        let bytes = Arc::clone(&self.bytes)
            .try_acquire_many_owned(u32::try_from(retained).ok()?)
            .ok()?;
        Some(OpsEvent(Arc::new(Retained {
            head_seq,
            data: STANDARD.encode(frame).into_boxed_str(),
            _bytes: bytes,
        })))
    }

    /// The bytes the retained events hold now.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.max - self.bytes.available_permits()
    }
}

#[cfg(test)]
#[path = "tests/ops_event.rs"]
mod tests;
