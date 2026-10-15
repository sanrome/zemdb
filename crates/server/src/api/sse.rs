use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::stream::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};
use zemdb_core::id::{ClientId, RoomId};

use crate::actor::command::{RoomCommand, RoomEvent};
use crate::api::data_plane::binary_error;
use crate::api::extract::EventStreamAuth;
use crate::api::router::AppState;
use crate::error::ServerError;

/// `GET /rooms/:room_id/events`: the room's signal-only Server-Sent Events (SSE) stream.
/// Authenticated via `EventStreamAuth` (Bearer token or `?token=` query parameter, issued for
/// the path room). Every error, before or after authentication (a room that does not exist,
/// an actor that stopped or did not answer in time), is a binary error frame, as in the rest of
/// the Data Plane.
///
/// Emits lightweight signals without transmitting row payloads: `head_advanced` (new head
/// sequence), `schema_reloaded` (schema id), `snapshot_wanted` (a client was designated to
/// upload a snapshot; empty data, the designee learns it is the one from a heartbeat) and
/// `snapshot_available` (sequence of a new usable snapshot). The replies to heartbeats, syncs
/// and commits remain the source of truth; these events only let foreground clients react
/// sooner. The stream ends when the room actor stops, the server begins shutting down, or the
/// same client opens a newer stream for the room that leaves it over `STREAMS_PER_CLIENT`.
/// Over `config.sse_stream_limit()` streams in all, a new one is refused with `RateLimited`.
pub async fn room_events(
    State(state): State<AppState>,
    EventStreamAuth { room_id, client_id }: EventStreamAuth,
) -> Response {
    let mut slot = match state.sse_streams.open(&room_id, &client_id) {
        Ok(slot) => slot,
        Err(err) => return binary_error(None, Some(room_id), err),
    };
    let receiver = match subscribe(&state, &room_id).await {
        Ok(receiver) => receiver,
        Err(err) => return binary_error(None, Some(room_id), err),
    };
    slot.replace_older();

    let shutdown = state.shutdown.clone();
    let stream = futures::stream::unfold(receiver, |mut rx| async move {
        let event = Event::default();
        let event = match rx.recv().await {
            Ok(RoomEvent::HeadAdvanced(seq)) => event.event("head_advanced").data(seq.to_string()),
            Ok(RoomEvent::SchemaReloaded(schema_id)) => {
                event.event("schema_reloaded").data(&schema_id)
            }
            // An empty data line: browsers drop events without one.
            Ok(RoomEvent::SnapshotWanted) => event.event("snapshot_wanted").data(""),
            Ok(RoomEvent::SnapshotAvailable(seq)) => {
                event.event("snapshot_available").data(seq.to_string())
            }
            Err(broadcast::error::RecvError::Lagged(_)) => event.event("lagged").data("true"),
            Err(broadcast::error::RecvError::Closed) => return None,
        };
        Some((Ok::<_, Infallible>(event), rx))
    })
    // End the stream when the server shuts down (otherwise the graceful shutdown would wait
    // for this connection forever) or when a newer stream takes its slot. The slot is held
    // until then.
    .take_until(async move {
        tokio::select! {
            () = shutdown.wait() => {}
            _ = &mut slot.replaced => {}
        }
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE_INTERVAL))
        .into_response()
}

/// Interval of the SSE keep-alive comments, which keep idle proxies from closing the stream.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Most SSE streams a client keeps open for a room. More than one, so that a stream reopened
/// after a network change does not have to wait for the old one to be detected as dead.
const STREAMS_PER_CLIENT: usize = 2;

/// The open SSE streams: at most `max` in all and [`STREAMS_PER_CLIENT`] per room and client.
pub(crate) struct SseStreams {
    max: usize,
    open: Mutex<OpenStreams>,
}

/// The room and client of a stream.
type StreamKey = (RoomId, ClientId);

#[derive(Default)]
struct OpenStreams {
    /// Streams in `by_client`. It may exceed `max` by one per client with a replacement
    /// starting.
    total: usize,
    next_id: u64,
    /// The streams of each room and client, oldest first, with the sender whose drop ends
    /// each one. An entry is removed with its last stream.
    by_client: HashMap<StreamKey, VecDeque<(u64, oneshot::Sender<()>)>>,
}

impl SseStreams {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            max,
            open: Mutex::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, OpenStreams> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes a slot for a new stream of `client_id` in `room_id`, or refuses it with
    /// `RateLimited` when `max` streams are open, unless the client is at
    /// [`STREAMS_PER_CLIENT`]: then the slot replaces its oldest stream once the new one starts
    /// ([`SseSlot::replace_older`]), so a stream that fails to start replaces none. A client
    /// has one such replacement starting at a time; another is refused with `RateLimited`.
    fn open(
        self: &Arc<Self>,
        room_id: &RoomId,
        client_id: &ClientId,
    ) -> Result<SseSlot, ServerError> {
        let key = (room_id.clone(), client_id.clone());
        let mut open = self.lock();
        let open = &mut *open;
        let len = open.by_client.get(&key).map_or(0, VecDeque::len);
        if len > STREAMS_PER_CLIENT || (len < STREAMS_PER_CLIENT && open.total >= self.max) {
            return Err(ServerError::RateLimited);
        }
        let id = open.next_id;
        open.next_id += 1;
        let (sender, replaced) = oneshot::channel();
        open.by_client
            .entry(key.clone())
            .or_default()
            .push_back((id, sender));
        open.total += 1;
        Ok(SseSlot {
            streams: Arc::clone(self),
            key,
            id,
            replaced,
        })
    }
}

/// The slot of an open SSE stream, released when dropped.
struct SseSlot {
    streams: Arc<SseStreams>,
    key: StreamKey,
    id: u64,
    /// Resolves (with an error) once a newer stream of the same client took the slot.
    replaced: oneshot::Receiver<()>,
}

impl SseSlot {
    /// Ends the oldest streams of the client beyond [`STREAMS_PER_CLIENT`].
    fn replace_older(&self) {
        let mut open = self.streams.lock();
        let open = &mut *open;
        if let Some(streams) = open.by_client.get_mut(&self.key) {
            while streams.len() > STREAMS_PER_CLIENT {
                streams.pop_front();
                open.total -= 1;
            }
        }
    }
}

impl Drop for SseSlot {
    fn drop(&mut self) {
        let mut open = self.streams.lock();
        let Some(streams) = open.by_client.get_mut(&self.key) else {
            return;
        };
        // A replaced stream already gave up its slot.
        let Some(index) = streams.iter().position(|(id, _)| *id == self.id) else {
            return;
        };
        streams.remove(index);
        if streams.is_empty() {
            open.by_client.remove(&self.key);
        }
        open.total -= 1;
    }
}

/// How many times a subscription refused with `Unavailable` is attempted.
const SUBSCRIBE_ATTEMPTS: usize = 3;

/// Subscribes to the room's event channel, spawning the room actor if needed.
///
/// An actor that is shutting down for inactivity refuses new subscriptions with
/// `Unavailable` (they would end at once); subscribing has no side effects, so it is retried
/// against the room reopened from disk instead of failing the request.
async fn subscribe(
    state: &AppState,
    room_id: &RoomId,
) -> Result<broadcast::Receiver<RoomEvent>, ServerError> {
    let mut attempt = 1;
    loop {
        let result = state
            .room_manager
            .ask(room_id, |reply| RoomCommand::SubscribeEvents { reply })
            .await?;
        match result {
            Err(ServerError::Unavailable(_)) if attempt < SUBSCRIBE_ATTEMPTS => attempt += 1,
            result => return result,
        }
    }
}

#[cfg(test)]
#[path = "tests/sse.rs"]
mod tests;
