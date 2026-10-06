use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::stream::StreamExt;
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::broadcast;
use zemdb_core::id::RoomId;

use crate::actor::command::{RoomCommand, RoomEvent};
use crate::api::data_plane::binary_error;
use crate::api::extract::EventStreamAuth;
use crate::api::router::AppState;
use crate::error::ServerError;

/// `GET /rooms/:room_id/events`: Signal-only Server-Sent Events (SSE) broadcast channel.
/// Authenticated via `EventStreamAuth` (Bearer token or `?token=` query parameter, issued for
/// the path room). Every error, before or after authentication (a room that does not exist,
/// an actor that stopped or did not answer in time), is a binary error frame, as in the rest of
/// the Data Plane.
/// Emits lightweight signals without transmitting row payloads: `head_advanced` (new head
/// sequence), `schema_reloaded` (schema id), `snapshot_wanted` (a client was designated to
/// upload a snapshot; empty data, the designee learns it is the one from a heartbeat) and
/// `snapshot_available` (sequence of a new usable snapshot). The replies to heartbeats, syncs
/// and commits remain the source of truth; these events only let foreground clients react sooner.
/// The stream ends when the room actor stops or the server begins shutting down.
pub async fn room_events(
    State(state): State<AppState>,
    EventStreamAuth { room_id, .. }: EventStreamAuth,
) -> Response {
    let receiver = match subscribe(&state, &room_id).await {
        Ok(receiver) => receiver,
        Err(err) => return binary_error(None, Some(room_id), err),
    };

    let shutdown = state.shutdown.clone();
    let stream = futures::stream::unfold(receiver, |mut rx| async move {
        match rx.recv().await {
            Ok(RoomEvent::HeadAdvanced(seq)) => {
                let event = Event::default()
                    .event("head_advanced")
                    .data(seq.to_string());
                Some((Ok::<_, Infallible>(event), rx))
            }
            Ok(RoomEvent::SchemaReloaded(schema_id)) => {
                let event = Event::default().event("schema_reloaded").data(&schema_id);
                Some((Ok(event), rx))
            }
            Ok(RoomEvent::SnapshotWanted) => {
                // An empty data line: browsers drop events without one.
                let event = Event::default().event("snapshot_wanted").data("");
                Some((Ok(event), rx))
            }
            Ok(RoomEvent::SnapshotAvailable(seq)) => {
                let event = Event::default()
                    .event("snapshot_available")
                    .data(seq.to_string());
                Some((Ok(event), rx))
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                let event = Event::default().event("lagged").data("true");
                Some((Ok(event), rx))
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    })
    // End the stream when the server shuts down; otherwise the graceful shutdown would wait
    // for this connection forever.
    .take_until(async move { shutdown.wait().await });

    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

/// Subscribes to the room's event channel, spawning the room actor if needed.
async fn subscribe(
    state: &AppState,
    room_id: &RoomId,
) -> Result<broadcast::Receiver<RoomEvent>, ServerError> {
    state
        .room_manager
        .ask(room_id, |reply| RoomCommand::SubscribeEvents { reply })
        .await
}
