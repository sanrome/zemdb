use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::{Stream, StreamExt};
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::broadcast;

use crate::actor::command::{RoomCommand, RoomEvent};
use crate::api::extract::EventStreamAuth;
use crate::api::router::AppState;
use crate::error::ServerError;

/// `GET /rooms/:room_id/events`: Signal-only Server-Sent Events (SSE) broadcast channel.
/// Authenticated via `EventStreamAuth` (Bearer token or `?token=` query parameter, issued for
/// the path room); authentication failures are binary error frames.
/// Emits lightweight signals without transmitting row payloads: `head_advanced` (new head
/// sequence), `schema_reloaded` (schema id), `snapshot_wanted` (a client was designated to
/// upload a snapshot; empty data, the designee learns it is the one from a heartbeat) and
/// `snapshot_available` (sequence of a new usable snapshot). The replies to heartbeats, syncs
/// and commits remain the source of truth; these events only let foreground clients react sooner.
/// The stream ends when the room actor stops or the server begins shutting down.
pub async fn room_events(
    State(state): State<AppState>,
    EventStreamAuth { room_id, .. }: EventStreamAuth,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ServerError> {
    let sender = match state.room_manager.get_room(&room_id) {
        Some(s) => s,
        None => state.room_manager.get_or_spawn(&room_id, None).await?,
    };

    let (tx, rx) = tokio::sync::oneshot::channel();
    let call = async {
        sender
            .send(RoomCommand::SubscribeEvents { reply: tx })
            .await
            .map_err(|_| ServerError::Internal("Room actor closed".to_string()))?;
        rx.await
            .map_err(|_| ServerError::Internal("No response from room actor".to_string()))
    };

    let receiver = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .map_err(|_| {
            ServerError::GatewayTimeout("Timeout subscribing to room events".to_string())
        })??;

    let shutdown = state.shutdown.clone();
    let stream = futures::stream::unfold(receiver, |mut rx| async move {
        match rx.recv().await {
            Ok(RoomEvent::HeadAdvanced(seq)) => {
                let event = Event::default()
                    .event("head_advanced")
                    .data(seq.to_string());
                Some((Ok(event), rx))
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

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}
