use std::convert::Infallible;
use std::time::Duration;
use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use rimdb_core::id::RoomId;
use tokio::sync::broadcast;

use crate::actor::command::RoomCommand;
use crate::api::router::AppState;
use crate::error::ServerError;

/// `GET /rooms/:room_id/events`: Signal-only Server-Sent Events (SSE) broadcast channel.
/// Emits `head_advanced` lightweight sequence signals without transmitting row payloads.
pub async fn room_events(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ServerError> {
    let room_id = RoomId::new(room_id_str);
    let sender = state
        .room_manager
        .get_room(&room_id)
        .or_else(|| state.room_manager.get_or_spawn(&room_id, None).ok())
        .ok_or_else(|| ServerError::RoomNotFound(room_id.to_string()))?;

    let (tx, rx) = tokio::sync::oneshot::channel();
    sender
        .send(RoomCommand::SubscribeEvents { reply: tx })
        .await
        .map_err(|_| ServerError::Internal("Room actor closed".to_string()))?;

    let receiver = rx
        .await
        .map_err(|_| ServerError::Internal("No response from room actor".to_string()))?;

    let stream = futures::stream::unfold(receiver, |mut rx| async move {
        match rx.recv().await {
            Ok(seq) => {
                let event = Event::default()
                    .event("head_advanced")
                    .data(seq.to_string());
                Some((Ok(event), rx))
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                let event = Event::default().event("lagged").data("true");
                Some((Ok(event), rx))
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}
