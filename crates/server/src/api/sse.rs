use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::broadcast;
use zemdb_core::id::RoomId;

use crate::actor::command::{RoomCommand, RoomEvent};
use crate::api::auth::ClientAuth;
use crate::api::router::AppState;
use crate::error::ServerError;

/// `GET /rooms/:room_id/events`: Signal-only Server-Sent Events (SSE) broadcast channel.
/// Authenticated via ClientAuth (Bearer token or ?token= query parameter).
/// Emits `head_advanced` and `schema_reloaded` lightweight signals without transmitting row payloads.
pub async fn room_events(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    auth: ClientAuth,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ServerError> {
    // Validate room_id in path matches authenticated token
    if auth.room_id.as_str() != room_id_str {
        return Err(ServerError::Config(
            "RoomId path and token mismatch".to_string(),
        ));
    }

    let room_id = RoomId::new(room_id_str);
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
            Err(broadcast::error::RecvError::Lagged(_)) => {
                let event = Event::default().event("lagged").data("true");
                Some((Ok(event), rx))
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}
