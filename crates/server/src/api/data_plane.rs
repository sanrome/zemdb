use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use http_body::Body as _;
use tokio::sync::oneshot;
use tracing::error;
use zemdb_core::id::{ClientId, CorrelationId, RoomId};
use zemdb_core::protocol::codec::encode_message;
use zemdb_core::protocol::messages::{ClientMessage, ErrorCode, ServerMessage};

use crate::actor::command::RoomCommand;
use crate::api::auth::verify_client_token_bound;
use crate::api::extract::{
    ensure_payload_identity, AuthenticatedRoom, BinaryMessage, RegisterMessage, RoomPath,
};
use crate::api::router::{AppState, ResponseBudget};
use crate::error::ServerError;
use crate::relay::Uploader;
use crate::response_budget::{no_room, BudgetHolder, Reservation};

/// Encodes `msg` as a binary frame response. A message that cannot be encoded (a bug: every
/// response is sized to fit a frame) is logged and answered with a small `Internal` error
/// frame, so the reply is still a frame.
pub(crate) fn binary_response(status: StatusCode, msg: &ServerMessage) -> Response {
    let (status, bytes) = match encode_message(msg) {
        Ok(bytes) => (status, bytes),
        Err(err) => {
            error!(error = %err, "Failed to encode a response frame");
            let fallback = ServerMessage::Error {
                correlation_id: None,
                room_id: None,
                code: ErrorCode::Internal,
                message: "The response could not be encoded".to_string(),
            };
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                encode_message(&fallback).unwrap_or_default(),
            )
        }
    };
    (
        status,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        bytes,
    )
        .into_response()
}

/// Reserves a frame of `holder`'s part of the budget of response bytes in flight, before the
/// response is materialized and encoded (see
/// [`InflightBudget::reserve_frame`](crate::response_budget::InflightBudget::reserve_frame)).
pub(crate) fn reserve_frame(state: &AppState, holder: BudgetHolder) -> Option<Reservation> {
    state.room_manager.response_budget().reserve_frame(holder)
}

/// The budget holder of a room's client.
fn client_holder(room_id: &RoomId, client_id: &ClientId) -> BudgetHolder {
    (room_id.clone(), Uploader::Client(client_id.clone()))
}

/// 200 with `msg`, holding as much of `reservation` as its encoding takes.
pub(crate) fn budgeted_response(mut reservation: Reservation, msg: &ServerMessage) -> Response {
    let mut response = binary_response(StatusCode::OK, msg);
    reservation.shrink_to(response.body().size_hint().exact().unwrap_or(u64::MAX));
    response
        .extensions_mut()
        .insert(ResponseBudget::new(reservation));
    response
}

/// `Unavailable` (503, to be retried): the budget of response bytes in flight has no room.
pub(crate) fn no_budget(correlation_id: CorrelationId, room_id: RoomId) -> Response {
    binary_error(Some(correlation_id), Some(room_id), no_room())
}

/// The `ServerMessage::Error` frame reporting `err`.
pub(crate) fn error_frame(
    correlation_id: Option<CorrelationId>,
    room_id: Option<RoomId>,
    err: &ServerError,
) -> ServerMessage {
    ServerMessage::Error {
        correlation_id,
        room_id,
        code: err.to_error_code(),
        message: err.to_string(),
    }
}

pub(crate) fn binary_error(
    correlation_id: Option<CorrelationId>,
    room_id: Option<RoomId>,
    err: ServerError,
) -> Response {
    let msg = error_frame(correlation_id, room_id, &err);
    let mut response = binary_response(err.to_status_code(), &msg);
    err.add_headers(&mut response);
    response
}

/// Reply to a message of another kind than the endpoint handles.
pub(crate) fn unexpected_message(room_id: Option<RoomId>, expected: &str) -> Response {
    binary_error(
        None,
        room_id,
        ServerError::BadRequest(format!("Expected {expected} message")),
    )
}

/// Sends a command to the room actor and waits for its reply, returning the actor's own
/// result. The exchange itself can fail with `RoomNotFound`, `Unavailable`, `Internal` or
/// `Timeout` (see [`RoomManager::ask`](crate::actor::manager::RoomManager::ask)).
async fn ask_room<R>(
    state: &AppState,
    room_id: &RoomId,
    command: impl FnOnce(oneshot::Sender<Result<R, ServerError>>) -> RoomCommand,
) -> Result<R, ServerError> {
    state.room_manager.ask(room_id, command).await?
}

/// 200 with the frame `ok` builds from the room's reply within `holder`'s budget, or the error
/// frame of the request.
fn respond_budgeted<R>(
    state: &AppState,
    holder: BudgetHolder,
    correlation_id: CorrelationId,
    result: Result<R, ServerError>,
    ok: impl FnOnce(RoomId, R) -> ServerMessage,
) -> Response {
    let room_id = holder.0.clone();
    match (result, reserve_frame(state, holder)) {
        (Ok(value), Some(reservation)) => budgeted_response(reservation, &ok(room_id, value)),
        (Ok(_), None) => no_budget(correlation_id, room_id),
        (Err(err), _) => binary_error(Some(correlation_id), Some(room_id), err),
    }
}

/// 200 with the frame `ok` builds from the room's reply, or the error frame of the request.
fn respond<R>(
    correlation_id: CorrelationId,
    room_id: RoomId,
    result: Result<R, ServerError>,
    ok: impl FnOnce(RoomId, R) -> ServerMessage,
) -> Response {
    match result {
        Ok(value) => binary_response(StatusCode::OK, &ok(room_id, value)),
        Err(err) => binary_error(Some(correlation_id), Some(room_id), err),
    }
}

/// `POST /rooms/:room_id/register`: registers the client and returns the room's log range and
/// schema.
///
/// The client token travels inside the message, so it is verified here, bound to the client
/// and room the message names. Until then the request is unauthenticated, so
/// [`RegisterMessage`] rejects any other message kind without decoding it.
pub async fn register(
    State(state): State<AppState>,
    RoomPath(path_room_id): RoomPath,
    RegisterMessage {
        correlation_id,
        room_id,
        client_id,
        auth_token,
        current_seq,
        known_incarnation,
    }: RegisterMessage,
) -> Response {
    let fail = |room_id: RoomId, err| binary_error(Some(correlation_id), Some(room_id), err);

    if let Err(err) = ensure_payload_identity(&path_room_id, None, &room_id, None) {
        return fail(room_id, err);
    }
    if let Err(err) =
        verify_client_token_bound(&auth_token, &client_id, &room_id, &state.config.auth_secret)
    {
        return fail(room_id, err);
    }

    let holder = client_holder(&room_id, &client_id);
    let result = ask_room(&state, &room_id, |reply| RoomCommand::RegisterClient {
        client_id,
        current_seq,
        known_incarnation,
        reply,
    })
    .await;
    respond_budgeted(
        &state,
        holder,
        correlation_id,
        result,
        |room_id, reg_resp| ServerMessage::Registered {
            correlation_id,
            room_id,
            head_seq: reg_resp.head_seq,
            tail_seq: reg_resp.tail_seq,
            schema_id: reg_resp.schema_id,
            schema: (*reg_resp.schema).clone(),
            active_snapshot_seq: reg_resp.active_snapshot_seq,
            room_incarnation: reg_resp.room_incarnation,
        },
    )
}

/// `POST /rooms/:room_id/commit`: sequences one mutation and returns its sequence with the
/// operations after the client's cursor (catch-up).
pub async fn commit(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage,
) -> Response {
    let ClientMessage::Commit {
        correlation_id,
        room_id,
        client_id,
        mutation_id,
        last_ack_seq,
        op,
    } = msg
    else {
        return unexpected_message(Some(auth.room_id), "Commit");
    };

    if let Err(err) = auth.ensure_payload(&room_id, &client_id) {
        return binary_error(Some(correlation_id), Some(room_id), err);
    }

    let result = ask_room(&state, &room_id, |reply| RoomCommand::Commit {
        client_id,
        mutation_id,
        last_ack_seq,
        op,
        reply,
    })
    .await;
    let commit_resp = match result {
        Ok(commit_resp) => commit_resp,
        Err(err) => return binary_error(Some(correlation_id), Some(room_id), err),
    };
    let ack = ServerMessage::CommitAck {
        correlation_id,
        room_id,
        mutation_id,
        assigned_seq: commit_resp.assigned_seq,
        catchup_ops: commit_resp.catchup_ops,
        has_more: commit_resp.has_more,
        snapshot_wanted: commit_resp.snapshot_wanted,
        schema_version: commit_resp.schema_version,
        schema_id: commit_resp.schema_id,
    };
    // The commit is durable, so it is never answered with an error: without room in the
    // budget, the room acknowledged it without its catch-up, which sends the client to `/sync`.
    match commit_resp.reservation {
        Some(reservation) => budgeted_response(reservation, &ack),
        None => binary_response(StatusCode::OK, &ack),
    }
}

/// `POST /rooms/:room_id/sync`: the next batch of operations after the client's cursor.
pub async fn sync(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage,
) -> Response {
    let ClientMessage::Sync {
        correlation_id,
        room_id,
        client_id,
        from_seq,
        max_batch_size,
    } = msg
    else {
        return unexpected_message(Some(auth.room_id), "Sync");
    };

    if let Err(err) = auth.ensure_payload(&room_id, &client_id) {
        return binary_error(Some(correlation_id), Some(room_id), err);
    }

    let result = ask_room(&state, &room_id, |reply| RoomCommand::Sync {
        client_id,
        from_seq,
        max_batch_size,
        reply,
    })
    .await;
    match result {
        Ok(sync_resp) => {
            let batch = ServerMessage::SyncBatch {
                correlation_id,
                room_id: room_id.clone(),
                head_seq: sync_resp.head_seq,
                ops: sync_resp.ops,
                has_more: sync_resp.has_more,
                snapshot_wanted: sync_resp.snapshot_wanted,
                schema_version: sync_resp.schema_version,
                schema_id: sync_resp.schema_id,
            };
            budgeted_response(sync_resp.reservation, &batch)
        }
        Err(err) => binary_error(Some(correlation_id), Some(room_id), err),
    }
}

/// `POST /rooms/:room_id/ack`: records how far the client has persisted.
pub async fn ack(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage,
) -> Response {
    let ClientMessage::Ack {
        correlation_id,
        room_id,
        client_id,
        ack_seq,
    } = msg
    else {
        return unexpected_message(Some(auth.room_id), "Ack");
    };

    if let Err(err) = auth.ensure_payload(&room_id, &client_id) {
        return binary_error(Some(correlation_id), Some(room_id), err);
    }

    let result = ask_room(&state, &room_id, |reply| RoomCommand::Ack {
        client_id,
        ack_seq,
        reply,
    })
    .await;
    respond(correlation_id, room_id, result, |room_id, head_seq| {
        ServerMessage::AckConfirmed {
            correlation_id,
            room_id,
            ack_seq,
            head_seq,
        }
    })
}

/// `POST /rooms/:room_id/heartbeat`: refreshes the client's lease. Never fails because
/// the client fell behind the log; the reply carries the snapshot signals.
pub async fn heartbeat(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage,
) -> Response {
    let ClientMessage::Heartbeat {
        correlation_id,
        room_id,
        client_id,
    } = msg
    else {
        return unexpected_message(Some(auth.room_id), "Heartbeat");
    };

    if let Err(err) = auth.ensure_payload(&room_id, &client_id) {
        return binary_error(Some(correlation_id), Some(room_id), err);
    }

    let result = ask_room(&state, &room_id, |reply| RoomCommand::Heartbeat {
        client_id,
        reply,
    })
    .await;
    respond(correlation_id, room_id, result, |room_id, hb_resp| {
        ServerMessage::HeartbeatAck {
            correlation_id,
            room_id,
            current_head_seq: hb_resp.head_seq,
            snapshot_wanted: hb_resp.snapshot_wanted,
            active_snapshot_seq: hb_resp.active_snapshot_seq,
            schema_version: hb_resp.schema_version,
            schema_id: hb_resp.schema_id,
        }
    })
}

/// `POST /rooms/:room_id/schema`: the room's current schema.
pub async fn get_schema(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage,
) -> Response {
    let ClientMessage::GetSchema {
        correlation_id,
        room_id,
    } = msg
    else {
        return unexpected_message(Some(auth.room_id), "GetSchema");
    };

    if let Err(err) = ensure_payload_identity(&auth.room_id, None, &room_id, None) {
        return binary_error(Some(correlation_id), Some(room_id), err);
    }

    let holder = client_holder(&room_id, &auth.client_id);
    let result = ask_room(&state, &room_id, |reply| RoomCommand::GetSchema { reply }).await;
    respond_budgeted(
        &state,
        holder,
        correlation_id,
        result,
        |room_id, (schema_id, schema)| ServerMessage::Schema {
            correlation_id,
            room_id,
            schema_id,
            schema: (*schema).clone(),
        },
    )
}

/// `POST /rooms/:room_id/deregister`: removes the client from the room.
pub async fn deregister(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage,
) -> Response {
    let ClientMessage::DeregisterClient {
        correlation_id,
        room_id,
        client_id,
    } = msg
    else {
        return unexpected_message(Some(auth.room_id), "DeregisterClient");
    };

    if let Err(err) = auth.ensure_payload(&room_id, &client_id) {
        return binary_error(Some(correlation_id), Some(room_id), err);
    }

    let result = ask_room(&state, &room_id, |reply| RoomCommand::DeregisterClient {
        client_id: client_id.clone(),
        reply,
    })
    .await;
    respond(correlation_id, room_id, result, |room_id, ()| {
        ServerMessage::DeregisterAck {
            correlation_id,
            room_id,
            client_id,
        }
    })
}

#[cfg(test)]
#[path = "tests/data_plane.rs"]
mod tests;
