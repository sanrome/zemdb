use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use std::time::Duration;
use tokio::sync::oneshot;
use zemdb_core::id::{CorrelationId, RoomId};
use zemdb_core::protocol::codec::encode_message;
use zemdb_core::protocol::messages::{ClientMessage, ServerMessage};

use crate::actor::command::RoomCommand;
use crate::api::auth::verify_client_token_bound;
use crate::api::extract::{ensure_payload_identity, AuthenticatedRoom, BinaryMessage, RoomPath};
use crate::api::router::AppState;
use crate::error::ServerError;

const ACTOR_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn binary_response(status: StatusCode, msg: &ServerMessage) -> Response {
    match encode_message(msg) {
        Ok(bytes) => (
            status,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            Bytes::new(),
        )
            .into_response(),
    }
}

pub(crate) fn binary_error(
    correlation_id: Option<CorrelationId>,
    room_id: Option<RoomId>,
    err: ServerError,
) -> Response {
    let status = err.to_status_code();
    let msg = ServerMessage::Error {
        correlation_id,
        room_id,
        code: err.to_error_code(),
        message: err.to_string(),
    };
    binary_response(status, &msg)
}

/// Reply to a message of another kind than the endpoint handles.
fn unexpected_message(room_id: RoomId, expected: &str) -> Response {
    binary_error(
        None,
        Some(room_id),
        ServerError::BadRequest(format!("Expected {expected} message")),
    )
}

/// Sends a command to the room actor and waits for its reply, bounded by `ACTOR_TIMEOUT`.
async fn ask_room<R>(
    state: &AppState,
    room_id: &RoomId,
    command: impl FnOnce(oneshot::Sender<Result<R, ServerError>>) -> RoomCommand,
) -> Result<R, ServerError> {
    let sender = state.room_manager.get_or_spawn(room_id, None).await?;
    let (tx, rx) = oneshot::channel();
    let call = async {
        sender
            .send(command(tx))
            .await
            .map_err(|_| ServerError::Internal("Room actor closed".to_string()))?;
        rx.await
            .map_err(|_| ServerError::Internal("Actor reply dropped".to_string()))?
    };
    tokio::time::timeout(ACTOR_TIMEOUT, call)
        .await
        .map_err(|_| {
            ServerError::GatewayTimeout("Request timed out waiting for room actor".to_string())
        })?
}

/// `POST /rooms/:room_id/register`: Handshake endpoint delivering current head_seq and schema in 1 RTT.
///
/// The client token travels inside the message, so it is verified here, bound to the client
/// and room the message names.
pub async fn register(
    State(state): State<AppState>,
    RoomPath(path_room_id): RoomPath,
    BinaryMessage(msg): BinaryMessage<ClientMessage>,
) -> Response {
    let ClientMessage::RegisterClient {
        correlation_id,
        room_id,
        client_id,
        auth_token,
        current_seq,
    } = msg
    else {
        return unexpected_message(path_room_id, "RegisterClient");
    };
    let fail = |room_id: RoomId, err| binary_error(Some(correlation_id), Some(room_id), err);

    if let Err(err) = ensure_payload_identity(&path_room_id, None, &room_id, None) {
        return fail(room_id, err);
    }
    if let Err(err) =
        verify_client_token_bound(&auth_token, &client_id, &room_id, &state.config.auth_secret)
    {
        return fail(room_id, err);
    }

    match ask_room(&state, &room_id, |reply| RoomCommand::RegisterClient {
        client_id,
        current_seq,
        reply,
    })
    .await
    {
        Ok(reg_resp) => binary_response(
            StatusCode::OK,
            &ServerMessage::Registered {
                correlation_id,
                room_id,
                head_seq: reg_resp.head_seq,
                tail_seq: reg_resp.tail_seq,
                schema_id: reg_resp.schema_id,
                schema: (*reg_resp.schema).clone(),
                active_snapshot_seq: reg_resp.active_snapshot_seq,
            },
        ),
        Err(err) => fail(room_id, err),
    }
}

/// `POST /rooms/:room_id/commit`: Atomic commit with 1-RTT catchup deltas and monotonic sequencing.
pub async fn commit(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage<ClientMessage>,
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
        return unexpected_message(auth.room_id, "Commit");
    };
    let fail = |room_id: RoomId, err| binary_error(Some(correlation_id), Some(room_id), err);

    if let Err(err) = ensure_payload_identity(
        &auth.room_id,
        Some(&auth.client_id),
        &room_id,
        Some(&client_id),
    ) {
        return fail(room_id, err);
    }

    match ask_room(&state, &room_id, |reply| RoomCommand::Commit {
        client_id,
        mutation_id,
        last_ack_seq,
        op,
        reply,
    })
    .await
    {
        Ok(commit_resp) => binary_response(
            StatusCode::OK,
            &ServerMessage::CommitAck {
                correlation_id,
                room_id,
                mutation_id,
                assigned_seq: commit_resp.assigned_seq,
                catchup_ops: commit_resp.catchup_ops,
                has_more: commit_resp.has_more,
            },
        ),
        Err(err) => fail(room_id, err),
    }
}

/// `POST /rooms/:room_id/sync`: Paginated batch synchronization with flow control.
pub async fn sync(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage<ClientMessage>,
) -> Response {
    let ClientMessage::Sync {
        correlation_id,
        room_id,
        client_id,
        from_seq,
        max_batch_size,
    } = msg
    else {
        return unexpected_message(auth.room_id, "Sync");
    };
    let fail = |room_id: RoomId, err| binary_error(Some(correlation_id), Some(room_id), err);

    if let Err(err) = ensure_payload_identity(
        &auth.room_id,
        Some(&auth.client_id),
        &room_id,
        Some(&client_id),
    ) {
        return fail(room_id, err);
    }

    match ask_room(&state, &room_id, |reply| RoomCommand::Sync {
        client_id,
        from_seq,
        max_batch_size,
        reply,
    })
    .await
    {
        Ok(sync_resp) => binary_response(
            StatusCode::OK,
            &ServerMessage::SyncBatch {
                correlation_id,
                room_id,
                head_seq: sync_resp.head_seq,
                ops: sync_resp.ops,
                has_more: sync_resp.has_more,
            },
        ),
        Err(err) => fail(room_id, err),
    }
}

/// `POST /rooms/:room_id/ack`: Explicit client persistence acknowledgment triggering proactive log pruning.
pub async fn ack(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage<ClientMessage>,
) -> Response {
    let ClientMessage::Ack {
        correlation_id,
        room_id,
        client_id,
        ack_seq,
    } = msg
    else {
        return unexpected_message(auth.room_id, "Ack");
    };
    let fail = |room_id: RoomId, err| binary_error(Some(correlation_id), Some(room_id), err);

    if let Err(err) = ensure_payload_identity(
        &auth.room_id,
        Some(&auth.client_id),
        &room_id,
        Some(&client_id),
    ) {
        return fail(room_id, err);
    }

    match ask_room(&state, &room_id, |reply| RoomCommand::Ack {
        client_id,
        ack_seq,
        reply,
    })
    .await
    {
        Ok(head_seq) => binary_response(
            StatusCode::OK,
            &ServerMessage::AckConfirmed {
                correlation_id,
                room_id,
                ack_seq,
                head_seq,
            },
        ),
        Err(err) => fail(room_id, err),
    }
}

/// `POST /rooms/:room_id/heartbeat`: Lightweight lease keep-alive ping.
pub async fn heartbeat(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage<ClientMessage>,
) -> Response {
    let ClientMessage::Heartbeat {
        correlation_id,
        room_id,
        client_id,
    } = msg
    else {
        return unexpected_message(auth.room_id, "Heartbeat");
    };
    let fail = |room_id: RoomId, err| binary_error(Some(correlation_id), Some(room_id), err);

    if let Err(err) = ensure_payload_identity(
        &auth.room_id,
        Some(&auth.client_id),
        &room_id,
        Some(&client_id),
    ) {
        return fail(room_id, err);
    }

    match ask_room(&state, &room_id, |reply| RoomCommand::Heartbeat {
        client_id,
        reply,
    })
    .await
    {
        Ok(current_head_seq) => binary_response(
            StatusCode::OK,
            &ServerMessage::HeartbeatAck {
                correlation_id,
                room_id,
                current_head_seq,
            },
        ),
        Err(err) => fail(room_id, err),
    }
}

/// `POST /rooms/:room_id/schema`: On-demand schema retrieval during active DDL evolution.
pub async fn get_schema(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage<ClientMessage>,
) -> Response {
    let ClientMessage::GetSchema {
        correlation_id,
        room_id,
    } = msg
    else {
        return unexpected_message(auth.room_id, "GetSchema");
    };
    let fail = |room_id: RoomId, err| binary_error(Some(correlation_id), Some(room_id), err);

    if let Err(err) = ensure_payload_identity(&auth.room_id, None, &room_id, None) {
        return fail(room_id, err);
    }

    match ask_room(&state, &room_id, |reply| RoomCommand::GetSchema { reply }).await {
        Ok((schema_id, schema)) => binary_response(
            StatusCode::OK,
            &ServerMessage::Schema {
                correlation_id,
                room_id,
                schema_id,
                schema: (*schema).clone(),
            },
        ),
        Err(err) => fail(room_id, err),
    }
}

/// `POST /rooms/:room_id/deregister`: Explicit client departure advancing retention window immediately.
pub async fn deregister(
    State(state): State<AppState>,
    auth: AuthenticatedRoom,
    BinaryMessage(msg): BinaryMessage<ClientMessage>,
) -> Response {
    let ClientMessage::DeregisterClient {
        correlation_id,
        room_id,
        client_id,
    } = msg
    else {
        return unexpected_message(auth.room_id, "DeregisterClient");
    };
    let fail = |room_id: RoomId, err| binary_error(Some(correlation_id), Some(room_id), err);

    if let Err(err) = ensure_payload_identity(
        &auth.room_id,
        Some(&auth.client_id),
        &room_id,
        Some(&client_id),
    ) {
        return fail(room_id, err);
    }

    match ask_room(&state, &room_id, |reply| RoomCommand::DeregisterClient {
        client_id: client_id.clone(),
        reply,
    })
    .await
    {
        Ok(()) => binary_response(
            StatusCode::OK,
            &ServerMessage::DeregisterAck {
                correlation_id,
                room_id,
                client_id,
            },
        ),
        Err(err) => fail(room_id, err),
    }
}
