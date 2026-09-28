use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use rimdb_core::id::{CorrelationId, RoomId};
use rimdb_core::protocol::codec::{decode_message, encode_message};
use rimdb_core::protocol::messages::{ClientMessage, ServerMessage};

use crate::actor::command::RoomCommand;
use crate::api::auth::verify_client_token;
use crate::api::router::AppState;
use crate::error::ServerError;

fn binary_response(status: StatusCode, msg: &ServerMessage) -> Response {
    match encode_message(msg) {
        Ok(bytes) => (
            status,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Serialization error: {}", e),
        )
            .into_response(),
    }
}

fn binary_error(
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

/// `POST /rooms/:room_id/register`: Handshake endpoint delivering current head_seq and schema in 1 RTT.
pub async fn register(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    body: Bytes,
) -> Response {
    let msg: ClientMessage = match decode_message(&body) {
        Ok(m) => m,
        Err(e) => {
            return binary_error(
                None,
                Some(RoomId::new(&room_id_str)),
                ServerError::Serialization(format!("Decode failure: {}", e)),
            );
        }
    };

    match msg {
        ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            client_id,
            auth_token,
        } => {
            if room_id.as_str() != room_id_str {
                return binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Config("RoomId path and payload mismatch".to_string()),
                );
            }

            // Verify stateless client auth_token
            if let Err(err) =
                verify_client_token(&auth_token, &client_id, &room_id, &state.config.auth_secret)
            {
                return binary_error(Some(correlation_id), Some(room_id), err);
            }

            // Obtain room actor sender
            let sender = match state.room_manager.get_or_spawn(&room_id, None) {
                Ok(s) => s,
                Err(err) => return binary_error(Some(correlation_id), Some(room_id), err),
            };

            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender
                .send(RoomCommand::RegisterClient { client_id, reply: tx })
                .await
                .is_err()
            {
                return binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Room actor closed".to_string()),
                );
            }

            match rx.await {
                Ok(Ok(reg_resp)) => binary_response(
                    StatusCode::OK,
                    &ServerMessage::Registered {
                        correlation_id,
                        room_id,
                        head_seq: reg_resp.head_seq,
                        schema_id: reg_resp.schema_id,
                        schema: (*reg_resp.schema).clone(),
                    },
                ),
                Ok(Err(err)) => binary_error(Some(correlation_id), Some(room_id), err),
                Err(_) => binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Actor reply dropped".to_string()),
                ),
            }
        }
        _ => binary_error(
            None,
            Some(RoomId::new(room_id_str)),
            ServerError::Config("Expected RegisterClient message".to_string()),
        ),
    }
}

/// `POST /rooms/:room_id/commit`: Atomic commit with 1-RTT catchup deltas and monotonic sequencing.
pub async fn commit(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    body: Bytes,
) -> Response {
    let msg: ClientMessage = match decode_message(&body) {
        Ok(m) => m,
        Err(e) => {
            return binary_error(
                None,
                Some(RoomId::new(&room_id_str)),
                ServerError::Serialization(format!("Decode failure: {}", e)),
            );
        }
    };

    match msg {
        ClientMessage::Commit {
            correlation_id,
            room_id,
            client_id,
            mutation_id,
            last_ack_seq,
            op,
        } => {
            let sender = match state.room_manager.get_room(&room_id) {
                Some(s) => s,
                None => {
                    return binary_error(
                        Some(correlation_id),
                        Some(room_id.clone()),
                        ServerError::RoomNotFound(room_id.to_string()),
                    )
                }
            };

            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender
                .send(RoomCommand::Commit {
                    client_id,
                    mutation_id,
                    last_ack_seq,
                    op,
                    reply: tx,
                })
                .await
                .is_err()
            {
                return binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Room actor closed".to_string()),
                );
            }

            match rx.await {
                Ok(Ok(commit_resp)) => binary_response(
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
                Ok(Err(err)) => binary_error(Some(correlation_id), Some(room_id), err),
                Err(_) => binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Actor reply dropped".to_string()),
                ),
            }
        }
        _ => binary_error(
            None,
            Some(RoomId::new(room_id_str)),
            ServerError::Config("Expected Commit message".to_string()),
        ),
    }
}

/// `POST /rooms/:room_id/sync`: Paginated batch synchronization with flow control.
pub async fn sync(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    body: Bytes,
) -> Response {
    let msg: ClientMessage = match decode_message(&body) {
        Ok(m) => m,
        Err(e) => {
            return binary_error(
                None,
                Some(RoomId::new(&room_id_str)),
                ServerError::Serialization(format!("Decode failure: {}", e)),
            );
        }
    };

    match msg {
        ClientMessage::Sync {
            correlation_id,
            room_id,
            client_id,
            from_seq,
            max_batch_size,
        } => {
            let sender = match state.room_manager.get_room(&room_id) {
                Some(s) => s,
                None => {
                    return binary_error(
                        Some(correlation_id),
                        Some(room_id.clone()),
                        ServerError::RoomNotFound(room_id.to_string()),
                    )
                }
            };

            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender
                .send(RoomCommand::Sync {
                    client_id,
                    from_seq,
                    max_batch_size,
                    reply: tx,
                })
                .await
                .is_err()
            {
                return binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Room actor closed".to_string()),
                );
            }

            match rx.await {
                Ok(Ok(sync_resp)) => binary_response(
                    StatusCode::OK,
                    &ServerMessage::SyncBatch {
                        correlation_id,
                        room_id,
                        head_seq: sync_resp.head_seq,
                        ops: sync_resp.ops,
                        has_more: sync_resp.has_more,
                    },
                ),
                Ok(Err(err)) => binary_error(Some(correlation_id), Some(room_id), err),
                Err(_) => binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Actor reply dropped".to_string()),
                ),
            }
        }
        _ => binary_error(
            None,
            Some(RoomId::new(room_id_str)),
            ServerError::Config("Expected Sync message".to_string()),
        ),
    }
}

/// `POST /rooms/:room_id/ack`: Explicit client persistence acknowledgment triggering proactive log pruning.
pub async fn ack(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    body: Bytes,
) -> Response {
    let msg: ClientMessage = match decode_message(&body) {
        Ok(m) => m,
        Err(e) => {
            return binary_error(
                None,
                Some(RoomId::new(&room_id_str)),
                ServerError::Serialization(format!("Decode failure: {}", e)),
            );
        }
    };

    match msg {
        ClientMessage::Ack {
            correlation_id,
            room_id,
            client_id,
            ack_seq,
        } => {
            let sender = match state.room_manager.get_room(&room_id) {
                Some(s) => s,
                None => {
                    return binary_error(
                        Some(correlation_id),
                        Some(room_id.clone()),
                        ServerError::RoomNotFound(room_id.to_string()),
                    )
                }
            };

            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender
                .send(RoomCommand::Ack {
                    client_id,
                    ack_seq,
                    reply: tx,
                })
                .await
                .is_err()
            {
                return binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Room actor closed".to_string()),
                );
            }

            match rx.await {
                Ok(Ok(head_seq)) => binary_response(
                    StatusCode::OK,
                    &ServerMessage::AckConfirmed {
                        correlation_id,
                        room_id,
                        ack_seq,
                        head_seq,
                    },
                ),
                Ok(Err(err)) => binary_error(Some(correlation_id), Some(room_id), err),
                Err(_) => binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Actor reply dropped".to_string()),
                ),
            }
        }
        _ => binary_error(
            None,
            Some(RoomId::new(room_id_str)),
            ServerError::Config("Expected Ack message".to_string()),
        ),
    }
}

/// `POST /rooms/:room_id/heartbeat`: Lightweight lease keep-alive ping.
pub async fn heartbeat(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    body: Bytes,
) -> Response {
    let msg: ClientMessage = match decode_message(&body) {
        Ok(m) => m,
        Err(e) => {
            return binary_error(
                None,
                Some(RoomId::new(&room_id_str)),
                ServerError::Serialization(format!("Decode failure: {}", e)),
            );
        }
    };

    match msg {
        ClientMessage::Heartbeat {
            correlation_id,
            room_id,
            client_id,
        } => {
            let sender = match state.room_manager.get_room(&room_id) {
                Some(s) => s,
                None => {
                    return binary_error(
                        Some(correlation_id),
                        Some(room_id.clone()),
                        ServerError::RoomNotFound(room_id.to_string()),
                    )
                }
            };

            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender
                .send(RoomCommand::Heartbeat {
                    client_id,
                    reply: tx,
                })
                .await
                .is_err()
            {
                return binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Room actor closed".to_string()),
                );
            }

            match rx.await {
                Ok(Ok(current_head_seq)) => binary_response(
                    StatusCode::OK,
                    &ServerMessage::HeartbeatAck {
                        correlation_id,
                        room_id,
                        current_head_seq,
                    },
                ),
                Ok(Err(err)) => binary_error(Some(correlation_id), Some(room_id), err),
                Err(_) => binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Actor reply dropped".to_string()),
                ),
            }
        }
        _ => binary_error(
            None,
            Some(RoomId::new(room_id_str)),
            ServerError::Config("Expected Heartbeat message".to_string()),
        ),
    }
}

/// `POST /rooms/:room_id/schema`: On-demand schema retrieval during active DDL evolution.
pub async fn get_schema(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    body: Bytes,
) -> Response {
    let msg: ClientMessage = match decode_message(&body) {
        Ok(m) => m,
        Err(e) => {
            return binary_error(
                None,
                Some(RoomId::new(&room_id_str)),
                ServerError::Serialization(format!("Decode failure: {}", e)),
            );
        }
    };

    match msg {
        ClientMessage::GetSchema {
            correlation_id,
            room_id,
        } => {
            let sender = match state.room_manager.get_room(&room_id) {
                Some(s) => s,
                None => {
                    return binary_error(
                        Some(correlation_id),
                        Some(room_id.clone()),
                        ServerError::RoomNotFound(room_id.to_string()),
                    )
                }
            };

            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender
                .send(RoomCommand::GetSchema { reply: tx })
                .await
                .is_err()
            {
                return binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Room actor closed".to_string()),
                );
            }

            match rx.await {
                Ok(Ok((schema_id, schema))) => binary_response(
                    StatusCode::OK,
                    &ServerMessage::Schema {
                        correlation_id,
                        room_id,
                        schema_id,
                        schema: (*schema).clone(),
                    },
                ),
                Ok(Err(err)) => binary_error(Some(correlation_id), Some(room_id), err),
                Err(_) => binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Actor reply dropped".to_string()),
                ),
            }
        }
        _ => binary_error(
            None,
            Some(RoomId::new(room_id_str)),
            ServerError::Config("Expected GetSchema message".to_string()),
        ),
    }
}

/// `POST /rooms/:room_id/deregister`: Explicit client departure advancing retention window immediately.
pub async fn deregister(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    body: Bytes,
) -> Response {
    let msg: ClientMessage = match decode_message(&body) {
        Ok(m) => m,
        Err(e) => {
            return binary_error(
                None,
                Some(RoomId::new(&room_id_str)),
                ServerError::Serialization(format!("Decode failure: {}", e)),
            );
        }
    };

    match msg {
        ClientMessage::DeregisterClient {
            correlation_id,
            room_id,
            client_id,
        } => {
            let sender = match state.room_manager.get_room(&room_id) {
                Some(s) => s,
                None => {
                    return binary_error(
                        Some(correlation_id),
                        Some(room_id.clone()),
                        ServerError::RoomNotFound(room_id.to_string()),
                    )
                }
            };

            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender
                .send(RoomCommand::DeregisterClient { client_id, reply: tx })
                .await
                .is_err()
            {
                return binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Room actor closed".to_string()),
                );
            }

            match rx.await {
                Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
                Ok(Err(err)) => binary_error(Some(correlation_id), Some(room_id), err),
                Err(_) => binary_error(
                    Some(correlation_id),
                    Some(room_id),
                    ServerError::Internal("Actor reply dropped".to_string()),
                ),
            }
        }
        _ => binary_error(
            None,
            Some(RoomId::new(room_id_str)),
            ServerError::Config("Expected DeregisterClient message".to_string()),
        ),
    }
}
