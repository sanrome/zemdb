//! HTTP endpoints of the snapshot relay. The relay itself (storage, acceptance rules) lives in
//! [`crate::relay`]; these handlers decode requests, ask the room actor for the retained log
//! range, and encode responses. Every error is a binary `ServerMessage::Error` frame.

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use zemdb_core::id::{RoomId, SequenceNumber};
use zemdb_core::protocol::messages::{ClientMessage, ServerMessage};

use crate::api::data_plane::{binary_error, binary_response};
use crate::api::extract::{ensure_payload_identity, BinaryBody, BinaryMessage, RelayAuth};
use crate::api::router::AppState;
use crate::error::ServerError;
use crate::relay::{LogBounds, SnapshotChunkUpload};

/// `POST /rooms/:room_id/snapshot/upload`: Staging endpoint where an active donor client
/// or automated snapshot worker uploads a room snapshot.
pub async fn upload_snapshot(
    State(state): State<AppState>,
    RelayAuth { room_id, .. }: RelayAuth,
    headers: HeaderMap,
    BinaryBody(body): BinaryBody,
) -> Response {
    // Errors use binary frames, like every other data plane endpoint; only the success
    // response of this endpoint is JSON.
    let head_seq_val = match parse_snapshot_head_seq(&headers) {
        Ok(seq) => seq,
        Err(err) => return binary_error(None, Some(room_id), err),
    };

    let head_seq = SequenceNumber::new(head_seq_val);
    let task_state = state.clone();
    let task_room = room_id.clone();
    let staged = run_to_completion(async move {
        task_state
            .snapshot_relay
            .stage_snapshot(
                &task_room,
                head_seq,
                body,
                log_bounds(&task_state, &task_room),
            )
            .await
    })
    .await;
    let hash = match staged {
        Ok(hash) => hash,
        Err(err) => return binary_error(None, Some(room_id), err),
    };

    let body_json = serde_json::json!({
        "room_id": room_id.as_str(),
        "head_seq": head_seq_val,
        "snapshot_hash": blake3::Hash::from(hash).to_hex().as_str(),
        "status": "staged"
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body_json.to_string(),
    )
        .into_response()
}

/// Reads the snapshot sequence from the `x-snapshot-head-seq` header: a positive integer.
fn parse_snapshot_head_seq(headers: &HeaderMap) -> Result<u64, ServerError> {
    let raw = headers
        .get("x-snapshot-head-seq")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::BadRequest("Missing x-snapshot-head-seq header".to_string()))?;
    let seq = raw.parse::<u64>().map_err(|_| {
        ServerError::BadRequest("Invalid x-snapshot-head-seq header format".to_string())
    })?;
    if seq == 0 {
        return Err(ServerError::BadRequest(
            "x-snapshot-head-seq must be greater than 0".to_string(),
        ));
    }
    Ok(seq)
}

/// `POST /rooms/:room_id/snapshot/chunk`: Handles `ClientMessage::RequestSnapshotChunk`
/// and streams the requested binary `ServerMessage::SnapshotChunk`.
pub async fn request_chunk(
    State(state): State<AppState>,
    RelayAuth { room_id, .. }: RelayAuth,
    BinaryMessage(msg): BinaryMessage<ClientMessage>,
) -> Response {
    let ClientMessage::RequestSnapshotChunk {
        correlation_id,
        room_id: msg_room_id,
        chunk_index,
        chunk_size,
        snapshot_hash,
    } = msg
    else {
        return binary_error(
            None,
            Some(room_id),
            ServerError::BadRequest("Expected RequestSnapshotChunk message".to_string()),
        );
    };

    if let Err(err) = ensure_payload_identity(&room_id, None, &msg_room_id, None) {
        return binary_error(Some(correlation_id), Some(room_id), err);
    }
    let result = state
        .snapshot_relay
        .get_chunk(
            correlation_id,
            &room_id,
            chunk_index,
            chunk_size,
            snapshot_hash,
            log_bounds(&state, &room_id),
        )
        .await;
    match result {
        Ok(chunk_msg) => binary_response(StatusCode::OK, &chunk_msg),
        Err(err) => binary_error(Some(correlation_id), Some(room_id), err),
    }
}

/// `POST /rooms/:room_id/snapshot/upload-chunk`: Handles multipart snapshot upload chunk streaming.
pub async fn upload_chunk(
    State(state): State<AppState>,
    RelayAuth { room_id, uploader }: RelayAuth,
    BinaryMessage(msg): BinaryMessage<ClientMessage>,
) -> Response {
    let ClientMessage::UploadSnapshotChunk {
        correlation_id,
        room_id: msg_room_id,
        snapshot_head_seq,
        chunk_index,
        total_chunks,
        total_bytes,
        snapshot_hash,
        data,
    } = msg
    else {
        return binary_error(
            None,
            Some(room_id),
            ServerError::BadRequest("Expected UploadSnapshotChunk message".to_string()),
        );
    };

    if let Err(err) = ensure_payload_identity(&room_id, None, &msg_room_id, None) {
        return binary_error(Some(correlation_id), Some(room_id), err);
    }

    let chunk_upload = SnapshotChunkUpload {
        room_id: room_id.clone(),
        uploader,
        head_seq: snapshot_head_seq,
        chunk_index,
        total_chunks,
        total_bytes,
        snapshot_hash,
        data,
    };
    let task_state = state.clone();
    let staged = run_to_completion(async move {
        let room_id = chunk_upload.room_id.clone();
        task_state
            .snapshot_relay
            .stage_chunk(chunk_upload, log_bounds(&task_state, &room_id))
            .await
    })
    .await;
    match staged {
        Ok(staged) => binary_response(
            StatusCode::OK,
            &ServerMessage::SnapshotUploadChunkAck {
                correlation_id,
                room_id,
                chunk_index,
                total_chunks,
                staged,
            },
        ),
        Err(err) => binary_error(Some(correlation_id), Some(room_id), err),
    }
}

/// Runs a relay write as its own task, so that a client that disconnects mid-request cannot
/// cancel it between writing a snapshot file and recording it.
async fn run_to_completion<T: Send + 'static>(
    work: impl std::future::Future<Output = Result<T, ServerError>> + Send + 'static,
) -> Result<T, ServerError> {
    tokio::spawn(work)
        .await
        .map_err(|e| ServerError::Internal(format!("Snapshot relay task failed: {e}")))?
}

/// Retained log range of the room, asked to its actor when the relay awaits it. A room that
/// does not exist is `RoomNotFound`.
async fn log_bounds(state: &AppState, room_id: &RoomId) -> Result<LogBounds, ServerError> {
    let (tail_seq, head_seq) = state.room_manager.log_bounds(room_id).await?;
    Ok(LogBounds { tail_seq, head_seq })
}
