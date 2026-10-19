//! HTTP endpoints of the snapshot relay. The relay itself (storage, acceptance rules) lives in
//! [`crate::relay`]; these handlers decode requests, ask the room actor for the retained log
//! range, and encode responses. Every error is a binary `ServerMessage::Error` frame.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use zemdb_core::id::{RoomId, SequenceNumber};
use zemdb_core::protocol::messages::{ClientMessage, ServerMessage};

use crate::api::data_plane::{
    binary_error, binary_response, budgeted_response, no_budget, reserve_frame, unexpected_message,
};
use crate::api::extract::{ensure_payload_identity, BinaryBody, BinaryMessage, RelayAuth};
use crate::api::router::AppState;
use crate::error::ServerError;
use crate::relay::{LogBounds, SnapshotChunkUpload};

/// `POST /rooms/:room_id/snapshot/upload`: uploads a room snapshot in one request. The body is
/// the raw snapshot (not a protocol frame), with its sequence in the `x-snapshot-head-seq`
/// header; success is answered with `ServerMessage::SnapshotStaged`.
pub async fn upload_snapshot(
    State(state): State<AppState>,
    RelayAuth { room_id, uploader }: RelayAuth,
    headers: HeaderMap,
    BinaryBody(body): BinaryBody,
) -> Response {
    let head_seq = match parse_snapshot_head_seq(&headers) {
        Ok(seq) => seq,
        Err(err) => return binary_error(None, Some(room_id), err),
    };
    let task_room = room_id.clone();
    let staged = run_to_completion(async move {
        state
            .snapshot_relay
            .stage_snapshot(
                &task_room,
                &uploader,
                head_seq,
                body,
                log_bounds(&state, &task_room),
            )
            .await
    })
    .await;
    match staged {
        Ok(snapshot_hash) => binary_response(
            StatusCode::OK,
            &ServerMessage::SnapshotStaged {
                room_id,
                snapshot_head_seq: head_seq,
                snapshot_hash,
            },
        ),
        Err(err) => binary_error(None, Some(room_id), err),
    }
}

/// Reads the snapshot sequence from the `x-snapshot-head-seq` header: a positive integer.
fn parse_snapshot_head_seq(headers: &HeaderMap) -> Result<SequenceNumber, ServerError> {
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
    Ok(SequenceNumber::new(seq))
}

/// `POST /rooms/:room_id/snapshot/chunk`: Handles `ClientMessage::RequestSnapshotChunk`
/// and streams the requested binary `ServerMessage::SnapshotChunk`.
pub async fn request_chunk(
    State(state): State<AppState>,
    RelayAuth { room_id, uploader }: RelayAuth,
    BinaryMessage(msg): BinaryMessage,
) -> Response {
    let ClientMessage::RequestSnapshotChunk {
        correlation_id,
        room_id: msg_room_id,
        chunk_index,
        chunk_size,
        snapshot_hash,
    } = msg
    else {
        return unexpected_message(Some(room_id), "RequestSnapshotChunk");
    };

    if let Err(err) = ensure_payload_identity(&room_id, None, &msg_room_id, None) {
        return binary_error(Some(correlation_id), Some(room_id), err);
    }
    // The first chunk asks for the room's log range after picking the snapshot, and before its
    // frame is reserved; the chunk is read from its file once the frame is reserved.
    let relay = &state.snapshot_relay;
    let bounds = log_bounds(&state, &room_id);
    let picked = match relay
        .pick_chunk(&room_id, chunk_index, chunk_size, snapshot_hash, bounds)
        .await
    {
        Ok(picked) => picked,
        Err(err) => return binary_error(Some(correlation_id), Some(room_id), err),
    };
    let Some(reservation) = reserve_frame(&state, (room_id.clone(), uploader)) else {
        return no_budget(correlation_id, room_id.clone());
    };
    match relay.read_picked(correlation_id, &room_id, picked).await {
        Ok(chunk_msg) => budgeted_response(reservation, &chunk_msg),
        Err(err) => binary_error(Some(correlation_id), Some(room_id), err),
    }
}

/// `POST /rooms/:room_id/snapshot/upload-chunk`: one chunk of a snapshot uploaded in several
/// requests (`ClientMessage::UploadSnapshotChunk`).
pub async fn upload_chunk(
    State(state): State<AppState>,
    RelayAuth { room_id, uploader }: RelayAuth,
    BinaryMessage(msg): BinaryMessage,
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
        return unexpected_message(Some(room_id), "UploadSnapshotChunk");
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
    let staged = run_to_completion(async move {
        let room_id = chunk_upload.room_id.clone();
        state
            .snapshot_relay
            .stage_chunk(chunk_upload, log_bounds(&state, &room_id))
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

/// Retained log range of the room and the highest cursor from which it found a gap, asked to
/// its actor when the future is awaited. A room that does not exist is `RoomNotFound`.
async fn log_bounds(state: &AppState, room_id: &RoomId) -> Result<LogBounds, ServerError> {
    state.room_manager.log_bounds(room_id).await
}
