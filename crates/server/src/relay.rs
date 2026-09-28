use std::time::{Duration, Instant};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use dashmap::DashMap;
use rimdb_core::id::{CorrelationId, RoomId, SequenceNumber};
use rimdb_core::protocol::codec::{decode_message, encode_message};
use rimdb_core::protocol::messages::{ClientMessage, ServerMessage};

use crate::api::router::AppState;
use crate::error::ServerError;

/// In-memory staged snapshot ready for multipart chunk streaming.
#[derive(Debug, Clone)]
pub struct StagedSnapshot {
    pub head_seq: SequenceNumber,
    pub data: Bytes,
    pub total_bytes: u64,
    pub snapshot_hash: [u8; 32],
    pub created_at: Instant,
    pub file_path: Option<std::path::PathBuf>,
}

/// Ephemeral relay facilitating state-transfer chunks between peers or cold snapshots and bootstrapping clients.
#[derive(Debug, Default)]
pub struct SnapshotRelay {
    snapshots: DashMap<RoomId, StagedSnapshot>,
    ttl: Duration,
    snapshots_dir: Option<std::path::PathBuf>,
}

impl SnapshotRelay {
    /// Creates a new snapshot relay backed by a disk directory for snapshot persistence.
    /// Creates the directory if it does not exist, and recovers any existing snapshots within TTL.
    pub fn new(snapshots_dir: impl AsRef<std::path::Path>, ttl: Duration) -> std::io::Result<Self> {
        let dir_buf = snapshots_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir_buf)?;
        let relay = Self {
            snapshots: DashMap::new(),
            ttl,
            snapshots_dir: Some(dir_buf),
        };
        relay.recover_disk_snapshots();
        Ok(relay)
    }

    /// Creates an in-memory snapshot relay without filesystem persistence (primarily for unit tests).
    pub fn new_in_memory(ttl: Duration) -> Self {
        Self {
            snapshots: DashMap::new(),
            ttl,
            snapshots_dir: None,
        }
    }

    /// Scans the snapshot directory and recovers valid staged snapshots that haven't exceeded TTL.
    fn recover_disk_snapshots(&self) {
        let dir = match self.snapshots_dir.as_ref() {
            Some(d) => d,
            None => return,
        };

        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        let now_system = std::time::SystemTime::now();

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let file_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            // Clean up orphan temporary write files
            if file_name.contains(".tmp") {
                let _ = std::fs::remove_file(&path);
                continue;
            }

            if !file_name.ends_with(".snap.zst") {
                continue;
            }

            let stem = &file_name[..file_name.len() - ".snap.zst".len()];
            let parts: Vec<&str> = stem.rsplitn(2, '_').collect();
            if parts.len() != 2 {
                continue;
            }

            let head_seq_val = match parts[0].parse::<u64>() {
                Ok(s) => s,
                Err(_) => continue,
            };
            let room_id = RoomId::new(parts[1]);
            let head_seq = SequenceNumber::new(head_seq_val);

            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };

            let modified = metadata.modified().unwrap_or(now_system);
            let age = now_system.duration_since(modified).unwrap_or(Duration::ZERO);

            if age >= self.ttl {
                let _ = std::fs::remove_file(&path);
                continue;
            }

            if let Ok(bytes) = std::fs::read(&path) {
                let data = Bytes::from(bytes);
                let snapshot_hash = ServerMessage::compute_snapshot_hash(&data);
                let total_bytes = data.len() as u64;
                let created_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);

                self.snapshots.insert(
                    room_id,
                    StagedSnapshot {
                        head_seq,
                        data,
                        total_bytes,
                        snapshot_hash,
                        created_at,
                        file_path: Some(path),
                    },
                );
            }
        }
    }

    /// Returns the active snapshot sequence number for a room if one is currently staged and valid.
    pub fn active_snapshot_seq(&self, room_id: &RoomId) -> Option<SequenceNumber> {
        self.cleanup_expired();
        self.snapshots.get(room_id).map(|s| s.head_seq)
    }

    /// Stages a complete snapshot for a room, calculating its cryptographic BLAKE3 digest.
    pub fn stage_snapshot(
        &self,
        room_id: RoomId,
        head_seq: SequenceNumber,
        data: Bytes,
    ) -> [u8; 32] {
        let snapshot_hash = ServerMessage::compute_snapshot_hash(&data);
        let total_bytes = data.len() as u64;

        let file_path = if let Some(ref dir) = self.snapshots_dir {
            let snap_path = dir.join(format!("{}_{}.snap.zst", room_id.as_str(), head_seq.get()));
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let tmp_path = snap_path.with_extension(format!("tmp.{}", nanos));
            if std::fs::write(&tmp_path, &data).is_ok() {
                let _ = std::fs::rename(&tmp_path, &snap_path);
                Some(snap_path)
            } else {
                None
            }
        } else {
            None
        };

        // If an existing snapshot for this room had a different file_path on disk, delete it
        if let Some(old_snap) = self.snapshots.get(&room_id) {
            if let Some(ref old_path) = old_snap.file_path {
                if file_path.as_ref() != Some(old_path) {
                    let _ = std::fs::remove_file(old_path);
                }
            }
        }

        self.snapshots.insert(
            room_id,
            StagedSnapshot {
                head_seq,
                data,
                total_bytes,
                snapshot_hash,
                created_at: Instant::now(),
                file_path,
            },
        );

        snapshot_hash
    }

    /// Retrieves a specific slice / chunk from the staged snapshot.
    pub fn get_chunk(
        &self,
        correlation_id: CorrelationId,
        room_id: &RoomId,
        chunk_index: u32,
        chunk_size: u32,
    ) -> Result<ServerMessage, ServerError> {
        self.cleanup_expired();

        let staged = self
            .snapshots
            .get(room_id)
            .ok_or_else(|| ServerError::RoomNotFound(format!("No staged snapshot for room {}", room_id)))?;

        let total_bytes = staged.total_bytes;
        let chunk_size_u64 = chunk_size.max(1) as u64;
        let total_chunks = if total_bytes == 0 {
            1
        } else {
            total_bytes.div_ceil(chunk_size_u64) as u32
        };

        if chunk_index >= total_chunks {
            return Err(ServerError::Config(format!(
                "Requested chunk index {} exceeds total chunks {}",
                chunk_index, total_chunks
            )));
        }

        let start = (chunk_index as u64 * chunk_size_u64) as usize;
        let end = (start + chunk_size as usize).min(staged.data.len());
        let chunk_data = staged.data.slice(start..end);

        Ok(ServerMessage::SnapshotChunk {
            correlation_id,
            room_id: room_id.clone(),
            snapshot_head_seq: staged.head_seq,
            chunk_index,
            total_chunks,
            total_bytes,
            snapshot_hash: staged.snapshot_hash,
            data: chunk_data,
        })
    }

    /// Purges staged snapshots that exceeded their TTL both from RAM and from disk.
    pub fn cleanup_expired(&self) {
        let now = Instant::now();
        self.snapshots.retain(|_, snap| {
            let alive = now.duration_since(snap.created_at) < self.ttl;
            if !alive {
                if let Some(ref path) = snap.file_path {
                    let _ = std::fs::remove_file(path);
                }
            }
            alive
        });
    }
}

/// `POST /rooms/:room_id/snapshot/upload`: Staging endpoint where an active donor client
/// or automated snapshot worker uploads a room snapshot.
pub async fn upload_snapshot(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ServerError> {
    let room_id = RoomId::new(room_id_str);

    let head_seq_str = headers
        .get("x-snapshot-head-seq")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::Config("Missing x-snapshot-head-seq header".to_string()))?;

    let head_seq_val = head_seq_str
        .parse::<u64>()
        .map_err(|_| ServerError::Config("Invalid x-snapshot-head-seq header format".to_string()))?;

    if head_seq_val == 0 {
        return Err(ServerError::Config("x-snapshot-head-seq must be greater than 0".to_string()));
    }

    let head_seq = SequenceNumber::new(head_seq_val);
    let hash = state.snapshot_relay.stage_snapshot(room_id.clone(), head_seq, body);

    let body_json = serde_json::json!({
        "room_id": room_id.as_str(),
        "head_seq": head_seq_val,
        "snapshot_hash": blake3::Hash::from(hash).to_hex().as_str(),
        "status": "staged"
    });

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body_json.to_string(),
    )
        .into_response())
}

/// `POST /rooms/:room_id/snapshot/chunk`: Handles `ClientMessage::RequestSnapshotChunk`
/// and streams the requested binary `ServerMessage::SnapshotChunk`.
pub async fn request_chunk(
    State(state): State<AppState>,
    Path(room_id_str): Path<String>,
    body: Bytes,
) -> Response {
    let msg: ClientMessage = match decode_message(&body) {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to decode RequestSnapshotChunk: {}", e),
            )
                .into_response();
        }
    };

    match msg {
        ClientMessage::RequestSnapshotChunk {
            correlation_id,
            room_id,
            chunk_index,
            chunk_size,
        } => {
            if room_id.as_str() != room_id_str {
                return (
                    StatusCode::BAD_REQUEST,
                    "RoomId path and message mismatch",
                )
                    .into_response();
            }

            match state
                .snapshot_relay
                .get_chunk(correlation_id, &room_id, chunk_index, chunk_size)
            {
                Ok(chunk_msg) => match encode_message(&chunk_msg) {
                    Ok(bytes) => (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, "application/octet-stream")],
                        bytes,
                    )
                        .into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Serialization error: {}", e),
                    )
                        .into_response(),
                },
                Err(err) => {
                    let err_msg = ServerMessage::Error {
                        correlation_id: Some(correlation_id),
                        room_id: Some(room_id),
                        code: err.to_error_code(),
                        message: err.to_string(),
                    };
                    let bytes = encode_message(&err_msg).unwrap_or_default();
                    (
                        err.to_status_code(),
                        [(header::CONTENT_TYPE, "application/octet-stream")],
                        bytes,
                    )
                        .into_response()
                }
            }
        }
        _ => (
            StatusCode::BAD_REQUEST,
            "Expected RequestSnapshotChunk message",
        )
            .into_response(),
    }
}

