use axum::http::header::RETRY_AFTER;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use thiserror::Error;
use zemdb_core::id::SequenceNumber;
use zemdb_core::protocol::codec::DecodeError;
use zemdb_core::protocol::messages::ErrorCode;

/// Seconds a client waits before retrying a request answered with 503 `Unavailable`
/// (sent in the `Retry-After` header).
pub const RETRY_AFTER_SECS: u64 = 1;

/// Core error type for the coordination server.
#[derive(Debug, Error)]
pub enum ServerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("WAL corruption detected: {0}")]
    WalCorruption(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Room not found: {0}")]
    RoomNotFound(String),

    #[error("Room already exists: {0}")]
    RoomAlreadyExists(String),

    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Schema violation: {0}")]
    SchemaViolation(String),

    #[error("Client is behind compaction, full snapshot required")]
    BehindCompaction,

    /// The client is not in the room's roster; it must register again.
    #[error("Client not registered: {0}")]
    ClientNotRegistered(String),

    #[error("Room is currently locked: {0}")]
    RoomLocked(String),

    /// Missing, malformed, wrongly signed or expired credentials.
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Valid credentials that do not grant the request: a token issued for another room, or a
    /// message naming another client than the token.
    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Rate limited")]
    RateLimited,

    #[error("Serialization error: {0}")]
    Serialization(String),

    /// An unexpected failure. Expected, retryable conditions use `Unavailable` or `Timeout`.
    #[error("Internal server error: {0}")]
    Internal(String),

    /// The room cannot answer right now (its actor stopped, is restarting after a failed
    /// write, or dropped the request); the request may be retried.
    #[error("Service unavailable: {0}")]
    Unavailable(String),

    /// The room actor did not answer in time; the request may be retried.
    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Invalid sequence number: expected <= {expected}, got {actual}")]
    InvalidSequence {
        expected: SequenceNumber,
        actual: SequenceNumber,
    },

    #[error("Protocol version mismatch: {0}")]
    ProtocolVersionMismatch(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Snapshot superseded: {0}")]
    SnapshotSuperseded(String),
}

impl From<zemdb_core::InvalidIdError> for ServerError {
    fn from(err: zemdb_core::InvalidIdError) -> Self {
        ServerError::BadRequest(err.to_string())
    }
}

/// A request frame that cannot be decoded is a client error: a frame of another protocol
/// version is `ProtocolVersionMismatch`, anything else `BadRequest`.
impl From<DecodeError> for ServerError {
    fn from(err: DecodeError) -> Self {
        match err {
            DecodeError::UnsupportedVersion { .. } => {
                ServerError::ProtocolVersionMismatch(err.to_string())
            }
            _ => ServerError::BadRequest(format!("Failed to decode request message: {err}")),
        }
    }
}

impl ServerError {
    /// Maps the ServerError to the protocol ErrorCode.
    pub fn to_error_code(&self) -> ErrorCode {
        match self {
            ServerError::ProtocolVersionMismatch(_) => ErrorCode::ProtocolVersionMismatch,
            ServerError::SchemaViolation(_) => ErrorCode::SchemaViolation,
            ServerError::RoomNotFound(_) => ErrorCode::RoomNotFound,
            ServerError::RoomAlreadyExists(_) => ErrorCode::RoomAlreadyExists,
            ServerError::SchemaNotFound(_) => ErrorCode::SchemaNotFound,
            ServerError::BehindCompaction => ErrorCode::BehindCompaction,
            ServerError::ClientNotRegistered(_) => ErrorCode::ClientNotRegistered,
            ServerError::RoomLocked(_) => ErrorCode::RoomLocked,
            ServerError::Unauthorized(_) => ErrorCode::Unauthorized,
            ServerError::Forbidden(_) => ErrorCode::Forbidden,
            ServerError::Unavailable(_) => ErrorCode::Unavailable,
            ServerError::Timeout(_) => ErrorCode::Timeout,
            ServerError::RateLimited => ErrorCode::RateLimited,
            ServerError::InvalidSequence { .. } => ErrorCode::InvalidSequence,
            ServerError::BadRequest(_) => ErrorCode::BadRequest,
            ServerError::SnapshotSuperseded(_) => ErrorCode::SnapshotSuperseded,
            ServerError::Io(_)
            | ServerError::Wal(_)
            | ServerError::WalCorruption(_)
            | ServerError::Config(_)
            | ServerError::Serialization(_)
            | ServerError::Internal(_) => ErrorCode::Internal,
        }
    }

    /// Maps the ServerError to an appropriate HTTP status code.
    pub fn to_status_code(&self) -> StatusCode {
        match self {
            ServerError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ServerError::Forbidden(_) => StatusCode::FORBIDDEN,
            ServerError::RoomNotFound(_) | ServerError::SchemaNotFound(_) => StatusCode::NOT_FOUND,
            ServerError::RoomAlreadyExists(_)
            | ServerError::SnapshotSuperseded(_)
            | ServerError::ClientNotRegistered(_) => StatusCode::CONFLICT,
            ServerError::ProtocolVersionMismatch(_)
            | ServerError::SchemaViolation(_)
            | ServerError::InvalidSequence { .. }
            | ServerError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ServerError::BehindCompaction => StatusCode::GONE,
            ServerError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ServerError::RoomLocked(_) => StatusCode::LOCKED,
            ServerError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ServerError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            ServerError::Io(_)
            | ServerError::Wal(_)
            | ServerError::WalCorruption(_)
            | ServerError::Config(_)
            | ServerError::Serialization(_)
            | ServerError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Adds the headers that go with this error's status: `Retry-After` on 503, so that
    /// clients know the request may be retried and when. Used for JSON and binary responses.
    pub(crate) fn add_headers(&self, response: &mut Response) {
        if self.to_status_code() == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from(RETRY_AFTER_SECS));
        }
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    code: ErrorCode,
    message: String,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status = self.to_status_code();
        let body = Json(ErrorResponse {
            code: self.to_error_code(),
            message: self.to_string(),
        });
        let mut response = (status, body).into_response();
        self.add_headers(&mut response);
        response
    }
}

#[cfg(test)]
#[path = "tests/error.rs"]
mod tests;
