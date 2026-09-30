use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use rimdb_core::id::SequenceNumber;
use rimdb_core::protocol::messages::ErrorCode;
use serde::Serialize;
use thiserror::Error;

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

    #[error("Client is deregistered")]
    ClientDeregistered,

    #[error("Room is currently locked: {0}")]
    RoomLocked(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Rate limited")]
    RateLimited,

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Internal server error: {0}")]
    Internal(String),

    #[error("Invalid sequence number: expected <= {expected}, got {actual}")]
    InvalidSequence {
        expected: SequenceNumber,
        actual: SequenceNumber,
    },

    #[error("Gateway timeout: {0}")]
    GatewayTimeout(String),
}

impl ServerError {
    /// Maps the ServerError to the protocol ErrorCode.
    pub fn to_error_code(&self) -> ErrorCode {
        match self {
            ServerError::SchemaViolation(_) => ErrorCode::SchemaViolation,
            ServerError::RoomNotFound(_) => ErrorCode::RoomNotFound,
            ServerError::RoomAlreadyExists(_) => ErrorCode::RoomAlreadyExists,
            ServerError::SchemaNotFound(_) => ErrorCode::SchemaNotFound,
            ServerError::BehindCompaction => ErrorCode::BehindCompaction,
            ServerError::ClientDeregistered => ErrorCode::ClientDeregistered,
            ServerError::RoomLocked(_) => ErrorCode::RoomLocked,
            ServerError::Unauthorized(_) => ErrorCode::Unauthorized,
            ServerError::RateLimited => ErrorCode::RateLimited,
            ServerError::InvalidSequence { .. } => ErrorCode::InvalidSequence,
            ServerError::GatewayTimeout(_)
            | ServerError::Io(_)
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
            ServerError::RoomNotFound(_) | ServerError::SchemaNotFound(_) => StatusCode::NOT_FOUND,
            ServerError::RoomAlreadyExists(_) => StatusCode::CONFLICT,
            ServerError::SchemaViolation(_) | ServerError::InvalidSequence { .. } => {
                StatusCode::BAD_REQUEST
            }
            ServerError::BehindCompaction => StatusCode::GONE,
            ServerError::ClientDeregistered => StatusCode::FORBIDDEN,
            ServerError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ServerError::RoomLocked(_) => StatusCode::LOCKED,
            ServerError::GatewayTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
            ServerError::Io(_)
            | ServerError::Wal(_)
            | ServerError::WalCorruption(_)
            | ServerError::Config(_)
            | ServerError::Serialization(_)
            | ServerError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
        (status, body).into_response()
    }
}
