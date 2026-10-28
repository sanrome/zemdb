use async_trait::async_trait;
use std::fmt::Debug;
use zemdb_core::ClientMessage;

/// How the client reaches the server.
#[async_trait]
pub trait TransportClient: Debug + Send + Sync {
    /// Sends `message` to the Data Plane endpoint of its kind ([`endpoint`]), with `token` as
    /// its bearer token, and returns the bytes of the response, whatever its HTTP status: every
    /// Data Plane response is a frame, which the client decodes. A registration also carries
    /// its token inside the message.
    async fn send(&self, message: &ClientMessage, token: &str) -> Result<Vec<u8>, TransportError>;
}

/// A request that got no response. It may have reached the server all the same.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("No response from the server: {0}")]
pub struct TransportError(pub String);

/// The path of the Data Plane endpoint of `message`: `/rooms/{room_id}/{endpoint}`.
pub fn endpoint(message: &ClientMessage) -> String {
    let (room_id, endpoint) = match message {
        ClientMessage::Commit { room_id, .. } => (room_id, "commit"),
        ClientMessage::Ack { room_id, .. } => (room_id, "ack"),
        ClientMessage::Sync { room_id, .. } => (room_id, "sync"),
        ClientMessage::Heartbeat { room_id, .. } => (room_id, "heartbeat"),
        ClientMessage::RegisterClient { room_id, .. } => (room_id, "register"),
        ClientMessage::GetSchema { room_id, .. } => (room_id, "schema"),
        ClientMessage::DeregisterClient { room_id, .. } => (room_id, "deregister"),
        ClientMessage::RequestSnapshotChunk { room_id, .. } => (room_id, "snapshot/chunk"),
        ClientMessage::UploadSnapshotChunk { room_id, .. } => (room_id, "snapshot/upload-chunk"),
    };
    format!("/rooms/{room_id}/{endpoint}")
}
