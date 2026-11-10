use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::fmt::{self, Debug};
use std::pin::Pin;
use zemdb_core::{encode_message, ClientMessage, ErrorCode, RoomId, ServerMessage};

/// How the client reaches the server.
#[async_trait]
pub trait TransportClient: Debug + Send + Sync {
    /// Sends `message` to the Data Plane endpoint of its kind ([`endpoint`]), with `token` as
    /// its bearer token, and returns the bytes of the response, whatever its HTTP status: every
    /// Data Plane response is a frame, which the client decodes. A registration also carries
    /// its token inside the message.
    async fn send(&self, message: &ClientMessage, token: &str) -> Result<Vec<u8>, TransportError>;

    /// Opens the event stream of `room_id` (`GET /rooms/{room_id}/events`), with `token` as its
    /// bearer token, over a connection of its own, so that a stream nobody reads does not hold
    /// back the requests of [`send`](Self::send). An open stream has no timeout: whoever reads
    /// it decides how long it may stay silent. By default the stream is refused with the frame
    /// of `EventsDisabled`, as by a server with its event stream turned off.
    async fn events(&self, room_id: &RoomId, token: &str) -> Result<EventStream, TransportError> {
        let _ = token;
        let refusal = ServerMessage::Error {
            correlation_id: None,
            room_id: Some(room_id.clone()),
            code: ErrorCode::EventsDisabled,
            message: "The transport has no event stream".to_string(),
        };
        let refusal = encode_message(&refusal).map_err(|err| TransportError(err.to_string()))?;
        Ok(EventStream::Refused(refusal))
    }
}

/// The body of an open event stream, in the blocks of bytes the transport receives: an error
/// ends it.
pub type EventBlocks = Pin<Box<dyn Stream<Item = Result<Bytes, TransportError>> + Send>>;

/// What the server answered to opening an event stream ([`TransportClient::events`]).
pub enum EventStream {
    /// The stream is open (HTTP 200): its body, which the client parses.
    Open(EventBlocks),
    /// The stream was refused: the bytes of the response, at most a frame, which the client
    /// decodes as the error (`EventsDisabled`, `Unauthorized`, `RateLimited`...).
    Refused(Vec<u8>),
}

impl Debug for EventStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open(_) => f.write_str("Open"),
            Self::Refused(frame) => f.debug_tuple("Refused").field(&frame.len()).finish(),
        }
    }
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
