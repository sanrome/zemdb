//! The event stream of a room: its bytes split into events, each one read whole up to a limit,
//! and each event decoded before the replica checks it.
// Nothing reads the stream yet but the tests of this module.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "the sync worker is not here yet")
)]

use crate::{EventBlocks, TransportError};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::{Buf, Bytes};
use futures::stream::{Fuse, StreamExt};
use zemdb_core::{
    decode_message, DecodeError, RoomId, SchemaId, SequenceNumber, SequencedOperation,
    ServerMessage,
};

/// The longest event of a stream, counting the bytes of its lines without their ends, and so
/// its longest line. An `ops` event takes up to about 5.5 KiB.
pub(crate) const MAX_EVENT_BYTES: usize = 64 << 10;

/// An event of a stream as it came: its name (`message` without an `event` field) and its data,
/// its `data` lines joined with line feeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SseEvent {
    pub(crate) name: String,
    pub(crate) data: String,
}

/// Why a stream cannot be read further: it is closed, and another one opened.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StreamError {
    #[error("An event of the stream is longer than {MAX_EVENT_BYTES} bytes")]
    TooLong,
    #[error("The stream is not UTF-8")]
    NotUtf8,
    #[error(transparent)]
    Transport(TransportError),
}

/// Splits the bytes of a stream into its events, as the format of Server-Sent Events defines
/// them: lines that end with a line feed, a carriage return or both; `event` and `data` fields,
/// the others and the comments skipped; and a blank line that ends an event, which is given if
/// it has data. Holds one event at a time, besides the block being read.
#[derive(Debug, Default)]
pub(crate) struct EventParser {
    /// What is left of the last block.
    pending: Bytes,
    /// The line being read, without its end.
    line: Vec<u8>,
    /// Whether the last line ended with a carriage return: a line feed right after it is part
    /// of that end.
    after_cr: bool,
    /// The bytes of the lines of the event being read, without their ends.
    size: usize,
    /// The name and the data of the event being read.
    name: String,
    data: String,
    /// Why the stream cannot be read further, once it cannot.
    failed: Option<StreamError>,
}

impl EventParser {
    /// Takes the next block of the stream, once [`next_event`](Self::next_event) asked for more.
    pub(crate) fn push(&mut self, block: Bytes) {
        self.pending = block;
    }

    /// The next whole event in the blocks pushed so far, or `None` if it needs more of them.
    /// After an error the stream cannot be read further: every call returns it again, and the
    /// parser keeps nothing.
    pub(crate) fn next_event(&mut self) -> Result<Option<SseEvent>, StreamError> {
        let next = match &self.failed {
            Some(failed) => Err(failed.clone()),
            None => self.parse(),
        };
        if let Err(err) = &next {
            self.fail(err.clone());
        }
        next
    }

    /// Stops reading the stream with `err`.
    fn fail(&mut self, err: StreamError) {
        *self = Self {
            failed: Some(err),
            ..Self::default()
        };
    }

    fn parse(&mut self) -> Result<Option<SseEvent>, StreamError> {
        while !self.pending.is_empty() {
            if std::mem::take(&mut self.after_cr) && self.pending[0] == b'\n' {
                self.pending.advance(1);
                continue;
            }
            let end = (self.pending.iter()).position(|&byte| byte == b'\n' || byte == b'\r');
            let taken = end.unwrap_or(self.pending.len());
            self.size += taken;
            if self.size > MAX_EVENT_BYTES {
                return Err(StreamError::TooLong);
            }
            self.line.extend_from_slice(&self.pending[..taken]);
            let Some(end) = end else {
                self.pending.clear();
                break;
            };
            self.after_cr = self.pending[end] == b'\r';
            self.pending.advance(end + 1);
            let line = std::mem::take(&mut self.line);
            let line = std::str::from_utf8(&line).map_err(|_| StreamError::NotUtf8)?;
            if let Some(event) = self.line_ended(line) {
                return Ok(Some(event));
            }
        }
        Ok(None)
    }

    /// Takes a line that ended, and gives the event that a blank one ends, if it has data.
    fn line_ended(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            self.size = 0;
            let name = std::mem::take(&mut self.name);
            let mut data = std::mem::take(&mut self.data);
            // Each data line added a line feed after it.
            data.pop()?;
            let name = if name.is_empty() {
                "message".to_string()
            } else {
                name
            };
            return Some(SseEvent { name, data });
        }
        // A line without a colon is a field without a value; one that starts with it, a
        // comment, whose field is empty.
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => value.clone_into(&mut self.name),
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
            }
            _ => {}
        }
        None
    }
}

/// The events of an open stream, read one at a time.
pub(crate) struct EventReader {
    blocks: Fuse<EventBlocks>,
    parser: EventParser,
}

impl EventReader {
    pub(crate) fn new(blocks: EventBlocks) -> Self {
        Self {
            blocks: blocks.fuse(),
            parser: EventParser::default(),
        }
    }

    /// The next event, or `None` once the stream ends. After an error the stream cannot be
    /// read further: every call returns it again.
    pub(crate) async fn next(&mut self) -> Result<Option<SseEvent>, StreamError> {
        loop {
            if let Some(event) = self.parser.next_event()? {
                return Ok(Some(event));
            }
            match self.blocks.next().await {
                Some(Ok(block)) => self.parser.push(block),
                Some(Err(err)) => {
                    self.parser.fail(StreamError::Transport(err.clone()));
                    return Err(StreamError::Transport(err));
                }
                None => return Ok(None),
            }
        }
    }
}

/// An event of the stream of a room, decoded, before the replica checks it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RoomEvent {
    /// New operations of the room, from the frame of the event.
    Ops(OpsEvent),
    /// The head of the room moved to this sequence: operations too large for an event.
    HeadAdvanced(SequenceNumber),
    /// The schema of the room changed.
    SchemaReloaded,
    /// The server designated a client to upload a snapshot.
    SnapshotWanted,
    /// A new usable snapshot, of this sequence.
    SnapshotAvailable(SequenceNumber),
    /// The stream missed events.
    Lagged,
    /// An event of a name the client does not know.
    Unknown,
}

/// The operations of an `ops` event (`ServerMessage::OpsEvent`), not yet checked.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpsEvent {
    pub(crate) room_id: RoomId,
    pub(crate) room_incarnation: u128,
    pub(crate) head_seq: SequenceNumber,
    pub(crate) ops: Vec<SequencedOperation>,
    pub(crate) schema_version: u64,
    pub(crate) schema_id: SchemaId,
}

/// Why an event does not decode: the replica syncs instead.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EventError {
    #[error("The data of an ops event is not base64")]
    Base64,
    #[error("The data of an ops event is not a frame: {0}")]
    Frame(#[source] DecodeError),
    #[error("The frame of an ops event is of another message")]
    NotOps,
    #[error("The data of a {0} event is not a sequence")]
    Sequence(&'static str),
}

impl SseEvent {
    /// The event of the room this is: the data of `ops` is a frame of `OpsEvent` in standard
    /// base64 with padding, and those of `head_advanced` and `snapshot_available`, a sequence.
    pub(crate) fn decode(&self) -> Result<RoomEvent, EventError> {
        let sequence = |name| {
            let seq = self.data.parse().map_err(|_| EventError::Sequence(name));
            seq.map(SequenceNumber::new)
        };
        match self.name.as_str() {
            "ops" => decode_ops(&self.data).map(RoomEvent::Ops),
            "head_advanced" => sequence("head_advanced").map(RoomEvent::HeadAdvanced),
            "schema_reloaded" => Ok(RoomEvent::SchemaReloaded),
            "snapshot_wanted" => Ok(RoomEvent::SnapshotWanted),
            "snapshot_available" => {
                sequence("snapshot_available").map(RoomEvent::SnapshotAvailable)
            }
            "lagged" => Ok(RoomEvent::Lagged),
            _ => Ok(RoomEvent::Unknown),
        }
    }
}

fn decode_ops(data: &str) -> Result<OpsEvent, EventError> {
    let frame = STANDARD.decode(data).map_err(|_| EventError::Base64)?;
    match decode_message(&frame).map_err(EventError::Frame)? {
        ServerMessage::OpsEvent {
            room_id,
            room_incarnation,
            head_seq,
            ops,
            schema_version,
            schema_id,
        } => Ok(OpsEvent {
            room_id,
            room_incarnation,
            head_seq,
            ops,
            schema_version,
            schema_id,
        }),
        _ => Err(EventError::NotOps),
    }
}

#[cfg(test)]
#[path = "tests/events.rs"]
mod tests;
