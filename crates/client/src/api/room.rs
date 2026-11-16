use std::fmt;
use zemdb_core::RoomId;

use super::{TableHandle, ZemdbClient};
use crate::{ClientError, Freshness, RoomStatus};

/// A room of a [`ZemdbClient`], by its id: what it reaches is the room as the client has it at
/// each call, so a handle of a room closed and opened again reaches it again. A clone shares the
/// client.
#[derive(Clone)]
pub struct RoomHandle {
    client: ZemdbClient,
    room_id: RoomId,
}

impl RoomHandle {
    pub(super) fn new(client: ZemdbClient, room_id: RoomId) -> Self {
        Self { client, room_id }
    }

    /// The id of the room.
    pub fn room_id(&self) -> &RoomId {
        &self.room_id
    }

    /// The handle of the table `name` of the schema of the replica: `InvalidOperation` if the
    /// schema has none, and `RoomClosed` if the replica is not open.
    pub fn table(&self, name: &str) -> Result<TableHandle, ClientError> {
        let read = |schema: &zemdb_core::Schema| schema.has_table_by_name(name);
        match self.client.core().read_schema(&self.room_id, read)? {
            (_, true) => Ok(TableHandle::new(self.clone(), name.to_string())),
            (_, false) => Err(ClientError::InvalidOperation(format!(
                "The room has no table '{name}'"
            ))),
        }
    }

    /// The diagnosis of the room, as [`Client::status`](crate::Client::status) gives it, with
    /// its worker: the mode it runs in, effectively too, and whether its event stream is open.
    pub async fn status(&self) -> Result<RoomStatus, ClientError> {
        self.client.core().status(self.room_id.clone()).await
    }

    /// Catches the replica up with the server, as [`Client::sync`](crate::Client::sync) does:
    /// the changes it applies reach the [`watch`](TableHandle::watch) streams of the room. In
    /// [`Freshness::Manual`], it is how the replica learns what the others write; and its reply
    /// lets the writes of a room that was offline go again.
    pub async fn sync(&self) -> Result<(), ClientError> {
        self.client.core().sync(self.room_id.clone()).await
    }

    /// Sets the mode of the room, in which its worker catches the replica up with what the others
    /// write, for as long as the client lives: also for the worker of its next open.
    ///
    /// In [`Freshness::Periodic`], the worker runs in [`Freshness::Live`] while the room has a
    /// [`watch`](TableHandle::watch) stream alive, and in `Periodic` once it has none; in
    /// [`Freshness::Manual`] and in `Live`, it runs in that mode, with streams or without them: a
    /// room in `Manual` catches up with its writes and its [`sync`](Self::sync), and its streams
    /// show what those bring. A worker that runs takes the mode once its step in progress ends
    /// (at once, unless a heartbeat with its acknowledgement is in flight). Until it is set, the
    /// room is in the mode of the configuration
    /// ([`ClientConfig::freshness`](crate::ClientConfig::freshness)). A server with its event
    /// stream turned off leaves the worker in `Periodic`, which [`status`](Self::status) shows.
    pub fn set_freshness(&self, freshness: Freshness) {
        self.client.set_mode(&self.room_id, freshness);
    }

    /// Closes the room, as [`Client::close_room`](crate::Client::close_room) does: its worker
    /// stops, and its `watch` streams end.
    pub async fn close(&self) -> Result<(), ClientError> {
        self.client.core().close_room(self.room_id.clone()).await
    }

    /// Discards the local replica of the room, as
    /// [`Client::discard_room`](crate::Client::discard_room) does: the way out of a room that
    /// fails closed. Its worker stops, its `watch` streams end, and the handle reads `RoomClosed`
    /// until [`ZemdbClient::room`] opens the room again, as new, with the schema and the state of
    /// the server.
    pub async fn discard(&self) -> Result<(), ClientError> {
        self.client.core().discard_room(self.room_id.clone()).await
    }

    pub(super) fn client(&self) -> &ZemdbClient {
        &self.client
    }
}

impl fmt::Debug for RoomHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoomHandle")
            .field("room_id", &self.room_id)
            .finish_non_exhaustive()
    }
}
