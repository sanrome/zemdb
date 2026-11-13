use std::fmt;
use zemdb_core::RoomId;

use super::{TableHandle, ZemdbClient};
use crate::{ClientError, RoomStatus};

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

    /// The diagnosis of the room, as [`Client::status`](crate::Client::status) gives it.
    pub async fn status(&self) -> Result<RoomStatus, ClientError> {
        self.client.core().status(self.room_id.clone()).await
    }

    /// Catches the replica up with the server, as [`Client::sync`](crate::Client::sync) does:
    /// the changes it applies reach the [`watch`](TableHandle::watch) streams of the room.
    pub async fn sync(&self) -> Result<(), ClientError> {
        self.client.core().sync(self.room_id.clone()).await
    }

    /// Closes the room, as [`Client::close_room`](crate::Client::close_room) does.
    pub async fn close(&self) -> Result<(), ClientError> {
        self.client.core().close_room(self.room_id.clone()).await
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
