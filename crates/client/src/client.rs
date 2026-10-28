use serde::{Deserialize, Serialize};
use zemdb_core::{ClientId, RoomId, SequenceNumber};

use crate::{ClientConfig, ClientError, MetadataStore};

/// The client of a device: one replica per room it opens.
#[derive(Debug)]
pub struct Client {
    client_id: ClientId,
    metadata: Box<dyn MetadataStore>,
}

/// The diagnosis of a room open in the client ([`Client::status`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomStatus {
    /// The head sequence of the replica.
    pub head_seq: SequenceNumber,
    /// How many times the client discarded the replica since it opened the room, counting the
    /// discard of a room opened without metadata.
    pub epoch: u64,
    /// Whether the client is registered in the room.
    pub registered: bool,
    /// Whether the room fails closed: its schema changed to one that does not evolve from the
    /// replica's.
    pub failed_closed: bool,
    /// The writes in flight.
    pub writes_in_flight: usize,
    /// The rejections and the ranges without a report that the room keeps for the writes in
    /// flight.
    pub retained: usize,
}

/// The client id of `person` on the device `device_id`: `person#device`, with the device id in
/// hexadecimal, so that its owner is `person`.
fn client_id(person: &str, device_id: u128) -> Result<ClientId, ClientError> {
    if person.is_empty() || person.contains('#') {
        return Err(ClientError::Config(format!(
            "The person {person:?} must not be empty or hold '#'"
        )));
    }
    ClientId::new(format!("{person}#{device_id:032x}"))
        .map_err(|err| ClientError::Config(err.to_string()))
}

impl Client {
    /// Opens the client of `config.person` over `metadata`, which it holds until
    /// [`shutdown`](Self::shutdown): another client over the same metadata fails. Its client id
    /// is `person#device`, with the device id that `metadata` keeps.
    ///
    /// An invalid person is `Config`, before `metadata` is touched; an error of `metadata`,
    /// `Storage`.
    pub async fn open(
        config: ClientConfig,
        metadata: impl MetadataStore + 'static,
    ) -> Result<Self, ClientError> {
        // Every device id makes a valid client id if one does.
        client_id(&config.person, 0)?;
        let device_id = metadata
            .open()
            .await
            .map_err(|err| ClientError::Storage(err.to_string()))?;
        Ok(Self {
            client_id: client_id(&config.person, device_id)?,
            metadata: Box::new(metadata),
        })
    }

    /// The client id, which the application sends to its backend to get the tokens of its
    /// rooms.
    pub fn client_id(&self) -> ClientId {
        self.client_id.clone()
    }

    /// The diagnosis of `room_id`; `RoomClosed` if it is not open in the client.
    pub async fn status(&self, room_id: RoomId) -> Result<RoomStatus, ClientError> {
        Err(ClientError::RoomClosed(room_id))
    }

    /// Shuts the client down, releasing its metadata for another client.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        self.metadata.close().await;
        Ok(())
    }
}
