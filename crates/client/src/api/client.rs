use std::fmt;
use std::sync::Arc;
use zemdb_core::{ClientId, CryptoEngine, RoomId};
use zemdb_storage::StorageEngine;

use super::RoomHandle;
use crate::{Client, ClientConfig, ClientError, MetadataStore, TokenProvider, TransportClient};

/// The client of a device, through the facade: the [`Client`] of the core, and the
/// [`CryptoEngine`] of the application, if it has encrypted columns. A clone shares the client,
/// and so do its handles.
#[derive(Clone)]
pub struct ZemdbClient {
    inner: Arc<Inner>,
}

struct Inner {
    client: Client,
    crypto: Option<Arc<dyn CryptoEngine>>,
    /// Whether the writes are prechecked (`ClientConfig::precheck`).
    precheck: bool,
}

impl ZemdbClient {
    /// Opens the [`Client`] of `config.person` over `storage`, `metadata` and `transport`, with
    /// `tokens` for the new tokens of its rooms, as [`Client::open`] does, and `crypto` for the
    /// encrypted columns of its rooms, if the application has one.
    pub async fn open(
        config: ClientConfig,
        storage: impl StorageEngine + 'static,
        metadata: impl MetadataStore + 'static,
        transport: impl TransportClient + 'static,
        tokens: impl TokenProvider + 'static,
        crypto: Option<Arc<dyn CryptoEngine>>,
    ) -> Result<Self, ClientError> {
        let precheck = config.precheck;
        let client = Client::open(config, storage, metadata, transport, tokens).await?;
        let inner = Inner {
            client,
            crypto,
            precheck,
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// The client id, which the application sends to its backend to get the tokens of its
    /// rooms.
    pub fn client_id(&self) -> ClientId {
        self.inner.client.client_id()
    }

    /// Opens `room_id` with `token`, as [`Client::open_room`] does, and returns its handle.
    ///
    /// A room whose replica fails closed (the schema of the server does not evolve from the
    /// replica's) has its handle all the same, to be read: its status says it fails closed. A
    /// room without metadata needs the server: without a valid reply, it has no handle, and the
    /// error is that of the open (`Network`).
    pub async fn room(&self, room_id: RoomId, token: String) -> Result<RoomHandle, ClientError> {
        match self.inner.client.open_room(room_id.clone(), token).await {
            Ok(()) => {}
            Err(ClientError::FailedClosed(_))
                if self.inner.client.read_schema(&room_id, |_| ()).is_ok() => {}
            Err(err) => return Err(err),
        }
        Ok(RoomHandle::new(self.clone(), room_id))
    }

    /// Discards the local replica of `room_id`, as [`Client::discard_room`] does, in any state:
    /// also a room without a handle, because its open failed (`SnapshotConstraints` or
    /// `SchemaMismatch`, say), or one that this client never opened. Its `watch` streams end,
    /// and its handles read `RoomClosed` until [`room`](Self::room) opens it again, as new, with
    /// the schema and the state of the server.
    pub async fn discard_room(&self, room_id: RoomId) -> Result<(), ClientError> {
        self.inner.client.discard_room(room_id).await
    }

    /// Shuts the client down, as [`Client::shutdown`] does: every room closes, and its handles
    /// read `RoomClosed`.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        self.inner.client.shutdown().await
    }

    /// The client of the core.
    pub(super) fn core(&self) -> &Client {
        &self.inner.client
    }

    /// Whether the application gave a [`CryptoEngine`].
    pub(super) fn has_crypto(&self) -> bool {
        self.inner.crypto.is_some()
    }

    /// The [`CryptoEngine`] of the application, if it gave one.
    pub(super) fn crypto(&self) -> Option<&dyn CryptoEngine> {
        self.inner.crypto.as_deref()
    }

    /// Whether the writes are prechecked.
    pub(super) fn prechecks(&self) -> bool {
        self.inner.precheck
    }
}

impl fmt::Debug for ZemdbClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZemdbClient")
            .field("client_id", &self.inner.client.client_id())
            .field("crypto", &self.has_crypto())
            .field("precheck", &self.prechecks())
            .finish_non_exhaustive()
    }
}
