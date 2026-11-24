use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use zemdb_core::{ClientId, CryptoEngine, RoomId};
use zemdb_storage::StorageEngine;

use super::RoomHandle;
use crate::{
    Client, ClientConfig, ClientError, Freshness, MetadataStore, TokenProvider, TransportClient,
};

/// The client of a device, through the facade: the [`Client`] of the core, which runs the worker
/// of each room it opens, and the [`CryptoEngine`] of the application, if it has encrypted
/// columns. A clone shares the client, and so do its handles.
#[derive(Clone)]
pub struct ZemdbClient {
    inner: Arc<Inner>,
}

struct Inner {
    client: Arc<Client>,
    crypto: Option<Arc<dyn CryptoEngine>>,
    /// Whether the writes are prechecked (`ClientConfig::precheck`).
    precheck: bool,
    /// The mode the application chose for each room it chose one for
    /// ([`RoomHandle::set_freshness`]); the others are in that of the configuration. Under this
    /// lock, which nothing holds across an `.await`, the mode that the worker of a room is asked
    /// for follows from the mode of the room and its subscriptions of `watch`
    /// ([`ZemdbClient::tune`]).
    modes: Mutex<HashMap<RoomId, Freshness>>,
}

impl ZemdbClient {
    /// Opens the [`Client`] of `config.person` over `storage`, `metadata` and `transport`, with
    /// `tokens` for the tokens of its rooms, as [`Client::open`] does, and `crypto` for the
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
            client: Arc::new(client),
            crypto,
            precheck,
            modes: Mutex::default(),
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

    /// Opens `room_id`, as [`Client::open_room`] does, with the token that the token provider
    /// gives for it, starts its worker, and returns its handle.
    ///
    /// The token provider has 60 s to give the token. A token that is not of the client id of
    /// the client (or not in the format in which the server issues tokens) is `Config`, and
    /// nothing is sent with it. Without a token (the provider fails, or does not answer in time,
    /// say without network), the room opens all the same, with the token it had in the client,
    /// or none: a room with metadata opens to be read, and its registration, refused for its
    /// token, asks the provider once more; the next one, from the worker, renews the token once
    /// the provider gives one. A room without metadata needs the server: without a valid reply,
    /// it has no handle, nor a worker, and the error is that of the open (`Network`).
    ///
    /// The worker runs in the mode of the room ([`RoomHandle::set_freshness`]), and a second
    /// handle of the room does not start another; [`RoomHandle::close`] stops it. A room whose
    /// replica fails closed (the schema of the server does not evolve from the replica's) has
    /// its handle all the same, to be read and discarded, without a worker: its status says it
    /// fails closed. A room closed while it starts its worker is `RoomClosed`, or, if the close
    /// takes the applier after the start, gets its worker stopped by the close.
    ///
    /// Each call asks the provider first, also for a room that is open, which keeps its token if
    /// the provider gives none; after [`shutdown`](Self::shutdown), it is `RoomClosed` without
    /// asking. The call asks the provider at most twice, each time with its 60 s: once more if the
    /// registration of the open needs a new token (refused for its token, or after a token the
    /// server forbade); and it waits for a registration of the room in progress, which may be
    /// waiting for the provider too, so a provider that does not answer may hold it for three
    /// such waits. A call cut once the room opened leaves it open without a worker, until the
    /// next call starts one.
    pub async fn room(&self, room_id: RoomId) -> Result<RoomHandle, ClientError> {
        let core = self.core();
        let token = core.first_token(&room_id).await?;
        match core.open_with(room_id.clone(), token).await {
            Ok(()) => {}
            Err(ClientError::FailedClosed(_)) if core.read_schema(&room_id, |_| ()).is_ok() => {
                return Ok(RoomHandle::new(self.clone(), room_id));
            }
            Err(err) => return Err(err),
        }
        // A worker that runs takes the mode again once it is set: the subscriptions may have
        // changed meanwhile.
        let freshness = self.tune(&room_id);
        core.set_freshness(room_id.clone(), freshness).await?;
        self.tune(&room_id);
        Ok(RoomHandle::new(self.clone(), room_id))
    }

    /// Discards the local replica of `room_id`, as [`Client::discard_room`] does, in any state:
    /// also a room without a handle, because its open failed (`SnapshotConstraints` or
    /// `SchemaMismatch`, say), or one that this client never opened. Its worker stops, its
    /// `watch` streams end, and its handles read `RoomClosed` until [`room`](Self::room) opens it
    /// again, as new, with the schema and the state of the server.
    pub async fn discard_room(&self, room_id: RoomId) -> Result<(), ClientError> {
        self.inner.client.discard_room(room_id).await
    }

    /// Shuts the client down, as [`Client::shutdown`] does: every room closes, with its worker
    /// stopped, and its handles read `RoomClosed`.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        self.inner.client.shutdown().await
    }

    /// The handle of `room_id`, without opening it: what it reaches is the room as the client has
    /// it at each call, as with the handle that [`room`](Self::room) gives.
    pub(crate) fn handle(&self, room_id: RoomId) -> RoomHandle {
        RoomHandle::new(self.clone(), room_id)
    }

    /// The client of the core.
    pub(super) fn core(&self) -> &Arc<Client> {
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

    /// Sets the mode of `room_id` to `freshness`, and its worker to the mode that follows.
    pub(super) fn set_mode(&self, room_id: &RoomId, freshness: Freshness) {
        let mut modes = self.modes();
        modes.insert(room_id.clone(), freshness);
        self.tune_with(&modes, room_id);
    }

    /// Asks the worker of `room_id`, if it runs, for the mode that follows from the mode of the
    /// room and its subscriptions of `watch` now, and returns it: `Live` while it has a
    /// subscription, in `Periodic`, and the mode of the room otherwise. Under the lock of the
    /// modes, so that, of two calls, the one that reads the subscriptions last asks last.
    pub(super) fn tune(&self, room_id: &RoomId) -> Freshness {
        self.tune_with(&self.modes(), room_id)
    }

    fn tune_with(&self, modes: &HashMap<RoomId, Freshness>, room_id: &RoomId) -> Freshness {
        let core = self.core();
        let mode = modes.get(room_id).copied();
        let freshness = match mode.unwrap_or(core.config().freshness) {
            Freshness::Periodic if core.is_watched(room_id) => Freshness::Live,
            freshness => freshness,
        };
        core.tune_worker(room_id, freshness);
        freshness
    }

    fn modes(&self) -> std::sync::MutexGuard<'_, HashMap<RoomId, Freshness>> {
        (self.inner.modes.lock()).unwrap_or_else(PoisonError::into_inner)
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

#[cfg(test)]
#[path = "tests/client.rs"]
mod tests;
