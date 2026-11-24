//! What crosses the boundary of the dispatcher: the commands of the pages, and the replies and the
//! notices of the dispatcher, all of them serializable data.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use zemdb_core::{ClientId, RoomId};

use crate::{ClientConfig, ClientError, Freshness, Key, RoomStatus, Row, Scan};

/// The configuration of the client of a dispatcher, which the page gives in its
/// [`Request::Open`]: what the application chooses of the client, and what the platform opens it
/// over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchConfig {
    /// The configuration of the client, which the open checks as [`ZemdbClient::open`] does.
    ///
    /// [`ZemdbClient::open`]: crate::ZemdbClient::open
    pub client: ClientConfig,
    /// The URL of the server.
    pub server: String,
    /// The directory of the client: its metadata and its replicas (in the browser, a directory of
    /// the origin private file system).
    pub directory: String,
    /// The longest a request of the transport may go without advancing (60 s).
    pub idle_timeout: Duration,
    /// The most event streams the transport keeps open at once, in the browser (2).
    pub max_streams: u32,
    /// Whether the open, in the browser, waits for the tab that holds the directory to let it go,
    /// instead of failing at once (off).
    pub wait: bool,
}

impl DispatchConfig {
    /// The configuration of a client of `person`, over `server`, in `directory`.
    pub fn new(person: &str, server: &str, directory: &str) -> Self {
        Self {
            client: ClientConfig::new(person),
            server: server.to_string(),
            directory: directory.to_string(),
            idle_timeout: Duration::from_secs(60),
            max_streams: 2,
            wait: false,
        }
    }
}

/// A command of a page: its request, with the id of the tab that sends it and its own id, which
/// its [`Reply`] carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    /// The tab that sends it, which its reply goes to.
    pub tab: u64,
    /// Its id, which the tab chooses.
    pub id: u64,
    /// What it asks for.
    pub request: Request,
}

/// What a [`Command`] asks for: each is one call to the facade, or a step of the dispatcher.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    /// Opens the client, once ([`Answer::ClientId`]): the platform opens it over what the
    /// configuration names.
    Open(Box<DispatchConfig>),
    /// The client id ([`Answer::ClientId`]).
    ClientId,
    /// Shuts the client down, as [`ZemdbClient::shutdown`](crate::ZemdbClient::shutdown) does:
    /// every command on a room that comes afterwards is `RoomClosed`.
    Shutdown,
    /// A call on a room, by its id.
    Room(RoomId, RoomCall),
    /// A call on a table of a room, by its name.
    Table(Table, TableCall),
    /// The tab left: the rooms that it opened, and no other tab did, close.
    Left,
}

/// A call on a room, as the [`RoomHandle`](crate::RoomHandle) of the room makes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoomCall {
    /// Opens the room, as [`ZemdbClient::room`](crate::ZemdbClient::room) does.
    Open,
    /// Closes the room.
    Close,
    /// Discards the local replica of the room.
    Discard,
    /// The diagnosis of the room ([`Answer::Status`]).
    Status,
    /// Catches the replica up with the server.
    Sync,
    /// Sets the mode of the room.
    SetFreshness(Freshness),
}

/// A table of a room, by its name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    /// The room.
    pub room_id: RoomId,
    /// The name of the table.
    pub name: String,
}

/// A call on a table, as the [`TableHandle`](crate::TableHandle) of the table makes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableCall {
    /// The row of a key ([`Answer::Row`]).
    Get(Key),
    /// The rows of a scan ([`Answer::Rows`]).
    Scan(Scan),
    /// Inserts a row ([`Answer::Key`]).
    Insert(Row),
    /// Updates the row of a key with some changes.
    Update(Key, Row),
    /// Deletes the row of a key.
    Delete(Key),
}

/// What a command gives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    /// Done, without a value.
    Done,
    /// The client id.
    ClientId(ClientId),
    /// The diagnosis of a room.
    Status(RoomStatus),
    /// The row of a key, if the replica has one.
    Row(Option<Row>),
    /// The rows of a scan.
    Rows(Vec<Row>),
    /// The key of the row that an insert wrote.
    Key(Key),
}

/// What the dispatcher publishes: the replies to the commands, in the order they end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// The reply to a command.
    Reply(Reply),
}

/// The reply to a [`Command`], to its tab.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reply {
    /// The tab of the command.
    pub tab: u64,
    /// The id of the command.
    pub id: u64,
    /// What it gave.
    pub result: Result<Answer, ClientError>,
}
