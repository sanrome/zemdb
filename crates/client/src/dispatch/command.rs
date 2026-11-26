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
    /// The tab that sends it, which its reply goes to, and the notices of a subscription it
    /// starts.
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
    /// Subscribes the tab to the values of a read of a table, as
    /// [`TableHandle::watch`](crate::TableHandle::watch) and
    /// [`TableHandle::watch_scan`](crate::TableHandle::watch_scan) read them
    /// ([`Answer::Subscription`], which comes before its first value): each comes as a
    /// [`Notice::Value`], and the next one once the tab confirms it.
    Watch(Table, Watch),
    /// Ends a subscription of the tab: once its reply comes, no notice of it comes.
    Unwatch(u64),
    /// Confirms the value of a subscription of the tab that is in flight: the next one may come.
    Confirm(u64),
    /// The answer of the tab to a [`Notice::Token`], by the id of the request: a token of the
    /// client for the room, or none.
    Token(u64, Option<String>),
    /// The tab left: its subscriptions and the requests of a token to it end, and the rooms that
    /// it opened, and no other tab did, close.
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

/// What a subscription reads of its table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Watch {
    /// The row of a key ([`Answer::Row`]).
    Row(Key),
    /// The rows of a scan ([`Answer::Rows`]).
    Rows(Scan),
}

/// What a command, or a value of a subscription, gives.
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
    /// The id of a subscription, which its notices carry.
    Subscription(u64),
}

/// What the dispatcher publishes: the replies to the commands, in the order they end, and its
/// notices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// The reply to a command.
    Reply(Reply),
    /// A notice to a tab.
    Notice(Notice),
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

/// A notice of the dispatcher to a tab.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Notice {
    /// A value of a subscription of the tab, or the error of its read: the next one comes once
    /// the tab confirms this one ([`Request::Confirm`]), and is the value of the read then.
    Value {
        tab: u64,
        subscription: u64,
        value: Result<Answer, ClientError>,
    },
    /// A subscription of the tab ended with its stream, once its value in flight, if any, was
    /// confirmed: its room closed or was discarded, the client shut down, or an error took the
    /// room out of the client. No notice of a subscription comes once the reply to its
    /// [`Request::Unwatch`] came, nor once the reply to the [`Request::Left`] of its tab did.
    Ended { tab: u64, subscription: u64 },
    /// The client asks the tab for a token of `client_id` for `room_id`, which the tab answers
    /// with [`Request::Token`] and the id of the request, without waiting for a command of the
    /// room: the client waits for it as long as for any token provider (60 s). It may come after
    /// the reply to the [`Request::Left`] of the tab, whose answer then changes nothing.
    Token {
        tab: u64,
        request: u64,
        client_id: ClientId,
        room_id: RoomId,
    },
}
