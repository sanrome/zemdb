//! The dispatcher of the runtime of a browser Worker: it takes the [`Command`]s of the pages, as
//! serializable data, runs each one as a call to the facade, and publishes the [`Reply`] of each
//! and its [`Notice`]s, also serializable: the values of the subscriptions of `watch`, one at a
//! time, their ends, and the requests of a token.
//!
//! It does not depend on the browser: the [`Platform`] opens the client over the storage, the
//! metadata and the transport of the platform, and the entry point of the Worker decodes the
//! messages of the pages and posts what the dispatcher publishes.

mod command;
mod subscription;
mod token;

use async_trait::async_trait;
use futures::future::OptionFuture;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use tokio::sync::oneshot::{self, error::TryRecvError};
use zemdb_core::RoomId;

pub use command::{
    Answer, Command, DispatchConfig, Message, Notice, Reply, Request, RoomCall, Table, TableCall,
    Watch,
};
pub use token::PageTokens;

use crate::{rt, ClientError, ZemdbClient};
use subscription::Watcher;

/// What a command gives.
type Outcome = Result<Answer, ClientError>;

/// What opens the client of a dispatcher: the platform, which builds the storage engine, the
/// metadata and the transport over what the configuration names.
#[async_trait]
pub trait Platform: Send + Sync {
    /// Opens the client of `config`, as [`ZemdbClient::open`] does, with `tokens` as its token
    /// provider, which asks the pages for the tokens.
    async fn open(
        &self,
        config: DispatchConfig,
        tokens: PageTokens,
    ) -> Result<ZemdbClient, ClientError>;
}

/// The dispatcher of the commands of the pages of a client.
///
/// Each command that reaches the client runs in a task of its own, and its reply carries its id
/// and its tab: a write that waits for its `CommitAck` does not hold back a read, the replies come
/// in the order the commands end, and two commands in flight have no order between them, as two
/// calls of the facade, but for the calls that open, close or discard a room, and the closes of a
/// tab that left: those of a room run one at a time, in the order they were dispatched. Each
/// command is one call to the facade, and the dispatcher never repeats a write. The client opens
/// once; once it shuts down, every command on a room is `RoomClosed`.
///
/// A subscription of `watch` is a task that publishes the values of its stream to its tab, each
/// once the tab confirmed the one before, and then the end of its stream; it also ends with
/// `Unwatch`, once its tab left, and once the dispatcher is dropped. A request of a token goes to
/// a tab of its room.
///
/// A command that panics ends without a reply, in native, and the turns it took or waited for
/// end with it: the next call of each room goes on. In WebAssembly, a panic closes the Worker.
pub struct Dispatcher {
    shared: Arc<Shared>,
}

/// The state of a dispatcher, which its tasks share.
struct Shared {
    platform: Box<dyn Platform>,
    publish: Box<dyn Fn(Message) + Send + Sync>,
    /// Held by an open and by a shutdown: an open is refused while another one or a shutdown is
    /// in flight, and a shutdown waits for the open in flight.
    life: Arc<tokio::sync::Mutex<()>>,
    /// Under a lock that no `.await` crosses.
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    client: Option<ZemdbClient>,
    shut_down: bool,
    /// The tabs that opened each room that some tab holds, until it is closed or discarded, which
    /// only the calls of the room in their turn change.
    rooms: HashMap<RoomId, HashSet<u64>>,
    /// Per room with a call to open, close or discard it, or the notice that a tab left, in its
    /// turn, what ends once the last of them dispatched ended; until another turn starts, also
    /// per room whose turns all ended.
    turns: HashMap<RoomId, oneshot::Receiver<()>>,
    /// The tab whose open of each room runs in its turn.
    opening: HashMap<RoomId, u64>,
    /// The tabs whose notice that they left is in flight, once per notice.
    leaving: Vec<u64>,
    /// The last id given to a subscription or to a request of a token.
    last: u64,
    /// The subscriptions, each with its tab, until their tasks end.
    subscriptions: HashMap<u64, (u64, Watcher)>,
    /// The requests of a token that the client waits for, each with the tab it went to.
    tokens: HashMap<u64, (u64, oneshot::Sender<Option<String>>)>,
    /// Whether the dispatcher was dropped: no tab answers a request of a token any more.
    dropped: bool,
}

impl Dispatcher {
    /// A dispatcher whose client `platform` opens, which publishes its replies and its notices
    /// with `publish`, without holding any lock: `publish` may dispatch a command.
    pub fn new(
        platform: impl Platform + 'static,
        publish: impl Fn(Message) + Send + Sync + 'static,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                platform: Box::new(platform),
                publish: Box::new(publish),
                life: Arc::default(),
                state: Mutex::default(),
            }),
        }
    }

    /// Runs `command` in a task of its own, of the Tokio runtime it is called in, and publishes
    /// its reply once it ends. A command that does not reach the client (a `ClientId`, or a
    /// command on a room before the open or after the shutdown) replies at once, and so do
    /// `Watch`, `Confirm` and `Token`. `Unwatch`, `Confirm` and `Token` change nothing for a
    /// subscription or a request that ended, or that is not of the tab.
    pub fn dispatch(&self, command: Command) {
        let Command { tab, id, request } = command;
        let shared = &self.shared;
        match request {
            Request::Open(config) => shared.open(tab, id, *config),
            Request::ClientId => {
                let client_id = shared.state().client.as_ref().map(ZemdbClient::client_id);
                let not_open = || ClientError::Config("the client is not open yet".to_string());
                shared.reply(
                    tab,
                    id,
                    client_id.map(Answer::ClientId).ok_or_else(not_open),
                );
            }
            Request::Shutdown => shared.shutdown(tab, id),
            Request::Room(room_id, call) => match shared.client(&room_id) {
                Ok(client)
                    if matches!(call, RoomCall::Open | RoomCall::Close | RoomCall::Discard) =>
                {
                    let (turn, shared) = (shared.turn(&room_id), Arc::clone(shared));
                    self.shared.spawn(tab, id, async move {
                        let _turn = turn.await;
                        let opening =
                            (call == RoomCall::Open).then(|| shared.opening(&room_id, tab));
                        let answer = room(client, room_id.clone(), call).await;
                        let mut state = shared.state();
                        if call != RoomCall::Open {
                            state.rooms.remove(&room_id);
                        } else if answer.is_ok() && !state.shut_down {
                            state.rooms.entry(room_id).or_default().insert(tab);
                        }
                        // The tab holds the room before the mark of its open goes.
                        drop((state, opening));
                        answer
                    });
                }
                Ok(client) => shared.spawn(tab, id, room(client, room_id, call)),
                Err(err) => shared.reply(tab, id, Err(err)),
            },
            Request::Table(table, call) => match shared.client(&table.room_id) {
                Ok(client) => shared.spawn(tab, id, on_table(client, table, call)),
                Err(err) => shared.reply(tab, id, Err(err)),
            },
            Request::Watch(table, watch) => subscription::start(shared, tab, id, table, watch),
            Request::Unwatch(subscription) => {
                let mut state = shared.state();
                let watcher = state.subscriptions.get_mut(&subscription);
                let ended = watcher.filter(|entry| entry.0 == tab).map(|(_, w)| w.end());
                drop(state);
                shared.spawn(tab, id, async move {
                    OptionFuture::from(ended).await;
                    Ok(Answer::Done)
                });
            }
            Request::Confirm(subscription) => {
                let mut state = shared.state();
                let watcher = state.subscriptions.get_mut(&subscription);
                let watcher = watcher.filter(|entry| entry.0 == tab);
                if let Some(confirm) = watcher.and_then(|(_, watcher)| watcher.confirm.take()) {
                    // A task cut meanwhile does not take it.
                    let _ = confirm.send(());
                }
                drop(state);
                shared.reply(tab, id, Ok(Answer::Done));
            }
            Request::Token(request, token) => {
                if let Some(answer) = take(&mut shared.state().tokens, tab, request) {
                    // A provider that stopped waiting meanwhile does not take it.
                    let _ = answer.send(token);
                }
                shared.reply(tab, id, Ok(Answer::Done));
            }
            Request::Left => shared.left(tab, id),
        }
    }
}

impl Drop for Dispatcher {
    /// Ends the subscriptions, whose tasks would keep the dispatcher otherwise, and the requests
    /// of a token, which no tab answers any more.
    fn drop(&mut self) {
        let mut state = self.shared.state();
        state.dropped = true;
        let ended = (
            std::mem::take(&mut state.subscriptions),
            std::mem::take(&mut state.tokens),
        );
        drop((state, ended));
    }
}

impl State {
    /// A new id of a subscription or of a request of a token.
    fn next_id(&mut self) -> u64 {
        self.last += 1;
        self.last
    }
}

/// The entry `id` of `tab` in `entries`, taken out of them: an entry of another tab stays.
fn take<T>(entries: &mut HashMap<u64, (u64, T)>, tab: u64, id: u64) -> Option<T> {
    match entries.get(&id) {
        Some((of, _)) if *of == tab => entries.remove(&id).map(|(_, entry)| entry),
        _ => None,
    }
}

/// What undoes a change of the state of a dispatcher.
type Change = Box<dyn FnOnce(&mut State) + Send>;

/// A change of the state of a dispatcher, undone once this is dropped, also on a panic.
struct Undo(Weak<Shared>, Option<Change>);

impl Drop for Undo {
    fn drop(&mut self) {
        if let (Some(shared), Some(undo)) = (self.0.upgrade(), self.1.take()) {
            undo(&mut shared.state());
        }
    }
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn reply(&self, tab: u64, id: u64, result: Outcome) {
        (self.publish)(Message::Reply(Reply { tab, id, result }));
    }

    fn notice(&self, notice: Notice) {
        (self.publish)(Message::Notice(notice));
    }

    /// What undoes `undo` on the state once it is dropped.
    fn undo(self: &Arc<Self>, undo: impl FnOnce(&mut State) + Send + 'static) -> Undo {
        Undo(Arc::downgrade(self), Some(Box::new(undo)))
    }

    /// Marks `tab` as the tab whose open of `room_id` runs in its turn, until what it gives is
    /// dropped.
    fn opening(self: &Arc<Self>, room_id: &RoomId, tab: u64) -> Undo {
        self.state().opening.insert(room_id.clone(), tab);
        let room_id = room_id.clone();
        self.undo(move |state| {
            state.opening.remove(&room_id);
        })
    }

    /// Marks `tab` in `state` as a tab whose notice that it left is in flight, until what it gives
    /// is dropped: no request of a token goes to it meanwhile.
    fn leaving(self: &Arc<Self>, state: &mut State, tab: u64) -> Undo {
        state.leaving.push(tab);
        self.undo(move |state| {
            if let Some(at) = state.leaving.iter().position(|of| *of == tab) {
                state.leaving.swap_remove(at);
            }
        })
    }

    /// The open client, for a command on `room_id`: `RoomClosed` before the open and once the
    /// client shuts down.
    fn client(&self, room_id: &RoomId) -> Result<ZemdbClient, ClientError> {
        let state = self.state();
        let client = state.client.clone().filter(|_| !state.shut_down);
        client.ok_or_else(|| ClientError::RoomClosed(room_id.clone()))
    }

    /// The turn of a call of `room_id` that opens, closes or discards it, dispatched now: it comes
    /// once the one dispatched before it ended, and ends once what it gives is dropped, also on a
    /// panic.
    fn turn(&self, room_id: &RoomId) -> impl Future<Output = oneshot::Sender<()>> + Send + 'static {
        let (ended, next) = oneshot::channel();
        let mut state = self.state();
        // The rooms whose turns all ended leave, so that only those with a turn in flight stay.
        let turns = &mut state.turns;
        turns.retain(|_, end| end.try_recv() == Err(TryRecvError::Empty));
        let before = state.turns.insert(room_id.clone(), next);
        async move {
            // It ends without a value, once the turn before it is dropped.
            let _ = OptionFuture::from(before).await;
            ended
        }
    }

    /// Runs `call` in a task of its own, and replies to the command `id` of `tab` with what it
    /// gives.
    fn spawn<F>(self: &Arc<Self>, tab: u64, id: u64, call: F)
    where
        F: Future<Output = Outcome> + Send + 'static,
    {
        let shared = Arc::clone(self);
        rt::spawn(async move {
            let result = call.await;
            shared.reply(tab, id, result);
        });
    }

    /// Opens the client in a task that holds the life lock, unless it is open, or an open or a
    /// shutdown came before. A client that fails to open may be opened again.
    fn open(self: &Arc<Self>, tab: u64, id: u64, config: DispatchConfig) {
        let state = self.state();
        let life = Arc::clone(&self.life).try_lock_owned();
        let (false, None, Ok(life)) = (state.shut_down, &state.client, life) else {
            drop(state);
            let refused = ClientError::Config("the client opens once".to_string());
            return self.reply(tab, id, Err(refused));
        };
        let shared = Arc::clone(self);
        self.spawn(tab, id, async move {
            let tokens = PageTokens::new(&shared);
            let client = shared.platform.open(config, tokens).await?;
            let client_id = client.client_id();
            // A shutdown that came meanwhile waits for the life lock, and shuts it down.
            shared.state().client = Some(client);
            drop(life);
            Ok(Answer::ClientId(client_id))
        });
    }

    /// Shuts the client down once the open in flight, if any, ended: every command on a room
    /// that comes afterwards is `RoomClosed`, and the subscriptions end with their streams. The
    /// tabs of the rooms are forgotten once the client shut down, so that a request of a token
    /// meanwhile (the last acknowledgement of a worker, a write in flight) goes to a tab of its
    /// room.
    fn shutdown(self: &Arc<Self>, tab: u64, id: u64) {
        self.state().shut_down = true;
        let shared = Arc::clone(self);
        self.spawn(tab, id, async move {
            let _life = shared.life.lock().await;
            let client = shared.state().client.clone();
            let shut = match client {
                Some(client) => client.shutdown().await.map(|()| Answer::Done),
                None => Ok(Answer::Done),
            };
            shared.state().rooms.clear();
            shut
        });
    }

    /// Ends the subscriptions of `tab` and the requests of a token to it, and closes the rooms
    /// that it opened and no other tab did (it releases them, but closes none, once the client
    /// shuts down, which closes them),
    /// each in the turn of the room: once the calls of the room dispatched before it ended, and
    /// before those dispatched after it. It takes the turn of
    /// the rooms that the tab holds and of those with a turn in flight, which may be of the tab,
    /// and waits for each on its own, so that a room does not wait for another. Until it replies,
    /// no request of a token goes to the tab, and once it replies, no notice of its subscriptions
    /// comes.
    fn left(self: &Arc<Self>, tab: u64, id: u64) {
        let Some(client) = self.state().client.clone() else {
            return self.reply(tab, id, Ok(Answer::Done));
        };
        let (leaving, rooms, watchers) = {
            let mut state = self.state();
            // With the requests to the tab ending, so that no other goes to it.
            let leaving = self.leaving(&mut state, tab);
            state.tokens.retain(|_, (of, _)| *of != tab);
            // Also those that an `Unwatch` cut, whose tasks may not have ended yet.
            let of_tab = state
                .subscriptions
                .values_mut()
                .filter(|(of, _)| *of == tab);
            let watchers = Vec::from_iter(of_tab.map(|(_, watcher)| watcher.end()));
            let mut rooms = HashSet::<RoomId>::from_iter(state.turns.keys().cloned());
            let held = state.rooms.iter().filter(|(_, tabs)| tabs.contains(&tab));
            rooms.extend(held.map(|(room_id, _)| room_id.clone()));
            (leaving, rooms, watchers)
        };
        let closes = Vec::from_iter(rooms.into_iter().map(|room_id| {
            let (turn, shared, client) = (self.turn(&room_id), Arc::clone(self), client.clone());
            async move {
                let _turn = turn.await;
                if !shared.release(&room_id, tab) {
                    return Ok(());
                }
                match client.handle(room_id).close().await {
                    // A room that the client closed by another way.
                    Err(ClientError::RoomClosed(_)) => Ok(()),
                    closed => closed,
                }
            }
        }));
        self.spawn(tab, id, async move {
            let _leaving = leaving;
            let ended = futures::future::join_all(watchers);
            let (closed, _) = futures::future::join(futures::future::join_all(closes), ended).await;
            let closed = closed.into_iter();
            closed.collect::<Result<Vec<()>, _>>().map(|_| Answer::Done)
        });
    }

    /// Takes `tab` out of the tabs of `room_id`, and the room out of `rooms` once no tab holds it:
    /// whether the tab held the room, was the last tab that did, and the room is to be closed,
    /// which it never is once the client shuts down, since the shutdown closes every room. So a
    /// notice that a tab left does not reach the client then, and no request of a token of the
    /// shutdown goes to the tab once the notice replied.
    fn release(&self, room_id: &RoomId, tab: u64) -> bool {
        let mut state = self.state();
        let tabs = state.rooms.get_mut(room_id);
        tabs.is_some_and(|tabs| tabs.remove(&tab) && tabs.is_empty())
            && state.rooms.remove(room_id).is_some()
            && !state.shut_down
    }
}

/// The call `call` on `room_id`.
async fn room(client: ZemdbClient, room_id: RoomId, call: RoomCall) -> Outcome {
    let room = client.handle(room_id.clone());
    match call {
        RoomCall::Open => client.room(room_id).await.map(|_| Answer::Done),
        RoomCall::Close => room.close().await.map(|()| Answer::Done),
        RoomCall::Discard => room.discard().await.map(|()| Answer::Done),
        RoomCall::Status => room.status().await.map(Answer::Status),
        RoomCall::Sync => room.sync().await.map(|()| Answer::Done),
        RoomCall::SetFreshness(freshness) => {
            room.set_freshness(freshness);
            Ok(Answer::Done)
        }
    }
}

/// The call `call` on `table`.
async fn on_table(client: ZemdbClient, table: Table, call: TableCall) -> Outcome {
    let table = client.handle(table.room_id).table(&table.name)?;
    match call {
        TableCall::Get(key) => table.get(&key).await.map(Answer::Row),
        TableCall::Scan(scan) => table.scan(&scan).await.map(Answer::Rows),
        TableCall::Insert(row) => table.insert(&row).await.map(Answer::Key),
        TableCall::Update(key, delta) => table.update(&key, &delta).await.map(|()| Answer::Done),
        TableCall::Delete(key) => table.delete(&key).await.map(|()| Answer::Done),
    }
}

#[cfg(test)]
#[path = "../tests/dispatch.rs"]
mod tests;
