//! The dispatcher of the runtime of a browser Worker: it takes the [`Command`]s of the pages, as
//! serializable data, runs each one as a call to the facade, and publishes the [`Reply`] of each,
//! also serializable.
//!
//! It does not depend on the browser: the [`Platform`] opens the client over the storage, the
//! metadata and the transport of the platform, and the entry point of the Worker decodes the
//! messages of the pages and posts what the dispatcher publishes.

mod command;

use async_trait::async_trait;
use futures::future::OptionFuture;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::oneshot::{self, error::TryRecvError};
use zemdb_core::RoomId;

pub use command::{
    Answer, Command, DispatchConfig, Message, Reply, Request, RoomCall, Table, TableCall,
};

use crate::{rt, ClientError, ZemdbClient};

/// What a command gives.
type Outcome = Result<Answer, ClientError>;

/// What opens the client of a dispatcher: the platform, which builds the storage engine, the
/// metadata, the transport and the token provider over what the configuration names.
#[async_trait]
pub trait Platform: Send + Sync {
    /// Opens the client of `config`, as [`ZemdbClient::open`] does.
    async fn open(&self, config: DispatchConfig) -> Result<ZemdbClient, ClientError>;
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
}

impl Dispatcher {
    /// A dispatcher whose client `platform` opens, which publishes its replies with `publish`,
    /// without holding any lock: `publish` may dispatch a command.
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
    /// command on a room before the open or after the shutdown) replies at once.
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
                        let answer = room(client, room_id.clone(), call).await;
                        let mut state = shared.state();
                        if call != RoomCall::Open {
                            state.rooms.remove(&room_id);
                        } else if answer.is_ok() && !state.shut_down {
                            state.rooms.entry(room_id).or_default().insert(tab);
                        }
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
            Request::Left => shared.left(tab, id),
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
            let client = shared.platform.open(config).await?;
            let client_id = client.client_id();
            // A shutdown that came meanwhile waits for the life lock, and shuts it down.
            shared.state().client = Some(client);
            drop(life);
            Ok(Answer::ClientId(client_id))
        });
    }

    /// Shuts the client down once the open in flight, if any, ended: every command on a room
    /// that comes afterwards is `RoomClosed`.
    fn shutdown(self: &Arc<Self>, tab: u64, id: u64) {
        let mut state = self.state();
        (state.shut_down, state.rooms) = (true, HashMap::new());
        let shared = Arc::clone(self);
        self.spawn(tab, id, async move {
            let _life = shared.life.lock().await;
            let client = shared.state().client.clone();
            match client {
                Some(client) => client.shutdown().await.map(|()| Answer::Done),
                None => Ok(Answer::Done),
            }
        });
    }

    /// Closes the rooms that `tab` opened and no other tab did, each in the turn of the room: once
    /// the calls of the room dispatched before it ended, and before those dispatched after it. It
    /// takes the turn of the rooms that the tab holds and of those with a turn in flight, which
    /// may be of the tab, and waits for each on its own, so that a room does not wait for another.
    fn left(self: &Arc<Self>, tab: u64, id: u64) {
        let (client, rooms) = {
            let state = self.state();
            let mut rooms = HashSet::<RoomId>::from_iter(state.turns.keys().cloned());
            let held = state.rooms.iter().filter(|(_, tabs)| tabs.contains(&tab));
            rooms.extend(held.map(|(room_id, _)| room_id.clone()));
            (state.client.clone(), rooms)
        };
        let Some(client) = client else {
            return self.reply(tab, id, Ok(Answer::Done));
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
            let closed = futures::future::join_all(closes).await.into_iter();
            closed.collect::<Result<Vec<()>, _>>().map(|_| Answer::Done)
        });
    }

    /// Whether `tab` held `room_id`, and was the last tab that did: then the room leaves `rooms`.
    fn release(&self, room_id: &RoomId, tab: u64) -> bool {
        let mut state = self.state();
        let tabs = state.rooms.get_mut(room_id);
        tabs.is_some_and(|tabs| tabs.remove(&tab) && tabs.is_empty())
            && state.rooms.remove(room_id).is_some()
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
