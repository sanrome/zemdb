use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use zemdb_core::RoomId;

use crate::client::{jitter, Backoff, Held, Transfer};
use crate::{Client, ClientConfig, ClientError, Freshness};

/// The worker of a room, as [`Client::status`](crate::Client::status) gives it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerStatus {
    /// The mode it runs in.
    pub freshness: Freshness,
    /// Whether it still runs: it stops by itself only once one of its steps found the room
    /// failing closed or out of the client.
    pub running: bool,
    /// Whether the last request of the room got a reply, from the worker or not: `None` before
    /// the first.
    pub connected: Option<bool>,
    /// The error of its last step, unless that step succeeded.
    pub last_error: Option<ClientError>,
}

/// What the worker of a room shares with its task.
struct Shared {
    freshness: Freshness,
    last_error: Option<ClientError>,
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The worker of a room: a task of its own, which holds the client by a `Weak`, so that it ends
/// once the application drops the client: at once, or at its next heartbeat if a task of a
/// worker, of any room, holds the client meanwhile (a heartbeat, a sync or a transfer). Dropping
/// the worker stops its task too.
pub(crate) struct Worker {
    shared: Arc<Mutex<Shared>>,
    /// Dropped to stop the task.
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl Worker {
    /// Starts the worker of `room_id` of `client`, in `freshness`.
    pub(crate) fn start(client: &Arc<Client>, room_id: RoomId, freshness: Freshness) -> Self {
        let shared = Arc::new(Mutex::new(Shared {
            freshness,
            last_error: None,
        }));
        let (stop, stopped) = oneshot::channel();
        let config = client.config().clone();
        let task = run(
            Arc::downgrade(client),
            config,
            room_id,
            Arc::clone(&shared),
            stopped,
        );
        Self {
            shared,
            stop,
            task: tokio::spawn(task),
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        !self.task.is_finished()
    }

    pub(crate) fn set_freshness(&self, freshness: Freshness) {
        lock(&self.shared).freshness = freshness;
    }

    /// Its status, with `connected` for the room.
    pub(crate) fn status(&self, connected: Option<bool>) -> WorkerStatus {
        let shared = lock(&self.shared);
        WorkerStatus {
            freshness: shared.freshness,
            running: self.is_running(),
            connected,
            last_error: shared.last_error.clone(),
        }
    }

    /// Stops its task at its next `.await`, and waits until the task, its sync and its transfer
    /// ended.
    pub(crate) async fn stop(self) {
        let Self { stop, task, .. } = self;
        drop(stop);
        let _ = task.await;
    }
}

/// Takes `result`, of a step of the worker: whether it stops the worker, as only
/// `FailedClosed` and `RoomClosed` do.
fn ended(shared: &Mutex<Shared>, result: Result<(), ClientError>) -> bool {
    let stops = matches!(
        result,
        Err(ClientError::FailedClosed(_) | ClientError::RoomClosed(_))
    );
    lock(shared).last_error = result.err();
    stops
}

/// Whether the mode of the worker syncs by the heads that its heartbeats report: all but
/// `Manual`.
fn syncs(shared: &Mutex<Shared>) -> bool {
    lock(shared).freshness != Freshness::Manual
}

/// The client of `weak`, unless the application dropped it: the tasks of the workers hold the
/// client while they run, so once nothing else does, the application dropped it.
fn held(weak: &Weak<Client>) -> Option<Held> {
    Held::upgrade(weak).filter(|client| !client.dropped())
}

/// What a task of the worker returns once it ends: a task that panicked or was cut is `Storage`.
async fn ended_task<T>(
    task: &mut Option<JoinHandle<Result<T, ClientError>>>,
) -> Result<T, ClientError> {
    let ended = match task {
        Some(task) => task.await,
        None => std::future::pending().await,
    };
    ended.unwrap_or_else(|err| {
        Err(ClientError::Storage(format!(
            "A task of the worker failed: {err}"
        )))
    })
}

/// Starts `wanted` in the task of the transfers, unless a transfer runs.
fn transfer(
    transfer: &mut Option<JoinHandle<Result<(), ClientError>>>,
    client: &Held,
    room_id: &RoomId,
    wanted: Transfer,
) {
    if transfer.is_none() {
        let (client, room_id) = (client.clone(), room_id.clone());
        let task = async move { client.transfer(&room_id, wanted).await };
        *transfer = Some(tokio::spawn(task));
    }
}

/// Starts a sync in the task of the syncs, unless a sync runs: then marks one more for after it.
fn sync(
    sync: &mut Option<JoinHandle<Result<bool, ClientError>>>,
    pending: &mut bool,
    client: &Held,
    room_id: &RoomId,
) {
    if sync.is_some() {
        *pending = true;
        return;
    }
    let (client, room_id) = (client.clone(), room_id.clone());
    let task = async move { client.worker_sync(&room_id).await };
    *sync = Some(tokio::spawn(task));
}

/// The task of the worker of `room_id`, until `stop` is dropped, the client is dropped, or a
/// step stops it; then it cuts its sync and its transfer, and waits until they ended. It sends
/// a heartbeat every interval, the first at a random moment within the first, so that the
/// clients that start together do not send theirs together. A heartbeat transfers nothing: the
/// snapshot its ack asks to upload or announces goes in a task of its own, one at a time, and
/// the heartbeats go on meanwhile. Unless the mode is `Manual`, a heartbeat whose ack reports a
/// head past that of the replica leads to a sync, in a task of its own, without an
/// acknowledgement: one at a time, and one more once it ends if a heartbeat asked for one
/// meanwhile and the mode still syncs. A batch of the sync that asks the client for a snapshot is confirmed by a
/// heartbeat in the task of the transfers. A heartbeat that fails is sent again after a backoff
/// with full jitter, and the first that succeeds goes back to the interval.
async fn run(
    weak: Weak<Client>,
    config: ClientConfig,
    room_id: RoomId,
    shared: Arc<Mutex<Shared>>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut backoff = Backoff::worker(&config);
    let mut next = Instant::now() + jitter(config.heartbeat_interval);
    let mut transferring: Option<JoinHandle<Result<(), ClientError>>> = None;
    let mut syncing: Option<JoinHandle<Result<bool, ClientError>>> = None;
    // Whether a heartbeat asked for a sync while one ran.
    let mut pending = false;
    loop {
        tokio::select! {
            _ = &mut stop => break,
            transferred = ended_task(&mut transferring) => {
                transferring = None;
                if ended(&shared, transferred) {
                    break;
                }
                continue;
            }
            synced = ended_task(&mut syncing) => {
                syncing = None;
                let wanted = matches!(synced, Ok(true));
                if ended(&shared, synced.map(|_| ())) {
                    break;
                }
                // Unless the mode turned `Manual` meanwhile.
                let again = std::mem::take(&mut pending) && syncs(&shared);
                if !wanted && !again {
                    continue;
                }
                let Some(client) = held(&weak) else {
                    break;
                };
                if wanted {
                    transfer(&mut transferring, &client, &room_id, Transfer::Beat);
                }
                if again {
                    sync(&mut syncing, &mut pending, &client, &room_id);
                }
                continue;
            }
            () = tokio::time::sleep_until(next) => {}
        }
        let Some(client) = held(&weak) else {
            break;
        };
        let started = Instant::now();
        let beat = tokio::select! {
            _ = &mut stop => break,
            beat = client.worker_beat(&room_id) => beat,
        };
        let asked = match beat {
            Ok(asked) => asked,
            Err(err) => {
                next = Instant::now() + backoff.next();
                match ended(&shared, Err(err)) {
                    true => break,
                    false => continue,
                }
            }
        };
        ended(&shared, Ok(()));
        backoff = Backoff::worker(&config);
        next = started + config.heartbeat_interval;
        if let Some(wanted) = asked.transfer {
            transfer(&mut transferring, &client, &room_id, wanted);
        }
        if asked.behind && syncs(&shared) {
            sync(&mut syncing, &mut pending, &client, &room_id);
        }
    }
    let (syncing, transferring) = (syncing.map(cut), transferring.map(cut));
    if let Some(syncing) = syncing {
        let _ = syncing.await;
    }
    if let Some(transferring) = transferring {
        let _ = transferring.await;
    }
}

/// `task`, cut at its next `.await`.
fn cut<T>(task: JoinHandle<T>) -> JoinHandle<T> {
    task.abort();
    task
}
