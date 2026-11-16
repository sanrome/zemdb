use futures::future::{self, FutureExt};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use zemdb_core::RoomId;

use crate::client::{jitter, Acking, Backoff, Held, Opened, Transfer};
use crate::events::{EventReader, RoomEvent, SseEvent, StreamError};
use crate::{Client, ClientConfig, ClientError, Freshness};

/// The worker of a room, as [`Client::status`](crate::Client::status) gives it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerStatus {
    /// The mode it runs in.
    pub freshness: Freshness,
    /// The mode it runs in effectively: [`Freshness::Periodic`] in [`Freshness::Live`] once the
    /// server answered that its event stream is turned off, until the worker opens one.
    pub effective: Freshness,
    /// Whether its event stream is open.
    pub stream_open: bool,
    /// Whether it still runs: it stops by itself only once one of its steps found the room
    /// failing closed or out of the client.
    pub running: bool,
    /// Whether the last request of the room got a reply, from the worker or not: `None` before
    /// the first.
    pub connected: Option<bool>,
    /// The error of its last step, unless that step succeeded.
    pub last_error: Option<ClientError>,
}

/// How long the worker waits for its last acknowledgement once it is stopped.
const LAST_ACK: Duration = Duration::from_secs(5);

/// How many heartbeats in a row leave the replica waiting for a snapshot that no reply announced
/// before the next one registers it again.
const WAITING_BEATS: u32 = 10;

/// Every how many heartbeats that succeed the worker acknowledges the head of the replica also if
/// it did not move.
const ACK_AGAIN_BEATS: u32 = 10;

/// What the worker of a room shares with its task.
struct Shared {
    freshness: Freshness,
    last_error: Option<ClientError>,
    /// Whether the server answered that its event stream is turned off, until a stream opens.
    disabled: bool,
    stream_open: bool,
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The end of the task of a worker: it resolves once the task ended, also for each caller that
/// waits for a stop when another caller started it, or once that caller was cut.
pub(crate) type Ended = future::Shared<oneshot::Receiver<()>>;

/// The worker of a room: a task of its own, which holds the client by a `Weak`, so that it ends
/// once the application drops the client: at once, or at its next heartbeat if a task of a
/// worker, of any room, holds the client meanwhile (a heartbeat, an acknowledgement, a sync, a
/// transfer or the opening of its event stream). Dropping the worker stops its task too.
pub(crate) struct Worker {
    shared: Arc<Mutex<Shared>>,
    /// Wakes the task when its mode changes.
    changed: Arc<Notify>,
    /// Dropped to stop the task.
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
    ended: Ended,
}

impl Worker {
    /// Starts the worker of `room_id` of `client`, in `freshness`.
    pub(crate) fn start(client: &Arc<Client>, room_id: RoomId, freshness: Freshness) -> Self {
        let shared = Arc::new(Mutex::new(Shared {
            freshness,
            last_error: None,
            disabled: false,
            stream_open: false,
        }));
        let changed = Arc::new(Notify::new());
        let (stop, stopped) = oneshot::channel();
        // Dropped with the task, once it ended.
        let (ending, ended) = oneshot::channel::<()>();
        let config = client.config().clone();
        let run = run(
            Arc::downgrade(client),
            config,
            room_id,
            (Arc::clone(&shared), Arc::clone(&changed)),
            stopped,
        );
        let task = async move {
            let _ending = ending;
            run.await;
        };
        Self {
            shared,
            changed,
            stop,
            task: tokio::spawn(task),
            ended: ended.shared(),
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        !self.task.is_finished()
    }

    /// Sets its mode, which its task takes once the step in progress ends (at once, unless a
    /// heartbeat with its acknowledgement is in flight): in `Live` it opens the event stream, when
    /// a wait after an error or a refusal allows, and in another mode it closes it.
    pub(crate) fn set_freshness(&self, freshness: Freshness) {
        lock(&self.shared).freshness = freshness;
        self.changed.notify_one();
    }

    /// Its status, with `connected` for the room.
    pub(crate) fn status(&self, connected: Option<bool>) -> WorkerStatus {
        let shared = lock(&self.shared);
        let effective = match shared.freshness {
            Freshness::Live if shared.disabled => Freshness::Periodic,
            freshness => freshness,
        };
        WorkerStatus {
            freshness: shared.freshness,
            effective,
            stream_open: shared.stream_open,
            running: self.is_running(),
            connected,
            last_error: shared.last_error.clone(),
        }
    }

    /// Stops its task at its next `.await`: the task closes its event stream, cuts its sync, its
    /// transfer and the opening of its stream, waits until they ended, and sends its last
    /// acknowledgement. Returns what resolves once the task ended.
    pub(crate) fn stop(self) -> Ended {
        let Self { stop, ended, .. } = self;
        drop(stop);
        ended
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

/// Whether the worker runs in `Live`, with its event stream.
fn live(shared: &Mutex<Shared>) -> bool {
    lock(shared).freshness == Freshness::Live
}

/// The event stream of a worker in `Live`: open, being opened, or to be opened at `retry_at`.
struct Events {
    stream: Option<Stream>,
    opening: Option<JoinHandle<Result<Opened, ClientError>>>,
    /// When to open it, once it is neither open nor being opened.
    retry_at: Instant,
    /// The waits before it opens again after an error or its end, which start over with an
    /// event: a stream that ends at once, or is refused, is opened again after a wait that
    /// doubles.
    backoff: Backoff,
    /// Whether the server answered that its event stream is turned off: the worker runs as in
    /// `Periodic`, and opens it again once `events_retry` went by.
    disabled: bool,
}

/// An open event stream, of the replica of `epoch`: its events are of that replica.
struct Stream {
    reader: EventReader,
    epoch: u64,
}

impl Events {
    fn new(config: &ClientConfig) -> Self {
        Self {
            stream: None,
            opening: None,
            retry_at: Instant::now(),
            backoff: Backoff::worker(config),
            disabled: false,
        }
    }

    /// Closes the stream, or cuts its opening, outside `Live`.
    fn close(&mut self) {
        self.stream = None;
        if let Some(opening) = self.opening.take() {
            opening.abort();
        }
    }

    /// Takes the end of the stream, or an error reading or opening it: another one opens after
    /// a wait.
    fn lost(&mut self) {
        self.stream = None;
        self.retry_at = Instant::now() + self.backoff.next();
    }

    /// Takes the stream for one of another epoch: another one opens at once, so that no event of
    /// the replica of that epoch is taken for one of this.
    fn replace(&mut self) {
        self.stream = None;
        self.retry_at = Instant::now();
    }

    /// Whether the stream is due to open: in `Live`, neither open nor being opened.
    fn due(&self, live: bool) -> bool {
        live && self.stream.is_none() && self.opening.is_none()
    }
}

/// What the status of the worker shows of its event stream while its task runs: once the task
/// ends, also if it is cut, the stream is closed.
struct Showing<'a>(&'a Mutex<Shared>);

impl Showing<'_> {
    fn show(&self, events: &Events) {
        let mut shared = lock(self.0);
        shared.disabled = events.disabled;
        shared.stream_open = events.stream.is_some();
    }
}

impl Drop for Showing<'_> {
    fn drop(&mut self) {
        lock(self.0).stream_open = false;
    }
}

/// The next event of `stream`, once it is open.
async fn read(stream: &mut Option<Stream>) -> Result<Option<SseEvent>, StreamError> {
    match stream {
        Some(stream) => stream.reader.next().await,
        None => std::future::pending().await,
    }
}

/// Takes `event`, of the stream of the worker of `room_id`, as a notice: a sync for the
/// operations it brings or does not decode, for a head that moved or for events missed; a
/// registration, with the cursor, for a change of the schema, which the sync of the worker sends
/// first; and a heartbeat in the task of the transfers for a designation, or for a snapshot when
/// the replica waits for one.
fn notice(
    event: &SseEvent,
    client: &Held,
    room_id: &RoomId,
    (syncing, pending): (
        &mut Option<JoinHandle<Result<bool, ClientError>>>,
        &mut bool,
    ),
    transferring: &mut Option<JoinHandle<Result<(), ClientError>>>,
) {
    match event.decode() {
        Ok(RoomEvent::SchemaReloaded) => {
            client.unregister(room_id);
            sync(syncing, pending, client, room_id);
        }
        Ok(RoomEvent::SnapshotWanted) => transfer(transferring, client, room_id, Transfer::Beat),
        Ok(RoomEvent::SnapshotAvailable(_)) => {
            if client.awaits_snapshot(room_id) {
                transfer(transferring, client, room_id, Transfer::Beat);
            }
        }
        Ok(RoomEvent::Unknown) => {}
        Ok(RoomEvent::Ops(_) | RoomEvent::HeadAdvanced(_) | RoomEvent::Lagged) | Err(_) => {
            sync(syncing, pending, client, room_id);
        }
    }
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
/// step stops it; then it closes its event stream, cuts its sync, its transfer and the opening
/// of its stream, waits until they ended, and, unless the client was dropped, acknowledges the
/// head of the replica if it moved, once and within [`LAST_ACK`] (a replica that failed closed
/// or left the client sends nothing). It sends a heartbeat every interval, the first at a random
/// moment within the first, so that the clients that start together do not send theirs
/// together, and after each one that succeeds, the acknowledgement of the head of the replica if
/// it moved, and every [`ACK_AGAIN_BEATS`] also if it did not, unless the replica waits for a
/// snapshot. A heartbeat transfers nothing: the snapshot its ack asks to upload or announces goes
/// in a task of its own, one at a time, and the heartbeats go on meanwhile. Unless the mode is
/// `Manual`, a heartbeat whose ack reports a head past that of the replica leads to a sync, in a
/// task of its own, without an acknowledgement: one at a time, and one more once it ends if a
/// heartbeat or an event asked for one meanwhile and the mode still syncs. A batch of the sync
/// that asks the client for a snapshot is confirmed by a heartbeat in the task of the transfers.
/// After [`WAITING_BEATS`] heartbeats in a row that leave the replica waiting for a snapshot that
/// no reply announced, the next one registers it again. A heartbeat that fails is sent again
/// after a backoff with full jitter, and the first that succeeds goes back to the interval.
///
/// In `Live`, it opens the event stream of the room (in a task of its own), and syncs once it is
/// open, for what came before it subscribed; each event of it is a notice ([`notice`]), once its
/// epoch is that of the replica: a stream of another epoch, which an event or a heartbeat finds,
/// is replaced at once. Its end, an error reading or opening it, or [`SILENCE`] without a block
/// open another one after a backoff with full jitter, which an event starts over; the refusal
/// of a server with its event stream turned off, after `events_retry`. Its mode is read at each
/// step, and a change of it wakes the task, which opens the stream or closes it once the step in
/// progress ends; the waits before opening it again are kept across modes.
///
/// [`SILENCE`]: crate::events::SILENCE
async fn run(
    weak: Weak<Client>,
    config: ClientConfig,
    room_id: RoomId,
    (shared, changed): (Arc<Mutex<Shared>>, Arc<Notify>),
    mut stop: oneshot::Receiver<()>,
) {
    let mut backoff = Backoff::worker(&config);
    let mut next = Instant::now() + jitter(config.heartbeat_interval);
    let mut transferring: Option<JoinHandle<Result<(), ClientError>>> = None;
    let mut syncing: Option<JoinHandle<Result<bool, ClientError>>> = None;
    // Whether a heartbeat or an event asked for a sync while one ran.
    let mut pending = false;
    // How many heartbeats in a row left the replica waiting for a snapshot that no reply
    // announced.
    let mut waiting = 0;
    // How many heartbeats succeeded since the last one acknowledged the head anyway.
    let mut beats = 0;
    let mut events = Events::new(&config);
    let showing = Showing(&shared);
    loop {
        let live = live(&shared);
        if !live {
            events.close();
        }
        showing.show(&events);
        tokio::select! {
            _ = &mut stop => break,
            () = changed.notified() => continue,
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
            opened = ended_task(&mut events.opening) => {
                events.opening = None;
                let opened = match opened {
                    Ok(opened) => opened,
                    Err(err) => {
                        if ended(&shared, Err(err)) {
                            break;
                        }
                        events.lost();
                        continue;
                    }
                };
                ended(&shared, Ok(()));
                match opened {
                    Opened::Disabled => {
                        events.disabled = true;
                        events.retry_at = Instant::now() + config.events_retry;
                    }
                    Opened::Stream(reader, epoch) => {
                        events.disabled = false;
                        events.stream = Some(Stream { reader, epoch });
                        // What came before it subscribed.
                        let Some(client) = held(&weak) else {
                            break;
                        };
                        sync(&mut syncing, &mut pending, &client, &room_id);
                    }
                }
                continue;
            }
            read = read(&mut events.stream) => {
                let Some(client) = held(&weak) else {
                    break;
                };
                let event = match read {
                    Ok(Some(event)) => event,
                    Ok(None) | Err(_) => {
                        events.lost();
                        continue;
                    }
                };
                let epoch = events.stream.as_ref().map(|stream| stream.epoch);
                if !epoch.is_some_and(|epoch| client.is_of_epoch(&room_id, epoch)) {
                    events.replace();
                    continue;
                }
                events.backoff = Backoff::worker(&config);
                let syncing = (&mut syncing, &mut pending);
                notice(&event, &client, &room_id, syncing, &mut transferring);
                continue;
            }
            () = tokio::time::sleep_until(events.retry_at), if events.due(live) => {
                let Some(client) = held(&weak) else {
                    break;
                };
                let room_id = room_id.clone();
                let task = async move { client.worker_events(&room_id).await };
                events.opening = Some(tokio::spawn(task));
                continue;
            }
            () = tokio::time::sleep_until(next) => {}
        }
        let Some(client) = held(&weak) else {
            break;
        };
        let started = Instant::now();
        let register = waiting >= WAITING_BEATS;
        if register {
            waiting = 0;
        }
        let beat = tokio::select! {
            _ = &mut stop => break,
            beat = client.worker_beat(&room_id, register) => beat,
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
        waiting = if asked.waits { waiting + 1 } else { 0 };
        if events
            .stream
            .as_ref()
            .is_some_and(|stream| stream.epoch != asked.epoch)
        {
            events.replace();
            showing.show(&events);
        }
        beats = (beats + 1) % ACK_AGAIN_BEATS;
        let acking = match beats {
            0 => Acking::Again,
            _ => Acking::Moved,
        };
        let acked = tokio::select! {
            _ = &mut stop => break,
            acked = client.worker_ack(&room_id, acking) => acked,
        };
        if ended(&shared, acked) {
            break;
        }
        if let Some(wanted) = asked.transfer {
            transfer(&mut transferring, &client, &room_id, wanted);
        }
        if asked.behind && syncs(&shared) {
            sync(&mut syncing, &mut pending, &client, &room_id);
        }
    }
    let Events {
        stream, opening, ..
    } = events;
    drop(stream);
    drop(showing);
    let (syncing, transferring, opening) =
        (syncing.map(cut), transferring.map(cut), opening.map(cut));
    if let Some(syncing) = syncing {
        let _ = syncing.await;
    }
    if let Some(transferring) = transferring {
        let _ = transferring.await;
    }
    if let Some(opening) = opening {
        let _ = opening.await;
    }
    if let Some(client) = held(&weak) {
        let last = client.worker_ack(&room_id, Acking::Last);
        let _ = tokio::time::timeout(LAST_ACK, last).await;
    }
}

/// `task`, cut at its next `.await`.
fn cut<T>(task: JoinHandle<T>) -> JoinHandle<T> {
    task.abort();
    task
}
