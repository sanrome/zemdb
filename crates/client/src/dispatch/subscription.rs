//! The subscriptions of `watch` of a dispatcher: a task per subscription reads its stream and
//! publishes each value as a notice to its tab, and does not read the next one until the tab
//! confirms it, so that a subscription has at most one value in flight, and its stream keeps the
//! last one meanwhile.

use futures::future::{FutureExt, Shared as Ended};
use futures::stream::{BoxStream, StreamExt};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::oneshot;

use super::{Answer, Notice, Outcome, Shared, Table, Watch};
use crate::rt;

/// A subscription in the dispatcher, until its task ended, also once an `Unwatch` or the notice
/// that its tab left cut it.
pub(super) struct Watcher {
    /// Confirms the value in flight, if any, until the task is cut.
    pub(super) confirm: Option<oneshot::Sender<()>>,
    /// Whether its task was cut.
    cut: bool,
    task: rt::Task<()>,
    /// What ends once its task ended, as many times as it is waited for.
    ended: Ended<oneshot::Receiver<()>>,
}

impl Watcher {
    /// Cuts the task of the subscription now, if it was not cut, and gives what ends once it
    /// ended: no notice of the subscription comes afterwards.
    pub(super) fn end(&mut self) -> impl Future<Output = ()> + Send + 'static {
        (self.cut, self.confirm) = (true, None);
        self.task.abort();
        let ended = self.ended.clone();
        async move {
            // It ends without a value, once the task dropped what it ran with.
            let _ = ended.await;
        }
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// What the task of a subscription runs with: once dropped, however the task ends (with its
/// stream, cut, or with a panic), the subscription leaves the dispatcher, then its stream is
/// dropped, and then its end is told to whoever waits for it.
pub(super) struct Running {
    shared: Arc<Shared>,
    subscription: u64,
    values: BoxStream<'static, Outcome>,
    _ended: oneshot::Sender<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.shared.state().subscriptions.remove(&self.subscription);
    }
}

/// Subscribes `tab` to `watch` of `table`, with the command `id`, whose reply, with the id of the
/// subscription, comes before its first value. The stream subscribes now, before its first read.
pub(super) fn start(shared: &Arc<Shared>, tab: u64, id: u64, table: Table, watch: Watch) {
    let values = shared.client(&table.room_id).and_then(|client| {
        let table = client.handle(table.room_id).table(&table.name)?;
        Ok(match watch {
            Watch::Row(key) => table.watch(&key).map(|row| row.map(Answer::Row)).boxed(),
            Watch::Rows(scan) => (table.watch_scan(&scan))
                .map(|rows| rows.map(Answer::Rows))
                .boxed(),
        })
    });
    let values = match values {
        Ok(values) => values,
        Err(err) => return shared.reply(tab, id, Err(err)),
    };
    let subscription = shared.state().next_id();
    let (watcher, start) = entered(shared, tab, subscription, values);
    shared
        .state()
        .subscriptions
        .insert(subscription, (tab, watcher));
    shared.reply(tab, id, Ok(Answer::Subscription(subscription)));
    // A subscription ended meanwhile does not take it.
    let _ = start.send(());
}

/// The subscription `subscription` of `tab` to `values`, with its task, which starts once what it
/// gives besides is sent.
pub(super) fn entered(
    shared: &Arc<Shared>,
    tab: u64,
    subscription: u64,
    values: BoxStream<'static, Outcome>,
) -> (Watcher, oneshot::Sender<()>) {
    let ((start, started), (ended, end)) = (oneshot::channel(), oneshot::channel());
    let running = Running {
        shared: Arc::clone(shared),
        subscription,
        values,
        _ended: ended,
    };
    let watcher = Watcher {
        confirm: None,
        cut: false,
        task: rt::spawn(run(running, tab, started)),
        ended: end.shared(),
    };
    (watcher, start)
}

/// Publishes to `tab`, once `started`, each value of its subscription, once the tab confirmed the
/// one before, and then the end of its stream.
async fn run(mut running: Running, tab: u64, started: oneshot::Receiver<()>) {
    let subscription = running.subscription;
    if started.await.is_err() {
        return;
    }
    while let Some(value) = running.values.next().await {
        let (confirm, confirmed) = oneshot::channel();
        match running.shared.state().subscriptions.get_mut(&subscription) {
            Some((_, watcher)) if !watcher.cut => watcher.confirm = Some(confirm),
            _ => return,
        }
        running.shared.notice(Notice::Value {
            tab,
            subscription,
            value,
        });
        // The stream keeps the last value meanwhile.
        if confirmed.await.is_err() {
            return;
        }
    }
    running.shared.notice(Notice::Ended { tab, subscription });
}

#[cfg(test)]
#[path = "tests/subscription.rs"]
mod tests;
