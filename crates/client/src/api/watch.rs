//! `watch`: the stream of the values of a read of the replica, which its subscription
//! reads again once the applier marks it after a change, and emits only when they change.

use futures::stream::{BoxStream, Stream, StreamExt};
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{Key, Row, Scan, TableHandle};
use crate::client::Subscription;
use crate::ClientError;

/// The values of a [`watch`](TableHandle::watch) or a [`watch_scan`](TableHandle::watch_scan):
/// a [`Stream`] whose first element is the current value of its read, and then one each time the
/// value changes in the replica. It holds its last value and a mark, nothing per change: a
/// consumer that reads late gets the current value, skipping those in between.
///
/// A read that fails (`Decryption`, `Storage`) is an element, and the stream goes on: the next
/// value is compared with the last one emitted, and the next change reads it again. The stream
/// emits nothing while the replica is empty (after a discard, until the room registers) or a
/// change takes it out of the client in its middle (the reopening of its storage room after an
/// error), and ends once the room is out of the client at the end of a change (it is closed, the
/// client shuts down, or an error took it out). Dropping the stream drops its subscription.
pub struct ChangeStream<T> {
    values: BoxStream<'static, Result<T, ClientError>>,
}

impl<T> Stream for ChangeStream<T> {
    type Item = Result<T, ClientError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.values.poll_next_unpin(cx)
    }
}

impl<T> fmt::Debug for ChangeStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChangeStream").finish_non_exhaustive()
    }
}

/// What a watch reads of its table.
#[derive(Debug)]
pub(super) enum Watch {
    /// The row of a key, as `get` reads it.
    Row(Key),
    /// The rows of a scan, as `scan` reads them.
    Rows(Scan),
}

impl<T: Clone + PartialEq + Send + 'static> ChangeStream<T> {
    /// The stream of `watch` of `table`, whose value is `value` of the rows it reads. Its
    /// subscription is entered now, before its first read.
    pub(super) fn new(table: TableHandle, watch: Watch, value: fn(Vec<Row>) -> T) -> Self {
        let room = table.room();
        let subscription = room.client().core().subscribe(room.room_id());
        let watcher = Watcher {
            table,
            watch,
            subscription,
            value,
            last: None,
        };
        let values = futures::stream::unfold(watcher, |mut watcher| async move {
            let next = watcher.next().await?;
            Some((next, watcher))
        });
        Self {
            values: values.fuse().boxed(),
        }
    }
}

/// The state of a [`ChangeStream`].
struct Watcher<T> {
    table: TableHandle,
    watch: Watch,
    subscription: Subscription,
    value: fn(Vec<Row>) -> T,
    /// The last value it emitted, if any.
    last: Option<T>,
}

impl<T: Clone + PartialEq> Watcher<T> {
    /// The next element: once a change marks the subscription, the value read again, if it is
    /// not the last one emitted, or the error of the read; `None` once the room ends the
    /// subscription.
    async fn next(&mut self) -> Option<Result<T, ClientError>> {
        loop {
            if !self.subscription.changed().await {
                return None;
            }
            let value = match self.table.watched(&self.watch, &self.subscription).await {
                // The replica is empty, or out of the client in the middle of a change: the change
                // that opens it marks the subscription again, or the one that leaves it out ends
                // it.
                Err(ClientError::RoomClosed(_)) => continue,
                Err(err) => return Some(Err(err)),
                Ok(rows) => (self.value)(rows),
            };
            if self.last.as_ref() != Some(&value) {
                self.last = Some(value.clone());
                return Some(Ok(value));
            }
        }
    }
}
