//! The subscriptions of `watch` to a room: what each one looks at, and its mark, which the
//! applier sets after each change of the replica and its stream takes before it reads the value
//! again. Nothing here waits or does I/O: the registry lives under the synchronous lock of the
//! room, and holds per subscription what it looks at, its mark and the waker of its stream,
//! nothing per change.

use std::collections::{HashMap, HashSet};
use std::task::{Context, Poll, Waker};
use zemdb_core::{PrimaryKey, SequenceNumber};
use zemdb_storage::RejectReason;

/// What a subscription looks at, as its stream resolved it against the schema of the replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Watched {
    /// Nothing that a batch changes: a subscription whose stream did not resolve what it reads
    /// yet (the replica was empty), or whose read the schema does not take. Only a change of
    /// every row marks it, after which the schema may be another.
    Unresolved,
    /// The row of a key of a table.
    Row(u16, PrimaryKey),
    /// The rows of a table.
    Table(u16),
}

/// A change of the replica, as the applier notifies it once it is visible.
#[derive(Debug)]
pub(crate) enum Change {
    /// Any row may have changed: a snapshot was installed, the storage room was opened again
    /// after an error, the schema was reloaded, or the replica was opened or discarded.
    All,
    /// The rows of these keys, by table: those of the operations of a batch that the replica
    /// applied, without those it rejected.
    Rows(HashMap<u16, HashSet<PrimaryKey>>),
}

impl Change {
    /// The rows that a batch changed: the keys of its operations `ops`, by their sequences,
    /// without those of `rejected`, in sequence order as the report of the batch gives them.
    pub(crate) fn applied(
        ops: Vec<(SequenceNumber, u16, PrimaryKey)>,
        rejected: &[(SequenceNumber, RejectReason)],
    ) -> Self {
        let mut rows: HashMap<u16, HashSet<PrimaryKey>> = HashMap::new();
        for (seq, table_id, pk) in ops {
            if rejected
                .binary_search_by_key(&seq, |(seq, _)| *seq)
                .is_err()
            {
                rows.entry(table_id).or_default().insert(pk);
            }
        }
        Self::Rows(rows)
    }

    /// Whether the change touches what `watched` looks at.
    fn touches(&self, watched: &Watched) -> bool {
        match (self, watched) {
            (Self::All, _) => true,
            (Self::Rows(_), Watched::Unresolved) => false,
            (Self::Rows(rows), Watched::Row(table_id, pk)) => {
                rows.get(table_id).is_some_and(|keys| keys.contains(pk))
            }
            (Self::Rows(rows), Watched::Table(table_id)) => rows.contains_key(table_id),
        }
    }
}

/// The subscriptions of a room, by their ids, which are never used again.
#[derive(Debug, Default)]
pub(crate) struct Watches {
    next: u64,
    entries: HashMap<u64, Watch>,
}

/// A subscription.
#[derive(Debug)]
struct Watch {
    watched: Watched,
    /// Whether its value may have changed since its stream last took the mark.
    marked: bool,
    /// The waker of its stream, while it waits for the mark.
    waker: Option<Waker>,
}

impl Watches {
    /// How many subscriptions there are.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are none.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Enters a new subscription, which looks at nothing resolved yet and is marked, so that its
    /// stream reads its first value: entered before that read, a change in the middle marks it
    /// again.
    pub(crate) fn add(&mut self) -> u64 {
        let id = self.next;
        self.next += 1;
        let watch = Watch {
            watched: Watched::Unresolved,
            marked: true,
            waker: None,
        };
        self.entries.insert(id, watch);
        id
    }

    /// Takes the subscription `id` out, if it is there.
    pub(crate) fn remove(&mut self, id: u64) {
        self.entries.remove(&id);
    }

    /// Sets what the subscription `id` looks at, if it is there.
    pub(crate) fn watch(&mut self, id: u64, watched: Watched) {
        if let Some(watch) = self.entries.get_mut(&id) {
            watch.watched = watched;
        }
    }

    /// Marks the subscriptions that `change` touches, and returns the wakers of the streams that
    /// wait for their mark, to be woken without the lock.
    pub(crate) fn mark(&mut self, change: &Change) -> Vec<Waker> {
        let touched = self.entries.values_mut();
        let touched = touched.filter(|watch| change.touches(&watch.watched));
        touched
            .filter_map(|watch| {
                watch.marked = true;
                watch.waker.take()
            })
            .collect()
    }

    /// Ends every subscription, once the room is out of the client: takes them out, and returns
    /// the wakers of their streams, which find them gone.
    pub(crate) fn end(&mut self) -> Vec<Waker> {
        let ended = self.entries.drain();
        ended.filter_map(|(_, watch)| watch.waker).collect()
    }

    /// Takes the mark of the subscription `id`: `true` if it was marked, and `false` if it ended.
    /// Otherwise the stream waits, with the waker of `cx`, until a change marks it or the room
    /// ends it.
    pub(crate) fn poll(&mut self, id: u64, cx: &mut Context<'_>) -> Poll<bool> {
        let Some(watch) = self.entries.get_mut(&id) else {
            return Poll::Ready(false);
        };
        if std::mem::take(&mut watch.marked) {
            return Poll::Ready(true);
        }
        match &mut watch.waker {
            Some(waker) if waker.will_wake(cx.waker()) => {}
            waker => *waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }
}

#[cfg(test)]
#[path = "tests/watch.rs"]
mod tests;
