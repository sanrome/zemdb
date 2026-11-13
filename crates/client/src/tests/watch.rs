use super::*;
use futures::task::{waker, ArcWake};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use zemdb_core::Value;

/// A waker that counts how many times it was woken.
#[derive(Default)]
struct Counted(AtomicUsize);

impl ArcWake for Counted {
    fn wake_by_ref(counted: &Arc<Self>) {
        counted.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counted() -> (Arc<Counted>, Waker) {
    let counted = Arc::new(Counted::default());
    (Arc::clone(&counted), waker(counted))
}

fn pk(id: i64) -> PrimaryKey {
    PrimaryKey::single(id)
}

/// The change of a batch of `rows`, as table and id, all applied.
fn rows(rows: &[(u16, i64)]) -> Change {
    let ops = (rows.iter().zip(1..))
        .map(|((table, id), seq)| (SequenceNumber::new(seq), *table, pk(*id)));
    Change::applied(ops.collect(), &[])
}

/// Polls the subscription `id` of `watches` with `waker`.
fn poll(watches: &mut Watches, id: u64, waker: &Waker) -> Poll<bool> {
    watches.poll(id, &mut Context::from_waker(waker))
}

#[test]
fn a_new_subscription_is_marked_once_for_its_first_read() {
    let mut watches = Watches::default();
    let (_, waker) = counted();
    let id = watches.add();
    assert_eq!(watches.len(), 1);
    assert_eq!(poll(&mut watches, id, &waker), Poll::Ready(true));
    assert_eq!(poll(&mut watches, id, &waker), Poll::Pending);
    // Ids are never used again.
    let other = watches.add();
    assert_ne!(other, id);
    watches.remove(id);
    assert_eq!(watches.len(), 1);
    assert_eq!(poll(&mut watches, id, &waker), Poll::Ready(false));
}

#[test]
fn a_batch_marks_the_subscriptions_of_its_keys_and_tables_and_wakes_those_that_wait() {
    let mut watches = Watches::default();
    let watched = [
        Watched::Row(0, pk(1)),
        Watched::Row(0, pk(2)),
        Watched::Row(1, pk(1)),
        Watched::Table(0),
        Watched::Table(1),
        Watched::Unresolved,
    ];
    let wakers: Vec<_> = watched.iter().map(|_| counted()).collect();
    let ids: Vec<_> = (watched.iter().zip(&wakers))
        .map(|(watched, (_, waker))| {
            let id = watches.add();
            watches.watch(id, watched.clone());
            assert_eq!(poll(&mut watches, id, waker), Poll::Ready(true));
            assert_eq!(poll(&mut watches, id, waker), Poll::Pending);
            id
        })
        .collect();
    let marked = |watches: &mut Watches| -> Vec<bool> {
        let polled = (ids.iter().zip(&wakers)).map(|(id, (_, waker))| poll(watches, *id, waker));
        polled.map(|poll| poll == Poll::Ready(true)).collect()
    };

    // The row 1 of the table 0: its key and its table.
    let woken = watches.mark(&rows(&[(0, 1)]));
    assert_eq!(woken.len(), 2);
    woken.into_iter().for_each(Waker::wake);
    let counts = wakers
        .iter()
        .map(|(counted, _)| counted.0.load(Ordering::SeqCst));
    assert_eq!(counts.collect::<Vec<_>>(), [1, 0, 0, 1, 0, 0]);
    assert_eq!(
        marked(&mut watches),
        [true, false, false, true, false, false]
    );
    // A key of another table, and two of the same.
    watches.mark(&rows(&[(1, 1), (0, 2), (0, 2)]));
    assert_eq!(marked(&mut watches), [false, true, true, true, true, false]);
    // Every row: also what is not resolved.
    watches.mark(&Change::All);
    assert_eq!(marked(&mut watches), [true; 6]);
    // An empty batch, and one of a table nobody looks at.
    watches.mark(&rows(&[]));
    watches.mark(&rows(&[(7, 1)]));
    assert_eq!(marked(&mut watches), [false; 6]);
}

#[test]
fn a_mark_is_kept_until_its_stream_takes_it_and_wakes_only_a_stream_that_waits() {
    let mut watches = Watches::default();
    let (counted, waker) = counted();
    let id = watches.add();
    watches.watch(id, Watched::Table(0));
    assert_eq!(poll(&mut watches, id, &waker), Poll::Ready(true));
    // Marked before the stream waits: nothing to wake, and the next poll takes it.
    assert!(watches.mark(&rows(&[(0, 1)])).is_empty());
    assert!(watches.mark(&rows(&[(0, 2)])).is_empty());
    assert_eq!(poll(&mut watches, id, &waker), Poll::Ready(true));
    assert_eq!(poll(&mut watches, id, &waker), Poll::Pending);
    // Once it waits, a mark wakes it once, and the waker is taken.
    let woken = watches.mark(&rows(&[(0, 3)]));
    assert_eq!(woken.len(), 1);
    assert!(watches.mark(&rows(&[(0, 4)])).is_empty());
    woken.into_iter().for_each(Waker::wake);
    assert_eq!(counted.0.load(Ordering::SeqCst), 1);
    assert_eq!(poll(&mut watches, id, &waker), Poll::Ready(true));
    // A later poll keeps the waker of the latest.
    let (later, later_waker) = self::counted();
    assert_eq!(poll(&mut watches, id, &waker), Poll::Pending);
    assert_eq!(poll(&mut watches, id, &later_waker), Poll::Pending);
    watches.mark(&Change::All).into_iter().for_each(Waker::wake);
    assert_eq!(counted.0.load(Ordering::SeqCst), 1);
    assert_eq!(later.0.load(Ordering::SeqCst), 1);
}

#[test]
fn ending_takes_every_subscription_out_and_wakes_their_streams() {
    let mut watches = Watches::default();
    let (waiting, waiting_waker) = counted();
    let (_, waker) = counted();
    let first = watches.add();
    let second = watches.add();
    assert_eq!(poll(&mut watches, first, &waiting_waker), Poll::Ready(true));
    assert_eq!(poll(&mut watches, first, &waiting_waker), Poll::Pending);
    let woken = watches.end();
    assert_eq!(woken.len(), 1);
    woken.into_iter().for_each(Waker::wake);
    assert_eq!(waiting.0.load(Ordering::SeqCst), 1);
    assert!(watches.is_empty());
    // Marked or not, they find themselves ended.
    assert_eq!(poll(&mut watches, first, &waker), Poll::Ready(false));
    assert_eq!(poll(&mut watches, second, &waker), Poll::Ready(false));
    assert!(watches.mark(&Change::All).is_empty());
    // What an ended subscription looks at changes nothing.
    watches.watch(first, Watched::Table(0));
    assert!(watches.is_empty());
}

#[test]
fn the_rows_of_a_batch_are_those_of_its_operations_that_the_replica_applied() {
    let seq = SequenceNumber::new;
    let ops = vec![
        (seq(4), 0, pk(1)),
        (seq(5), 0, pk(2)),
        (seq(6), 1, pk(1)),
        (seq(7), 1, pk(3)),
        (seq(8), 2, pk(1)),
    ];
    let rejected = [
        (seq(5), RejectReason::Duplicate),
        (seq(8), RejectReason::NotOwner),
    ];
    let Change::Rows(rows) = Change::applied(ops, &rejected) else {
        panic!("A batch changes rows");
    };
    let expected: HashMap<u16, HashSet<PrimaryKey>> = [
        (0, HashSet::from([pk(1)])),
        (1, HashSet::from([pk(1), pk(3)])),
    ]
    .into_iter()
    .collect();
    assert_eq!(rows, expected);
    // A key compares as `Value` does: `-0.0` is `0.0`.
    let float = |float: f64| PrimaryKey::single(Value::Float(float));
    let change = Change::applied(vec![(seq(1), 0, float(-0.0))], &[]);
    assert!(change.touches(&Watched::Row(0, float(0.0))));
    assert!(!change.touches(&Watched::Row(0, float(1.0))));
}
