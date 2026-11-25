use super::*;
use crate::dispatch::{
    Command, DispatchConfig, Dispatcher, Message, PageTokens, Platform, Request,
};
use crate::{ClientError, ZemdbClient};
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::Duration;

/// A platform that opens no client: a subscription over a stream of its own does not need one.
struct Closed;

#[async_trait]
impl Platform for Closed {
    async fn open(&self, _: DispatchConfig, _: PageTokens) -> Result<ZemdbClient, ClientError> {
        Err(ClientError::Config("No client".to_string()))
    }
}

/// A dispatcher whose notices go to the vector it gives.
fn noticing() -> (Dispatcher, Arc<Mutex<Vec<Notice>>>) {
    let notices = Arc::new(Mutex::new(Vec::new()));
    let published = Arc::clone(&notices);
    let dispatcher = Dispatcher::new(Closed, move |message| {
        if let Message::Notice(notice) = message {
            published.lock().unwrap().push(notice);
        }
    });
    (dispatcher, notices)
}

/// Enters a subscription of `tab` to `values` in `shared`, as a watch enters it, and gives its id
/// and what starts its task.
fn subscribed(shared: &Arc<Shared>, tab: u64, values: Vec<Outcome>) -> (u64, oneshot::Sender<()>) {
    let subscription = shared.state().next_id();
    let values = futures::stream::iter(values).boxed();
    let (watcher, start) = entered(shared, tab, subscription, values);
    shared
        .state()
        .subscriptions
        .insert(subscription, (tab, watcher));
    (subscription, start)
}

/// Waits until `check` holds, within a few seconds.
async fn until(check: impl Fn() -> bool) {
    let wait = async {
        while !check() {
            tokio::task::yield_now().await;
        }
    };
    let waited = tokio::time::timeout(Duration::from_secs(5), wait).await;
    waited.expect("It did not come");
}

/// Lets the tasks of the runtime run for a while.
async fn idle() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_subscription_publishes_one_value_at_a_time_and_leaves_once_its_stream_ended() {
    let (dispatcher, notices) = noticing();
    let shared = &dispatcher.shared;
    let values = vec![Ok(Answer::Done), Err(ClientError::RowMissing)];
    let (subscription, start) = subscribed(shared, 1, values);
    // Nothing comes before it starts.
    idle().await;
    assert!(notices.lock().unwrap().is_empty());
    start.send(()).unwrap();
    let value = |value| Notice::Value {
        tab: 1,
        subscription,
        value,
    };
    let confirm = || Command {
        tab: 1,
        id: 0,
        request: Request::Confirm(subscription),
    };
    // One value at a time, each once the one before is confirmed, also an error.
    until(|| notices.lock().unwrap().len() == 1).await;
    idle().await;
    assert_eq!(*notices.lock().unwrap(), [value(Ok(Answer::Done))]);
    dispatcher.dispatch(confirm());
    until(|| notices.lock().unwrap().len() == 2).await;
    idle().await;
    assert_eq!(notices.lock().unwrap().len(), 2);
    assert_eq!(
        notices.lock().unwrap()[1],
        value(Err(ClientError::RowMissing))
    );
    assert_eq!(shared.state().subscriptions.len(), 1);
    // Once its stream ended, it tells it, and leaves the dispatcher.
    dispatcher.dispatch(confirm());
    until(|| shared.state().subscriptions.is_empty()).await;
    let ended = Notice::Ended {
        tab: 1,
        subscription,
    };
    assert_eq!(notices.lock().unwrap().last(), Some(&ended));
    assert_eq!(notices.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn a_subscription_cut_stays_until_its_task_ended_and_its_end_is_waited_for_more_than_once() {
    let (dispatcher, notices) = noticing();
    let shared = &dispatcher.shared;
    // One whose watch did not end its dispatch (its reply panicked) leaves without a notice.
    let (_, start) = subscribed(shared, 1, vec![Ok(Answer::Done)]);
    drop(start);
    until(|| shared.state().subscriptions.is_empty()).await;
    assert!(notices.lock().unwrap().is_empty());
    // One cut with its value in flight stays, cut, until its task ended: a confirmation does not
    // reach it, and its end is waited for by each of those that cut it.
    let values = vec![Ok(Answer::Done), Ok(Answer::Done)];
    let (subscription, start) = subscribed(shared, 1, values);
    start.send(()).unwrap();
    until(|| notices.lock().unwrap().len() == 1).await;
    let end = || {
        shared
            .state()
            .subscriptions
            .get_mut(&subscription)
            .unwrap()
            .1
            .end()
    };
    let (first, second) = (end(), end());
    let confirm = Request::Confirm(subscription);
    dispatcher.dispatch(Command {
        tab: 1,
        id: 0,
        request: confirm,
    });
    let wait = futures::future::join(first, second);
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap();
    assert!(shared.state().subscriptions.is_empty());
    idle().await;
    assert_eq!(notices.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_subscription_whose_publish_panics_leaves_the_dispatcher() {
    let dispatcher = Dispatcher::new(Closed, |message| {
        if let Message::Notice(Notice::Value { .. }) = message {
            panic!("The page panics");
        }
    });
    let shared = &dispatcher.shared;
    let (_, start) = subscribed(shared, 1, vec![Ok(Answer::Done)]);
    start.send(()).unwrap();
    until(|| shared.state().subscriptions.is_empty()).await;
}

/// A stream that never gives a value, and tells once it is dropped.
struct Told(Arc<AtomicBool>);

impl futures::Stream for Told {
    type Item = Outcome;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Outcome>> {
        Poll::Pending
    }
}

impl Drop for Told {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_end_of_a_subscription_comes_once_it_left_the_dispatcher_and_its_stream_was_dropped() {
    let (dispatcher, _) = noticing();
    let shared = &dispatcher.shared;
    // Its end is waited for from the thread of the test, which is not a worker of the runtime.
    for _ in 0..200 {
        let dropped = Arc::new(AtomicBool::new(false));
        let subscription = shared.state().next_id();
        let values = Told(Arc::clone(&dropped)).boxed();
        let (watcher, start) = entered(shared, 1, subscription, values);
        shared
            .state()
            .subscriptions
            .insert(subscription, (1, watcher));
        start.send(()).unwrap();
        let ended = shared
            .state()
            .subscriptions
            .get_mut(&subscription)
            .unwrap()
            .1
            .end();
        tokio::time::timeout(Duration::from_secs(5), ended)
            .await
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert!(!shared.state().subscriptions.contains_key(&subscription));
    }
}
