use super::*;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;

/// Marks its flag once it is dropped.
struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn a_task_runs_until_its_end_after_its_handle_is_dropped() {
    let (go, wait) = oneshot::channel::<()>();
    let (done, ended) = oneshot::channel();
    let task = spawn(async move {
        let _ = wait.await;
        // Past an `.await` that comes after its handle was dropped.
        tokio::task::yield_now().await;
        let _ = done.send(7);
    });
    drop(task);
    go.send(()).unwrap();
    assert_eq!(ended.await, Ok(7));
}

#[tokio::test]
async fn cutting_a_task_stops_it_at_its_next_await_and_drops_what_it_holds() {
    let (dropped, after) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let (started, running) = oneshot::channel();
    let (go, wait) = oneshot::channel::<()>();
    let task = spawn({
        let (held, after) = (Dropped(Arc::clone(&dropped)), Arc::clone(&after));
        async move {
            let _held = held;
            let _ = started.send(());
            let _ = wait.await;
            after.store(true, Ordering::SeqCst);
        }
    });
    running.await.unwrap();
    assert!(!task.is_finished());
    task.abort();
    // What would wake it, once it was cut.
    let _ = go.send(());
    let cut = task.await.unwrap_err();
    assert!(cut.0.is_cancelled(), "{cut}");
    // What the worker shows of a task that failed: the text of the error of Tokio.
    assert_eq!(cut.to_string(), cut.0.to_string());
    assert!(dropped.load(Ordering::SeqCst));
    assert!(!after.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_task_that_ended_gives_what_it_gave_also_once_cut() {
    let task = spawn(async { 7 });
    for _ in 0..100 {
        if task.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(task.is_finished());
    task.abort();
    assert_eq!(task.await.unwrap(), 7);
}

#[tokio::test]
async fn a_task_that_panicked_gives_an_error() {
    let task = spawn(async { panic!("a task that panics") });
    let failed = task.await.unwrap_err();
    assert!(failed.0.is_panic(), "{failed}");
    assert_eq!(failed.to_string(), failed.0.to_string());
}

#[tokio::test]
async fn the_work_of_the_cpu_runs_out_of_the_thread_of_the_runtime() {
    let runtime = std::thread::current().id();
    let work = spawn_blocking(move || (std::thread::current().id(), 7));
    let (thread, worked) = work.await.unwrap();
    assert_ne!(thread, runtime);
    assert_eq!(worked, 7);
}

#[tokio::test(start_paused = true)]
async fn the_instant_is_that_of_the_clock_of_tokio() {
    let start = now();
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert_eq!(
        now().saturating_duration_since(start),
        Duration::from_secs(3600)
    );
}

#[tokio::test(start_paused = true)]
async fn the_timers_count_by_the_clock_of_tokio() {
    // Far past what went by on the clock of the system.
    tokio::time::advance(Duration::from_secs(3600)).await;
    let start = now();
    sleep(Duration::from_secs(30)).await;
    sleep_until(start + Duration::from_secs(60)).await;
    let ends = timeout(Duration::from_secs(5), sleep(Duration::from_secs(4)));
    assert_eq!(ends.await, Ok(()));
    let late = timeout(Duration::from_secs(5), sleep(Duration::from_secs(6)));
    assert_eq!(late.await, Err(Elapsed));
    let late = timeout_at(
        now() + Duration::from_secs(5),
        sleep(Duration::from_secs(6)),
    );
    assert_eq!(late.await, Err(Elapsed));
    let slept = now().saturating_duration_since(start);
    let expected = Duration::from_secs(60 + 4 + 5 + 5);
    assert!(
        slept >= expected && slept < expected + Duration::from_millis(10),
        "{slept:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_sleep_until_wakes_at_its_deadline_and_never_before() {
    let deadline = now() + Duration::from_secs(30);
    let mut asleep = std::pin::pin!(sleep_until(deadline));
    assert!(futures::poll!(&mut asleep).is_pending());
    tokio::time::advance(Duration::from_millis(29_999)).await;
    assert!(futures::poll!(&mut asleep).is_pending());
    asleep.await;
    assert!(now() >= deadline);
    // The clock went only up to the deadline.
    assert!(now() < deadline + Duration::from_millis(2));
}

#[tokio::test(start_paused = true)]
async fn a_sleep_counts_from_its_call() {
    let start = now();
    let asleep = sleep(Duration::from_secs(30));
    tokio::time::advance(Duration::from_secs(10)).await;
    asleep.await;
    let slept = now().saturating_duration_since(start);
    assert!(slept >= Duration::from_secs(30), "{slept:?}");
    assert!(slept < Duration::from_millis(30_002), "{slept:?}");
}

#[tokio::test(start_paused = true)]
async fn a_deadline_gives_the_future_that_ends_before_it_and_cuts_the_one_that_does_not() {
    let start = now();
    let ends = timeout_at(start + Duration::from_secs(5), async {
        sleep(Duration::from_secs(4)).await;
        7
    });
    assert_eq!(ends.await, Ok(7));
    assert_eq!(
        now().saturating_duration_since(start),
        Duration::from_secs(4)
    );

    let deadline = now() + Duration::from_secs(5);
    let dropped = Arc::new(AtomicBool::new(false));
    let held = Dropped(Arc::clone(&dropped));
    let mut late = std::pin::pin!(timeout_at(deadline, async move {
        let _held = held;
        sleep(Duration::from_secs(6)).await;
    }));
    assert!(futures::poll!(&mut late).is_pending());
    tokio::time::advance(Duration::from_millis(4_999)).await;
    assert!(futures::poll!(&mut late).is_pending());
    assert_eq!(late.await, Err(Elapsed));
    assert!(now() >= deadline);
    assert!(now() < deadline + Duration::from_millis(2));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn a_duration_whose_end_does_not_fit_in_the_clock_never_comes() {
    let mut asleep = std::pin::pin!(sleep(Duration::MAX));
    let mut late = std::pin::pin!(timeout(Duration::MAX, std::future::pending::<()>()));
    assert!(futures::poll!(&mut asleep).is_pending());
    assert!(futures::poll!(&mut late).is_pending());
    tokio::time::advance(Duration::from_secs(10 * 365 * 86_400)).await;
    assert!(futures::poll!(&mut asleep).is_pending());
    assert!(futures::poll!(&mut late).is_pending());
    assert_eq!(timeout(Duration::MAX, async { 7 }).await, Ok(7));
}

#[tokio::test(start_paused = true)]
async fn a_timeout_counts_from_its_call() {
    let start = now();
    let ends = timeout(Duration::from_secs(5), sleep(Duration::from_secs(4)));
    assert_eq!(ends.await, Ok(()));
    assert_eq!(
        now().saturating_duration_since(start),
        Duration::from_secs(4)
    );

    let late = timeout(Duration::from_secs(5), std::future::pending::<()>());
    tokio::time::advance(Duration::from_secs(5)).await;
    let polled = now();
    assert_eq!(late.await, Err(Elapsed));
    assert_eq!(now(), polled);
}

#[test]
fn a_random_uuid_is_of_version_4_and_at_random() {
    let ids: HashSet<u128> = (0..1000).map(|_| random_uuid()).collect();
    assert_eq!(ids.len(), 1000);
    for id in ids {
        let id = uuid::Uuid::from_u128(id);
        assert_eq!(id.get_version(), Some(uuid::Version::Random), "{id}");
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122, "{id}");
    }
}

#[test]
fn the_wall_clock_is_the_time_of_the_system_in_milliseconds() {
    let millis = || {
        let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        u64::try_from(since.as_millis()).unwrap()
    };
    let before = millis();
    let wall = wall_millis();
    let after = millis();
    assert!(before <= wall && wall <= after, "{before} {wall} {after}");
}
