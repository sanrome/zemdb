use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test]
async fn runs_the_work_on_another_thread_and_returns_its_result() {
    let pool = IoPool::new(2);
    let caller = std::thread::current().id();
    let (out, worker) = pool.run(move || (7, std::thread::current().id())).await;
    assert_eq!(out, 7);
    assert_ne!(worker, caller, "the work ran on the caller's thread");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn never_runs_more_operations_at_once_than_its_threads() {
    let pool = Arc::new(IoPool::new(3));
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let operations: Vec<_> = (0..40)
        .map(|_| {
            let (pool, running, peak) =
                (Arc::clone(&pool), Arc::clone(&running), Arc::clone(&peak));
            tokio::spawn(async move {
                pool.run(move || {
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(5));
                    running.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            })
        })
        .collect();
    for operation in operations {
        operation.await.unwrap();
    }
    assert_eq!(peak.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn a_panic_in_the_work_reaches_the_caller_and_the_pool_keeps_working() {
    let pool = Arc::new(IoPool::new(1));
    let panicked = tokio::spawn({
        let pool = Arc::clone(&pool);
        async move {
            pool.run(|| panic!("injected panic in a file operation"))
                .await
        }
    })
    .await;
    assert!(panicked.unwrap_err().is_panic());
    assert_eq!(pool.run(|| 1).await, 1);
}

/// Sets its flag when dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn work_whose_caller_is_dropped_runs_to_its_end_and_drops_what_it_owns_afterwards() {
    let pool = IoPool::new(1);
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (ended, dropped) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let owned = DropFlag(Arc::clone(&dropped));
    let work = {
        let ended = Arc::clone(&ended);
        move || {
            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            ended.store(true, Ordering::SeqCst);
            owned
        }
    };
    let abandoned = tokio::time::timeout(Duration::from_millis(50), pool.run(work)).await;
    assert!(abandoned.is_err());
    assert!(
        !dropped.load(Ordering::SeqCst),
        "what the work owns was dropped while the work runs"
    );

    release_tx.send(()).unwrap();
    for _ in 0..500 {
        if dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(dropped.load(Ordering::SeqCst));
    assert!(ended.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_compression_never_keeps_the_other_file_operations_waiting() {
    let threads = IoThreads::new(1);
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let compressing = tokio::spawn({
        let threads = Arc::clone(&threads);
        async move {
            threads
                .compression()
                .run(move || release_rx.recv_timeout(Duration::from_secs(10)))
                .await
        }
    });
    let other = tokio::time::timeout(Duration::from_secs(2), threads.io().run(|| 1)).await;
    assert_eq!(other.ok(), Some(1));
    release_tx.send(()).unwrap();
    assert!(compressing.await.unwrap().is_ok());
}
