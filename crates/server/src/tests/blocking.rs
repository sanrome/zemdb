use super::*;
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn runs_inline_without_a_runtime() {
    assert_eq!(blocking_io(|| 7), 7);
}

#[test]
fn runs_inline_on_a_current_thread_runtime() {
    // `block_in_place` panics on a current-thread runtime; the helper must not use it there.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let value = runtime.block_on(async { tokio::spawn(async { blocking_io(|| 7) }).await });
    assert_eq!(value.unwrap(), 7);
}

#[test]
fn other_tasks_keep_running_while_a_worker_blocks() {
    // With a single worker, a task blocking it directly would starve every other task.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (unblock_tx, unblock_rx) = mpsc::channel::<()>();
        let blocked = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            blocking_io(|| unblock_rx.recv_timeout(Duration::from_secs(2)))
        });
        started_rx.await.unwrap();

        // Only runs if the worker is free while the other task blocks.
        tokio::spawn(async move { unblock_tx.send(()).unwrap() });

        assert!(
            blocked.await.unwrap().is_ok(),
            "the blocked task starved the runtime's only worker"
        );
    });
}
