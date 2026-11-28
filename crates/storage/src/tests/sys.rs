//! The unit tests of `sys`, natively on Tokio and in wasm32 in a dedicated Worker of a browser,
//! with `wasm-bindgen-test`: its task, which runs until its end, and its randomness.

use super::*;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::oneshot;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A task goes on once it is dropped, past an `.await` after the drop, and gives nothing to
/// anybody: what it holds is released when it ends.
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn a_task_runs_until_its_end_after_it_is_dropped() {
    let (release, released) = oneshot::channel::<()>();
    let (ended, end) = oneshot::channel();
    let held = Arc::new(());
    let in_task = Arc::clone(&held);
    drop(spawn(async move {
        released.await.unwrap();
        ended.send(Arc::strong_count(&in_task)).unwrap();
    }));
    release.send(()).unwrap();
    assert_eq!(end.await.unwrap(), 2, "the task held what it was given");
    // Released once the task ended, which it did right after it sent.
    for _ in 0..1000 {
        if Arc::strong_count(&held) == 1 {
            return;
        }
        yield_once().await;
    }
    panic!("the task kept what it held after its end");
}

/// A task gives what it returned, also when it ended before anybody awaited it.
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn a_task_gives_what_it_returned() {
    assert_eq!(spawn(async { 7 }).await.unwrap(), 7);
    let task = spawn(async { "ended" });
    for _ in 0..10 {
        yield_once().await;
    }
    assert_eq!(task.await.unwrap(), "ended");
}

/// A task that panics gives an error that says so, never a panic of its caller.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_task_that_panics_gives_an_error_that_says_so() {
    let err = spawn(async { panic!("a panic on purpose") })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("panicked"), "{err}");
}

/// A task that its runtime drops before its end gives an error, never a panic.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn a_task_dropped_by_its_runtime_gives_an_error() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let task = {
        let _entered = runtime.enter();
        spawn(std::future::pending::<()>())
    };
    drop(runtime);
    let err = futures::executor::block_on(task).unwrap_err();
    assert!(err.to_string().contains("cancelled"), "{err}");
}

/// Names at random are all different, of 128 bits in lowercase hexadecimal digits (natively a
/// UUID of version 4, with its hyphens), and with nothing that the file layers take for a mark.
#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn names_at_random_are_different_and_hexadecimal() {
    let names: Vec<String> = (0..1000).map(|_| random_name().unwrap()).collect();
    assert_eq!(names.iter().collect::<HashSet<_>>().len(), names.len());
    let bits: Vec<u128> = names
        .iter()
        .map(|name| {
            let digits: String = name.chars().filter(|c| *c != '-').collect();
            assert_eq!(digits.len(), 32, "{name}");
            assert!(
                digits
                    .chars()
                    .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
                "{name}"
            );
            u128::from_str_radix(&digits, 16).unwrap()
        })
        .collect();
    #[cfg(not(target_arch = "wasm32"))]
    for name in &names {
        assert_eq!(uuid::Uuid::parse_str(name).unwrap().get_version_num(), 4);
    }
    // In WebAssembly every bit varies: none is fixed, as the version of a UUID would be.
    #[cfg(target_arch = "wasm32")]
    {
        let ones = bits.iter().fold(0, |seen, bits| seen | bits);
        let zeros = bits.iter().fold(0, |seen, bits| seen | !bits);
        assert_eq!((ones, zeros), (u128::MAX, u128::MAX), "{names:?}");
    }
    #[cfg(not(target_arch = "wasm32"))]
    drop(bits);
}

/// Lets the other tasks run once.
async fn yield_once() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}
