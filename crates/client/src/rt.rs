//! The runtime of the client: its tasks, its timers, its clock, its randomness and its wall clock,
//! which nothing else in the client takes from Tokio or from the system. In native they are
//! Tokio's and the system's: the paused clock of a test on Tokio controls every timer of the
//! client.
//!
//! Only this module calls what panics or does not run in WebAssembly, where Tokio has no runtime
//! and `std::time` has no clock: elsewhere, the lint `clippy::disallowed_methods`, with the
//! `clippy.toml` of the crate, refuses it, in native too, outside the unit tests of the crate.
#![allow(clippy::disallowed_methods)]

use futures::future::FutureExt;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The monotonic instant of the timers of the client: Tokio's, which the paused clock of a test
/// controls. [`now`] gives the current one.
pub(crate) use tokio::time::Instant;

/// What a task, what it gives and the work of [`spawn_blocking`] need to be spawned: in native,
/// `Send`, since the runtime may move a task between its threads.
pub(crate) trait MaybeSend: Send {}

impl<T: Send> MaybeSend for T {}

/// A task of the runtime, which runs until its end whether or not anything waits for it: dropping
/// its handle lets it go on. Awaiting it gives what the task gave, or an error once it panicked or
/// was cut.
#[derive(Debug)]
pub(crate) struct Task<T>(tokio::task::JoinHandle<T>);

impl<T> Task<T> {
    /// Cuts the task at its next `.await`, where what it holds is dropped: awaiting it then gives
    /// an error, unless it had already ended. The work of [`spawn_blocking`] is not cut once it
    /// started.
    pub(crate) fn abort(&self) {
        self.0.abort();
    }

    /// Whether the task ended: it returned, panicked or was cut.
    pub(crate) fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

impl<T> Future for Task<T> {
    type Output = Result<T, TaskError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map_err(TaskError)
    }
}

/// Why a task gave nothing: it panicked or was cut.
#[derive(Debug)]
pub(crate) struct TaskError(tokio::task::JoinError);

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Runs `future` in a task of its own, on the Tokio runtime it is called in.
#[track_caller]
pub(crate) fn spawn<F>(future: F) -> Task<F::Output>
where
    F: Future + MaybeSend + 'static,
    F::Output: MaybeSend + 'static,
{
    Task(tokio::spawn(future))
}

/// Runs `work`, work of the CPU that would stall the runtime, in a task of its own on the
/// blocking pool of Tokio.
#[track_caller]
pub(crate) fn spawn_blocking<F, T>(work: F) -> Task<T>
where
    F: FnOnce() -> T + MaybeSend + 'static,
    T: MaybeSend + 'static,
{
    Task(tokio::task::spawn_blocking(work))
}

/// The current instant.
pub(crate) fn now() -> Instant {
    Instant::now()
}

/// What sleeps for `duration`, counted from the call. A `duration` whose end does not fit in the
/// clock never ends, as in Tokio, which puts it about thirty years ahead.
#[track_caller]
pub(crate) fn sleep(duration: Duration) -> impl Future<Output = ()> {
    tokio::time::sleep(duration)
}

/// What sleeps until `deadline`: it never wakes before it.
#[track_caller]
pub(crate) fn sleep_until(deadline: Instant) -> impl Future<Output = ()> {
    tokio::time::sleep_until(deadline)
}

/// A deadline that came before its future ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Elapsed;

/// What `future` gives, or [`Elapsed`] once `duration`, counted from the call, went by before it:
/// `future` is dropped there. A `duration` whose end does not fit in the clock never comes, as in
/// Tokio, which puts it about thirty years ahead.
#[track_caller]
pub(crate) fn timeout<F: Future>(
    duration: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, Elapsed>> {
    tokio::time::timeout(duration, future).map(|output| output.map_err(|_| Elapsed))
}

/// What `future` gives, or [`Elapsed`] once `deadline` came before it: `future` is dropped there.
#[track_caller]
pub(crate) fn timeout_at<F: Future>(
    deadline: Instant,
    future: F,
) -> impl Future<Output = Result<F::Output, Elapsed>> {
    tokio::time::timeout_at(deadline, future).map(|output| output.map_err(|_| Elapsed))
}

/// A random version 4 UUID, as a number, from the randomness of the system: the `MutationId` of a
/// write, the id of a device and the jitter of a backoff.
pub(crate) fn random_uuid() -> u128 {
    uuid::Uuid::new_v4().as_u128()
}

/// The wall clock, in milliseconds since the Unix epoch: 0 before it, and `u64::MAX` past what
/// fits.
pub(crate) fn wall_millis() -> u64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH);
    let millis = since.map_or(0, |since| since.as_millis());
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "tests/rt.rs"]
mod tests;
