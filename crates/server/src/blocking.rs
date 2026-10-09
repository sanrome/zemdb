//! Blocking I/O from the async code of the server.
//!
//! A room actor is a Tokio task, and most of what it does is synchronous file I/O: the append
//! of every commit ends in an `fsync` (`F_FULLFSYNC` on macOS, milliseconds), metadata files are
//! replaced atomically, the roster is saved, cold segments are decompressed, and opening a room
//! reads the newest segments of its log. Run directly on a worker thread, that I/O stalls every
//! other task queued on the same worker, for example other rooms committing at the same time.
//!
//! [`blocking_io`] is the single place where the server runs such work from async code. It uses
//! `tokio::task::block_in_place`, which hands the worker's other tasks to another thread while
//! the calling task blocks, and keeps the caller's borrows: the actor runs the I/O on its own
//! state without moving it into a `spawn_blocking` closure. `block_in_place` panics on a
//! current-thread runtime (the default of `#[tokio::test]`), where there is no other worker
//! to hand tasks to, so there the work simply runs in place, as it does outside any runtime.
//! Nested calls are fine: once the worker has been handed off, the inner call runs in place.

use tokio::runtime::{Handle, RuntimeFlavor};

/// Runs blocking I/O `work` without stalling the other tasks of a multi-thread runtime.
pub(crate) fn blocking_io<T>(work: impl FnOnce() -> T) -> T {
    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(work)
        }
        _ => work(),
    }
}

#[cfg(test)]
#[path = "tests/blocking.rs"]
mod tests;
