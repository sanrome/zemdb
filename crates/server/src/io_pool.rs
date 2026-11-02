//! The bounded pools that run the file I/O of the server's async code.
//!
//! A room actor is a Tokio task, and most of what it does is synchronous file I/O; every commit
//! ends in an `fsync`. Run on a worker thread, that I/O stalls every other task queued on the
//! worker, for example other rooms committing at the same time; handing the worker over to
//! another thread while it blocks (`block_in_place`) needs a new thread for each room blocked
//! at once. Instead, async code sends each file operation to a pool of a fixed size and awaits
//! its result: no worker does file I/O, and the threads of the process do not depend on how
//! many rooms there are. An operation runs on Tokio's blocking threads, once it holds one of
//! the pool's permits: an operation waits, in the order it came, until one is free, so at most
//! as many threads run a pool's operations as it has permits.
//!
//! An operation owns what it touches (a room's log travels to the pool and back), so a caller
//! dropped while it waits never leaves an operation behind that borrows something gone: the
//! operation runs to its end, and what it owns is dropped on its thread afterwards. A panic in
//! an operation is resumed in the task that awaits it. Dropping the runtime waits for the
//! operations that started, so `server.lock` is released only after them (as for any blocking
//! work of the runtime).

use std::panic;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Operations at once of the pool that compresses warm segments into cold ones. Compressing
/// takes far longer than any other file operation (about 0.1–0.3 s per segment, mostly CPU),
/// so it has a small pool of its own, which never keeps the other operations waiting.
const COMPRESSION_THREADS: usize = 2;

/// A pool that runs at most a fixed number of blocking operations at once, on Tokio's blocking
/// threads.
#[derive(Debug)]
pub(crate) struct IoPool {
    permits: Arc<Semaphore>,
}

impl IoPool {
    fn new(threads: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(threads)),
        }
    }

    /// Runs `work` on a blocking thread once the pool has room for it, and waits for its result
    /// without blocking the caller's thread. A panic in `work` is resumed here.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        run_holding(self.permit().await, work).await
    }

    /// One of the pool's permits, once one is free, in the order they were asked for.
    pub(crate) async fn permit(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("the pool's semaphore is never closed")
    }

    /// One of the pool's permits, if one is free now.
    pub(crate) fn try_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.permits).try_acquire_owned().ok()
    }
}

/// Runs `work` on a blocking thread, holding `permit`, a permit of the pool it counts against,
/// until `work` ends, and waits for its result like [`IoPool::run`].
pub(crate) async fn run_holding<T: Send + 'static>(
    permit: OwnedSemaphorePermit,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    });
    match task.await {
        Ok(out) => out,
        Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
        // Only a runtime shutting down cancels the task (before it runs, dropping `work`), and
        // it is about to drop this caller too.
        Err(_) => std::future::pending().await,
    }
}

/// The pools that run the file I/O of the rooms: one that runs at most `io_threads` operations
/// at once, and the one that compresses warm segments.
#[derive(Debug)]
pub struct IoThreads {
    io: IoPool,
    compression: IoPool,
}

impl IoThreads {
    /// The pools of a server whose rooms run at most `io_threads` file operations at once.
    pub fn new(io_threads: usize) -> Arc<Self> {
        Arc::new(Self {
            io: IoPool::new(io_threads),
            compression: IoPool::new(COMPRESSION_THREADS),
        })
    }

    /// The pool of the file operations of the rooms.
    pub(crate) fn io(&self) -> &IoPool {
        &self.io
    }

    /// The pool that compresses warm segments.
    pub(crate) fn compression(&self) -> &IoPool {
        &self.compression
    }
}

#[cfg(test)]
#[path = "tests/io_pool.rs"]
mod tests;
