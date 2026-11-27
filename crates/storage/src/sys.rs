//! What the storage takes from its runtime and from the system: its tasks, the work of the CPU,
//! its randomness and the sync of a directory. Natively they are Tokio's and the system's; in
//! WebAssembly, the event loop of the browser and `crypto.getRandomValues`.
//!
//! Only this module calls what panics or does not run in WebAssembly, where Tokio has no runtime
//! and `std::time` has no clock: elsewhere, the lint `clippy::disallowed_methods`, with the
//! `clippy.toml` of the crate, refuses it, natively too, outside the unit tests of the crate.
#![allow(clippy::disallowed_methods)]

use crate::error::StorageError;
use std::fmt;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Ensures that any directory entry changes (creates, renames, unlinks)
/// are written to durable storage on POSIX filesystems.
///
/// A no-op elsewhere: on Windows `File::open` cannot open a directory (that needs
/// `FILE_FLAG_BACKUP_SEMANTICS`) and NTFS journals directory changes itself; and in WebAssembly
/// `std::fs` has no file system, and the file layer over the Origin Private File System does not
/// call this (an entry there is durable once the promise that changed it resolves).
pub fn sync_dir(dir_path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let target = if dir_path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir_path
        };
        std::fs::File::open(target)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir_path;
    }
    Ok(())
}

/// Runs `f` on Tokio's blocking pool and returns its result, with its error converted.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn run_blocking<T, E>(
    f: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> Result<T, StorageError>
where
    T: Send + 'static,
    E: Into<StorageError> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
        .map_err(Into::into)
}

/// Runs `f` in place: WebAssembly has no blocking pool.
#[cfg(target_arch = "wasm32")]
pub(crate) async fn run_blocking<T, E: Into<StorageError>>(
    f: impl FnOnce() -> Result<T, E>,
) -> Result<T, StorageError> {
    f().map_err(Into::into)
}

/// What a task and what it gives need to be spawned: natively `Send`, since Tokio may move a task
/// between its threads; in WebAssembly nothing, since everything runs on the thread of the Worker.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

/// What a task and what it gives need to be spawned: natively `Send`, since Tokio may move a task
/// between its threads; in WebAssembly nothing, since everything runs on the thread of the Worker.
#[cfg(target_arch = "wasm32")]
pub(crate) trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// A task of the storage, which runs until its end whether or not anything waits for it: dropping
/// it lets the task go on, with what it holds (the locks of a room, a file in flight). Awaiting it
/// gives what the task gave, or a [`TaskError`] if the task ended without giving it: natively,
/// because it panicked or its runtime shut down. In WebAssembly a panic aborts the instance, so
/// there it only happens if the event loop drops the task.
pub(crate) struct Task<T> {
    #[cfg(not(target_arch = "wasm32"))]
    handle: tokio::task::JoinHandle<T>,
    #[cfg(target_arch = "wasm32")]
    output: tokio::sync::oneshot::Receiver<T>,
}

/// Why a task gave nothing.
#[derive(Debug)]
pub(crate) struct TaskError(String);

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<T> Future for Task<T> {
    type Output = Result<T, TaskError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(not(target_arch = "wasm32"))]
        let polled = Pin::new(&mut self.handle).poll(cx);
        #[cfg(target_arch = "wasm32")]
        let polled = Pin::new(&mut self.output).poll(cx);
        polled.map_err(|err| TaskError(err.to_string()))
    }
}

/// Runs `task` in a task of its own until its end, even if nobody awaits it: natively on the
/// Tokio runtime it is called in, and in WebAssembly in the event loop of the browser
/// (`wasm_bindgen_futures::spawn_local`).
#[track_caller]
pub(crate) fn spawn<F>(task: F) -> Task<F::Output>
where
    F: Future + MaybeSend + 'static,
    F::Output: MaybeSend + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    {
        Task {
            handle: tokio::spawn(task),
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        let (sender, output) = tokio::sync::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let given = task.await;
            // The receiver is gone if nobody waits any more: the task ran to its end all the same.
            drop(sender.send(given));
        });
        Task { output }
    }
}

/// A name at random for a temporary file, of 128 bits: natively a UUID of version 4, and in
/// WebAssembly 32 hexadecimal digits from `crypto.getRandomValues`. Both are made of lowercase
/// hexadecimal digits and `-`, which no layer takes for anything else (the generations of the
/// layer over the Origin Private File System are marked with `~`).
pub(crate) fn random_name() -> Result<String, StorageError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        Ok(uuid::Uuid::new_v4().to_string())
    }
    #[cfg(target_arch = "wasm32")]
    {
        let mut bits = [0; 16];
        getrandom::fill(&mut bits).map_err(|err| {
            StorageError::Other(format!("crypto.getRandomValues gave no randomness: {err}"))
        })?;
        Ok(format!("{:032x}", u128::from_le_bytes(bits)))
    }
}

#[cfg(test)]
#[path = "tests/sys.rs"]
mod tests;
