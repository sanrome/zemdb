use crate::error::StorageError;
use std::path::Path;

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

/// Runs `task` until its end, in the event loop of the browser, even if nobody awaits it, and
/// gives its output to the receiver it returns, while someone waits for it.
#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn<T: 'static>(
    task: impl std::future::Future<Output = T> + 'static,
) -> tokio::sync::oneshot::Receiver<T> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        let output = task.await;
        // The receiver is gone if nobody waits any more: the task ran to its end all the same.
        drop(sender.send(output));
    });
    receiver
}
