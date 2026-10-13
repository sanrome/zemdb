//! Test infrastructure: fault injection points for crash and I/O failure tests.
//!
//! Production code marks the spots where a test must inject a failure or hold an operation in
//! the middle. Outside test builds every point does nothing (or returns its input) and the
//! compiler removes it. A test arms a point for a specific file path, so tests running in
//! parallel never interfere with each other, and each armed point fires once.

use crate::error::StorageError;
use std::path::Path;

#[cfg(test)]
static ARMED: std::sync::Mutex<Vec<(&'static str, std::path::PathBuf)>> =
    std::sync::Mutex::new(Vec::new());

/// A pause point armed for one path: signals `reached` when hit, then waits for `release`.
#[cfg(test)]
type ArmedPause = (
    &'static str,
    std::path::PathBuf,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

#[cfg(test)]
static PAUSES: std::sync::Mutex<Vec<ArmedPause>> = std::sync::Mutex::new(Vec::new());

/// Arms the fail point `name` for `path`; the next `check` with the same pair fails.
#[cfg(test)]
pub(crate) fn arm(name: &'static str, path: &Path) {
    ARMED.lock().unwrap().push((name, path.to_path_buf()));
}

/// Returns an injected I/O error if the fail point `name` is armed for `path`.
#[inline(always)]
pub(crate) fn check(name: &'static str, path: &Path) -> Result<(), StorageError> {
    #[cfg(test)]
    {
        let mut armed = ARMED.lock().unwrap();
        if let Some(pos) = armed.iter().position(|(n, p)| *n == name && p == path) {
            armed.remove(pos);
            return Err(StorageError::Io(std::io::Error::other(format!(
                "injected failure at {name}"
            ))));
        }
    }
    #[cfg(not(test))]
    let _ = (name, path);
    Ok(())
}

/// Panics if the fail point `name` is armed for `path`, to reproduce a panic at that spot.
#[inline(always)]
pub(crate) fn panic_point(name: &'static str, path: &Path) {
    if check(name, path).is_err() {
        panic!("injected panic at {name}");
    }
}

/// Returns an injected I/O error instead of `result` if the fail point `name` is armed for
/// `path`, as if the call that produced `result` had failed.
#[inline(always)]
pub(crate) fn io_result<T>(
    name: &'static str,
    path: &Path,
    result: std::io::Result<T>,
) -> std::io::Result<T> {
    #[cfg(test)]
    if check(name, path).is_err() {
        return Err(std::io::Error::other(format!("injected failure at {name}")));
    }
    #[cfg(not(test))]
    let _ = (name, path);
    result
}

/// Replaces `file`, open on `file_path`, with a read-only handle of the same file if the fail
/// point `name` is armed for `path`. The writes that follow are accepted by `write_all` and then
/// fail on the blocking pool, as on a full disk. Writes already issued through `file` complete
/// first.
///
/// Dropping the replaced handle releases any lock it held, such as the room's exclusive lock on
/// its WAL; only tests that arm this point are affected.
#[inline(always)]
pub(crate) async fn read_only_handle(
    name: &'static str,
    path: &Path,
    file: &mut tokio::fs::File,
    file_path: &Path,
) {
    #[cfg(test)]
    if check(name, path).is_err() {
        use tokio::io::AsyncWriteExt;
        file.flush().await.unwrap();
        *file = tokio::fs::File::from_std(std::fs::File::open(file_path).unwrap());
    }
    #[cfg(not(test))]
    let _ = (name, path, file, file_path);
}

/// Handle of an armed pause point, held by the test.
#[cfg(test)]
pub(crate) struct Pause {
    reached: tokio::sync::oneshot::Receiver<()>,
    release: tokio::sync::oneshot::Sender<()>,
}

#[cfg(test)]
impl Pause {
    /// Waits until the operation reaches the pause point.
    pub(crate) async fn reached(&mut self) {
        (&mut self.reached).await.unwrap();
    }

    /// Lets the paused operation continue.
    pub(crate) fn release(self) {
        let _ = self.release.send(());
    }
}

/// Arms the pause point `name` for `path`: the next `pause` with the same pair waits until the
/// returned handle is released.
#[cfg(test)]
pub(crate) fn arm_pause(name: &'static str, path: &Path) -> Pause {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    PAUSES
        .lock()
        .unwrap()
        .push((name, path.to_path_buf(), reached_tx, release_rx));
    Pause {
        reached: reached_rx,
        release: release_tx,
    }
}

/// Waits for the test to release the pause point `name` if it is armed for `path`.
#[inline(always)]
pub(crate) async fn pause(name: &'static str, path: &Path) {
    #[cfg(test)]
    {
        let armed = {
            let mut pauses = PAUSES.lock().unwrap();
            pauses
                .iter()
                .position(|(n, p, _, _)| *n == name && p == path)
                .map(|pos| pauses.remove(pos))
        };
        if let Some((_, _, reached, release)) = armed {
            let _ = reached.send(());
            let _ = release.await;
        }
    }
    #[cfg(not(test))]
    let _ = (name, path);
}

/// How long a blocking pause waits to be released before it panics.
#[cfg(test)]
const BLOCKING_PAUSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A blocking pause point armed for one path: signals `reached` when hit, then blocks its thread
/// until `release`.
#[cfg(test)]
type ArmedBlockingPause = (
    &'static str,
    std::path::PathBuf,
    tokio::sync::oneshot::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

#[cfg(test)]
static BLOCKING_PAUSES: std::sync::Mutex<Vec<ArmedBlockingPause>> =
    std::sync::Mutex::new(Vec::new());

/// Handle of an armed blocking pause point, held by the test.
#[cfg(test)]
pub(crate) struct BlockingPause {
    reached: tokio::sync::oneshot::Receiver<()>,
    release: std::sync::mpsc::Sender<()>,
}

#[cfg(test)]
impl BlockingPause {
    /// Waits until the operation reaches the pause point.
    pub(crate) async fn reached(&mut self) {
        (&mut self.reached).await.unwrap();
    }

    /// Lets the paused operation continue.
    pub(crate) fn release(self) {
        let _ = self.release.send(());
    }
}

/// Arms the blocking pause point `name` for `path`: the next `pause_blocking` with the same pair
/// blocks its thread until the returned handle is released.
#[cfg(test)]
pub(crate) fn arm_blocking_pause(name: &'static str, path: &Path) -> BlockingPause {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    BLOCKING_PAUSES
        .lock()
        .unwrap()
        .push((name, path.to_path_buf(), reached_tx, release_rx));
    BlockingPause {
        reached: reached_rx,
        release: release_tx,
    }
}

/// Blocks the calling thread until the test releases the pause point `name`, if it is armed for
/// `path`.
///
/// The test releases it from an async task, which can only run while some runtime thread is
/// free; so it panics if it is not released within a few seconds instead of hanging, which is
/// what happens when it is reached on the only thread of a current-thread runtime.
#[inline(always)]
pub(crate) fn pause_blocking(name: &'static str, path: &Path) {
    #[cfg(test)]
    {
        let armed = {
            let mut pauses = BLOCKING_PAUSES.lock().unwrap();
            pauses
                .iter()
                .position(|(n, p, _, _)| *n == name && p == path)
                .map(|pos| pauses.remove(pos))
        };
        if let Some((_, _, reached, release)) = armed {
            let _ = reached.send(());
            if release.recv_timeout(BLOCKING_PAUSE_TIMEOUT).is_err() {
                panic!("pause point {name} was not released: the runtime is blocked");
            }
        }
    }
    #[cfg(not(test))]
    let _ = (name, path);
}
