//! Test infrastructure: fault injection points for crash and I/O failure tests.
//!
//! Production code calls `check` at the exact spots where a crash or I/O error must be
//! reproducible, and `pause` where a test must hold an operation in the middle (for example
//! while a WAL sync is in progress). Three variants act on what is at hand instead of failing
//! before it: `panic_point` panics, `io_result` replaces the result of an I/O call with an error,
//! and `read_only_handle` swaps a file handle for one whose writes fail in the background. In
//! non-test builds all of them are empty functions (or return their input) that the compiler
//! removes, so they have no runtime effect. In unit-test builds a test arms a point for a
//! specific file path with `arm` or `arm_pause`, so tests running in parallel never interfere
//! with each other; each armed point fires once.

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
