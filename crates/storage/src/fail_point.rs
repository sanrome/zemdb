//! Test infrastructure: fault injection points for crash and I/O failure tests.
//!
//! Production code calls `check` at the exact spots where a crash or I/O error must be
//! reproducible, and `pause` where a test must hold an operation in the middle (for example
//! while a WAL sync is in progress). In non-test builds both are empty functions that the
//! compiler removes, so they have no runtime effect. In unit-test builds a test arms a point for
//! a specific file path with `arm` or `arm_pause`, so tests running in parallel never interfere
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
