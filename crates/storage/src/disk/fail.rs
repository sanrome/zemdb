//! Fault injection points for crash and I/O failure tests.
//!
//! In test builds a fail point can be armed for a specific file path, so tests running in
//! parallel never interfere with each other. Each armed point fires once. In non-test builds
//! `check` is an empty function and the compiler removes it.

use crate::error::StorageError;
use std::path::Path;

#[cfg(test)]
static ARMED: std::sync::Mutex<Vec<(&'static str, std::path::PathBuf)>> =
    std::sync::Mutex::new(Vec::new());

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
