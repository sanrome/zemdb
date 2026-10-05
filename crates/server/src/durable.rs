//! Crash-safe file persistence helpers.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::fail_point;

/// Flushes a directory's entries to disk so that file creations, renames and deletions
/// inside it survive a power loss. A no-op on platforms that cannot open directories.
pub(crate) fn sync_dir(dir_path: &Path) -> std::io::Result<()> {
    injected("sync_dir", dir_path)?;
    #[cfg(unix)]
    {
        let target = if dir_path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir_path
        };
        File::open(target)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir_path;
    }
    Ok(())
}

/// Creates `path` and any missing parent directories, syncing the parent of every directory
/// it creates so that the new entries survive a power loss.
pub(crate) fn create_dir_all_synced(path: &Path) -> std::io::Result<()> {
    let mut missing = Vec::new();
    let mut current = path;
    while !current.as_os_str().is_empty() && !current.is_dir() {
        missing.push(current);
        match current.parent() {
            Some(parent) => current = parent,
            None => break,
        }
    }
    for dir in missing.into_iter().rev() {
        match fs::create_dir(dir) {
            Ok(()) => {}
            // Created concurrently; its parent is synced below all the same.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        sync_dir(dir.parent().unwrap_or_else(|| Path::new("")))?;
    }
    Ok(())
}

/// Temporary sibling path used while atomically replacing `path`.
pub(crate) fn tmp_path_for(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

/// Atomically replaces `path` with `bytes`.
///
/// Writes a temporary sibling file, syncs it, renames it over the destination and syncs the
/// parent directory. A crash at any point leaves either the previous or the new content,
/// never a partially written file.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp_path = tmp_path_for(path);
    {
        let mut tmp_file = File::create(&tmp_path)?;
        tmp_file.write_all(bytes)?;
        tmp_file.sync_all()?;
    }
    injected("write_atomic_before_rename", path)?;
    fs::rename(&tmp_path, path)?;
    sync_dir(path.parent().unwrap_or_else(|| Path::new("")))
}

/// Surfaces an armed test fail point as the I/O error the caller would have seen.
fn injected(name: &'static str, path: &Path) -> std::io::Result<()> {
    fail_point::check(name, path).map_err(|e| std::io::Error::other(e.to_string()))
}

#[cfg(test)]
mod tests;
