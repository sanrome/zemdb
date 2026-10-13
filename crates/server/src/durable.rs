//! Crash-safe file persistence helpers.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::blocking::blocking_io;
use crate::fail_point;

/// Flushes a directory's entries to disk so that file creations, renames and deletions
/// inside it survive a power loss.
///
/// A no-op on platforms other than Unix. On Windows `File::open` cannot open a directory
/// (that needs `FILE_FLAG_BACKUP_SEMANTICS`), and NTFS journals directory changes itself, so
/// there is nothing to call; opening the directory anyway would only turn every durable write
/// into an error there.
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
    Ok(())
}

/// Creates `path` and any missing parent directories, syncing the parent of every directory
/// it creates so that the new entries survive a power loss.
pub(crate) fn create_dir_all_synced(path: &Path) -> std::io::Result<()> {
    let missing: Vec<&Path> = path
        .ancestors()
        .take_while(|dir| !dir.as_os_str().is_empty() && !dir.is_dir())
        .collect();
    for dir in missing.into_iter().rev() {
        match fs::create_dir(dir) {
            Ok(()) => {}
            // Created concurrently; its parent is synced below all the same.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        sync_parent(dir)?;
    }
    Ok(())
}

/// Syncs the directory holding `path` (see [`sync_dir`]).
pub(crate) fn sync_parent(path: &Path) -> std::io::Result<()> {
    sync_dir(path.parent().unwrap_or_else(|| Path::new("")))
}

/// Deletes the file at `path`; a file that does not exist counts as deleted.
pub(crate) fn remove_file_if_exists(path: &Path) -> std::io::Result<()> {
    injected("remove_file", path)?;
    match fs::remove_file(path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
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
/// never a partially written file. Callers are often async, so the blocking I/O runs through
/// [`blocking_io`].
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    blocking_io(|| write_atomic_blocking(path, bytes))
}

fn write_atomic_blocking(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp_path = tmp_path_for(path);
    {
        let mut tmp_file = File::create(&tmp_path)?;
        tmp_file.write_all(bytes)?;
        tmp_file.sync_all()?;
    }
    injected("write_atomic_before_rename", path)?;
    fs::rename(&tmp_path, path)?;
    sync_parent(path)
}

/// Surfaces an armed test fail point as the I/O error the caller would have seen.
pub(crate) fn injected(name: &'static str, path: &Path) -> std::io::Result<()> {
    fail_point::check(name, path).map_err(|e| std::io::Error::other(e.to_string()))
}

#[cfg(test)]
#[path = "tests/durable.rs"]
mod tests;
