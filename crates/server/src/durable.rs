//! Crash-safe file persistence helpers.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Flushes a directory's entries to disk so that file creations, renames and deletions
/// inside it survive a power loss. A no-op on platforms that cannot open directories.
pub(crate) fn sync_dir(dir_path: &Path) -> std::io::Result<()> {
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
    fs::rename(&tmp_path, path)?;
    sync_dir(path.parent().unwrap_or_else(|| Path::new("")))
}

#[cfg(test)]
mod tests;
