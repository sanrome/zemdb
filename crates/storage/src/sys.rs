use std::path::Path;

/// Ensures that any directory entry changes (creates, renames, unlinks)
/// are written to durable storage on POSIX filesystems.
///
/// A no-op elsewhere: on Windows `File::open` cannot open a directory (that needs
/// `FILE_FLAG_BACKUP_SEMANTICS`) and NTFS journals directory changes itself, and in
/// WebAssembly there is no file system.
pub fn sync_dir(dir_path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let target = if dir_path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir_path
        };
        let file = std::fs::File::open(target)?;
        file.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir_path;
    }
    Ok(())
}
