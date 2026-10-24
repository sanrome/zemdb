//! The file layer of the disk engine: the operations it makes on its data directory and on the
//! files it opens there.
//!
//! [`NativeFiles`] implements it on the file system of the process. The operations are those the
//! engine makes, and only on files in its data directory (the data directory itself and its
//! ancestors, for [`RoomFiles::create_dir_all`] and [`RoomFiles::sync_dir`]). Every operation is
//! asynchronous: on the Origin Private File System of a browser, opening a file, renaming it,
//! removing it and listing a directory are promises.

pub mod native;

pub use native::{NativeFile, NativeFiles};

use async_trait::async_trait;
use std::fmt::Debug;
use std::io;
use std::path::{Path, PathBuf};

/// The data directory of a disk engine.
///
/// A file renamed or removed by path has no handle open, nor does the destination of a rename:
/// the Origin Private File System refuses to move or remove a file that has one. An open file is
/// renamed through its handle ([`RoomFile::rename`]).
#[async_trait]
pub trait RoomFiles: Debug + Send + Sync + 'static {
    /// A file opened by this directory.
    type File: RoomFile;

    /// Opens `path` for reading and writing, creating it if it does not exist, and takes its
    /// exclusive lock. `None` if the lock cannot be taken (because another handle, in this
    /// process or another, holds it).
    async fn open_locked(&self, path: &Path) -> io::Result<Option<Self::File>>;

    /// Creates `path` for reading and writing and takes its exclusive lock. Fails with
    /// [`io::ErrorKind::AlreadyExists`] if the file exists, which it never truncates or adopts.
    /// `None` if the lock cannot be taken; the file is then left created.
    async fn create_locked(&self, path: &Path) -> io::Result<Option<Self::File>>;

    /// Creates `path` for writing, emptying it if it exists, without a lock.
    async fn create(&self, path: &Path) -> io::Result<Self::File>;

    /// Opens the existing file `path` for reading, without a lock.
    async fn open_read(&self, path: &Path) -> io::Result<Self::File>;

    /// Whether `path` exists. An error is not a missing file.
    async fn exists(&self, path: &Path) -> io::Result<bool>;

    /// The paths of the files in `dir` whose names start with `prefix`. Fails if any entry of
    /// `dir` cannot be read.
    async fn list(&self, dir: &Path, prefix: &str) -> io::Result<Vec<PathBuf>>;

    /// Renames `from` to `to`, replacing `to` if it exists.
    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Removes the file `path`; [`io::ErrorKind::NotFound`] if there is none.
    async fn remove(&self, path: &Path) -> io::Result<()>;

    /// Creates the directory `path` and any missing ancestors, making each new entry durable
    /// before it returns.
    async fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Makes the changes to the entries of `dir` (files created, renamed or removed in it)
    /// durable. A no-op where they are durable without it.
    async fn sync_dir(&self, dir: &Path) -> io::Result<()>;
}

/// A file opened by a [`RoomFiles`], read and written at explicit offsets: it has no position.
///
/// Each operation reports its own error. One whose caller stopped waiting still runs to its end,
/// and the handle keeps its file, and its lock, until then: operations run one at a time, in the
/// order they were called, and the error of one that nobody awaited is reported by the next
/// operation on the handle, which then does not run.
#[async_trait]
pub trait RoomFile: Debug + Send + Sync + 'static {
    /// Reads `len` bytes at `offset`; [`io::ErrorKind::UnexpectedEof`] if the file ends first.
    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    /// Writes all of `bytes` at `offset`, extending the file if needed.
    async fn write_at(&self, offset: u64, bytes: Vec<u8>) -> io::Result<()>;

    /// The size of the file, in bytes.
    async fn size(&self) -> io::Result<u64>;

    /// Truncates or extends the file to `len` bytes.
    async fn set_len(&self, len: u64) -> io::Result<()>;

    /// Makes the data written durable (`fdatasync`), which is enough after an append.
    async fn sync_data(&self) -> io::Result<()>;

    /// Makes the data and the metadata written durable (`fsync`).
    async fn sync_all(&self) -> io::Result<()>;

    /// Renames the file to `to`, replacing `to` if it exists. The handle follows the file, and
    /// keeps its lock. Nothing else may have `to` open.
    async fn rename(&mut self, to: &Path) -> io::Result<()>;
}
