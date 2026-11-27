//! The file layer of the disk engine: the operations it makes on its data directory and on the
//! files it opens there.
//!
//! `NativeFiles` implements it on the file system of the process, and, in WebAssembly,
//! `OpfsFiles` on the Origin Private File System of a browser. The operations are those the
//! engine makes, and only on files in its data directory (the data directory itself and its
//! ancestors, for [`RoomFiles::create_dir_all`] and [`RoomFiles::sync_dir`]). Every operation is
//! asynchronous: on the Origin Private File System, opening a file, renaming it, removing it and
//! listing a directory are promises.

// The native layer runs on Tokio's blocking pool, which the lint of the crate keeps out of
// everything that builds in WebAssembly; the layer never does.
#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::disallowed_methods)]
pub mod native;
#[cfg(target_arch = "wasm32")]
pub mod opfs;
#[cfg(any(target_arch = "wasm32", test))]
mod opfs_names;

#[cfg(not(target_arch = "wasm32"))]
pub use native::{NativeFile, NativeFiles};
#[cfg(target_arch = "wasm32")]
pub use opfs::{OpfsFile, OpfsFiles};

/// The file layer of the platform, the one of `DiskStorageEngine::new`: natively the file system
/// of the process, and in WebAssembly the Origin Private File System.
#[cfg(not(target_arch = "wasm32"))]
pub type PlatformFiles = NativeFiles;
/// The file layer of the platform, the one of `DiskStorageEngine::new`: natively the file system
/// of the process, and in WebAssembly the Origin Private File System.
#[cfg(target_arch = "wasm32")]
pub type PlatformFiles = OpfsFiles;

use crate::engine::EngineConcurrencyBounds;
use async_trait::async_trait;
use std::fmt::{self, Debug};
use std::io;
use std::path::{Path, PathBuf};

/// The data directory of a disk engine.
///
/// A file renamed or removed by path has no handle open, nor does the destination of a rename:
/// the Origin Private File System refuses to move or remove a file that has one. An open file is
/// renamed through its handle ([`RoomFile::rename`]).
///
/// `Send + Sync`, except in WebAssembly, where the objects of JavaScript are neither.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RoomFiles: Debug + EngineConcurrencyBounds + 'static {
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

    /// Creates `path` for writing, emptying it if it exists, without a lock. Where the layer
    /// cannot open a file besides another handle, one open on `path` fails it with the error of
    /// a taken file ([`FileTaken`]).
    async fn create(&self, path: &Path) -> io::Result<Self::File>;

    /// Opens the existing file `path` for reading, without a lock. Where the layer cannot open a
    /// file besides another handle, one open on `path` fails it with the error of a taken file
    /// ([`FileTaken`]).
    async fn open_read(&self, path: &Path) -> io::Result<Self::File>;

    /// Whether `path` exists. An error is not a missing file.
    async fn exists(&self, path: &Path) -> io::Result<bool>;

    /// The paths of the files in `dir` whose names start with `prefix`, each once. Fails if any
    /// entry of `dir` cannot be read.
    async fn list(&self, dir: &Path, prefix: &str) -> io::Result<Vec<PathBuf>>;

    /// Renames `from` to `to`, replacing `to` if it exists. A rename that fails changes what
    /// neither `from` nor `to` holds. Where the layer cannot move a file with a handle open, one
    /// open on `from` fails it with the error of a taken file ([`FileTaken`]).
    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Removes the file `path`; [`io::ErrorKind::NotFound`] if there is none. Where the layer
    /// cannot remove a file with a handle open, one open on `path` fails it with the error of a
    /// taken file ([`FileTaken`]).
    async fn remove(&self, path: &Path) -> io::Result<()>;

    /// Creates the directory `path` and any missing ancestors, making each new entry durable
    /// before it returns.
    async fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Makes durable the changes to the entries of `dir` (files created, renamed or removed in
    /// it) of the operations that succeeded. Where it does nothing, because each such change is
    /// durable once its operation succeeds, only those are guaranteed durable: a change that an
    /// operation that failed left behind may stay, and not be durable. Whether it also makes
    /// durable a change that already happened, that of an operation that failed included, is
    /// what [`sync_dir_covers_earlier_changes`](Self::sync_dir_covers_earlier_changes) says.
    async fn sync_dir(&self, dir: &Path) -> io::Result<()>;

    /// Whether [`sync_dir`](Self::sync_dir) makes durable every change to the entries of the
    /// directory that already happened, also one that an operation that failed left behind. On
    /// the file system of the process it does (on Windows, where it does nothing, the journal of
    /// NTFS does); on the Origin Private File System it does not.
    fn sync_dir_covers_earlier_changes(&self) -> bool;
}

/// A file opened by a [`RoomFiles`], read and written at explicit offsets: it has no position.
///
/// Each operation reports its own error. One whose caller stopped waiting still runs to its end,
/// and the handle keeps its file, and its lock, until then: operations run one at a time, in the
/// order they were called, and the error of one that nobody awaited is reported by the next
/// operation on the handle, which then does not run.
///
/// `Send + Sync`, except in WebAssembly, where the objects of JavaScript are neither.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RoomFile: Debug + EngineConcurrencyBounds + 'static {
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

/// The error of an operation on a file that another handle has open, where the layer cannot
/// make it besides that handle, as on the Origin Private File System, whose handles are
/// exclusive. It is an [`io::Error`] of kind [`io::ErrorKind::ResourceBusy`] that wraps this
/// type, and [`FileTaken::is`] recognizes it: another error of that kind is not this one.
#[derive(Debug)]
pub struct FileTaken {
    message: String,
}

impl FileTaken {
    /// The error of a taken file, with `message`, which says what was taken.
    pub fn error(message: impl Into<String>) -> io::Error {
        let message = message.into();
        io::Error::new(io::ErrorKind::ResourceBusy, Self { message })
    }

    /// Whether `err` is the error of a taken file.
    pub fn is(err: &io::Error) -> bool {
        err.get_ref().is_some_and(|inner| inner.is::<Self>())
    }
}

impl fmt::Display for FileTaken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FileTaken {}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "../tests/files.rs"]
mod tests;
