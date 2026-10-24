//! The file layer on the file system of the process: `std::fs` and `fs2`, on Tokio's blocking
//! pool.

use async_trait::async_trait;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};
use tokio::sync::{oneshot, Mutex, OwnedMutexGuard};

use super::{RoomFile, RoomFiles};
use crate::fail_point;
use crate::sys::sync_dir;

/// The file system of the process. Every operation runs on Tokio's blocking pool, and runs to
/// its end even if its caller stops waiting.
#[derive(Debug, Default, Clone, Copy)]
pub struct NativeFiles;

/// Runs `f` on Tokio's blocking pool and returns its result.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| io::Error::other(format!("Join error: {e}")))?
}

/// `file` with its exclusive lock taken, or `None` if the lock cannot be taken.
fn locked(file: File, path: PathBuf) -> Option<NativeFile> {
    file.try_lock_exclusive()
        .ok()
        .map(|()| NativeFile::new(file, path))
}

#[async_trait]
impl RoomFiles for NativeFiles {
    type File = NativeFile;

    async fn open_locked(&self, path: &Path) -> io::Result<Option<NativeFile>> {
        let path = path.to_path_buf();
        blocking(move || {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&path)?;
            Ok(locked(file, path))
        })
        .await
    }

    async fn create_locked(&self, path: &Path) -> io::Result<Option<NativeFile>> {
        let path = path.to_path_buf();
        blocking(move || {
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?;
            Ok(locked(file, path))
        })
        .await
    }

    async fn create(&self, path: &Path) -> io::Result<NativeFile> {
        let path = path.to_path_buf();
        blocking(move || Ok(NativeFile::new(File::create(&path)?, path))).await
    }

    async fn open_read(&self, path: &Path) -> io::Result<NativeFile> {
        let path = path.to_path_buf();
        blocking(move || Ok(NativeFile::new(File::open(&path)?, path))).await
    }

    async fn exists(&self, path: &Path) -> io::Result<bool> {
        let path = path.to_path_buf();
        blocking(move || path.try_exists()).await
    }

    async fn list(&self, dir: &Path, prefix: &str) -> io::Result<Vec<PathBuf>> {
        let (dir, prefix) = (dir.to_path_buf(), prefix.to_string());
        blocking(move || {
            let mut found = Vec::new();
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                if entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(&prefix))
                {
                    found.push(entry.path());
                }
            }
            Ok(found)
        })
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (from, to) = (from.to_path_buf(), to.to_path_buf());
        blocking(move || std::fs::rename(from, to)).await
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        let path = path.to_path_buf();
        blocking(move || {
            fail_point::pause_blocking("remove", &path);
            std::fs::remove_file(&path)
        })
        .await
    }

    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let path = path.to_path_buf();
        blocking(move || create_dir_all_synced(&path)).await
    }

    async fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let dir = dir.to_path_buf();
        blocking(move || sync_dir(&dir)).await
    }
}

/// Creates `path` and any missing parent directories, syncing the parent of every directory it
/// creates so that the new entries survive a power loss. Blocking. A sync that fails is not
/// retried by a later call, which finds the directory already there.
pub(crate) fn create_dir_all_synced(path: &Path) -> io::Result<()> {
    let missing: Vec<&Path> = path
        .ancestors()
        .take_while(|dir| !dir.as_os_str().is_empty() && !dir.is_dir())
        .collect();
    for dir in missing.into_iter().rev() {
        match std::fs::create_dir(dir) {
            Ok(()) => {}
            // Created concurrently; its parent is synced below all the same.
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && dir.is_dir() => {}
            Err(e) => return Err(e),
        }
        if let Some(parent) = dir.parent() {
            fail_point::io_result("sync_dir", parent, sync_dir(parent))?;
        }
    }
    Ok(())
}

/// The error of an operation that nobody awaited, kept for the next operation on its handle.
type UnseenError = Arc<std::sync::Mutex<Option<io::Error>>>;

/// Keeps `err` for the next operation, unless an earlier unseen error is already kept.
fn keep_unseen(unseen: &UnseenError, err: io::Error) {
    unseen
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get_or_insert(err);
}

/// A file opened by [`NativeFiles`].
///
/// Each operation takes the handle's permit and moves it to the blocking pool, together with a
/// reference to the file, until the operation ends: a later operation, such as the sync of a
/// room being closed, waits for one whose caller stopped waiting, and the file, with its lock,
/// stays open until the last operation ends, even after the handle is dropped. Since the file
/// stays locked, a room cannot be opened again while a write of a dropped engine is still
/// landing in it.
#[derive(Debug)]
pub struct NativeFile {
    file: Arc<File>,
    /// The permit, which holds the path of the file (a rename of the open file changes it).
    permit: Arc<Mutex<PathBuf>>,
    unseen: UnseenError,
}

/// The result of an operation on the blocking pool, as its caller awaits it. Dropped before the
/// result is taken, it keeps the operation's error for the next one: the operation may have sent
/// it just before.
struct Outcome<T> {
    receiver: oneshot::Receiver<io::Result<T>>,
    unseen: UnseenError,
}

impl<T> Drop for Outcome<T> {
    fn drop(&mut self) {
        // Nothing can be sent after `close`, so a result is either taken here or kept by the
        // operation's `Reply` when its send fails.
        self.receiver.close();
        if let Ok(Err(err)) = self.receiver.try_recv() {
            keep_unseen(&self.unseen, err);
        }
    }
}

/// The end of an operation on the blocking pool: it sends the operation's result to its caller,
/// or keeps its error for the next operation if nobody awaits it. Dropped without a result (the
/// operation panicked, or the runtime shut down before it ran), it reports that the operation did
/// not complete in the same way. It holds the handle's permit, released after the result.
struct Reply<T> {
    sender: Option<oneshot::Sender<io::Result<T>>>,
    unseen: UnseenError,
    name: &'static str,
    permit: OwnedMutexGuard<PathBuf>,
}

impl<T> Reply<T> {
    fn send(&mut self, result: io::Result<T>) {
        if let Some(sender) = self.sender.take() {
            if let Err(Err(err)) = sender.send(result) {
                keep_unseen(&self.unseen, err);
            }
        }
    }
}

impl<T> Drop for Reply<T> {
    fn drop(&mut self) {
        let name = self.name;
        self.send(Err(io::Error::other(format!("{name} did not complete"))));
    }
}

impl NativeFile {
    fn new(file: File, path: PathBuf) -> Self {
        Self {
            file: Arc::new(file),
            permit: Arc::new(Mutex::new(path)),
            unseen: Arc::default(),
        }
    }

    /// Runs `op` on the file, on the blocking pool, once the operations before it ended; it gets
    /// the file's path, which it may change. Returns the error of an earlier operation that
    /// nobody awaited instead, if there is one.
    async fn run<T: Send + 'static>(
        &self,
        name: &'static str,
        op: impl FnOnce(&File, &mut PathBuf) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let permit = Arc::clone(&self.permit).lock_owned().await;
        let earlier = self
            .unseen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(err) = earlier {
            return Err(err);
        }
        let (sender, receiver) = oneshot::channel();
        let mut reply = Reply {
            sender: Some(sender),
            unseen: Arc::clone(&self.unseen),
            name,
            permit,
        };
        let mut outcome = Outcome {
            receiver,
            unseen: Arc::clone(&self.unseen),
        };
        let file = Arc::clone(&self.file);
        tokio::task::spawn_blocking(move || {
            fail_point::hold_blocking(name, &reply.permit);
            let result = op(&file, &mut reply.permit);
            reply.send(result);
        });
        (&mut outcome.receiver)
            .await
            .unwrap_or_else(|_| Err(io::Error::other(format!("{name} did not complete"))))
    }
}

#[async_trait]
impl RoomFile for NativeFile {
    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.run("file.read_at", move |file, _| {
            let mut buf = vec![0; len];
            read_exact_at(file, &mut buf, offset)?;
            Ok(buf)
        })
        .await
    }

    async fn write_at(&self, offset: u64, bytes: Vec<u8>) -> io::Result<()> {
        self.run("file.write_at", move |file, _| {
            write_all_at(file, &bytes, offset)
        })
        .await
    }

    async fn size(&self) -> io::Result<u64> {
        self.run("file.size", |file, _| Ok(file.metadata()?.len()))
            .await
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        self.run("file.set_len", move |file, _| file.set_len(len))
            .await
    }

    async fn sync_data(&self) -> io::Result<()> {
        self.run("file.sync_data", |file, _| file.sync_data()).await
    }

    async fn sync_all(&self) -> io::Result<()> {
        self.run("file.sync_all", |file, _| file.sync_all()).await
    }

    async fn rename(&mut self, to: &Path) -> io::Result<()> {
        let to = to.to_path_buf();
        self.run("file.rename", move |_, path| {
            std::fs::rename(&*path, &to)?;
            *path = to;
            Ok(())
        })
        .await
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(unix)]
fn write_all_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !bytes.is_empty() {
        match file.seek_write(bytes, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => {
                bytes = &bytes[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/native.rs"]
mod tests;
