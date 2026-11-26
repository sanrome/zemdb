//! The file layer over the Origin Private File System (OPFS) of the browser, from the dedicated
//! Worker the SDK runs in.
//!
//! A path is relative to the root of the OPFS of the origin (`navigator.storage.getDirectory()`),
//! with its names separated by `/`. One that is absolute, has `..` or has a name with `~` is
//! refused, with [`io::ErrorKind::InvalidInput`], before anything is touched.
//!
//! **The generations of a name.** A name is, in the OPFS, a series of entries, its generations:
//! the name itself, and after its first replacement the name followed by `~` and a number that
//! grows (`room_a.wal~1`, `room_a.wal~2`). A name holds what its newest generation holds, and it
//! exists if any of them does. A replacement never moves a file onto an existing entry, which
//! Firefox does not do atomically if the browser is cut in the middle: [`RoomFiles::rename`]
//! moves the newest generation of the source to the next generation of the destination, always
//! a free entry. Each operation lists the directory to find the generations of its names,
//! comparing names exactly.
//!
//! **An operation dropped by its caller.** A promise of the OPFS goes on in the browser even if
//! the future that awaits it is dropped, so every operation runs in a task of its own, with all
//! its steps, until it ends. The operations of the layer run one at a time, across every
//! [`OpfsFiles`] of the Worker: the next one waits for the one its caller dropped.

use super::opfs_names::{self, entry, TAKEN_BY_MOVE, TAKEN_BY_REMOVE};
use super::{FileTaken, RoomFile, RoomFiles};
use crate::sys;
use async_trait::async_trait;
use js_sys::{IteratorNext, Promise};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    DomException, FileSystemDirectoryHandle, FileSystemGetDirectoryOptions, WorkerGlobalScope,
};

#[wasm_bindgen]
extern "C" {
    /// A `FileSystemFileHandle`, with the `move()` that `web-sys` does not have.
    #[wasm_bindgen(js_name = FileSystemFileHandle)]
    type MovableFile;

    /// `move(directory, name)`, the only form that Safari has.
    #[wasm_bindgen(method, catch, js_name = "move")]
    fn move_to(
        this: &MovableFile,
        dir: &FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<Promise, JsValue>;
}

/// The turn of the operations of the layer, in every [`OpfsFiles`] of the Worker: an operation
/// holds it from its start to its end.
static TURN: Mutex<()> = Mutex::const_new(());

/// The Origin Private File System of the origin, from a dedicated Worker.
///
/// It does not open files yet: [`RoomFiles::open_locked`], [`RoomFiles::create_locked`],
/// [`RoomFiles::create`] and [`RoomFiles::open_read`] fail with
/// [`io::ErrorKind::Unsupported`].
#[derive(Debug, Default, Clone, Copy)]
pub struct OpfsFiles;

/// Runs `operation` in a task of its own once the operations before it ended, and gives its
/// result. The task holds the turn until the operation ends, even if its caller stops waiting;
/// an operation whose caller stopped waiting before its turn never runs.
async fn run<T: 'static>(
    operation: impl Future<Output = io::Result<T>> + 'static,
) -> io::Result<T> {
    let turn = TURN.lock().await;
    sys::spawn(async move {
        let result = operation.await;
        drop(turn);
        result
    })
    .await
    .unwrap_or_else(|_| {
        Err(io::Error::other(
            "an operation on the OPFS did not complete",
        ))
    })
}

/// The error `err` of `what`, a call to the OPFS: of the kind of its name, which the message
/// gives, and the error of a taken file if its name is one of `taken`.
fn error(what: &str, err: &JsValue, taken: &[&str]) -> io::Error {
    let (name, message) = if let Some(err) = err.dyn_ref::<DomException>() {
        (err.name(), err.message())
    } else if let Some(err) = err.dyn_ref::<js_sys::Error>() {
        (String::from(err.name()), String::from(err.message()))
    } else {
        (String::new(), format!("{err:?}"))
    };
    let message = format!("{what}: {name}: {message}");
    if taken.contains(&name.as_str()) {
        return FileTaken::error(message);
    }
    io::Error::new(opfs_names::error_kind(&name), message)
}

/// The value of `promise`, a call `what` to the OPFS, or its error (see [`error`]).
async fn wait(what: &str, promise: Promise, taken: &[&str]) -> io::Result<JsValue> {
    JsFuture::from(promise)
        .await
        .map_err(|err| error(what, &err, taken))
}

/// The directory of `names` under the root, each created if missing when `create`.
async fn directory(names: &[String], create: bool) -> io::Result<FileSystemDirectoryHandle> {
    let scope: WorkerGlobalScope = js_sys::global().dyn_into().map_err(|_| {
        io::Error::new(io::ErrorKind::Unsupported, "the OPFS is used from a Worker")
    })?;
    let root = scope.navigator().storage().get_directory();
    let mut dir: FileSystemDirectoryHandle =
        wait("getDirectory", root, &[]).await?.unchecked_into();
    let options = FileSystemGetDirectoryOptions::new();
    options.set_create(create);
    for name in names {
        let next = dir.get_directory_handle_with_options(name, &options);
        let what = format!("getDirectoryHandle({name})");
        dir = wait(&what, next, &[]).await?.unchecked_into();
    }
    Ok(dir)
}

/// The names of the entries of `dir`.
async fn entries(dir: &FileSystemDirectoryHandle) -> io::Result<Vec<String>> {
    let keys = dir.keys();
    let mut entries = Vec::new();
    loop {
        let next = keys.next().map_err(|err| error("keys", &err, &[]))?;
        let next: IteratorNext = wait("keys", next, &[]).await?.unchecked_into();
        if next.done() {
            return Ok(entries);
        }
        let name = next.value().as_string();
        entries.push(name.ok_or_else(|| io::Error::other("keys gave a name that is no string"))?);
    }
}

/// The generations of `name` in `dir`, from the oldest to the newest.
async fn generations(dir: &FileSystemDirectoryHandle, name: &str) -> io::Result<Vec<u64>> {
    let entries = entries(dir).await?;
    Ok(opfs_names::generations(
        entries.iter().map(String::as_str),
        name,
    ))
}

async fn remove_entry(dir: &FileSystemDirectoryHandle, entry: &str) -> io::Result<()> {
    let what = format!("removeEntry({entry})");
    wait(&what, dir.remove_entry(entry), TAKEN_BY_REMOVE)
        .await
        .map(drop)
}

fn missing(name: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("{name} does not exist"))
}

fn unsupported(path: &Path) -> io::Error {
    let message = format!("the OPFS layer does not open {path:?}: it opens no file yet");
    io::Error::new(io::ErrorKind::Unsupported, message)
}

#[async_trait(?Send)]
impl RoomFiles for OpfsFiles {
    type File = OpfsFile;

    async fn open_locked(&self, path: &Path) -> io::Result<Option<OpfsFile>> {
        Err(unsupported(path))
    }

    async fn create_locked(&self, path: &Path) -> io::Result<Option<OpfsFile>> {
        Err(unsupported(path))
    }

    async fn create(&self, path: &Path) -> io::Result<OpfsFile> {
        Err(unsupported(path))
    }

    async fn open_read(&self, path: &Path) -> io::Result<OpfsFile> {
        Err(unsupported(path))
    }

    async fn exists(&self, path: &Path) -> io::Result<bool> {
        let (dirs, name) = opfs_names::file_names(path)?;
        run(async move {
            let dir = match directory(&dirs, false).await {
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
                dir => dir?,
            };
            Ok(!generations(&dir, &name).await?.is_empty())
        })
        .await
    }

    async fn list(&self, dir: &Path, prefix: &str) -> io::Result<Vec<PathBuf>> {
        let names = opfs_names::dir_names(dir)?;
        let (dir, prefix) = (dir.to_path_buf(), prefix.to_owned());
        run(async move {
            let entries = entries(&directory(&names, false).await?).await?;
            let found = opfs_names::names(&entries, &prefix);
            Ok(found.into_iter().map(|name| dir.join(name)).collect())
        })
        .await
    }

    /// Removes the old generations of `from`, if any, and fails if it cannot, with the value of
    /// no name changed; moves the newest to the next generation of `to`, a free entry; and then
    /// removes the old generations of `to`, without failing if it cannot: one left over changes
    /// no value, and the next replacement or removal of its name removes it.
    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (from_dirs, from) = opfs_names::file_names(from)?;
        let (to_dirs, to) = opfs_names::file_names(to)?;
        run(async move {
            let from_dir = directory(&from_dirs, false).await?;
            let mut old = generations(&from_dir, &from).await?;
            let newest = entry(&from, old.pop().ok_or_else(|| missing(&from))?);
            for generation in old {
                remove_entry(&from_dir, &entry(&from, generation)).await?;
            }
            let to_dir = directory(&to_dirs, false).await?;
            let replaced = generations(&to_dir, &to).await?;
            let target = entry(&to, opfs_names::next(&replaced)?);
            let what = format!("getFileHandle({newest})");
            let file: MovableFile = wait(&what, from_dir.get_file_handle(&newest), &[])
                .await?
                .unchecked_into();
            let what = format!("move({newest}, {target})");
            let moved = file
                .move_to(&to_dir, &target)
                .map_err(|err| error(&what, &err, TAKEN_BY_MOVE))?;
            wait(&what, moved, TAKEN_BY_MOVE).await?;
            for generation in replaced {
                if let Err(err) = remove_entry(&to_dir, &entry(&to, generation)).await {
                    tracing::debug!(%err, "an old generation stays until its name is replaced");
                }
            }
            Ok(())
        })
        .await
    }

    /// Removes the generations of `path` from the oldest to the newest, so that one that fails
    /// leaves the name with its value.
    async fn remove(&self, path: &Path) -> io::Result<()> {
        let (dirs, name) = opfs_names::file_names(path)?;
        run(async move {
            let dir = directory(&dirs, false).await?;
            let generations = generations(&dir, &name).await?;
            if generations.is_empty() {
                return Err(missing(&name));
            }
            for generation in generations {
                remove_entry(&dir, &entry(&name, generation)).await?;
            }
            Ok(())
        })
        .await
    }

    /// Each new entry is durable once its promise resolves.
    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let names = opfs_names::dir_names(path)?;
        run(async move { directory(&names, true).await.map(drop) }).await
    }

    /// Does nothing: an entry created, moved or removed is durable once its promise resolves.
    async fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        opfs_names::dir_names(dir).map(drop)
    }

    fn sync_dir_covers_earlier_changes(&self) -> bool {
        false
    }
}

/// A file opened by [`OpfsFiles`], which opens none yet.
#[derive(Debug)]
pub struct OpfsFile {
    never: Infallible,
}

#[async_trait(?Send)]
impl RoomFile for OpfsFile {
    async fn read_at(&self, _: u64, _: usize) -> io::Result<Vec<u8>> {
        match self.never {}
    }

    async fn write_at(&self, _: u64, _: Vec<u8>) -> io::Result<()> {
        match self.never {}
    }

    async fn size(&self) -> io::Result<u64> {
        match self.never {}
    }

    async fn set_len(&self, _: u64) -> io::Result<()> {
        match self.never {}
    }

    async fn sync_data(&self) -> io::Result<()> {
        match self.never {}
    }

    async fn sync_all(&self) -> io::Result<()> {
        match self.never {}
    }

    async fn rename(&mut self, _: &Path) -> io::Result<()> {
        match self.never {}
    }
}
