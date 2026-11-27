//! The file layer over the Origin Private File System (OPFS) of the browser, from the dedicated
//! Worker the SDK runs in.
//!
//! A path is relative to the root of the OPFS of the origin (`navigator.storage.getDirectory()`),
//! with its names separated by `/`. Refused with [`io::ErrorKind::InvalidInput`], before
//! anything is touched, are: a path that is absolute or has `..`; one with a name that has `~`,
//! `\` or a NUL character; the path of a file that ends in `/` or in `.`, which names no file; and
//! the prefix of a listing with `~`. So the names that a browser is known not to take never reach
//! it: `/`, `.`, `..` and the empty name, in every browser, and `\`, in Chromium (NUL is refused
//! as a precaution, untried). A browser may still refuse another name with a `TypeError`, which
//! is [`io::ErrorKind::InvalidInput`] too.
//!
//! **The generations of a name.** A name is, in the OPFS, a series of entries, its generations:
//! the name itself, and after its first replacement the name followed by `~` and a number that
//! grows (`room_a.wal~1`, `room_a.wal~2`). A name holds what its newest generation holds, and it
//! exists if any of them does. A replacement never moves a file onto an existing entry, which
//! Firefox does not do atomically if the browser is cut in the middle: [`RoomFiles::rename`], and
//! the rename of an open file ([`RoomFile::rename`]), move the file to the next generation of the
//! destination, always a free entry. Each operation lists the directory to find the generations
//! of its names, comparing names exactly, and an opening opens the newest generation of its name.
//!
//! **An operation dropped by its caller.** A promise of the OPFS goes on in the browser even if
//! the future that awaits it is dropped, so every operation that awaits one (those of the
//! directory, the openings and the rename of an open file) runs in a task of its own, with all its
//! steps, until it ends. Those operations run one at a time, across every [`OpfsFiles`] and
//! [`OpfsFile`] of the Worker: the next one waits for the one its caller dropped. The other
//! operations of a file are synchronous calls of its `FileSystemSyncAccessHandle`, which end in
//! the same call.

use super::opfs_names::{self, entry, Opening, TAKEN_BY_MOVE, TAKEN_BY_OPEN, TAKEN_BY_REMOVE};
use super::{FileTaken, RoomFile, RoomFiles};
use crate::sys;
use async_trait::async_trait;
use js_sys::{IteratorNext, Promise};
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    DomException, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemReadWriteOptions, FileSystemSyncAccessHandle,
    WorkerGlobalScope,
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

/// The turn of the operations of the layer that await promises, in every [`OpfsFiles`] and
/// [`OpfsFile`] of the Worker: an operation holds it from its start to its end. It is the last
/// lock an operation takes, and nothing that holds it waits for another operation of the layer.
static TURN: Mutex<()> = Mutex::const_new(());

/// The Origin Private File System of the origin, from a dedicated Worker.
#[derive(Debug, Default, Clone, Copy)]
pub struct OpfsFiles;

/// Runs `operation` in a task of its own once the operations before it ended, and gives its
/// result. The task holds the turn until the operation ends, even if its caller stops waiting;
/// an operation whose caller stopped waiting before its turn never runs. A result that nobody
/// waits for any more is dropped as the task ends, before the next operation runs: an opening
/// that its caller dropped closes its file before then.
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

/// The directories of the names `from` and `to`, with one handle when they are the same, so
/// that a missing one fails a rename before anything is touched.
async fn directories(
    from: &[String],
    to: &[String],
) -> io::Result<(FileSystemDirectoryHandle, FileSystemDirectoryHandle)> {
    let from_dir = directory(from, false).await?;
    let to_dir = if to == from {
        from_dir.clone()
    } else {
        directory(to, false).await?
    };
    Ok((from_dir, to_dir))
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

/// The handle of the entry `entry` of `dir`, created if missing when `create`.
async fn file_handle(
    dir: &FileSystemDirectoryHandle,
    entry: &str,
    create: bool,
) -> io::Result<FileSystemFileHandle> {
    let options = FileSystemGetFileOptions::new();
    options.set_create(create);
    let what = format!("getFileHandle({entry})");
    let file = dir.get_file_handle_with_options(entry, &options);
    Ok(wait(&what, file, &[]).await?.unchecked_into())
}

/// A sync access handle on `file`, the entry `entry`: the error of a taken file if another one
/// has it and `taken` names the error of the browser.
async fn access(
    file: &FileSystemFileHandle,
    entry: &str,
    taken: &[&str],
) -> io::Result<FileSystemSyncAccessHandle> {
    let what = format!("createSyncAccessHandle({entry})");
    let access = file.create_sync_access_handle();
    Ok(wait(&what, access, taken).await?.unchecked_into())
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

/// What a replacement moves, once its first step is done: `source`, an entry of the source, to
/// `target`, the entry of the generation `generation` of the destination, free, whose older
/// generations are `replaced`.
struct Replacement {
    source: String,
    target: String,
    generation: u64,
    replaced: Vec<u64>,
}

/// The first step of the replacement of `to`, in `to_dir`, with `from`, in `from_dir`: removes
/// the generations of `from` other than `moved` (its newest, if `None`), and fails if it cannot,
/// with the value of no name changed; then finds the next generation of `to`.
async fn replace_first(
    from_dir: &FileSystemDirectoryHandle,
    from: &str,
    moved: Option<u64>,
    to_dir: &FileSystemDirectoryHandle,
    to: &str,
) -> io::Result<Replacement> {
    let mut old = generations(from_dir, from).await?;
    let moved = match moved {
        Some(moved) => moved,
        None => *old.last().ok_or_else(|| missing(from))?,
    };
    old.retain(|&generation| generation != moved);
    for generation in old {
        remove_entry(from_dir, &entry(from, generation)).await?;
    }
    let replaced = generations(to_dir, to).await?;
    let generation = opfs_names::next(&replaced)?;
    let (source, target) = (entry(from, moved), entry(to, generation));
    Ok(Replacement {
        source,
        target,
        generation,
        replaced,
    })
}

/// The second step: moves `file`, the source of `replacement`, to its target in `to_dir`.
async fn move_entry(
    file: &FileSystemFileHandle,
    to_dir: &FileSystemDirectoryHandle,
    replacement: &Replacement,
) -> io::Result<()> {
    let target = &replacement.target;
    let what = format!("move({}, {target})", replacement.source);
    let file: &MovableFile = file.unchecked_ref();
    let moved = file
        .move_to(to_dir, target)
        .map_err(|err| error(&what, &err, TAKEN_BY_MOVE))?;
    wait(&what, moved, TAKEN_BY_MOVE).await.map(drop)
}

/// The last step: removes the old generations of `to`, without failing if it cannot: one left
/// over changes no value, and the next replacement or removal of its name removes it.
async fn replace_last(to_dir: &FileSystemDirectoryHandle, to: &str, replaced: Vec<u64>) {
    for generation in replaced {
        if let Err(err) = remove_entry(to_dir, &entry(to, generation)).await {
            tracing::debug!(%err, "an old generation stays until its name is replaced");
        }
    }
}

/// The file `name` of the directory of `dirs`, opened as `how` says. Another handle on the file
/// is the error of a taken file.
async fn open_newest(dirs: Vec<String>, name: String, how: Opening) -> io::Result<OpfsFile> {
    let dir = directory(&dirs, false).await?;
    let generation = opfs_names::opened(&generations(&dir, &name).await?, &name, how)?;
    let entry = entry(&name, generation);
    let file = file_handle(&dir, &entry, how != Opening::Existing).await?;
    point("open", &entry).await;
    let access = access(&file, &entry, TAKEN_BY_OPEN).await?;
    let open = Open {
        file,
        access: Some(access),
        dirs,
        name,
        generation,
        unseen: None,
    };
    let file = OpfsFile {
        permit: Arc::default(),
        open: Rc::new(RefCell::new(open)),
    };
    if how == Opening::Emptied {
        let emptied = file.open.borrow_mut().access()?.truncate_with_f64(0.0);
        emptied.map_err(|err| error("truncate", &err, &[]))?;
    }
    Ok(file)
}

/// `None` for the error of a taken file, which another handle has.
fn unless_taken(opened: io::Result<OpfsFile>) -> io::Result<Option<OpfsFile>> {
    match opened {
        Err(err) if FileTaken::is(&err) => Ok(None),
        opened => opened.map(Some),
    }
}

#[async_trait(?Send)]
impl RoomFiles for OpfsFiles {
    type File = OpfsFile;

    /// Opens the newest generation of the name, or creates the name itself if it has none.
    async fn open_locked(&self, path: &Path) -> io::Result<Option<OpfsFile>> {
        let (dirs, name) = opfs_names::file_names(path)?;
        unless_taken(run(open_newest(dirs, name, Opening::Found)).await)
    }

    /// The OPFS has no atomic creation of a new file: it checks that the name has no
    /// generation, and then creates it, which is safe under the lock of the room, with one tab
    /// per directory.
    async fn create_locked(&self, path: &Path) -> io::Result<Option<OpfsFile>> {
        let (dirs, name) = opfs_names::file_names(path)?;
        unless_taken(run(open_newest(dirs, name, Opening::New)).await)
    }

    /// Empties the newest generation of the name.
    async fn create(&self, path: &Path) -> io::Result<OpfsFile> {
        let (dirs, name) = opfs_names::file_names(path)?;
        run(open_newest(dirs, name, Opening::Emptied)).await
    }

    async fn open_read(&self, path: &Path) -> io::Result<OpfsFile> {
        let (dirs, name) = opfs_names::file_names(path)?;
        run(open_newest(dirs, name, Opening::Existing)).await
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
        let (names, prefix) = (opfs_names::dir_names(dir)?, opfs_names::prefix(prefix)?);
        let dir = dir.to_path_buf();
        run(async move {
            let entries = entries(&directory(&names, false).await?).await?;
            let found = opfs_names::names(&entries, &prefix);
            Ok(found.into_iter().map(|name| dir.join(name)).collect())
        })
        .await
    }

    /// Finds the directories of `from` and of `to`, so that one that is missing fails it with
    /// nothing touched; removes the old generations of `from`, if any, and fails if it cannot,
    /// with the value of no name changed; moves the newest to the next generation of `to`, a free
    /// entry; and then removes the old generations of `to`, without failing if it cannot: one
    /// left over changes no value, and the next replacement or removal of its name removes it.
    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (from_dirs, from) = opfs_names::file_names(from)?;
        let (to_dirs, to) = opfs_names::file_names(to)?;
        run(async move {
            let (from_dir, to_dir) = directories(&from_dirs, &to_dirs).await?;
            let replacement = replace_first(&from_dir, &from, None, &to_dir, &to).await?;
            let file = file_handle(&from_dir, &replacement.source, false).await?;
            move_entry(&file, &to_dir, &replacement).await?;
            replace_last(&to_dir, &to, replacement.replaced).await;
            Ok(())
        })
        .await
    }

    /// Removes the generations of `path` from the oldest to the newest, so that one that fails
    /// leaves the name with its value. One that is gone already is skipped: the removal is
    /// [`io::ErrorKind::NotFound`] only if it removed none.
    async fn remove(&self, path: &Path) -> io::Result<()> {
        let (dirs, name) = opfs_names::file_names(path)?;
        run(async move {
            let dir = directory(&dirs, false).await?;
            let mut removed = false;
            for generation in generations(&dir, &name).await? {
                match remove_entry(&dir, &entry(&name, generation)).await {
                    Ok(()) => removed = true,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                    Err(err) => return Err(err),
                }
            }
            if removed {
                Ok(())
            } else {
                Err(missing(&name))
            }
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

/// A file opened by [`OpfsFiles`]: a `FileSystemSyncAccessHandle` on a generation of its name,
/// exclusive, also for reading.
///
/// Its operations run one at a time, in the order they were called, each once it has the
/// handle's permit. All but its rename are synchronous calls of the sync access handle. Its
/// rename awaits promises, so it runs in a task of its own, with the permit and then the turn of
/// the layer, until it ends; its error, if nobody awaited it, is the one of the next operation,
/// which then does not run. The handle closes its sync access handle when it is dropped, once
/// its last operation ended.
#[derive(Debug)]
pub struct OpfsFile {
    /// The permit of the operations of the file.
    permit: Arc<Mutex<()>>,
    /// What the file shares with its rename in flight, which holds it until it ends.
    open: Rc<RefCell<Open>>,
}

/// An open file of the OPFS: its entry, where it is, and its sync access handle.
#[derive(Debug)]
struct Open {
    /// The handle of its entry, which follows the file when it moves.
    file: FileSystemFileHandle,
    /// The sync access handle, which only a rename closes and reopens: `None` once a reopening
    /// failed, which leaves the file dead.
    access: Option<FileSystemSyncAccessHandle>,
    /// The names of the directory of the file.
    dirs: Vec<String>,
    /// The name of the file, in that directory.
    name: String,
    /// The generation of the name that the file is.
    generation: u64,
    /// The error of an operation that nobody awaited, for the next one.
    unseen: Option<io::Error>,
}

impl Open {
    /// The sync access handle, unless an operation that nobody awaited failed, whose error it
    /// gives once, or a rename could not reopen the file, which is then dead.
    fn access(&mut self) -> io::Result<&FileSystemSyncAccessHandle> {
        if let Some(err) = self.unseen.take() {
            return Err(err);
        }
        self.access.as_ref().ok_or_else(|| {
            let message = format!("{} is closed: a rename could not reopen it", self.name);
            io::Error::other(message)
        })
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        if let Some(access) = self.access.take() {
            access.close();
        }
    }
}

/// The result of the rename of an open file, as its caller awaits it. Dropped before the result
/// is taken, it keeps the rename's error for the next operation: the rename may have sent it
/// just before.
struct Outcome {
    receiver: oneshot::Receiver<io::Result<()>>,
    open: Rc<RefCell<Open>>,
}

impl Drop for Outcome {
    fn drop(&mut self) {
        self.receiver.close();
        if let Ok(Err(err)) = self.receiver.try_recv() {
            self.open.borrow_mut().unseen.get_or_insert(err);
        }
    }
}

/// The options of a read or a write at `offset`.
fn at(offset: u64) -> FileSystemReadWriteOptions {
    let options = FileSystemReadWriteOptions::new();
    options.set_at(offset as f64);
    options
}

impl OpfsFile {
    /// Runs `op`, a synchronous call, on the sync access handle once the operations before it
    /// ended (see [`Open::access`]).
    async fn call<T>(
        &self,
        what: &str,
        op: impl FnOnce(&FileSystemSyncAccessHandle) -> Result<T, JsValue>,
    ) -> io::Result<T> {
        let _permit = self.permit.lock().await;
        let mut open = self.open.borrow_mut();
        op(open.access()?).map_err(|err| error(what, &err, &[]))
    }
}

/// Renames the file of `open` to the name `to` of the directory of `to_dirs`, with the steps of
/// a replacement: the first one before it closes the sync access handle, so that a rename that
/// fails there leaves the file open; then it closes it, moves the file and reopens it on the
/// same handle of its entry, which followed it. A move that fails reopens the file where it was;
/// a reopening that fails leaves it closed, and every later operation fails.
async fn rename_open(open: &RefCell<Open>, to_dirs: Vec<String>, to: String) -> io::Result<()> {
    let (file, from_dirs, from, moved) = {
        let open = open.borrow();
        let (dirs, name) = (open.dirs.clone(), open.name.clone());
        (open.file.clone(), dirs, name, open.generation)
    };
    let (from_dir, to_dir) = directories(&from_dirs, &to_dirs).await?;
    let replacement = replace_first(&from_dir, &from, Some(moved), &to_dir, &to).await?;
    if let Some(access) = open.borrow_mut().access.take() {
        access.close();
    }
    point("rename.closed", &replacement.source).await;
    let moved = move_entry(&file, &to_dir, &replacement).await;
    point("rename.moved", &replacement.target).await;
    let reopened = if moved.is_ok() {
        &replacement.target
    } else {
        &replacement.source
    };
    open.borrow_mut().access = Some(access(&file, reopened, &[]).await?);
    moved?;
    {
        let mut open = open.borrow_mut();
        open.dirs = to_dirs;
        open.name = to.clone();
        open.generation = replacement.generation;
    }
    replace_last(&to_dir, &to, replacement.replaced).await;
    Ok(())
}

#[async_trait(?Send)]
impl RoomFile for OpfsFile {
    /// Repeats a read that gives less than asked, until the file ends.
    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let read = self.call("read", |access| {
            Ok(opfs_names::read_exact(offset, len, |buf, at_offset| {
                let read = access.read_with_u8_array_and_options(buf, &at(at_offset));
                read.map(|read| read as usize)
                    .map_err(|err| error("read", &err, &[]))
            }))
        });
        read.await?
    }

    async fn write_at(&self, offset: u64, bytes: Vec<u8>) -> io::Result<()> {
        let written = self.call("write", |access| {
            Ok(opfs_names::write_all(offset, &bytes, |buf, at_offset| {
                let written = access.write_with_u8_array_and_options(buf, &at(at_offset));
                written
                    .map(|written| written as usize)
                    .map_err(|err| error("write", &err, &[]))
            }))
        });
        written.await?
    }

    async fn size(&self) -> io::Result<u64> {
        let size = self.call("getSize", FileSystemSyncAccessHandle::get_size);
        Ok(size.await? as u64)
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        self.call("truncate", |access| access.truncate_with_f64(len as f64))
            .await
    }

    /// `flush()`, as [`sync_all`](Self::sync_all): the OPFS has no metadata to sync apart.
    async fn sync_data(&self) -> io::Result<()> {
        self.call("flush", FileSystemSyncAccessHandle::flush).await
    }

    async fn sync_all(&self) -> io::Result<()> {
        self.call("flush", FileSystemSyncAccessHandle::flush).await
    }

    /// Takes the handle's permit, then the turn of the layer, and runs in a task of its own the
    /// steps of [`RoomFiles::rename`], without taking the turn again: the first one before it
    /// closes the sync access handle; then it closes it, moves the file and reopens it on the
    /// same handle of its entry. A move that fails reopens the file where it was; a reopening
    /// that fails leaves it closed, and every later operation fails. `to` may not be open.
    async fn rename(&mut self, to: &Path) -> io::Result<()> {
        let (to_dirs, to) = opfs_names::file_names(to)?;
        let permit = Arc::clone(&self.permit).lock_owned().await;
        self.open.borrow_mut().access()?;
        let (sender, receiver) = oneshot::channel();
        let mut outcome = Outcome {
            receiver,
            open: Rc::clone(&self.open),
        };
        let open = Rc::clone(&self.open);
        run(async move {
            let renamed = rename_open(&open, to_dirs, to).await;
            if let Err(Err(err)) = sender.send(renamed) {
                open.borrow_mut().unseen.get_or_insert(err);
            }
            drop(permit);
            Ok(())
        })
        .await?;
        (&mut outcome.receiver)
            .await
            .unwrap_or_else(|_| Err(io::Error::other("the rename did not complete")))
    }
}

/// A point of an operation, in its task, where a unit test acts on the entry `entry`: there it
/// runs the action armed for them, if any.
#[cfg(test)]
async fn point(name: &'static str, entry: &str) {
    let armed = ARMED.with(|armed| {
        let mut armed = armed.borrow_mut();
        let found = armed
            .iter()
            .position(|(at, on, _)| *at == name && on == entry)?;
        Some(armed.remove(found).2)
    });
    if let Some(action) = armed {
        action.await;
    }
}

#[cfg(not(test))]
async fn point(_: &'static str, _: &str) {}

/// An action of a unit test, which an operation awaits at a point.
#[cfg(test)]
type Action = std::pin::Pin<Box<dyn Future<Output = ()>>>;

#[cfg(test)]
thread_local! {
    /// The actions that the unit tests armed, with their points and their entries: each runs
    /// once, the first time an operation reaches its point on its entry.
    static ARMED: RefCell<Vec<(&'static str, String, Action)>> = const { RefCell::new(Vec::new()) };
}

/// Arms `action` for the point `point` of an operation on the entry `entry`.
#[cfg(test)]
fn arm(point: &'static str, entry: &str, action: impl Future<Output = ()> + 'static) {
    let action: Action = Box::pin(action);
    ARMED.with(|armed| armed.borrow_mut().push((point, entry.to_owned(), action)));
}

#[cfg(test)]
#[path = "tests/opfs.rs"]
mod tests;
