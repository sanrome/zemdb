use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use zemdb_core::{RoomId, Schema, SchemaId};
use zemdb_storage::files::{NativeFiles, RoomFile, RoomFiles};

use crate::rt::{self, MaybeSend};

/// The metadata of the replica of a room, which describe all of its data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomMetadata {
    /// The incarnation of the room the replica belongs to, from `Registered`.
    pub room_incarnation: u128,
    /// The id of the schema the replica is opened with.
    pub schema_id: SchemaId,
    /// The schema the replica is opened with.
    pub schema: Schema,
}

/// Where a client keeps its durable metadata: the device id and the metadata of each room.
#[async_trait]
pub trait MetadataStore: Debug + Send + Sync {
    /// Takes the store for a client until [`close`](Self::close), and returns the device id,
    /// generating and saving one the first time. Fails if another client holds the store. An
    /// error reading the device id is an error, never a missing id.
    async fn open(&self) -> io::Result<u128>;

    /// Releases the store, so that another client can open it, once its operations in progress
    /// ended.
    async fn close(&self);

    /// The metadata of `room_id`, or `None` if it has none. An error reading them is an error,
    /// never missing metadata. While the store is open, from [`open`](Self::open) to
    /// [`close`](Self::close), what it serves is as durable as the store keeps its metadata, also
    /// after a save or a removal of them that failed; a store that is not open serves what it
    /// reads, without that guarantee.
    async fn room(&self, room_id: &RoomId) -> io::Result<Option<RoomMetadata>>;

    /// Replaces the metadata of `room_id`, durably and atomically.
    async fn save_room(&self, room_id: &RoomId, metadata: &RoomMetadata) -> io::Result<()>;

    /// Removes the metadata of `room_id`, durably, with what a cut save left. Removing missing
    /// metadata succeeds.
    async fn remove_room(&self, room_id: &RoomId) -> io::Result<()>;
}

/// The metadata in files of a directory, through a file layer: any directory, also the data
/// directory of a disk engine, next to its files and with names it does not use.
///
/// - `client.lock`, whose exclusive lock the store holds while it is open;
/// - `client.meta`, the device id;
/// - `room_{id}.meta`, the metadata of a room.
///
/// A `.meta` file is JSON followed by a line with its CRC32, and is replaced atomically through
/// a temporary file (`.meta.tmp`), synced before it is renamed over the file. A file that does
/// not verify counts as missing. A temporary file is never read: the next save replaces it, and
/// removing the metadata of its room removes it.
///
/// While the store is open, the metadata of a room are durable before they are served: the
/// first time since the store opened, and after a save or a removal of them that failed, at any
/// step. Where a sync of the directory makes durable what already happened
/// (`sync_dir_covers_earlier_changes`), as on the file system of the process, the store syncs
/// the directory and its parent when it opens, and the directory before it serves a room after
/// a failure. Where it does not, as on the Origin Private File System, it saves again what it
/// reads before it serves it, and repeats a removal that failed, serving no metadata. If it
/// cannot, reading the room fails, so the room does not open.
///
/// Each operation runs in a task of its own, which runs to its end even if its caller stops
/// waiting, after the operations before it: none lands after a later one, and the lock of
/// `client.lock` is released only once the last one ended.
#[derive(Debug)]
pub struct FileMetadata<F: RoomFiles = NativeFiles> {
    files: Arc<F>,
    dir: PathBuf,
    state: Arc<tokio::sync::Mutex<State<F>>>,
}

/// What the operations of a [`FileMetadata`] share, under the lock each one holds while it runs
/// in its task.
#[derive(Debug)]
struct State<F: RoomFiles> {
    /// The handle of `client.lock` while the store is open.
    lock: Option<F::File>,
    /// What the store knows of the metadata of each room since it opened, by the name of their
    /// file, which holds while it is open. Those of a room it knows nothing of are durable where
    /// a sync of the directory makes durable what already happened, since the open synced it;
    /// elsewhere, they are served for the first time.
    rooms: HashMap<String, Known>,
}

/// What a [`FileMetadata`] knows of the metadata of a room. A save or a removal marks them failed
/// before its first step, and durable once it succeeded, in its task, which runs to its end even
/// if its caller stopped waiting: an error at any step, or a panic, leaves them marked failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Known {
    /// Saved, removed or made durable since the store opened.
    Durable,
    /// A save of them failed.
    SaveFailed,
    /// A removal of them failed.
    RemovalFailed,
}

/// The lock of the [`State`] of a [`FileMetadata`], which an operation holds in its task.
type Held<F> = tokio::sync::OwnedMutexGuard<State<F>>;

impl FileMetadata {
    /// The metadata in `dir`, on the file system of the process.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self::with_files(dir, NativeFiles)
    }
}

impl<F: RoomFiles> FileMetadata<F> {
    /// The metadata in `dir`, through `files`.
    pub fn with_files(dir: impl Into<PathBuf>, files: F) -> Self {
        let state = State {
            lock: None,
            rooms: HashMap::new(),
        };
        Self {
            files: Arc::new(files),
            dir: dir.into(),
            state: Arc::new(tokio::sync::Mutex::new(state)),
        }
    }

    /// Runs `op` on the files, the directory and the state of the store in a task of its own,
    /// once the operations before it ended: the task holds the state until `op` ends.
    async fn run<T, Fut>(&self, op: impl FnOnce(Arc<F>, PathBuf, Held<F>) -> Fut) -> io::Result<T>
    where
        T: MaybeSend + 'static,
        Fut: Future<Output = io::Result<T>> + MaybeSend + 'static,
    {
        let state = Arc::clone(&self.state).lock_owned().await;
        let task = rt::spawn(op(Arc::clone(&self.files), self.dir.clone(), state));
        task.await
            .map_err(|err| io::Error::other(format!("Metadata task failed: {err}")))?
    }
}

/// The name of the metadata file of `room_id`.
fn room_file(room_id: &RoomId) -> String {
    format!("room_{room_id}.meta")
}

/// The value saved in the file `name` of `dir`, or `None` if there is none or it does not verify.
async fn read<T: DeserializeOwned>(
    files: &impl RoomFiles,
    dir: &Path,
    name: &str,
) -> io::Result<Option<T>> {
    let file = match files.open_read(&dir.join(name)).await {
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        file => file?,
    };
    let len = usize::try_from(file.size().await?).map_err(io::Error::other)?;
    let bytes = file.read_at(0, len).await?;
    Ok(verified_json(&bytes).and_then(|json| serde_json::from_str(json).ok()))
}

/// Replaces the file `name` of `dir` with `value`.
async fn write(
    files: &impl RoomFiles,
    dir: &Path,
    name: &str,
    value: &impl Serialize,
) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(format!("\ncrc32:{crc:08x}\n").as_bytes());
    let tmp = dir.join(format!("{name}.tmp"));
    let file = files.create(&tmp).await?;
    file.write_at(0, bytes).await?;
    file.sync_all().await?;
    // A file is renamed by path with no handle open (the Origin Private File System refuses to
    // move one that has).
    drop(file);
    files.rename(&tmp, &dir.join(name)).await?;
    files.sync_dir(dir).await
}

/// Removes the file `name` of `dir` and its temporary file, if they are there.
async fn remove(files: &impl RoomFiles, dir: &Path, name: &str) -> io::Result<()> {
    for path in [dir.join(name), dir.join(format!("{name}.tmp"))] {
        match files.remove(&path).await {
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            removed => removed?,
        }
    }
    files.sync_dir(dir).await
}

/// The content of `client.meta`.
#[derive(Serialize, Deserialize)]
struct Device {
    device_id: u128,
}

#[async_trait]
impl<F: RoomFiles> MetadataStore for FileMetadata<F> {
    async fn open(&self) -> io::Result<u128> {
        let (lock, device_id) = self
            .run(|files, dir, mut state| async move {
                files.create_dir_all(&dir).await?;
                let lock = files.open_locked(&dir.join("client.lock")).await?;
                let lock = lock.ok_or_else(|| {
                    let message = format!("{} is held by another client", dir.display());
                    io::Error::new(io::ErrorKind::WouldBlock, message)
                })?;
                // Where it does, this sync makes durable what the operations before this open
                // left, those that failed included, before anything is served, and so does the
                // sync of the parent for the entry of the directory, whose creation may not have
                // been synced; elsewhere, each room is saved again the first time it is served.
                // So the store forgets its failures, which another store over the same directory
                // never knew.
                for synced in [Some(dir.as_path()), dir.parent()].into_iter().flatten() {
                    files.sync_dir(synced).await?;
                }
                state.rooms.clear();
                let device_id = match read::<Device>(&*files, &dir, "client.meta").await? {
                    Some(device) => device.device_id,
                    None => {
                        let device_id = rt::random_uuid();
                        write(&*files, &dir, "client.meta", &Device { device_id }).await?;
                        device_id
                    }
                };
                Ok((lock, device_id))
            })
            .await?;
        self.state.lock().await.lock = Some(lock);
        Ok(device_id)
    }

    async fn close(&self) {
        self.state.lock().await.lock = None;
    }

    async fn room(&self, room_id: &RoomId) -> io::Result<Option<RoomMetadata>> {
        let name = room_file(room_id);
        self.run(|files, dir, mut state| async move {
            let known = state.rooms.get(&name).copied();
            let metadata = if files.sync_dir_covers_earlier_changes() {
                if known.is_some_and(|known| known != Known::Durable) {
                    files.sync_dir(&dir).await?;
                }
                read(&*files, &dir, &name).await?
            } else if known == Some(Known::RemovalFailed) {
                remove(&*files, &dir, &name).await?;
                None
            } else {
                let metadata = read::<RoomMetadata>(&*files, &dir, &name).await?;
                let unknown = known != Some(Known::Durable);
                if let Some(metadata) = metadata.as_ref().filter(|_| unknown) {
                    write(&*files, &dir, &name, metadata).await?;
                }
                metadata
            };
            state.rooms.insert(name, Known::Durable);
            Ok(metadata)
        })
        .await
    }

    async fn save_room(&self, room_id: &RoomId, metadata: &RoomMetadata) -> io::Result<()> {
        let (name, metadata) = (room_file(room_id), metadata.clone());
        self.run(|files, dir, mut state| async move {
            state.rooms.insert(name.clone(), Known::SaveFailed);
            write(&*files, &dir, &name, &metadata).await?;
            state.rooms.insert(name, Known::Durable);
            Ok(())
        })
        .await
    }

    async fn remove_room(&self, room_id: &RoomId) -> io::Result<()> {
        let name = room_file(room_id);
        self.run(|files, dir, mut state| async move {
            state.rooms.insert(name.clone(), Known::RemovalFailed);
            remove(&*files, &dir, &name).await?;
            state.rooms.insert(name, Known::Durable);
            Ok(())
        })
        .await
    }
}

/// The JSON of a file written by [`FileMetadata`], or `None` if it is not UTF-8 or its CRC32
/// line is missing or does not match.
fn verified_json(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?.strip_suffix('\n')?;
    let (json, crc) = text.rsplit_once("\ncrc32:")?;
    (crc == format!("{:08x}", crc32fast::hash(json.as_bytes()))).then_some(json)
}

/// The metadata in memory, for a client over a `MemoryStorageEngine`: they last as long as the
/// store, which has a new device id each time it is opened.
#[derive(Debug, Default)]
pub struct MemoryMetadata {
    rooms: Mutex<HashMap<RoomId, RoomMetadata>>,
}

#[async_trait]
impl MetadataStore for MemoryMetadata {
    async fn open(&self) -> io::Result<u128> {
        Ok(rt::random_uuid())
    }

    async fn close(&self) {}

    async fn room(&self, room_id: &RoomId) -> io::Result<Option<RoomMetadata>> {
        let rooms = self.rooms.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(rooms.get(room_id).cloned())
    }

    async fn save_room(&self, room_id: &RoomId, metadata: &RoomMetadata) -> io::Result<()> {
        let mut rooms = self.rooms.lock().unwrap_or_else(PoisonError::into_inner);
        rooms.insert(room_id.clone(), metadata.clone());
        Ok(())
    }

    async fn remove_room(&self, room_id: &RoomId) -> io::Result<()> {
        let mut rooms = self.rooms.lock().unwrap_or_else(PoisonError::into_inner);
        rooms.remove(room_id);
        Ok(())
    }
}
