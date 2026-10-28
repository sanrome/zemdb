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
    /// never missing metadata.
    async fn room(&self, room_id: &RoomId) -> io::Result<Option<RoomMetadata>>;

    /// Replaces the metadata of `room_id`, durably and atomically.
    async fn save_room(&self, room_id: &RoomId, metadata: &RoomMetadata) -> io::Result<()>;
}

/// The metadata in files of a directory, through a file layer: the data directory of a disk
/// engine, next to its files and with names it does not use.
///
/// - `client.lock`, whose exclusive lock the store holds while it is open;
/// - `client.meta`, the device id;
/// - `room_{id}.meta`, the metadata of a room.
///
/// A `.meta` file is JSON followed by a line with its CRC32, and is replaced atomically through
/// a temporary file (`.meta.tmp`), synced before it is renamed over the file. A file that does
/// not verify counts as missing. A temporary file is never read: the next save replaces it.
///
/// Each operation runs in a task of its own, which runs to its end even if its caller stops
/// waiting, after the operations before it: none lands after a later one, and the lock of
/// `client.lock` is released only once the last one ended.
#[derive(Debug)]
pub struct FileMetadata<F: RoomFiles = NativeFiles> {
    files: Arc<F>,
    dir: PathBuf,
    /// The handle of `client.lock` while the store is open, under the lock each operation holds.
    lock: Arc<tokio::sync::Mutex<Option<F::File>>>,
}

impl FileMetadata {
    /// The metadata in `dir`, on the file system of the process.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self::with_files(dir, NativeFiles)
    }
}

impl<F: RoomFiles> FileMetadata<F> {
    /// The metadata in `dir`, through `files`.
    pub fn with_files(dir: impl Into<PathBuf>, files: F) -> Self {
        Self {
            files: Arc::new(files),
            dir: dir.into(),
            lock: Arc::default(),
        }
    }

    /// Runs `op` on the files and the directory in a task of its own, once the operations before
    /// it ended.
    async fn run<T, Fut>(&self, op: impl FnOnce(Arc<F>, PathBuf) -> Fut) -> io::Result<T>
    where
        T: Send + 'static,
        Fut: Future<Output = io::Result<T>> + Send + 'static,
    {
        let lock = Arc::clone(&self.lock).lock_owned().await;
        let op = op(Arc::clone(&self.files), self.dir.clone());
        let task = tokio::spawn(async move {
            let _lock = lock;
            op.await
        });
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

/// The content of `client.meta`.
#[derive(Serialize, Deserialize)]
struct Device {
    device_id: u128,
}

#[async_trait]
impl<F: RoomFiles> MetadataStore for FileMetadata<F> {
    async fn open(&self) -> io::Result<u128> {
        let (lock, device_id) = self
            .run(|files, dir| async move {
                files.create_dir_all(&dir).await?;
                let lock = files.open_locked(&dir.join("client.lock")).await?;
                let lock = lock.ok_or_else(|| {
                    let message = format!("{} is held by another client", dir.display());
                    io::Error::new(io::ErrorKind::WouldBlock, message)
                })?;
                let device_id = match read::<Device>(&*files, &dir, "client.meta").await? {
                    Some(device) => device.device_id,
                    None => {
                        let device_id = uuid::Uuid::new_v4().as_u128();
                        write(&*files, &dir, "client.meta", &Device { device_id }).await?;
                        device_id
                    }
                };
                Ok((lock, device_id))
            })
            .await?;
        *self.lock.lock().await = Some(lock);
        Ok(device_id)
    }

    async fn close(&self) {
        *self.lock.lock().await = None;
    }

    async fn room(&self, room_id: &RoomId) -> io::Result<Option<RoomMetadata>> {
        let name = room_file(room_id);
        self.run(|files, dir| async move { read(&*files, &dir, &name).await })
            .await
    }

    async fn save_room(&self, room_id: &RoomId, metadata: &RoomMetadata) -> io::Result<()> {
        let (name, metadata) = (room_file(room_id), metadata.clone());
        self.run(|files, dir| async move { write(&*files, &dir, &name, &metadata).await })
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
        Ok(uuid::Uuid::new_v4().as_u128())
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
}
