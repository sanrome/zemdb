//! The disk engine on a file layer that refuses what the Origin Private File System refuses
//! (spike S3 of DC17): moving or removing a file with a handle open, at the source or at the
//! destination, read-only handles included. It renames an open file as a layer on the OPFS will:
//! it closes the handle, moves the file and opens it again. Its paths are virtual, mapped to a
//! real directory, and nothing exists at them: an operation of the engine on its files that does
//! not go through the layer fails.

use super::snapshot_with_rows;
use super::support::*;
use crate::disk::compactor::{absorbing_wal_path, compacting_wal_path};
use crate::fail_point;
use crate::files::{NativeFile, NativeFiles, RoomFile, RoomFiles};
use crate::{DiskStorageEngine, DiskStorageOptions, StorageEngine, StorageError};
use async_trait::async_trait;
use std::collections::{BTreeSet, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use zemdb_core::{RoomId, SequenceNumber};

/// How a file was opened, so that it is opened the same way again after a move.
#[derive(Debug, Clone, Copy)]
enum Mode {
    Locked,
    Write,
    Read,
}

/// The handles open on each virtual path, what the layer refused, and every file opened or
/// created, with the operation that did it.
#[derive(Debug, Default)]
struct Handles {
    open: HashMap<PathBuf, usize>,
    refused: Vec<String>,
    opened: Vec<(PathBuf, &'static str)>,
}

#[derive(Debug, Clone)]
struct OpfsLikeFiles {
    native: NativeFiles,
    virtual_root: PathBuf,
    real_root: PathBuf,
    handles: Arc<Mutex<Handles>>,
}

impl OpfsLikeFiles {
    /// A layer whose virtual root is `dir/virtual`, mapped to `dir/real`.
    fn new(dir: &Path) -> Self {
        let real_root = dir.join("real");
        std::fs::create_dir(&real_root).unwrap();
        Self {
            native: NativeFiles,
            virtual_root: dir.join("virtual"),
            real_root,
            handles: Arc::default(),
        }
    }

    /// The real path of the virtual `path`.
    fn real(&self, path: &Path) -> PathBuf {
        let relative = path
            .strip_prefix(&self.virtual_root)
            .unwrap_or_else(|_| panic!("{path:?} is outside the layer's root"));
        self.real_root.join(relative)
    }

    fn handles(&self) -> MutexGuard<'_, Handles> {
        self.handles.lock().unwrap()
    }

    fn refused(&self) -> Vec<String> {
        self.handles().refused.clone()
    }

    /// Fails as the OPFS does if `path` has more than `own` handles open.
    fn refuse_if_open(&self, what: &str, path: &Path, own: usize) -> io::Result<()> {
        let mut handles = self.handles();
        if handles.open.get(path).copied().unwrap_or(0) > own {
            let refusal = format!("NoModificationAllowedError: {what} {path:?}, open elsewhere");
            handles.refused.push(refusal.clone());
            return Err(io::Error::other(refusal));
        }
        Ok(())
    }

    fn register(&self, native: NativeFile, path: &Path, mode: Mode) -> OpfsLikeFile {
        *self.handles().open.entry(path.to_path_buf()).or_default() += 1;
        OpfsLikeFile {
            native: Some(native),
            path: path.to_path_buf(),
            mode,
            layer: self.clone(),
        }
    }

    /// Opens the real file of `path` again as `mode` opened it.
    async fn reopen(&self, path: &Path, mode: Mode) -> io::Result<NativeFile> {
        let real = self.real(path);
        match mode {
            Mode::Locked => self
                .native
                .open_locked(&real)
                .await?
                .ok_or_else(|| io::Error::other("the lock was taken while the file was closed")),
            Mode::Read => self.native.open_read(&real).await,
            Mode::Write => Err(io::Error::other(
                "a file created truncating is not reopened",
            )),
        }
    }
}

#[async_trait]
impl RoomFiles for OpfsLikeFiles {
    type File = OpfsLikeFile;

    async fn open_locked(&self, path: &Path) -> io::Result<Option<OpfsLikeFile>> {
        self.handles()
            .opened
            .push((path.to_path_buf(), "open_locked"));
        let native = self.native.open_locked(&self.real(path)).await?;
        Ok(native.map(|native| self.register(native, path, Mode::Locked)))
    }

    async fn create_locked(&self, path: &Path) -> io::Result<Option<OpfsLikeFile>> {
        self.handles()
            .opened
            .push((path.to_path_buf(), "create_locked"));
        let native = self.native.create_locked(&self.real(path)).await?;
        Ok(native.map(|native| self.register(native, path, Mode::Locked)))
    }

    async fn create(&self, path: &Path) -> io::Result<OpfsLikeFile> {
        self.handles().opened.push((path.to_path_buf(), "create"));
        let native = self.native.create(&self.real(path)).await?;
        Ok(self.register(native, path, Mode::Write))
    }

    async fn open_read(&self, path: &Path) -> io::Result<OpfsLikeFile> {
        self.handles()
            .opened
            .push((path.to_path_buf(), "open_read"));
        let native = self.native.open_read(&self.real(path)).await?;
        Ok(self.register(native, path, Mode::Read))
    }

    async fn exists(&self, path: &Path) -> io::Result<bool> {
        self.native.exists(&self.real(path)).await
    }

    async fn list(&self, dir: &Path, prefix: &str) -> io::Result<Vec<PathBuf>> {
        let found = self.native.list(&self.real(dir), prefix).await?;
        Ok(found
            .into_iter()
            .map(|real| {
                self.virtual_root
                    .join(real.strip_prefix(&self.real_root).unwrap())
            })
            .collect())
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.refuse_if_open("cannot move", from, 0)?;
        self.refuse_if_open("cannot move to", to, 0)?;
        self.native.rename(&self.real(from), &self.real(to)).await
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        self.refuse_if_open("cannot remove", path, 0)?;
        self.native.remove(&self.real(path)).await
    }

    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.native.create_dir_all(&self.real(path)).await
    }

    async fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.native.sync_dir(&self.real(dir)).await
    }
}

#[derive(Debug)]
struct OpfsLikeFile {
    /// `None` once the file could not be opened again after a move: the handle is dead.
    native: Option<NativeFile>,
    path: PathBuf,
    mode: Mode,
    layer: OpfsLikeFiles,
}

impl OpfsLikeFile {
    fn native(&self) -> io::Result<&NativeFile> {
        self.native
            .as_ref()
            .ok_or_else(|| io::Error::other("the handle was closed by a move"))
    }

    fn close(&mut self) {
        if self.native.take().is_some() {
            let mut handles = self.layer.handles();
            let open = handles.open.get_mut(&self.path).unwrap();
            *open -= 1;
            if *open == 0 {
                handles.open.remove(&self.path);
            }
        }
    }
}

impl Drop for OpfsLikeFile {
    fn drop(&mut self) {
        self.close();
    }
}

#[async_trait]
impl RoomFile for OpfsLikeFile {
    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.native()?.read_at(offset, len).await
    }

    async fn write_at(&self, offset: u64, bytes: Vec<u8>) -> io::Result<()> {
        self.native()?.write_at(offset, bytes).await
    }

    async fn size(&self) -> io::Result<u64> {
        self.native()?.size().await
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        self.native()?.set_len(len).await
    }

    async fn sync_data(&self) -> io::Result<()> {
        self.native()?.sync_data().await
    }

    async fn sync_all(&self) -> io::Result<()> {
        self.native()?.sync_all().await
    }

    /// Closes the handle, moves the file and opens it again where it is (at `to`, unless the move
    /// failed). If it cannot be opened again, the handle is dead.
    async fn rename(&mut self, to: &Path) -> io::Result<()> {
        self.native()?;
        self.layer.refuse_if_open("cannot move", &self.path, 1)?;
        self.layer.refuse_if_open("cannot move to", to, 0)?;
        self.close();
        let layer = self.layer.clone();
        let moved = layer
            .native
            .rename(&layer.real(&self.path), &layer.real(to))
            .await;
        if moved.is_ok() {
            self.path = to.to_path_buf();
        }
        let native = match layer.reopen(&self.path, self.mode).await {
            Ok(native) => native,
            Err(err) => return Err(moved.err().unwrap_or(err)),
        };
        *layer.handles().open.entry(self.path.clone()).or_default() += 1;
        self.native = Some(native);
        moved
    }
}

/// Opens a file of `layer` at `path`, which exists, as `mode`.
async fn open_as(layer: &OpfsLikeFiles, path: &Path, mode: Mode) -> OpfsLikeFile {
    match mode {
        Mode::Locked => layer.open_locked(path).await.unwrap().unwrap(),
        Mode::Write => layer.create(path).await.unwrap(),
        Mode::Read => layer.open_read(path).await.unwrap(),
    }
}

#[tokio::test]
async fn the_layer_refuses_to_move_or_remove_a_file_with_a_handle_open() {
    let dir = tempfile::tempdir().unwrap();
    let layer = OpfsLikeFiles::new(dir.path());
    let (a, b) = (layer.virtual_root.join("a"), layer.virtual_root.join("b"));
    drop(layer.create(&a).await.unwrap());
    drop(layer.create(&b).await.unwrap());

    for mode in [Mode::Locked, Mode::Write, Mode::Read] {
        let handle = open_as(&layer, &a, mode).await;
        assert!(layer.rename(&a, &b).await.is_err(), "{mode:?}");
        assert!(layer.rename(&b, &a).await.is_err(), "{mode:?}");
        assert!(layer.remove(&a).await.is_err(), "{mode:?}");
        drop(handle);
        layer.rename(&a, &b).await.unwrap();
        layer.rename(&b, &a).await.unwrap();
    }
    assert_eq!(layer.refused().len(), 9);
    layer.remove(&a).await.unwrap();
    assert!(!layer.exists(&a).await.unwrap() && !layer.exists(&b).await.unwrap());
    assert!(!layer.virtual_root.exists());
}

#[tokio::test]
async fn the_layer_renames_an_open_file_by_closing_moving_and_reopening_it() {
    let dir = tempfile::tempdir().unwrap();
    let layer = OpfsLikeFiles::new(dir.path());
    let (a, b) = (layer.virtual_root.join("a"), layer.virtual_root.join("b"));
    let mut file = layer.open_locked(&a).await.unwrap().unwrap();
    file.write_at(0, b"wal".to_vec()).await.unwrap();

    // Another handle at the source or at the destination refuses the move.
    let reader = layer.open_read(&a).await.unwrap();
    assert!(file.rename(&b).await.is_err());
    drop(reader);
    drop(layer.create(&b).await.unwrap());
    let reader = layer.open_read(&b).await.unwrap();
    assert!(file.rename(&b).await.is_err());
    drop(reader);
    assert_eq!(layer.refused().len(), 2);
    assert!(layer.open_locked(&a).await.unwrap().is_none());

    file.rename(&b).await.unwrap();
    assert!(!layer.exists(&a).await.unwrap());
    assert_eq!(file.read_at(0, 3).await.unwrap(), b"wal");
    assert!(
        layer.open_locked(&b).await.unwrap().is_none(),
        "reopened without its lock"
    );

    // A move that fails opens the file again where it was.
    let nowhere = layer.virtual_root.join("missing").join("c");
    assert!(file.rename(&nowhere).await.is_err());
    assert_eq!(file.read_at(0, 3).await.unwrap(), b"wal");
    assert!(layer.open_locked(&b).await.unwrap().is_none());
    assert_eq!(layer.refused().len(), 2);
}

/// A disk engine on `layer`, with its data directory under the layer's root, that compacts only
/// when asked to.
fn engine_on(layer: &OpfsLikeFiles) -> DiskStorageEngine<OpfsLikeFiles> {
    let options = DiskStorageOptions::new(layer.virtual_root.join("data")).auto_compact(false);
    DiskStorageEngine::with_files(options, layer.clone()).unwrap()
}

async fn open_on(layer: &OpfsLikeFiles, room: &RoomId) -> DiskStorageEngine<OpfsLikeFiles> {
    let engine = engine_on(layer);
    engine.open_room(room, schema()).await.unwrap();
    engine
}

async fn reopen_on(
    engine: DiskStorageEngine<OpfsLikeFiles>,
    layer: &OpfsLikeFiles,
    room: &RoomId,
) -> DiskStorageEngine<OpfsLikeFiles> {
    engine.close_room(room).await.unwrap();
    drop(engine);
    open_on(layer, room).await
}

/// How the engine opens or creates each of its files (DC17): the lock file and the WAL with their
/// lock, creating them if missing; the fresh WAL of a rotation and the staged snapshots with their
/// lock, never adopting a file; the copy of an absorption truncating; the snapshot and the rotated
/// WAL for reading.
fn expected_opening(path: &Path) -> &'static [&'static str] {
    let name = path.file_name().unwrap().to_str().unwrap();
    if name.ends_with(".lock") {
        &["open_locked"]
    } else if name.ends_with(".wal") {
        &["open_locked", "create_locked"]
    } else if name.contains(".snap.tmp.") {
        &["create_locked"]
    } else if name.ends_with(".wal.compacting.tmp") {
        &["create"]
    } else if name.ends_with(".snap") || name.ends_with(".wal.compacting") {
        &["open_read"]
    } else {
        panic!("the engine opened {path:?}")
    }
}

/// Closes `room` and checks that the layer refused nothing, that no handle is left open, that
/// every file was opened as `expected_opening` says, that nothing exists at a virtual path, and
/// that the room's directory holds its lock file, its snapshot and its WAL, and nothing else.
async fn assert_clean(
    engine: DiskStorageEngine<OpfsLikeFiles>,
    layer: &OpfsLikeFiles,
    room: &RoomId,
) {
    engine.close_room(room).await.unwrap();
    let expected: BTreeSet<PathBuf> = [
        engine.lock_file_path(room),
        engine.snap_file_path(room),
        engine.wal_file_path(room),
    ]
    .iter()
    .map(|path| layer.real(path))
    .collect();
    let data_dir = layer.real(&layer.virtual_root.join("data"));
    drop(engine);

    assert_eq!(layer.refused(), Vec::<String>::new());
    assert!(
        layer.handles().open.is_empty(),
        "{:?}",
        layer.handles().open
    );
    for (path, operation) in &layer.handles().opened {
        assert!(
            expected_opening(path).contains(operation),
            "{path:?} opened with {operation}"
        );
    }
    assert!(!layer.virtual_root.exists());
    let found: BTreeSet<PathBuf> = std::fs::read_dir(data_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(found, expected);
}

#[tokio::test]
async fn compaction_runs_on_a_layer_that_fails_like_the_opfs() {
    let dir = tempfile::tempdir().unwrap();
    let layer = OpfsLikeFiles::new(dir.path());
    let room = RoomId::new("opfs-compaction").unwrap();
    let engine = open_on(&layer, &room).await;

    apply_range(&engine, &room, 1..=5).await;
    engine.compact_room(&room).await.unwrap();
    apply_range(&engine, &room, 6..=8).await;
    engine.compact_room(&room).await.unwrap();
    apply_range(&engine, &room, 9..=9).await;

    let engine = reopen_on(engine, &layer, &room).await;
    assert_rows(&engine, &room, 9).await;
    assert_clean(engine, &layer, &room).await;
}

/// A compaction that finds the rotated WAL of a failed one absorbs the active WAL into it,
/// also after an absorption that failed writing or renaming its copy.
#[tokio::test]
async fn absorption_runs_on_a_layer_that_fails_like_the_opfs() {
    let dir = tempfile::tempdir().unwrap();
    let layer = OpfsLikeFiles::new(dir.path());
    let room = RoomId::new("opfs-absorption").unwrap();
    let engine = open_on(&layer, &room).await;
    let wal_path = engine.wal_file_path(&room);

    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=10).await;
    for point in ["compaction.absorb_write", "compaction.absorb_rename"] {
        fail_point::arm(point, &wal_path);
        assert!(engine.compact_room(&room).await.is_err(), "{point}");
        assert!(
            !layer.real(&absorbing_wal_path(&wal_path)).exists(),
            "{point}"
        );
    }
    engine.compact_room(&room).await.unwrap();
    assert!(!layer.real(&compacting_wal_path(&wal_path)).exists());
    apply_range(&engine, &room, 11..=11).await;

    let engine = reopen_on(engine, &layer, &room).await;
    assert_rows(&engine, &room, 11).await;
    assert_clean(engine, &layer, &room).await;
}

/// The two rollbacks of a rotation whose fresh WAL cannot be created: the rotated WAL is renamed
/// back with its handle, over a file found in the fresh WAL's place too, and a rollback that
/// fails leaves the room failed until it is reopened.
#[tokio::test]
async fn rotation_rollbacks_run_on_a_layer_that_fails_like_the_opfs() {
    let dir = tempfile::tempdir().unwrap();
    let layer = OpfsLikeFiles::new(dir.path());
    let room = RoomId::new("opfs-rollbacks").unwrap();
    let engine = open_on(&layer, &room).await;
    let wal_path = engine.wal_file_path(&room);
    let compacting = layer.real(&compacting_wal_path(&wal_path));

    apply_range(&engine, &room, 1..=5).await;
    fail_point::arm("compaction.rotate_open", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());
    assert!(!compacting.exists());
    apply_range(&engine, &room, 6..=7).await;

    let mut renamed = fail_point::arm_pause("compaction.rotate_renamed", &wal_path);
    let compaction = {
        let (engine, room) = (engine.clone(), room.clone());
        tokio::spawn(async move { engine.compact_room(&room).await })
    };
    tokio::time::timeout(Duration::from_secs(10), renamed.reached())
        .await
        .expect("the rotation renames the WAL");
    std::fs::write(layer.real(&wal_path), vec![0xFF; 4096]).unwrap();
    renamed.release();
    let result = compaction.await.unwrap();
    assert!(
        matches!(&result, Err(StorageError::Io(err)) if err.kind() == io::ErrorKind::AlreadyExists),
        "{result:?}"
    );
    assert!(!compacting.exists());
    apply_range(&engine, &room, 8..=10).await;

    fail_point::arm("compaction.rotate_open", &wal_path);
    fail_point::arm("compaction.rotate_rollback", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());
    assert_room_failed(&engine, &room, 11).await;

    let engine = reopen_on(engine, &layer, &room).await;
    assert_rows(&engine, &room, 10).await;
    apply_range(&engine, &room, 11..=11).await;
    engine.compact_room(&room).await.unwrap();
    let engine = reopen_on(engine, &layer, &room).await;
    assert_rows(&engine, &room, 11).await;
    assert_clean(engine, &layer, &room).await;
}

/// `apply_snapshot` installs a snapshot over a room with a rotated WAL left behind, after an
/// install whose rename failed.
#[tokio::test]
async fn snapshot_install_runs_on_a_layer_that_fails_like_the_opfs() {
    let dir = tempfile::tempdir().unwrap();
    let layer = OpfsLikeFiles::new(dir.path());
    let room = RoomId::new("opfs-install").unwrap();
    let engine = open_on(&layer, &room).await;
    let wal_path = engine.wal_file_path(&room);
    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=7).await;

    let snapshot = snapshot_with_rows(&room, 1, 20).await;
    let head = SequenceNumber::from(20u64);
    fail_point::arm("apply_snapshot.rename", &wal_path);
    let result = engine
        .apply_snapshot(&room, schema(), snapshot.clone().into(), head)
        .await;
    assert!(result.is_err(), "{result:?}");
    assert!(staged_snapshot_files(&layer.real(&layer.virtual_root.join("data"))).is_empty());

    engine
        .apply_snapshot(&room, schema(), snapshot.into(), head)
        .await
        .unwrap();
    assert!(!layer.real(&compacting_wal_path(&wal_path)).exists());
    apply_range(&engine, &room, 21..=21).await;

    let engine = reopen_on(engine, &layer, &room).await;
    assert_rows(&engine, &room, 21).await;
    assert_clean(engine, &layer, &room).await;
}

/// Opening a room recovers it from compactions interrupted before their snapshot, after it and
/// while staging it, and from the files a crash leaves behind.
#[tokio::test]
async fn recovery_of_interrupted_compactions_runs_on_a_layer_that_fails_like_the_opfs() {
    let dir = tempfile::tempdir().unwrap();
    let layer = OpfsLikeFiles::new(dir.path());
    let room = RoomId::new("opfs-recovery").unwrap();
    let engine = open_on(&layer, &room).await;
    let (snap_path, wal_path) = (engine.snap_file_path(&room), engine.wal_file_path(&room));

    apply_range(&engine, &room, 1..=5).await;
    fail_compaction_leaving_compacting_wal(&engine, &room).await;
    apply_range(&engine, &room, 6..=8).await;
    let staged = layer.real(&snap_path).with_extension("snap.tmp.crashed");
    std::fs::write(&staged, b"partial").unwrap();
    std::fs::write(layer.real(&absorbing_wal_path(&wal_path)), b"partial").unwrap();
    let engine = reopen_on(engine, &layer, &room).await;
    assert_rows(&engine, &room, 8).await;
    assert!(!staged.exists());

    apply_range(&engine, &room, 9..=10).await;
    fail_point::arm("compaction.cleanup", &wal_path);
    assert!(engine.compact_room(&room).await.is_err());
    assert!(layer.real(&compacting_wal_path(&wal_path)).exists());
    let engine = reopen_on(engine, &layer, &room).await;
    assert_rows(&engine, &room, 10).await;

    apply_range(&engine, &room, 11..=12).await;
    fail_point::arm("snapshot.stage_write", &snap_path);
    assert!(engine.compact_room(&room).await.is_err());
    let engine = reopen_on(engine, &layer, &room).await;
    assert_rows(&engine, &room, 12).await;
    assert_clean(engine, &layer, &room).await;
}
