//! Fixtures shared by the integration tests: the server in process, with a transport to it, and
//! a file layer that logs what is done to the files and fails on demand.

// Every file in `tests/` is a crate of its own that uses only part of this module.
#![allow(dead_code)]

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::Request;
use axum::Router;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;
use zemdb_client::{endpoint, TransportClient, TransportError};
use zemdb_core::*;
use zemdb_server::{
    build_router, generate_client_token, AppState, RoomManager, SchemaRegistry, ServerConfig,
    SnapshotRelay,
};
use zemdb_storage::files::{NativeFile, NativeFiles, RoomFile, RoomFiles};

pub const AUTH_SECRET: &str = "test_cluster_secret_key_1234567890";
pub const ADMIN_SECRET: &str = "test_admin_secret_key_12345678901";

/// Table `tasks`: an int key `id` and a string `title`.
pub fn tasks_schema() -> Schema {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    Schema::builder().table(table).build()
}

/// An insert of row `id` into `tasks` of [`tasks_schema`].
pub fn insert_task(id: i64) -> Operation {
    let row = RowBuilder::new().set("id", id).set("title", "task").build();
    tasks_schema()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap()
}

/// A client token of `client_id` for `room_id`, valid for 300 s.
pub fn token(client_id: &ClientId, room_id: &RoomId) -> String {
    generate_client_token(client_id, room_id, Duration::from_secs(300), AUTH_SECRET)
}

/// The pieces of a server keeping its data in a temporary directory, built as the server's tests
/// build them (without the lock of the data directory and the expiry sweeper of `main`), with the
/// test secrets.
pub struct TestServer {
    pub state: AppState,
    _dir: TempDir,
}

impl TestServer {
    pub fn new() -> Self {
        let dir = tempdir().unwrap();
        let config = Arc::new(ServerConfig {
            data_dir: dir.path().join("data"),
            auth_secret: AUTH_SECRET.to_string(),
            admin_secret: ADMIN_SECRET.to_string(),
            ..ServerConfig::default()
        });
        let registry = Arc::new(SchemaRegistry::new(config.data_dir.join("schemas")).unwrap());
        let relay = Arc::new(
            SnapshotRelay::new(
                config.data_dir.join("snapshots"),
                Duration::from_secs(config.snapshot_ttl_secs),
                config.max_snapshot_bytes,
                Duration::from_secs(config.snapshot_upload_max_secs),
            )
            .unwrap(),
        );
        let manager = Arc::new(RoomManager::new(
            Arc::clone(&config),
            Arc::clone(&registry),
            Arc::clone(&relay),
        ));
        Self {
            state: AppState::new(config, registry, manager, relay),
            _dir: dir,
        }
    }

    /// Creates `room_id` with [`tasks_schema`], registered as `tasks` if it is not yet.
    pub async fn create_room(&self, room_id: &str) -> RoomId {
        let schema_id = SchemaId::new("tasks").unwrap();
        if self.state.schema_registry.get_schema(&schema_id).is_none() {
            self.state
                .schema_registry
                .register_schema(schema_id.clone(), tasks_schema())
                .unwrap();
        }
        let room_id = RoomId::new(room_id).unwrap();
        self.state
            .room_manager
            .create_room(room_id.clone(), schema_id, None)
            .await
            .unwrap();
        room_id
    }

    /// A transport to this server's router, called as a `tower` service.
    pub fn transport(&self) -> TestTransport {
        TestTransport {
            router: build_router(self.state.clone()),
        }
    }
}

/// A transport to a [`TestServer`]. Each request runs in a task of its own, as over a network,
/// where the server goes on with a request whose client stopped waiting.
#[derive(Debug, Clone)]
pub struct TestTransport {
    router: Router,
}

#[async_trait]
impl TransportClient for TestTransport {
    async fn send(&self, message: &ClientMessage, token: &str) -> Result<Vec<u8>, TransportError> {
        let request = Request::post(endpoint(message))
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(encode_message(message).unwrap()))
            .unwrap();
        let router = self.router.clone();
        let response = tokio::spawn(async move {
            let response = router.oneshot(request).await.unwrap();
            to_bytes(response.into_body(), usize::MAX).await
        });
        let no_response = |err: &dyn std::fmt::Display| TransportError(err.to_string());
        let body = response.await.map_err(|err| no_response(&err))?;
        Ok(body.map_err(|err| no_response(&err))?.to_vec())
    }
}

/// The file system of the process through a layer that logs what is done to each file (by its
/// name, without its reads), and where one operation on one file can fail with an I/O error (not
/// a missing file) and the first write can be slow.
#[derive(Debug, Clone, Default)]
pub struct TestFiles {
    failing: Option<(&'static str, &'static str)>,
    first_write_delay: Arc<Mutex<Option<Duration>>>,
    log: Arc<Mutex<Vec<String>>>,
}

impl TestFiles {
    /// A layer where `operation` (a method of `RoomFiles` or of `RoomFile`) fails on the file
    /// named `name`: for a rename by path, the file it would replace.
    pub fn failing(operation: &'static str, name: &'static str) -> Self {
        Self {
            failing: Some((operation, name)),
            ..Self::default()
        }
    }

    /// A layer where the first write lands `delay` after it is called, even if its caller
    /// stopped waiting, as a write handed to a slow disk.
    pub fn slow(delay: Duration) -> Self {
        Self {
            first_write_delay: Arc::new(Mutex::new(Some(delay))),
            ..Self::default()
        }
    }

    /// What was done to the files so far, in order, as `operation name`.
    pub fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// Waits until `entry` is in the log.
    pub async fn logged(&self, entry: &str) {
        while !self.log().iter().any(|logged| logged == entry) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Logs `operation` on the files `names` unless it is a read, and fails if it is the failing
    /// one on the last of them.
    fn run(&self, operation: &'static str, names: &[&str]) -> io::Result<()> {
        if !matches!(operation, "size" | "read_at") {
            self.push(format!("{operation} {}", names.join(" ")));
        }
        if self.failing.is_some() && self.failing == names.last().map(|name| (operation, *name)) {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "injected"));
        }
        Ok(())
    }

    fn push(&self, entry: String) {
        self.log.lock().unwrap().push(entry);
    }

    fn file(&self, path: &Path, file: NativeFile) -> TestFile {
        TestFile {
            file: Arc::new(file),
            name: name(path).to_string(),
            files: self.clone(),
        }
    }
}

fn name(path: &Path) -> &str {
    path.file_name().and_then(|name| name.to_str()).unwrap()
}

#[async_trait]
impl RoomFiles for TestFiles {
    type File = TestFile;

    async fn open_locked(&self, path: &Path) -> io::Result<Option<TestFile>> {
        self.run("open_locked", &[name(path)])?;
        let file = NativeFiles.open_locked(path).await?;
        Ok(file.map(|file| self.file(path, file)))
    }

    async fn create_locked(&self, path: &Path) -> io::Result<Option<TestFile>> {
        self.run("create_locked", &[name(path)])?;
        let file = NativeFiles.create_locked(path).await?;
        Ok(file.map(|file| self.file(path, file)))
    }

    async fn create(&self, path: &Path) -> io::Result<TestFile> {
        self.run("create", &[name(path)])?;
        Ok(self.file(path, NativeFiles.create(path).await?))
    }

    async fn open_read(&self, path: &Path) -> io::Result<TestFile> {
        self.run("open_read", &[name(path)])?;
        Ok(self.file(path, NativeFiles.open_read(path).await?))
    }

    async fn exists(&self, path: &Path) -> io::Result<bool> {
        NativeFiles.exists(path).await
    }

    async fn list(&self, dir: &Path, prefix: &str) -> io::Result<Vec<PathBuf>> {
        NativeFiles.list(dir, prefix).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.run("rename", &[name(from), name(to)])?;
        NativeFiles.rename(from, to).await
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        self.run("remove", &[name(path)])?;
        NativeFiles.remove(path).await
    }

    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.run("create_dir_all", &[name(path)])?;
        NativeFiles.create_dir_all(path).await
    }

    async fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.run("sync_dir", &[name(dir)])?;
        NativeFiles.sync_dir(dir).await
    }
}

/// A file opened by [`TestFiles`], which logs what is done to it, and when it is closed.
#[derive(Debug)]
pub struct TestFile {
    file: Arc<NativeFile>,
    name: String,
    files: TestFiles,
}

impl Drop for TestFile {
    fn drop(&mut self) {
        self.files.push(format!("close {}", self.name));
    }
}

#[async_trait]
impl RoomFile for TestFile {
    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.files.run("read_at", &[&self.name])?;
        self.file.read_at(offset, len).await
    }

    async fn write_at(&self, offset: u64, bytes: Vec<u8>) -> io::Result<()> {
        self.files.run("write_at", &[&self.name])?;
        let Some(delay) = self.files.first_write_delay.lock().unwrap().take() else {
            return self.file.write_at(offset, bytes).await;
        };
        let file = Arc::clone(&self.file);
        let write = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            file.write_at(offset, bytes).await
        });
        write.await.unwrap()
    }

    async fn size(&self) -> io::Result<u64> {
        self.files.run("size", &[&self.name])?;
        self.file.size().await
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        self.files.run("set_len", &[&self.name])?;
        self.file.set_len(len).await
    }

    async fn sync_data(&self) -> io::Result<()> {
        self.files.run("sync_data", &[&self.name])?;
        self.file.sync_data().await
    }

    async fn sync_all(&self) -> io::Result<()> {
        self.files.run("sync_all", &[&self.name])?;
        self.file.sync_all().await
    }

    async fn rename(&mut self, to: &Path) -> io::Result<()> {
        self.files.run("rename", &[&self.name])?;
        let file = Arc::get_mut(&mut self.file).expect("no write in flight");
        file.rename(to).await
    }
}
