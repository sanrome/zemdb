//! The `zemdb-server` binary and its `data_dir` lock.
#![cfg(unix)]

mod common;

use common::*;
use fs2::FileExt;
use reqwest::StatusCode;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use zemdb_core::*;

/// Starts the server binary on `data_dir` and waits until it listens. Returns the process and
/// its base URL.
fn start_server(data_dir: &Path) -> (std::process::Child, String) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_zemdb-server"))
        .env("ZEMDB_HOST", "127.0.0.1")
        .env("ZEMDB_PORT", port.to_string())
        .env("ZEMDB_DATA_DIR", data_dir)
        .env("ZEMDB_AUTH_SECRET", AUTH_SECRET)
        .env("ZEMDB_ADMIN_SECRET", ADMIN_SECRET)
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    while !lines
        .next()
        .expect("the server exited")
        .unwrap()
        .contains("listening")
    {}
    (child, format!("http://127.0.0.1:{port}"))
}

/// Whether `dir` holds a staged snapshot of room `big`.
fn has_staged_snapshot(dir: &Path) -> bool {
    std::fs::read_dir(dir).unwrap().any(|entry| {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        name.starts_with("big_") && name.ends_with(".snap")
    })
}

/// A multipart upload whose client disconnects after its last chunk keeps promoting the
/// snapshot on the blocking pool while the server shuts down. The server only releases
/// `server.lock` once that write is done: a second server never starts while the first one
/// still writes to `data_dir`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_binary_holds_the_data_dir_lock_until_it_stops_writing() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let (mut server, base) = start_server(&data_dir);
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap();
    let post_frame = |path: &str, bearer: &str, msg: &ClientMessage| {
        client
            .post(format!("{base}/rooms/big/{path}"))
            .header("content-type", "application/octet-stream")
            .bearer_auth(bearer)
            .body(encode_message(msg).unwrap())
            .send()
    };

    for (path, body) in [
        (
            "schemas",
            serde_json::json!({"schema_id": "tasks", "schema": tasks_schema()}),
        ),
        (
            "rooms",
            serde_json::json!({"room_id": "big", "schema_id": "tasks"}),
        ),
    ] {
        let resp = client
            .post(format!("{base}/admin/{path}"))
            .bearer_auth(ADMIN_SECRET)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // One commit, so that a snapshot at sequence 1 is inside the retained range.
    let (room_id, client_id) = (
        RoomId::new("big").unwrap(),
        ClientId::new("member").unwrap(),
    );
    let member_token = token(&client_id, &room_id);
    let register = register_msg(&room_id, &client_id, &member_token);
    let commit = ClientMessage::Commit {
        correlation_id: CorrelationId::new(2),
        room_id: room_id.clone(),
        client_id: client_id.clone(),
        mutation_id: MutationId::new([1; 16]),
        last_ack_seq: SequenceNumber::new(0),
        op: make_test_op(1),
    };
    for (path, msg) in [("register", &register), ("commit", &commit)] {
        let resp = post_frame(path, &member_token, msg).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // A 256 MiB snapshot in 64 chunks, all but the last one acknowledged.
    const CHUNK: usize = 4 * 1024 * 1024;
    const CHUNKS: u32 = 64;
    let snapshot = envelope(CHUNK * CHUNKS as usize, 9);
    let chunk = |index: u32| ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(10 + u64::from(index)),
        room_id: room_id.clone(),
        snapshot_head_seq: SequenceNumber::new(1),
        chunk_index: index,
        total_chunks: CHUNKS,
        total_bytes: snapshot.len() as u64,
        snapshot_hash: ServerMessage::compute_snapshot_hash(&snapshot),
        data: snapshot[index as usize * CHUNK..][..CHUNK].to_vec().into(),
    };
    for index in 0..CHUNKS - 1 {
        let resp = post_frame("snapshot/upload-chunk", ADMIN_SECRET, &chunk(index))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // The last chunk, from a client that disconnects once the request is sent.
    let body = encode_message(&chunk(CHUNKS - 1)).unwrap();
    let mut stream = std::net::TcpStream::connect(base.trim_start_matches("http://")).unwrap();
    write!(
        stream,
        "POST /rooms/big/snapshot/upload-chunk HTTP/1.1\r\nHost: zemdb\r\nAuthorization: Bearer \
         {ADMIN_SECRET}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(&body).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    drop(stream);
    let snapshots_dir = data_dir.join("snapshots");
    assert!(
        !has_staged_snapshot(&snapshots_dir),
        "the upload completed before the shutdown; the window was not reached"
    );

    // An orderly shutdown, polling the lock that a second server would take.
    let killed = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(data_dir.join("server.lock"))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while lock_file.try_lock_exclusive().is_err() {
        assert!(Instant::now() < deadline, "the lock was never released");
        std::thread::sleep(Duration::from_millis(2));
    }
    let staged_when_unlocked = has_staged_snapshot(&snapshots_dir);
    server.wait().unwrap();
    assert!(
        staged_when_unlocked || !has_staged_snapshot(&snapshots_dir),
        "server.lock was released before the server promoted the upload into snapshots/: a \
         second server could have started and used data_dir meanwhile"
    );
}
