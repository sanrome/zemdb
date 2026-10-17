//! Limits of the HTTP edge as served by `serve_until_shutdown`: header and body read
//! timeouts, the response stall timeout, the connection limit, the budget of response bytes in
//! flight, the SSE stream limits, and HTTP/2 without TLS.
//!
//! The tests run on the real clock with the shortest timeouts the configuration accepts: a
//! paused clock jumps to the next timer whenever the runtime waits for socket events, which
//! would let a server timeout fire before the bytes it waits for are read.

mod common;

use common::{
    assert_binary_error, build, commit, read_sse_until, register, register_msg, Parts,
    ADMIN_SECRET, AUTH_SECRET,
};
use futures::StreamExt;
use reqwest::StatusCode;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use zemdb_core::protocol::codec::MAX_FRAME_SIZE;
use zemdb_core::*;
use zemdb_server::{generate_client_token, ServerConfig, ServerError};

/// Header and body timeouts, and minimum body rate, of the tests that exercise them.
const HEADER_TIMEOUT: Duration = Duration::from_secs(1);
const BODY_TIMEOUT: Duration = Duration::from_secs(2);
const BODY_MIN_RATE: u64 = 1024;

fn short_timeouts(config: &mut ServerConfig) {
    config.header_read_timeout_secs = HEADER_TIMEOUT.as_secs();
    config.body_read_timeout_secs = BODY_TIMEOUT.as_secs();
    config.body_min_rate_bytes_per_sec = BODY_MIN_RATE;
}

struct TestServer {
    addr: std::net::SocketAddr,
    room_id: RoomId,
    client_id: ClientId,
    token: String,
    stop: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<Result<(), ServerError>>>,
    parts: Parts,
}

/// Serves a room `room-edge` (schema: table `tasks` with an int key and a nullable bytes
/// column) with the default limits changed by `configure`.
async fn start_server(configure: impl FnOnce(&mut ServerConfig)) -> TestServer {
    let parts = build(|config| {
        configure(config);
        config.validate().unwrap();
    });
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .nullable_column("data", DataType::Bytes);
    let schema_id = parts.register_schema("tasks", Schema::builder().table(table).build());
    let room_id = parts.create_room("room-edge", &schema_id).await;
    let client_id = ClientId::new("edge-client").unwrap();
    let token = generate_client_token(&client_id, &room_id, Duration::from_secs(3600), AUTH_SECRET);

    let (addr, stop, server) = parts.serve_until_stopped().await;
    TestServer {
        addr,
        room_id,
        client_id,
        token,
        stop: Some(stop),
        server: Some(server),
        parts,
    }
}

impl TestServer {
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn register_frame(&self) -> Vec<u8> {
        encode_message(&register_msg(&self.room_id, &self.client_id, &self.token)).unwrap()
    }

    /// Starts shutting the server down after `delay`, without waiting for it to stop.
    fn shutdown_after(&mut self, delay: Duration) {
        let stop = self.stop.take().unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            stop.send(()).unwrap();
        });
    }

    /// Waits for a shutdown started by [`TestServer::shutdown_after`] and checks that it
    /// finished cleanly.
    async fn wait_stopped(mut self) {
        self.server.take().unwrap().await.unwrap().unwrap();
    }

    /// Shuts the server down and checks that it stopped cleanly.
    async fn shutdown(self) {
        self.timed_shutdown().await.1.unwrap();
    }

    /// Shuts the server down, returning how long it took and how it ended (`Ok` only after
    /// every room was shut down).
    async fn timed_shutdown(mut self) -> (Duration, Result<(), ServerError>) {
        let start = Instant::now();
        self.stop.take().unwrap().send(()).unwrap();
        let result = self.server.take().unwrap().await.unwrap();
        (start.elapsed(), result)
    }

    /// Registers the test client and commits `count` rows of `size` bytes each, through the
    /// room actor.
    async fn commit_large_rows(&self, count: u8, size: usize) {
        let sender = self
            .parts
            .room_manager
            .get_or_spawn(&self.room_id, None)
            .await
            .unwrap();
        register(&sender, &self.client_id).await;
        for i in 0..count {
            let row = vec![Value::Int(i.into()), Value::Bytes(vec![i; size].into())];
            let op =
                Operation::insert(0, PrimaryKey::single(i64::from(i)), CompactRow::new(row), 0);
            commit(
                &sender,
                &self.client_id,
                MutationId::new([i + 1; 16]),
                0,
                op,
            )
            .await
            .unwrap();
        }
    }

    /// A `Sync` request from the start of the log.
    fn sync_frame(&self) -> Vec<u8> {
        encode_message(&ClientMessage::Sync {
            correlation_id: CorrelationId::new(5),
            room_id: self.room_id.clone(),
            client_id: self.client_id.clone(),
            from_seq: SequenceNumber::new(0),
            max_batch_size: 100,
        })
        .unwrap()
    }
}

/// Reads from `stream` until the server closes it, returning what it sent.
async fn read_until_closed(stream: &mut TcpStream) -> Vec<u8> {
    let mut received = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return received,
            Ok(n) => received.extend_from_slice(&buf[..n]),
        }
    }
}

/// Asserts that the server closes `stream` no sooner than `open_for` after `since`, and
/// within a few seconds of it.
async fn assert_closed_after(stream: &mut TcpStream, since: Instant, open_for: Duration) {
    tokio::time::timeout(open_for + Duration::from_secs(5), read_until_closed(stream))
        .await
        .expect("the server must close the connection");
    let elapsed = since.elapsed();
    assert!(
        elapsed >= open_for,
        "closed after {elapsed:?}, before {open_for:?}"
    );
}

/// Sends the HTTP/2 connection preface and an empty SETTINGS frame.
async fn send_h2_preface(stream: &mut TcpStream) {
    stream
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    stream
        .write_all(&[0, 0, 0, 0x4, 0, 0, 0, 0, 0])
        .await
        .unwrap();
}

/// Reads from `stream` until the end of a response's headers, returning what it read.
async fn read_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut received = Vec::new();
    let mut buf = [0u8; 1024];
    while !received.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed before the response");
        received.extend_from_slice(&buf[..n]);
    }
    received
}

/// Sends a GET request for an unknown path on a keep-alive connection and reads the 404.
async fn get_not_found(stream: &mut TcpStream) {
    stream
        .write_all(b"GET /nothing HTTP/1.1\r\nHost: zemdb\r\n\r\n")
        .await
        .unwrap();
    let response = read_head(stream).await;
    assert!(
        response.starts_with(b"HTTP/1.1 404"),
        "{}",
        String::from_utf8_lossy(&response)
    );
}

#[tokio::test]
async fn connection_that_does_not_complete_its_headers_is_closed() {
    let server = start_server(short_timeouts).await;
    let start = Instant::now();
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    stream
        .write_all(b"POST /rooms/room-edge/register HTTP/1.1\r\nHost: zemdb\r\n")
        .await
        .unwrap();
    assert_closed_after(&mut stream, start, HEADER_TIMEOUT).await;
    server.shutdown().await;
}

#[tokio::test]
async fn connection_that_sends_nothing_is_closed() {
    // Before its first bytes the server cannot even tell HTTP/1.1 from HTTP/2.
    let server = start_server(short_timeouts).await;
    let start = Instant::now();
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    assert_closed_after(&mut stream, start, HEADER_TIMEOUT).await;
    server.shutdown().await;
}

#[tokio::test]
async fn idle_keep_alive_connection_is_closed() {
    let server = start_server(short_timeouts).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    let start = Instant::now();
    get_not_found(&mut stream).await;
    assert_closed_after(&mut stream, start, HEADER_TIMEOUT).await;
    server.shutdown().await;
}

#[tokio::test]
async fn http2_connection_without_requests_is_closed() {
    let server = start_server(short_timeouts).await;
    let start = Instant::now();
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    // The HTTP/2 connection preface and an empty SETTINGS frame, then nothing.
    send_h2_preface(&mut stream).await;
    assert_closed_after(&mut stream, start, HEADER_TIMEOUT).await;
    server.shutdown().await;
}

/// A body made of `chunks`, each sent `every` after the previous one.
fn dripping_body(chunks: Vec<Vec<u8>>, every: Duration) -> reqwest::Body {
    let drip = futures::stream::iter(chunks).then(move |chunk| async move {
        tokio::time::sleep(every).await;
        Ok::<_, std::io::Error>(chunk)
    });
    reqwest::Body::wrap_stream(drip)
}

/// How long [`post_dripping`] keeps sending its body.
const DRIP_DURATION: Duration = Duration::from_millis(1500);

/// Posts to `path` with `headers`, declaring a body of 1,000 bytes but sending only one byte
/// every 100 ms for [`DRIP_DURATION`]. Returns the response head and body, read until the
/// server closes the connection.
///
/// The drip stops before the body timeout, so that the server has read every byte sent when
/// it answers: closing a socket with unread data resets the connection, and some platforms
/// then discard the response before the client reads it.
async fn post_dripping(addr: std::net::SocketAddr, path: &str, headers: &str) -> (String, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let head =
        format!("POST {path} HTTP/1.1\r\nHost: zemdb\r\n{headers}Content-Length: 1000\r\n\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    let drip_start = Instant::now();
    while drip_start.elapsed() < DRIP_DURATION {
        stream.write_all(&[0]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let response = read_until_closed(&mut stream).await;
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a complete response head");
    (
        String::from_utf8_lossy(&response[..split]).into_owned(),
        response[split + 4..].to_vec(),
    )
}

#[tokio::test]
async fn body_sent_drip_by_drip_fails_when_its_time_is_up() {
    let server = start_server(short_timeouts).await;

    // Bytes keep arriving well within the header timeout, but far below the minimum rate
    // (each one buys 1 ms), so the body is still incomplete when its base time is up. Data Plane endpoints answer with a binary error frame, admin
    // endpoints in JSON, as for any other error.
    let start = Instant::now();
    let admin_headers =
        format!("Authorization: Bearer {ADMIN_SECRET}\r\nContent-Type: application/json\r\n");
    let ((binary_head, binary_body), (admin_head, admin_body)) =
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                post_dripping(server.addr, "/rooms/room-edge/register", ""),
                post_dripping(server.addr, "/admin/schemas", &admin_headers),
            )
        })
        .await
        .expect("an incomplete body must be answered");
    let elapsed = start.elapsed();
    // The time limit counts from the end of the headers, not from the last byte received
    // (that would only expire a body timeout after the drip stopped), and the trickled bytes
    // extend it by next to nothing.
    assert!(elapsed >= BODY_TIMEOUT, "{elapsed:?}");
    assert!(
        elapsed < BODY_TIMEOUT + Duration::from_secs(1),
        "{elapsed:?}"
    );

    assert!(binary_head.starts_with("HTTP/1.1 408"), "{binary_head}");
    let reply: ServerMessage = decode_message(&binary_body).unwrap();
    assert!(
        matches!(
            &reply,
            ServerMessage::Error { code: ErrorCode::RequestTimeout, room_id: Some(room), .. }
                if *room == server.room_id
        ),
        "{reply:?}"
    );

    assert!(admin_head.starts_with("HTTP/1.1 408"), "{admin_head}");
    let body: serde_json::Value = serde_json::from_slice(&admin_body).unwrap();
    assert_eq!(body["code"], "RequestTimeout", "{body}");

    server.shutdown().await;
}

#[tokio::test]
async fn body_above_the_minimum_rate_may_take_longer_than_the_base_time() {
    let server = start_server(short_timeouts).await;
    // A schema declaration padded with whitespace to 6 KiB, sent at about 1.7 KiB/s (above
    // the minimum of 1 KiB/s) in chunks 600 ms apart: about 3.6 s in all, well over the base
    // time of 2 s. Each KiB received extends the deadline by one second.
    let schema = Schema::builder()
        .table(TableSchema::builder("notes").primary_key("id", DataType::Int))
        .build();
    let mut json = serde_json::to_vec(&serde_json::json!({
        "schema_id": "slow-schema",
        "schema": schema,
    }))
    .unwrap();
    json.resize(6 * 1024, b' ');
    let chunks = json.chunks(1024).map(<[u8]>::to_vec).collect();
    let start = Instant::now();
    let response = reqwest::Client::new()
        .post(server.url("/admin/schemas"))
        .bearer_auth(ADMIN_SECRET)
        .header("content-type", "application/json")
        .body(dripping_body(chunks, Duration::from_millis(600)))
        .send()
        .await
        .unwrap();
    assert!(start.elapsed() > BODY_TIMEOUT, "{:?}", start.elapsed());
    assert_eq!(response.status(), StatusCode::CREATED);
    server.shutdown().await;
}

#[tokio::test]
async fn body_that_arrives_in_time_is_served() {
    let server = start_server(|config| config.body_read_timeout_secs = 5).await;
    // Four chunks 250 ms apart: the whole frame arrives slowly, but within the body timeout.
    let frame = server.register_frame();
    let chunks = frame
        .chunks(frame.len().div_ceil(4))
        .map(<[u8]>::to_vec)
        .collect();
    let response = reqwest::Client::new()
        .post(server.url("/rooms/room-edge/register"))
        .body(dripping_body(chunks, Duration::from_millis(250)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    server.shutdown().await;
}

/// Opens the event stream of `room_id` with `token`, on a connection of its own.
async fn open_events(server: &TestServer, room_id: &str, token: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(server.url(&format!("/rooms/{room_id}/events")))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
}

/// Token of `client_id` for `room_id`.
fn client_token(client_id: &str, room_id: &str) -> String {
    let client_id = ClientId::new(client_id).unwrap();
    let room_id = RoomId::new(room_id).unwrap();
    generate_client_token(&client_id, &room_id, Duration::from_secs(3600), AUTH_SECRET)
}

#[tokio::test]
async fn open_sse_stream_outlives_every_timeout() {
    let server = start_server(|config| {
        config.header_read_timeout_secs = 1;
        config.body_read_timeout_secs = 1;
        config.response_stall_timeout_secs = STALL_TIMEOUT.as_secs();
    })
    .await;
    let response = open_events(&server, "room-edge", &server.token).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut events = response.bytes_stream();

    // Without events, the stream is not waiting for the client: it is not stalled.
    tokio::time::sleep(STALL_TIMEOUT + Duration::from_secs(1)).await;

    // The stream still delivers events: a commit advances the head.
    let sender = server
        .parts
        .room_manager
        .get_or_spawn(&server.room_id, None)
        .await
        .unwrap();
    register(&sender, &server.client_id).await;
    let op = Operation::insert(
        0,
        PrimaryKey::single(1i64),
        CompactRow::new(vec![Value::Int(1), Value::Null]),
        0,
    );
    commit(&sender, &server.client_id, MutationId::new([1; 16]), 0, op)
        .await
        .unwrap();

    read_sse_until(&mut events, "event: head_advanced").await;
    drop(events);
    server.shutdown().await;
}

#[tokio::test]
async fn at_the_connection_limit_new_connections_wait_for_one_to_close() {
    // Default timeouts: the open connections stay open, idle, during the test.
    let server = start_server(|config| config.max_connections = 2).await;
    let mut first = TcpStream::connect(server.addr).await.unwrap();
    get_not_found(&mut first).await;
    let mut second = TcpStream::connect(server.addr).await.unwrap();
    get_not_found(&mut second).await;

    // The third connection waits in the listen backlog: its request is not served.
    let mut third = TcpStream::connect(server.addr).await.unwrap();
    let waiting = tokio::time::timeout(Duration::from_millis(300), get_not_found(&mut third)).await;
    assert!(waiting.is_err(), "a connection over the limit was served");

    // The open connections keep working.
    get_not_found(&mut first).await;

    // Once one closes, the waiting connection is accepted and its request served.
    drop(second);
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut response = [0u8; 12];
        third.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"HTTP/1.1 404");
    })
    .await
    .expect("the waiting connection must be served once another closes");

    drop(first);
    drop(third);
    server.shutdown().await;
}

#[tokio::test]
async fn http2_without_tls_is_served() {
    let server = start_server(|_| {}).await;
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let response = client
        .post(server.url("/rooms/room-edge/register"))
        .body(server.register_frame())
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), reqwest::Version::HTTP_2);
    assert_eq!(response.status(), StatusCode::OK);
    let reply: ServerMessage = decode_message(&response.bytes().await.unwrap()).unwrap();
    assert!(
        matches!(reply, ServerMessage::Registered { .. }),
        "{reply:?}"
    );
    drop(client);
    server.shutdown().await;
}

/// Rows of the slow reader tests: 2 rows of 4 MiB, a `SyncBatch` of about 8.4 MB. Read at
/// about 3 MB/s, it outlasts the header timeout by more than the socket buffers of both ends
/// can hold, so the server is still writing it after the timeout.
const LARGE_ROWS: u8 = 2;
const LARGE_ROW_SIZE: usize = 4 * 1024 * 1024;

/// The slow reader tests start shutting the server down after this time, once the header
/// timeout has passed: the response in progress is completed before the server stops.
const SHUTDOWN_DURING_RESPONSE: Duration = Duration::from_millis(1500);

/// Pause per 64 KiB read of the slow reader tests: about 3 MB/s.
const SLOW_READ_PAUSE: Duration = Duration::from_millis(20);

/// Sleeps `pause` for every 64 KiB in `unpaced`, leaving the remainder. Paced by bytes, not by
/// reads, so the rate does not depend on how much each read returns or on the resolution of
/// the timer.
async fn pace(unpaced: &mut usize, pause: Duration) {
    while *unpaced >= 64 * 1024 {
        tokio::time::sleep(pause).await;
        *unpaced -= 64 * 1024;
    }
}

/// Asserts that `body` is the `SyncBatch` of the [`LARGE_ROWS`] rows.
fn assert_large_sync_batch(body: &[u8]) {
    let reply: ServerMessage = decode_message(body).unwrap();
    assert!(
        matches!(&reply, ServerMessage::SyncBatch { ops, .. } if ops.len() == LARGE_ROWS as usize),
        "unexpected reply"
    );
}

#[tokio::test]
async fn large_response_to_a_slow_http1_reader_is_delivered_in_full() {
    let mut server = start_server(short_timeouts).await;
    server.commit_large_rows(LARGE_ROWS, LARGE_ROW_SIZE).await;

    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(64 * 1024).unwrap();
    let mut stream = socket.connect(server.addr).await.unwrap();
    let frame = server.sync_frame();
    let head = format!(
        "POST /rooms/room-edge/sync HTTP/1.1\r\nHost: zemdb\r\nAuthorization: Bearer {}\r\n\
         Content-Length: {}\r\n\r\n",
        server.token,
        frame.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&frame).await.unwrap();
    server.shutdown_after(SHUTDOWN_DURING_RESPONSE);

    // About 3 MB/s: the response takes about three header timeouts to read.
    let mut response = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    let expected = loop {
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed after {} bytes", response.len());
        response.extend_from_slice(&buf[..n]);
        if let Some(split) = response.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&response[..split]).to_lowercase();
            let len: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .expect("a content length")
                .parse()
                .unwrap();
            break split + 4 + len;
        }
    };
    let mut unpaced = 0;
    while response.len() < expected {
        match stream.read(&mut buf).await {
            Ok(n) if n > 0 => {
                response.extend_from_slice(&buf[..n]);
                unpaced += n;
            }
            other => panic!(
                "connection closed after {} of {expected} bytes: {other:?}",
                response.len()
            ),
        }
        pace(&mut unpaced, SLOW_READ_PAUSE).await;
    }
    let split = response.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    assert_large_sync_batch(&response[split + 4..]);
    server.wait_stopped().await;
}

/// An HTTP/2 client that sends nothing but its requests.
fn http2_client() -> reqwest::Client {
    reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap()
}

impl TestServer {
    /// A `Sync` from the start of the log, through `client`.
    fn sync_request(&self, client: &reqwest::Client) -> reqwest::RequestBuilder {
        client
            .post(self.url("/rooms/room-edge/sync"))
            .bearer_auth(&self.token)
            .body(self.sync_frame())
    }

    /// Sends [`TestServer::sync_request`], returning once the response headers arrive,
    /// without reading the body.
    async fn send_sync(&self, client: &reqwest::Client) -> reqwest::Response {
        self.sync_request(client).send().await.unwrap()
    }
}

/// Reads `response` at one 64 KiB frame per `pause`, checking that it arrives in full.
async fn read_paced(response: reqwest::Response, pause: Duration) -> Vec<u8> {
    let expected = response.content_length().unwrap() as usize;
    let mut chunks = response.bytes_stream();
    let mut body = Vec::new();
    let mut unpaced = 0;
    while let Some(chunk) = chunks.next().await {
        match chunk {
            Ok(chunk) => {
                body.extend_from_slice(&chunk);
                unpaced += chunk.len();
            }
            Err(err) => panic!("body cut after {} of {expected} bytes: {err:?}", body.len()),
        }
        pace(&mut unpaced, pause).await;
    }
    assert_eq!(body.len(), expected);
    body
}

#[tokio::test]
async fn large_response_to_a_slow_http2_reader_is_delivered_in_full() {
    let mut server = start_server(short_timeouts).await;
    server.commit_large_rows(LARGE_ROWS, LARGE_ROW_SIZE).await;

    let client = http2_client();
    let response = server.send_sync(&client).await;
    assert_eq!(response.status(), StatusCode::OK);
    server.shutdown_after(SHUTDOWN_DURING_RESPONSE);
    // About 3 MB/s, as for HTTP/1.1.
    assert_large_sync_batch(&read_paced(response, SLOW_READ_PAUSE).await);
    drop(client);
    server.wait_stopped().await;
}

/// Response stall timeout of the tests that exercise it: the shortest accepted.
const STALL_TIMEOUT: Duration = Duration::from_secs(5);

/// A single connection at a time, with the shortest response stall timeout and the longest
/// header timeout, so that only the stall timeout can close a connection during the test.
fn one_connection_short_stall(config: &mut ServerConfig) {
    config.max_connections = 1;
    // Room for the several responses of one client that the tests hold.
    config.max_inflight_response_bytes_per_client = config.max_inflight_response_bytes;
    config.response_stall_timeout_secs = STALL_TIMEOUT.as_secs();
    config.header_read_timeout_secs = 300;
}

/// Rows of the stall tests: a `SyncBatch` of about 12.6 MB, well over the socket buffers of
/// both ends and hyper's write buffer.
const STALL_ROWS: u8 = 3;

/// Asserts that a new connection is served within the stall timeout and some slack, counted
/// once the response that stalls has started: the only connection allowed must have been
/// closed for stalling.
async fn assert_new_connection_served(server: &TestServer) {
    tokio::time::timeout(STALL_TIMEOUT + Duration::from_secs(10), async {
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        get_not_found(&mut stream).await;
    })
    .await
    .expect("a stalled connection must be closed, freeing its place");
}

#[tokio::test]
async fn http2_client_that_stops_reading_is_disconnected() {
    let server = start_server(one_connection_short_stall).await;
    server.commit_large_rows(STALL_ROWS, LARGE_ROW_SIZE).await;

    // Syncs whose bodies are never read: flow control holds most of each one in the server.
    // On a loaded machine, the first response may stall the connection before the others get
    // their headers out; those fail to send.
    let client = http2_client();
    let sends = (0..4).map(|_| server.sync_request(&client).send());
    let held: Vec<_> = futures::future::join_all(sends)
        .await
        .into_iter()
        .filter_map(Result::ok)
        .collect();
    assert!(!held.is_empty());
    assert!(held
        .iter()
        .all(|response| response.status() == StatusCode::OK));
    assert_new_connection_served(&server).await;
    for response in held {
        assert!(response.bytes().await.is_err(), "the body must be cut");
    }
    drop(client);
    server.shutdown().await;
}

#[tokio::test]
async fn http1_client_that_stops_reading_is_disconnected() {
    let server = start_server(one_connection_short_stall).await;
    server.commit_large_rows(STALL_ROWS, LARGE_ROW_SIZE).await;

    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(64 * 1024).unwrap();
    let mut stream = socket.connect(server.addr).await.unwrap();
    let frame = server.sync_frame();
    let head = format!(
        "POST /rooms/room-edge/sync HTTP/1.1\r\nHost: zemdb\r\nAuthorization: Bearer {}\r\n\
         Content-Length: {}\r\n\r\n",
        server.token,
        frame.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&frame).await.unwrap();
    // Wait for the response to start, as the HTTP/2 test does: the stall timeout counts from
    // there, while building the response can take seconds on a loaded machine.
    let mut received = read_head(&mut stream).await;
    assert!(received.starts_with(b"HTTP/1.1 200"));

    assert_new_connection_served(&server).await;
    received.extend(read_until_closed(&mut stream).await);
    assert!(
        received.len() < usize::from(STALL_ROWS) * LARGE_ROW_SIZE,
        "the response must be cut"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn slow_reader_that_keeps_reading_outlasts_the_stall_timeout() {
    let server = start_server(|config| {
        config.response_stall_timeout_secs = STALL_TIMEOUT.as_secs();
    })
    .await;
    server.commit_large_rows(LARGE_ROWS, LARGE_ROW_SIZE).await;

    // About 1 MB/s: the response takes well over the stall timeout to read, but every frame
    // is taken far within it.
    let client = http2_client();
    let response = server.send_sync(&client).await;
    let started = Instant::now();
    assert_large_sync_batch(&read_paced(response, Duration::from_millis(60)).await);
    assert!(started.elapsed() > STALL_TIMEOUT);
    drop(client);
    server.shutdown().await;
}

/// Rows whose `SyncBatch` (about 10.5 MB) fits the smallest budget of response bytes in flight
/// once, but not twice.
const BUDGET_ROWS: u8 = 2;
const BUDGET_ROW_SIZE: usize = 5 * 1024 * 1024;

#[tokio::test]
async fn responses_over_the_budget_in_flight_wait_for_held_bodies() {
    let server = start_server(|config| {
        config.max_inflight_response_bytes = MAX_FRAME_SIZE as u64;
        config.max_inflight_response_bytes_per_client = MAX_FRAME_SIZE as u64;
    })
    .await;
    server.commit_large_rows(BUDGET_ROWS, BUDGET_ROW_SIZE).await;
    let held_client = http2_client();
    let held = server.send_sync(&held_client).await;
    assert_eq!(held.status(), StatusCode::OK);

    let client = reqwest::Client::new();
    let refused = server.send_sync(&client).await;
    assert!(refused.headers().contains_key("retry-after"));
    assert_binary_error(
        refused,
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Unavailable,
    )
    .await;

    // A commit is durable before its reply, so it is acknowledged, without its catch-up.
    let commit = ClientMessage::Commit {
        correlation_id: CorrelationId::new(7),
        room_id: server.room_id.clone(),
        client_id: server.client_id.clone(),
        mutation_id: MutationId::new([0xEE; 16]),
        last_ack_seq: SequenceNumber::new(0),
        op: Operation::insert(
            0,
            PrimaryKey::single(100i64),
            CompactRow::new(vec![Value::Int(100), Value::Null]),
            0,
        ),
    };
    let response = client
        .post(server.url("/rooms/room-edge/commit"))
        .bearer_auth(&server.token)
        .body(encode_message(&commit).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    match decode_message(&response.bytes().await.unwrap()).unwrap() {
        ServerMessage::CommitAck {
            assigned_seq,
            catchup_ops,
            has_more,
            ..
        } => {
            assert_eq!(assigned_seq, SequenceNumber::new(3));
            assert!(catchup_ops.is_empty());
            assert!(has_more);
        }
        other => panic!("expected a CommitAck, got {other:?}"),
    }

    // Once the held body is released, syncs are served again. Each attempt encodes a whole
    // batch, which takes a while on a slow machine.
    drop((held, held_client));
    tokio::time::timeout(Duration::from_secs(30), async {
        while server.send_sync(&client).await.status() != StatusCode::OK {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a released body must return its bytes to the budget");
    server.shutdown().await;
}

#[tokio::test]
async fn client_over_its_part_of_the_budget_leaves_the_rest_to_other_rooms() {
    let server = start_server(|config| {
        config.max_inflight_response_bytes = 4 * MAX_FRAME_SIZE as u64;
        config.max_inflight_response_bytes_per_client = MAX_FRAME_SIZE as u64;
    })
    .await;
    server.commit_large_rows(BUDGET_ROWS, BUDGET_ROW_SIZE).await;
    let held_client = http2_client();
    let held = server.send_sync(&held_client).await;
    assert_eq!(held.status(), StatusCode::OK);

    // The client's next response does not fit in its part...
    let client = reqwest::Client::new();
    assert_binary_error(
        server.send_sync(&client).await,
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Unavailable,
    )
    .await;

    // ...while a client of another room is served.
    let schema_id = SchemaId::new("tasks").unwrap();
    let other_room = server.parts.create_room("room-other", &schema_id).await;
    let other = ClientId::new("other-client").unwrap();
    let sender = server
        .parts
        .room_manager
        .get_or_spawn(&other_room, None)
        .await;
    register(&sender.unwrap(), &other).await;
    let sync = ClientMessage::Sync {
        correlation_id: CorrelationId::new(1),
        room_id: other_room,
        client_id: other,
        from_seq: SequenceNumber::new(0),
        max_batch_size: 100,
    };
    let response = client
        .post(server.url("/rooms/room-other/sync"))
        .bearer_auth(client_token("other-client", "room-other"))
        .body(encode_message(&sync).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Releasing the held body frees the client's part.
    drop((held, held_client));
    tokio::time::timeout(Duration::from_secs(30), async {
        while server.send_sync(&client).await.status() != StatusCode::OK {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a released body must return its bytes to the client's part");
    server.shutdown().await;
}

#[tokio::test]
async fn shutdown_closes_connections_without_a_request_in_progress_at_once() {
    // A long header timeout, valid but over the shutdown grace period: waiting for these
    // connections to time out would abandon the shutdown before the rooms are closed.
    let server = start_server(|config| config.header_read_timeout_secs = 30).await;
    // Headers that never complete.
    let mut partial = TcpStream::connect(server.addr).await.unwrap();
    partial
        .write_all(b"GET /nothing HTTP/1.1\r\nHost: zemdb\r\n")
        .await
        .unwrap();
    // An HTTP/2 connection that never answers, not even the ping of a graceful shutdown.
    let mut mute = TcpStream::connect(server.addr).await.unwrap();
    send_h2_preface(&mut mute).await;
    // An idle keep-alive connection and one that sent nothing.
    let mut idle = TcpStream::connect(server.addr).await.unwrap();
    get_not_found(&mut idle).await;
    let _silent = TcpStream::connect(server.addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (elapsed, result) = server.timed_shutdown().await;
    result.unwrap();
    assert!(
        elapsed < Duration::from_secs(2),
        "shutdown took {elapsed:?}"
    );
}

#[tokio::test]
async fn a_third_sse_stream_of_a_client_ends_its_oldest() {
    let server = start_server(|_| {}).await;
    let mut streams = Vec::new();
    for _ in 0..3 {
        let response = open_events(&server, "room-edge", &server.token).await;
        assert_eq!(response.status(), StatusCode::OK);
        streams.push(response.bytes_stream());
    }
    let mut oldest = streams.remove(0);
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = oldest.next().await {
            chunk.unwrap();
        }
    })
    .await
    .expect("the oldest stream must end");

    // The two newest still deliver events.
    server.commit_large_rows(1, 1).await;
    for stream in &mut streams {
        read_sse_until(stream, "event: head_advanced").await;
    }
    drop(streams);
    server.shutdown().await;
}

#[tokio::test]
async fn sse_streams_over_their_limit_are_refused_and_leave_connections_for_requests() {
    let server = start_server(|config| {
        config.max_connections = 4;
        config.max_sse_streams = Some(2);
    })
    .await;
    let schema_id = SchemaId::new("tasks").unwrap();
    server.parts.create_room("room-other", &schema_id).await;

    let first = open_events(&server, "room-edge", &server.token).await;
    let second = open_events(&server, "room-edge", &client_token("second", "room-edge")).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::OK);
    let other_token = client_token("other", "room-other");
    let refused = open_events(&server, "room-other", &other_token).await;
    assert_binary_error(
        refused,
        StatusCode::TOO_MANY_REQUESTS,
        ErrorCode::RateLimited,
    )
    .await;

    // With every SSE stream open, a request still gets a connection.
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    get_not_found(&mut stream).await;

    // Closing a stream frees its place.
    drop(first);
    tokio::time::timeout(Duration::from_secs(5), async {
        while open_events(&server, "room-other", &other_token)
            .await
            .status()
            != StatusCode::OK
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a closed stream must free its place");
    drop((second, stream));
    server.shutdown().await;
}
