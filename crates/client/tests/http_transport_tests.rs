//! The HTTP transport against the server on a local socket, and against minimal servers of the
//! test for what the server does not do: answering slowly, too much, or not at all.
#![cfg(feature = "native")]

mod common;

use common::*;
use futures::StreamExt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use zemdb_client::{
    Client, ClientConfig, ClientError, EventBlocks, EventStream, HttpTransport,
    HttpTransportConfig, MemoryMetadata, TransportClient,
};
use zemdb_core::*;
use zemdb_server::{generate_client_token, serve_until_shutdown};
use zemdb_storage::MemoryStorageEngine;

/// Serves `server` on a local socket until the returned sender is dropped.
async fn serve(server: &TestServer) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let signal = async move {
        let _ = stopped.await;
    };
    let state = server.state.clone();
    tokio::spawn(serve_until_shutdown(
        listener,
        state,
        signal,
        Duration::from_secs(10),
    ));
    (addr, stop)
}

/// A TCP front of a server that forwards each connection, and keeps its first bytes, until it
/// goes silent: then it drops the connections it forwards, and reads each new one without ever
/// answering.
struct Front {
    addr: SocketAddr,
    silent: Arc<AtomicBool>,
    starts: Arc<Mutex<Vec<Vec<u8>>>>,
    forwarding: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Front {
    async fn new(server: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = Self {
            addr: listener.local_addr().unwrap(),
            silent: Arc::default(),
            starts: Arc::default(),
            forwarding: Arc::default(),
        };
        let (silent, starts) = (Arc::clone(&front.silent), Arc::clone(&front.starts));
        let forwarding = Arc::clone(&front.forwarding);
        tokio::spawn(async move {
            loop {
                let (mut client, _) = listener.accept().await.unwrap();
                if silent.load(Ordering::SeqCst) {
                    tokio::spawn(async move {
                        let mut buf = vec![0; 64 << 10];
                        while client.read(&mut buf).await.is_ok_and(|n| n > 0) {}
                    });
                    continue;
                }
                let starts = Arc::clone(&starts);
                let task = tokio::spawn(async move {
                    let mut upstream = TcpStream::connect(server).await.unwrap();
                    let mut start = vec![0; 24];
                    // The pool may open a connection it never uses, and close it idle.
                    let n = client.read(&mut start).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    starts.lock().unwrap().push(start[..n].to_vec());
                    upstream.write_all(&start[..n]).await.unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                });
                forwarding.lock().unwrap().push(task);
            }
        });
        front
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Goes silent, dropping the connections it forwards.
    fn silence(&self) {
        self.silent.store(true, Ordering::SeqCst);
        for task in self.forwarding.lock().unwrap().drain(..) {
            task.abort();
        }
    }

    /// The connections forwarded so far.
    fn connections(&self) -> usize {
        self.starts.lock().unwrap().len()
    }

    /// Whether every connection forwarded so far started with the preface of HTTP/2.
    fn all_http2(&self) -> bool {
        let starts = self.starts.lock().unwrap();
        starts
            .iter()
            .all(|start| start.starts_with(b"PRI * HTTP/2.0"))
    }

    /// The connections forwarded so far that started with `prefix`.
    fn started_with(&self, prefix: &[u8]) -> usize {
        let starts = self.starts.lock().unwrap();
        starts
            .iter()
            .filter(|start| start.starts_with(prefix))
            .count()
    }

    /// Whether every connection forwarded so far started with an HTTP/1.1 `POST`.
    fn all_http1(&self) -> bool {
        let starts = self.starts.lock().unwrap();
        starts
            .iter()
            .all(|start| start.starts_with(b"POST /rooms/"))
    }
}

fn transport(url: String, http2: bool) -> HttpTransport {
    let mut config = HttpTransportConfig::new(url);
    config.http2_prior_knowledge = http2;
    HttpTransport::new(config).unwrap()
}

/// A transport whose requests are cut after `idle` without advancing.
fn idle_transport(addr: SocketAddr, idle: Duration) -> HttpTransport {
    let mut config = HttpTransportConfig::new(format!("http://{addr}"));
    config.idle_timeout = idle;
    HttpTransport::new(config).unwrap()
}

async fn open(transport: HttpTransport, config: ClientConfig) -> Client {
    let (storage, metadata) = (MemoryStorageEngine::new(), MemoryMetadata::default());
    Client::open(config, storage, metadata, transport, TestTokens::new())
        .await
        .unwrap()
}

fn heartbeat(room_id: &RoomId) -> ClientMessage {
    ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(1),
        room_id: room_id.clone(),
        client_id: ClientId::new("ana#phone").unwrap(),
    }
}

/// An upload of a chunk of `len` bytes, larger than what the sockets of a connection hold.
fn upload(len: usize) -> ClientMessage {
    ClientMessage::UploadSnapshotChunk {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("room-a").unwrap(),
        snapshot_head_seq: SequenceNumber::new(1),
        chunk_index: 0,
        total_chunks: 1,
        total_bytes: len as u64,
        snapshot_hash: [0; 32],
        data: vec![7; len].into(),
        room_incarnation: 1,
    }
}

/// The error code of the frame `bytes`.
fn error_code(bytes: &[u8]) -> ErrorCode {
    match decode_message::<ServerMessage>(bytes).unwrap() {
        ServerMessage::Error { code, .. } => code,
        other => panic!("Not an error: {other:?}"),
    }
}

/// A client over the transport opens a room, writes, syncs a write of another client and
/// sends a heartbeat, over `http2` or HTTP/1.1.
async fn round_trip(http2: bool) {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (addr, _stop) = serve(&server).await;
    let front = Front::new(addr).await;
    let client = open(transport(front.url(), http2), ClientConfig::new("ana")).await;
    let client_token = token(&client.client_id(), &room_id);
    client
        .open_room(room_id.clone(), client_token)
        .await
        .unwrap();
    client.write(room_id.clone(), insert_task(1)).await.unwrap();

    let other = open(transport(front.url(), http2), ClientConfig::new("bob")).await;
    let other_token = token(&other.client_id(), &room_id);
    other.open_room(room_id.clone(), other_token).await.unwrap();
    other.write(room_id.clone(), insert_task(2)).await.unwrap();

    client.sync(room_id.clone()).await.unwrap();
    client.heartbeat(room_id.clone()).await.unwrap();
    assert_eq!(
        client.status(room_id.clone()).await.unwrap().head_seq.get(),
        2
    );
    assert!(front.connections() > 0);
    if http2 {
        assert!(front.all_http2());
    } else {
        assert!(front.all_http1());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_registers_writes_syncs_and_sends_heartbeats_over_http1() {
    round_trip(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_registers_writes_syncs_and_sends_heartbeats_over_http2() {
    round_trip(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_errors_of_the_server_arrive_as_its_frames_whatever_the_http_status() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let other_room = server.create_room("room-b").await;
    let (addr, _stop) = serve(&server).await;
    let client_id = ClientId::new("ana#phone").unwrap();
    for http2 in [false, true] {
        let transport = transport(format!("http://{addr}"), http2);
        let expired = generate_client_token(&client_id, &room_id, Duration::ZERO, AUTH_SECRET);
        let reply = transport.send(&heartbeat(&room_id), &expired).await;
        assert_eq!(error_code(&reply.unwrap()), ErrorCode::Unauthorized);

        let elsewhere = token(&client_id, &other_room);
        let reply = transport.send(&heartbeat(&room_id), &elsewhere).await;
        assert_eq!(error_code(&reply.unwrap()), ErrorCode::Forbidden);
    }
}

/// Answers the first request of the first connection with `head` and then `body` in pieces of
/// `piece` bytes, `pause` apart, after reading the request until its end; `written` counts the
/// bytes of `body` the socket took.
async fn answering(
    head: String,
    body: Vec<u8>,
    piece: usize,
    pause: Duration,
    written: Arc<AtomicUsize>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket, None).await;
        socket.write_all(head.as_bytes()).await.unwrap();
        for piece in body.chunks(piece) {
            if socket.write_all(piece).await.is_err() {
                return;
            }
            written.fetch_add(piece.len(), Ordering::SeqCst);
            tokio::time::sleep(pause).await;
        }
        // Keeps the connection open, so its end does not end the response.
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    addr
}

/// Reads an HTTP/1.1 request until the end of the body its `Content-Length` gives (none, without
/// it), `pace` giving how many bytes to read before which pause while its body is read, if any.
async fn read_request(
    socket: &mut TcpStream,
    pace: Option<&(dyn Fn(usize) -> (usize, Duration) + Sync)>,
) {
    let mut received = Vec::new();
    let mut buf = vec![0; 1 << 20];
    let (head_len, body_len) = loop {
        let n = socket.read(&mut buf[..64 << 10]).await.unwrap();
        assert!(n > 0, "The request ended before its headers");
        received.extend_from_slice(&buf[..n]);
        if let Some(end) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&received[..end]).to_lowercase();
            let length = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "));
            let length = length.map_or(0, |length| length.trim().parse::<usize>().unwrap());
            break (end + 4, length);
        }
    };
    let mut read = received.len() - head_len;
    while read < body_len {
        let (want, pause) = pace.map_or((buf.len(), Duration::ZERO), |pace| pace(read));
        let n = socket
            .read(&mut buf[..want.min(body_len - read)])
            .await
            .unwrap();
        assert!(n > 0, "The request ended before its body");
        read += n;
        tokio::time::sleep(pause).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_response_that_advances_is_not_cut() {
    let idle = Duration::from_secs(1);
    let body = vec![1; 896 << 10];
    let head = format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n", body.len());
    let pause = Duration::from_millis(300);
    let written = Arc::default();
    let addr = answering(head, body.clone(), 64 << 10, pause, written).await;
    let (transport, message) = (
        idle_transport(addr, idle),
        heartbeat(&RoomId::new("r").unwrap()),
    );
    let started = Instant::now();
    assert_eq!(transport.send(&message, "t").await.unwrap(), body);
    assert!(started.elapsed() > idle * 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_body_the_server_takes_slowly_is_not_cut() {
    let idle = Duration::from_secs(2);
    let addr = accepting(1 << 20, |mut socket| async move {
        // Slowly while the client has more of the body than the sockets hold, which it hands
        // over as they take it (a socket of the client is writable again once a third of its
        // buffer, about 1.3 MiB over loopback in Linux, went out); then at once.
        let pace = |read: usize| match read < 10 << 20 {
            true => (512 << 10, Duration::from_millis(250)),
            false => (1 << 20, Duration::ZERO),
        };
        read_request(&mut socket, Some(&pace)).await;
        let reply = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
        socket.write_all(reply.as_bytes()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let transport = idle_transport(addr.await, idle);
    let started = Instant::now();
    let reply = transport.send(&upload(15 << 20), "t").await;
    assert_eq!(reply.unwrap(), b"ok");
    assert!(started.elapsed() > idle * 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_response_longer_than_a_frame_by_its_length_is_not_read() {
    let length = MAX_FRAME_SIZE + 1;
    let head = format!("HTTP/1.1 200 OK\r\ncontent-length: {length}\r\n\r\n");
    let written = Arc::new(AtomicUsize::new(0));
    let addr = answering(head, vec![1; 64 << 10], 1, Duration::from_secs(60), written).await;
    let started = Instant::now();
    let transport = idle_transport(addr, Duration::from_secs(30));
    let message = heartbeat(&RoomId::new("r").unwrap());
    assert!(transport.send(&message, "t").await.is_err());
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_response_longer_than_a_frame_without_a_length_is_not_read_whole() {
    let head = "HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n".to_string();
    let len = 4 * MAX_FRAME_SIZE;
    let written = Arc::new(AtomicUsize::new(0));
    let addr = answering(
        head,
        vec![1; len],
        1 << 20,
        Duration::ZERO,
        Arc::clone(&written),
    )
    .await;
    let transport = idle_transport(addr, Duration::from_secs(30));
    let message = heartbeat(&RoomId::new("r").unwrap());
    assert!(transport.send(&message, "t").await.is_err());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(written.load(Ordering::SeqCst) < len);
}

/// Accepts one connection, with a receive buffer of `buffer` bytes, and runs `then` on it.
async fn accepting<F>(buffer: u32, then: impl FnOnce(TcpStream) -> F + Send + 'static) -> SocketAddr
where
    F: std::future::Future<Output = ()> + Send,
{
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(buffer).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(8).unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        then(socket).await;
    });
    addr
}

/// `send` fails within a few times `idle`, and not before it.
async fn cut_after(transport: HttpTransport, message: ClientMessage, idle: Duration) {
    let started = Instant::now();
    let sent = tokio::time::timeout(idle * 10, transport.send(&message, "t")).await;
    assert!(matches!(sent, Ok(Err(_))), "{sent:?}");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= idle && elapsed < idle * 10,
        "Cut after {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_silent_before_the_headers_is_cut() {
    let idle = Duration::from_millis(500);
    let addr = accepting(64 << 10, |mut socket| async move {
        read_request(&mut socket, None).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let message = heartbeat(&RoomId::new("r").unwrap());
    cut_after(idle_transport(addr.await, idle), message, idle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_silent_in_the_middle_of_the_response_is_cut() {
    let idle = Duration::from_millis(500);
    let addr = accepting(64 << 10, |mut socket| async move {
        read_request(&mut socket, None).await;
        let head = "HTTP/1.1 200 OK\r\ncontent-length: 1000\r\n\r\npart";
        socket.write_all(head.as_bytes()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let message = heartbeat(&RoomId::new("r").unwrap());
    cut_after(idle_transport(addr.await, idle), message, idle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_silent_while_it_receives_the_body_is_cut() {
    let idle = Duration::from_millis(500);
    let addr = accepting(64 << 10, |socket| async move {
        let _socket = socket;
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    cut_after(idle_transport(addr.await, idle), upload(15 << 20), idle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_port_is_a_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let transport = idle_transport(addr, Duration::from_secs(30));
    let message = heartbeat(&RoomId::new("r").unwrap());
    assert!(transport.send(&message, "t").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_through_a_silent_server_is_uncertain() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (addr, _stop) = serve(&server).await;
    let front = Front::new(addr).await;
    let mut config = HttpTransportConfig::new(front.url());
    config.idle_timeout = Duration::from_millis(300);
    let mut client_config = ClientConfig::new("ana");
    client_config.write_timeout = Duration::from_secs(2);
    let client = open(HttpTransport::new(config).unwrap(), client_config).await;
    let token = token(&client.client_id(), &room_id);
    client.open_room(room_id.clone(), token).await.unwrap();

    front.silence();
    let write = client.write(room_id.clone(), insert_task(1)).await;
    assert_eq!(write, Err(ClientError::OutcomeUnknown));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_idle_past_the_pool_timeout_is_not_reused() {
    let server = TestServer::with(|config| config.header_read_timeout_secs = 1);
    let room_id = server.create_room("room-a").await;
    let (addr, _stop) = serve(&server).await;
    let front = Front::new(addr).await;
    let mut config = HttpTransportConfig::new(front.url());
    config.pool_idle_timeout = Duration::from_millis(300);
    let transport = HttpTransport::new(config).unwrap();
    let client_id = ClientId::new("ana#phone").unwrap();
    let token = token(&client_id, &room_id);
    let heartbeat = || async {
        let reply = transport.send(&heartbeat(&room_id), &token).await.unwrap();
        assert_eq!(error_code(&reply), ErrorCode::ClientNotRegistered);
    };
    heartbeat().await;
    // Gives the connection the time to go back to the pool.
    tokio::time::sleep(Duration::from_millis(100)).await;
    heartbeat().await;
    assert_eq!(front.connections(), 1);
    // Idle for longer than the pool keeps it, and less than the server does.
    tokio::time::sleep(Duration::from_millis(600)).await;
    heartbeat().await;
    assert_eq!(front.connections(), 2);
}

#[tokio::test]
async fn a_transport_that_cannot_work_is_config() {
    let urls = [
        "http://user:secret@localhost",
        "http://user@localhost",
        "localhost:3000",
        "ftp://localhost",
        "http://localhost/?a=1",
        "http://localhost/#a",
        "not a url",
    ];
    for url in urls {
        let transport = HttpTransport::new(HttpTransportConfig::new(url));
        assert!(matches!(transport, Err(ClientError::Config(_))), "{url}");
    }
    let mut config = HttpTransportConfig::new("http://localhost");
    config.idle_timeout = Duration::ZERO;
    let transport = HttpTransport::new(config.clone());
    assert!(matches!(transport, Err(ClientError::Config(_))));
    config.idle_timeout = Duration::from_secs(1);
    config.connect_timeout = Duration::ZERO;
    let transport = HttpTransport::new(config.clone());
    assert!(matches!(transport, Err(ClientError::Config(_))));
    config.connect_timeout = Duration::from_secs(1);
    for longest in [Duration::from_secs(301), Duration::MAX] {
        config.pool_idle_timeout = longest;
        let transport = HttpTransport::new(config.clone());
        assert!(matches!(transport, Err(ClientError::Config(_))));
    }
    config.pool_idle_timeout = Duration::from_secs(300);
    assert!(HttpTransport::new(config).is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_url_with_a_path_takes_the_endpoints_under_it() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (seen, received) = oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.unwrap();
        let _ = seen.send(String::from_utf8_lossy(&buf[..n]).to_string());
        let reply = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
        socket.write_all(reply.as_bytes()).await.unwrap();
    });
    let transport = transport(format!("http://{addr}/zemdb/"), false);
    let room_id = RoomId::new("room-a").unwrap();
    transport
        .send(&heartbeat(&room_id), "secret")
        .await
        .unwrap();
    let request = received.await.unwrap();
    assert!(request.starts_with("POST /zemdb/rooms/room-a/heartbeat HTTP/1.1\r\n"));
    let request = request.to_lowercase();
    assert!(request.contains("authorization: bearer secret\r\n"));
    assert!(request.contains("content-type: application/octet-stream\r\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_redirect_is_not_followed() {
    let body = b"the body of the redirect".to_vec();
    let head = format!(
        "HTTP/1.1 302 Found\r\nlocation: /elsewhere\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    let written = Arc::default();
    let addr = answering(head, body.clone(), 1 << 10, Duration::ZERO, written).await;
    let transport = idle_transport(addr, Duration::from_secs(1));
    let message = heartbeat(&RoomId::new("r").unwrap());
    assert_eq!(transport.send(&message, "t").await.unwrap(), body);
}

/// A server that answers a response of exactly the largest frame, with its length or without.
async fn exactly_a_frame(with_length: bool) -> SocketAddr {
    let head = match with_length {
        true => format!("HTTP/1.1 200 OK\r\ncontent-length: {MAX_FRAME_SIZE}\r\n\r\n"),
        false => "HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n".to_string(),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket, None).await;
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(&vec![1; MAX_FRAME_SIZE]).await.unwrap();
        if with_length {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_response_of_exactly_a_frame_is_read_whole() {
    for with_length in [true, false] {
        let transport = idle_transport(exactly_a_frame(with_length).await, Duration::from_secs(30));
        let message = heartbeat(&RoomId::new("r").unwrap());
        let reply = transport.send(&message, "t").await.unwrap();
        assert_eq!(
            reply.len(),
            MAX_FRAME_SIZE,
            "With its length: {with_length}"
        );
    }
}

/// The first bytes the transport sends to an `https://` server that never answers, for a
/// request or, if `stream`, for an event stream: its TLS `ClientHello`.
async fn client_hello(http2_prior_knowledge: bool, stream: bool) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (seen, hello) = oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // A TLS record: its type, its version, its length in two bytes, and the record.
        let mut hello = vec![0; 5];
        socket.read_exact(&mut hello).await.unwrap();
        let len = u16::from_be_bytes([hello[3], hello[4]]) as usize;
        hello.resize(5 + len, 0);
        socket.read_exact(&mut hello[5..]).await.unwrap();
        let _ = seen.send(hello);
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let mut config = HttpTransportConfig::new(format!("https://{addr}"));
    config.http2_prior_knowledge = http2_prior_knowledge;
    config.idle_timeout = Duration::from_millis(500);
    let transport = HttpTransport::new(config).unwrap();
    let room_id = RoomId::new("r").unwrap();
    if stream {
        assert!(transport.events(&room_id, "t").await.is_err());
    } else {
        assert!(transport.send(&heartbeat(&room_id), "t").await.is_err());
    }
    let hello = tokio::time::timeout(Duration::from_secs(5), hello);
    hello.await.expect("No TLS handshake").unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_offers_http2_and_http1_in_the_tls_handshake_also_with_prior_knowledge() {
    for http2_prior_knowledge in [false, true] {
        let hello = client_hello(http2_prior_knowledge, false).await;
        // A TLS handshake record whose protocols (ALPN) are h2 and http/1.1.
        assert_eq!(hello.first(), Some(&0x16));
        assert!(hello.windows(3).any(|bytes| bytes == b"\x02h2"));
        assert!(hello.windows(9).any(|bytes| bytes == b"\x08http/1.1"));
    }
}

/// A TCP front of a server that forwards each connection and counts those it accepts. Its
/// freeze makes the connections open at that moment stop carrying bytes, their sockets still
/// open; those that open afterwards are forwarded.
struct FreezingFront {
    addr: SocketAddr,
    open: Arc<Mutex<Vec<Arc<AtomicBool>>>>,
    accepted: Arc<AtomicUsize>,
}

impl FreezingFront {
    async fn new(server: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = Self {
            addr: listener.local_addr().unwrap(),
            open: Arc::default(),
            accepted: Arc::default(),
        };
        let (open, accepted) = (Arc::clone(&front.open), Arc::clone(&front.accepted));
        tokio::spawn(async move {
            loop {
                let (client, _) = listener.accept().await.unwrap();
                accepted.fetch_add(1, Ordering::SeqCst);
                let upstream = TcpStream::connect(server).await.unwrap();
                let frozen = Arc::new(AtomicBool::new(false));
                open.lock().unwrap().push(Arc::clone(&frozen));
                let (client_read, client_write) = client.into_split();
                let (upstream_read, upstream_write) = upstream.into_split();
                tokio::spawn(carry(
                    client_read,
                    upstream_write,
                    Some(Arc::clone(&frozen)),
                ));
                tokio::spawn(carry(upstream_read, client_write, Some(frozen)));
            }
        });
        front
    }

    fn freeze(&self) {
        for frozen in self.open.lock().unwrap().iter() {
            frozen.store(true, Ordering::SeqCst);
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

/// Copies `from` into `to`, holding what it read while `frozen` is set.
async fn carry(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    frozen: Option<Arc<AtomicBool>>,
) {
    let mut buf = vec![0; 64 << 10];
    while let Ok(n @ 1..) = from.read(&mut buf).await {
        while frozen
            .as_ref()
            .is_some_and(|frozen| frozen.load(Ordering::SeqCst))
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_http2_connection_that_stops_answering_is_replaced() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (addr, _stop) = serve(&server).await;
    let front = FreezingFront::new(addr).await;
    let mut config = HttpTransportConfig::new(format!("http://{}", front.addr));
    config.http2_prior_knowledge = true;
    config.idle_timeout = Duration::from_millis(400);
    let transport = HttpTransport::new(config).unwrap();
    let client_id = ClientId::new("ana#phone").unwrap();
    let token = token(&client_id, &room_id);
    let message = heartbeat(&room_id);
    transport.send(&message, &token).await.unwrap();

    // The request cut for not advancing leaves the connection, but the next one goes over
    // another, although requests come more often than the pool would let the first one go;
    // and so again with the second connection.
    for connections in [2, 3] {
        front.freeze();
        assert!(transport.send(&message, &token).await.is_err());
        transport.send(&message, &token).await.unwrap();
        assert_eq!(front.accepted(), connections);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeats_of_the_worker_leave_behind_an_http2_connection_that_stops_answering() {
    // A write lets its request go at its deadline, before the transport cuts it for not
    // advancing, so the transport keeps the connection that stopped answering; the heartbeats of
    // the worker, which the client does not cut, leave it behind.
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (addr, _stop) = serve(&server).await;
    let front = FreezingFront::new(addr).await;
    let mut config = HttpTransportConfig::new(format!("http://{}", front.addr));
    config.http2_prior_knowledge = true;
    config.idle_timeout = Duration::from_millis(400);
    let transport = HttpTransport::new(config).unwrap();
    let config = ClientConfig {
        write_timeout: Duration::from_millis(200),
        heartbeat_interval: Duration::from_millis(300),
        retry_base: Duration::from_millis(50),
        retry_max: Duration::from_millis(100),
        ..ClientConfig::new("ana")
    };
    let client = Arc::new(open(transport, config).await);
    let token = token(&client.client_id(), &room_id);
    client.open_room(room_id.clone(), token).await.unwrap();
    front.freeze();
    for id in 1..=3 {
        let written = client.write(room_id.clone(), insert_task(id)).await;
        assert!(written.is_err(), "{written:?}");
    }
    assert_eq!(front.accepted(), 1);

    client.start_worker(room_id.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(front.accepted(), 2);
    client.write(room_id.clone(), insert_task(4)).await.unwrap();
}

/// A TCP front of a server that carries what the client sends at `rate` bytes per second for
/// its first `limited` bytes, and at once afterwards. It follows a schedule from its start, so a
/// timer that wakes late (by up to a tick of the system clock, 15.6 ms in Windows) does not
/// slow it down.
async fn throttling_front(server: SocketAddr, rate: usize, limited: usize) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let upstream = TcpStream::connect(server).await.unwrap();
        let (mut client_read, client_write) = client.into_split();
        let (upstream_read, mut upstream_write) = upstream.into_split();
        tokio::spawn(carry(upstream_read, client_write, None));
        let (mut buf, mut carried, start) = (vec![0; 16 << 10], 0, Instant::now());
        while let Ok(n @ 1..) = client_read.read(&mut buf).await {
            carried += n;
            if carried <= limited {
                let due = start + Duration::from_secs_f64(carried as f64 / rate as f64);
                tokio::time::sleep_until(due.into()).await;
            }
            if upstream_write.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_request_body_over_http2_is_not_cut() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let token = token(&ClientId::new("ana#phone").unwrap(), &room_id);
    let (addr, _stop) = serve(&server).await;
    let front = throttling_front(addr, 1 << 20, 3 << 20).await;
    let mut config = HttpTransportConfig::new(format!("http://{front}"));
    config.http2_prior_knowledge = true;
    let idle = Duration::from_millis(750);
    config.idle_timeout = idle;
    let transport = HttpTransport::new(config).unwrap();
    let started = Instant::now();
    // The server reads the whole upload, and answers it with an error frame: the room
    // designated no one.
    let reply = transport.send(&upload(4 << 20), &token).await.unwrap();
    assert!(decode_message::<ServerMessage>(&reply).is_ok());
    assert!(started.elapsed() > idle * 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_a_response_cut_halfway_over_http2_the_next_request_goes_over_another_connection() {
    // The first response stops after its first piece; the others come whole.
    let answered = Arc::new(AtomicUsize::new(0));
    let handler = move || {
        let first = answered.fetch_add(1, Ordering::SeqCst) == 0;
        async move {
            let piece = futures::stream::once(async { Ok::<_, std::io::Error>("part") });
            match first {
                true => axum::body::Body::from_stream(piece.chain(futures::stream::pending())),
                false => axum::body::Body::from("whole"),
            }
        }
    };
    let router = axum::Router::new().route("/rooms/r/heartbeat", axum::routing::post(handler));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });
    let front = FreezingFront::new(addr).await;
    let mut config = HttpTransportConfig::new(format!("http://{}", front.addr));
    config.http2_prior_knowledge = true;
    config.idle_timeout = Duration::from_millis(300);
    let transport = HttpTransport::new(config).unwrap();
    let message = heartbeat(&RoomId::new("r").unwrap());

    let cut = tokio::time::timeout(Duration::from_secs(5), transport.send(&message, "t")).await;
    assert!(matches!(cut, Ok(Err(_))), "{cut:?}");
    assert_eq!(transport.send(&message, "t").await.unwrap(), b"whole");
    assert_eq!(front.accepted(), 2);
}

/// Opens the event stream of `room_id` with `token`, which the server must open.
async fn open_stream(transport: &HttpTransport, room_id: &RoomId, token: &str) -> EventBlocks {
    match transport.events(room_id, token).await.unwrap() {
        EventStream::Open(blocks) => blocks,
        EventStream::Refused(refusal) => panic!("Refused: {:?}", error_code(&refusal)),
    }
}

/// The error code with which the server refuses the event stream of `room_id` with `token`.
async fn refused(transport: &HttpTransport, room_id: &RoomId, token: &str) -> ErrorCode {
    match transport.events(room_id, token).await.unwrap() {
        EventStream::Refused(refusal) => error_code(&refusal),
        EventStream::Open(_) => panic!("Opened"),
    }
}

/// Waits until `blocks` ends, within 10 s.
async fn ends(blocks: &mut EventBlocks) {
    let end = async { while let Some(Ok(_)) = blocks.next().await {} };
    let end = tokio::time::timeout(Duration::from_secs(10), end).await;
    end.expect("The stream did not end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_event_stream_brings_the_operations_of_a_commit_of_another_client() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let incarnation = server.incarnation(&room_id).await;
    let (addr, _stop) = serve(&server).await;
    let token = token(&ClientId::new("ana#phone").unwrap(), &room_id);
    for (id, http2) in [(1, false), (2, true)] {
        let transport = transport(format!("http://{addr}"), http2);
        let mut blocks = open_stream(&transport, &room_id, &token).await;
        let committed = server.commit(&room_id, [id]).await;
        let event = ops_event(&next_event(&mut blocks, "ops").await);
        let ServerMessage::OpsEvent {
            room_id: of,
            room_incarnation,
            ops,
            ..
        } = event
        else {
            panic!("{event:?}");
        };
        assert_eq!(
            (&of, room_incarnation, ops),
            (&room_id, incarnation, committed)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_the_stream_is_refused_with_the_frame_of_the_server() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let other_room = server.create_room("room-b").await;
    let (addr, _stop) = serve(&server).await;
    let to_server = transport(format!("http://{addr}"), false);
    let client_id = ClientId::new("ana#phone").unwrap();
    let expired = generate_client_token(&client_id, &room_id, Duration::ZERO, AUTH_SECRET);
    for token in [expired.as_str(), "", "not a token"] {
        let refusal = refused(&to_server, &room_id, token).await;
        assert_eq!(refusal, ErrorCode::Unauthorized, "{token}");
    }
    let elsewhere = token(&client_id, &other_room);
    let refusal = refused(&to_server, &room_id, &elsewhere).await;
    assert_eq!(refusal, ErrorCode::Forbidden);

    let disabled = TestServer::with(|config| config.max_sse_streams = Some(0));
    let room_id = disabled.create_room("room-a").await;
    let (addr, _stop) = serve(&disabled).await;
    let transport = transport(format!("http://{addr}"), false);
    let refusal = refused(&transport, &room_id, &token(&client_id, &room_id)).await;
    assert_eq!(refusal, ErrorCode::EventsDisabled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_limits_of_the_server_refuse_a_stream_or_end_the_oldest_of_a_client() {
    let server = TestServer::with(|config| config.max_sse_streams = Some(2));
    let room_id = server.create_room("room-a").await;
    let (addr, _stop) = serve(&server).await;
    let transport = transport(format!("http://{addr}"), false);
    let ana = token(&ClientId::new("ana#phone").unwrap(), &room_id);
    let mut first = open_stream(&transport, &room_id, &ana).await;
    let _second = open_stream(&transport, &room_id, &ana).await;
    let bob = token(&ClientId::new("bob#phone").unwrap(), &room_id);
    assert_eq!(
        refused(&transport, &room_id, &bob).await,
        ErrorCode::RateLimited
    );
    let _third = open_stream(&transport, &room_id, &ana).await;
    ends(&mut first).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stream_ends_when_its_token_expires() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (addr, _stop) = serve(&server).await;
    let transport = transport(format!("http://{addr}"), false);
    let client_id = ClientId::new("ana#phone").unwrap();
    let short = generate_client_token(&client_id, &room_id, Duration::from_secs(2), AUTH_SECRET);
    let mut blocks = open_stream(&transport, &room_id, &short).await;
    ends(&mut blocks).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stream_ends_when_the_server_shuts_down() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (addr, stop) = serve(&server).await;
    let transport = transport(format!("http://{addr}"), false);
    let token = token(&ClientId::new("ana#phone").unwrap(), &room_id);
    let mut blocks = open_stream(&transport, &room_id, &token).await;
    drop(stop);
    ends(&mut blocks).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_go_over_connections_of_their_own_and_do_not_hold_back_the_requests() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let (addr, _stop) = serve(&server).await;
    let front = Front::new(addr).await;
    // The server takes 32 streams at once over an HTTP/2 connection.
    let transport = transport(front.url(), true);
    let opening = async {
        let mut streams = Vec::new();
        for i in 0..33 {
            let client_id = ClientId::new(format!("c{i}#phone")).unwrap();
            streams.push(open_stream(&transport, &room_id, &token(&client_id, &room_id)).await);
        }
        streams
    };
    let opening = tokio::time::timeout(Duration::from_secs(10), opening);
    let _streams = opening.await.expect("The streams did not open");
    let ana = token(&ClientId::new("ana#phone").unwrap(), &room_id);
    let heartbeat = heartbeat(&room_id);
    let reply = tokio::time::timeout(Duration::from_secs(5), transport.send(&heartbeat, &ana));
    let reply = reply.await.expect("The request was held back").unwrap();
    assert_eq!(error_code(&reply), ErrorCode::ClientNotRegistered);
    assert_eq!(front.started_with(b"GET /rooms/room-a/events"), 33);
    assert_eq!(front.started_with(b"PRI * HTTP/2.0"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stream_goes_under_the_url_with_its_token_and_brings_the_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (seen, received) = oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.unwrap();
        let _ = seen.send(String::from_utf8_lossy(&buf[..n]).to_string());
        let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\
                    transfer-encoding: chunked\r\n\r\n";
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(b"9\r\ndata: x\n\n\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        socket.write_all(b"0\r\n\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let transport = transport(format!("http://{addr}/zemdb/"), true);
    let room_id = RoomId::new("room-a").unwrap();
    let mut blocks = open_stream(&transport, &room_id, "secret").await;
    let mut body = Vec::new();
    while let Some(block) = blocks.next().await {
        body.extend_from_slice(&block.unwrap());
    }
    assert_eq!(body, b"data: x\n\n");
    let request = received.await.unwrap();
    assert!(request.starts_with("GET /zemdb/rooms/room-a/events HTTP/1.1\r\n"));
    let request = request.to_lowercase();
    assert!(request.contains("authorization: bearer secret\r\n"));
    assert!(request.contains("accept: text/event-stream\r\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_https_the_stream_offers_only_http1_in_the_tls_handshake() {
    for http2_prior_knowledge in [false, true] {
        let hello = client_hello(http2_prior_knowledge, true).await;
        assert_eq!(hello.first(), Some(&0x16));
        assert!(!hello.windows(3).any(|bytes| bytes == b"\x02h2"));
        assert!(hello.windows(9).any(|bytes| bytes == b"\x08http/1.1"));
    }
}

/// Opening the stream fails within a few times `idle`, and not before it.
async fn stream_cut_after(addr: SocketAddr, idle: Duration) {
    let transport = idle_transport(addr, idle);
    let started = Instant::now();
    let room_id = RoomId::new("r").unwrap();
    let opened = tokio::time::timeout(idle * 10, transport.events(&room_id, "t")).await;
    assert!(matches!(opened, Ok(Err(_))), "{opened:?}");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= idle && elapsed < idle * 10,
        "Cut after {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_silent_before_the_headers_of_the_stream_or_in_its_refusal_is_cut() {
    let addr = accepting(64 << 10, |mut socket| async move {
        let mut buf = vec![0; 64 << 10];
        while socket.read(&mut buf).await.is_ok_and(|n| n > 0) {}
    })
    .await;
    let idle = Duration::from_millis(300);
    stream_cut_after(addr, idle).await;

    let head = "HTTP/1.1 404 Not Found\r\ncontent-length: 100\r\n\r\n".to_string();
    let addr = answering(
        head,
        vec![0; 100],
        10,
        Duration::from_secs(10),
        Arc::default(),
    )
    .await;
    stream_cut_after(addr, idle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_is_read_up_to_a_frame() {
    let refusal = b"the refusal".to_vec();
    let head = format!(
        "HTTP/1.1 429 Too Many Requests\r\ncontent-length: {}\r\n\r\n",
        refusal.len()
    );
    let addr = answering(head, refusal.clone(), 4, Duration::ZERO, Arc::default()).await;
    let transport = idle_transport(addr, Duration::from_secs(5));
    let opened = transport.events(&RoomId::new("r").unwrap(), "t").await;
    assert!(matches!(opened, Ok(EventStream::Refused(read)) if read == refusal));

    let head = format!(
        "HTTP/1.1 404 Not Found\r\ncontent-length: {}\r\n\r\n",
        MAX_FRAME_SIZE + 1
    );
    let written = Arc::new(AtomicUsize::new(0));
    let body = vec![0; MAX_FRAME_SIZE + 1];
    let addr = answering(head, body, 64 << 10, Duration::ZERO, Arc::clone(&written)).await;
    let transport = idle_transport(addr, Duration::from_secs(5));
    let opened = transport.events(&RoomId::new("r").unwrap(), "t").await;
    assert!(opened.is_err(), "{opened:?}");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(written.load(Ordering::SeqCst) <= MAX_FRAME_SIZE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_redirect_or_another_success_of_the_stream_is_a_refusal() {
    let body = b"the body of the redirect".to_vec();
    let head = format!(
        "HTTP/1.1 302 Found\r\nlocation: /elsewhere\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    let addr = answering(head, body.clone(), 1 << 10, Duration::ZERO, Arc::default()).await;
    let transport = idle_transport(addr, Duration::from_secs(1));
    let opened = transport.events(&RoomId::new("r").unwrap(), "t").await;
    assert!(
        matches!(&opened, Ok(EventStream::Refused(read)) if *read == body),
        "{opened:?}"
    );

    let head = "HTTP/1.1 204 No Content\r\n\r\n".to_string();
    let addr = answering(head, vec![], 1, Duration::ZERO, Arc::default()).await;
    let transport = idle_transport(addr, Duration::from_secs(1));
    let opened = transport.events(&RoomId::new("r").unwrap(), "t").await;
    assert!(
        matches!(&opened, Ok(EventStream::Refused(read)) if read.is_empty()),
        "{opened:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_connection_idle_past_the_pool_timeout_is_not_reused() {
    let server = TestServer::with(|config| config.header_read_timeout_secs = 1);
    let room_id = server.create_room("room-a").await;
    let (addr, _stop) = serve(&server).await;
    let front = Front::new(addr).await;
    let mut config = HttpTransportConfig::new(front.url());
    config.pool_idle_timeout = Duration::from_millis(300);
    let transport = HttpTransport::new(config).unwrap();
    // A refusal read whole leaves its connection in the pool.
    assert_eq!(
        refused(&transport, &room_id, "").await,
        ErrorCode::Unauthorized
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        refused(&transport, &room_id, "").await,
        ErrorCode::Unauthorized
    );
    assert_eq!(front.connections(), 1);
    // Idle for longer than the pool keeps it, and less than the server does.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        refused(&transport, &room_id, "").await,
        ErrorCode::Unauthorized
    );
    assert_eq!(front.connections(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_cut_in_the_middle_ends_with_an_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 4096];
        let _ = socket.read(&mut buf).await.unwrap();
        let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\
                    transfer-encoding: chunked\r\n\r\n";
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(b"9\r\ndata: x\n\n\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Closed without the last chunk: the stream was cut, it did not end.
    });
    let transport = transport(format!("http://{addr}"), false);
    let room_id = RoomId::new("room-a").unwrap();
    let mut blocks = open_stream(&transport, &room_id, "t").await;
    assert_eq!(&blocks.next().await.unwrap().unwrap()[..], b"data: x\n\n");
    let next = blocks.next().await;
    assert!(matches!(next, Some(Err(_))), "{next:?}");
}
