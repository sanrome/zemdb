//! The test server of the browser harness (`tools/browser/README.md`).
//!
//! One origin on `127.0.0.1`, over HTTP/1.1, that serves:
//! - the router of `zemdb-server` over a temporary data directory, built as the simulation
//!   builds it, on its own paths (`/rooms/...` and `/admin/...`);
//! - the files of the page, from the directory given with `--www`;
//! - the routes of the harness, under `/harness/`, all `POST`:
//!   - `/harness/admin/...`: the Control Plane request `/admin/...`, with the admin secret;
//!   - `/harness/token`: a client token for `{"client_id", "room_id"}`, valid for `ttl_secs`
//!     (300 by default);
//!   - `/harness/network`: `{"up": false}` cuts the network between the page and the router,
//!     and `{"up": true}` restores it.
//!
//! It prints `listening on http://127.0.0.1:<port>/` once it accepts connections. It stops when
//! its standard input closes (the runner that started it ends) or on Ctrl+C: it closes the
//! connections, shuts every room down and deletes its data directory. It exits with an error if
//! a connection panicked: a panic of the router, which would otherwise reach the page only as a
//! closed connection.

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, Method, Request, Response, StatusCode, Uri};
use axum::Router;
use bytes::Bytes;
use http_body::{Frame, SizeHint};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinError, JoinSet};
use tower::ServiceExt;
use tracing_subscriber::EnvFilter;
use zemdb_core::id::{ClientId, RoomId};
use zemdb_server::{
    build_router, generate_client_token, AppState, IoThreads, RoomManager, SchemaRegistry,
    ServerConfig, SnapshotRelay,
};

/// The secret of the client tokens. The server only listens on `127.0.0.1`, for the tests.
const AUTH_SECRET: &str = "zemdb browser harness: client tokens";
/// The admin secret, which only this server sends, to its own router.
const ADMIN_SECRET: &str = "zemdb browser harness: control plane";
/// Time the connections get, once the server stops, to finish their request in progress.
const GRACE: Duration = Duration::from_secs(5);
/// The largest body of a request to `/harness/token` or `/harness/network`.
const MAX_HARNESS_BODY: usize = 64 * 1024;
/// Validity of a client token when the request does not give one.
const DEFAULT_TOKEN_TTL_SECS: u64 = 300;

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse()?;
    let dir = tempfile::tempdir()?;
    let config = ServerConfig {
        data_dir: dir.path().join("data"),
        auth_secret: AUTH_SECRET.to_string(),
        admin_secret: ADMIN_SECRET.to_string(),
        ..ServerConfig::default()
    };
    config.validate_secrets()?;
    let runtime = IoThreads::runtime_builder(config.io_threads).build()?;
    let served = runtime.block_on(serve(config, args));
    // Dropping the runtime waits for the blocking work still in flight, every file operation
    // included, so the directory is deleted once nothing writes to it.
    drop(runtime);
    dir.close()?;
    served
}

/// The command line: `--www <dir> [--port <port>]`. Port 0, the default, is a free port.
struct Args {
    www: PathBuf,
    port: u16,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let usage = "usage: zemdb-browser-server --www <dir> [--port <port>]";
        let mut www = None;
        let mut port = 0;
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--www" => www = Some(PathBuf::from(args.next().ok_or(usage)?)),
                "--port" => {
                    port = args
                        .next()
                        .and_then(|port| port.parse().ok())
                        .ok_or(usage)?;
                }
                _ => return Err(format!("unknown argument {arg}; {usage}")),
            }
        }
        Ok(Self {
            www: www.ok_or(usage)?,
            port,
        })
    }
}

async fn serve(config: ServerConfig, args: Args) -> Result<(), Box<dyn Error>> {
    let state = state(config)?;
    let listener = TcpListener::bind(("127.0.0.1", args.port)).await?;
    let (network, _) = watch::channel(true);
    let harness = Arc::new(Harness {
        router: build_router(state.clone()),
        www: args.www,
        network,
    });
    println!("listening on http://{}/", listener.local_addr()?);

    let stop = stop_signal();
    tokio::pin!(stop);
    let mut connections = JoinSet::new();
    let mut panics = 0;
    loop {
        tokio::select! {
            () = &mut stop => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    connections.spawn(serve_connection(stream, Arc::clone(&harness), state.clone()));
                }
                Err(err) => {
                    // Out of file descriptors, say: let some connections end first.
                    eprintln!("accepting a connection failed: {err}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                panics += panicked(&joined);
            }
        }
    }
    drop(listener);
    // Ends the event streams, and every connection once its request in progress ends.
    state.shutdown.trigger();
    let drained = tokio::time::timeout(GRACE, async {
        while let Some(joined) = connections.join_next().await {
            panics += panicked(&joined);
        }
    })
    .await;
    if drained.is_err() {
        connections.shutdown().await;
    }
    state.room_manager.shutdown_all().await;
    if panics > 0 {
        return Err(format!("{panics} connections panicked").into());
    }
    Ok(())
}

/// 1 if the task of a connection panicked, 0 if not.
fn panicked(joined: &Result<(), JoinError>) -> usize {
    usize::from(joined.as_ref().is_err_and(JoinError::is_panic))
}

/// The pieces of a server with `config`, as the simulation builds them
/// (`crates/client/tests/common/mod.rs`).
fn state(config: ServerConfig) -> Result<AppState, Box<dyn Error>> {
    let config = Arc::new(config);
    let registry = Arc::new(SchemaRegistry::new(config.data_dir.join("schemas"))?);
    let relay = Arc::new(SnapshotRelay::new(
        config.data_dir.join("snapshots"),
        Duration::from_secs(config.snapshot_ttl_secs),
        config.max_snapshot_bytes,
        Duration::from_secs(config.snapshot_upload_max_secs),
    )?);
    let manager = Arc::new(RoomManager::new(
        Arc::clone(&config),
        Arc::clone(&registry),
        Arc::clone(&relay),
        IoThreads::new(config.io_threads),
    ));
    Ok(AppState::new(config, registry, manager, relay))
}

/// Resolves when the standard input closes, or on Ctrl+C.
async fn stop_signal() {
    let (closed, stdin_closed) = oneshot::channel::<()>();
    // A thread of its own, not Tokio's `stdin`: a blocking read that never ends would keep the
    // runtime from shutting down.
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink());
        let _ = closed.send(());
    });
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        _ = stdin_closed => {}
        () = ctrl_c => {}
    }
}

/// Serves one connection until it closes; once the server stops, until its request in
/// progress ends.
async fn serve_connection(stream: TcpStream, harness: Arc<Harness>, state: AppState) {
    let service = service_fn(move |request: Request<Incoming>| {
        let harness = Arc::clone(&harness);
        async move { harness.handle(request.map(Body::new)).await }
    });
    let connection = http1::Builder::new().serve_connection(TokioIo::new(stream), service);
    tokio::pin!(connection);
    tokio::select! {
        _ = connection.as_mut() => return,
        () = state.shutdown.wait() => {}
    }
    connection.as_mut().graceful_shutdown();
    // A connection that ends with an error (the page went away, or the network was cut) is
    // not the server's failure.
    let _ = connection.as_mut().await;
}

/// The error with which the harness closes a connection without an answer, or cuts a
/// response in progress: the page sees a network error.
#[derive(Debug)]
struct Cut;

impl fmt::Display for Cut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the harness cut the network")
    }
}

impl Error for Cut {}

struct Harness {
    router: Router,
    www: PathBuf,
    /// Whether the network between the page and the router is up.
    network: watch::Sender<bool>,
}

impl Harness {
    async fn handle(&self, request: Request<Body>) -> Result<Response<Body>, Cut> {
        let path = request.uri().path().to_owned();
        if let Some(route) = path.strip_prefix("/harness/") {
            return Ok(self.harness_route(route, request).await);
        }
        if path.starts_with("/rooms/") || path == "/admin" || path.starts_with("/admin/") {
            return self.router_request(request).await;
        }
        Ok(self.file(request.method(), &path).await)
    }

    async fn harness_route(&self, route: &str, request: Request<Body>) -> Response<Body> {
        if request.method() != Method::POST {
            return status(StatusCode::METHOD_NOT_ALLOWED);
        }
        if let Some(admin) = route.strip_prefix("admin/") {
            return self.control_plane(admin, request).await;
        }
        let Ok(body) = axum::body::to_bytes(request.into_body(), MAX_HARNESS_BODY).await else {
            return text(StatusCode::BAD_REQUEST, "the body could not be read".into());
        };
        match route {
            "token" => token(&body),
            "network" => self.set_network(&body),
            _ => status(StatusCode::NOT_FOUND),
        }
    }

    /// The Control Plane request `/admin/{path}`, with the admin secret. The network switch
    /// does not apply: the Control Plane belongs to the test, not to the client.
    async fn control_plane(&self, path: &str, request: Request<Body>) -> Response<Body> {
        let (mut parts, body) = request.into_parts();
        let query = parts
            .uri
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();
        let Ok(uri) = Uri::try_from(format!("/admin/{path}{query}")) else {
            return status(StatusCode::BAD_REQUEST);
        };
        parts.uri = uri;
        let Ok(bearer) = HeaderValue::from_str(&format!("Bearer {ADMIN_SECRET}")) else {
            return status(StatusCode::INTERNAL_SERVER_ERROR);
        };
        parts.headers.insert(AUTHORIZATION, bearer);
        let request = Request::from_parts(parts, body);
        match self.router.clone().oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        }
    }

    fn set_network(&self, body: &[u8]) -> Response<Body> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Network {
            up: bool,
        }
        match serde_json::from_slice::<Network>(body) {
            Ok(network) => {
                self.network.send_replace(network.up);
                status(StatusCode::NO_CONTENT)
            }
            Err(err) => text(StatusCode::BAD_REQUEST, err.to_string()),
        }
    }

    /// A request to the router. With the network down, its connection closes without an
    /// answer; if the network goes down before its response ends, the response is cut.
    async fn router_request(&self, request: Request<Body>) -> Result<Response<Body>, Cut> {
        let mut network = self.network.subscribe();
        if !*network.borrow_and_update() {
            return Err(Cut);
        }
        let response = tokio::select! {
            response = self.router.clone().oneshot(request) => match response {
                Ok(response) => response,
                Err(never) => match never {},
            },
            () = went_down(network.clone()) => return Err(Cut),
        };
        Ok(response.map(|body| {
            Body::new(CutWhenDown {
                body,
                down: Box::pin(went_down(network)),
            })
        }))
    }

    /// The file `path` of the page directory ([`page_file`]).
    async fn file(&self, method: &Method, path: &str) -> Response<Body> {
        if method != Method::GET && method != Method::HEAD {
            return status(StatusCode::METHOD_NOT_ALLOWED);
        }
        let Some(relative) = page_file(path) else {
            return status(StatusCode::NOT_FOUND);
        };
        let Ok(contents) = tokio::fs::read(self.www.join(relative)).await else {
            return status(StatusCode::NOT_FOUND);
        };
        let content_type = match Path::new(relative).extension().and_then(|e| e.to_str()) {
            Some("html") => "text/html; charset=utf-8",
            Some("js" | "mjs") => "text/javascript; charset=utf-8",
            Some("wasm") => "application/wasm",
            Some("json") => "application/json",
            _ => "application/octet-stream",
        };
        let mut response = Response::new(Body::from(contents));
        let headers = response.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        // Each load of the page gets the files of the last build.
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }
}

/// The file of the page directory that the path of a request names, relative to the directory:
/// `index.html` for `/`. A path with an empty, `.` or `..` segment, a backslash or a colon (a
/// drive, on Windows) names none, so no path leaves the directory.
fn page_file(path: &str) -> Option<&str> {
    let relative = match path.strip_prefix('/')? {
        "" => "index.html",
        relative => relative,
    };
    let unsafe_segment = |segment: &str| {
        segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.contains('\\')
            || segment.contains(':')
    };
    if relative.split('/').any(unsafe_segment) {
        return None;
    }
    Some(relative)
}

/// A client token for the `client_id` and `room_id` of the request.
fn token(body: &[u8]) -> Response<Body> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct TokenRequest {
        client_id: String,
        room_id: String,
        ttl_secs: Option<u64>,
    }
    let request = match serde_json::from_slice::<TokenRequest>(body) {
        Ok(request) => request,
        Err(err) => return text(StatusCode::BAD_REQUEST, err.to_string()),
    };
    let ids = ClientId::new(request.client_id)
        .and_then(|client_id| RoomId::new(request.room_id).map(|room_id| (client_id, room_id)));
    let (client_id, room_id) = match ids {
        Ok(ids) => ids,
        Err(err) => return text(StatusCode::BAD_REQUEST, err.to_string()),
    };
    let ttl = Duration::from_secs(request.ttl_secs.unwrap_or(DEFAULT_TOKEN_TTL_SECS));
    text(
        StatusCode::OK,
        generate_client_token(&client_id, &room_id, ttl, AUTH_SECRET),
    )
}

/// Resolves once the network is down.
async fn went_down(mut network: watch::Receiver<bool>) {
    // The sender lives in the harness, which outlives every request.
    if network.wait_for(|up| !*up).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// A response body that fails, closing its connection, once the network goes down.
struct CutWhenDown {
    body: Body,
    down: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl http_body::Body for CutWhenDown {
    type Data = Bytes;
    type Error = Box<dyn Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if this.down.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Err(Box::new(Cut))));
        }
        Pin::new(&mut this.body).poll_frame(cx).map_err(Into::into)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

fn status(code: StatusCode) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = code;
    response
}

fn text(code: StatusCode, body: String) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = code;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

#[cfg(test)]
#[path = "tests/main.rs"]
mod tests;
