use crate::{endpoint, ClientError, EventStream, TransportClient, TransportError};
use async_trait::async_trait;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;
use zemdb_core::{encode_message, ClientMessage, RoomId, MAX_FRAME_SIZE};

/// The pieces the body of a request is handed over in: each one the HTTP layer takes is
/// progress of the request.
const BODY_PIECE: usize = 16 << 10;

/// The longest an idle connection may be kept: what the server accepts for its
/// `header_read_timeout_secs`, after which it closes it.
const MAX_POOL_IDLE: Duration = Duration::from_secs(300);

/// The configuration of an [`HttpTransport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTransportConfig {
    /// The URL of the server, `http://` or `https://`, without a user, a query or a fragment:
    /// the paths of the Data Plane go under it (`{url}/rooms/{room_id}/commit`).
    pub url: String,
    /// The longest a connection may take to open, with its TLS handshake (10 s). Above zero.
    pub connect_timeout: Duration,
    /// The longest a request may go without advancing (60 s), however long it takes in all.
    /// Above zero.
    pub idle_timeout: Duration,
    /// How long an idle connection is kept for another request (5 s): below the time the
    /// server keeps it (`header_read_timeout_secs`, 10 s by default). At most 300 s, the
    /// longest the server accepts.
    pub pool_idle_timeout: Duration,
    /// Whether an `http://` URL speaks HTTP/2 from its first byte (prior knowledge) instead of
    /// HTTP/1.1. With `https://` it has no effect: the version is agreed in the TLS handshake.
    pub http2_prior_knowledge: bool,
}

impl HttpTransportConfig {
    /// The configuration of a transport to the server at `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            pool_idle_timeout: Duration::from_secs(5),
            http2_prior_knowledge: false,
        }
    }
}

/// The transport of a native client: each message is the body of a `POST` to its Data Plane
/// endpoint, over HTTP/1.1 or HTTP/2, with the proxy of the environment (`HTTP_PROXY`,
/// `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`) and the certificates of the platform.
///
/// A request is cut only when it stops advancing for the idle timeout: while connecting (or
/// waiting for a free stream of an HTTP/2 connection), sending its body (each piece the HTTP
/// layer takes), waiting for the response or receiving its body (each piece that arrives),
/// however long it takes in all. What the HTTP layer took is in its buffers and in those of
/// the system until the network takes it, which the client does not see, and the wait for the
/// response counts from the last piece taken: TCP sizes the send buffer for the path, so on a
/// slow link it holds little. Over HTTP/2, a cut request does not close the connection it went
/// over, which other requests share: after a request the transport cuts, the requests that
/// follow go over a new HTTP client, with new connections (not after one its caller drops
/// first, such as a write past its deadline).
///
/// A response is read up to the largest frame: one that announces more, or brings more, fails
/// without reading the rest.
///
/// The event streams go over an HTTP client of their own, over HTTP/1.1 only, so that each one
/// has a connection of its own: over HTTP/2 they would share the connection of the requests,
/// whose streams they would take, and a stream nobody reads would make the server close it.
/// Opening a stream has the timeouts of a request until its headers; an open one has none.
#[derive(Debug, Clone)]
pub struct HttpTransport {
    clients: Arc<Clients>,
    /// The HTTP client of the event streams, which the cuts of the requests do not replace.
    streams: reqwest::Client,
    url: String,
    idle_timeout: Duration,
}

/// The HTTP client the requests go through, with its connections, replaced when the transport
/// cuts a request for not advancing.
#[derive(Debug)]
struct Clients {
    config: HttpTransportConfig,
    /// The current client and its generation.
    current: Mutex<(u64, reqwest::Client)>,
    /// Whether a replacement is being built.
    replacing: AtomicBool,
}

/// Marks a replacement as being built while it lives.
struct Replacing<'a>(&'a AtomicBool);

impl Drop for Replacing<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl Clients {
    fn builder(config: &HttpTransportConfig) -> reqwest::ClientBuilder {
        reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .pool_idle_timeout(config.pool_idle_timeout)
            .redirect(reqwest::redirect::Policy::none())
    }

    fn build(config: &HttpTransportConfig) -> Result<reqwest::Client, reqwest::Error> {
        let mut builder = Self::builder(config);
        if config.http2_prior_knowledge && config.url.starts_with("http://") {
            builder = builder.http2_prior_knowledge();
        }
        builder.build()
    }

    /// The client of the event streams.
    fn build_streams(config: &HttpTransportConfig) -> Result<reqwest::Client, reqwest::Error> {
        Self::builder(config).http1_only().build()
    }

    fn current(&self) -> (u64, reqwest::Client) {
        let current = self
            .current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (current.0, current.1.clone())
    }

    /// Replaces the client of `generation`, if it is still the current one and no other
    /// replacement is being built, so that the requests that follow do not go over a
    /// connection that may have stopped answering; the requests in flight on it go on. A cut
    /// while another replacement is being built does not wait for it, so the request that
    /// follows it may still go over the current client. Building a client reads the
    /// certificates of the platform in Linux, so it runs on the blocking pool, in a task of its
    /// own that a caller who stops waiting does not cancel. One that does not build leaves the
    /// current client, until the next cut.
    async fn replace(self: &Arc<Self>, generation: u64) {
        if self.current().0 != generation || self.replacing.swap(true, Ordering::SeqCst) {
            return;
        }
        let clients = Arc::clone(self);
        let replacement = tokio::spawn(async move {
            let _replacing = Replacing(&clients.replacing);
            let config = clients.config.clone();
            let built = tokio::task::spawn_blocking(move || Self::build(&config)).await;
            let Ok(Ok(client)) = built else {
                return;
            };
            let replaced = {
                let mut current = clients
                    .current
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                (current.0 == generation)
                    .then(|| std::mem::replace(&mut *current, (generation + 1, client)))
            };
            drop(replaced);
        });
        // Dropping the handle does not cancel the task.
        let _ = replacement.await;
    }
}

impl HttpTransport {
    /// A transport with `config`: a URL that is not `http://` or `https://`, or has a user, a
    /// query or a fragment, or a timeout at zero, is `Config`, as is an HTTP client that does
    /// not start, also for `http://` (in Linux, without certificates of the platform), whose
    /// error gives the cause. Building it reads those certificates in Linux, once for the
    /// requests and once for the event streams.
    pub fn new(config: HttpTransportConfig) -> Result<Self, ClientError> {
        let invalid = |reason: &str| ClientError::Config(format!("{reason}: {}", config.url));
        let url = reqwest::Url::parse(&config.url).map_err(|_| invalid("Invalid server URL"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(invalid("The server URL is not http:// or https://"));
        }
        if !url.username().is_empty() || url.password().is_some() {
            // The client would send them as a second `Authorization` header.
            return Err(invalid("The server URL has a user"));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(invalid("The server URL has a query or a fragment"));
        }
        if config.connect_timeout.is_zero() || config.idle_timeout.is_zero() {
            return Err(ClientError::Config(
                "The timeouts of the transport must be above zero".to_string(),
            ));
        }
        if config.pool_idle_timeout > MAX_POOL_IDLE {
            return Err(ClientError::Config(format!(
                "The pool keeps an idle connection for at most {MAX_POOL_IDLE:?}"
            )));
        }
        let config = HttpTransportConfig {
            url: url.as_str().trim_end_matches('/').to_string(),
            ..config
        };
        let not_started = |err: reqwest::Error| {
            ClientError::Config(format!("The HTTP client did not start: {}", causes(&err)))
        };
        let client = Clients::build(&config).map_err(not_started)?;
        let streams = Clients::build_streams(&config).map_err(not_started)?;
        Ok(Self {
            streams,
            url: config.url.clone(),
            idle_timeout: config.idle_timeout,
            clients: Arc::new(Clients {
                config,
                current: Mutex::new((0, client)),
                replacing: AtomicBool::new(false),
            }),
        })
    }
}

#[async_trait]
impl TransportClient for HttpTransport {
    async fn send(&self, message: &ClientMessage, token: &str) -> Result<Vec<u8>, TransportError> {
        let url = format!("{}{}", self.url, endpoint(message));
        let frame = encode_message(message).map_err(|err| {
            TransportError(format!("The request to {url} was not encoded: {err}"))
        })?;
        let progress = Progress::new();
        let body = FrameBody {
            frame: Bytes::from(frame),
            progress: progress.clone(),
        };
        let (generation, client) = self.clients.current();
        let request = client
            .post(&url)
            .bearer_auth(token)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(reqwest::Body::wrap(body))
            .send();
        let stalled = || {
            let idle = self.idle_timeout;
            TransportError(format!("The request to {url} did not advance for {idle:?}"))
        };
        let Some(response) = progress.while_advancing(request, self.idle_timeout).await else {
            self.clients.replace(generation).await;
            return Err(stalled());
        };
        let mut response = response.map_err(|err| failed(&err))?;
        match read_frame(&mut response, &url, self.idle_timeout).await {
            Ok(reply) => Ok(reply),
            Err(Read::Stalled) => {
                self.clients.replace(generation).await;
                Err(stalled())
            }
            Err(Read::Failed(err)) => Err(err),
        }
    }

    async fn events(&self, room_id: &RoomId, token: &str) -> Result<EventStream, TransportError> {
        let url = format!("{}/rooms/{room_id}/events", self.url);
        let idle = self.idle_timeout;
        let stalled = || TransportError(format!("The stream {url} did not advance for {idle:?}"));
        let request = (self.streams.get(&url))
            .bearer_auth(token)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send();
        let response = tokio::time::timeout(idle, request).await;
        let mut response = response
            .map_err(|_| stalled())?
            .map_err(|err| failed(&err))?;
        if response.status() != reqwest::StatusCode::OK {
            return match read_frame(&mut response, &url, idle).await {
                Ok(refusal) => Ok(EventStream::Refused(refusal)),
                Err(Read::Stalled) => Err(stalled()),
                Err(Read::Failed(err)) => Err(err),
            };
        }
        let blocks = futures::stream::unfold(Some(response), |response| async move {
            let mut response = response?;
            match response.chunk().await {
                Ok(Some(block)) => Some((Ok(block), Some(response))),
                Ok(None) => None,
                Err(err) => Some((Err(failed(&err)), None)),
            }
        });
        Ok(EventStream::Open(Box::pin(blocks)))
    }
}

/// Why the body of a response was not read.
enum Read {
    /// It went the idle timeout without a piece.
    Stalled,
    Failed(TransportError),
}

/// The body of `response`, from `url`, read up to the largest frame, with at most `idle`
/// between its pieces: one longer by its `Content-Length` fails before it is read, and one
/// longer without it, without reading the rest.
async fn read_frame(
    response: &mut reqwest::Response,
    url: &str,
    idle: Duration,
) -> Result<Vec<u8>, Read> {
    let announced = response.content_length().unwrap_or(0);
    let too_long = || {
        Read::Failed(TransportError(format!(
            "The response from {url} is longer than a frame"
        )))
    };
    if announced > MAX_FRAME_SIZE as u64 {
        return Err(too_long());
    }
    let mut reply = Vec::new();
    reply.try_reserve_exact(announced as usize).map_err(|_| {
        Read::Failed(TransportError(format!(
            "No memory for the response from {url}"
        )))
    })?;
    loop {
        let Ok(piece) = tokio::time::timeout(idle, response.chunk()).await else {
            return Err(Read::Stalled);
        };
        let Some(piece) = piece.map_err(|err| Read::Failed(failed(&err)))? else {
            return Ok(reply);
        };
        if reply.len() + piece.len() > MAX_FRAME_SIZE {
            return Err(too_long());
        }
        reply.extend_from_slice(&piece);
    }
}

/// A request that failed, with the chain of its causes.
fn failed(err: &reqwest::Error) -> TransportError {
    TransportError(causes(err))
}

/// `err` with the chain of its causes.
fn causes(err: &dyn Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        message = format!("{message}: {cause}");
        source = cause.source();
    }
    message
}

/// When a request last advanced: when it was created, and when the HTTP layer took each piece
/// of its body.
#[derive(Debug, Clone)]
struct Progress(Arc<Mutex<Instant>>);

impl Progress {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Instant::now())))
    }

    fn advance(&self) {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
    }

    fn last(&self) -> Instant {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The output of `request`, or `None` once it went `idle` without advancing.
    async fn while_advancing<F: Future>(&self, request: F, idle: Duration) -> Option<F::Output> {
        let mut request = std::pin::pin!(request);
        loop {
            let left = idle.saturating_sub(self.last().elapsed());
            if left.is_zero() {
                return None;
            }
            if let Ok(output) = tokio::time::timeout(left, request.as_mut()).await {
                return Some(output);
            }
        }
    }
}

/// The body of a request: its frame, handed over in pieces that count as progress.
struct FrameBody {
    frame: Bytes,
    progress: Progress,
}

impl Body for FrameBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if self.frame.is_empty() {
            return Poll::Ready(None);
        }
        let len = self.frame.len().min(BODY_PIECE);
        let piece = self.frame.split_to(len);
        self.progress.advance();
        Poll::Ready(Some(Ok(Frame::data(piece))))
    }

    fn is_end_stream(&self) -> bool {
        self.frame.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.frame.len() as u64)
    }
}

#[cfg(test)]
#[path = "tests/http_transport.rs"]
mod tests;
