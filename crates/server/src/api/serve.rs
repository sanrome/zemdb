//! HTTP serving with connection limits, read timeouts and graceful shutdown.
//!
//! Connections speak HTTP/1.1 or HTTP/2 without TLS (h2c with prior knowledge), detected from
//! their first bytes. TLS is expected to end at a reverse proxy in front of the server.

use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::{BoxError, Router};
use http_body::{Frame, SizeHint};
use hyper::body::Incoming;
use hyper::service::Service as _;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info};

use crate::api::router::{build_router, AppState, ShutdownSignal};
use crate::config::ServerConfig;
use crate::error::ServerError;

/// Time allowed, once shutdown starts, for in-flight requests to finish and rooms to close.
pub const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(10);

/// Pause after an accept error other than a connection aborted by its client, such as
/// running out of file descriptors, before accepting again.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// Serves the API on `listener` until `signal` resolves or `state.shutdown` is triggered,
/// then shuts down in order:
///
/// 1. stops accepting connections and fires `state.shutdown`, which ends open SSE streams;
/// 2. waits for in-flight requests to complete;
/// 3. shuts down every room actor, which persists its clients roster.
///
/// If steps 2 and 3 take longer than `grace`, returns an error without waiting further; the
/// caller is expected to exit the process. Commits are durable before they are acknowledged,
/// so an abandoned shutdown only loses unflushed client cursors.
///
/// While serving, the limits of `state.config` apply to every connection:
/// - at most `max_connections` are open at once; at the limit, new connections wait in the
///   listen backlog until an open one closes. An HTTP/2 connection carries at most
///   [`MAX_CONCURRENT_STREAMS`] requests at once;
/// - a connection must deliver the headers of each request within `header_read_timeout_secs`,
///   counted from its opening or from the end of its previous response; one that does not, or
///   that stays open without a request in progress for longer, is closed;
/// - a request body must arrive in full within `body_read_timeout_secs`, extended by one
///   second per `body_min_rate_bytes_per_sec` bytes received (see [`build_router`]).
///
/// A request in progress, including the writing of its whole response at the client's pace
/// and an open SSE stream, is never cut by these limits. Over HTTP/2, a client that stops
/// reading the end of a response for longer than the header timeout is disconnected.
pub async fn serve_until_shutdown<F>(
    listener: TcpListener,
    state: AppState,
    signal: F,
    grace: Duration,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let shutdown = state.shutdown.clone();
    let room_manager = Arc::clone(&state.room_manager);
    let limits = ConnectionLimits::new(&state.config);
    let app = build_router(state);

    let stop = {
        let shutdown = shutdown.clone();
        async move {
            tokio::select! {
                () = signal => {}
                () = shutdown.wait() => {}
            }
        }
    };
    let connections = accept_until(listener, app, &limits, &shutdown, stop).await;
    info!("Shutdown requested: no longer accepting connections");
    shutdown.trigger();

    let drain = async {
        connections.wait_closed().await;
        info!("Connections drained; shutting down rooms");
        room_manager.shutdown_all().await;
    };
    tokio::time::timeout(grace, drain).await.map_err(|_| {
        ServerError::Internal(format!(
            "Graceful shutdown did not finish within {:?}",
            grace
        ))
    })
}

/// Limits applied to every connection.
#[derive(Debug, Clone, Copy)]
struct ConnectionLimits {
    header_read_timeout: Duration,
    max_connections: usize,
}

impl ConnectionLimits {
    fn new(config: &ServerConfig) -> Self {
        Self {
            header_read_timeout: Duration::from_secs(config.header_read_timeout_secs),
            max_connections: config.max_connections,
        }
    }
}

/// The open connections, each holding one permit of a semaphore of `max` permits.
struct OpenConnections {
    permits: Arc<Semaphore>,
    max: usize,
}

impl OpenConnections {
    /// Resolves once every connection has closed: all the permits are back.
    async fn wait_closed(&self) {
        // `max_connections` is validated to fit in a `u32`; the semaphore is never closed.
        let all = u32::try_from(self.max).unwrap_or(u32::MAX);
        let _all = self.permits.acquire_many(all).await;
    }
}

/// Accepts connections on `listener` and serves each in its own task, until `stop`
/// resolves. Returns the connections still open, which finish on their own once `shutdown`
/// fires; the listener is closed on return.
async fn accept_until(
    listener: TcpListener,
    app: Router,
    limits: &ConnectionLimits,
    shutdown: &ShutdownSignal,
    stop: impl Future<Output = ()>,
) -> OpenConnections {
    let permits = Arc::new(Semaphore::new(limits.max_connections));
    let builder = connection_builder(limits);
    tokio::pin!(stop);
    loop {
        // At the limit, wait for a connection to close before accepting the next one: the
        // kernel queues new connections in the listen backlog meanwhile.
        let permit = tokio::select! {
            () = &mut stop => break,
            permit = Arc::clone(&permits).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let stream = tokio::select! {
            () = &mut stop => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(err) if is_connection_error(&err) => continue,
                Err(err) => {
                    error!(error = %err, "Failed to accept a connection");
                    tokio::select! {
                        () = &mut stop => break,
                        () = tokio::time::sleep(ACCEPT_ERROR_BACKOFF) => continue,
                    }
                }
            },
        };
        tokio::spawn(serve_connection(
            stream,
            app.clone(),
            builder.clone(),
            limits.header_read_timeout,
            shutdown.clone(),
            permit,
        ));
    }
    OpenConnections {
        permits,
        max: limits.max_connections,
    }
}

/// An accept error caused by the client (it aborted the connection before it was accepted),
/// which does not affect the listener.
fn is_connection_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

/// Most HTTP/2 streams (requests) a connection may have in progress at once, so that
/// `max_connections` still bounds the requests in flight.
pub const MAX_CONCURRENT_STREAMS: u32 = 100;

/// Largest frame of a response body handed to hyper. Hyper asks HTTP/2 for send capacity
/// before taking each frame, so with small frames a body is done only when the client has
/// accepted all but the last frame, instead of as soon as hyper buffers it whole.
const MAX_RESPONSE_FRAME: usize = 64 * 1024;

/// While the server shuts down, how long a connection that was serving a request may stay
/// idle before it is closed: enough for the end of a response that hyper still holds to reach
/// the client, without waiting for clients that do not answer.
const SHUTDOWN_LINGER: Duration = Duration::from_secs(1);

/// Builder of HTTP/1.1 and HTTP/2 (h2c) connections. Hyper enforces the header timeout on
/// HTTP/1.1 request heads, starting once the previous response is fully written;
/// [`serve_connection`] enforces it for everything else.
fn connection_builder(limits: &ConnectionLimits) -> auto::Builder<TokioExecutor> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_read_timeout);
    builder
        .http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS);
    builder
}

/// Serves one connection until it ends, it goes idle for too long, or the server shuts down.
///
/// Idle means without a request in progress: a request stays in progress until hyper has
/// taken the last frame of its response body. The idle limit is `header_read_timeout`; each
/// time a write that had to wait for the client makes progress the limit starts over, so the
/// end of a response is delivered at the client's pace, but a client that stops reading cannot
/// hold the connection. Hyper times out HTTP/1.1 heads itself, starting once the previous
/// response is fully written, so the idle limit applies only before the protocol is known
/// (hyper does not time out the bytes that tell HTTP/1.1 from HTTP/2) and to HTTP/2, whose
/// streams have no header timeout.
///
/// On shutdown, an idle connection is closed at once; one with a request in progress is
/// shut down gracefully, and closed if it stays idle for [`SHUTDOWN_LINGER`] instead of
/// waiting for the client to confirm (a client that does not answer would hold the shutdown).
async fn serve_connection(
    stream: TcpStream,
    app: Router,
    builder: auto::Builder<TokioExecutor>,
    header_read_timeout: Duration,
    shutdown: ShutdownSignal,
    _permit: OwnedSemaphorePermit,
) {
    let activity = Arc::new(Activity::default());
    let service = {
        let activity = Arc::clone(&activity);
        let app = TowerToHyperService::new(app);
        hyper::service::service_fn(move |request: Request<Incoming>| {
            let in_progress = activity.begin_request();
            let response = app.call(request);
            async move {
                let response = response.await?;
                // The request stays in progress until its response body is done or dropped.
                Ok::<_, Infallible>(response.map(|body| {
                    Body::new(TrackedBody {
                        inner: body,
                        pending: Bytes::new(),
                        _in_progress: in_progress,
                    })
                }))
            }
        })
    };
    let io = TrackedIo {
        inner: stream,
        activity: Arc::clone(&activity),
        detection: ProtocolDetection::default(),
    };
    let connection = builder.serve_connection(TokioIo::new(io), service);
    tokio::pin!(connection);
    let mut shutting_down = false;
    loop {
        let idle = if shutting_down {
            activity.idle_for(SHUTDOWN_LINGER, true)
        } else {
            activity.idle_for(header_read_timeout, false)
        };
        tokio::select! {
            result = connection.as_mut() => {
                if let Err(err) = result {
                    debug!(error = %err, "Connection ended with an error");
                }
                break;
            }
            () = shutdown.wait(), if !shutting_down => {
                if activity.is_idle() {
                    break;
                }
                connection.as_mut().graceful_shutdown();
                shutting_down = true;
            }
            () = idle => {
                debug!("Closing an idle connection");
                break;
            }
        }
    }
}

/// Protocol of a connection, as far as it is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Protocol {
    Unknown = 0,
    Http1 = 1,
    Http2 = 2,
}

/// What [`serve_connection`] needs to know about a connection: requests in progress, writes
/// waiting for the client, and its protocol.
#[derive(Default)]
struct Activity {
    in_progress: AtomicUsize,
    write_blocked: AtomicBool,
    protocol: AtomicU8,
    /// Notified when any of the above changes, and when a blocked write makes progress. It
    /// has a single waiter ([`Activity::idle_for`]), so `notify_one` keeps a notification
    /// that arrives while it is not waiting.
    changed: Notify,
}

impl Activity {
    fn begin_request(self: &Arc<Self>) -> InProgress {
        self.in_progress.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
        InProgress(Arc::clone(self))
    }

    fn protocol(&self) -> Protocol {
        match self.protocol.load(Ordering::SeqCst) {
            1 => Protocol::Http1,
            2 => Protocol::Http2,
            _ => Protocol::Unknown,
        }
    }

    fn set_protocol(&self, protocol: Protocol) {
        self.protocol.store(protocol as u8, Ordering::SeqCst);
        self.changed.notify_one();
    }

    /// Records whether the last write had to wait for the client; a change notifies.
    fn set_write_blocked(&self, blocked: bool) {
        if self.write_blocked.swap(blocked, Ordering::SeqCst) != blocked {
            self.changed.notify_one();
        }
    }

    /// Whether no request is in progress and no write is waiting for the client.
    fn is_idle(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst) == 0 && !self.write_blocked.load(Ordering::SeqCst)
    }

    /// Resolves once no request has been in progress for `timeout` and, in that time, no
    /// write waiting for the client has made progress (a write that keeps waiting without
    /// progress does not hold the connection). Unless `any_protocol`, never resolves for an
    /// HTTP/1.1 connection, whose idle time hyper limits.
    async fn idle_for(&self, timeout: Duration, any_protocol: bool) {
        loop {
            let changed = self.changed.notified();
            if self.in_progress.load(Ordering::SeqCst) > 0
                || (!any_protocol && self.protocol() == Protocol::Http1)
            {
                changed.await;
            } else if tokio::time::timeout(timeout, changed).await.is_err() {
                return;
            }
        }
    }
}

/// A request in progress, until dropped.
struct InProgress(Arc<Activity>);

impl Drop for InProgress {
    fn drop(&mut self) {
        self.0.in_progress.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_one();
    }
}

/// The HTTP/2 connection preface, which an HTTP/2 client with prior knowledge sends first.
const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Tells HTTP/1.1 from HTTP/2 by the first bytes a connection sends, as hyper does: HTTP/2
/// once the whole preface arrived, HTTP/1.1 at the first byte that differs from it.
#[derive(Default)]
struct ProtocolDetection {
    matched: usize,
    decided: bool,
}

impl ProtocolDetection {
    /// Feeds bytes received; returns the protocol once it is known, the first time.
    fn feed(&mut self, bytes: &[u8]) -> Option<Protocol> {
        if self.decided {
            return None;
        }
        for &byte in bytes {
            if byte != HTTP2_PREFACE[self.matched] {
                self.decided = true;
                return Some(Protocol::Http1);
            }
            self.matched += 1;
            if self.matched == HTTP2_PREFACE.len() {
                self.decided = true;
                return Some(Protocol::Http2);
            }
        }
        None
    }
}

/// The socket of a connection, reporting to its [`Activity`] the protocol the client speaks
/// and whether writes are waiting for the client.
struct TrackedIo {
    inner: TcpStream,
    activity: Arc<Activity>,
    detection: ProtocolDetection,
}

impl TrackedIo {
    fn track_write<T>(&self, poll: Poll<io::Result<T>>) -> Poll<io::Result<T>> {
        self.activity.set_write_blocked(poll.is_pending());
        poll
    }
}

impl AsyncRead for TrackedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        let before = buf.filled().len();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Some(protocol) = this.detection.feed(&buf.filled()[before..]) {
            this.activity.set_protocol(protocol);
        }
        poll
    }
}

impl AsyncWrite for TrackedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.track_write(poll)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.track_write(poll)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let poll = Pin::new(&mut self.inner).poll_flush(cx);
        self.track_write(poll)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Response body that keeps its request in progress while it is being sent, handed to hyper
/// in frames of at most [`MAX_RESPONSE_FRAME`] bytes.
struct TrackedBody {
    inner: Body,
    /// The rest of a data frame of `inner` larger than [`MAX_RESPONSE_FRAME`].
    pending: Bytes,
    _in_progress: InProgress,
}

impl http_body::Body for TrackedBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = &mut *self;
        if this.pending.is_empty() {
            match ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(data) => this.pending = data,
                    Err(frame) => return Poll::Ready(Some(Ok(frame))),
                },
                Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                None => return Poll::Ready(None),
            }
        }
        let len = this.pending.len().min(MAX_RESPONSE_FRAME);
        Poll::Ready(Some(Ok(Frame::data(this.pending.split_to(len)))))
    }

    fn is_end_stream(&self) -> bool {
        self.pending.is_empty() && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        let pending = self.pending.len() as u64;
        let inner = self.inner.size_hint();
        let mut hint = SizeHint::new();
        if let Some(upper) = inner.upper() {
            hint.set_upper(upper + pending);
        }
        hint.set_lower(inner.lower() + pending);
        hint
    }
}

#[cfg(test)]
#[path = "tests/serve.rs"]
mod tests;
