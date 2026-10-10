//! Time limit for receiving a request body.
//!
//! The router wraps every request body in a [`DeadlineBody`], which fails once the body is
//! still incomplete at its deadline. The deadline starts at a base time from the end of the
//! headers and moves forward as data arrives, by one second per `min_rate` bytes received
//! (like Apache's `mod_reqtimeout`): a body that arrives at least at the minimum rate is never
//! cut, however long it is, while one that trickles in gains next to nothing over the base
//! time. Extractors that read the body recognize the failure with [`is_body_read_timeout`]
//! and answer 408. Only reading the request is limited: a handler that does not read its body
//! (SSE) and the responses (SSE streams) are not affected.

use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::BoxError;
use http_body::{Frame, SizeHint};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::time::{Instant, Sleep};

/// The request body did not arrive in time.
#[derive(Debug, Error)]
#[error(
    "the request body did not arrive in time: {received} bytes in {elapsed:?}, below \
     {min_rate} bytes per second after the first {base:?}"
)]
pub(crate) struct BodyReadTimeout {
    received: u64,
    elapsed: Duration,
    base: Duration,
    min_rate: u64,
}

/// Time limit of a request body: `base`, plus one second per `min_rate` bytes received.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BodyReadLimit {
    pub(crate) base: Duration,
    /// Bytes per second; at least 1.
    pub(crate) min_rate: u64,
}

impl BodyReadLimit {
    /// The time a body that has received `received` bytes may take in all. It never decreases
    /// as `received` grows, and saturates instead of overflowing.
    fn allowance(&self, received: u64) -> Duration {
        let rate = u128::from(self.min_rate.max(1));
        let nanos = u128::from(received) * 1_000_000_000 / rate;
        let extension = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
        self.base.saturating_add(extension)
    }

    /// The deadline of a body that started at `start` and has received `received` bytes. An
    /// allowance beyond what an `Instant` can represent is a deadline that never comes.
    fn deadline(&self, start: Instant, received: u64) -> Option<Instant> {
        start.checked_add(self.allowance(received))
    }
}

/// Wraps the body of `request` so that reading it fails once it falls behind `limit`, counted
/// from now.
pub(crate) fn limit_body_read_time(request: Request, limit: BodyReadLimit) -> Request {
    let start = Instant::now();
    request.map(|body| {
        Body::new(DeadlineBody {
            inner: body,
            limit,
            start,
            received: 0,
            sleep: None,
        })
    })
}

/// Whether `err`, or any error in its source chain, is a [`BodyReadTimeout`]. The body
/// extractors wrap the errors of the body in their own rejections.
pub(crate) fn is_body_read_timeout(err: &(dyn Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(err) = current {
        if err.is::<BodyReadTimeout>() {
            return true;
        }
        current = err.source();
    }
    false
}

/// Request body that fails with [`BodyReadTimeout`] when it is still incomplete at its
/// deadline. Frames already available are delivered; the deadline only ends a wait.
///
/// Receiving data only adds to a counter: the timer keeps its earlier deadline, and when it
/// fires the deadline is computed again from the bytes received by then, so it is moved at
/// most once per wait instead of once per chunk.
struct DeadlineBody {
    inner: Body,
    limit: BodyReadLimit,
    start: Instant,
    received: u64,
    /// Timer of the deadline, created the first time the body has to wait for data.
    sleep: Option<Pin<Box<Sleep>>>,
}

impl DeadlineBody {
    /// Polls the timer, moving it to the current deadline when it fires early. Ready once
    /// the body is behind its limit.
    fn poll_deadline(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(deadline) = self.limit.deadline(self.start, self.received) else {
            return Poll::Pending;
        };
        let sleep = self
            .sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
        loop {
            if sleep.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            match self.limit.deadline(self.start, self.received) {
                Some(deadline) if deadline > sleep.deadline() => sleep.as_mut().reset(deadline),
                Some(_) => return Poll::Ready(()),
                None => return Poll::Pending,
            }
        }
    }
}

impl http_body::Body for DeadlineBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = &mut *self;
        if let Poll::Ready(frame) = Pin::new(&mut this.inner).poll_frame(cx) {
            if let Some(Ok(frame)) = &frame {
                if let Some(data) = frame.data_ref() {
                    let len = u64::try_from(data.len()).unwrap_or(u64::MAX);
                    this.received = this.received.saturating_add(len);
                }
            }
            return Poll::Ready(frame.map(|frame| frame.map_err(BoxError::from)));
        }
        match this.poll_deadline(cx) {
            Poll::Ready(()) => Poll::Ready(Some(Err(BodyReadTimeout {
                received: this.received,
                elapsed: this.start.elapsed(),
                base: this.limit.base,
                min_rate: this.limit.min_rate,
            }
            .into()))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
#[path = "tests/body_timeout.rs"]
mod tests;
