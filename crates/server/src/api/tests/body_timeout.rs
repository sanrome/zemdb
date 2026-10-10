use super::*;
use std::convert::Infallible;
use tokio::sync::mpsc;

const BASE: Duration = Duration::from_secs(10);
const LIMIT: BodyReadLimit = BodyReadLimit {
    base: BASE,
    min_rate: 100,
};

#[test]
fn each_min_rate_bytes_add_one_second() {
    assert_eq!(LIMIT.allowance(0), BASE);
    assert_eq!(LIMIT.allowance(50), BASE + Duration::from_millis(500));
    assert_eq!(LIMIT.allowance(100), BASE + Duration::from_secs(1));
    assert_eq!(LIMIT.allowance(1_000), BASE + Duration::from_secs(10));
}

#[test]
fn the_allowance_never_decreases_and_saturates() {
    let mut previous = Duration::ZERO;
    for received in [0, 1, 99, 100, 1 << 20, 1 << 40, u64::MAX / 2, u64::MAX] {
        let allowance = LIMIT.allowance(received);
        assert!(allowance >= previous, "{received}");
        previous = allowance;
    }
    let slowest = BodyReadLimit {
        base: Duration::MAX,
        min_rate: 1,
    };
    assert_eq!(slowest.allowance(u64::MAX), Duration::MAX);
    // A deadline beyond what an instant can hold never comes.
    assert_eq!(slowest.deadline(Instant::now(), u64::MAX), None);
}

/// A body fed through the returned sender, limited by [`LIMIT`] from now.
fn limited_body() -> (mpsc::UnboundedSender<Bytes>, Body) {
    let (tx, rx) = mpsc::unbounded_channel::<Bytes>();
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|chunk| (Ok::<_, Infallible>(chunk), rx))
    });
    let request = Request::new(Body::from_stream(stream));
    (tx, limit_body_read_time(request, LIMIT).into_body())
}

/// Sends `chunks` chunks of `size` bytes, one every `every`, then ends the body.
fn drip(tx: mpsc::UnboundedSender<Bytes>, chunks: usize, size: usize, every: Duration) {
    tokio::spawn(async move {
        for _ in 0..chunks {
            tokio::time::sleep(every).await;
            if tx.send(Bytes::from(vec![0u8; size])).is_err() {
                return;
            }
        }
    });
}

#[tokio::test(start_paused = true)]
async fn a_body_at_the_minimum_rate_may_take_longer_than_the_base_time() {
    let (tx, body) = limited_body();
    // 200 bytes per second for 30 s, three times the base time.
    drip(tx, 60, 100, Duration::from_millis(500));
    let start = Instant::now();
    let received = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    assert_eq!(received.len(), 6_000);
    assert!(start.elapsed() >= BASE * 3);
}

#[tokio::test(start_paused = true)]
async fn a_body_below_the_minimum_rate_is_cut_close_to_the_base_time() {
    let (tx, body) = limited_body();
    // One byte a second: each extends the deadline by 10 ms only.
    drip(tx, 1_000, 1, Duration::from_secs(1));
    let start = Instant::now();
    let err = axum::body::to_bytes(body, usize::MAX).await.unwrap_err();
    let elapsed = start.elapsed();
    assert!(is_body_read_timeout(&err), "{err}");
    assert!(
        elapsed >= BASE && elapsed < BASE + Duration::from_secs(1),
        "cut after {elapsed:?}"
    );
}
