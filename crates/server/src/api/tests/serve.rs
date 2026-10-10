use super::*;
use http_body::Body as _;
use std::future::poll_fn;

#[test]
fn http2_is_detected_from_the_whole_preface_even_split() {
    let mut detection = ProtocolDetection::default();
    assert_eq!(detection.feed(&HTTP2_PREFACE[..3]), None);
    assert_eq!(detection.feed(&[]), None);
    assert_eq!(detection.feed(&HTTP2_PREFACE[3..]), Some(Protocol::Http2));
    // Decided once: later bytes change nothing.
    assert_eq!(detection.feed(b"GET"), None);
}

#[test]
fn http1_is_detected_at_the_first_byte_that_differs() {
    let mut detection = ProtocolDetection::default();
    assert_eq!(detection.feed(b"G"), Some(Protocol::Http1));
    let mut detection = ProtocolDetection::default();
    assert_eq!(detection.feed(b"PRI * HTTP/1.1"), Some(Protocol::Http1));
}

async fn next_frame(body: &mut Pin<Box<TrackedBody>>) -> Option<Frame<Bytes>> {
    poll_fn(|cx| body.as_mut().poll_frame(cx))
        .await
        .map(|frame| frame.unwrap())
}

#[tokio::test]
async fn response_bodies_are_handed_over_in_small_frames() {
    let activity = Arc::new(Activity::default());
    let data: Vec<u8> = (0..200 * 1024).map(|i| i as u8).collect();
    let mut body = Box::pin(TrackedBody {
        inner: Body::from(data.clone()),
        pending: Bytes::new(),
        _in_progress: activity.begin_request(),
    });
    assert_eq!(body.size_hint().exact(), Some(data.len() as u64));

    let mut received = Vec::new();
    while let Some(frame) = next_frame(&mut body).await {
        let chunk = frame.into_data().unwrap();
        assert!(chunk.len() <= MAX_RESPONSE_FRAME);
        received.extend_from_slice(&chunk);
        let left = (data.len() - received.len()) as u64;
        assert_eq!(body.size_hint().exact(), Some(left));
        assert_eq!(body.is_end_stream(), left == 0);
    }
    assert_eq!(received, data);
    // The request is in progress until hyper drops the body.
    assert!(!activity.is_idle());
    drop(body);
    assert!(activity.is_idle());
}

const LIMIT: Duration = Duration::from_secs(10);

/// Whether `idle_for(LIMIT, any_protocol)` resolves within `within`.
async fn idles_within(activity: &Activity, any_protocol: bool, within: Duration) -> bool {
    tokio::time::timeout(within, activity.idle_for(LIMIT, any_protocol))
        .await
        .is_ok()
}

#[tokio::test(start_paused = true)]
async fn a_request_in_progress_keeps_the_connection_from_idling() {
    let activity = Arc::new(Activity::default());
    let request = activity.begin_request();
    assert!(!idles_within(&activity, true, LIMIT * 10).await);
    drop(request);
    // The limit counts from the end of the request.
    assert!(!idles_within(&activity, true, LIMIT - Duration::from_secs(1)).await);
    assert!(idles_within(&activity, true, LIMIT + Duration::from_secs(1)).await);
}

#[tokio::test(start_paused = true)]
async fn a_blocked_write_holds_the_connection_only_while_it_makes_progress() {
    let activity = Arc::new(Activity::default());
    let idle = {
        let activity = Arc::clone(&activity);
        tokio::spawn(async move { activity.idle_for(LIMIT, true).await })
    };
    // The end of a response is written at the client's pace: each write waits for the
    // client, then makes progress, well within the limit.
    for _ in 0..10 {
        activity.set_write_blocked(true);
        tokio::time::sleep(LIMIT / 2).await;
        activity.set_write_blocked(false);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(!idle.is_finished());
    // A write that waits without progress does not hold the connection.
    activity.set_write_blocked(true);
    tokio::time::sleep(LIMIT + Duration::from_secs(1)).await;
    assert!(idle.is_finished());
    assert!(!activity.is_idle());
}

#[tokio::test(start_paused = true)]
async fn http1_connections_are_left_to_hyper_except_on_shutdown() {
    let activity = Activity::default();
    activity.set_protocol(Protocol::Http1);
    assert!(!idles_within(&activity, false, LIMIT * 10).await);
    assert!(idles_within(&activity, true, LIMIT + Duration::from_secs(1)).await);
    let activity = Activity::default();
    activity.set_protocol(Protocol::Http2);
    assert!(idles_within(&activity, false, LIMIT + Duration::from_secs(1)).await);
}
