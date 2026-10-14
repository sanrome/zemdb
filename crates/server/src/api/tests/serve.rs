use super::*;
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
    let mut body = Box::pin(TrackedBody::new(
        Body::from(data.clone()),
        activity.begin_request(),
        None,
    ));
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

/// Whether `stalled(LIMIT)` resolves within `within`.
async fn stalls_within(activity: &Activity, within: Duration) -> bool {
    tokio::time::timeout(within, activity.stalled(LIMIT))
        .await
        .is_ok()
}

#[tokio::test(start_paused = true)]
async fn a_response_the_client_does_not_take_stalls() {
    let activity = Arc::new(Activity::default());
    let _body = TrackedBody::new(Body::from(vec![0u8; 16]), activity.begin_request(), None);
    // Its headers and first frame wait for the client from the start.
    assert!(!stalls_within(&activity, LIMIT - Duration::from_secs(1)).await);
    assert!(stalls_within(&activity, Duration::from_secs(2)).await);
}

#[tokio::test(start_paused = true)]
async fn a_response_taken_frame_by_frame_does_not_stall() {
    let activity = Arc::new(Activity::default());
    let stalled = {
        let activity = Arc::clone(&activity);
        tokio::spawn(async move { activity.stalled(LIMIT).await })
    };
    let data = vec![0u8; 20 * MAX_RESPONSE_FRAME];
    let mut body = Box::pin(TrackedBody::new(
        Body::from(data),
        activity.begin_request(),
        None,
    ));
    // The client takes one frame every half the limit: the whole response takes ten limits.
    while next_frame(&mut body).await.is_some() {
        tokio::time::sleep(LIMIT / 2).await;
    }
    assert!(!stalled.is_finished());
    // Taken in full, the body is dropped and nothing waits for the client any more.
    drop(body);
    tokio::time::sleep(LIMIT * 2).await;
    assert!(!stalled.is_finished());
}

#[tokio::test(start_paused = true)]
async fn a_response_with_nothing_to_send_does_not_stall() {
    let activity = Arc::new(Activity::default());
    let events = futures::stream::pending::<Result<Bytes, Infallible>>();
    let mut body = Box::pin(TrackedBody::new(
        Body::from_stream(events),
        activity.begin_request(),
        None,
    ));
    // Hyper asks for a frame and the body has none: the server, not the client, is waiting.
    assert!(poll_fn(|cx| Poll::Ready(body.as_mut().poll_frame(cx).is_pending())).await);
    assert!(!stalls_within(&activity, LIMIT * 10).await);
}

#[tokio::test(start_paused = true)]
async fn one_stalled_response_is_not_hidden_by_another_that_progresses() {
    let activity = Arc::new(Activity::default());
    let _stuck = TrackedBody::new(Body::from(vec![0u8; 16]), activity.begin_request(), None);
    let stalled = {
        let activity = Arc::clone(&activity);
        tokio::spawn(async move { activity.stalled(LIMIT).await })
    };
    let mut moving = Box::pin(TrackedBody::new(
        Body::from(vec![0u8; 20 * MAX_RESPONSE_FRAME]),
        activity.begin_request(),
        None,
    ));
    for _ in 0..4 {
        next_frame(&mut moving).await.unwrap();
        tokio::time::sleep(LIMIT / 3).await;
    }
    assert!(stalled.is_finished());
}

/// The frames of `data` as a body, holding part of a budget if `budgeted`.
async fn frames_of(data: &Bytes, budgeted: bool) -> Vec<Bytes> {
    let budget = budgeted
        .then(|| ResponseBudget::new(Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap()));
    let activity = Arc::new(Activity::default());
    let mut body = Box::pin(TrackedBody::new(
        Body::from(data.clone()),
        activity.begin_request(),
        budget,
    ));
    let mut frames = Vec::new();
    while let Some(frame) = next_frame(&mut body).await {
        frames.push(frame.into_data().unwrap());
    }
    frames
}

#[tokio::test]
async fn budgeted_bodies_hand_copies_with_the_last_byte_alone() {
    let data = Bytes::from(vec![7u8; MAX_RESPONSE_FRAME + 10]);
    let shares_data = |frame: &Bytes| data.as_ptr_range().contains(&frame.as_ptr());
    let frames = frames_of(&data, true).await;
    let lens: Vec<usize> = frames.iter().map(Bytes::len).collect();
    assert_eq!(lens, [MAX_RESPONSE_FRAME, 9, 1]);
    // What hyper may still hold once it drops the body does not keep the encoding alive.
    assert!(!frames.iter().any(shares_data));
    // Small responses, outside the budget, are not copied.
    assert!(frames_of(&data, false).await.iter().all(shares_data));
}

#[tokio::test(start_paused = true)]
async fn a_blocked_write_stalls_even_with_a_request_in_progress() {
    let activity = Arc::new(Activity::default());
    let _request = activity.begin_request();
    activity.set_write_blocked(true);
    tokio::time::sleep(LIMIT / 2).await;
    // Progress restarts the wait.
    activity.set_write_blocked(false);
    activity.set_write_blocked(true);
    assert!(!stalls_within(&activity, LIMIT - Duration::from_secs(1)).await);
    assert!(stalls_within(&activity, Duration::from_secs(2)).await);
}
