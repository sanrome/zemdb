use super::*;
use std::task::Waker;

const IDLE: Duration = Duration::from_secs(60);

/// A request that advances `times` times, `every` apart, and then ends with `7`, or never ends
/// if `ends` is false.
async fn advancing(progress: Progress, times: u32, every: Duration, ends: bool) -> u32 {
    for _ in 0..times {
        tokio::time::sleep(every).await;
        progress.advance();
    }
    if !ends {
        std::future::pending::<()>().await;
    }
    7
}

#[tokio::test(start_paused = true)]
async fn a_request_that_keeps_advancing_is_not_cut_however_long_it_takes() {
    let progress = Progress::new();
    let started = Instant::now();
    let request = advancing(progress.clone(), 10, IDLE * 9 / 10, true);
    assert_eq!(progress.while_advancing(request, IDLE).await, Some(7));
    assert!(started.elapsed() >= IDLE * 9);
}

#[tokio::test(start_paused = true)]
async fn a_request_that_stops_advancing_is_cut_one_idle_timeout_after_its_last_progress() {
    let progress = Progress::new();
    let started = Instant::now();
    let request = advancing(progress.clone(), 3, IDLE / 2, false);
    let cut = tokio::time::timeout(IDLE * 10, progress.while_advancing(request, IDLE));
    assert_eq!(cut.await, Ok(None));
    assert_eq!(started.elapsed(), IDLE / 2 * 3 + IDLE);
}

#[tokio::test(start_paused = true)]
async fn a_request_that_never_advances_is_cut_one_idle_timeout_after_it_started() {
    let progress = Progress::new();
    let started = Instant::now();
    let request = std::future::pending::<()>();
    let cut = tokio::time::timeout(IDLE * 10, progress.while_advancing(request, IDLE));
    assert_eq!(cut.await, Ok(None));
    assert_eq!(started.elapsed(), IDLE);
}

#[tokio::test(start_paused = true)]
async fn the_body_is_handed_over_in_pieces_and_each_piece_taken_is_progress() {
    let progress = Progress::new();
    let mut body = FrameBody {
        frame: Bytes::from(vec![1; 2 * BODY_PIECE + 10]),
        progress: progress.clone(),
    };
    assert_eq!(body.size_hint().exact(), Some(2 * BODY_PIECE as u64 + 10));
    let mut cx = Context::from_waker(Waker::noop());
    let mut pieces = Vec::new();
    loop {
        tokio::time::advance(Duration::from_secs(1)).await;
        let before = Instant::now();
        match Pin::new(&mut body).poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => {
                pieces.push(frame.into_data().unwrap().len());
                assert_eq!(progress.last(), before);
            }
            Poll::Ready(None) => break,
            other => panic!("Unexpected {other:?}"),
        }
    }
    assert_eq!(pieces, [BODY_PIECE, BODY_PIECE, 10]);
    assert!(body.is_end_stream());
    assert_eq!(body.size_hint().exact(), Some(0));
}
