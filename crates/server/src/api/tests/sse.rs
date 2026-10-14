use super::*;
use crate::api::test_support::TestState;
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc;

/// App state whose room `room` is played by a task that refuses the first `refusals`
/// subscriptions with `Unavailable`, as an actor shutting down for inactivity does, and
/// accepts the next ones.
fn state_with_refusing_room(dir: &TempDir, refusals: usize) -> (AppState, RoomId) {
    let state = TestState::new(dir.path(), |_| {});
    let room_id = RoomId::new("room").unwrap();
    let (sender, mut receiver) = mpsc::channel(8);
    let exit = state.manager.install_test_actor(&room_id, sender);
    tokio::spawn(async move {
        let _exit = exit;
        let (events, _) = broadcast::channel(8);
        let mut refused = 0;
        while let Some(command) = receiver.recv().await {
            if let RoomCommand::SubscribeEvents { reply } = command {
                if refused < refusals {
                    refused += 1;
                    let _ = reply.send(Err(ServerError::Unavailable("stopping".to_string())));
                } else {
                    let _ = reply.send(Ok(events.subscribe()));
                }
            }
        }
    });
    (state.app_state(), room_id)
}

#[tokio::test]
async fn subscription_refused_by_a_stopping_room_is_retried() {
    let dir = tempdir().unwrap();
    let (state, room_id) = state_with_refusing_room(&dir, SUBSCRIBE_ATTEMPTS - 1);
    assert!(subscribe(&state, &room_id).await.is_ok());
}

#[tokio::test]
async fn subscription_retries_are_bounded() {
    let dir = tempdir().unwrap();
    let (state, room_id) = state_with_refusing_room(&dir, SUBSCRIBE_ATTEMPTS);
    assert!(matches!(
        subscribe(&state, &room_id).await,
        Err(ServerError::Unavailable(_))
    ));
}

fn ids(room: &str, client: &str) -> (RoomId, ClientId) {
    (RoomId::new(room).unwrap(), ClientId::new(client).unwrap())
}

fn total(streams: &SseStreams) -> usize {
    streams.lock().total
}

/// Whether a newer stream took the slot of `slot`'s stream, which then ends.
fn is_replaced(slot: &mut SseSlot) -> bool {
    matches!(
        slot.replaced.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    )
}

#[test]
fn a_client_over_its_streams_replaces_its_oldest() {
    let streams = Arc::new(SseStreams::new(10));
    let (room, client) = ids("room", "client");
    let mut first = streams.open(&room, &client).unwrap();
    let mut second = streams.open(&room, &client).unwrap();
    let mut third = streams.open(&room, &client).unwrap();
    // The oldest is replaced only once the newest starts.
    assert!(!is_replaced(&mut first));
    third.replace_older();
    // The slot of the oldest passes to the newest, so the total does not grow.
    assert!(is_replaced(&mut first));
    assert!(!is_replaced(&mut second));
    assert!(!is_replaced(&mut third));
    assert_eq!(total(&streams), STREAMS_PER_CLIENT);
    // Dropping the replaced stream frees nothing; dropping the others frees their slots.
    drop(first);
    assert_eq!(total(&streams), STREAMS_PER_CLIENT);
    drop((second, third));
    assert_eq!(total(&streams), 0);
    assert!(streams.lock().by_client.is_empty());
}

#[test]
fn streams_over_the_limit_are_refused_until_one_closes() {
    let streams = Arc::new(SseStreams::new(STREAMS_PER_CLIENT));
    let (room, client) = ids("room", "client");
    let (other_room, other_client) = ids("other-room", "other-client");
    let first = streams.open(&room, &client).unwrap();
    let second = streams.open(&room, &client).unwrap();
    let refused = || {
        matches!(
            streams.open(&other_room, &other_client),
            Err(ServerError::RateLimited)
        )
    };
    assert!(refused());
    // At the limit, a client with all its streams open still replaces its oldest, whose end
    // frees nothing.
    let third = streams.open(&room, &client).unwrap();
    third.replace_older();
    drop(first);
    assert!(refused());
    drop(second);
    assert!(!refused());
}

#[test]
fn a_stream_that_fails_to_start_replaces_none() {
    let streams = Arc::new(SseStreams::new(10));
    let (room, client) = ids("room", "client");
    let mut first = streams.open(&room, &client).unwrap();
    let _second = streams.open(&room, &client).unwrap();
    drop(streams.open(&room, &client).unwrap());
    assert!(!is_replaced(&mut first));
    assert_eq!(total(&streams), STREAMS_PER_CLIENT);
}

#[test]
fn a_client_has_one_replacement_starting_at_a_time() {
    let streams = Arc::new(SseStreams::new(10));
    let (room, client) = ids("room", "client");
    let _first = streams.open(&room, &client).unwrap();
    let _second = streams.open(&room, &client).unwrap();
    let starting = streams.open(&room, &client).unwrap();
    assert!(matches!(
        streams.open(&room, &client),
        Err(ServerError::RateLimited)
    ));
    starting.replace_older();
    assert!(streams.open(&room, &client).is_ok());
}
