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
