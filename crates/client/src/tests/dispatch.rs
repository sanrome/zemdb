use super::*;

/// A platform that opens no client: the turns and the tabs of the rooms do not need one.
struct Closed;

#[async_trait]
impl Platform for Closed {
    async fn open(&self, _: DispatchConfig) -> Result<ZemdbClient, ClientError> {
        Err(ClientError::Config("No client".to_string()))
    }
}

fn shared() -> Arc<Shared> {
    Dispatcher::new(Closed, |_| {}).shared
}

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

#[tokio::test]
async fn a_room_keeps_its_turn_only_while_a_call_of_it_is_in_flight() {
    let shared = shared();
    // The next turn of a room waits for the one in flight, and a turn of another room does not.
    let first = shared.turn(&room("room-a")).await;
    let mut second = Box::pin(shared.turn(&room("room-a")));
    let other = shared.turn(&room("room-b")).await;
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(shared.state().turns.len(), 2);
    drop(first);
    let second = second.await;
    // Once the turns of a room all ended, the next turn of any room takes the room out.
    drop((second, other));
    let last = shared.turn(&room("room-c")).await;
    let rooms = Vec::from_iter(shared.state().turns.keys().cloned());
    assert_eq!(rooms, [room("room-c")]);
    // Many rooms whose turns ended, one after another, leave one entry.
    drop(last);
    for index in 0..100 {
        drop(shared.turn(&room(&format!("room-{index}"))).await);
    }
    assert_eq!(shared.state().turns.len(), 1);
}

#[test]
fn a_room_leaves_the_tabs_of_the_rooms_once_no_tab_holds_it() {
    let shared = shared();
    shared
        .state()
        .rooms
        .insert(room("room-a"), HashSet::from([1, 2]));
    assert!(!shared.release(&room("room-a"), 1));
    assert!(!shared.release(&room("room-a"), 3));
    assert_eq!(shared.state().rooms[&room("room-a")], HashSet::from([2]));
    assert!(shared.release(&room("room-a"), 2));
    assert!(shared.state().rooms.is_empty());
    assert!(!shared.release(&room("room-a"), 2));
}
