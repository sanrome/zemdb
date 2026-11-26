use super::*;

/// A platform that opens no client: the turns, the tabs of the rooms and the marks do not need
/// one.
struct Closed;

#[async_trait]
impl Platform for Closed {
    async fn open(&self, _: DispatchConfig, _: PageTokens) -> Result<ZemdbClient, ClientError> {
        Err(ClientError::Config("No client".to_string()))
    }
}

fn dispatcher() -> Dispatcher {
    Dispatcher::new(Closed, |_| {})
}

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

#[tokio::test]
async fn a_room_keeps_its_turn_only_while_a_call_of_it_is_in_flight() {
    let dispatcher = dispatcher();
    let shared = &dispatcher.shared;
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
    let dispatcher = dispatcher();
    let shared = &dispatcher.shared;
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

#[tokio::test]
async fn the_marks_of_an_open_and_of_a_tab_that_left_are_undone_once_dropped() {
    let dispatcher = dispatcher();
    let shared = &dispatcher.shared;
    let opening = shared.opening(&room("room-a"), 4);
    assert_eq!(shared.state().opening[&room("room-a")], 4);
    drop(opening);
    assert!(shared.state().opening.is_empty());
    // A tab is marked once per notice in flight: the first undone leaves the other.
    let first = shared.leaving(&mut shared.state(), 4);
    let second = shared.leaving(&mut shared.state(), 4);
    let other = shared.leaving(&mut shared.state(), 5);
    assert_eq!(shared.state().leaving, [4, 4, 5]);
    drop(first);
    assert_eq!(shared.state().leaving, [5, 4]);
    drop((second, other));
    assert!(shared.state().leaving.is_empty());
    // Also on a panic.
    let shared_too = Arc::clone(shared);
    let panicked = tokio::spawn(async move {
        let _opening = shared_too.opening(&room("room-b"), 5);
        let _leaving = shared_too.leaving(&mut shared_too.state(), 5);
        panic!("The notice panics");
    });
    assert!(panicked.await.unwrap_err().is_panic());
    assert!(shared.state().opening.is_empty());
    assert!(shared.state().leaving.is_empty());
}
