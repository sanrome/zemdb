use super::*;
use crate::dispatch::{Command, DispatchConfig, Dispatcher, Message, Platform, Request};
use crate::{ClientError, ZemdbClient};
use std::collections::HashSet;
use std::sync::Mutex;

/// A platform that opens no client: the requests of a token do not need one.
struct Closed;

#[async_trait]
impl Platform for Closed {
    async fn open(&self, _: DispatchConfig, _: PageTokens) -> Result<ZemdbClient, ClientError> {
        Err(ClientError::Config("No client".to_string()))
    }
}

/// A dispatcher whose notices go to the vector it gives.
fn noticing() -> (Dispatcher, Arc<Mutex<Vec<Notice>>>) {
    let notices = Arc::new(Mutex::new(Vec::new()));
    let published = Arc::clone(&notices);
    let dispatcher = Dispatcher::new(Closed, move |message| {
        if let Message::Notice(notice) = message {
            published.lock().unwrap().push(notice);
        }
    });
    (dispatcher, notices)
}

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

/// The command of `tab` that answers `request` with `token`.
fn answer(tab: u64, request: u64, token: Option<&str>) -> Command {
    let request = Request::Token(request, token.map(str::to_string));
    Command {
        tab,
        id: 0,
        request,
    }
}

/// What `asked` gives, which comes within a few seconds.
async fn soon<T>(asked: impl std::future::Future<Output = T>) -> T {
    let asked = tokio::time::timeout(std::time::Duration::from_secs(5), asked);
    asked.await.expect("The request of a token still waits")
}

/// The id of the last request of a token in `notices`.
fn last_request(notices: &Mutex<Vec<Notice>>) -> u64 {
    match notices.lock().unwrap().last() {
        Some(Notice::Token { request, .. }) => *request,
        notice => panic!("{notice:?}"),
    }
}

#[test]
fn a_request_of_a_token_goes_to_the_tab_that_opens_the_room_or_else_to_one_that_holds_it() {
    let dispatcher = Dispatcher::new(Closed, |_| {});
    let mut state = dispatcher.shared.state();
    let (a, b) = (room("room-a"), room("room-b"));
    state.rooms.insert(a.clone(), HashSet::from([3, 2, 7]));
    assert_eq!(state.token_tab(&a), Some(2));
    // The tab whose open of the room runs in its turn comes first, also one that does not hold
    // it.
    state.opening.insert(a.clone(), 9);
    assert_eq!(state.token_tab(&a), Some(9));
    // A tab whose notice that it left is in flight gets none.
    state.leaving.extend([9, 2]);
    assert_eq!(state.token_tab(&a), Some(3));
    state.leaving.extend([3, 7]);
    assert_eq!(state.token_tab(&a), None);
    // A room that no tab holds nor opens has no tab to ask.
    state.opening.insert(b.clone(), 5);
    assert_eq!(state.token_tab(&b), Some(5));
    state.opening.remove(&b);
    assert_eq!(state.token_tab(&b), None);
    // Nor any room once the dispatcher was dropped.
    state.leaving.clear();
    state.dropped = true;
    assert_eq!(state.token_tab(&a), None);
}

#[tokio::test]
async fn a_request_of_a_token_leaves_the_dispatcher_once_answered_or_once_its_caller_stops_waiting()
{
    let (dispatcher, notices) = noticing();
    let shared = Arc::clone(&dispatcher.shared);
    let tokens = PageTokens::new(&shared);
    let client_id = ClientId::new("ana#7").unwrap();
    let a = room("room-a");
    shared.state().rooms.insert(a.clone(), HashSet::from([1]));

    // Asked, the tab gets the request, which waits for its answer: an answer of another tab
    // changes nothing.
    let mut asking = Box::pin(tokens.token(&client_id, &a));
    assert!(futures::poll!(&mut asking).is_pending());
    let Notice::Token { tab, request, .. } = notices.lock().unwrap()[0].clone() else {
        panic!("{notices:?}");
    };
    assert_eq!(tab, 1);
    assert_eq!(shared.state().tokens.len(), 1);
    dispatcher.dispatch(answer(2, request, Some("v1.token")));
    assert!(futures::poll!(&mut asking).is_pending());
    dispatcher.dispatch(answer(1, request, Some("v1.token")));
    assert_eq!(soon(asking).await, Ok("v1.token".to_string()));
    assert!(shared.state().tokens.is_empty());

    // An answer without a token is none.
    let mut asking = Box::pin(tokens.token(&client_id, &a));
    assert!(futures::poll!(&mut asking).is_pending());
    dispatcher.dispatch(answer(1, last_request(&notices), None));
    assert!(soon(asking).await.is_err());
    assert!(shared.state().tokens.is_empty());

    // A caller that stops waiting takes the request out, and an answer that comes later changes
    // nothing.
    let mut asking = Box::pin(tokens.token(&client_id, &a));
    assert!(futures::poll!(&mut asking).is_pending());
    assert_eq!(shared.state().tokens.len(), 1);
    drop(asking);
    assert!(shared.state().tokens.is_empty());
    dispatcher.dispatch(answer(1, last_request(&notices), Some("v1.token")));
    assert!(shared.state().tokens.is_empty());

    // A room without a tab to ask has no token, and nothing is asked.
    let sent = notices.lock().unwrap().len();
    let given = tokens.token(&client_id, &room("room-b")).await;
    assert!(given.is_err(), "{given:?}");
    assert_eq!(notices.lock().unwrap().len(), sent);
    assert!(shared.state().tokens.is_empty());

    // Dropping the dispatcher, while a command in flight still holds what it shares, ends the
    // request in flight, and a new one asks nothing.
    let mut asking = Box::pin(tokens.token(&client_id, &a));
    assert!(futures::poll!(&mut asking).is_pending());
    drop(dispatcher);
    assert!(soon(asking).await.is_err());
    let sent = notices.lock().unwrap().len();
    assert!(soon(tokens.token(&client_id, &a)).await.is_err());
    assert_eq!(notices.lock().unwrap().len(), sent);
    assert!(shared.state().tokens.is_empty());
    // Nor once nothing holds it.
    drop(shared);
    assert!(soon(tokens.token(&client_id, &a)).await.is_err());
}
