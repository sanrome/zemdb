//! The token provider of a dispatcher, which asks a tab of the room for each token.

use async_trait::async_trait;
use std::sync::{Arc, Weak};
use tokio::sync::oneshot;
use zemdb_core::{ClientId, RoomId};

use super::{Notice, Shared, State};
use crate::{TokenError, TokenProvider};

/// The token provider of the client of a [`Dispatcher`](super::Dispatcher), which
/// [`Platform::open`](super::Platform::open) gets: it asks a tab for each token, with a
/// [`Notice::Token`], and waits for its answer, a [`Request::Token`](super::Request::Token) with
/// the id of the request.
///
/// The request goes to the tab whose open of the room runs in its turn, or else to the one with
/// the lowest id of the tabs that opened the room, but never to a tab whose notice that it left
/// is in flight; without one, there is no token, and nothing is asked. It has no deadline of its
/// own: the client waits 60 s for a token, as for any provider, and then the request leaves the
/// dispatcher, and an answer that comes later changes nothing. An answer without a token, the
/// tab leaving and the dispatcher dropped give no token either, and the client goes on as without
/// a new token.
#[derive(Debug, Clone)]
pub struct PageTokens {
    shared: Weak<Shared>,
}

impl PageTokens {
    pub(super) fn new(shared: &Arc<Shared>) -> Self {
        Self {
            shared: Arc::downgrade(shared),
        }
    }
}

impl State {
    /// The tab that a request of a token of `room_id` goes to: the one whose open of the room
    /// runs in its turn, or else the one with the lowest id of those that opened it, but never a
    /// tab whose notice that it left is in flight, and none once the dispatcher was dropped.
    pub(super) fn token_tab(&self, room_id: &RoomId) -> Option<u64> {
        let staying = |tab: &&u64| !self.leaving.contains(tab);
        let held = (self.rooms.get(room_id).into_iter().flatten()).filter(staying);
        let opening = self.opening.get(room_id).filter(staying);
        opening.or(held.min()).copied().filter(|_| !self.dropped)
    }
}

#[async_trait]
impl TokenProvider for PageTokens {
    async fn token(&self, client_id: &ClientId, room_id: &RoomId) -> Result<String, TokenError> {
        let no_token = || TokenError("The page gave no token".to_string());
        let shared = self.shared.upgrade().ok_or_else(no_token)?;
        let (answer, answered) = oneshot::channel();
        let (tab, request) = {
            let mut state = shared.state();
            let tab = state.token_tab(room_id).ok_or_else(no_token)?;
            let request = state.next_id();
            state.tokens.insert(request, (tab, answer));
            (tab, request)
        };
        // The request leaves the dispatcher once the client stops waiting for it.
        let _asked = shared.undo(move |state| {
            state.tokens.remove(&request);
        });
        shared.notice(Notice::Token {
            tab,
            request,
            client_id: client_id.clone(),
            room_id: room_id.clone(),
        });
        drop(shared);
        answered.await.ok().flatten().ok_or_else(no_token)
    }
}

#[cfg(test)]
#[path = "tests/token.rs"]
mod tests;
