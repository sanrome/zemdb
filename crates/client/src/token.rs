use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use std::fmt::Debug;
use zemdb_core::{ClientId, RoomId};

/// Where the client gets a new token of a room: the application, whose backend issues them. The
/// client asks for one when the server refuses the token of a room (`Unauthorized`), and before
/// the next request of a room after the server forbade its token (`Forbidden`).
///
/// The request may be a registration, which holds the registration lock of the room while it
/// waits for the token: a provider that opens or discards the same room from inside
/// [`token`](Self::token) waits for that lock until the client stops waiting for the token
/// (60 s) and drops it, and the request goes on as without a new token. If the registration is
/// one of the worker of the room, a discard from inside it stops that worker first, which drops
/// the registration with the discard in it: nothing is discarded, and the room has no worker.
#[async_trait]
pub trait TokenProvider: Debug + Send + Sync {
    /// A new token of `client_id` for `room_id`, as the backend issues them.
    async fn token(&self, client_id: &ClientId, room_id: &RoomId) -> Result<String, TokenError>;
}

/// Why a [`TokenProvider`] gave no token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("No token: {0}")]
pub struct TokenError(pub String);

/// Whether `token` has the format in which the server issues tokens
/// (`v1.<client id>.<room id>.<expiry>.<signature>`, with the ids in base64url without padding,
/// the expiry in decimal digits and the signature in hexadecimal) and names `client_id`. Whether
/// it is valid, and of the room, is the server's to check.
pub(crate) fn issued_to(token: &str, client_id: &ClientId) -> bool {
    let mut fields = token.split('.');
    let mut field = || fields.next();
    let (Some("v1"), Some(client), Some(room), Some(expiry), Some(signature), None) =
        (field(), field(), field(), field(), field(), field())
    else {
        return false;
    };
    let id = |field: &str| {
        URL_SAFE_NO_PAD
            .decode(field)
            .ok()
            .filter(|id| !id.is_empty())
    };
    let all = |field: &str, valid: fn(&u8) -> bool| {
        !field.is_empty() && field.as_bytes().iter().all(valid)
    };
    id(room).is_some()
        && all(expiry, u8::is_ascii_digit)
        && all(signature, u8::is_ascii_hexdigit)
        && id(client).is_some_and(|client| client == client_id.as_str().as_bytes())
}

#[cfg(test)]
#[path = "tests/token.rs"]
mod tests;
