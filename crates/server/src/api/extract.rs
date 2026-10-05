//! Axum extractors that validate untrusted input at the HTTP edge.
//!
//! Every request to the binary API goes through the same steps: parse the room id from the
//! URL path, authenticate the caller, and decode the binary body. These extractors do each
//! step once, so handlers receive validated values and only check what depends on the message
//! type (see `ensure_payload_identity`).
//!
//! Rejections of the binary API ([`RoomPath`], [`AuthenticatedRoom`], [`RelayAuth`],
//! [`BinaryMessage`]) are binary `ServerMessage::Error` frames. Rejections of the admin API
//! ([`AdminPath`], [`AdminJson`]) are `ServerError::BadRequest`, rendered as JSON.

use axum::body::Bytes;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRef, FromRequest, FromRequestParts, Path, Request};
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use axum::RequestExt;
use serde::de::DeserializeOwned;
use subtle::ConstantTimeEq;
use zemdb_core::id::{ClientId, RoomId};
use zemdb_core::protocol::codec::decode_message;
use zemdb_core::protocol::messages::ServerMessage;

use crate::api::auth::verify_client_token;
use crate::api::data_plane::{binary_error, binary_response};
use crate::api::router::AppState;
use crate::error::ServerError;

/// Room id taken from the `:room_id` URL path segment, validated as a [`RoomId`].
///
/// Rejection: binary error frame, 400 `BadRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomPath(pub RoomId);

#[axum::async_trait]
impl<S> FromRequestParts<S> for RoomPath
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(raw) = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|e| {
                binary_error(
                    None,
                    None,
                    ServerError::BadRequest(format!("Invalid request path: {}", e.body_text())),
                )
            })?;
        RoomId::new(raw)
            .map(RoomPath)
            .map_err(|e| binary_error(None, None, e.into()))
    }
}

/// A client authenticated for the room named in the URL path.
///
/// The client token comes from the `Authorization: Bearer <token>` header only (a token in the
/// URL would end up in access logs). It must be validly signed, unexpired, and issued for the
/// path room. The SSE endpoint, whose `EventSource` clients cannot set headers, uses
/// [`EventStreamAuth`] instead.
///
/// Rejections (binary error frames): invalid path room id → 400 `BadRequest`; missing or
/// invalid token, or token for another room → 401 `Unauthorized`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedRoom {
    pub room_id: RoomId,
    pub client_id: ClientId,
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for AuthenticatedRoom
where
    S: Send + Sync,
    AppState: FromRef<S>,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let RoomPath(room_id) = RoomPath::from_request_parts(parts, state).await?;
        let app_state = AppState::from_ref(state);

        let token = bearer_token(parts)
            .ok_or_else(|| unauthorized(&room_id, "Missing Authorization header".to_string()))?;

        let client_id = verify_token_for_room(token, &room_id, &app_state)
            .map_err(|err| binary_error(None, Some(room_id.clone()), err))?;

        Ok(AuthenticatedRoom { room_id, client_id })
    }
}

/// A client authenticated for the room named in the URL path, for the SSE event stream.
///
/// Same rules as [`AuthenticatedRoom`], except that the token may also come from the
/// `?token=` query parameter, because browser `EventSource` connections cannot set headers.
/// Only the SSE endpoint accepts tokens in the URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventStreamAuth {
    pub room_id: RoomId,
    pub client_id: ClientId,
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for EventStreamAuth
where
    S: Send + Sync,
    AppState: FromRef<S>,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let RoomPath(room_id) = RoomPath::from_request_parts(parts, state).await?;
        let app_state = AppState::from_ref(state);

        let token = bearer_token(parts)
            .or_else(|| query_token(parts))
            .ok_or_else(|| unauthorized(&room_id, "Missing Authorization header".to_string()))?;

        let client_id = verify_token_for_room(token, &room_id, &app_state)
            .map_err(|err| binary_error(None, Some(room_id.clone()), err))?;

        Ok(EventStreamAuth { room_id, client_id })
    }
}

/// Caller of a snapshot relay endpoint, for the room named in the URL path.
///
/// Besides room clients (a client token issued for the path room, as in
/// [`AuthenticatedRoom`]), the relay also accepts the admin secret, used by automated snapshot
/// workers. Only the `Authorization` header is read.
///
/// Rejections (binary error frames): invalid path room id → 400 `BadRequest`; missing or
/// invalid credentials, or token for another room → 401 `Unauthorized`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayAuth {
    pub room_id: RoomId,
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for RelayAuth
where
    S: Send + Sync,
    AppState: FromRef<S>,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let RoomPath(room_id) = RoomPath::from_request_parts(parts, state).await?;
        let app_state = AppState::from_ref(state);

        let token = bearer_token(parts)
            .ok_or_else(|| unauthorized(&room_id, "Missing Authorization header".to_string()))?;

        // Constant-time comparison to prevent timing side-channel attacks
        let is_admin = token
            .as_bytes()
            .ct_eq(app_state.config.admin_secret.as_bytes())
            .unwrap_u8()
            == 1;
        if !is_admin {
            verify_token_for_room(token, &room_id, &app_state)
                .map_err(|err| binary_error(None, Some(room_id.clone()), err))?;
        }

        Ok(RelayAuth { room_id })
    }
}

/// Binary protocol message decoded from the request body with `decode_message`.
///
/// The body is read through axum's `Bytes` extractor, so the router's body size limit applies.
/// Rejections are binary frames with code `BadRequest`: a body that cannot be read keeps
/// axum's status (413 when too large), and any decode failure, including an invalid identifier
/// inside the payload, is a 400. The frame carries the path room id when it is valid.
#[derive(Debug, Clone)]
pub struct BinaryMessage<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequest<S> for BinaryMessage<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = Response;

    async fn from_request(mut req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let room_id = req
            .extract_parts::<Path<String>>()
            .await
            .ok()
            .and_then(|Path(raw)| RoomId::new(raw).ok());

        let body = Bytes::from_request(req, state).await.map_err(|rejection| {
            let status = rejection.status();
            let err = ServerError::BadRequest(format!(
                "Failed to read request body: {}",
                rejection.body_text()
            ));
            binary_response(
                status,
                &ServerMessage::Error {
                    correlation_id: None,
                    room_id: room_id.clone(),
                    code: err.to_error_code(),
                    message: err.to_string(),
                },
            )
        })?;

        decode_message(&body).map(BinaryMessage).map_err(|e| {
            binary_error(
                None,
                room_id.clone(),
                ServerError::BadRequest(format!("Failed to decode request message: {e}")),
            )
        })
    }
}

/// Path parameters of an admin endpoint, deserialized into validated types (`RoomId`,
/// `SchemaId`). Rejection: `ServerError::BadRequest` (JSON 400).
#[derive(Debug, Clone)]
pub struct AdminPath<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequestParts<S> for AdminPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ServerError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| AdminPath(value))
            .map_err(|e| ServerError::BadRequest(e.body_text()))
    }
}

/// JSON body of an admin endpoint. Wraps `axum::Json` so that any rejection (malformed JSON,
/// missing content type, an invalid identifier inside the payload) becomes a JSON error with
/// code `BadRequest` instead of axum's plain-text responses. The status is 400, except that a
/// body too large (413) or a missing JSON content type (415) keep their specific status, as
/// in the binary API.
#[derive(Debug, Clone)]
pub struct AdminJson<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequest<S> for AdminJson<T>
where
    S: Send + Sync,
    Json<T>: FromRequest<S, Rejection = JsonRejection>,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(value)| AdminJson(value))
            .map_err(|rejection| {
                let status = match rejection.status() {
                    StatusCode::PAYLOAD_TOO_LARGE | StatusCode::UNSUPPORTED_MEDIA_TYPE => {
                        rejection.status()
                    }
                    _ => StatusCode::BAD_REQUEST,
                };
                let mut response = ServerError::BadRequest(rejection.body_text()).into_response();
                *response.status_mut() = status;
                response
            })
    }
}

/// Checks that the identity carried inside a decoded message matches the request.
///
/// A payload room other than the path room is a malformed request (400 `BadRequest`). A
/// payload client other than the authenticated one is an impersonation attempt
/// (401 `Unauthorized`); pass `None` for messages that carry no client id.
pub(crate) fn ensure_payload_identity(
    room_id: &RoomId,
    client_id: Option<&ClientId>,
    payload_room_id: &RoomId,
    payload_client_id: Option<&ClientId>,
) -> Result<(), ServerError> {
    if payload_room_id != room_id {
        return Err(ServerError::BadRequest(format!(
            "Room id in payload ({payload_room_id}) does not match the request path ({room_id})"
        )));
    }
    if let (Some(expected), Some(actual)) = (client_id, payload_client_id) {
        if expected != actual {
            return Err(ServerError::Unauthorized(
                "Client ID in payload does not match token".to_string(),
            ));
        }
    }
    Ok(())
}

/// Token from the `Authorization` header. The `Bearer ` prefix is optional.
fn bearer_token(parts: &Parts) -> Option<&str> {
    let header = parts.headers.get(AUTHORIZATION)?.to_str().ok()?;
    Some(header.strip_prefix("Bearer ").unwrap_or(header))
}

/// Token from the `?token=` query parameter.
fn query_token(parts: &Parts) -> Option<&str> {
    parts
        .uri
        .query()?
        .split('&')
        .find_map(|pair| pair.strip_prefix("token="))
}

/// Verifies a client token and requires it to be issued for `room_id`.
fn verify_token_for_room(
    token: &str,
    room_id: &RoomId,
    state: &AppState,
) -> Result<ClientId, ServerError> {
    let verified = verify_client_token(token, &state.config.auth_secret)?;
    if &verified.room_id != room_id {
        return Err(ServerError::Unauthorized(format!(
            "Token room mismatch: expected {}, got {}",
            room_id, verified.room_id
        )));
    }
    Ok(verified.client_id)
}

fn unauthorized(room_id: &RoomId, message: String) -> Response {
    binary_error(
        None,
        Some(room_id.clone()),
        ServerError::Unauthorized(message),
    )
}
