use axum::extract::{FromRef, FromRequestParts};
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use zemdb_core::id::{ClientId, RoomId};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

use crate::api::router::AppState;
use crate::error::ServerError;

/// Axum extractor that validates administrative Bearer token authentication.
pub struct AdminAuth;

#[axum::async_trait]
impl FromRequestParts<AppState> for AdminAuth {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| ServerError::Unauthorized("Missing Authorization header".to_string()))?;

        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
            ServerError::Unauthorized(
                "Invalid Authorization header format, expected Bearer <token>".to_string(),
            )
        })?;

        // Constant-time comparison to prevent timing side-channel attacks
        let is_valid = token
            .as_bytes()
            .ct_eq(state.config.admin_secret.as_bytes())
            .unwrap_u8()
            == 1;

        if is_valid {
            Ok(AdminAuth)
        } else {
            Err(ServerError::Unauthorized(
                "Invalid admin secret token".to_string(),
            ))
        }
    }
}

/// Cryptographically verified client token claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedClientToken {
    pub client_id: ClientId,
    pub room_id: RoomId,
    pub expires_at: u64,
}

/// Generates a signed, stateless authentication ticket for a client in a specific room.
/// Format: `<client_id>.<room_id>.<expires_at>.<hex_blake3_signature>`
pub fn generate_client_token(
    client_id: &ClientId,
    room_id: &RoomId,
    ttl: Duration,
    secret: &str,
) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let expires_at = now.saturating_add(ttl.as_secs());
    let payload = format!("{}.{}.{}", client_id, room_id, expires_at);

    let key = blake3::hash(secret.as_bytes());
    let sig = blake3::keyed_hash(key.as_bytes(), payload.as_bytes());

    format!("{}.{}", payload, sig.to_hex())
}

/// Cryptographically validates an incoming client `auth_token` against the cluster secret.
/// Enforces constant-time signature comparison and rejects development tokens.
pub fn verify_client_token(
    auth_token: &str,
    secret: &str,
) -> Result<VerifiedClientToken, ServerError> {
    let parts: Vec<&str> = auth_token.split('.').collect();
    if parts.len() != 4 {
        return Err(ServerError::Unauthorized(
            "Malformed client auth token structure".to_string(),
        ));
    }

    let (c_id, r_id, exp_str, sig_hex) = (parts[0], parts[1], parts[2], parts[3]);

    let expires_at: u64 = exp_str
        .parse()
        .map_err(|_| ServerError::Unauthorized("Invalid timestamp in auth token".to_string()))?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if now >= expires_at {
        return Err(ServerError::Unauthorized(
            "Client auth token expired".to_string(),
        ));
    }

    let payload = format!("{}.{}.{}", c_id, r_id, exp_str);
    let key = blake3::hash(secret.as_bytes());
    let expected_sig = blake3::keyed_hash(key.as_bytes(), payload.as_bytes());

    // Constant-time signature comparison to eliminate timing side-channels
    let sig_valid = expected_sig
        .to_hex()
        .as_bytes()
        .ct_eq(sig_hex.as_bytes())
        .unwrap_u8()
        == 1;

    if !sig_valid {
        return Err(ServerError::Unauthorized(
            "Cryptographic signature verification failed".to_string(),
        ));
    }

    Ok(VerifiedClientToken {
        client_id: ClientId::new(c_id),
        room_id: RoomId::new(r_id),
        expires_at,
    })
}

/// Cryptographically validates an incoming client `auth_token` and strictly binds it
/// to an expected client_id and room_id.
pub fn verify_client_token_bound(
    auth_token: &str,
    client_id: &ClientId,
    room_id: &RoomId,
    secret: &str,
) -> Result<VerifiedClientToken, ServerError> {
    let verified = verify_client_token(auth_token, secret)?;

    if &verified.client_id != client_id {
        return Err(ServerError::Unauthorized(format!(
            "Token client mismatch: expected {}, got {}",
            client_id, verified.client_id
        )));
    }

    if &verified.room_id != room_id {
        return Err(ServerError::Unauthorized(format!(
            "Token room mismatch: expected {}, got {}",
            room_id, verified.room_id
        )));
    }

    Ok(verified)
}

/// Axum extractor that cryptographically validates client Bearer tokens in Data Plane endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientAuth {
    pub client_id: ClientId,
    pub room_id: RoomId,
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for ClientAuth
where
    S: Send + Sync,
    AppState: FromRef<S>,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);

        let token_str = if let Some(auth_header) = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
        {
            auth_header.strip_prefix("Bearer ").unwrap_or(auth_header)
        } else if let Some(query) = parts.uri.query() {
            // Fallback for query parameter token (useful for SSE EventSource /events)
            query
                .split('&')
                .find_map(|pair| pair.strip_prefix("token="))
                .ok_or_else(|| {
                    ServerError::Unauthorized("Missing Authorization header".to_string())
                        .into_response()
                })?
        } else {
            return Err(
                ServerError::Unauthorized("Missing Authorization header".to_string())
                    .into_response(),
            );
        };

        let verified = verify_client_token(token_str, &app_state.config.auth_secret)
            .map_err(|err| err.into_response())?;

        Ok(ClientAuth {
            client_id: verified.client_id,
            room_id: verified.room_id,
        })
    }
}
