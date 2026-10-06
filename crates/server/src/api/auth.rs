use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use zemdb_core::id::{ClientId, RoomId};

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

/// Version tag at the start of every client token issued in the current format.
const TOKEN_VERSION: &str = "v1";

/// Key-derivation context for client token signatures. Deriving the signing key from the
/// cluster secret under this context keeps it distinct from any other use of the same secret.
const TOKEN_KEY_CONTEXT: &str = "zemdb 2026-10 client auth token v1";

fn token_signing_key(secret: &str) -> [u8; 32] {
    blake3::derive_key(TOKEN_KEY_CONTEXT, secret.as_bytes())
}

/// Signature over the token claims. Each variable-length field is prefixed with its length,
/// so no two different `(client, room)` pairs can produce the same signed input.
fn token_signature(secret: &str, client_id: &str, room_id: &str, expires_at: u64) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new_keyed(&token_signing_key(secret));
    hasher.update(&(client_id.len() as u64).to_le_bytes());
    hasher.update(client_id.as_bytes());
    hasher.update(&(room_id.len() as u64).to_le_bytes());
    hasher.update(room_id.as_bytes());
    hasher.update(&expires_at.to_le_bytes());
    hasher.finalize()
}

/// Generates a signed, stateless authentication ticket for a client in a specific room.
///
/// Format: `v1.<base64url(client_id)>.<base64url(room_id)>.<expires_at>.<hex_signature>`.
/// The IDs are base64url-encoded (an alphabet without `.`), so IDs may contain any character
/// their own rules allow without making the token ambiguous. The signature is keyed BLAKE3
/// over the length-prefixed claims, with a key derived from the cluster secret.
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
    let signature = token_signature(secret, client_id.as_str(), room_id.as_str(), expires_at);

    format!(
        "{}.{}.{}.{}.{}",
        TOKEN_VERSION,
        URL_SAFE_NO_PAD.encode(client_id.as_str()),
        URL_SAFE_NO_PAD.encode(room_id.as_str()),
        expires_at,
        signature.to_hex()
    )
}

fn malformed_token() -> ServerError {
    ServerError::Unauthorized("Malformed client auth token structure".to_string())
}

fn decode_token_field(field: &str) -> Result<String, ServerError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(field)
        .map_err(|_| malformed_token())?;
    String::from_utf8(bytes).map_err(|_| malformed_token())
}

/// Parses the expiry field, accepting only its canonical decimal form: ASCII digits, no sign,
/// and no leading zero unless the value is exactly `0`. `u64::from_str` also accepts `+123`
/// and `0123`; since the signature covers the numeric value, those would give one claim set
/// several valid token strings.
fn parse_canonical_expiry(field: &str) -> Option<u64> {
    let canonical = !field.is_empty()
        && field.bytes().all(|b| b.is_ascii_digit())
        && (field == "0" || !field.starts_with('0'));
    if !canonical {
        return None;
    }
    field.parse().ok()
}

/// Cryptographically validates an incoming client `auth_token` against the cluster secret.
///
/// Checks the format and version, then the signature in constant time, then expiry, and
/// finally that the signed IDs are valid identifiers.
pub fn verify_client_token(
    auth_token: &str,
    secret: &str,
) -> Result<VerifiedClientToken, ServerError> {
    let parts: Vec<&str> = auth_token.split('.').collect();
    let [version, client_b64, room_b64, exp_str, sig_hex] = parts[..] else {
        return Err(malformed_token());
    };
    if version != TOKEN_VERSION {
        return Err(malformed_token());
    }

    let client_raw = decode_token_field(client_b64)?;
    let room_raw = decode_token_field(room_b64)?;
    let expires_at = parse_canonical_expiry(exp_str)
        .ok_or_else(|| ServerError::Unauthorized("Invalid timestamp in auth token".to_string()))?;

    // Constant-time signature comparison to eliminate timing side-channels
    let expected = token_signature(secret, &client_raw, &room_raw, expires_at);
    let sig_valid = expected
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

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if now >= expires_at {
        return Err(ServerError::Unauthorized(
            "Client auth token expired".to_string(),
        ));
    }

    Ok(VerifiedClientToken {
        client_id: ClientId::new(client_raw)
            .map_err(|e| ServerError::Unauthorized(format!("Invalid client id in token: {e}")))?,
        room_id: RoomId::new(room_raw)
            .map_err(|e| ServerError::Unauthorized(format!("Invalid room id in token: {e}")))?,
        expires_at,
    })
}

/// Cryptographically validates an incoming client `auth_token` and strictly binds it
/// to an expected client_id and room_id.
///
/// An invalid or expired token is `Unauthorized`; a valid token issued for another client or
/// room is `Forbidden`.
pub fn verify_client_token_bound(
    auth_token: &str,
    client_id: &ClientId,
    room_id: &RoomId,
    secret: &str,
) -> Result<VerifiedClientToken, ServerError> {
    let verified = verify_client_token(auth_token, secret)?;

    if &verified.client_id != client_id {
        return Err(ServerError::Forbidden(format!(
            "Token client mismatch: expected {}, got {}",
            client_id, verified.client_id
        )));
    }

    if &verified.room_id != room_id {
        return Err(ServerError::Forbidden(format!(
            "Token room mismatch: expected {}, got {}",
            room_id, verified.room_id
        )));
    }

    Ok(verified)
}

#[cfg(test)]
#[path = "tests/auth.rs"]
mod tests;
