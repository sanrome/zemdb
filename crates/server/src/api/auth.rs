use std::time::{Duration, SystemTime, UNIX_EPOCH};
use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use rimdb_core::id::{ClientId, RoomId};

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

        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or_else(|| {
                ServerError::Unauthorized(
                    "Invalid Authorization header format, expected Bearer <token>".to_string(),
                )
            })?;

        if token == state.config.admin_secret {
            Ok(AdminAuth)
        } else {
            Err(ServerError::Unauthorized("Invalid admin secret token".to_string()))
        }
    }
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

/// Cryptographically validates an incoming client `auth_token` against the room, client, and cluster secret.
pub fn verify_client_token(
    auth_token: &str,
    client_id: &ClientId,
    room_id: &RoomId,
    secret: &str,
) -> Result<(), ServerError> {
    // Development fallback
    if auth_token == "dev-token" && secret == "default_auth_secret_dev_32bytes!" {
        return Ok(());
    }

    let parts: Vec<&str> = auth_token.split('.').collect();
    if parts.len() != 4 {
        return Err(ServerError::Unauthorized(
            "Malformed client auth token structure".to_string(),
        ));
    }

    let (c_id, r_id, exp_str, sig_hex) = (parts[0], parts[1], parts[2], parts[3]);

    if c_id != client_id.as_str() {
        return Err(ServerError::Unauthorized(format!(
            "Token client mismatch: expected {}, got {}",
            client_id, c_id
        )));
    }

    if r_id != room_id.as_str() {
        return Err(ServerError::Unauthorized(format!(
            "Token room mismatch: expected {}, got {}",
            room_id, r_id
        )));
    }

    let expires_at: u64 = exp_str.parse().map_err(|_| {
        ServerError::Unauthorized("Invalid timestamp in auth token".to_string())
    })?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if now >= expires_at {
        return Err(ServerError::Unauthorized("Client auth token expired".to_string()));
    }

    let payload = format!("{}.{}.{}", c_id, r_id, exp_str);
    let key = blake3::hash(secret.as_bytes());
    let expected_sig = blake3::keyed_hash(key.as_bytes(), payload.as_bytes());

    if expected_sig.to_hex().as_str() != sig_hex {
        return Err(ServerError::Unauthorized(
            "Cryptographic signature verification failed".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_token_generation_and_verification() {
        let client_id = ClientId::new("client-alpha");
        let room_id = RoomId::new("room-xyz");
        let secret = "super_secret_cluster_key_12345";

        let token = generate_client_token(&client_id, &room_id, Duration::from_secs(60), secret);
        assert!(verify_client_token(&token, &client_id, &room_id, secret).is_ok());

        // Token mismatch client
        let wrong_client = ClientId::new("client-beta");
        assert!(verify_client_token(&token, &wrong_client, &room_id, secret).is_err());

        // Token mismatch room
        let wrong_room = RoomId::new("room-other");
        assert!(verify_client_token(&token, &client_id, &wrong_room, secret).is_err());

        // Wrong secret
        assert!(verify_client_token(&token, &client_id, &room_id, "wrong_secret").is_err());

        // Expired token (0 TTL)
        let expired_token = generate_client_token(&client_id, &room_id, Duration::from_secs(0), secret);
        assert!(verify_client_token(&expired_token, &client_id, &room_id, secret).is_err());
    }
}
