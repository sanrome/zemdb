use super::*;

const SECRET: &str = "secret";

fn far_future() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600
}

/// Builds a correctly signed token for arbitrary raw claims, bypassing ID validation.
fn signed_token(client_id: &str, room_id: &str, expires_at: u64) -> String {
    let signature = token_signature(SECRET, client_id, room_id, expires_at);
    format!(
        "{}.{}.{}.{}.{}",
        TOKEN_VERSION,
        URL_SAFE_NO_PAD.encode(client_id),
        URL_SAFE_NO_PAD.encode(room_id),
        expires_at,
        signature.to_hex()
    )
}

#[test]
fn signed_token_with_valid_claims_verifies() {
    let token = signed_token("alice", "room-1", far_future());
    let verified = verify_client_token(&token, SECRET).unwrap();
    assert_eq!(verified.client_id.as_str(), "alice");
    assert_eq!(verified.room_id.as_str(), "room-1");
}

#[test]
fn correctly_signed_token_with_invalid_room_id_is_rejected() {
    for room in ["../escape", "Room", "", "con"] {
        let token = signed_token("alice", room, far_future());
        assert!(
            matches!(
                verify_client_token(&token, SECRET),
                Err(ServerError::Unauthorized(_))
            ),
            "room id {room:?} must be rejected"
        );
    }
}

#[test]
fn correctly_signed_token_with_invalid_client_id_is_rejected() {
    for client in ["", "bad\0id", "zero\u{200B}width"] {
        let token = signed_token(client, "room-1", far_future());
        assert!(
            matches!(
                verify_client_token(&token, SECRET),
                Err(ServerError::Unauthorized(_))
            ),
            "client id {client:?} must be rejected"
        );
    }
}

#[test]
fn expiry_must_be_canonical_decimal() {
    assert_eq!(parse_canonical_expiry("0"), Some(0));
    assert_eq!(parse_canonical_expiry("1700000000"), Some(1_700_000_000));
    for field in ["", "00", "01", "+1", "-1", " 1", "1 ", "1e3", "0x10", "١"] {
        assert_eq!(parse_canonical_expiry(field), None, "{field:?}");
    }
    // Out of range for u64.
    assert_eq!(parse_canonical_expiry("18446744073709551616"), None);
}
