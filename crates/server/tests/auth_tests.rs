use std::time::Duration;
use zemdb_core::{ClientId, RoomId};
use zemdb_server::api::auth::{
    generate_client_token, verify_client_token, verify_client_token_bound,
};

#[test]
fn test_client_token_generation_and_verification() {
    let client_id = ClientId::new("client-alpha").unwrap();
    let room_id = RoomId::new("room-xyz").unwrap();
    let secret = "super_secret_cluster_key_12345";

    let token = generate_client_token(&client_id, &room_id, Duration::from_secs(60), secret);
    let verified = verify_client_token(&token, secret).expect("token must verify");
    assert_eq!(verified.client_id, client_id);
    assert_eq!(verified.room_id, room_id);

    assert!(verify_client_token_bound(&token, &client_id, &room_id, secret).is_ok());

    // Token mismatch client
    let wrong_client = ClientId::new("client-beta").unwrap();
    assert!(verify_client_token_bound(&token, &wrong_client, &room_id, secret).is_err());

    // Token mismatch room
    let wrong_room = RoomId::new("room-other").unwrap();
    assert!(verify_client_token_bound(&token, &client_id, &wrong_room, secret).is_err());

    // Wrong secret
    assert!(verify_client_token(&token, "wrong_secret").is_err());

    // Expired token (0 TTL)
    let expired_token = generate_client_token(&client_id, &room_id, Duration::from_secs(0), secret);
    assert!(verify_client_token(&expired_token, secret).is_err());

    // Dev backdoor elimination test
    assert!(verify_client_token("dev-token", "default_auth_secret_dev_32bytes!").is_err());
}

#[test]
fn client_ids_with_dots_and_unicode_round_trip() {
    let secret = "super_secret_cluster_key_12345";
    let room_id = RoomId::new("room-1").unwrap();
    for raw in ["user.name@example.com", "Ñandú 42", "a.b.c.d"] {
        let client_id = ClientId::new(raw).unwrap();
        let token = generate_client_token(&client_id, &room_id, Duration::from_secs(60), secret);
        let verified = verify_client_token(&token, secret)
            .unwrap_or_else(|e| panic!("token for client {raw:?} must verify: {e}"));
        assert_eq!(verified.client_id, client_id);
        assert_eq!(verified.room_id, room_id);
    }
}

#[test]
fn token_has_versioned_unambiguous_format() {
    let token = generate_client_token(
        &ClientId::new("a.b").unwrap(),
        &RoomId::new("room-1").unwrap(),
        Duration::from_secs(60),
        "secret",
    );
    assert!(token.starts_with("v1."), "unexpected token format: {token}");
    assert_eq!(token.split('.').count(), 5);
}

#[test]
fn tampered_claims_are_rejected() {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let secret = "secret";
    let token = generate_client_token(
        &ClientId::new("alice").unwrap(),
        &RoomId::new("room-1").unwrap(),
        Duration::from_secs(60),
        secret,
    );
    let mut parts: Vec<String> = token.split('.').map(str::to_string).collect();
    assert_eq!(parts.len(), 5);

    let mut other_client = parts.clone();
    other_client[1] = URL_SAFE_NO_PAD.encode("mallory");
    assert!(verify_client_token(&other_client.join("."), secret).is_err());

    let mut other_room = parts.clone();
    other_room[2] = URL_SAFE_NO_PAD.encode("room-2");
    assert!(verify_client_token(&other_room.join("."), secret).is_err());

    let expires_at: u64 = parts[3].parse().unwrap();
    parts[3] = (expires_at + 3600).to_string();
    assert!(verify_client_token(&parts.join("."), secret).is_err());
}

#[test]
fn legacy_token_format_is_rejected() {
    // A token in the previous `client.room.expires.signature` format, signed with the previous
    // scheme, must no longer be accepted.
    let secret = "secret";
    let expires_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;
    let payload = format!("alice.room-1.{expires_at}");
    let key = blake3::hash(secret.as_bytes());
    let sig = blake3::keyed_hash(key.as_bytes(), payload.as_bytes());
    let legacy = format!("{payload}.{}", sig.to_hex());

    assert!(verify_client_token(&legacy, secret).is_err());
}

#[test]
fn malformed_tokens_are_rejected() {
    for token in [
        "",
        "v1",
        "v1....",
        "v2.YQ.cm9vbS0x.9999999999.00",
        "v1.***.cm9vbS0x.9999999999.00",
    ] {
        assert!(
            verify_client_token(token, "secret").is_err(),
            "{token:?} must be rejected"
        );
    }
}

#[test]
fn signature_binds_the_boundary_between_client_and_room() {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    // (client "a_", room "b") and (client "a", room "_b") concatenate to the same bytes; the
    // signature of one must not validate the other.
    let secret = "secret";
    let token = generate_client_token(
        &ClientId::new("a_").unwrap(),
        &RoomId::new("b").unwrap(),
        Duration::from_secs(60),
        secret,
    );
    assert!(verify_client_token(&token, secret).is_ok());

    let mut parts: Vec<String> = token.split('.').map(str::to_string).collect();
    parts[1] = URL_SAFE_NO_PAD.encode("a");
    parts[2] = URL_SAFE_NO_PAD.encode("_b");
    assert!(verify_client_token(&parts.join("."), secret).is_err());
}

#[test]
fn non_canonical_expiry_is_rejected() {
    let secret = "secret";
    let token = generate_client_token(
        &ClientId::new("alice").unwrap(),
        &RoomId::new("room-1").unwrap(),
        Duration::from_secs(60),
        secret,
    );
    let parts: Vec<&str> = token.split('.').collect();
    let exp = parts[3];

    // Each variant decodes to the same expiry, so the signature still matches: only a strict
    // decimal format guarantees one token string per claim set.
    for variant in [format!("0{exp}"), format!("00{exp}"), format!("+{exp}")] {
        let mut altered: Vec<String> = parts.iter().map(|p| p.to_string()).collect();
        altered[3] = variant.clone();
        assert!(
            verify_client_token(&altered.join("."), secret).is_err(),
            "expiry {variant:?} must be rejected"
        );
    }
}
