use super::*;
use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use std::time::Duration;
use zemdb_server::generate_client_token;

const SECRET: &str = "test_cluster_secret_key_1234567890";

fn client(id: &str) -> ClientId {
    ClientId::new(id).unwrap()
}

fn room() -> RoomId {
    RoomId::new("room-a").unwrap()
}

fn issue(client_id: &ClientId, ttl: Duration) -> String {
    generate_client_token(client_id, &room(), ttl, SECRET)
}

/// `token` with its field `index` (from 0) replaced by `field`.
fn with_field(token: &str, index: usize, field: &str) -> String {
    let mut fields: Vec<&str> = token.split('.').collect();
    fields[index] = field;
    fields.join(".")
}

#[test]
fn a_token_the_server_issues_names_its_client() {
    // Ids whose encoding in base64url has the characters that set it apart from standard
    // base64, and need no padding.
    for id in [
        "ana#phone",
        "ana#>>>>>",
        "ana#~~~~~",
        "ana#0123456789abcdefg",
    ] {
        let client_id = client(id);
        assert!(URL_SAFE_NO_PAD.encode(id).len() % 4 == 0, "{id}");
        for ttl in [Duration::ZERO, Duration::from_secs(300)] {
            let token = issue(&client_id, ttl);
            assert!(issued_to(&token, &client_id), "{token}");
            // A token of the same client for another room names it too: the server checks
            // the room.
            let other_room = RoomId::new("room-b").unwrap();
            let token = generate_client_token(&client_id, &other_room, ttl, SECRET);
            assert!(issued_to(&token, &client_id), "{token}");
        }
    }
    // An id whose encoding is shorter than a multiple of four.
    let client_id = client("ana#phones");
    assert!(issued_to(&issue(&client_id, Duration::ZERO), &client_id));
}

#[test]
fn a_token_of_another_client_does_not() {
    let token = issue(&client("ana#tablet"), Duration::from_secs(300));
    for other in ["ana#phone", "ana#tablet2", "ana#table", "bob#tablet", "ana"] {
        assert!(!issued_to(&token, &client(other)), "{other}");
    }
}

#[test]
fn a_token_without_the_format_of_the_server_names_no_client() {
    let client_id = client("ana#>>>>>");
    let token = issue(&client_id, Duration::from_secs(300));
    let encoded = URL_SAFE_NO_PAD.encode(client_id.as_str());
    let fields: Vec<&str> = token.split('.').collect();
    let not_tokens = [
        String::new(),
        "not-a-token".to_string(),
        encoded.clone(),
        format!("v1.{encoded}"),
        // Four fields, and six.
        fields[..4].join("."),
        format!("{token}.extra"),
        format!("{token}."),
        // Another version.
        with_field(&token, 0, "v2"),
        with_field(&token, 0, "V1"),
        with_field(&token, 0, ""),
        // The client in standard base64, with padding, or not in base64.
        with_field(&token, 1, &STANDARD.encode(client_id.as_str())),
        with_field(&token, 1, &format!("{encoded}=")),
        with_field(&token, 1, "ana#>>>>>"),
        with_field(&token, 1, ""),
        // Bytes that are not UTF-8.
        with_field(&token, 1, &URL_SAFE_NO_PAD.encode([0xff, 0xfe, 0xfd])),
        // The room not in base64url, or empty.
        with_field(&token, 2, "room$a"),
        with_field(&token, 2, ""),
        // The expiry not in decimal digits, or empty.
        with_field(&token, 3, "12a"),
        with_field(&token, 3, "-1"),
        with_field(&token, 3, ""),
        // The signature not in hexadecimal, or empty.
        with_field(&token, 4, "xyz"),
        with_field(&token, 4, ""),
    ];
    for not_token in not_tokens {
        assert!(!issued_to(&not_token, &client_id), "{not_token}");
    }
    // The client of an id that needs padding, with it.
    let client_id = client("ana#phones");
    let token = issue(&client_id, Duration::from_secs(300));
    let padded = with_field(&token, 1, &URL_SAFE.encode(client_id.as_str()));
    assert_ne!(padded, token);
    assert!(!issued_to(&padded, &client_id));
}
