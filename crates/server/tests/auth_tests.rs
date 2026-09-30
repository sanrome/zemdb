use rimdb_core::{ClientId, RoomId};
use rimdb_server::api::auth::{
    generate_client_token, verify_client_token, verify_client_token_bound,
};
use std::time::Duration;

#[test]
fn test_client_token_generation_and_verification() {
    let client_id = ClientId::new("client-alpha");
    let room_id = RoomId::new("room-xyz");
    let secret = "super_secret_cluster_key_12345";

    let token = generate_client_token(&client_id, &room_id, Duration::from_secs(60), secret);
    let verified = verify_client_token(&token, secret).expect("token must verify");
    assert_eq!(verified.client_id, client_id);
    assert_eq!(verified.room_id, room_id);

    assert!(verify_client_token_bound(&token, &client_id, &room_id, secret).is_ok());

    // Token mismatch client
    let wrong_client = ClientId::new("client-beta");
    assert!(verify_client_token_bound(&token, &wrong_client, &room_id, secret).is_err());

    // Token mismatch room
    let wrong_room = RoomId::new("room-other");
    assert!(verify_client_token_bound(&token, &client_id, &wrong_room, secret).is_err());

    // Wrong secret
    assert!(verify_client_token(&token, "wrong_secret").is_err());

    // Expired token (0 TTL)
    let expired_token = generate_client_token(&client_id, &room_id, Duration::from_secs(0), secret);
    assert!(verify_client_token(&expired_token, secret).is_err());

    // Dev backdoor elimination test
    assert!(verify_client_token("dev-token", "default_auth_secret_dev_32bytes!").is_err());
}
