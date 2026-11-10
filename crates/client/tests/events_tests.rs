//! The event stream of a transport: by default refused as by a server with its event stream
//! turned off, and over the test transport, the stream of the server in process.

mod common;

use async_trait::async_trait;
use common::*;
use std::time::Duration;
use zemdb_client::{EventStream, TransportClient, TransportError};
use zemdb_core::*;
use zemdb_server::generate_client_token;

/// A transport that only sends messages.
#[derive(Debug)]
struct SendOnly;

#[async_trait]
impl TransportClient for SendOnly {
    async fn send(&self, _: &ClientMessage, _: &str) -> Result<Vec<u8>, TransportError> {
        Err(TransportError("Unreachable".to_string()))
    }
}

#[tokio::test]
async fn a_transport_without_an_event_stream_refuses_it_with_events_disabled() {
    let room_id = RoomId::new("room-a").unwrap();
    let opened = SendOnly.events(&room_id, "t").await.unwrap();
    let EventStream::Refused(refusal) = opened else {
        panic!("{opened:?}");
    };
    let refusal = decode_message::<ServerMessage>(&refusal).unwrap();
    let ServerMessage::Error {
        correlation_id: None,
        room_id: Some(refused),
        code: ErrorCode::EventsDisabled,
        ..
    } = refusal
    else {
        panic!("{refusal:?}");
    };
    assert_eq!(refused, room_id);
}

#[tokio::test]
async fn the_test_transport_opens_the_stream_of_the_server_in_process() {
    let server = TestServer::new();
    let room_id = server.create_room("room-a").await;
    let client_id = ClientId::new("ana#phone").unwrap();
    let transport = server.transport();
    let opened = transport
        .events(&room_id, &token(&client_id, &room_id))
        .await;
    let EventStream::Open(mut blocks) = opened.unwrap() else {
        panic!("Refused");
    };
    let committed = server.commit(&room_id, [1]).await;
    let ServerMessage::OpsEvent { ops, .. } = ops_event(&next_event(&mut blocks, "ops").await)
    else {
        panic!("Not an ops event");
    };
    assert_eq!(ops, committed);

    let expired = generate_client_token(&client_id, &room_id, Duration::ZERO, AUTH_SECRET);
    let EventStream::Refused(refusal) = transport.events(&room_id, &expired).await.unwrap() else {
        panic!("Opened with an expired token");
    };
    let refusal = decode_message::<ServerMessage>(&refusal).unwrap();
    assert!(matches!(
        refusal,
        ServerMessage::Error {
            code: ErrorCode::Unauthorized,
            ..
        }
    ));
}
