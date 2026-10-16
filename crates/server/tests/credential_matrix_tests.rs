//! Every Data Plane endpoint against every credential it must refuse, checking that no refused
//! request changed either room, and every authenticated endpoint answering an unauthenticated
//! request without reading its body.

mod common;

use common::{envelope, make_test_op, tasks_schema, token, TestServer, ADMIN_SECRET, AUTH_SECRET};
use reqwest::{Method, StatusCode};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use zemdb_core::protocol::codec::MAX_FRAME_SIZE;
use zemdb_core::*;
use zemdb_server::generate_client_token;

fn cid(s: &str) -> ClientId {
    ClientId::new(s).unwrap()
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Ep {
    Commit,
    Sync,
    Ack,
    Heartbeat,
    Schema,
    Deregister,
    Events,
    Upload,
    Chunk,
    UploadChunk,
    Register,
}

const ALL: [Ep; 11] = [
    Ep::Commit,
    Ep::Sync,
    Ep::Ack,
    Ep::Heartbeat,
    Ep::Schema,
    Ep::Deregister,
    Ep::Events,
    Ep::Upload,
    Ep::Chunk,
    Ep::UploadChunk,
    Ep::Register,
];

impl Ep {
    fn path(self) -> &'static str {
        match self {
            Ep::Commit => "commit",
            Ep::Sync => "sync",
            Ep::Ack => "ack",
            Ep::Heartbeat => "heartbeat",
            Ep::Schema => "schema",
            Ep::Deregister => "deregister",
            Ep::Events => "events",
            Ep::Upload => "snapshot/upload",
            Ep::Chunk => "snapshot/chunk",
            Ep::UploadChunk => "snapshot/upload-chunk",
            Ep::Register => "register",
        }
    }

    fn is_relay(self) -> bool {
        matches!(self, Ep::Upload | Ep::Chunk | Ep::UploadChunk)
    }

    /// The message this endpoint expects, naming `room` and `client`.
    fn message(
        self,
        room: &RoomId,
        client: &ClientId,
        register_token: &str,
    ) -> Option<ClientMessage> {
        let correlation_id = CorrelationId::new(7);
        let snap = envelope(1000, 7);
        Some(match self {
            Ep::Commit => ClientMessage::Commit {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
                mutation_id: MutationId::new([0xA5; 16]),
                last_ack_seq: SequenceNumber::new(0),
                op: make_test_op(100),
            },
            Ep::Sync => ClientMessage::Sync {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
                from_seq: SequenceNumber::new(0),
                max_batch_size: 10,
            },
            Ep::Ack => ClientMessage::Ack {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
                ack_seq: SequenceNumber::new(2),
            },
            Ep::Heartbeat => ClientMessage::Heartbeat {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
            },
            Ep::Schema => ClientMessage::GetSchema {
                correlation_id,
                room_id: room.clone(),
            },
            Ep::Deregister => ClientMessage::DeregisterClient {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
            },
            Ep::Chunk => ClientMessage::RequestSnapshotChunk {
                correlation_id,
                room_id: room.clone(),
                chunk_index: 0,
                chunk_size: 64 * 1024,
                snapshot_hash: None,
            },
            Ep::UploadChunk => ClientMessage::UploadSnapshotChunk {
                correlation_id,
                room_id: room.clone(),
                snapshot_head_seq: SequenceNumber::new(2),
                chunk_index: 0,
                total_chunks: 1,
                total_bytes: snap.len() as u64,
                snapshot_hash: ServerMessage::compute_snapshot_hash(&snap),
                data: snap.into(),
            },
            Ep::Register => ClientMessage::RegisterClient {
                correlation_id,
                room_id: room.clone(),
                client_id: client.clone(),
                auth_token: register_token.to_string(),
                current_seq: None,
                known_incarnation: None,
            },
            Ep::Events | Ep::Upload => return None,
        })
    }

    /// A message of another kind than the endpoint expects.
    fn wrong_kind(self, room: &RoomId) -> ClientMessage {
        match self {
            Ep::Heartbeat => ClientMessage::GetSchema {
                correlation_id: CorrelationId::new(1),
                room_id: room.clone(),
            },
            _ => ClientMessage::Heartbeat {
                correlation_id: CorrelationId::new(1),
                room_id: room.clone(),
                client_id: cid("alice"),
            },
        }
    }
}

#[derive(Clone, Debug)]
enum Cred {
    None,
    Header(String),
    Query(String),
}

/// Sends a request to `ep` of `path_room` with `cred` and `body` (or the endpoint's message).
async fn send(
    server: &TestServer,
    ep: Ep,
    path_room: &RoomId,
    cred: &Cred,
    body: Option<Vec<u8>>,
) -> reqwest::Response {
    let mut url = server.url(&format!("/rooms/{path_room}/{}", ep.path()));
    if let Cred::Query(t) = cred {
        url = format!("{url}?token={t}");
    }
    let mut req = if ep == Ep::Events {
        server.client.get(url)
    } else {
        server.client.post(url)
    };
    // Registration carries its token inside the message, never in a header.
    if let (Cred::Header(t), false) = (cred, ep == Ep::Register) {
        req = req.bearer_auth(t);
    }
    if ep == Ep::Upload {
        req = req.header("x-snapshot-head-seq", "2");
    }
    if ep != Ep::Events {
        let body = body.unwrap_or_else(|| {
            if ep == Ep::Upload {
                envelope(1000, 7)
            } else {
                let register_token = match cred {
                    Cred::Header(t) => t.clone(),
                    _ => String::new(),
                };
                encode_message(
                    &ep.message(path_room, &cid("alice"), &register_token)
                        .unwrap(),
                )
                .unwrap()
            }
        });
        req = req.body(body);
    }
    req.send().await.unwrap()
}

async fn status_and_code(resp: reqwest::Response) -> (StatusCode, Option<ErrorCode>) {
    let status = resp.status();
    let is_sse = resp
        .headers()
        .get("content-type")
        .is_some_and(|ct| ct.to_str().unwrap().starts_with("text/event-stream"));
    if is_sse {
        return (status, None);
    }
    let body = resp.bytes().await.unwrap();
    let code = match decode_message::<ServerMessage>(&body) {
        Ok(ServerMessage::Error { code, .. }) => Some(code),
        _ => None,
    };
    (status, code)
}

struct Fixture {
    server: TestServer,
    room_a: RoomId,
    room_b: RoomId,
}

async fn fixture() -> Fixture {
    let server = TestServer::start(|_| {}).await;
    let schema = server.register_schema("tasks", tasks_schema());
    let room_a = server.create_room("room-a", &schema).await;
    let room_b = server.create_room("room-b", &schema).await;
    let (alice_token, _) = server.register(&room_a, &cid("alice")).await;
    server.register(&room_a, &cid("bob")).await;
    for i in 1..=2u8 {
        let msg = ClientMessage::Commit {
            correlation_id: CorrelationId::new(1),
            room_id: room_a.clone(),
            client_id: cid("alice"),
            mutation_id: MutationId::new([i; 16]),
            last_ack_seq: SequenceNumber::new(u64::from(i) - 1),
            op: make_test_op(i64::from(i)),
        };
        let (status, reply) = server
            .post_message(&room_a, "commit", &alice_token, &msg)
            .await;
        assert_eq!(status, StatusCode::OK, "{reply:?}");
    }
    Fixture {
        server,
        room_a,
        room_b,
    }
}

/// Room metrics as JSON (admin).
async fn metrics(server: &TestServer, room: &RoomId) -> serde_json::Value {
    let resp = server
        .admin(Method::GET, &format!("/admin/rooms/{room}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json().await.unwrap()
}

/// Every endpoint refuses every credential that does not grant it, with the documented status
/// and code, and nothing changes in either room.
#[tokio::test]
async fn every_endpoint_refuses_every_credential_that_does_not_grant_it() {
    let fx = fixture().await;
    let server = &fx.server;
    let (room_a, room_b) = (&fx.room_a, &fx.room_b);
    let alice_a = token(&cid("alice"), room_a);
    let alice_b = token(&cid("alice"), room_b);
    let expired = generate_client_token(&cid("alice"), room_a, Duration::ZERO, AUTH_SECRET);
    let bad_sig = {
        let mut t = alice_a.clone();
        let last = t.pop().unwrap();
        t.push(if last == '0' { '1' } else { '0' });
        t
    };
    let other_secret = generate_client_token(
        &cid("alice"),
        room_a,
        Duration::from_secs(60),
        "another_secret_of_32_bytes_xxxxx",
    );
    let before_a = metrics(server, room_a).await;
    let before_b = metrics(server, room_b).await;

    let refused: Vec<(&str, Cred, StatusCode, ErrorCode)> = vec![
        (
            "none",
            Cred::None,
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
        ),
        (
            "malformed",
            Cred::Header("garbage".into()),
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
        ),
        (
            "bad-signature",
            Cred::Header(bad_sig.clone()),
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
        ),
        (
            "other-secret",
            Cred::Header(other_secret.clone()),
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
        ),
        (
            "expired",
            Cred::Header(expired.clone()),
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
        ),
        (
            "other-room",
            Cred::Header(alice_b.clone()),
            StatusCode::FORBIDDEN,
            ErrorCode::Forbidden,
        ),
    ];
    for ep in ALL {
        for (name, cred, status, code) in &refused {
            let got = status_and_code(send(server, ep, room_a, cred, None).await).await;
            assert_eq!(got, (*status, Some(*code)), "{ep:?} with {name}");
        }
    }

    // Admin secret: only the relay accepts it.
    for ep in ALL.into_iter().filter(|ep| !ep.is_relay()) {
        let got = status_and_code(
            send(server, ep, room_a, &Cred::Header(ADMIN_SECRET.into()), None).await,
        )
        .await;
        assert_eq!(
            got,
            (StatusCode::UNAUTHORIZED, Some(ErrorCode::Unauthorized)),
            "{ep:?} with the admin secret"
        );
    }

    // Token in the query string: only /events, and never the admin secret.
    for ep in ALL.into_iter().filter(|ep| *ep != Ep::Register) {
        let got =
            status_and_code(send(server, ep, room_a, &Cred::Query(alice_a.clone()), None).await)
                .await;
        if ep == Ep::Events {
            assert_eq!(got.0, StatusCode::OK);
        } else {
            assert_eq!(
                got,
                (StatusCode::UNAUTHORIZED, Some(ErrorCode::Unauthorized)),
                "{ep:?}"
            );
        }
        let got = status_and_code(
            send(server, ep, room_a, &Cred::Query(ADMIN_SECRET.into()), None).await,
        )
        .await;
        assert_eq!(
            got,
            (StatusCode::UNAUTHORIZED, Some(ErrorCode::Unauthorized)),
            "{ep:?}"
        );
    }

    // Valid token, payload naming another client: 403 wherever the message has a client.
    for ep in [
        Ep::Commit,
        Ep::Sync,
        Ep::Ack,
        Ep::Heartbeat,
        Ep::Deregister,
        Ep::Register,
    ] {
        let msg = ep.message(room_a, &cid("bob"), &alice_a).unwrap();
        let got = status_and_code(
            send(
                server,
                ep,
                room_a,
                &Cred::Header(alice_a.clone()),
                Some(encode_message(&msg).unwrap()),
            )
            .await,
        )
        .await;
        assert_eq!(
            got,
            (StatusCode::FORBIDDEN, Some(ErrorCode::Forbidden)),
            "{ep:?}"
        );
    }

    // Valid token for the path room, payload naming another room: 400 on every binary endpoint.
    for ep in ALL
        .into_iter()
        .filter(|ep| !matches!(ep, Ep::Events | Ep::Upload))
    {
        let msg = ep.message(room_b, &cid("alice"), &alice_a).unwrap();
        let got = status_and_code(
            send(
                server,
                ep,
                room_a,
                &Cred::Header(alice_a.clone()),
                Some(encode_message(&msg).unwrap()),
            )
            .await,
        )
        .await;
        assert_eq!(
            got,
            (StatusCode::BAD_REQUEST, Some(ErrorCode::BadRequest)),
            "{ep:?}"
        );
    }

    // A message of another kind: 400.
    for ep in ALL
        .into_iter()
        .filter(|ep| !matches!(ep, Ep::Events | Ep::Upload))
    {
        let msg = ep.wrong_kind(room_a);
        let got = status_and_code(
            send(
                server,
                ep,
                room_a,
                &Cred::Header(alice_a.clone()),
                Some(encode_message(&msg).unwrap()),
            )
            .await,
        )
        .await;
        assert_eq!(
            got,
            (StatusCode::BAD_REQUEST, Some(ErrorCode::BadRequest)),
            "{ep:?}"
        );
    }

    // Nothing changed: same head, same roster, no snapshot, bob still registered with his cursor.
    let after_a = metrics(server, room_a).await;
    let after_b = metrics(server, room_b).await;
    for key in ["head_seq", "tail_seq", "total_clients", "connected_clients"] {
        assert_eq!(before_a[key], after_a[key], "room-a {key}");
        assert_eq!(before_b[key], after_b[key], "room-b {key}");
    }
    assert_eq!(after_a["head_seq"], 2);
    assert_eq!(after_a["total_clients"], 2);
    assert!(server.relay.active_snapshot_seq(room_a).is_none());
    assert!(server.relay.active_snapshot_seq(room_b).is_none());
    let bob = token(&cid("bob"), room_a);
    let (status, reply) = server
        .post_message(
            room_a,
            "heartbeat",
            &bob,
            &ClientMessage::Heartbeat {
                correlation_id: CorrelationId::new(9),
                room_id: room_a.clone(),
                client_id: cid("bob"),
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");

    // The admin secret is accepted by the relay (authorization passes; the outcome is the
    // relay's own answer).
    for ep in [Ep::Chunk, Ep::Upload, Ep::UploadChunk] {
        let (status, code) = status_and_code(
            send(server, ep, room_a, &Cred::Header(ADMIN_SECRET.into()), None).await,
        )
        .await;
        assert!(
            !matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN),
            "{ep:?}: {status} {code:?}"
        );
    }
}

/// Request head of a POST to `path` declaring `content_length` bytes, with no credentials.
fn head(path: &str, content_type: &str, content_length: usize) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: zemdb\r\nContent-Type: {content_type}\r\n\
         Content-Length: {content_length}\r\n\r\n"
    )
}

async fn read_response_head(stream: &mut TcpStream) -> String {
    let mut received = Vec::new();
    let mut buf = [0u8; 1024];
    while !received.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed before the response");
        received.extend_from_slice(&buf[..n]);
    }
    String::from_utf8_lossy(&received).into_owned()
}

/// An unauthenticated request with a declared body of `MAX_FRAME_SIZE` bytes is answered
/// without waiting for a single body byte, on every endpoint that authenticates.
#[tokio::test]
async fn unauthenticated_requests_are_answered_without_reading_the_body() {
    let fx = fixture().await;
    let addr = fx.server.base_url.trim_start_matches("http://").to_string();
    let other_room_token = token(&cid("alice"), &fx.room_b);
    let paths = [
        ("/rooms/room-a/commit", "application/octet-stream"),
        ("/rooms/room-a/sync", "application/octet-stream"),
        ("/rooms/room-a/ack", "application/octet-stream"),
        ("/rooms/room-a/heartbeat", "application/octet-stream"),
        ("/rooms/room-a/schema", "application/octet-stream"),
        ("/rooms/room-a/deregister", "application/octet-stream"),
        ("/rooms/room-a/snapshot/upload", "application/octet-stream"),
        ("/rooms/room-a/snapshot/chunk", "application/octet-stream"),
        (
            "/rooms/room-a/snapshot/upload-chunk",
            "application/octet-stream",
        ),
        ("/admin/schemas", "application/json"),
        ("/admin/schemas/tasks/columns", "application/json"),
        ("/admin/rooms", "application/json"),
    ];
    for (path, content_type) in paths {
        for auth in [
            None,
            Some(format!("Authorization: Bearer {other_room_token}\r\n")),
        ] {
            // A token for another room is still refused before the body on the data plane;
            // on admin endpoints it is just a wrong secret.
            let mut stream = TcpStream::connect(&addr).await.unwrap();
            let mut request = head(path, content_type, MAX_FRAME_SIZE);
            if let Some(auth) = &auth {
                request = request.replacen("Host: zemdb\r\n", &format!("Host: zemdb\r\n{auth}"), 1);
            }
            stream.write_all(request.as_bytes()).await.unwrap();
            let response =
                tokio::time::timeout(Duration::from_secs(3), read_response_head(&mut stream))
                    .await
                    .unwrap_or_else(|_| panic!("{path}: no answer without the body"));
            let expected = if auth.is_some() && path.starts_with("/rooms/") {
                "HTTP/1.1 403"
            } else {
                "HTTP/1.1 401"
            };
            assert!(response.starts_with(expected), "{path}: {response}");
        }
    }

    // Registration reads at most MAX_REGISTER_BODY_SIZE (+1) before refusing.
    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(
            head(
                "/rooms/room-a/register",
                "application/octet-stream",
                MAX_FRAME_SIZE,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    stream.write_all(&vec![0u8; 64 * 1024 + 1]).await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(3), read_response_head(&mut stream))
        .await
        .expect("register answers once its limit is passed");
    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
}
