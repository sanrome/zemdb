use super::*;
use crate::api::auth::generate_client_token;
use crate::api::router::build_router;
use crate::api::test_support::TestState;
use crate::fail_point;
use axum::body::Body;
use axum::http::Request;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;
use zemdb_core::id::{ClientId, MutationId, SchemaId, SequenceNumber};
use zemdb_core::mutation::{Operation, OperationKind};
use zemdb_core::protocol::codec::{
    decode_message, MAX_FRAME_SIZE, MAX_MESSAGE_SIZE, PROTOCOL_HEADER_LEN,
};
use zemdb_core::protocol::messages::{ErrorCode, SequencedOperation};
use zemdb_core::protocol::wal_frame::encode_wal_record;
use zemdb_core::schema::{Schema, TableSchema, MAX_COLUMNS};
use zemdb_core::value::{CompactRow, DataType, PrimaryKey, RowBuilder, Value};

const AUTH_SECRET: &str = "data_plane_cluster_secret_key_1234";

struct Fixture {
    dir: TempDir,
    app: axum::Router,
    manager: std::sync::Arc<crate::RoomManager>,
    relay: std::sync::Arc<crate::relay::SnapshotRelay>,
    room_id: RoomId,
    /// The incarnation of the room, which the snapshot chunks carry.
    incarnation: u128,
    client_id: ClientId,
    token: String,
}

/// Int columns of the `wide` table, besides its key and its string column.
const WIDE_INT_COLUMNS: usize = 5000;

fn schema() -> Schema {
    let tasks = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .nullable_column("title", DataType::String);
    let mut wide = TableSchema::builder("wide")
        .primary_key("id", DataType::Int)
        .nullable_column("text", DataType::String);
    for i in 0..WIDE_INT_COLUMNS {
        wide = wide.nullable_column(format!("c{i}"), DataType::Int);
    }
    Schema::builder().table(tasks).table(wide).build()
}

/// Insert into `tasks` with a title of `title_len` bytes.
fn task_with_title(id: i64, title_len: usize) -> Operation {
    let row = RowBuilder::new()
        .set("id", id)
        .set("title", "x".repeat(title_len))
        .build();
    schema().to_operation_insert("tasks", &row, 1000).unwrap()
}

/// Insert into `wide` with a text of `text_len` bytes and every int column set to 0. Each
/// small int takes 2 bytes on the wire and 12 in a log record.
fn wide_row(id: i64, text_len: usize) -> Operation {
    let table_id = schema().get_table_by_name("wide").unwrap().table_id();
    let mut values = vec![Value::Int(id), Value::String("x".repeat(text_len).into())];
    values.extend(std::iter::repeat_n(Value::Int(0), WIDE_INT_COLUMNS));
    Operation::new(
        table_id,
        PrimaryKey::single(Value::Int(id)),
        1000,
        OperationKind::Insert {
            row: CompactRow::new(values),
        },
    )
}

impl Fixture {
    /// A router over a room with one registered client.
    async fn new() -> Self {
        Self::with(|_| {}).await
    }

    /// [`Fixture::new`] with the configuration adjusted by `configure`.
    async fn with(configure: impl FnOnce(&mut crate::config::ServerConfig)) -> Self {
        let dir = tempdir().unwrap();
        let state = TestState::new(dir.path(), |config| {
            config.auth_secret = AUTH_SECRET.to_string();
            configure(config);
        });
        let schema_id = SchemaId::new("tasks").unwrap();
        state
            .registry
            .register_schema(schema_id.clone(), schema())
            .unwrap();
        let room_id = RoomId::new("room").unwrap();
        let incarnation = state
            .manager
            .create_room(room_id.clone(), schema_id, None)
            .await
            .unwrap()
            .room_incarnation;
        let client_id = ClientId::new("writer").unwrap();
        let token =
            generate_client_token(&client_id, &room_id, Duration::from_secs(300), AUTH_SECRET);
        let fx = Self {
            app: build_router(state.app_state()),
            manager: state.manager,
            relay: state.relay,
            dir,
            room_id,
            incarnation,
            client_id,
            token,
        };

        fx.register().await;
        fx
    }

    /// Registers the client, which must succeed.
    async fn register(&self) {
        let (status, _, _) = self.post("register", &self.register_msg()).await;
        assert_eq!(status, StatusCode::OK);
    }

    fn register_msg(&self) -> ClientMessage {
        ClientMessage::RegisterClient {
            correlation_id: CorrelationId::new(1),
            room_id: self.room_id.clone(),
            client_id: self.client_id.clone(),
            auth_token: self.token.clone(),
            current_seq: None,
            known_incarnation: None,
        }
    }

    /// Posts `msg`, returning the response without reading its body.
    async fn send(&self, endpoint: &str, msg: &ClientMessage) -> Response {
        let request = Request::post(format!("/rooms/{}/{endpoint}", self.room_id))
            .header(header::AUTHORIZATION, format!("Bearer {}", self.token))
            .body(Body::from(encode_message(msg).unwrap()))
            .unwrap();
        self.app.clone().oneshot(request).await.unwrap()
    }

    async fn post(
        &self,
        endpoint: &str,
        msg: &ClientMessage,
    ) -> (StatusCode, axum::http::HeaderMap, ServerMessage) {
        let response = self.send(endpoint, msg).await;
        let status = response.status();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, headers, decode_message(&body).unwrap())
    }

    fn commit(&self, n: u8, last_ack_seq: u64) -> ClientMessage {
        let row = RowBuilder::new().set("id", n as i64).build();
        self.commit_op(
            n,
            last_ack_seq,
            schema().to_operation_insert("tasks", &row, 1000).unwrap(),
        )
    }

    fn commit_op(&self, n: u8, last_ack_seq: u64, op: Operation) -> ClientMessage {
        ClientMessage::Commit {
            correlation_id: CorrelationId::new(n as u64),
            room_id: self.room_id.clone(),
            client_id: self.client_id.clone(),
            mutation_id: MutationId::new([n; 16]),
            last_ack_seq: SequenceNumber::new(last_ack_seq),
            op,
        }
    }

    fn sync(&self, from_seq: u64) -> ClientMessage {
        ClientMessage::Sync {
            correlation_id: CorrelationId::new(100 + from_seq),
            room_id: self.room_id.clone(),
            client_id: self.client_id.clone(),
            from_seq: SequenceNumber::new(from_seq),
            max_batch_size: 100,
        }
    }

    /// The first chunk of a download, anchored to `snapshot_hash` if given.
    fn first_chunk(&self, snapshot_hash: Option<[u8; 32]>) -> ClientMessage {
        ClientMessage::RequestSnapshotChunk {
            correlation_id: CorrelationId::new(3),
            room_id: self.room_id.clone(),
            chunk_index: 0,
            chunk_size: 0,
            snapshot_hash,
            room_incarnation: self.incarnation,
        }
    }

    /// Has the snapshot worker stage a snapshot at `seq` against `bounds`.
    async fn stage_snapshot(
        &self,
        seq: u64,
        bounds: impl std::future::Future<Output = Result<crate::relay::LogBounds, ServerError>>,
    ) -> [u8; 32] {
        let body = b"snapshot";
        let header = zemdb_core::protocol::snapshot_envelope::SnapshotEnvelopeHeader::for_body(
            zemdb_core::protocol::snapshot_envelope::SnapshotCompression::Raw,
            body.len() as u32,
            body,
        );
        let data = [&header.to_bytes()[..], body].concat();
        self.relay
            .stage_snapshot(
                &self.room_id,
                &Uploader::Admin,
                SequenceNumber::new(seq),
                data.into(),
                bounds,
            )
            .await
            .unwrap()
    }

    /// The budget holder of the client.
    fn holder(&self) -> BudgetHolder {
        (
            self.room_id.clone(),
            Uploader::Client(self.client_id.clone()),
        )
    }

    /// Holds all of the client's part of the budget of response bytes in flight (the factory
    /// part, two frames) but `room` bytes, as its unread responses would.
    fn hold_all_but(&self, room: u64) -> Reservation {
        let part = 2 * MAX_FRAME_SIZE as u64;
        let budget = self.manager.response_budget();
        budget.reserve(self.holder(), part - room).unwrap()
    }

    /// Posts a commit, expecting it to be acknowledged.
    async fn commit_ok(&self, msg: &ClientMessage) {
        let (status, _, reply) = self.post("commit", msg).await;
        assert_eq!(status, StatusCode::OK, "{reply:?}");
    }
}

/// Asserts an error frame with `code`, answered with HTTP `expected`.
fn assert_error(status: StatusCode, expected: StatusCode, code: ErrorCode, reply: &ServerMessage) {
    assert_eq!(status, expected, "{reply:?}");
    assert!(
        matches!(reply, ServerMessage::Error { code: got, .. } if *got == code),
        "{reply:?}"
    );
}

/// Asserts a `BadRequest` error frame with HTTP 400.
fn assert_bad_request(status: StatusCode, reply: &ServerMessage) {
    assert_error(
        status,
        StatusCode::BAD_REQUEST,
        ErrorCode::BadRequest,
        reply,
    );
}

#[tokio::test]
async fn commit_that_restarts_the_room_is_unavailable_with_retry_after() {
    let fx = Fixture::new().await;
    let (status, _, _) = fx.post("commit", &fx.commit(1, 0)).await;
    assert_eq!(status, StatusCode::OK);

    // The log append fails: the room stops to be recovered from disk.
    fail_point::arm(
        "warm_append_write",
        &fx.dir
            .path()
            .join("rooms")
            .join(fx.room_id.as_str())
            .join("segments")
            .join("active.wal"),
    );
    let (status, headers, reply) = fx.post("commit", &fx.commit(2, 1)).await;
    assert_error(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Unavailable,
        &reply,
    );
    assert_eq!(headers[header::RETRY_AFTER], "1");

    // Retrying is safe and succeeds once the room is back.
    let (status, _, reply) = fx.post("commit", &fx.commit(2, 1)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(matches!(
        reply,
        ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(2)
    ));
}

#[tokio::test]
async fn request_that_makes_the_room_panic_is_internal_and_the_room_recovers() {
    let fx = Fixture::new().await;
    let heartbeat = ClientMessage::Heartbeat {
        correlation_id: CorrelationId::new(7),
        room_id: fx.room_id.clone(),
        client_id: fx.client_id.clone(),
    };

    fail_point::arm(
        "room_command_panic",
        &fx.dir.path().join("rooms").join(fx.room_id.as_str()),
    );
    let (status, headers, reply) = fx.post("heartbeat", &heartbeat).await;
    assert_error(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCode::Internal,
        &reply,
    );
    assert!(headers.get(header::RETRY_AFTER).is_none());

    // The next request reopens the room from disk. The roster is not saved after a panic, so
    // the client registers again (its registration may not have reached the disk).
    fx.register().await;
    let (status, _, reply) = fx.post("heartbeat", &heartbeat).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    assert!(
        matches!(reply, ServerMessage::HeartbeatAck { .. }),
        "{reply:?}"
    );
}

#[test]
fn retryable_errors_map_to_their_own_codes_and_statuses() {
    let unavailable = binary_error(
        None,
        None,
        ServerError::Unavailable("room restarting".to_string()),
    );
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unavailable.headers()[header::RETRY_AFTER], "1");

    let timeout = binary_error(None, None, ServerError::Timeout("slow room".to_string()));
    assert_eq!(timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(timeout.headers().get(header::RETRY_AFTER).is_none());
    assert_eq!(
        ServerError::Timeout(String::new()).to_error_code(),
        ErrorCode::Timeout
    );
}

#[tokio::test]
async fn commit_of_the_largest_frame_whose_log_record_does_not_fit_is_a_bad_request() {
    let fx = Fixture::new().await;
    fx.commit_ok(&fx.commit(1, 0)).await;

    // A commit request of exactly the largest frame: legal on the wire, but its log record
    // (fixed-width integers) is larger than the log's limit.
    let probe_len = 1 << 20;
    let overhead = encode_message(&fx.commit_op(2, 1, task_with_title(2, probe_len)))
        .unwrap()
        .len()
        - probe_len;
    let msg = fx.commit_op(2, 1, task_with_title(2, MAX_FRAME_SIZE - overhead));
    assert_eq!(encode_message(&msg).unwrap().len(), MAX_FRAME_SIZE);

    let (status, _, reply) = fx.post("commit", &msg).await;
    assert_bad_request(status, &reply);

    // Nothing changed and the room kept running: the next commit gets the next sequence.
    let (status, _, reply) = fx.post("commit", &fx.commit(3, 1)).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    assert!(matches!(
        reply,
        ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(2)
    ));
}

#[tokio::test]
async fn commit_whose_small_ints_expand_past_the_log_limit_is_a_bad_request() {
    let fx = Fixture::new().await;

    // On the wire the operation fits comfortably in a request and in a reply...
    let probe = encode_message(&wide_row(1, 0)).unwrap().len();
    let op = wide_row(1, MAX_MESSAGE_SIZE as usize - 40_000 - probe);
    let wire_len = encode_message(&op).unwrap().len();
    assert!(wire_len < MAX_MESSAGE_SIZE as usize - 30_000);
    // ...but its small ints take 12 bytes each in the log record, which no longer fits.
    let mutation_id = MutationId::new([1; 16]);
    assert!(encode_wal_record(&SequencedOperation::new(1, op.clone()), Some(mutation_id)).is_err());

    let (status, _, reply) = fx.post("commit", &fx.commit_op(1, 0, op)).await;
    assert_bad_request(status, &reply);

    let (status, _, reply) = fx.post("commit", &fx.commit(2, 0)).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    assert!(matches!(
        reply,
        ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(1)
    ));
}

#[tokio::test]
async fn register_rejects_another_message_kind_without_decoding_it() {
    let fx = Fixture::new().await;
    // A frame tagged as a Commit whose fields are garbage. Registration does not authenticate
    // the request before reading the message, so it must stop at the tag: decoding the rest
    // would answer "malformed" instead (and, with a crafted row, reserve its memory first).
    let mut frame = encode_message(&fx.commit(1, 0)).unwrap();
    frame.truncate(PROTOCOL_HEADER_LEN + 1);
    frame.extend_from_slice(&[0xFF; 64]);
    let request = Request::post(format!("/rooms/{}/register", fx.room_id))
        .body(Body::from(frame))
        .unwrap();
    let response = fx.app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let reply: ServerMessage = decode_message(&body).unwrap();
    assert_bad_request(status, &reply);
    assert!(
        matches!(&reply, ServerMessage::Error { message, room_id: Some(room), .. }
            if message.contains("Expected RegisterClient message") && *room == fx.room_id),
        "{reply:?}"
    );
}

/// Posts a registration frame of exactly `len` bytes (its token padded), without credentials.
async fn register_with_frame_of(fx: &Fixture, len: usize) -> (StatusCode, ServerMessage) {
    let register = |auth_token: String| ClientMessage::RegisterClient {
        correlation_id: CorrelationId::new(1),
        room_id: fx.room_id.clone(),
        client_id: fx.client_id.clone(),
        auth_token,
        current_seq: None,
        known_incarnation: None,
    };
    let probe = encode_message(&register("x".repeat(1024))).unwrap().len();
    let frame = encode_message(&register("x".repeat(1024 + len - probe))).unwrap();
    assert_eq!(frame.len(), len);
    let request = Request::post(format!("/rooms/{}/register", fx.room_id))
        .body(Body::from(frame))
        .unwrap();
    let response = fx.app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, decode_message(&body).unwrap())
}

#[tokio::test]
async fn registration_has_its_own_small_body_limit() {
    use crate::api::router::MAX_REGISTER_BODY_SIZE;
    let fx = Fixture::new().await;

    // At the limit the frame is read and decoded: the padded token is then rejected.
    let (status, reply) = register_with_frame_of(&fx, MAX_REGISTER_BODY_SIZE).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{reply:?}");

    // One byte more is rejected before it is buffered, as the general body limit does.
    let (status, reply) = register_with_frame_of(&fx, MAX_REGISTER_BODY_SIZE + 1).await;
    assert_error(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::BadRequest,
        &reply,
    );
}

#[tokio::test]
async fn commit_with_more_values_than_a_table_has_columns_is_rejected_when_decoded() {
    let fx = Fixture::new().await;
    // One Null more than any table can have columns: 64 KiB on the wire, rejected by the
    // decoder before the room sees it (the room would answer `SchemaViolation`).
    let table_id = schema().get_table_by_name("tasks").unwrap().table_id();
    let op = Operation::insert(
        table_id,
        PrimaryKey::single(Value::Int(1)),
        CompactRow::new(vec![Value::Null; MAX_COLUMNS + 1]),
        1000,
    );
    let (status, _, reply) = fx.post("commit", &fx.commit_op(1, 0, op)).await;
    assert_bad_request(status, &reply);
    assert!(
        matches!(&reply, ServerMessage::Error { message, .. } if message.contains("Malformed")),
        "{reply:?}"
    );

    // The room is untouched: the next commit gets the first sequence.
    let (status, _, reply) = fx.post("commit", &fx.commit(2, 0)).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    assert!(matches!(
        reply,
        ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(1)
    ));
}

#[tokio::test]
async fn large_operations_are_split_across_catch_up_and_sync_batches() {
    let fx = Fixture::new().await;
    let six_mib = 6 * 1024 * 1024;
    for n in 1..=3u8 {
        fx.commit_ok(&fx.commit_op(n, n as u64 - 1, task_with_title(n as i64, six_mib)))
            .await;
    }

    // The catch-up of a commit from cursor 0 would carry 4 operations of 6 MiB: only as many
    // as fit in one frame are returned, flagged as partial.
    let (status, _, reply) = fx
        .post("commit", &fx.commit_op(4, 0, task_with_title(4, six_mib)))
        .await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    match reply {
        ServerMessage::CommitAck {
            assigned_seq,
            catchup_ops,
            has_more,
            ..
        } => {
            assert_eq!(assigned_seq, SequenceNumber::new(4));
            let seqs: Vec<u64> = catchup_ops.iter().map(|op| op.seq.get()).collect();
            assert_eq!(seqs, vec![1, 2]);
            assert!(has_more);
        }
        other => panic!("expected CommitAck, got {other:?}"),
    }

    // Sync pages through the same operations, at least one per batch.
    let mut from = 0;
    let mut pages = Vec::new();
    loop {
        let (status, _, reply) = fx.post("sync", &fx.sync(from)).await;
        assert_eq!(status, StatusCode::OK, "{reply:?}");
        let ServerMessage::SyncBatch { ops, has_more, .. } = reply else {
            panic!("expected SyncBatch, got {reply:?}");
        };
        assert!(!ops.is_empty());
        from = ops.last().unwrap().seq.get();
        pages.push(ops.iter().map(|op| op.seq.get()).collect::<Vec<_>>());
        if !has_more {
            break;
        }
    }
    assert_eq!(pages, vec![vec![1, 2], vec![3, 4]]);
}

#[tokio::test]
async fn responses_without_room_in_the_budget_are_unavailable_before_they_are_built() {
    let fx = Fixture::new().await;
    // Not even what a response takes besides its operations is left.
    let held = fx.hold_all_but(RESPONSE_OVERHEAD - 1);
    let get_schema = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(2),
        room_id: fx.room_id.clone(),
    };
    // Nothing is served: a snapshot chunk, before the relay reads it.
    let bounds = crate::relay::LogBounds {
        tail_seq: SequenceNumber::new(1),
        head_seq: SequenceNumber::new(1),
        unrestorable_through: None,
        room_incarnation: 0,
    };
    let hash = fx.stage_snapshot(1, std::future::ready(Ok(bounds))).await;
    let read = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_flag = std::sync::Arc::clone(&read);
    fail_point::arm_hook(
        "relay_read_chunk",
        &fx.dir.path().join("snapshots"),
        move || {
            read_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        },
    );
    let chunk = fx.first_chunk(Some(hash));
    for (endpoint, msg) in [
        ("schema", get_schema.clone()),
        ("register", fx.register_msg()),
        ("sync", fx.sync(0)),
        ("snapshot/chunk", chunk),
    ] {
        let (status, _, reply) = fx.post(endpoint, &msg).await;
        let expected = StatusCode::SERVICE_UNAVAILABLE;
        assert_error(status, expected, ErrorCode::Unavailable, &reply);
    }
    assert!(!read.load(std::sync::atomic::Ordering::SeqCst));
    // A commit, durable, is acknowledged without its catch-up, and so is its retry.
    for _ in 0..2 {
        let (status, _, reply) = fx.post("commit", &fx.commit(1, 0)).await;
        assert_eq!(status, StatusCode::OK);
        let ServerMessage::CommitAck {
            catchup_ops,
            has_more,
            ..
        } = reply
        else {
            panic!("expected CommitAck, got {reply:?}");
        };
        assert!(catchup_ops.is_empty() && has_more);
    }

    drop(held);
    let (status, _, reply) = fx.post("schema", &get_schema).await;
    assert_eq!(status, StatusCode::OK, "{reply:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_waiting_for_its_room_holds_none_of_the_budget() {
    // One frame per client.
    let fx = Fixture::with(|config| {
        config.max_inflight_response_bytes_per_client = MAX_FRAME_SIZE as u64;
    })
    .await;
    // The room's actor stalls, as on a slow fsync.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let room_dir = crate::actor::room::room_dir(fx.dir.path(), &fx.room_id);
    fail_point::arm_hook("room_shutdown", &room_dir, move || {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    let sender = fx.manager.get_or_spawn(&fx.room_id, None).await.unwrap();
    let (reply, _) = tokio::sync::oneshot::channel();
    sender.send(RoomCommand::Shutdown { reply }).await.unwrap();
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    // A sync and a first chunk, which asks the room for its log range, wait for it.
    let chunk = |chunk_index, snapshot_hash| ClientMessage::RequestSnapshotChunk {
        correlation_id: CorrelationId::new(3),
        room_id: fx.room_id.clone(),
        chunk_index,
        chunk_size: 0,
        snapshot_hash,
        room_incarnation: fx.incarnation,
    };
    let (sync, first_chunk) = (fx.sync(0), chunk(0, None));
    let waiting = futures::future::join(
        fx.send("sync", &sync),
        fx.send("snapshot/chunk", &first_chunk),
    );
    tokio::pin!(waiting);
    let wait = tokio::time::timeout(Duration::from_millis(200), &mut waiting).await;
    assert!(wait.is_err());

    // Without holding the client's part: an anchored chunk, which the relay answers without
    // the room, still gets its frame.
    let (status, _, reply) = fx.post("snapshot/chunk", &chunk(1, Some([0; 32]))).await;
    assert_error(
        status,
        StatusCode::CONFLICT,
        ErrorCode::SnapshotSuperseded,
        &reply,
    );
    release_tx.send(()).unwrap();
    waiting.await;
}

/// What the operation of `msg`, a commit, takes in a response as the operation `seq`.
fn in_response(seq: u64, msg: &ClientMessage) -> u64 {
    let ClientMessage::Commit { op, .. } = msg else {
        panic!("expected a commit");
    };
    let op = SequencedOperation::new(SequenceNumber::new(seq), op.clone());
    crate::log::budget::OpSize::of(&op).in_response()
}

/// The sequences of `ops`.
fn seqs_of(ops: &[SequencedOperation]) -> Vec<u64> {
    ops.iter().map(|op| op.seq.get()).collect()
}

/// The operations and `has_more` of a `SyncBatch` answered with 200.
fn batch_of(status: StatusCode, reply: ServerMessage) -> (Vec<u64>, bool) {
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    let ServerMessage::SyncBatch { ops, has_more, .. } = reply else {
        panic!("expected SyncBatch, got {reply:?}");
    };
    (seqs_of(&ops), has_more)
}

/// The catch-up and `has_more` of a `CommitAck` answered with 200.
fn catchup_of(status: StatusCode, reply: ServerMessage) -> (Vec<u64>, bool) {
    assert_eq!(status, StatusCode::OK, "{reply:?}");
    let ServerMessage::CommitAck {
        catchup_ops,
        has_more,
        ..
    } = reply
    else {
        panic!("expected CommitAck, got {reply:?}");
    };
    (seqs_of(&catchup_ops), has_more)
}

/// With little room left in the client's part, a sync returns the operations that fit with
/// `has_more` instead of `Unavailable`, and the next one goes on from there; a commit's
/// catch-up carries what fits, and a client up to date gets its own operation only if it fits.
#[tokio::test]
async fn a_sync_with_little_room_returns_a_shorter_batch_and_the_next_goes_on() {
    let fx = Fixture::new().await;
    // Ten operations of about 1 KiB, all of the same size.
    let commits: Vec<ClientMessage> = (1..=10u8)
        .map(|n| fx.commit_op(n, u64::from(n) - 1, task_with_title(i64::from(n), 1000)))
        .collect();
    for commit in &commits {
        fx.commit_ok(commit).await;
    }
    let one = in_response(1, &commits[0]);
    // Room for a response with three of them, and most of a fourth.
    let held = fx.hold_all_but(RESPONSE_OVERHEAD + 4 * one - 1);

    let (status, _, reply) = fx.post("sync", &fx.sync(0)).await;
    assert_eq!(batch_of(status, reply), (vec![1, 2, 3], true));
    let (status, _, reply) = fx.post("sync", &fx.sync(3)).await;
    assert_eq!(batch_of(status, reply), (vec![4, 5, 6], true));

    let behind = fx.commit_op(11, 6, task_with_title(11, 1000));
    let (status, _, reply) = fx.post("commit", &behind).await;
    assert_eq!(catchup_of(status, reply), (vec![7, 8, 9], true));

    let up_to_date = fx.commit_op(12, 11, task_with_title(12, 1000));
    let (status, _, reply) = fx.post("commit", &up_to_date).await;
    assert_eq!(catchup_of(status, reply), (vec![12], false));
    drop(held);

    // Without room for its own operation, it gets none.
    let held = fx.hold_all_but(RESPONSE_OVERHEAD + one - 1);
    let up_to_date = fx.commit_op(13, 12, task_with_title(13, 1000));
    let (status, _, reply) = fx.post("commit", &up_to_date).await;
    assert_eq!(catchup_of(status, reply), (vec![], true));
    drop(held);
}

/// A sync is answered `Unavailable` only when not even its first operation fits, and that
/// leaves no trace: its cursor stays where the client's commits left it, and the same sync,
/// with room for one operation, is served and only then moves it.
#[tokio::test]
async fn a_sync_without_room_for_one_operation_is_unavailable_and_changes_nothing() {
    let fx = Fixture::new().await;
    let commits: Vec<ClientMessage> = (1..=3u8).map(|n| fx.commit(n, 0)).collect();
    for commit in &commits {
        fx.commit_ok(commit).await;
    }
    let cursor = || async {
        fx.manager
            .ask(&fx.room_id, |reply| RoomCommand::GetClientCursor {
                client_id: fx.client_id.clone(),
                reply,
            })
            .await
            .unwrap()
            .unwrap()
    };
    assert_eq!(cursor().await, Some(SequenceNumber::new(0)));

    let second = in_response(2, &commits[1]);
    let held = fx.hold_all_but(RESPONSE_OVERHEAD + second - 1);
    let (status, headers, reply) = fx.post("sync", &fx.sync(1)).await;
    assert_error(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Unavailable,
        &reply,
    );
    assert!(headers.contains_key(header::RETRY_AFTER));
    assert_eq!(
        cursor().await,
        Some(SequenceNumber::new(0)),
        "a sync answered Unavailable moved the cursor"
    );
    drop(held);

    let held = fx.hold_all_but(RESPONSE_OVERHEAD + second);
    let (status, _, reply) = fx.post("sync", &fx.sync(1)).await;
    assert_eq!(batch_of(status, reply), (vec![2], true));
    assert_eq!(cursor().await, Some(SequenceNumber::new(1)));
    drop(held);
}

/// A second room with a registered client and `ops` small operations committed by it.
async fn other_room(fx: &Fixture, ops: u8) -> (RoomId, ClientId) {
    let room_id = RoomId::new("other").unwrap();
    fx.manager
        .create_room(room_id.clone(), SchemaId::new("tasks").unwrap(), None)
        .await
        .unwrap();
    let client_id = ClientId::new("reader").unwrap();
    fx.manager
        .ask(&room_id, |reply| RoomCommand::RegisterClient {
            client_id: client_id.clone(),
            current_seq: None,
            known_incarnation: None,
            reply,
        })
        .await
        .unwrap()
        .unwrap();
    for n in 1..=ops {
        let row = RowBuilder::new().set("id", i64::from(n)).build();
        fx.manager
            .ask(&room_id, |reply| RoomCommand::Commit {
                client_id: client_id.clone(),
                mutation_id: MutationId::new([n; 16]),
                last_ack_seq: SequenceNumber::new(0),
                op: schema().to_operation_insert("tasks", &row, 1000).unwrap(),
                reply,
            })
            .await
            .unwrap()
            .unwrap();
    }
    (room_id, client_id)
}

/// A sync of `client_id` in `room_id` from 0, straight to the room: the response keeps its
/// reservation, as between the room's reply and its encoding.
async fn sync_held(
    fx: &Fixture,
    room_id: &RoomId,
    client_id: &ClientId,
) -> Result<crate::actor::command::SyncBatchResponse, ServerError> {
    fx.manager
        .ask(room_id, |reply| RoomCommand::Sync {
            client_id: client_id.clone(),
            from_seq: SequenceNumber::new(0),
            max_batch_size: 100,
            reply,
        })
        .await
        .unwrap()
}

/// A client that does not read its responses fills only its own part, counted by what they
/// hold: its syncs get shorter batches as the part fills, and then `Unavailable`, while a
/// client of another room keeps getting whole batches.
#[tokio::test]
async fn a_client_that_does_not_read_its_responses_only_fills_its_own_part() {
    let fx = Fixture::new().await;
    // Ten operations of 1 MiB: a whole batch holds about 10 MiB once encoded, and about twice
    // that while it is read.
    for n in 1..=10u8 {
        let commit = fx.commit_op(n, u64::from(n) - 1, task_with_title(i64::from(n), 1 << 20));
        fx.commit_ok(&commit).await;
    }
    let (other, reader) = other_room(&fx, 5).await;

    let mut held = Vec::new();
    let refused = loop {
        let response = fx.send("sync", &fx.sync(0)).await;
        if response.status() != StatusCode::OK {
            break response;
        }
        held.push(response);
        assert!(held.len() < 32, "the client's part must run out");
        // Another room is served whole meanwhile.
        let batch = sync_held(&fx, &other, &reader).await.unwrap();
        assert_eq!((batch.ops.len(), batch.has_more), (5, false));
    };
    // The rest of the budget is left to the other clients.
    let factory = crate::config::ServerConfig::default();
    let others =
        factory.max_inflight_response_bytes - factory.max_inflight_response_bytes_per_client;
    assert!(fx.manager.response_budget().reserved().1 as u64 >= others);
    let body = axum::body::to_bytes(refused.into_body(), usize::MAX)
        .await
        .unwrap();
    let reply = decode_message(&body).unwrap();
    assert!(
        matches!(
            reply,
            ServerMessage::Error {
                code: ErrorCode::Unavailable,
                ..
            }
        ),
        "{reply:?}"
    );
    let batch = sync_held(&fx, &other, &reader).await.unwrap();
    assert_eq!((batch.ops.len(), batch.has_more), (5, false));

    // Before running out, the part gave shorter batches instead of refusing them.
    let mut lengths = Vec::new();
    for response in held {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let (ops, has_more) = batch_of(StatusCode::OK, decode_message(&body).unwrap());
        assert_eq!(has_more, ops.len() < 10);
        lengths.push(ops.len());
    }
    assert!(lengths.iter().any(|&len| len < 10), "{lengths:?}");
}

/// Many small responses being built at once fit in the factory budget: more than the sixteen
/// frames it holds, from many clients and from one.
#[tokio::test]
async fn many_small_responses_in_flight_fit_in_the_factory_budget() {
    let fx = Fixture::new().await;
    fx.commit_ok(&fx.commit(1, 0)).await;
    let clients: Vec<ClientId> = (0..40)
        .map(|n| ClientId::new(format!("client-{n}")).unwrap())
        .collect();
    for client_id in &clients {
        fx.manager
            .ask(&fx.room_id, |reply| RoomCommand::RegisterClient {
                client_id: client_id.clone(),
                current_seq: None,
                known_incarnation: None,
                reply,
            })
            .await
            .unwrap()
            .unwrap();
    }

    let mut syncs = Vec::new();
    for client_id in clients.iter().chain(std::iter::repeat_n(&fx.client_id, 40)) {
        let batch = sync_held(&fx, &fx.room_id, client_id).await.unwrap();
        assert_eq!(batch.ops.len(), 1);
        syncs.push(batch);
    }
    let mut commits = Vec::new();
    for (n, client_id) in clients.iter().enumerate() {
        let row = RowBuilder::new().set("id", 100 + n as i64).build();
        let ack = fx
            .manager
            .ask(&fx.room_id, |reply| RoomCommand::Commit {
                client_id: client_id.clone(),
                mutation_id: MutationId::from_u128(1000 + n as u128),
                last_ack_seq: SequenceNumber::new(0),
                op: schema().to_operation_insert("tasks", &row, 1000).unwrap(),
                reply,
            })
            .await
            .unwrap()
            .unwrap();
        assert!(ack.reservation.is_some() && !ack.catchup_ops.is_empty());
        commits.push(ack);
    }
    drop((syncs, commits));
    assert_eq!(fx.manager.response_budget().reserved().0, 0);
}

/// Drops the request of `msg` to `endpoint` after `polls` polls, letting the room run between
/// them, and returns whether it had completed (its response is dropped too).
async fn send_cancelled_after(
    fx: &Fixture,
    endpoint: &str,
    msg: &ClientMessage,
    polls: usize,
) -> bool {
    let mut request = std::pin::pin!(fx.send(endpoint, msg));
    for _ in 0..polls {
        if futures::poll!(request.as_mut()).is_ready() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

/// Every response returns what it reserved once it ends: read, dropped unread, or cancelled
/// at any of its suspension points, before or after the room answered.
#[tokio::test]
async fn the_budget_returns_to_zero_once_every_response_ends_or_is_cancelled() {
    let fx = Fixture::new().await;
    for n in 1..=3u8 {
        fx.commit_ok(&fx.commit(n, 0)).await;
    }
    let budget = fx.manager.response_budget();
    let idle = budget.reserved();
    assert_eq!(idle.0, 0);
    let get_schema = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(2),
        room_id: fx.room_id.clone(),
    };
    for (endpoint, msg) in [
        ("sync", fx.sync(0)),
        ("commit", fx.commit(4, 0)),
        ("register", fx.register_msg()),
        ("schema", get_schema),
    ] {
        for polls in 0.. {
            let completed = send_cancelled_after(&fx, endpoint, &msg, polls).await;
            // The room handles its mailbox in order: once it answers this, it has answered
            // the request, whose reply was dropped if the request was.
            fx.manager
                .ask(&fx.room_id, |reply| RoomCommand::GetClientCursor {
                    client_id: fx.client_id.clone(),
                    reply,
                })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(budget.reserved(), idle, "{endpoint} after {polls} polls");
            if completed {
                break;
            }
            assert!(polls < 100, "{endpoint} never completed");
        }
    }
}

/// A registration and a schema request reserve their known size, not a frame: they are served
/// while the client's part has room for that, and only then.
#[tokio::test]
async fn registration_and_schema_reserve_their_known_size() {
    let fx = Fixture::new().await;
    let get_schema = ClientMessage::GetSchema {
        correlation_id: CorrelationId::new(2),
        room_id: fx.room_id.clone(),
    };
    let size = schema_response_size(&schema());
    let held = fx.hold_all_but(size);
    for (endpoint, msg) in [("schema", &get_schema), ("register", &fx.register_msg())] {
        let (status, _, reply) = fx.post(endpoint, msg).await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {reply:?}");
    }
    drop(held);
    let held = fx.hold_all_but(size - 1);
    for (endpoint, msg) in [("schema", &get_schema), ("register", &fx.register_msg())] {
        let (status, _, reply) = fx.post(endpoint, msg).await;
        let expected = StatusCode::SERVICE_UNAVAILABLE;
        assert_error(status, expected, ErrorCode::Unavailable, &reply);
    }
    drop(held);
}

/// An operation that takes more than a frame in a response (10 MiB encoded, and as much again
/// once read) is served with the smallest part and budget accepted, a frame each: a reservation
/// never asks for more than a frame.
#[tokio::test]
async fn an_operation_larger_than_a_frame_in_a_response_is_served_with_the_smallest_budget() {
    let fx = Fixture::with(|config| {
        config.max_inflight_response_bytes = MAX_FRAME_SIZE as u64;
        config.max_inflight_response_bytes_per_client = MAX_FRAME_SIZE as u64;
    })
    .await;
    let commit = fx.commit_op(1, 0, task_with_title(1, 10 << 20));
    let (status, _, reply) = fx.post("commit", &commit).await;
    assert_eq!(catchup_of(status, reply), (vec![1], false));
    let (status, _, reply) = fx.post("sync", &fx.sync(0)).await;
    assert_eq!(batch_of(status, reply), (vec![1], false));
}

/// A `Registered` or `Schema` response never takes more once encoded than it reserved before
/// its schema was copied, with every other field at its largest.
#[test]
fn schema_responses_reserve_at_least_what_they_take_encoded() {
    let longest = "r".repeat(zemdb_core::id::MAX_PATH_ID_LEN);
    let schema = schema();
    let registered = ServerMessage::Registered {
        correlation_id: CorrelationId::new(u64::MAX),
        room_id: RoomId::new(longest.clone()).unwrap(),
        head_seq: SequenceNumber::new(u64::MAX),
        tail_seq: SequenceNumber::new(u64::MAX),
        schema_id: SchemaId::new(longest.clone()).unwrap(),
        schema: schema.clone(),
        active_snapshot_seq: Some(SequenceNumber::new(u64::MAX)),
        room_incarnation: u128::MAX,
    };
    let schema_msg = ServerMessage::Schema {
        correlation_id: CorrelationId::new(u64::MAX),
        room_id: RoomId::new(longest.clone()).unwrap(),
        schema_id: SchemaId::new(longest).unwrap(),
        schema: schema.clone(),
    };
    for msg in [registered, schema_msg] {
        let encoded = encode_message(&msg).unwrap().len() as u64;
        assert!(encoded <= schema_response_size(&schema), "{encoded}");
    }
    // And never more than a frame, the most any response takes.
    let frame = MAX_FRAME_SIZE as u64;
    assert_eq!(known_response_size(frame - RESPONSE_OVERHEAD), frame);
    assert_eq!(known_response_size(frame), frame);
}

#[test]
fn response_that_cannot_be_encoded_is_an_internal_error_frame() {
    let oversized = ServerMessage::SnapshotChunk {
        correlation_id: CorrelationId::new(1),
        room_id: RoomId::new("room").unwrap(),
        snapshot_head_seq: SequenceNumber::new(1),
        chunk_index: 0,
        total_chunks: 1,
        total_bytes: 1,
        snapshot_hash: [0; 32],
        data: vec![0u8; MAX_MESSAGE_SIZE as usize].into(),
    };
    let response = binary_response(StatusCode::OK, &oversized);
    let status = response.status();
    let body =
        futures::executor::block_on(axum::body::to_bytes(response.into_body(), 1 << 20)).unwrap();
    assert_error(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCode::Internal,
        &decode_message(&body).unwrap(),
    );
}

/// Posts the first chunk of a download, unanchored, with `meanwhile` run to completion right
/// after the relay picks the snapshot to serve, before it asks for the log range.
async fn first_chunk_while(
    fx: &std::sync::Arc<Fixture>,
    meanwhile: impl std::future::Future<Output = ()> + Send + 'static,
) -> ServerMessage {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let runtime = tokio::runtime::Handle::current();
    let snapshots = fx.dir.path().join("snapshots");
    fail_point::arm_hook("first_chunk_picked", &snapshots, move || {
        runtime.spawn(async move {
            meanwhile.await;
            let _ = done_tx.send(());
        });
        let done = tokio::task::block_in_place(|| done_rx.recv_timeout(Duration::from_secs(10)));
        assert!(done.is_ok());
    });
    fx.post("snapshot/chunk", &fx.first_chunk(None)).await.2
}

/// The first chunk checks its incarnation against the room it asks for the log range after
/// picking the snapshot: if the room was deleted and created again in between, it is
/// `RoomReset`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_chunk_of_a_room_created_again_after_its_snapshot_was_picked_is_room_reset() {
    let fx = std::sync::Arc::new(Fixture::new().await);
    fx.commit_ok(&fx.commit(1, 0)).await;
    fx.stage_snapshot(1, fx.manager.log_bounds(&fx.room_id))
        .await;
    let other = std::sync::Arc::clone(&fx);
    let reply = first_chunk_while(&fx, async move {
        let (manager, room_id) = (&other.manager, other.room_id.clone());
        manager.delete_room(&room_id).await.unwrap();
        let schema_id = SchemaId::new("tasks").unwrap();
        manager.create_room(room_id, schema_id, None).await.unwrap();
    })
    .await;
    assert!(
        matches!(
            reply,
            ServerMessage::Error {
                code: ErrorCode::RoomReset,
                ..
            }
        ),
        "{reply:?}"
    );
}

/// The first chunk checks the snapshot it serves against the log range read after picking it:
/// one below a gap that a sync found in between is dropped instead of served.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_chunk_does_not_serve_a_snapshot_below_a_gap_found_after_it_was_picked() {
    let fx = std::sync::Arc::new(Fixture::with(|config| config.ram_max_ops = 2).await);
    for n in 1..=6u8 {
        fx.commit_ok(&fx.commit(n, u64::from(n) - 1)).await;
    }
    fx.stage_snapshot(2, fx.manager.log_bounds(&fx.room_id))
        .await;
    let other = std::sync::Arc::clone(&fx);
    let reply = first_chunk_while(&fx, async move {
        let room = crate::actor::room::room_dir(other.dir.path(), &other.room_id);
        std::fs::remove_file(room.join("segments/segment_0000000000000003_0000000000000004.wal"))
            .unwrap();
        let (_, _, reply) = other.post("sync", &other.sync(2)).await;
        assert!(
            matches!(
                reply,
                ServerMessage::Error {
                    code: ErrorCode::BehindCompaction,
                    ..
                }
            ),
            "{reply:?}"
        );
    })
    .await;
    assert!(
        matches!(
            reply,
            ServerMessage::Error {
                code: ErrorCode::RoomNotFound,
                ..
            }
        ),
        "{reply:?}"
    );
    assert_eq!(fx.relay.active_snapshot_seq(&fx.room_id), None);
}
