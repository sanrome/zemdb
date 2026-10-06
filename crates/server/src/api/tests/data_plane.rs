use super::*;
use crate::api::auth::generate_client_token;
use crate::api::router::build_router;
use crate::config::ServerConfig;
use crate::fail_point;
use crate::relay::SnapshotRelay;
use crate::schema_registry::SchemaRegistry;
use crate::RoomManager;
use axum::body::Body;
use axum::http::Request;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;
use zemdb_core::id::{ClientId, MutationId, SchemaId, SequenceNumber};
use zemdb_core::mutation::{Operation, OperationKind};
use zemdb_core::protocol::codec::{decode_message, MAX_FRAME_SIZE, MAX_MESSAGE_SIZE};
use zemdb_core::protocol::messages::{ErrorCode, SequencedOperation};
use zemdb_core::protocol::wal_frame::encode_wal_record;
use zemdb_core::schema::{Schema, TableSchema};
use zemdb_core::value::{CompactRow, DataType, PrimaryKey, RowBuilder, Value};

const AUTH_SECRET: &str = "data_plane_cluster_secret_key_1234";

struct Fixture {
    dir: TempDir,
    app: axum::Router,
    room_id: RoomId,
    client_id: ClientId,
    token: String,
}

/// Int columns of the `wide` table, besides its key and its string column.
const WIDE_INT_COLUMNS: usize = 5000;

fn schema() -> Schema {
    let tasks = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .nullable_column("title", DataType::String)
        .build()
        .unwrap();
    let mut wide = TableSchema::builder("wide")
        .primary_key("id", DataType::Int)
        .nullable_column("text", DataType::String);
    for i in 0..WIDE_INT_COLUMNS {
        wide = wide.nullable_column(format!("c{i}"), DataType::Int);
    }
    Schema::from_tables(vec![tasks, wide.build().unwrap()])
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
        let dir = tempdir().unwrap();
        let config = Arc::new(ServerConfig {
            data_dir: dir.path().to_path_buf(),
            auth_secret: AUTH_SECRET.to_string(),
            ..ServerConfig::default()
        });
        let registry = Arc::new(SchemaRegistry::new(dir.path().join("schemas")).unwrap());
        let schema_id = SchemaId::new("tasks").unwrap();
        registry
            .register_schema(schema_id.clone(), schema())
            .unwrap();
        let relay = Arc::new(
            SnapshotRelay::new(
                dir.path().join("snapshots"),
                Duration::from_secs(60),
                config.max_snapshot_bytes,
            )
            .unwrap(),
        );
        let manager = Arc::new(RoomManager::new(
            Arc::clone(&config),
            Arc::clone(&registry),
            Arc::clone(&relay),
        ));
        let room_id = RoomId::new("room").unwrap();
        manager
            .create_room(room_id.clone(), schema_id, None)
            .await
            .unwrap();
        let client_id = ClientId::new("writer").unwrap();
        let token =
            generate_client_token(&client_id, &room_id, Duration::from_secs(300), AUTH_SECRET);
        let fx = Self {
            app: build_router(AppState::new(config, registry, manager, relay)),
            dir,
            room_id,
            client_id,
            token,
        };

        let register = ClientMessage::RegisterClient {
            correlation_id: CorrelationId::new(1),
            room_id: fx.room_id.clone(),
            client_id: fx.client_id.clone(),
            auth_token: fx.token.clone(),
            current_seq: None,
        };
        let (status, _, _) = fx.post("register", &register).await;
        assert_eq!(status, StatusCode::OK);
        fx
    }

    async fn post(
        &self,
        endpoint: &str,
        msg: &ClientMessage,
    ) -> (StatusCode, axum::http::HeaderMap, ServerMessage) {
        let request = Request::post(format!("/rooms/{}/{endpoint}", self.room_id))
            .header(header::AUTHORIZATION, format!("Bearer {}", self.token))
            .body(Body::from(encode_message(msg).unwrap()))
            .unwrap();
        let response = self.app.clone().oneshot(request).await.unwrap();
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

    /// Posts a commit, expecting it to be acknowledged.
    async fn commit_ok(&self, msg: &ClientMessage) {
        let (status, _, reply) = self.post("commit", msg).await;
        assert_eq!(status, StatusCode::OK, "{reply:?}");
    }
}

/// Asserts a `BadRequest` error frame with HTTP 400.
fn assert_bad_request(status: StatusCode, reply: &ServerMessage) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply:?}");
    assert!(
        matches!(
            reply,
            ServerMessage::Error {
                code: ErrorCode::BadRequest,
                ..
            }
        ),
        "{reply:?}"
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
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{reply:?}");
    assert_eq!(headers[header::RETRY_AFTER], "1");
    assert!(matches!(
        reply,
        ServerMessage::Error {
            code: ErrorCode::Unavailable,
            ..
        }
    ));

    // Retrying is safe and succeeds once the room is back.
    let (status, _, reply) = fx.post("commit", &fx.commit(2, 1)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(matches!(
        reply,
        ServerMessage::CommitAck { assigned_seq, .. } if assigned_seq == SequenceNumber::new(2)
    ));
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
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body =
        futures::executor::block_on(axum::body::to_bytes(response.into_body(), 1 << 20)).unwrap();
    assert!(matches!(
        decode_message::<ServerMessage>(&body).unwrap(),
        ServerMessage::Error {
            code: ErrorCode::Internal,
            ..
        }
    ));
}
