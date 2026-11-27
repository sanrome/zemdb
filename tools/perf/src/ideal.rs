//! The `ideal` mode: the least code that takes a commit, with no disk, no network and no
//! runtime. One thread decodes the frame of a commit, checks it, keeps it in memory and encodes
//! its acknowledgement, with the functions of `zemdb-core` the server uses. It is the bound of
//! what one core can commit, to compare the server against.
//!
//! ```text
//! zemdb-perf ideal [--n COMMITS] [--echo 0|1]
//! ```
//!
//! `--echo 1` acknowledges each commit with its operation as the catch-up, as the server does
//! for a client that is up to date. Prints one JSON line: the mean of each stage, in
//! nanoseconds, measured with a clock read around it, and the commits per second of a pass
//! without those clock reads.

use std::collections::{HashMap, VecDeque};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;
use zemdb_core::*;

/// The operations kept in memory, as `ram_max_ops` of the server.
const WINDOW: usize = 1_000;
/// The mutations remembered for deduplication.
const DEDUP: usize = 10_000;
/// Frames encoded ahead of each timed batch, as a client would have sent them.
const BATCH: usize = 50_000;

struct Room {
    schema: Schema,
    client_id: ClientId,
    room_id: RoomId,
    head: u64,
    ops: VecDeque<Arc<SequencedOperation>>,
    dedup: HashMap<MutationId, SequenceNumber>,
    dedup_order: VecDeque<MutationId>,
    echo: bool,
}

struct Commit {
    correlation_id: CorrelationId,
    mutation_id: MutationId,
    client_id: ClientId,
    last_ack_seq: SequenceNumber,
    op: Operation,
}

impl Room {
    fn receive(&self, frame: &[u8]) -> Commit {
        match decode_client_message(frame).unwrap() {
            ClientMessage::Commit {
                correlation_id,
                mutation_id,
                client_id,
                last_ack_seq,
                op,
                ..
            } => Commit {
                correlation_id,
                mutation_id,
                client_id,
                last_ack_seq,
                op,
            },
            _ => unreachable!(),
        }
    }

    fn check(&self, commit: Commit) -> (Commit, Arc<SequencedOperation>) {
        assert!(commit.client_id == self.client_id);
        assert!(commit.last_ack_seq.get() <= self.head);
        assert!(!self.dedup.contains_key(&commit.mutation_id));
        self.schema.validate_operation(&commit.op).unwrap();
        self.schema
            .check_owner(&commit.op, &commit.client_id)
            .unwrap();
        let op = commit.op.clone();
        let seq_op = Arc::new(SequencedOperation::new(
            SequenceNumber::new(self.head + 1),
            op,
        ));
        check_operation_size(&seq_op, Some(commit.mutation_id)).unwrap();
        (commit, seq_op)
    }

    fn write_in_memory(&mut self, mutation_id: MutationId, seq_op: &Arc<SequencedOperation>) {
        self.head = seq_op.seq.get();
        self.ops.push_back(Arc::clone(seq_op));
        if self.ops.len() > WINDOW {
            self.ops.pop_front();
        }
        self.dedup.insert(mutation_id, seq_op.seq);
        self.dedup_order.push_back(mutation_id);
        if self.dedup_order.len() > DEDUP {
            let oldest = self.dedup_order.pop_front().unwrap();
            self.dedup.remove(&oldest);
        }
    }

    fn confirm(&self, commit: &Commit, seq_op: &Arc<SequencedOperation>) -> Vec<u8> {
        let catchup_ops = if self.echo {
            vec![(**seq_op).clone()]
        } else {
            Vec::new()
        };
        encode_message(&ServerMessage::CommitAck {
            correlation_id: commit.correlation_id,
            room_id: self.room_id.clone(),
            mutation_id: commit.mutation_id,
            assigned_seq: seq_op.seq,
            catchup_ops,
            has_more: false,
            snapshot_wanted: false,
            schema_version: self.schema.version(),
            schema_id: SchemaId::new(crate::SCHEMA_ID).unwrap(),
        })
        .unwrap()
    }
}

fn frames(room: &Room, first_id: u64, head: u64) -> Vec<Vec<u8>> {
    (0..BATCH as u64)
        .map(|i| {
            let id = first_id + i;
            encode_message(&ClientMessage::Commit {
                correlation_id: CorrelationId::new(id),
                room_id: room.room_id.clone(),
                client_id: room.client_id.clone(),
                mutation_id: MutationId::from_u128((1 << 64) | u128::from(id)),
                // The cursor of a client that is up to date when it sends.
                last_ack_seq: SequenceNumber::new(head),
                op: crate::insert(&room.schema, id as i64),
            })
            .unwrap()
        })
        .collect()
}

pub fn ideal_mode(args: &HashMap<String, String>) {
    let n = args.get("n").map_or(2_000_000, |v| v.parse().unwrap());
    let echo = args.get("echo").is_some_and(|v| v == "1");
    let mut room = Room {
        schema: crate::schema(),
        client_id: ClientId::new(crate::CLIENT_ID).unwrap(),
        room_id: RoomId::new("ideal").unwrap(),
        head: 0,
        ops: VecDeque::new(),
        dedup: HashMap::new(),
        dedup_order: VecDeque::new(),
        echo,
    };

    // Pass 1: each stage between two clock reads. Pass 2: the whole commit, with none.
    let mut stage_ns = [0u64; 4];
    let mut whole_ns = 0u64;
    let (mut request_bytes, mut response_bytes) = (0, 0);
    let mut next_id = 0u64;
    for pass in 0..2 {
        let mut done = 0;
        while done < n {
            let batch = frames(&room, next_id, room.head);
            next_id += BATCH as u64;
            let start = Instant::now();
            let mut at = start;
            for frame in &batch {
                let commit = room.receive(black_box(frame));
                if pass == 0 {
                    let now = Instant::now();
                    stage_ns[0] += (now - at).as_nanos() as u64;
                    at = now;
                }
                let (commit, seq_op) = room.check(commit);
                if pass == 0 {
                    let now = Instant::now();
                    stage_ns[1] += (now - at).as_nanos() as u64;
                    at = now;
                }
                room.write_in_memory(commit.mutation_id, &seq_op);
                if pass == 0 {
                    let now = Instant::now();
                    stage_ns[2] += (now - at).as_nanos() as u64;
                    at = now;
                }
                let ack = room.confirm(&commit, &seq_op);
                request_bytes = frame.len();
                response_bytes = ack.len();
                black_box(ack);
                if pass == 0 {
                    let now = Instant::now();
                    stage_ns[3] += (now - at).as_nanos() as u64;
                    at = now;
                }
            }
            if pass == 1 {
                whole_ns += start.elapsed().as_nanos() as u64;
            }
            done += BATCH;
        }
    }
    let commits = (n.div_ceil(BATCH) * BATCH) as f64;
    let mean = |ns: u64| ns as f64 / commits;
    println!(
        "{}",
        serde_json::json!({
            "mode": "ideal",
            "echo": echo,
            "commits": commits,
            "request_bytes": request_bytes,
            "response_bytes": response_bytes,
            "receive_ns": mean(stage_ns[0]),
            "check_ns": mean(stage_ns[1]),
            "write_in_memory_ns": mean(stage_ns[2]),
            "confirm_ns": mean(stage_ns[3]),
            "staged_total_ns": mean(stage_ns.iter().sum()),
            "whole_ns": mean(whole_ns),
            "commits_per_sec": commits / (whole_ns as f64 / 1e9),
        })
    );
}
