use super::*;
use crate::protocol::codec::{decode_message, encode_message, peek_client_message_kind};
use crate::protocol::DecodeError;
use crate::value::PrimaryKey;

/// A message of `kind`. The match has no wildcard, so a new kind does not compile until it
/// has a sample here.
fn sample(kind: ClientMessageKind) -> ClientMessage {
    let correlation_id = CorrelationId::new(9);
    let room_id = RoomId::new("room-kinds").unwrap();
    let client_id = ClientId::new("client-kinds").unwrap();
    match kind {
        ClientMessageKind::Commit => ClientMessage::Commit {
            correlation_id,
            room_id,
            client_id,
            mutation_id: MutationId::new([3u8; 16]),
            last_ack_seq: SequenceNumber::new(0),
            op: Operation::delete(0, PrimaryKey::single(1i64), 0),
        },
        ClientMessageKind::Ack => ClientMessage::Ack {
            correlation_id,
            room_id,
            client_id,
            ack_seq: SequenceNumber::new(1),
        },
        ClientMessageKind::Sync => ClientMessage::Sync {
            correlation_id,
            room_id,
            client_id,
            from_seq: SequenceNumber::new(1),
            max_batch_size: 10,
        },
        ClientMessageKind::Heartbeat => ClientMessage::Heartbeat {
            correlation_id,
            room_id,
            client_id,
        },
        ClientMessageKind::RegisterClient => ClientMessage::RegisterClient {
            correlation_id,
            room_id,
            client_id,
            auth_token: "token".to_string(),
            current_seq: None,
        },
        ClientMessageKind::GetSchema => ClientMessage::GetSchema {
            correlation_id,
            room_id,
        },
        ClientMessageKind::DeregisterClient => ClientMessage::DeregisterClient {
            correlation_id,
            room_id,
            client_id,
        },
        ClientMessageKind::RequestSnapshotChunk => ClientMessage::RequestSnapshotChunk {
            correlation_id,
            room_id,
            chunk_index: 0,
            chunk_size: 1024,
            snapshot_hash: None,
        },
        ClientMessageKind::UploadSnapshotChunk => ClientMessage::UploadSnapshotChunk {
            correlation_id,
            room_id,
            snapshot_head_seq: SequenceNumber::new(1),
            chunk_index: 0,
            total_chunks: 1,
            total_bytes: 3,
            snapshot_hash: [0u8; 32],
            data: bytes::Bytes::from_static(b"abc"),
        },
    }
}

#[test]
fn every_kind_is_listed_in_order_and_read_from_its_frame() {
    for (index, &kind) in ClientMessageKind::ALL.iter().enumerate() {
        assert_eq!(kind as usize, index, "{kind:?} out of place");
        let msg = sample(kind);
        assert_eq!(msg.kind(), kind);
        let frame = encode_message(&msg).unwrap();
        assert_eq!(peek_client_message_kind(&frame).unwrap(), kind);
    }
}

#[test]
fn client_messages_have_as_many_variants_as_listed_kinds() {
    // The variant index right after the last listed kind is not a `ClientMessage`: a variant
    // added to it without its kind in `ClientMessageKind::ALL` fails here.
    let count = ClientMessageKind::ALL.len();
    let mut frame = encode_message(&sample(ClientMessageKind::Commit)).unwrap();
    frame.truncate(crate::protocol::PROTOCOL_HEADER_LEN);
    frame.push(count as u8);
    let err = decode_message::<ClientMessage>(&frame).unwrap_err();
    assert!(
        matches!(&err, DecodeError::Malformed(e)
            if e.to_string().contains(&format!("variant index 0 <= i < {count}"))),
        "{err:?}"
    );
    assert!(ClientMessageKind::from_variant_index(count as u32).is_none());
}
