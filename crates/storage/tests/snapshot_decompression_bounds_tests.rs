use zemdb_core::*;
use zemdb_storage::snapshot::{
    decode_snapshot_envelope, decode_snapshot_envelope_with_limit, encode_snapshot_envelope,
    SnapshotCompression, SnapshotEnvelopeHeader, DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
    SNAPSHOT_HEADER_LEN,
};
use zemdb_storage::{
    DiskStorageEngine, DiskStorageOptions, MemoryStorageEngine, StorageEngine, StorageError,
};

/// Builds an envelope whose header declares `declared_len` regardless of the body.
fn envelope_with(compression: SnapshotCompression, declared_len: u32, body: &[u8]) -> Vec<u8> {
    let header = SnapshotEnvelopeHeader::for_body(compression, declared_len, body);
    let mut envelope = header.to_bytes().to_vec();
    envelope.extend_from_slice(body);
    envelope
}

/// Builds a Zstandard envelope whose header declares `declared_len` regardless of the body.
fn zstd_envelope(declared_len: u32, body: &[u8]) -> Vec<u8> {
    envelope_with(SnapshotCompression::Zstd, declared_len, body)
}

fn corruption_message<T: std::fmt::Debug>(result: Result<T, StorageError>) -> String {
    match result {
        Err(StorageError::SnapshotCorruption(msg)) => msg,
        other => panic!("expected a snapshot corruption error, got {other:?}"),
    }
}

#[test]
fn declared_size_above_the_limit_is_rejected_before_decompressing() {
    // The body is not even a Zstandard frame: the size check must come first.
    let envelope = zstd_envelope(1_000_001, b"not a zstd frame");
    let msg = corruption_message(decode_snapshot_envelope_with_limit(&envelope, 1_000_000));
    assert!(msg.contains("exceeds the maximum"), "{msg}");
}

#[test]
fn default_limit_rejects_a_declared_size_above_two_gib() {
    assert_eq!(
        DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
        2 * 1024 * 1024 * 1024
    );
    let envelope = zstd_envelope(3 * 1024 * 1024 * 1024, b"not a zstd frame");
    let msg = corruption_message(decode_snapshot_envelope(&envelope));
    assert!(msg.contains("exceeds the maximum"), "{msg}");
}

#[test]
fn header_that_understates_the_payload_stops_decompression() {
    // 8 MiB of zeros compress to a few hundred bytes; the header claims 1 KiB.
    let body = zstd::encode_all(&vec![0u8; 8 * 1024 * 1024][..], 3).unwrap();
    let envelope = zstd_envelope(1024, &body);
    let msg = corruption_message(decode_snapshot_envelope(&envelope));
    assert!(msg.contains("larger than declared"), "{msg}");
}

#[test]
fn header_that_overstates_the_payload_is_rejected() {
    let body = zstd::encode_all(&[7u8; 100][..], 3).unwrap();
    let envelope = zstd_envelope(200, &body);
    let msg = corruption_message(decode_snapshot_envelope(&envelope));
    assert!(msg.contains("length mismatch"), "{msg}");
}

#[test]
fn payload_within_the_limit_roundtrips() {
    let payload = vec![3u8; 4096];
    let envelope = encode_snapshot_envelope(&payload, true, 3).unwrap();
    assert_eq!(
        decode_snapshot_envelope_with_limit(&envelope, 4096).unwrap(),
        payload
    );
    assert!(decode_snapshot_envelope_with_limit(&envelope, 4095).is_err());
}

#[tokio::test]
async fn disk_engine_applies_its_configured_snapshot_limit() {
    let table = TableSchema::builder("items")
        .table_id(1)
        .primary_key("id", DataType::Int);
    let schema = Schema::builder().table(table).build();
    let room_id = RoomId::new("bounded-room").unwrap();

    let source = MemoryStorageEngine::new();
    source.open_room(&room_id, schema.clone()).await.unwrap();
    let snapshot = source.create_snapshot(&room_id).await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let engine = DiskStorageEngine::new(
        DiskStorageOptions::new(dir.path()).max_snapshot_uncompressed_bytes(1),
    );
    engine.open_room(&room_id, schema.clone()).await.unwrap();
    let result = engine
        .apply_snapshot(&room_id, schema, snapshot.into(), 0u64.into())
        .await;
    match result {
        Err(StorageError::SnapshotCorruption(msg)) => {
            assert!(msg.contains("exceeds the maximum"), "{msg}")
        }
        other => panic!("expected the snapshot to be rejected, got {other:?}"),
    }
}

fn items_schema() -> Schema {
    let table = TableSchema::builder("items")
        .table_id(1)
        .primary_key("id", DataType::Int);
    Schema::builder().table(table).build()
}

/// The payload of a snapshot of an empty room at sequence 0, as the engines write it.
async fn empty_room_payload() -> Vec<u8> {
    let room_id = RoomId::new("payload-source").unwrap();
    let source = MemoryStorageEngine::new();
    source.open_room(&room_id, items_schema()).await.unwrap();
    let envelope = source.create_snapshot(&room_id).await.unwrap();
    envelope[SNAPSHOT_HEADER_LEN..].to_vec()
}

/// Applies `envelope` to an empty room, announced at sequence 0. A snapshot that passes every
/// check changes nothing and returns sequence 0.
async fn apply_to_empty_room(envelope: Vec<u8>) -> Result<SequenceNumber, StorageError> {
    let room_id = RoomId::new("empty-room").unwrap();
    let engine = MemoryStorageEngine::new();
    engine.open_room(&room_id, items_schema()).await.unwrap();
    engine
        .apply_snapshot(&room_id, items_schema(), envelope.into(), 0u64.into())
        .await
}

#[tokio::test]
async fn compressed_payload_of_exactly_the_declared_length_is_applied() {
    let payload = empty_room_payload().await;
    let body = zstd::encode_all(&payload[..], 3).unwrap();
    let envelope = zstd_envelope(payload.len() as u32, &body);
    assert_eq!(apply_to_empty_room(envelope).await.unwrap().get(), 0);
}

#[tokio::test]
async fn data_after_a_compressed_payload_is_rejected() {
    let payload = empty_room_payload().await;
    let mut stream = payload.clone();
    stream.push(0);
    // The header declares the payload alone, so the extra byte is past the declared length.
    let body = zstd::encode_all(&stream[..], 3).unwrap();
    let envelope = zstd_envelope(payload.len() as u32, &body);
    let msg = corruption_message(apply_to_empty_room(envelope).await);
    assert!(msg.contains("after the end of the payload"), "{msg}");
}

#[tokio::test]
async fn compressed_payload_shorter_than_declared_is_rejected() {
    let payload = empty_room_payload().await;
    let body = zstd::encode_all(&payload[..], 3).unwrap();
    let envelope = zstd_envelope(payload.len() as u32 + 1, &body);
    let msg = corruption_message(apply_to_empty_room(envelope).await);
    assert!(msg.contains("expected"), "{msg}");
}

#[tokio::test]
async fn data_after_a_raw_payload_is_rejected() {
    let mut body = empty_room_payload().await;
    body.push(0);
    let envelope = envelope_with(SnapshotCompression::Raw, body.len() as u32, &body);
    corruption_message(apply_to_empty_room(envelope).await);
}
