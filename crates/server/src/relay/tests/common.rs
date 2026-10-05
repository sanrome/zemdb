//! Helpers shared by the snapshot relay unit tests.

pub(super) use super::files::{snapshot_file_name, UPLOADS_DIR};
pub(super) use super::{LogBounds, SnapshotChunkUpload, SnapshotRelay, Uploader};
pub(super) use crate::error::ServerError;
pub(super) use crate::fail_point;
pub(super) use bytes::Bytes;
pub(super) use std::fs::{self, File};
pub(super) use std::future::{ready, Ready};
pub(super) use std::path::{Path, PathBuf};
pub(super) use std::sync::Arc;
pub(super) use std::time::Duration;
pub(super) use tempfile::{tempdir, TempDir};
pub(super) use zemdb_core::id::{ClientId, CorrelationId, RoomId, SequenceNumber};
pub(super) use zemdb_core::protocol::messages::ServerMessage;
pub(super) use zemdb_core::protocol::snapshot_envelope::{
    SnapshotCompression, SnapshotEnvelopeHeader, SNAPSHOT_HEADER_LEN,
};

pub(super) const TTL: Duration = Duration::from_secs(600);

pub(super) const MAX_BYTES: u64 = 16 * 1024 * 1024;

pub(super) fn room() -> RoomId {
    RoomId::new("relay-room").unwrap()
}

pub(super) fn seq(n: u64) -> SequenceNumber {
    SequenceNumber::new(n)
}

pub(super) fn open(dir: &TempDir) -> SnapshotRelay {
    SnapshotRelay::new(dir.path().join("snapshots"), TTL, MAX_BYTES).unwrap()
}

pub(super) fn bounds(tail: u64, head: u64) -> Ready<Result<LogBounds, ServerError>> {
    ready(Ok(LogBounds {
        tail_seq: seq(tail),
        head_seq: seq(head),
    }))
}

/// A valid raw snapshot envelope of exactly `total_len` bytes.
pub(super) fn envelope(total_len: usize, fill: u8) -> Vec<u8> {
    let body = vec![fill; total_len - SNAPSHOT_HEADER_LEN];
    let header =
        SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, body.len() as u32, &body);
    let mut out = header.to_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

pub(super) async fn stage(
    relay: &SnapshotRelay,
    n: u64,
    data: &[u8],
) -> Result<[u8; 32], ServerError> {
    relay
        .stage_snapshot(
            &room(),
            seq(n),
            Bytes::copy_from_slice(data),
            bounds(0, 100),
        )
        .await
}

pub(super) async fn chunk(
    relay: &SnapshotRelay,
    index: u32,
    size: u32,
    anchor: Option<[u8; 32]>,
) -> Result<(u32, [u8; 32], Bytes), ServerError> {
    match relay
        .get_chunk(
            CorrelationId::new(1),
            &room(),
            index,
            size,
            anchor,
            bounds(0, 100),
        )
        .await?
    {
        ServerMessage::SnapshotChunk {
            total_chunks,
            snapshot_hash,
            data,
            ..
        } => Ok((total_chunks, snapshot_hash, data)),
        other => panic!("expected a snapshot chunk, got {other:?}"),
    }
}

pub(super) fn files_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_file())
        .map(|e| e.file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

pub(super) fn snapshots_dir(dir: &TempDir) -> PathBuf {
    dir.path().join("snapshots")
}

pub(super) fn uploads_dir(dir: &TempDir) -> PathBuf {
    dir.path().join("snapshots").join(UPLOADS_DIR)
}

pub(super) fn part(
    n: u64,
    index: u32,
    layout: (u64, u32),
    hash: [u8; 32],
    data: &[u8],
) -> SnapshotChunkUpload {
    SnapshotChunkUpload {
        room_id: room(),
        uploader: Uploader::Client(ClientId::new("uploader").unwrap()),
        head_seq: seq(n),
        chunk_index: index,
        total_chunks: layout.1,
        total_bytes: layout.0,
        snapshot_hash: hash,
        data: Bytes::copy_from_slice(data),
    }
}

pub(super) fn assert_bad_request<T: std::fmt::Debug>(result: Result<T, ServerError>) {
    assert!(
        matches!(result, Err(ServerError::BadRequest(_))),
        "expected BadRequest, got {result:?}"
    );
}

pub(super) fn assert_superseded<T: std::fmt::Debug>(result: Result<T, ServerError>) {
    assert!(
        matches!(result, Err(ServerError::SnapshotSuperseded(_))),
        "expected SnapshotSuperseded, got {result:?}"
    );
}

/// Snapshot bytes, `(total_bytes, total_chunks)`, hash, and the byte range of each chunk.
pub(super) type ThreePartSnapshot = (Vec<u8>, (u64, u32), [u8; 32], [std::ops::Range<usize>; 3]);

/// A 200 KiB snapshot uploaded in 3 chunks: 68 267 + 68 267 + 68 266 bytes.
pub(super) fn three_part_snapshot(fill: u8) -> ThreePartSnapshot {
    let data = envelope(200 * 1024, fill);
    let hash = ServerMessage::compute_snapshot_hash(&data);
    let chunk_len = data.len().div_ceil(3);
    let ranges = [
        0..chunk_len,
        chunk_len..2 * chunk_len,
        2 * chunk_len..data.len(),
    ];
    (data.clone(), (data.len() as u64, 3), hash, ranges)
}

pub(super) fn part_by(
    uploader: Uploader,
    n: u64,
    index: u32,
    snapshot: &ThreePartSnapshot,
) -> SnapshotChunkUpload {
    let (data, layout, hash, ranges) = snapshot;
    SnapshotChunkUpload {
        uploader,
        ..part(
            n,
            index,
            *layout,
            *hash,
            &data[ranges[index as usize].clone()],
        )
    }
}

pub(super) fn member(name: &str) -> Uploader {
    Uploader::Client(ClientId::new(name).unwrap())
}
