//! Snapshot relay files: naming, writing, reading, syncing and verification. Everything here is
//! blocking I/O, run on Tokio's blocking pool through [`run_blocking`].

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use tracing::warn;
use zemdb_core::id::{RoomId, SequenceNumber};
use zemdb_core::protocol::snapshot_envelope::SnapshotEnvelopeValidator;

use crate::durable;
use crate::error::ServerError;
use crate::fail_point;

/// Subdirectory of the snapshots directory holding partial uploads.
pub(super) const UPLOADS_DIR: &str = "uploads";

/// Extension of a staged snapshot file: `<room>_<seq>_<blake3 hex>.snap`.
const SNAPSHOT_EXTENSION: &str = "snap";

/// Extension of a multipart upload in progress: `uploads/<room>_<seq>.part`.
pub(super) const PART_EXTENSION: &str = "part";

/// Extension of a single-request upload being written: `uploads/<room>_<seq>.tmp`.
pub(super) const TMP_EXTENSION: &str = "tmp";

/// Buffer size used to hash and validate snapshot files.
const VERIFY_BUFFER_BYTES: usize = 1024 * 1024;

/// Runs blocking file work on Tokio's blocking pool.
pub(super) async fn run_blocking<T, F>(work: F) -> Result<T, ServerError>
where
    F: FnOnce() -> Result<T, ServerError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| ServerError::Internal(format!("Snapshot relay task failed: {e}")))?
}

pub(super) fn snapshot_file_name(room_id: &RoomId, seq: SequenceNumber, hash: &[u8; 32]) -> String {
    format!(
        "{}_{}_{}.{SNAPSHOT_EXTENSION}",
        room_id.as_str(),
        seq.get(),
        blake3::Hash::from(*hash).to_hex()
    )
}

/// Parses `<room>_<seq>_<blake3 hex>.snap`. Room ids may contain `_`, so the name is split
/// from the right.
pub(super) fn parse_snapshot_file_name(name: &str) -> Option<(RoomId, SequenceNumber, [u8; 32])> {
    let stem = name.strip_suffix(&format!(".{SNAPSHOT_EXTENSION}"))?;
    let (rest, hash_hex) = stem.rsplit_once('_')?;
    let (room, seq) = rest.rsplit_once('_')?;
    let hash = blake3::Hash::from_hex(hash_hex).ok()?;
    Some((
        RoomId::new(room).ok()?,
        SequenceNumber::new(seq.parse().ok()?),
        *hash.as_bytes(),
    ))
}

pub(super) fn upload_file_name(room_id: &RoomId, seq: SequenceNumber, extension: &str) -> String {
    format!("{}_{}.{extension}", room_id.as_str(), seq.get())
}

/// Parses `<room>_<seq>.part` and `<room>_<seq>.tmp`.
pub(super) fn parse_upload_file_name(name: &str) -> Option<(RoomId, SequenceNumber)> {
    let (stem, extension) = name.rsplit_once('.')?;
    if extension != PART_EXTENSION && extension != TMP_EXTENSION {
        return None;
    }
    let (room, seq) = stem.rsplit_once('_')?;
    Some((
        RoomId::new(room).ok()?,
        SequenceNumber::new(seq.parse().ok()?),
    ))
}

/// Deletes every file in `dir` whose name `owner` attributes to `room_id`, then syncs `dir`.
pub(super) fn remove_room_files(
    dir: &Path,
    room_id: &RoomId,
    owner: impl Fn(&str) -> Option<RoomId>,
) -> Result<(), ServerError> {
    let mut removed = false;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let belongs = entry
            .file_name()
            .to_str()
            .and_then(&owner)
            .is_some_and(|owner| &owner == room_id);
        if belongs {
            remove_file_if_exists(&entry.path())?;
            removed = true;
        }
    }
    if removed {
        durable::sync_dir(dir)?;
    }
    Ok(())
}

pub(super) fn remove_file_if_exists(path: &Path) -> std::io::Result<()> {
    injected("relay_remove_file", path)?;
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Surfaces an armed test fail point as the I/O error the caller would have seen.
fn injected(name: &'static str, path: &Path) -> std::io::Result<()> {
    fail_point::check(name, path).map_err(|e| std::io::Error::other(e.to_string()))
}

/// Flushes a file's data to disk. The handle is opened for writing because Windows can only
/// flush through a writable handle.
pub(super) fn sync_file(path: &Path) -> std::io::Result<()> {
    OpenOptions::new().write(true).open(path)?.sync_all()
}

/// Writes `bytes` to a new file at `path` (replacing any leftover) and syncs it.
pub(super) fn write_new_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Writes one upload chunk at its offset. The file is synced once, when the upload completes.
pub(super) fn write_at(path: &Path, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)
}

/// Reads `len` bytes at `offset`, through a handle opened for this read only.
pub(super) fn read_range(path: &Path, offset: u64, len: u64) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let len = usize::try_from(len).map_err(std::io::Error::other)?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

/// Moves a synced temporary file in the uploads directory to its final name in the snapshots
/// directory and makes both directory entries durable. If that fails, the new file is removed
/// so that it is not mistaken for an accepted snapshot.
pub(super) fn promote(
    tmp_path: &Path,
    final_path: &Path,
    uploads_dir: &Path,
    snapshots_dir: &Path,
) -> std::io::Result<()> {
    fs::rename(tmp_path, final_path)?;
    let synced = durable::sync_dir(snapshots_dir).and_then(|()| durable::sync_dir(uploads_dir));
    if let Err(err) = synced {
        if let Err(remove_err) = remove_file_if_exists(final_path) {
            warn!(path = ?final_path, error = %remove_err, "Failed to delete snapshot after a failed directory sync");
        }
        return Err(err);
    }
    Ok(())
}

#[derive(Debug)]
pub(super) enum VerifyError {
    /// The file is not the snapshot it claims to be.
    Invalid(String),
    Io(std::io::Error),
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VerifyError::Invalid(msg) => f.write_str(msg),
            VerifyError::Io(err) => write!(f, "{err}"),
        }
    }
}

/// Reads a snapshot file once, checking its envelope header and CRC and that its BLAKE3 hash
/// is `expected_hash`. Returns its size.
pub(super) fn verify_snapshot_file(
    path: &Path,
    expected_hash: &[u8; 32],
) -> Result<u64, VerifyError> {
    injected("relay_verify_snapshot", path).map_err(VerifyError::Io)?;
    let mut file = File::open(path).map_err(VerifyError::Io)?;
    let mut hasher = blake3::Hasher::new();
    let mut envelope = SnapshotEnvelopeValidator::new();
    let mut buf = vec![0u8; VERIFY_BUFFER_BYTES];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf).map_err(VerifyError::Io)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        envelope.update(&buf[..n]);
        total += n as u64;
    }
    if hasher.finalize().as_bytes() != expected_hash {
        return Err(VerifyError::Invalid(
            "BLAKE3 digest verification failed: the assembled snapshot does not match its hash"
                .to_string(),
        ));
    }
    envelope
        .finish()
        .map_err(|e| VerifyError::Invalid(format!("Invalid snapshot envelope: {e}")))?;
    Ok(total)
}

#[cfg(test)]
#[path = "tests/files.rs"]
mod tests;
