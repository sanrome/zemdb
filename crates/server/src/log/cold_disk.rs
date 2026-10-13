use crate::durable;
use crate::error::ServerError;
use crate::fail_point;
use crate::log::budget::ReadBudget;
use crate::log::io_probe::{self, IoEvent};
use crate::log::segment_reader::{classify_in, read_frames, read_full};
use crate::log::warm_disk::{parse_segment_filename, RecoveredLogData};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Cursor, Read, Write};
use std::path::Path;
use zemdb_core::id::SequenceNumber;
use zemdb_core::protocol::wal_frame::{
    decode_batch_payload, parse_batch_header, BatchPayloadDecode, BATCH_HEADER_SIZE,
};

/// Zstandard level of cold segments.
const ZSTD_LEVEL: i32 = 3;

/// Chunk in which a warm segment is read while compressing it.
const COPY_CHUNK: usize = 64 * 1024;

/// Compression of warm segments into Zstandard cold segments (`.wal.zst`).
#[derive(Debug, Default)]
pub struct ColdDiskLog;

/// A warm segment whose cold copy is durably in place: written, synced, renamed over its
/// final path with the directory synced, and verified readable.
#[derive(Debug)]
pub struct CompressedSegment {
    /// Size of the cold segment.
    pub bytes: u64,
    /// Set when removing the warm original, or making its removal durable, failed. The cold
    /// segment replaces the warm one all the same. A warm file that is still there (or comes
    /// back after a crash) is found next to its cold copy the next time the log opens, which
    /// keeps the warm one and compresses it again.
    pub cleanup_error: Option<ServerError>,
}

impl ColdDiskLog {
    /// Compresses the warm segment at `warm_path` into the cold segment at `cold_path`.
    ///
    /// Writes to a `.tmp` file first, flushes, syncs, atomically renames over the destination
    /// and syncs the directory, verifies readability, and only then deletes the uncompressed
    /// `.wal` file (syncing the directory again). A power loss can therefore never persist the
    /// deletion of the warm segment without the cold segment that replaces it. Opening the log
    /// removes a `.tmp` file left by an interrupted compression.
    ///
    /// An `Err` means the cold segment is not in place and the warm one is untouched. Once the
    /// cold segment is in place the call returns `Ok`, reporting a failed removal of the warm
    /// segment in the outcome.
    pub fn compress_warm_segment_sync(
        warm_path: &Path,
        cold_path: &Path,
    ) -> Result<CompressedSegment, ServerError> {
        if !warm_path.exists() {
            return Err(ServerError::Wal(format!(
                "Cannot compress non-existent warm segment: {:?}",
                warm_path
            )));
        }

        let tmp_path = durable::tmp_path_for(cold_path);
        let range = warm_path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| parse_segment_filename(name, ".wal"));
        let warm_digest = match compress_to(warm_path, &tmp_path, range) {
            Ok(digest) => digest,
            Err(err) => {
                // Opening the log removes it too; removing it now keeps a segment that fails
                // on every pass from leaving one behind each time.
                if let Err(e) = durable::remove_file_if_exists(&tmp_path) {
                    tracing::warn!(path = ?tmp_path, error = %e, "Removing a failed compression's temporary file failed");
                }
                return Err(err);
            }
        };

        std::fs::rename(&tmp_path, cold_path)?;
        durable::sync_parent(cold_path)?;

        // Verify that the cold segment decompresses to exactly the warm one before purging it.
        let cold_digest = decompressed_digest(cold_path)?;
        if cold_digest != warm_digest {
            return Err(ServerError::WalCorruption(format!(
                "Cold segment verification failed: it decompresses to {} bytes with CRC32 {}, \
                 the warm segment has {} bytes with CRC32 {}",
                cold_digest.len, cold_digest.crc, warm_digest.len, warm_digest.crc
            )));
        }
        let bytes = std::fs::metadata(cold_path)?.len();

        let cleanup = std::fs::remove_file(warm_path)
            .map_err(ServerError::from)
            .and_then(|()| fail_point::check("compress_after_warm_removed", warm_path))
            .and_then(|()| durable::sync_parent(warm_path).map_err(ServerError::from));
        Ok(CompressedSegment {
            bytes,
            cleanup_error: cleanup.err(),
        })
    }

    /// [`compress_warm_segment_sync`](Self::compress_warm_segment_sync) on the blocking pool.
    pub async fn compress_warm_segment(
        warm_path: impl AsRef<Path>,
        cold_path: impl AsRef<Path>,
    ) -> Result<CompressedSegment, ServerError> {
        let warm = warm_path.as_ref().to_path_buf();
        let cold = cold_path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || Self::compress_warm_segment_sync(&warm, &cold))
            .await
            .map_err(|e| ServerError::Internal(format!("Spawn blocking task failed: {}", e)))?
    }
}

/// Length and CRC32 of a stream of bytes.
#[derive(Debug, Default, PartialEq, Eq)]
struct Digest {
    len: u64,
    crc: u32,
}

/// [`Digest`] of the bytes written to it.
#[derive(Default)]
struct DigestWriter {
    hasher: crc32fast::Hasher,
    len: u64,
}

impl DigestWriter {
    fn finish(self) -> Digest {
        Digest {
            len: self.len,
            crc: self.hasher.finalize(),
        }
    }
}

impl Write for DigestWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        self.len += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Compresses the file at `warm_path` into a new, synced file at `tmp_path`, streaming it, and
/// returns the digest of the uncompressed bytes.
///
/// Every frame is checked (header and payload checksums, decodable operations) as it is
/// compressed, so a warm segment is only ever replaced by a copy of frames that were all
/// verified: reads skip frames before their cursor without checking their payloads. With the
/// `(start, end)` sequences of the segment's name, its operations must also run exactly from
/// `start` to `end`, the start that reads trust when they skip frames. A frame that cannot be
/// read fails the compression and the warm segment stays.
fn compress_to(
    warm_path: &Path,
    tmp_path: &Path,
    range: Option<(u64, u64)>,
) -> Result<Digest, ServerError> {
    let mut warm = BufReader::with_capacity(COPY_CHUNK, File::open(warm_path)?);
    let tmp = File::create(tmp_path)?;
    let mut encoder = zstd::stream::write::Encoder::new(BufWriter::new(tmp), ZSTD_LEVEL)
        .map_err(|e| ServerError::Wal(format!("Zstd compression failed: {e}")))?;
    let mut digest = DigestWriter::default();
    let mut position = 0u64;
    let mut next_seq = range.map(|(start, _)| start);
    loop {
        let invalid = |reason: String| {
            ServerError::WalCorruption(format!(
                "Warm segment {warm_path:?} cannot be compressed, frame at offset {position}: {reason}"
            ))
        };
        let mut header_bytes = [0u8; BATCH_HEADER_SIZE];
        let available = read_full(&mut warm, &mut header_bytes)?;
        if available == 0 {
            break;
        }
        if available < BATCH_HEADER_SIZE {
            return Err(invalid(format!("incomplete header of {available} bytes")));
        }
        let header = parse_batch_header(&header_bytes).map_err(|e| invalid(e.to_string()))?;
        let mut payload = vec![0u8; header.payload_len()];
        let payload_read = read_full(&mut warm, &mut payload)?;
        if payload_read < payload.len() {
            return Err(invalid(format!(
                "payload of {payload_read} bytes, the header declares {}",
                payload.len()
            )));
        }
        match decode_batch_payload(&header, &payload).map_err(|e| invalid(e.to_string()))? {
            BatchPayloadDecode::Batch { ops, .. } => {
                for op in &ops {
                    let seq = op.seq.get();
                    if let Some(expected) = next_seq.filter(|&expected| expected != seq) {
                        return Err(invalid(format!("operation {seq}, expected {expected}")));
                    }
                    next_seq = Some(seq + 1);
                }
            }
            BatchPayloadDecode::ChecksumMismatch { expected, actual } => {
                return Err(invalid(format!(
                    "payload CRC32 is {actual}, expected {expected}"
                )));
            }
        }
        for part in [&header_bytes[..], &payload] {
            digest.write_all(part)?;
            encoder.write_all(part)?;
        }
        position += header.frame_len() as u64;
    }
    if let Some((start, end)) = range {
        if next_seq != Some(end + 1) {
            return Err(ServerError::WalCorruption(format!(
                "Warm segment {warm_path:?} holds operations {start} to {}, its name says {start} to {end}",
                next_seq.map_or(start, |next| next - 1)
            )));
        }
    }
    let mut tmp = encoder
        .finish()?
        .into_inner()
        .map_err(|e| ServerError::from(e.into_error()))?;
    tmp.flush()?;
    tmp.sync_all()?;
    Ok(digest.finish())
}

/// Digest of the decompressed contents of the cold segment at `cold_path`, streaming it.
fn decompressed_digest(cold_path: &Path) -> Result<Digest, ServerError> {
    let mut decoder = open_decoder(File::open(cold_path)?)?;
    let mut digest = DigestWriter::default();
    io::copy(&mut decoder, &mut digest).map_err(decompression_error)?;
    Ok(digest.finish())
}

/// An error of the file under a zstd stream, told apart from errors in the compressed data.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct FileError(io::Error);

/// A file whose read errors are tagged as [`FileError`].
struct TaggedFile(File);

impl Read for TaggedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0
            .read(buf)
            .map_err(|e| io::Error::new(e.kind(), FileError(e)))
    }
}

type ColdDecoder = zstd::stream::read::Decoder<'static, io::BufReader<TaggedFile>>;

fn open_decoder(file: File) -> Result<ColdDecoder, ServerError> {
    zstd::stream::read::Decoder::new(TaggedFile(file)).map_err(decompression_error)
}

/// An error reading a zstd stream: I/O if the file failed, corruption otherwise.
fn decompression_error(e: io::Error) -> ServerError {
    if e.get_ref().is_some_and(|inner| inner.is::<FileError>()) {
        ServerError::Io(e)
    } else {
        ServerError::WalCorruption(format!("Zstd decompression failed: {e}"))
    }
}

/// Reads the operations after `from_seq` that fit in `budget` from the cold segment at `path`,
/// whose first operation is `start_seq`, decompressing it as a stream. A missing file yields
/// nothing; the caller detects the gap it leaves.
pub(crate) fn read_cold_file(
    path: &Path,
    start_seq: Option<SequenceNumber>,
    from_seq: SequenceNumber,
    budget: &mut ReadBudget,
) -> Result<RecoveredLogData, ServerError> {
    if budget.is_exhausted() {
        return Ok(Default::default());
    }
    io_probe::record(
        IoEvent::SegmentRead,
        path.parent().unwrap_or_else(|| Path::new("")),
    );
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Default::default()),
        Err(e) => return Err(e.into()),
    };
    let mut decoder = open_decoder(file)?;
    let read =
        read_frames(&mut decoder, None, start_seq, from_seq, budget).map_err(|err| match err {
            ServerError::Io(e) => decompression_error(e),
            other => other,
        })?;

    if let Some((at, anomaly)) = read.anomaly {
        // The stream cannot seek back, so the rest of the segment is decompressed again to
        // look for a valid frame after the damage. This only happens on an anomaly, and the
        // segment is bounded by `segment_max_bytes` plus one frame.
        let mut decoder = open_decoder(File::open(path)?)?;
        let mut rest = Vec::new();
        io::copy(&mut decoder.by_ref().take(at), &mut io::sink())
            .and_then(|_| decoder.read_to_end(&mut rest))
            .map_err(decompression_error)?;
        let end = rest.len() as u64;
        let reason = classify_in(&mut Cursor::new(rest), 0, anomaly, end)?;
        tracing::warn!(path = ?path, valid_bytes = at, %reason, "Torn write detected in decompressed cold segment");
    }
    Ok((read.ops, read.mutations))
}

#[cfg(test)]
#[path = "tests/cold_disk.rs"]
mod tests;
