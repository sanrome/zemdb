use crate::durable;
use crate::error::ServerError;
use crate::log::budget::ReadBudget;
use crate::log::cold_disk::read_cold_file;
use crate::log::io_probe::{self, IoEvent};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use zemdb_core::id::{MutationId, SequenceNumber};

/// Storage tier of a sealed segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tier {
    /// Uncompressed `.wal`.
    Warm,
    /// Zstandard-compressed `.wal.zst`.
    Cold,
}

impl Tier {
    /// Name of the tier in log messages.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Tier::Warm => "warm sealed",
            Tier::Cold => "cold",
        }
    }

    /// Extension of the tier's segment files.
    pub(crate) fn extension(self) -> &'static str {
        match self {
            Tier::Warm => ".wal",
            Tier::Cold => ".wal.zst",
        }
    }
}

/// A sealed segment of the log: the operations `start_seq..=end_seq` in one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentEntry {
    pub(crate) start_seq: SequenceNumber,
    pub(crate) end_seq: SequenceNumber,
    pub(crate) tier: Tier,
    pub(crate) path: PathBuf,
    /// Size of the file.
    pub(crate) bytes: u64,
    /// When the segment entered its tier (sealed, or compressed). Compression and the cold TTL
    /// count from here; after a reopen it is the file's modification time.
    pub(crate) since: SystemTime,
}

impl SegmentEntry {
    /// Bytes of the segment and of its sidecar, whose size follows from the range: a sidecar
    /// holds at most one entry per operation.
    pub(crate) fn disk_bytes(&self) -> u64 {
        let ops = (self.end_seq.get().saturating_sub(self.start_seq.get())).saturating_add(1);
        let sidecar = ops.saturating_mul(MUTATION_ID_ENTRY as u64);
        self.bytes
            .saturating_add(sidecar.saturating_add(SIDECAR_FRAMING))
    }
}

/// In-memory index of the sealed segments of a room log, in sequence order.
///
/// Built with a single listing of the segments directory when the log opens and kept up to
/// date by the log itself on every rotation, compression and prune, so that serving deltas and
/// running maintenance never list the directory. It also keeps the total size of the sealed
/// segments and their sidecars, for the room's disk quota.
#[derive(Debug, Default)]
pub(crate) struct SegmentIndex {
    segments: BTreeMap<u64, SegmentEntry>,
    total_bytes: u64,
}

impl SegmentIndex {
    /// Lists `segments_dir` once and indexes the sealed segments in it.
    ///
    /// Also finishes the cleanup of writes interrupted by a crash: temporary files
    /// (`segment_*.tmp`, from a compression or a sidecar) are removed. When a cold segment and
    /// its warm original both exist (the crash came between the rename of the cold segment and
    /// the removal of the warm one), the warm one is removed if the cold one holds exactly its
    /// range, every frame verified; otherwise the cold one is, and the warm one is compressed
    /// again later.
    /// Segments ending at or below `pruned_through_seq` are removed with their sidecars too: a
    /// prune or a removal that failed or was interrupted left them behind. The directory is
    /// synced after removing anything.
    pub(crate) fn scan(
        segments_dir: &Path,
        pruned_through_seq: SequenceNumber,
    ) -> Result<Self, ServerError> {
        io_probe::record(IoEvent::Listing, segments_dir);
        let mut warm = Vec::new();
        let mut cold = Vec::new();
        let mut leftovers = Vec::new();
        for entry in std::fs::read_dir(segments_dir)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !name.starts_with("segment_") || !entry.file_type()?.is_file() {
                continue;
            }
            if name.ends_with(".tmp") {
                leftovers.push(entry.path());
                continue;
            }
            let Some(tier) = [Tier::Cold, Tier::Warm]
                .into_iter()
                .find(|tier| name.ends_with(tier.extension()))
            else {
                continue;
            };
            let Some((start, end)) = parse_segment_filename(&name, tier) else {
                continue;
            };
            if end <= pruned_through_seq.get() {
                // The sidecar first, as pruning does: never a sidecar without its segment.
                leftovers.push(mutation_ids_path(
                    segments_dir,
                    SequenceNumber::new(start),
                    SequenceNumber::new(end),
                ));
                leftovers.push(entry.path());
                continue;
            }
            // Queried through the file rather than the listing: on Windows the size a
            // directory listing reports can lag behind the file's.
            let metadata = std::fs::metadata(entry.path())?;
            let segment = SegmentEntry {
                start_seq: SequenceNumber::new(start),
                end_seq: SequenceNumber::new(end),
                tier,
                path: entry.path(),
                bytes: metadata.len(),
                since: metadata.modified().unwrap_or_else(|_| SystemTime::now()),
            };
            match tier {
                Tier::Warm => warm.push(segment),
                Tier::Cold => cold.push(segment),
            }
        }

        let mut index = Self::default();
        for segment in warm {
            index.insert(segment);
        }
        for segment in cold {
            let duplicate = index
                .segments
                .get(&segment.start_seq.get())
                .is_some_and(|warm| warm.end_seq == segment.end_seq);
            if duplicate && !holds_its_range(&segment)? {
                leftovers.push(segment.path);
            } else if let Some(warm) = index.insert(segment).filter(|_| duplicate) {
                leftovers.push(warm.path);
            }
        }

        if !leftovers.is_empty() {
            for path in &leftovers {
                tracing::warn!(path = ?path, "Removing a leftover segment file");
                durable::remove_file_if_exists(path)?;
            }
            durable::sync_dir(segments_dir)?;
        }
        Ok(index)
    }

    /// Adds `segment`, replacing and returning the one with the same `start_seq`.
    pub(crate) fn insert(&mut self, segment: SegmentEntry) -> Option<SegmentEntry> {
        self.total_bytes = self.total_bytes.saturating_add(segment.disk_bytes());
        let replaced = self.segments.insert(segment.start_seq.get(), segment)?;
        self.total_bytes = self.total_bytes.saturating_sub(replaced.disk_bytes());
        Some(replaced)
    }

    /// Removes the segment starting at `start_seq`.
    pub(crate) fn remove(&mut self, start_seq: SequenceNumber) -> Option<SegmentEntry> {
        let removed = self.segments.remove(&start_seq.get())?;
        self.total_bytes = self.total_bytes.saturating_sub(removed.disk_bytes());
        Some(removed)
    }

    /// The segments in sequence order.
    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = &SegmentEntry> {
        self.segments.values()
    }

    /// The segments in sequence order, from the last one starting at or before `seq`.
    pub(crate) fn iter_from(&self, seq: SequenceNumber) -> impl Iterator<Item = &SegmentEntry> {
        let first = self.segments.range(..=seq.get()).next_back();
        let start = first.map_or(0, |(start, _)| *start);
        self.segments.range(start..).map(|(_, segment)| segment)
    }

    /// First sequence of the oldest segment.
    pub(crate) fn first_start(&self) -> Option<SequenceNumber> {
        self.segments
            .values()
            .next()
            .map(|segment| segment.start_seq)
    }

    /// Last sequence of the newest segment.
    pub(crate) fn last_end(&self) -> Option<SequenceNumber> {
        self.segments
            .values()
            .next_back()
            .map(|segment| segment.end_seq)
    }

    /// Total size of the indexed segment files and their sidecars.
    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// Whether the cold `segment` holds exactly the operations of its name, every frame verified.
/// An I/O error is returned instead: it does not prove the copy damaged.
fn holds_its_range(segment: &SegmentEntry) -> Result<bool, ServerError> {
    let all = &mut ReadBudget::ops(usize::MAX);
    match read_cold_file(
        &segment.path,
        Some(segment.start_seq),
        SequenceNumber::new(0),
        all,
    ) {
        Ok((ops, _)) => Ok(ops.last().map(|op| op.seq) == Some(segment.end_seq)),
        Err(ServerError::WalCorruption(_)) => Ok(false),
        Err(err) => Err(err),
    }
}

/// Path of the sidecar of the segment `start..=end`: the mutation IDs of its operations, so
/// that opening the room does not decode the segment to rebuild the deduplication window.
pub(crate) fn mutation_ids_path(
    segments_dir: &Path,
    start: SequenceNumber,
    end: SequenceNumber,
) -> PathBuf {
    segments_dir.join(format!("segment_{:016}_{:016}.ids", start.get(), end.get()))
}

/// Size of one entry of a sidecar: the mutation ID and its sequence, little-endian.
const MUTATION_ID_ENTRY: usize = 16 + 8;

/// Size of a sidecar without its entries: the segment's range and the CRC32.
const SIDECAR_FRAMING: u64 = 16 + 4;

/// Encodes the sidecar of the segment `start..=end`: the segment's range, its entries, and a
/// CRC32 of both, so that a sidecar is valid only for its own segment.
pub(crate) fn encode_mutation_ids(
    start: SequenceNumber,
    end: SequenceNumber,
    ids: &[(MutationId, SequenceNumber)],
) -> Vec<u8> {
    let mut bytes = [start.get().to_le_bytes(), end.get().to_le_bytes()].concat();
    for (id, seq) in ids {
        bytes.extend_from_slice(id.as_bytes());
        bytes.extend_from_slice(&seq.get().to_le_bytes());
    }
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    bytes
}

/// Decodes the sidecar of the segment `start..=end`, or `None` if it is damaged or belongs to
/// another segment.
pub(crate) fn decode_mutation_ids(
    bytes: &[u8],
    start: SequenceNumber,
    end: SequenceNumber,
) -> Option<Vec<(MutationId, SequenceNumber)>> {
    let (body, crc) = bytes.split_last_chunk::<4>()?;
    let (range, entries) = body.split_first_chunk::<16>()?;
    if range[..] != [start.get().to_le_bytes(), end.get().to_le_bytes()].concat()
        || entries.len() % MUTATION_ID_ENTRY != 0
        || crc32fast::hash(body) != u32::from_le_bytes(*crc)
    {
        return None;
    }
    entries
        .chunks_exact(MUTATION_ID_ENTRY)
        .map(|entry| {
            let (id, seq) = entry.split_first_chunk::<16>()?;
            let seq = u64::from_le_bytes(seq.try_into().ok()?);
            Some((MutationId::new(*id), SequenceNumber::new(seq)))
        })
        .collect()
}

/// Path of the `tier` segment holding `start..=end` in `segments_dir`.
pub(crate) fn segment_path(
    segments_dir: &Path,
    start: SequenceNumber,
    end: SequenceNumber,
    tier: Tier,
) -> PathBuf {
    segments_dir.join(format!(
        "segment_{:016}_{:016}{}",
        start.get(),
        end.get(),
        tier.extension()
    ))
}

/// `(start, end)` of a `tier` segment file name, the inverse of [`segment_path`], or `None` if
/// the name does not parse or its range ends before it starts.
pub(crate) fn parse_segment_filename(name: &str, tier: Tier) -> Option<(u64, u64)> {
    let core = name
        .strip_prefix("segment_")?
        .strip_suffix(tier.extension())?;
    let (start, end) = core.split_once('_')?;
    let (start, end) = (start.parse().ok()?, end.parse().ok()?);
    (start <= end).then_some((start, end))
}

#[cfg(test)]
#[path = "tests/segment_index.rs"]
mod tests;
