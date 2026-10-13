use crate::durable;
use crate::error::ServerError;
use crate::log::io_probe::{self, IoEvent};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use zemdb_core::id::SequenceNumber;

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

/// In-memory index of the sealed segments of a room log, in sequence order.
///
/// Built with a single listing of the segments directory when the log opens and kept up to
/// date by the log itself on every rotation, compression and prune, so that serving deltas and
/// running maintenance never list the directory. It also keeps the total size of the sealed
/// segments, for the room's disk quota.
#[derive(Debug, Default)]
pub(crate) struct SegmentIndex {
    segments: BTreeMap<u64, SegmentEntry>,
    total_bytes: u64,
}

impl SegmentIndex {
    /// Lists `segments_dir` once and indexes the sealed segments in it.
    ///
    /// Also finishes the cleanup of compressions interrupted by a crash: temporary cold files
    /// (`segment_*.tmp`) are removed, and so is a cold segment whose warm original still
    /// exists (the crash came between the rename of the cold segment and the removal of the
    /// warm one; the warm segment is kept and compressed again later). The directory is synced
    /// after removing anything.
    pub(crate) fn scan(segments_dir: &Path) -> Result<Self, ServerError> {
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
            if duplicate {
                leftovers.push(segment.path);
            } else {
                index.insert(segment);
            }
        }

        if !leftovers.is_empty() {
            for path in &leftovers {
                tracing::warn!(path = ?path, "Removing leftover of an interrupted segment compression");
                durable::remove_file_if_exists(path)?;
            }
            durable::sync_dir(segments_dir)?;
        }
        Ok(index)
    }

    /// Adds `segment`, replacing the one with the same `start_seq`.
    pub(crate) fn insert(&mut self, segment: SegmentEntry) {
        self.total_bytes += segment.bytes;
        if let Some(replaced) = self.segments.insert(segment.start_seq.get(), segment) {
            self.total_bytes -= replaced.bytes;
        }
    }

    /// Removes the segment starting at `start_seq`.
    pub(crate) fn remove(&mut self, start_seq: SequenceNumber) -> Option<SegmentEntry> {
        let removed = self.segments.remove(&start_seq.get())?;
        self.total_bytes -= removed.bytes;
        Some(removed)
    }

    /// The segments in sequence order.
    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = &SegmentEntry> {
        self.segments.values()
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

    /// Total size of the indexed segment files.
    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
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

/// `(start, end)` of a `tier` segment file name, the inverse of [`segment_path`].
pub(crate) fn parse_segment_filename(name: &str, tier: Tier) -> Option<(u64, u64)> {
    let core = name
        .strip_prefix("segment_")?
        .strip_suffix(tier.extension())?;
    let (start, end) = core.split_once('_')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

#[cfg(test)]
#[path = "tests/segment_index.rs"]
mod tests;
