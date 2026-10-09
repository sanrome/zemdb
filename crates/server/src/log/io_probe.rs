//! Test instrumentation of the log's file access.
//!
//! Production code calls `record` wherever the log lists its segments directory or reads a
//! segment file. In non-test builds `record` is an empty function that the compiler removes.
//! In unit-test builds every event is counted per segments directory, so tests running in
//! parallel never see each other's events, and a test can assert which disk work an operation
//! did (for example, that maintenance never lists the directory).

use std::path::Path;

/// A kind of file access made by the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IoEvent {
    /// A listing of the segments directory.
    Listing,
    /// A sealed segment (warm or cold) read from disk.
    SegmentRead,
    /// `active.wal` read through its path, that is, through a second handle instead of the
    /// one holding the lock.
    ActiveWalPathRead,
}

#[cfg(test)]
static EVENTS: std::sync::Mutex<Vec<(IoEvent, std::path::PathBuf, usize)>> =
    std::sync::Mutex::new(Vec::new());

/// Records `event` for the segments directory `segments_dir`.
#[inline(always)]
pub(crate) fn record(event: IoEvent, segments_dir: &Path) {
    #[cfg(test)]
    {
        let mut events = EVENTS.lock().unwrap();
        match events
            .iter_mut()
            .find(|(e, dir, _)| *e == event && dir == segments_dir)
        {
            Some((_, _, count)) => *count += 1,
            None => events.push((event, segments_dir.to_path_buf(), 1)),
        }
    }
    #[cfg(not(test))]
    let _ = (event, segments_dir);
}

/// How many times `event` was recorded for `segments_dir`.
#[cfg(test)]
pub(crate) fn count(event: IoEvent, segments_dir: &Path) -> usize {
    EVENTS
        .lock()
        .unwrap()
        .iter()
        .find(|(e, dir, _)| *e == event && dir == segments_dir)
        .map_or(0, |(_, _, count)| *count)
}
