//! What the layer over the Origin Private File System decides without the browser: the names of
//! a path, the generations of a name and the one that an opening opens, a read and a write that
//! repeat until they are done, and what each error of the browser is.

use std::collections::BTreeSet;
use std::io;
use std::path::Path;

/// What separates a name from the number of one of its generations. No name of the engine or of
/// the metadata of the client has it (a room id has only lowercase letters, digits, `-` and
/// `_`), and the layer refuses a path with it, so no name can be taken for a generation.
pub(crate) const GENERATION: char = '~';

/// The errors the browser gives a `createSyncAccessHandle` of a file that another handle has
/// open: `NoModificationAllowedError` in Chromium and Firefox, and `InvalidStateError` in Safari,
/// which gives it for nothing else.
pub(crate) const TAKEN_BY_OPEN: &[&str] = &["NoModificationAllowedError", "InvalidStateError"];

/// The errors the browser gives a `move()` of a file that another handle has open, as it gives
/// them to `createSyncAccessHandle`.
pub(crate) const TAKEN_BY_MOVE: &[&str] = TAKEN_BY_OPEN;

/// The error the browser gives a `removeEntry` of a file that another handle has open, in the
/// three.
pub(crate) const TAKEN_BY_REMOVE: &[&str] = &["NoModificationAllowedError"];

/// The names of the directories of the directory `path`, from the root of the layer. A path is
/// relative to the root: an absolute one, or one with `..`, is refused, as is a name with
/// [`GENERATION`], or with `\` (which Chromium does not take) or a NUL character (refused as a
/// precaution). Empty names and `.` stand for no directory.
pub(crate) fn dir_names(path: &Path) -> io::Result<Vec<String>> {
    let text = path.to_str().ok_or_else(|| refused(path, "is not UTF-8"))?;
    if text.starts_with('/') {
        return Err(refused(path, "leaves the root"));
    }
    text.split('/')
        .filter(|name| !name.is_empty() && *name != ".")
        .map(|name| match name {
            ".." => Err(refused(path, "leaves the root")),
            _ if name.contains(GENERATION) => Err(refused(path, "has a name with '~'")),
            _ if name.contains(['\\', '\0']) => Err(refused(path, "has a name with '\\' or NUL")),
            _ => Ok(name.to_owned()),
        })
        .collect()
}

/// The names of the directories of the file `path`, as [`dir_names`] gives them, and its own,
/// which is its last name: a path that ends in `/` or in `.` names no file.
pub(crate) fn file_names(path: &Path) -> io::Result<(Vec<String>, String)> {
    let mut names = dir_names(path)?;
    let last = path.to_str().and_then(|text| text.rsplit('/').next());
    match (names.pop(), last) {
        (Some(name), Some(last)) if name == last => Ok((names, name)),
        _ => Err(refused(path, "does not end in the name of a file")),
    }
}

/// `prefix`, the start of the names that a listing gives, which no name has if it has
/// [`GENERATION`].
pub(crate) fn prefix(prefix: &str) -> io::Result<String> {
    if prefix.contains(GENERATION) {
        let message = format!("the prefix {prefix:?} has '~', which no name has");
        return Err(io::Error::new(io::ErrorKind::InvalidInput, message));
    }
    Ok(prefix.to_owned())
}

fn refused(path: &Path, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("the path {path:?} {why}"),
    )
}

/// The entry of the generation `generation` of `name`: the generation 0 is the name itself.
pub(crate) fn entry(name: &str, generation: u64) -> String {
    if generation == 0 {
        name.to_owned()
    } else {
        format!("{name}{GENERATION}{generation}")
    }
}

/// The name and the generation of `entry`; `None` for an entry that no generation has.
pub(crate) fn generation(entry: &str) -> Option<(&str, u64)> {
    let Some((name, number)) = entry.rsplit_once(GENERATION) else {
        return Some((entry, 0));
    };
    let generation: u64 = number.parse().ok()?;
    let canonical = generation > 0 && generation.to_string() == number;
    (canonical && !name.is_empty() && !name.contains(GENERATION)).then_some((name, generation))
}

/// The generations of `name` among `entries`, from the oldest to the newest. They are those of
/// exactly that name, never of another that starts with it.
pub(crate) fn generations<'a>(entries: impl IntoIterator<Item = &'a str>, name: &str) -> Vec<u64> {
    let mut found: Vec<u64> = entries
        .into_iter()
        .filter_map(generation)
        .filter(|(of, _)| *of == name)
        .map(|(_, generation)| generation)
        .collect();
    found.sort_unstable();
    found
}

/// The generation that replacing a name with `generations` gives it: the one after its newest,
/// or the name itself if it has none, so that a replacement never moves onto an entry.
pub(crate) fn next(generations: &[u64]) -> io::Result<u64> {
    match generations.last() {
        None => Ok(0),
        Some(newest) => newest
            .checked_add(1)
            .ok_or_else(|| io::Error::other("the name has no generation after its newest")),
    }
}

/// What an opening opens: the newest generation of its name, or the name itself if it has none.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Opening {
    /// As found (`open_locked`).
    Found,
    /// Only the name itself, which must have no generation (`create_locked`).
    New,
    /// Emptied (`create`).
    Emptied,
    /// Only the newest generation, which must exist (`open_read`).
    Existing,
}

/// The generation of `name` that `how` opens, among `generations`, its own from the oldest.
pub(crate) fn opened(generations: &[u64], name: &str, how: Opening) -> io::Result<u64> {
    match (generations.last(), how) {
        (Some(_), Opening::New) => {
            let message = format!("{name} exists");
            Err(io::Error::new(io::ErrorKind::AlreadyExists, message))
        }
        (Some(&newest), _) => Ok(newest),
        (None, Opening::Existing) => {
            let message = format!("{name} does not exist");
            Err(io::Error::new(io::ErrorKind::NotFound, message))
        }
        (None, _) => Ok(0),
    }
}

/// Reads `len` bytes at `offset` with `read`, which reads into a buffer at an offset and gives how
/// many bytes it read, none at the end of the file: it repeats a read that gives fewer, until it
/// has them all, and is [`io::ErrorKind::UnexpectedEof`] if the file ends first.
pub(crate) fn read_exact(
    offset: u64,
    len: usize,
    mut read: impl FnMut(&mut [u8], u64) -> io::Result<usize>,
) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0; len];
    let mut done = 0;
    while done < len {
        match read(&mut bytes[done..], offset.saturating_add(done as u64))? {
            0 => {
                let message = format!("read: the file ends at {done} of {len} bytes from {offset}");
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, message));
            }
            read => done += read.min(len - done),
        }
    }
    Ok(bytes)
}

/// Writes `bytes` at `offset` with `write`, which writes a buffer at an offset and gives how many
/// bytes it wrote: it repeats a write that writes fewer, and is [`io::ErrorKind::WriteZero`] if
/// one writes none.
pub(crate) fn write_all(
    offset: u64,
    bytes: &[u8],
    mut write: impl FnMut(&[u8], u64) -> io::Result<usize>,
) -> io::Result<()> {
    let mut done = 0;
    while done < bytes.len() {
        match write(&bytes[done..], offset.saturating_add(done as u64))? {
            0 => {
                let message = format!("write: wrote {done} of {} bytes", bytes.len());
                return Err(io::Error::new(io::ErrorKind::WriteZero, message));
            }
            written => done += written.min(bytes.len() - done),
        }
    }
    Ok(())
}

/// The names of `entries` that start with `prefix`, without the suffix of their generation: each
/// once, in order.
pub(crate) fn names<'a>(entries: &'a [String], prefix: &str) -> BTreeSet<&'a str> {
    entries
        .iter()
        .filter_map(|entry| generation(entry))
        .map(|(name, _)| name)
        .filter(|name| name.starts_with(prefix))
        .collect()
}

/// The kind of an error that the browser names `name`.
pub(crate) fn error_kind(name: &str) -> io::ErrorKind {
    match name {
        "NotFoundError" => io::ErrorKind::NotFound,
        "QuotaExceededError" => io::ErrorKind::StorageFull,
        "TypeError" => io::ErrorKind::InvalidInput,
        "NotAllowedError" | "SecurityError" => io::ErrorKind::PermissionDenied,
        _ => io::ErrorKind::Other,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "tests/opfs_names.rs"]
mod tests;
