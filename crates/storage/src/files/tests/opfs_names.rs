use super::*;
use crate::disk::compactor::{absorbing_wal_path, compacting_wal_path};
use crate::{DiskStorageEngine, DiskStorageOptions};
use std::path::PathBuf;
use zemdb_core::RoomId;

fn names(path: &str) -> io::Result<Vec<String>> {
    dir_names(Path::new(path))
}

fn refused(result: io::Result<impl std::fmt::Debug>) {
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{err}");
}

#[test]
fn a_path_is_split_into_its_names_from_the_root() {
    assert_eq!(names("zemdb/data").unwrap(), ["zemdb", "data"]);
    assert_eq!(names("./zemdb//data/").unwrap(), ["zemdb", "data"]);
    assert_eq!(names("").unwrap(), Vec::<String>::new());
    assert_eq!(names(".").unwrap(), Vec::<String>::new());
    let (dirs, name) = file_names(Path::new("zemdb/data/room_a.wal")).unwrap();
    assert_eq!(
        (dirs, name.as_str()),
        (vec!["zemdb".into(), "data".into()], "room_a.wal")
    );
    let (dirs, name) = file_names(Path::new("client.lock")).unwrap();
    assert_eq!((dirs.len(), name.as_str()), (0, "client.lock"));
}

#[test]
fn a_path_or_a_prefix_that_the_layer_does_not_take_is_refused() {
    for path in [
        "..",
        "../data",
        "zemdb/../data",
        "zemdb/data/..",
        "/zemdb",
        "/",
        "zemdb/data~1",
        "zemdb~/data",
        "~",
        "zemdb/b\\c",
        "zemdb\\/data",
        "zemdb/b\0c",
    ] {
        refused(names(path));
        refused(file_names(Path::new(path)));
    }
    // A file needs a name, and it is the last one of its path.
    for path in [
        "",
        ".",
        "./",
        "zemdb/",
        "zemdb/f/",
        "zemdb/f/.",
        "zemdb/f//",
    ] {
        refused(file_names(Path::new(path)));
    }
    assert_eq!(names("zemdb/f/.").unwrap(), ["zemdb", "f"]);
    // No name has the mark of a generation, so a listing does not look for it.
    refused(prefix("room_a.snap~"));
    refused(prefix("~"));
    assert_eq!(prefix("room_a.snap.tmp.").unwrap(), "room_a.snap.tmp.");
    assert_eq!(prefix("").unwrap(), "");
}

#[test]
fn the_first_generation_of_a_name_is_the_name_itself() {
    assert_eq!(entry("room_a.wal", 0), "room_a.wal");
    assert_eq!(entry("room_a.wal", 1), "room_a.wal~1");
    assert_eq!(entry("room_a.wal", 12), "room_a.wal~12");
    for generation in [0, 1, 9, 10, 12, u64::MAX] {
        let entry = entry("room_a.wal", generation);
        assert_eq!(super::generation(&entry), Some(("room_a.wal", generation)));
    }
}

#[test]
fn an_entry_that_no_generation_has_is_not_one() {
    for entry in [
        "room_a.wal~0",
        "room_a.wal~01",
        "room_a.wal~+1",
        "room_a.wal~",
        "room_a.wal~x",
        "room_a.wal~1~2",
        "~1",
        "room_a.wal~18446744073709551616",
    ] {
        assert_eq!(generation(entry), None, "{entry}");
    }
}

#[test]
fn the_generations_of_a_name_are_those_of_exactly_that_name_from_the_oldest() {
    let entries = [
        "room_a.wal~2",
        "room_a.wal.compacting",
        "room_a.wal",
        "room_a.wal.compacting~4",
        "room_a.wal~10",
        "room_a.wa",
        "room_a.wal~x",
        "room_ab.wal~3",
    ];
    assert_eq!(generations(entries, "room_a.wal"), [0, 2, 10]);
    assert_eq!(generations(entries, "room_a.wal.compacting"), [0, 4]);
    assert_eq!(generations(entries, "room_a.wa"), [0]);
    assert_eq!(generations(entries, "room_ab.wal"), [3]);
    assert_eq!(generations(entries, "room_b.wal"), Vec::<u64>::new());
}

#[test]
fn a_replacement_goes_to_a_free_entry() {
    assert_eq!(next(&[]).unwrap(), 0);
    assert_eq!(next(&[0]).unwrap(), 1);
    assert_eq!(next(&[0, 2, 10]).unwrap(), 11);
    assert!(next(&[3, u64::MAX]).is_err());
}

#[test]
fn a_listing_gives_each_name_once_without_its_generations() {
    let entries: Vec<String> = [
        "room_a.snap.tmp.x~3",
        "room_a.snap",
        "room_a.snap.tmp.x",
        "room_a.snap~1",
        "room_a.snap.tmp.y~2",
        "room_a.snap~x",
        "room_b.snap",
    ]
    .map(String::from)
    .into();
    let found: Vec<&str> = super::names(&entries, "room_a.snap").into_iter().collect();
    assert_eq!(
        found,
        ["room_a.snap", "room_a.snap.tmp.x", "room_a.snap.tmp.y"]
    );
    let found: Vec<&str> = super::names(&entries, "room_a.snap.tmp.")
        .into_iter()
        .collect();
    assert_eq!(found, ["room_a.snap.tmp.x", "room_a.snap.tmp.y"]);
    assert_eq!(super::names(&entries, "").len(), 4);
}

/// Each opening opens the newest generation of its name, or the name itself if it has none;
/// `create_locked` only a name with none, and `open_read` only one with some.
#[test]
fn an_opening_opens_the_newest_generation_or_the_name_itself() {
    for how in [Opening::Found, Opening::Emptied] {
        assert_eq!(opened(&[], "a", how).unwrap(), 0, "{how:?}");
        assert_eq!(opened(&[0, 3, 7], "a", how).unwrap(), 7, "{how:?}");
    }
    assert_eq!(opened(&[], "a", Opening::New).unwrap(), 0);
    for generations in [&[0][..], &[2], &[0, 1]] {
        let err = opened(generations, "a", Opening::New).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{generations:?}");
    }
    assert_eq!(opened(&[1, 4], "a", Opening::Existing).unwrap(), 4);
    let err = opened(&[], "a", Opening::Existing).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

/// A file of `contents` read or written at most `step` bytes at a time, as a sync access handle
/// may do, which reads nothing past its end and grows as it is written.
struct Short {
    contents: Vec<u8>,
    step: usize,
    calls: usize,
}

impl Short {
    fn read(&mut self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.calls += 1;
        let start = (offset as usize).min(self.contents.len());
        let n = buf.len().min(self.step).min(self.contents.len() - start);
        buf[..n].copy_from_slice(&self.contents[start..start + n]);
        Ok(n)
    }

    fn write(&mut self, bytes: &[u8], offset: u64) -> io::Result<usize> {
        self.calls += 1;
        let n = bytes.len().min(self.step);
        let end = offset as usize + n;
        if self.contents.len() < end {
            self.contents.resize(end, 0);
        }
        self.contents[offset as usize..end].copy_from_slice(&bytes[..n]);
        Ok(n)
    }
}

/// A read that gives fewer bytes than asked is repeated until it has them all; one that ends past
/// the size of the file is `UnexpectedEof` before it reserves or reads anything, and so is one
/// whose file ends before its size; and an error of a read is the error.
#[test]
fn a_read_repeats_until_it_has_every_byte_or_the_file_ends() {
    let mut file = Short {
        contents: b"0123456789".to_vec(),
        step: 3,
        calls: 0,
    };
    let mut read = |buf: &mut [u8], at| file.read(buf, at);
    assert_eq!(read_exact(1, 8, 10, &mut read).unwrap(), b"12345678");
    assert_eq!(read_exact(0, 10, 10, &mut read).unwrap(), b"0123456789");
    assert_eq!(read_exact(10, 0, 10, &mut read).unwrap(), b"");
    // Past the end, as far as the largest length or offset: nothing is reserved or read.
    for (offset, len) in [(6, 5), (12, 1), (0, usize::MAX), (u64::MAX, 2)] {
        let err = read_exact(offset, len, 10, &mut read).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{offset} {len}");
    }
    // A file that ends before the size it had.
    let err = read_exact(8, 4, 12, &mut read).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(file.calls, 3 + 4 + 2);
    let failing = |_: &mut [u8], _| Err(io::Error::from(io::ErrorKind::StorageFull));
    let err = read_exact(0, 1, 1, failing).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::StorageFull);
}

/// A count larger than what was asked, as Chromium 153 gives for a write over the quota, is an
/// error with the count, never a read or a write done.
#[test]
fn a_count_larger_than_what_was_asked_is_an_error() {
    let mut calls = 0;
    let says_more = |bytes: &[u8], _| {
        calls += 1;
        Ok(if calls == 1 {
            2
        } else {
            bytes.len() + 4_294_967_288
        })
    };
    let err = write_all(0, b"abcde", says_more).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert!(err.to_string().contains("4294967291 bytes for 3"), "{err}");
    assert_eq!(calls, 2);
    let says_more = |buf: &mut [u8], _| Ok(buf.len() + 1);
    let err = read_exact(0, 4, 10, says_more).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert!(err.to_string().contains("5 bytes for 4"), "{err}");
}

/// A write that writes fewer bytes than given is repeated until it wrote them all, and one that
/// writes none is `WriteZero`; one that would end past 2^53, where the offsets of the browser
/// are no longer exact, is refused before anything is written; and an error of a write is the
/// error.
#[test]
fn a_write_repeats_until_it_wrote_every_byte() {
    let mut file = Short {
        contents: b"abc".to_vec(),
        step: 2,
        calls: 0,
    };
    write_all(1, b"XYZW", |bytes, at| file.write(bytes, at)).unwrap();
    assert_eq!(file.contents, b"aXYZW");
    assert_eq!(file.calls, 2);
    let mut calls = 0;
    let stuck = |bytes: &[u8], _| {
        calls += 1;
        Ok(if calls == 1 { bytes.len().min(2) } else { 0 })
    };
    let err = write_all(0, b"abcde", stuck).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    assert_eq!(calls, 2);
    let failing = |_: &[u8], _| Err(io::Error::from(io::ErrorKind::StorageFull));
    let err = write_all(0, b"a", failing).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::StorageFull);

    let mut at_offsets = Vec::new();
    let mut last = |bytes: &[u8], at| {
        at_offsets.push(at);
        Ok(bytes.len())
    };
    write_all(MAX_END - 1, b"z", &mut last).unwrap();
    for (offset, bytes) in [(MAX_END - 1, &b"zz"[..]), (MAX_END, b"z"), (u64::MAX, b"z")] {
        let err = write_all(offset, bytes, &mut last).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{offset}");
    }
    assert_eq!(at_offsets, [MAX_END - 1]);
}

#[test]
fn each_error_of_the_browser_has_its_kind() {
    assert_eq!(error_kind("NotFoundError"), io::ErrorKind::NotFound);
    assert_eq!(error_kind("QuotaExceededError"), io::ErrorKind::StorageFull);
    assert_eq!(error_kind("TypeError"), io::ErrorKind::InvalidInput);
    assert_eq!(
        error_kind("NotAllowedError"),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(error_kind("SecurityError"), io::ErrorKind::PermissionDenied);
    for name in [
        "UnknownError",
        "InvalidModificationError",
        "TypeMismatchError",
        "",
    ] {
        assert_eq!(error_kind(name), io::ErrorKind::Other, "{name}");
    }
}

/// A file that another handle has open is the error each browser gives it: for a `move()`, the
/// same as for `createSyncAccessHandle` (Safari's included, which only Safari gives); for a
/// `removeEntry`, the same in the three, where `InvalidStateError` is another error.
#[test]
fn a_taken_file_is_the_error_that_each_browser_gives_it() {
    assert_eq!(
        TAKEN_BY_OPEN,
        ["NoModificationAllowedError", "InvalidStateError"]
    );
    assert_eq!(TAKEN_BY_MOVE, TAKEN_BY_OPEN);
    assert_eq!(TAKEN_BY_REMOVE, ["NoModificationAllowedError"]);
}

/// Every name that the engine and the metadata of the client give their files, with the longest
/// room id and every character a room id may have, is a path the layer takes, and is the first
/// generation of itself: none can be taken for the generation of another.
#[test]
fn no_name_of_the_engine_or_of_the_metadata_has_the_mark_of_a_generation() {
    let room = RoomId::new(format!("az09-_{}", "x".repeat(58))).unwrap();
    let engine = DiskStorageEngine::new(DiskStorageOptions::new("zemdb/data")).unwrap();
    let wal = engine.wal_file_path(&room);
    let snap = engine.snap_file_path(&room);
    let staged = snap.with_extension(format!("snap.tmp.{}", uuid::Uuid::new_v4()));
    let mut paths = vec![
        engine.lock_file_path(&room),
        snap,
        staged,
        compacting_wal_path(&wal),
        absorbing_wal_path(&wal),
        wal,
    ];
    // The files of `FileMetadata`, in the client.
    for name in ["client.lock", "client.meta", "client.meta.tmp"] {
        paths.push(PathBuf::from(name));
    }
    for name in [format!("room_{room}.meta"), format!("room_{room}.meta.tmp")] {
        paths.push(PathBuf::from(name));
    }
    for path in paths {
        // Only its name: the separator of a native path is not that of the OPFS on Windows.
        let name = path.file_name().unwrap().to_str().unwrap();
        let (dirs, own) = file_names(Path::new(name)).unwrap();
        assert!(dirs.is_empty() && own == name, "{path:?}");
        assert_eq!(generation(name), Some((name, 0)), "{path:?}");
    }
}
