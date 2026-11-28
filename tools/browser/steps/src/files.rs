//! The suite `files`: the operations on the directory of the file layer of the storage over the
//! OPFS (`zemdb_storage::files::OpfsFiles`), on the real layer, also after the page reloads.
//!
//! The layer opens no file yet, so the steps make the files with the browser, under
//! `harness/files-{run}/`. A name of the layer is a series of entries, its generations: the name
//! itself, and then the name with `~1`, `~2`, and so on. The steps make those that a cut of the
//! browser leaves, and check the entries that each operation leaves, by their names.

use crate::js::failed;
use crate::opfs::{file_handle, harness_dir, open, read_all, write_all};
use futures::future::Either;
use futures::FutureExt;
use js_sys::IteratorNext;
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{FileSystemDirectoryHandle, FileSystemGetDirectoryOptions};
use zemdb_storage::files::{FileTaken, OpfsFiles, RoomFiles};

/// The directory of a run, relative to the root of the OPFS, where the paths of the layer are.
pub(crate) fn base(run: &str) -> PathBuf {
    PathBuf::from(format!("harness/files-{run}"))
}

/// The directory `path` of the run, with its names separated by `/`, through the browser,
/// created if missing.
pub(crate) async fn dir(run: &str, path: &str) -> Result<FileSystemDirectoryHandle, JsError> {
    let options = FileSystemGetDirectoryOptions::new();
    options.set_create(true);
    let mut dir = harness_dir().await?;
    let base = format!("files-{run}");
    for name in std::iter::once(base.as_str()).chain(path.split('/')) {
        let next = dir.get_directory_handle_with_options(name, &options);
        let next = JsFuture::from(next)
            .await
            .map_err(failed("getDirectoryHandle"))?;
        dir = next.unchecked_into();
    }
    Ok(dir)
}

/// The names of the entries of `dir`, through the browser.
async fn entries(dir: &FileSystemDirectoryHandle) -> Result<BTreeSet<String>, JsError> {
    let keys = dir.keys();
    let mut found = BTreeSet::new();
    loop {
        let next = JsFuture::from(keys.next().map_err(failed("keys"))?)
            .await
            .map_err(failed("keys"))?;
        let next: IteratorNext = next.unchecked_into();
        if next.done() {
            return Ok(found);
        }
        let name = next.value().as_string();
        found.insert(name.ok_or_else(|| JsError::new("keys gave a name that is no string"))?);
    }
}

/// Fails unless the entries of `dir` are exactly `expected`, after `what`.
pub(crate) async fn expect_entries(
    dir: &FileSystemDirectoryHandle,
    expected: &[&str],
    what: &str,
) -> Result<(), JsError> {
    let found = entries(dir).await?;
    let expected: BTreeSet<String> = expected.iter().map(|name| name.to_string()).collect();
    check(found == expected, || {
        format!("after {what}, the entries are {found:?}, not {expected:?}")
    })
}

/// Makes the entry `entry` of `dir` with `bytes`, through the browser.
pub(crate) async fn put(
    dir: &FileSystemDirectoryHandle,
    entry: &str,
    bytes: &[u8],
) -> Result<(), JsError> {
    let file = file_handle(dir, entry, true)
        .await
        .map_err(failed("getFileHandle"))?;
    let handle = open(&file)
        .await
        .map_err(failed("createSyncAccessHandle"))?;
    let written = write_all(&handle, bytes);
    handle.close();
    written
}

/// What the entry `entry` of `dir` holds, through the browser.
async fn get(dir: &FileSystemDirectoryHandle, entry: &str) -> Result<Vec<u8>, JsError> {
    let file = file_handle(dir, entry, false)
        .await
        .map_err(failed(&format!("getFileHandle({entry})")))?;
    let handle = open(&file)
        .await
        .map_err(failed("createSyncAccessHandle"))?;
    let bytes = read_all(&handle);
    handle.close();
    bytes
}

/// Fails unless the entry `entry` of `dir` holds `bytes`.
pub(crate) async fn expect_holds(
    dir: &FileSystemDirectoryHandle,
    entry: &str,
    bytes: &[u8],
) -> Result<(), JsError> {
    let found = get(dir, entry).await?;
    check(found == bytes, || {
        format!(
            "{entry} holds {:?}, not {:?}",
            String::from_utf8_lossy(&found),
            String::from_utf8_lossy(bytes)
        )
    })
}

/// A sync access handle of the browser on the entry `entry` of `dir`, which another one cannot
/// take while it is open.
pub(crate) async fn hold(
    dir: &FileSystemDirectoryHandle,
    entry: &str,
) -> Result<web_sys::FileSystemSyncAccessHandle, JsError> {
    let file = file_handle(dir, entry, false)
        .await
        .map_err(failed("getFileHandle"))?;
    open(&file).await.map_err(failed("createSyncAccessHandle"))
}

pub(crate) fn check(holds: bool, what: impl FnOnce() -> String) -> Result<(), JsError> {
    if holds {
        Ok(())
    } else {
        Err(JsError::new(&what()))
    }
}

/// Makes an error of the layer the failure of `what`.
pub(crate) fn layer(what: &str) -> impl FnOnce(io::Error) -> JsError + '_ {
    move |err| JsError::new(&format!("{what}: {err}"))
}

/// The error of `result`, an operation that has to fail, of the kind `kind`.
pub(crate) fn expect_error<T>(
    result: io::Result<T>,
    kind: io::ErrorKind,
    what: &str,
) -> Result<io::Error, JsError> {
    let err = match result {
        Ok(_) => return Err(JsError::new(&format!("{what} succeeded"))),
        Err(err) => err,
    };
    check(err.kind() == kind, || {
        format!("{what} failed with {err} ({:?}), not {kind:?}", err.kind())
    })?;
    Ok(err)
}

/// The error of `result`, which has to be the error of a taken file.
pub(crate) fn expect_taken<T>(result: io::Result<T>, what: &str) -> Result<(), JsError> {
    let err = expect_error(result, io::ErrorKind::ResourceBusy, what)?;
    check(FileTaken::is(&err), || {
        format!("{what} failed with {err}, which is not the error of a taken file")
    })
}

/// Each operation on the directory, and the generations it leaves; then the entries of names with
/// several generations, as a cut leaves them, which `files_after_reload` reads.
#[wasm_bindgen]
pub async fn files_write(run: String) -> Result<(), JsError> {
    let files = OpfsFiles;
    let data = base(&run).join("data");
    let path = |name: &str| data.join(name);
    files
        .create_dir_all(&data.join("deep"))
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(&run, "data").await?;
    expect_entries(&dir, &["deep"], "create_dir_all").await?;
    let listed = files.list(&data, "").await.map_err(layer("list"))?;
    check(listed == [path("deep")], || format!("list gave {listed:?}"))?;

    put(&dir, "a", b"a1").await?;
    put(&dir, "b", b"b1").await?;
    let exists = |name: &'static str| {
        let path = path(name);
        async move { files.exists(&path).await.map_err(layer("exists")) }
    };
    check(exists("a").await? && exists("b").await?, || {
        "a and b exist".into()
    })?;
    check(!exists("c").await?, || "c does not exist".into())?;
    let elsewhere = base(&run).join("nowhere").join("c");
    let found = files.exists(&elsewhere).await.map_err(layer("exists"))?;
    check(!found, || {
        "a file in a missing directory does not exist".into()
    })?;
    files.sync_dir(&data).await.map_err(layer("sync_dir"))?;

    // To a free name, the file moves to the name itself.
    files
        .rename(&path("a"), &path("c"))
        .await
        .map_err(layer("rename a c"))?;
    expect_entries(&dir, &["b", "c", "deep"], "renaming a to c").await?;
    expect_holds(&dir, "c", b"a1").await?;

    // Onto a name that exists, to its next generation, never onto it; the old one goes.
    put(&dir, "d", b"d1").await?;
    files
        .rename(&path("d"), &path("b"))
        .await
        .map_err(layer("rename d b"))?;
    expect_entries(&dir, &["b~1", "c", "deep"], "renaming d onto b").await?;
    expect_holds(&dir, "b~1", b"d1").await?;
    put(&dir, "e", b"e1").await?;
    files
        .rename(&path("e"), &path("b"))
        .await
        .map_err(layer("rename e b"))?;
    expect_entries(&dir, &["b~2", "c", "deep"], "renaming e onto b").await?;
    expect_holds(&dir, "b~2", b"e1").await?;
    check(exists("b").await? && !exists("e").await?, || {
        "b exists, and e does not".into()
    })?;

    files.remove(&path("c")).await.map_err(layer("remove c"))?;
    expect_entries(&dir, &["b~2", "deep"], "removing c").await?;

    // To another directory, onto a name with two generations there, from one with two here.
    let deep = self::dir(&run, "data/deep").await?;
    for (dir, entry, bytes) in [
        (&dir, "x", "x0"),
        (&dir, "x~1", "x1"),
        (&deep, "x", "deep x0"),
        (&deep, "x~2", "deep x2"),
    ] {
        put(dir, entry, bytes.as_bytes()).await?;
    }
    files
        .rename(&path("x"), &data.join("deep").join("x"))
        .await
        .map_err(layer("rename x deep/x"))?;
    expect_entries(&dir, &["b~2", "deep"], "renaming x to deep/x").await?;
    expect_entries(&deep, &["x~3"], "renaming x to deep/x").await?;
    expect_holds(&deep, "x~3", b"x1").await?;

    // Two generations of g and three of h, as cuts in the middle of replacements leave them, and
    // a name that starts with another.
    for (entry, bytes) in [
        ("g", "g0"),
        ("g~3", "g3"),
        ("h", "h0"),
        ("h~1", "h1"),
        ("h~2", "h2"),
        ("p.wal", "wal"),
        ("p.wal.compacting~1", "compacting"),
    ] {
        put(&dir, entry, bytes.as_bytes()).await?;
    }
    expect_listing(
        &files,
        &data,
        "",
        &["b", "deep", "g", "h", "p.wal", "p.wal.compacting"],
    )
    .await?;
    expect_listing(&files, &data, "p.wal", &["p.wal", "p.wal.compacting"]).await?;
    expect_listing(&files, &data, "h", &["h"]).await
}

/// Fails unless `list(dir, prefix)` gives the paths of `names`, in that order.
async fn expect_listing(
    files: &OpfsFiles,
    dir: &Path,
    prefix: &str,
    names: &[&str],
) -> Result<(), JsError> {
    let listed = files.list(dir, prefix).await.map_err(layer("list"))?;
    let expected: Vec<PathBuf> = names.iter().map(|name| dir.join(name)).collect();
    check(listed == expected, || {
        format!("list({prefix:?}) gave {listed:?}, not {expected:?}")
    })
}

/// After the page reloaded: what `files_write` left is there, and each name holds its newest
/// generation, which a rename moves, removing the old ones of its source; a removal removes every
/// generation of exactly its name.
#[wasm_bindgen]
pub async fn files_after_reload(run: String) -> Result<(), JsError> {
    let files = OpfsFiles;
    let data = base(&run).join("data");
    let path = |name: &str| data.join(name);
    let dir = dir(&run, "data").await?;
    let left = [
        "b~2",
        "deep",
        "g",
        "g~3",
        "h",
        "h~1",
        "h~2",
        "p.wal",
        "p.wal.compacting~1",
    ];
    expect_entries(&dir, &left, "the reload").await?;
    expect_holds(&dir, "b~2", b"e1").await?;
    let deep = self::dir(&run, "data/deep").await?;
    expect_entries(&deep, &["x~3"], "the reload").await?;
    expect_holds(&deep, "x~3", b"x1").await?;
    for (name, expected) in [
        ("b", true),
        ("g", true),
        ("h", true),
        ("a", false),
        ("c", false),
    ] {
        let found = files.exists(&path(name)).await.map_err(layer("exists"))?;
        check(found == expected, || {
            format!("after the reload, exists({name}) is {found}")
        })?;
    }
    expect_listing(
        &files,
        &data,
        "",
        &["b", "deep", "g", "h", "p.wal", "p.wal.compacting"],
    )
    .await?;

    files
        .rename(&path("g"), &path("k"))
        .await
        .map_err(layer("rename g k"))?;
    let left = [
        "b~2",
        "deep",
        "h",
        "h~1",
        "h~2",
        "k",
        "p.wal",
        "p.wal.compacting~1",
    ];
    expect_entries(&dir, &left, "renaming g, with two generations, to k").await?;
    expect_holds(&dir, "k", b"g3").await?;

    files
        .rename(&path("h"), &path("b"))
        .await
        .map_err(layer("rename h b"))?;
    let left = ["b~3", "deep", "k", "p.wal", "p.wal.compacting~1"];
    expect_entries(&dir, &left, "renaming h, with three generations, onto b").await?;
    expect_holds(&dir, "b~3", b"h2").await?;

    files
        .remove(&path("p.wal"))
        .await
        .map_err(layer("remove p.wal"))?;
    let left = ["b~3", "deep", "k", "p.wal.compacting~1"];
    expect_entries(&dir, &left, "removing p.wal").await?;
    let wal = files
        .exists(&path("p.wal"))
        .await
        .map_err(layer("exists"))?;
    let compacting = files
        .exists(&path("p.wal.compacting"))
        .await
        .map_err(layer("exists"))?;
    check(!wal && compacting, || {
        format!("after removing p.wal, it exists: {wal}, and p.wal.compacting: {compacting}")
    })?;

    put(&dir, "k~5", b"k5").await?;
    files.remove(&path("k")).await.map_err(layer("remove k"))?;
    expect_entries(&dir, &["b~3", "deep", "p.wal.compacting~1"], "removing k").await
}

/// With a handle of the browser open on an entry: a rename or a removal that has to move or
/// remove it fails with the error of a taken file, and changes no value; one that only has to
/// remove the old generation of its destination succeeds, and leaves it.
#[wasm_bindgen]
pub async fn files_taken(run: String) -> Result<(), JsError> {
    let files = OpfsFiles;
    let taken = base(&run).join("taken");
    let path = |name: &str| taken.join(name);
    files
        .create_dir_all(&taken)
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(&run, "taken").await?;
    put(&dir, "s", b"s").await?;
    put(&dir, "t", b"t").await?;

    let held = hold(&dir, "s").await?;
    expect_taken(
        files.rename(&path("s"), &path("t")).await,
        "rename s t, with s open",
    )?;
    expect_taken(files.remove(&path("s")).await, "remove s, with s open")?;
    let bytes = read_all(&held);
    held.close();
    check(bytes? == b"s", || "s keeps what it held".into())?;
    expect_entries(&dir, &["s", "t"], "the operations that failed").await?;
    expect_holds(&dir, "t", b"t").await?;

    // The destination open: the move goes to a free entry, and the old one, open, stays.
    let held = hold(&dir, "t").await?;
    let renamed = files.rename(&path("s"), &path("t")).await;
    held.close();
    renamed.map_err(layer("rename s t, with t open"))?;
    expect_entries(&dir, &["t", "t~1"], "renaming s onto t, open").await?;
    expect_holds(&dir, "t~1", b"s").await?;

    // An old generation of the source open: the rename fails, and no value changes.
    put(&dir, "u", b"u0").await?;
    put(&dir, "u~1", b"u1").await?;
    let held = hold(&dir, "u").await?;
    let renamed = files.rename(&path("u"), &path("v")).await;
    let removed = files.remove(&path("u")).await;
    held.close();
    expect_taken(renamed, "rename u v, with the old generation of u open")?;
    // The removal goes from the oldest generation, so it fails at once.
    expect_taken(removed, "remove u, with its oldest generation open")?;
    expect_entries(
        &dir,
        &["t", "t~1", "u", "u~1"],
        "the operations that failed",
    )
    .await?;

    // The newest open: the removal removes the older ones first, and the name keeps its value.
    let held = hold(&dir, "u~1").await?;
    let removed = files.remove(&path("u")).await;
    let bytes = read_all(&held);
    held.close();
    expect_taken(removed, "remove u, with its newest generation open")?;
    check(bytes? == b"u1", || "u~1 keeps what it held".into())?;
    expect_entries(&dir, &["t", "t~1", "u~1"], "the removal that failed at u~1").await?;
    let found = files.exists(&path("u")).await.map_err(layer("exists"))?;
    check(found, || "u still exists".into())?;

    // Once closed, the next removals remove them, the old generation of t included.
    files.remove(&path("u")).await.map_err(layer("remove u"))?;
    files.remove(&path("t")).await.map_err(layer("remove t"))?;
    expect_entries(&dir, &[], "removing u and t").await
}

/// A path that is absolute, has `..`, has a name with `~` or `\`, or is that of a file and ends in
/// `/` or `.`, is refused, as is a listing for a prefix with `~`, and nothing is touched: neither
/// the root of the OPFS nor the directory of the run.
#[wasm_bindgen]
pub async fn files_refused(run: String) -> Result<(), JsError> {
    let files = OpfsFiles;
    let base = base(&run);
    let refused = base.join("refused");
    files
        .create_dir_all(&refused)
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(&run, "refused").await?;
    put(&dir, "r", b"r0").await?;
    put(&dir, "r~1", b"r1").await?;
    let root = JsFuture::from(crate::js::scope()?.navigator().storage().get_directory())
        .await
        .map_err(failed("getDirectory"))?
        .unchecked_into();
    let before = entries(&root).await?;

    let invalid = io::ErrorKind::InvalidInput;
    let made = refused.join("made").join("..").join("other");
    expect_error(
        files.create_dir_all(&made).await,
        invalid,
        "create_dir_all with ..",
    )?;
    let absolute = PathBuf::from(format!("/files-{run}"));
    expect_error(
        files.create_dir_all(&absolute).await,
        invalid,
        "create_dir_all /",
    )?;
    let marked = refused.join("x~1");
    expect_error(
        files.create_dir_all(&marked).await,
        invalid,
        "create_dir_all with ~",
    )?;
    let outside = refused.join("..").join("..").join("r");
    expect_error(
        files.rename(&refused.join("r"), &outside).await,
        invalid,
        "rename to ..",
    )?;
    expect_error(
        files.rename(&outside, &refused.join("s")).await,
        invalid,
        "rename from ..",
    )?;
    expect_error(
        files.rename(&refused.join("r~1"), &refused.join("s")).await,
        invalid,
        "rename r~1",
    )?;
    expect_error(files.remove(&absolute).await, invalid, "remove /")?;
    expect_error(
        files.remove(&refused.join("r~1")).await,
        invalid,
        "remove r~1",
    )?;
    expect_error(files.exists(&outside).await, invalid, "exists with ..")?;
    expect_error(files.list(Path::new(".."), "").await, invalid, "list ..")?;
    expect_error(files.list(&refused, "r~").await, invalid, "list r~")?;
    expect_error(files.sync_dir(&absolute).await, invalid, "sync_dir /")?;

    // A file path that ends in `/` or in `.` names no file: not the directory, nor the file.
    files
        .create_dir_all(&refused.join("sub"))
        .await
        .map_err(layer("create_dir_all"))?;
    let sub = PathBuf::from(format!("{}/sub/", refused.display()));
    expect_error(files.remove(&sub).await, invalid, "remove sub/")?;
    for path in [
        format!("{}/r/", refused.display()),
        format!("{}/r/.", refused.display()),
    ] {
        let what = format!("exists {path}");
        expect_error(files.exists(Path::new(&path)).await, invalid, &what)?;
    }
    // A name that a browser does not take, refused before its directory is made.
    let backslash = refused.join("n").join("b\\c");
    expect_error(
        files.create_dir_all(&backslash).await,
        invalid,
        "create_dir_all with \\",
    )?;

    let after = entries(&root).await?;
    check(after == before, || {
        format!("the root of the OPFS had {before:?}, and has {after:?}")
    })?;
    expect_entries(&dir, &["r", "r~1", "sub"], "the refused operations").await
}

/// An operation dropped by its caller once it started runs to its end, and the next operation
/// on its file waits for it; one dropped before its turn never runs.
#[wasm_bindgen]
pub async fn files_dropped(run: String) -> Result<(), JsError> {
    let files = OpfsFiles;
    let dropped = base(&run).join("dropped");
    let path = |name: &str| dropped.join(name);
    files
        .create_dir_all(&dropped)
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(&run, "dropped").await?;
    for (entry, bytes) in [("a", "a0"), ("a~1", "a1"), ("a~2", "a2")] {
        put(&dir, entry, bytes.as_bytes()).await?;
    }

    // Polled once and dropped, with its promises in flight.
    let rename = files.rename(&path("a"), &path("b")).now_or_never();
    check(rename.is_none(), || {
        "the rename ended before its first promise".into()
    })?;
    let found = files.exists(&path("b")).await.map_err(layer("exists b"))?;
    check(found, || {
        "the next operation on b did not wait for the dropped rename".into()
    })?;
    expect_entries(&dir, &["b"], "the dropped rename").await?;
    expect_holds(&dir, "b", b"a2").await?;

    // The removal of c waits for the rename to c, its caller drops it, and it never runs.
    let rename = files.rename(&path("b"), &path("c")).now_or_never();
    let remove = files.remove(&path("c")).now_or_never();
    check(rename.is_none() && remove.is_none(), || {
        "the rename and the removal ended before their first promise".into()
    })?;
    let found = files.exists(&path("c")).await.map_err(layer("exists c"))?;
    check(found, || "the removal dropped before its turn ran".into())?;
    expect_entries(&dir, &["c"], "the dropped removal").await?;

    // The next operation waits also for the last steps of a dropped one: after the move, the
    // removals of the 200 old generations of its destination.
    for generation in 0..200 {
        file_handle(&dir, &generation_entry("d", generation), true)
            .await
            .map_err(failed("getFileHandle"))?;
    }
    let rename = files.rename(&path("c"), &path("d")).now_or_never();
    check(rename.is_none(), || {
        "the rename onto d ended before its first promise".into()
    })?;
    files.exists(&path("c")).await.map_err(layer("exists c"))?;
    expect_entries(&dir, &["d~200"], "the dropped rename onto d").await?;
    expect_holds(&dir, "d~200", b"a2").await
}

/// The entry of the generation `generation` of `name`, as the layer names it.
fn generation_entry(name: &str, generation: u32) -> String {
    if generation == 0 {
        name.to_owned()
    } else {
        format!("{name}~{generation}")
    }
}

/// What is missing is `NotFound`, with the name of the error of the browser when it is the one
/// that says so, and with nothing touched; a removal skips a generation that is gone; and
/// `sync_dir` does not make an earlier change durable.
#[wasm_bindgen]
pub async fn files_errors(run: String) -> Result<(), JsError> {
    let files = OpfsFiles;
    let errors = base(&run).join("errors");
    let path = |name: &str| errors.join(name);
    files
        .create_dir_all(&errors)
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(&run, "errors").await?;
    put(&dir, "x", b"x0").await?;
    put(&dir, "x~1", b"x1").await?;

    let missing = io::ErrorKind::NotFound;
    expect_error(
        files.rename(&path("y"), &path("z")).await,
        missing,
        "rename y",
    )?;
    expect_error(files.remove(&path("y")).await, missing, "remove y")?;
    let nowhere = errors.join("nowhere");
    let err = expect_error(files.list(&nowhere, "").await, missing, "list nowhere")?;
    check(err.to_string().contains("NotFoundError"), || {
        format!("the error of the listing does not name the error of the browser: {err}")
    })?;
    let err = expect_error(
        files.rename(&path("x"), &nowhere.join("x")).await,
        missing,
        "rename x to a missing directory",
    )?;
    check(err.to_string().contains("NotFoundError"), || {
        format!("the error of the rename does not name the error of the browser: {err}")
    })?;
    // The directory of the destination is found first: nothing was touched, not even the old
    // generation of x.
    expect_entries(&dir, &["x", "x~1"], "the rename that failed").await?;
    expect_holds(&dir, "x~1", b"x1").await?;

    // A generation that someone else removes while the removal is under way, as the removal of
    // a Worker that ended may land: the removal goes on with the newer ones, and removes them.
    for generation in 0..300 {
        file_handle(&dir, &generation_entry("v", generation), true)
            .await
            .map_err(failed("getFileHandle"))?;
    }
    let v = path("v");
    let removal = std::pin::pin!(files.remove(&v));
    let meanwhile = std::pin::pin!(async {
        // Once `v` is gone, the removal listed its generations and removes them, from the oldest.
        while file_handle(&dir, "v", false).await.is_ok() {}
        JsFuture::from(dir.remove_entry("v~250"))
            .await
            .map_err(failed(
                "removeEntry(v~250), before the removal of the layer reached it",
            ))
    });
    // A removal that ends first, before the step removed `v~250`, fails the step at once.
    match futures::future::select(removal, meanwhile).await {
        Either::Left((removed, _)) => {
            return Err(JsError::new(&format!(
                "the removal of v ended, with {removed:?}, before the step removed v~250"
            )));
        }
        Either::Right((meanwhile, removal)) => {
            meanwhile?;
            let removed = removal.await;
            removed.map_err(layer("remove v, with v~250 removed in the middle"))?;
        }
    }
    expect_entries(&dir, &["x", "x~1"], "the removal of v").await?;

    // An error is not a missing file: a file where a directory goes is one.
    put(&dir, "f", b"f").await?;
    match files.exists(&path("f").join("y")).await {
        Err(err) if err.to_string().contains("TypeMismatchError") => {}
        other => {
            let what = format!("exists below a file gave {other:?}, not a TypeMismatchError");
            return Err(JsError::new(&what));
        }
    }

    check(!files.sync_dir_covers_earlier_changes(), || {
        "sync_dir does nothing on the OPFS".into()
    })
}
