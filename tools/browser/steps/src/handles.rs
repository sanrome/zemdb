//! The suite `handles`: the files of the file layer of the storage over the OPFS
//! (`zemdb_storage::files::OpfsFiles`), on the real layer, under `harness/files-{run}/`:
//! - its openings, each on the newest generation of its name;
//! - reading and writing at an offset, the size, truncating and flushing, also after the page
//!   reloads;
//! - renaming an open file, which its handle follows, also onto a name that exists;
//! - a file that another handle has, also one of the Worker of a second page;
//! - operations dropped by their callers;
//! - and, in a context of its own whose quota the runner makes small, a write over the quota.
//!
//! Each step fails if it does not end within [`LIMIT_MS`], so that an operation that hangs fails
//! it there.

use crate::files::{
    base, check, dir, expect_entries, expect_error, expect_holds, expect_taken, hold, layer, put,
};
use crate::js::{now, page, sleep, within};
use futures::FutureExt;
use std::cell::RefCell;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use wasm_bindgen::prelude::*;
use zemdb_storage::files::{OpfsFile, OpfsFiles, RoomFile, RoomFiles};

/// The longest a step of the suite may take.
const LIMIT_MS: i32 = 20_000;

/// How long `handles_free` waits for the file that the first page closed, which the browser
/// releases on its own time, in milliseconds.
const RELEASE_WAIT_MS: f64 = 5000.0;

thread_local! {
    /// The file that `handles_hold` keeps open, through the layer, until `handles_release`.
    static HELD: RefCell<Option<OpfsFile>> = const { RefCell::new(None) };
}

/// `path` opened with `open_locked`, which no other handle has.
async fn open(files: &OpfsFiles, path: &Path) -> Result<OpfsFile, JsError> {
    let what = format!("open_locked {}", path.display());
    let file = files.open_locked(path).await.map_err(layer(&what))?;
    file.ok_or_else(|| JsError::new(&format!("{what} gave None, with no other handle on it")))
}

async fn expect_size(file: &OpfsFile, size: u64) -> Result<(), JsError> {
    let found = file.size().await.map_err(layer("size"))?;
    check(found == size, || format!("the size is {found}, not {size}"))
}

/// Fails unless `read_at(offset, bytes.len())` gives `bytes`.
async fn expect_read(file: &OpfsFile, offset: u64, bytes: &[u8]) -> Result<(), JsError> {
    let what = format!("read_at({offset}, {})", bytes.len());
    let found = file
        .read_at(offset, bytes.len())
        .await
        .map_err(layer(&what))?;
    check(found == bytes, || {
        format!(
            "{what} gave {:?}, not {:?}",
            String::from_utf8_lossy(&found),
            String::from_utf8_lossy(bytes)
        )
    })
}

/// What `handles_write` leaves in `w`: `HELLO` at 0 and `WOR` at 4096, with zeros between.
fn written() -> Vec<u8> {
    let mut bytes = b"HELLO".to_vec();
    bytes.resize(4096, 0);
    bytes.extend_from_slice(b"WOR");
    bytes
}

/// Reading and writing at an offset, the size, truncating and extending, and flushing, on a file
/// that `open_locked` creates; a read past the end is `UnexpectedEof`, never a short read; and
/// `create` empties a file. `handles_after_reload` reads what they leave.
#[wasm_bindgen]
pub async fn handles_write(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "handles_write", write(&run)).await
}

async fn write(run: &str) -> Result<(), JsError> {
    let files = OpfsFiles;
    let io_dir = base(run).join("open-io");
    files
        .create_dir_all(&io_dir)
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(run, "open-io").await?;
    let file = open(&files, &io_dir.join("w")).await?;
    expect_entries(&dir, &["w"], "open_locked w").await?;
    file.write_at(0, b"HELLO".to_vec())
        .await
        .map_err(layer("write_at(0)"))?;
    file.write_at(4096, b"WORLD".to_vec())
        .await
        .map_err(layer("write_at(4096)"))?;
    expect_size(&file, 4101).await?;
    expect_read(&file, 0, b"HELLO").await?;
    expect_read(&file, 5, &[0; 4091]).await?;
    expect_read(&file, 4097, b"ORLD").await?;
    expect_read(&file, 4101, b"").await?;
    for (offset, len) in [(4099, 5), (4101, 1), (1 << 20, 1)] {
        let what = format!("read_at({offset}, {len}), past the end");
        expect_error(
            file.read_at(offset, len).await,
            ErrorKind::UnexpectedEof,
            &what,
        )?;
    }

    file.set_len(4099).await.map_err(layer("set_len(4099)"))?;
    expect_size(&file, 4099).await?;
    expect_read(&file, 4096, b"WOR").await?;
    let what = "read_at(4096, 5), after the truncation";
    expect_error(file.read_at(4096, 5).await, ErrorKind::UnexpectedEof, what)?;
    file.set_len(5000).await.map_err(layer("set_len(5000)"))?;
    expect_size(&file, 5000).await?;
    expect_read(&file, 4099, &[0; 901]).await?;

    // A write of a few hundred KiB, read back exactly.
    let large: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    file.write_at(8192, large.clone())
        .await
        .map_err(layer("write_at(8192)"))?;
    expect_size(&file, 8192 + 300_000).await?;
    expect_read(&file, 8192, &large).await?;
    file.set_len(4099).await.map_err(layer("set_len(4099)"))?;
    file.sync_data().await.map_err(layer("sync_data"))?;
    file.sync_all().await.map_err(layer("sync_all"))?;
    drop(file);
    // Closed, so the browser opens it: what the layer wrote is in `w`, and nowhere else.
    expect_holds(&dir, "w", &written()).await?;
    expect_entries(&dir, &["w"], "closing w").await?;

    put(&dir, "c", b"what c held").await?;
    let created = files
        .create(&io_dir.join("c"))
        .await
        .map_err(layer("create c"))?;
    expect_size(&created, 0).await?;
    created
        .write_at(0, b"new".to_vec())
        .await
        .map_err(layer("write_at"))?;
    created.sync_all().await.map_err(layer("sync_all"))?;
    drop(created);
    expect_holds(&dir, "c", b"new").await
}

/// Renaming an open file, as the rotation of a WAL and its rollback do: the handle follows the
/// file to a free name, back, onto a name that exists (to its next generation, removing the old
/// ones), to another directory and from a name with an old generation (which goes). A rename
/// whose first step fails (an old generation of the source open), to a missing directory or to a
/// refused path, fails before it closes the file, which goes on where it was.
/// `handles_after_reload` reads what they leave.
#[wasm_bindgen]
pub async fn handles_rename(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "handles_rename", rename(&run)).await
}

async fn rename(run: &str) -> Result<(), JsError> {
    let files = OpfsFiles;
    let base = base(run).join("open-rename");
    let path = |name: &str| base.join(name);
    files
        .create_dir_all(&base.join("deep"))
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(run, "open-rename").await?;
    let deep = self::dir(run, "open-rename/deep").await?;

    let mut wal = open(&files, &path("wal")).await?;
    wal.write_at(0, b"w1".to_vec())
        .await
        .map_err(layer("write_at"))?;
    wal.rename(&path("wal.compacting"))
        .await
        .map_err(layer("rename wal to wal.compacting"))?;
    expect_entries(&dir, &["deep", "wal.compacting"], "renaming the open wal").await?;
    wal.write_at(2, b"w2".to_vec())
        .await
        .map_err(layer("write_at, after the rename"))?;
    expect_read(&wal, 0, b"w1w2").await?;
    wal.rename(&path("wal"))
        .await
        .map_err(layer("rename wal.compacting back to wal"))?;
    expect_entries(&dir, &["deep", "wal"], "renaming it back").await?;
    expect_read(&wal, 0, b"w1w2").await?;

    put(&dir, "snap", b"s0").await?;
    put(&dir, "snap~1", b"s1").await?;
    wal.rename(&path("snap"))
        .await
        .map_err(layer("rename wal onto snap"))?;
    expect_entries(&dir, &["deep", "snap~2"], "renaming the open wal onto snap").await?;
    wal.write_at(4, b"w3".to_vec())
        .await
        .map_err(layer("write_at, after the rename onto snap"))?;
    expect_read(&wal, 0, b"w1w2w3").await?;

    put(&deep, "x", b"x0").await?;
    wal.rename(&base.join("deep").join("x"))
        .await
        .map_err(layer("rename snap to deep/x"))?;
    expect_entries(&dir, &["deep"], "renaming the open snap to deep/x").await?;
    expect_entries(&deep, &["x~1"], "renaming the open snap to deep/x").await?;
    expect_read(&wal, 0, b"w1w2w3").await?;

    // From a name with old generations, onto one that exists; then, from where it went, with an
    // old generation there that a replacement could not remove, which the next one removes.
    put(&dir, "q", b"q0").await?;
    put(&dir, "q~1", b"q1").await?;
    put(&dir, "q~2", b"q2").await?;
    put(&dir, "r", b"r0").await?;
    let mut q = open(&files, &path("q")).await?;
    expect_read(&q, 0, b"q2").await?;
    q.rename(&path("r")).await.map_err(layer("rename q to r"))?;
    let what = "renaming the open q, with old generations, onto r";
    expect_entries(&dir, &["deep", "r~1"], what).await?;
    put(&dir, "r", b"left").await?;
    q.rename(&path("s")).await.map_err(layer("rename r to s"))?;
    let what = "renaming the open r, with an old generation left, to s";
    expect_entries(&dir, &["deep", "s"], what).await?;
    expect_read(&q, 0, b"q2").await?;

    put(&dir, "u", b"u0").await?;
    put(&dir, "u~1", b"u1").await?;
    let mut u = open(&files, &path("u")).await?;
    let held = hold(&dir, "u").await?;
    let renamed = u.rename(&path("v")).await;
    held.close();
    expect_taken(
        renamed,
        "rename the open u to v, with its old generation open",
    )?;
    let nowhere = base.join("nowhere").join("v");
    let what = "rename the open u to a missing directory";
    expect_error(u.rename(&nowhere).await, ErrorKind::NotFound, what)?;
    let outside = base.join("..").join("v");
    let what = "rename the open u to ..";
    expect_error(u.rename(&outside).await, ErrorKind::InvalidInput, what)?;
    let left = ["deep", "s", "u", "u~1"];
    expect_entries(&dir, &left, "the renames of u that failed").await?;
    u.write_at(2, b"!".to_vec())
        .await
        .map_err(layer("write_at, after the renames that failed"))?;
    expect_read(&u, 0, b"u1!").await?;
    u.rename(&path("v")).await.map_err(layer("rename u to v"))?;
    expect_entries(&dir, &["deep", "s", "v"], "renaming the open u to v").await?;

    drop((wal, q, u));
    expect_holds(&deep, "x~1", b"w1w2w3").await?;
    expect_holds(&dir, "s", b"q2").await?;
    expect_holds(&dir, "v", b"u1!").await
}

/// After the page reloaded: what `handles_write` and `handles_rename` left is there, and the
/// openings read it.
#[wasm_bindgen]
pub async fn handles_after_reload(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "handles_after_reload", after_reload(&run)).await
}

async fn after_reload(run: &str) -> Result<(), JsError> {
    let files = OpfsFiles;
    let io_dir = base(run).join("open-io");
    let read = files
        .open_read(&io_dir.join("w"))
        .await
        .map_err(layer("open_read w"))?;
    expect_size(&read, 4099).await?;
    expect_read(&read, 0, &written()).await?;
    drop(read);
    let created = files
        .open_read(&io_dir.join("c"))
        .await
        .map_err(layer("open_read c"))?;
    expect_size(&created, 3).await?;
    expect_read(&created, 0, b"new").await?;
    drop(created);
    let file = open(&files, &io_dir.join("w")).await?;
    file.write_at(4099, b"LD".to_vec())
        .await
        .map_err(layer("write_at, after the reload"))?;
    expect_read(&file, 4096, b"WORLD").await?;
    drop(file);

    let rename = base(run).join("open-rename");
    expect_entries(
        &dir(run, "open-rename").await?,
        &["deep", "s", "v"],
        "the reload",
    )
    .await?;
    let deep = dir(run, "open-rename/deep").await?;
    expect_entries(&deep, &["x~1"], "the reload").await?;
    for (name, bytes) in [("deep/x", "w1w2w3"), ("s", "q2"), ("v", "u1!")] {
        let file = files
            .open_read(&rename.join(name))
            .await
            .map_err(layer(&format!("open_read {name}")))?;
        expect_read(&file, 0, bytes.as_bytes()).await?;
    }
    Ok(())
}

/// Each opening opens the newest generation of its name; `create` empties it, and
/// `create_locked` is `AlreadyExists` with any generation, and truncates nothing. A name with no
/// generation is `NotFound` to `open_read`, and a missing directory to every opening; a refused
/// path touches nothing.
#[wasm_bindgen]
pub async fn handles_generations(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "handles_generations", generations(&run)).await
}

async fn generations(run: &str) -> Result<(), JsError> {
    let files = OpfsFiles;
    let base = base(run).join("open-gen");
    let path = |name: &str| base.join(name);
    files
        .create_dir_all(&base)
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(run, "open-gen").await?;
    put(&dir, "n", b"n0").await?;
    put(&dir, "n~2", b"n2").await?;

    let read = files
        .open_read(&path("n"))
        .await
        .map_err(layer("open_read n"))?;
    expect_read(&read, 0, b"n2").await?;
    drop(read);
    let locked = open(&files, &path("n")).await?;
    expect_read(&locked, 0, b"n2").await?;
    locked
        .write_at(0, b"N".to_vec())
        .await
        .map_err(layer("write_at"))?;
    drop(locked);
    expect_holds(&dir, "n~2", b"N2").await?;
    let created = files.create(&path("n")).await.map_err(layer("create n"))?;
    expect_size(&created, 0).await?;
    created
        .write_at(0, b"c".to_vec())
        .await
        .map_err(layer("write_at"))?;
    drop(created);
    expect_holds(&dir, "n~2", b"c").await?;
    expect_holds(&dir, "n", b"n0").await?;

    let exists = ErrorKind::AlreadyExists;
    expect_error(
        files.create_locked(&path("n")).await,
        exists,
        "create_locked n",
    )?;
    put(&dir, "m~3", b"m3").await?;
    let what = "create_locked m, with only m~3";
    expect_error(files.create_locked(&path("m")).await, exists, what)?;
    let left = ["m~3", "n", "n~2"];
    expect_entries(&dir, &left, "the create_locked that failed").await?;
    expect_holds(&dir, "n~2", b"c").await?;
    expect_holds(&dir, "m~3", b"m3").await?;
    let m = open(&files, &path("m")).await?;
    expect_read(&m, 0, b"m3").await?;
    drop(m);
    let fresh = files
        .create_locked(&path("f"))
        .await
        .map_err(layer("create_locked f"))?
        .ok_or_else(|| JsError::new("create_locked f gave None, with no other handle"))?;
    expect_size(&fresh, 0).await?;
    drop(fresh);
    let left = ["f", "m~3", "n", "n~2"];
    expect_entries(&dir, &left, "create_locked f").await?;

    let missing = ErrorKind::NotFound;
    expect_error(files.open_read(&path("g")).await, missing, "open_read g")?;
    let nowhere = base.join("nowhere").join("x");
    expect_error(
        files.open_locked(&nowhere).await,
        missing,
        "open_locked nowhere/x",
    )?;
    expect_error(
        files.create_locked(&nowhere).await,
        missing,
        "create_locked nowhere/x",
    )?;
    expect_error(files.create(&nowhere).await, missing, "create nowhere/x")?;
    expect_error(
        files.open_read(&nowhere).await,
        missing,
        "open_read nowhere/x",
    )?;

    let refused = [
        path("n~2"),
        base.join("..").join("n"),
        PathBuf::from(format!("{}/", base.display())),
    ];
    for path in refused {
        let invalid = ErrorKind::InvalidInput;
        let what = |opening: &str| format!("{opening} {}", path.display());
        expect_error(
            files.open_locked(&path).await,
            invalid,
            &what("open_locked"),
        )?;
        expect_error(
            files.create_locked(&path).await,
            invalid,
            &what("create_locked"),
        )?;
        expect_error(files.create(&path).await, invalid, &what("create"))?;
        expect_error(files.open_read(&path).await, invalid, &what("open_read"))?;
    }
    expect_entries(&dir, &left, "the refused openings").await?;
    expect_holds(&dir, "n~2", b"c").await
}

/// With another handle on `name` of `base`: `open_locked` is `None`, `create_locked` is
/// `AlreadyExists`, and `open_read` and `create` are the error of a taken file.
async fn taken_by_another(files: &OpfsFiles, base: &Path, name: &str) -> Result<(), JsError> {
    let path = base.join(name);
    let opened = files
        .open_locked(&path)
        .await
        .map_err(layer(&format!("open_locked {name}")))?;
    check(opened.is_none(), || {
        format!("open_locked {name} opened it, with another handle on it")
    })?;
    let what = format!("create_locked {name}, with another handle on it");
    expect_error(
        files.create_locked(&path).await,
        ErrorKind::AlreadyExists,
        &what,
    )?;
    let what = format!("open_read {name}, with another handle on it");
    expect_taken(files.open_read(&path).await, &what)?;
    let what = format!("create {name}, with another handle on it");
    expect_taken(files.create(&path).await, &what)
}

/// In the first page: opens `t` through the layer, writes in it, and keeps it open until
/// `handles_release`, with the load of the page in `t.holder`. With `t` open, and with a handle of
/// the browser on `s`, the openings of the layer find them taken, and change neither.
#[wasm_bindgen]
pub async fn handles_hold(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "handles_hold", hold_file(&run)).await
}

async fn hold_file(run: &str) -> Result<(), JsError> {
    let files = OpfsFiles;
    let base = base(run).join("open-taken");
    files
        .create_dir_all(&base)
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(run, "open-taken").await?;
    put(&dir, "t.holder", page()?.as_bytes()).await?;
    let file = open(&files, &base.join("t")).await?;
    file.write_at(0, b"held".to_vec())
        .await
        .map_err(layer("write_at"))?;
    file.sync_all().await.map_err(layer("sync_all"))?;
    taken_by_another(&files, &base, "t").await?;
    expect_read(&file, 0, b"held").await?;

    put(&dir, "s", b"s").await?;
    let held = hold(&dir, "s").await?;
    let taken = taken_by_another(&files, &base, "s").await;
    held.close();
    taken?;
    expect_holds(&dir, "s", b"s").await?;
    HELD.with(|held| held.replace(Some(file)));
    Ok(())
}

/// In the second page, while the first one has `t` open through the layer: the openings of the
/// layer, from the Worker of this page, find it taken.
#[wasm_bindgen]
pub async fn handles_taken(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "handles_taken", taken_in_peer(&run)).await
}

async fn taken_in_peer(run: &str) -> Result<(), JsError> {
    let files = OpfsFiles;
    let base = base(run).join("open-taken");
    let holder = files
        .open_read(&base.join("t.holder"))
        .await
        .map_err(layer("open_read t.holder"))?;
    let size = holder.size().await.map_err(layer("size"))?;
    let holder = holder
        .read_at(0, size as usize)
        .await
        .map_err(layer("read_at"))?;
    check(holder != page()?.as_bytes(), || {
        "handles_taken runs in the page that has t open, not in a second page".into()
    })?;
    taken_by_another(&files, &base, "t").await
}

/// In the first page: closes `t`, which `handles_hold` kept open.
#[wasm_bindgen]
pub async fn handles_release(_run: String) -> Result<(), JsError> {
    let held = HELD.with(|held| held.take());
    check(held.is_some(), || {
        "handles_hold did not run in this Worker".into()
    })
}

/// In the second page, once the first one closed `t`: it opens, with what the first one wrote.
#[wasm_bindgen]
pub async fn handles_free(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "handles_free", free_in_peer(&run)).await
}

async fn free_in_peer(run: &str) -> Result<(), JsError> {
    let files = OpfsFiles;
    let path = base(run).join("open-taken").join("t");
    let deadline = now()? + RELEASE_WAIT_MS;
    let file = loop {
        match files
            .open_locked(&path)
            .await
            .map_err(layer("open_locked t"))?
        {
            Some(file) => break file,
            None if now()? < deadline => sleep(50).await?,
            None => {
                return Err(JsError::new(
                    "t is still taken 5 s after the first page closed it",
                ))
            }
        }
    };
    expect_read(&file, 0, b"held").await?;
    expect_size(&file, 4).await
}

/// Operations dropped by their callers once they started end: an opening, which closes the file
/// it opened; the rename of an open file, which the next operation on the file waits for, and
/// whose error, if it fails, that operation gives instead of running; and the rename of a file
/// that is dropped too, which closes the file once it ends.
#[wasm_bindgen]
pub async fn handles_dropped(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "handles_dropped", dropped(&run)).await
}

async fn dropped(run: &str) -> Result<(), JsError> {
    let files = OpfsFiles;
    let base = base(run).join("open-dropped");
    let path = |name: &str| base.join(name);
    files
        .create_dir_all(&base)
        .await
        .map_err(layer("create_dir_all"))?;
    let dir = dir(run, "open-dropped").await?;
    let started =
        |done: bool, what: &str| check(!done, || format!("{what} ended before its first promise"));

    // Each next opening waits for the dropped one, which closed its file.
    started(
        files.open_locked(&path("a")).now_or_never().is_some(),
        "open_locked a",
    )?;
    let a = files
        .open_locked(&path("a"))
        .await
        .map_err(layer("open_locked a"))?;
    check(a.is_some(), || {
        "the dropped open_locked of a did not close it".into()
    })?;
    drop(a);
    put(&dir, "a", b"a0").await?;
    started(
        files.create(&path("a")).now_or_never().is_some(),
        "create a",
    )?;
    let a = files
        .open_read(&path("a"))
        .await
        .map_err(layer("open_read a, after the dropped create"))?;
    expect_size(&a, 0).await?;
    drop(a);
    started(
        files.open_read(&path("a")).now_or_never().is_some(),
        "open_read a",
    )?;
    drop(open(&files, &path("a")).await?);
    started(
        files.create_locked(&path("b")).now_or_never().is_some(),
        "create_locked b",
    )?;
    let mut b = open(&files, &path("b")).await?;
    expect_entries(&dir, &["a", "b"], "the dropped openings").await?;

    b.write_at(0, b"b0".to_vec())
        .await
        .map_err(layer("write_at"))?;
    started(b.rename(&path("c")).now_or_never().is_some(), "rename b c")?;
    b.write_at(2, b"!".to_vec())
        .await
        .map_err(layer("write_at, after the dropped rename"))?;
    expect_entries(&dir, &["a", "c"], "the dropped rename of b").await?;
    expect_read(&b, 0, b"b0!").await?;

    put(&dir, "d", b"d0").await?;
    put(&dir, "d~1", b"d1").await?;
    let mut d = open(&files, &path("d")).await?;
    let held = hold(&dir, "d").await?;
    started(d.rename(&path("e")).now_or_never().is_some(), "rename d e")?;
    let written = d.write_at(0, b"zz".to_vec()).await;
    held.close();
    expect_taken(written, "write_at after a dropped rename of d that failed")?;
    expect_read(&d, 0, b"d1").await?;
    // Dropped once it sent its error, which its caller did not take: the next operation gives it.
    let held = hold(&dir, "d").await?;
    let e = path("e");
    let mut rename = Box::pin(d.rename(&e));
    started(rename.as_mut().now_or_never().is_some(), "rename d e")?;
    // Its turn comes after the rename, which sends its error before it ends.
    files.exists(&path("d")).await.map_err(layer("exists d"))?;
    drop(rename);
    let written = d.write_at(0, b"zz".to_vec()).await;
    held.close();
    let what = "write_at after a rename of d that failed, dropped once it sent its error";
    expect_taken(written, what)?;
    expect_read(&d, 0, b"d1").await?;
    expect_entries(&dir, &["a", "c", "d", "d~1"], "the dropped renames of d").await?;
    drop(d);

    started(b.rename(&path("g")).now_or_never().is_some(), "rename c g")?;
    drop(b);
    let g = open(&files, &path("g")).await?;
    expect_read(&g, 0, b"b0!").await?;
    drop(g);
    expect_entries(&dir, &["a", "d", "d~1", "g"], "the dropped rename of c").await
}

/// In a context whose quota the runner made small: a write over the quota fails with
/// `StorageFull`, from the `QuotaExceededError` of the browser, and the file goes on.
#[wasm_bindgen]
pub async fn handles_quota(run: String) -> Result<(), JsError> {
    within(LIMIT_MS, "handles_quota", quota(&run)).await
}

async fn quota(run: &str) -> Result<(), JsError> {
    let files = OpfsFiles;
    let base = base(run).join("quota");
    files
        .create_dir_all(&base)
        .await
        .map_err(layer("create_dir_all"))?;
    let file = open(&files, &base.join("q")).await?;
    file.write_at(0, vec![1; 1024])
        .await
        .map_err(layer("write_at, under the quota"))?;
    let what = "write_at of 16 MiB, over the quota";
    let err = expect_error(
        file.write_at(1024, vec![2; 16 << 20]).await,
        ErrorKind::StorageFull,
        what,
    )?;
    check(err.to_string().contains("QuotaExceededError"), || {
        format!("{what} failed with {err}, which does not name the error of the browser")
    })?;
    file.sync_all().await.map_err(layer("sync_all"))?;
    expect_read(&file, 0, &[1; 1024]).await
}
