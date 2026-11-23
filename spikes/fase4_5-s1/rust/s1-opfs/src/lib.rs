//! Spike S1, question 1, from Rust: a `FileSystemSyncAccessHandle` of the OPFS, opened in a
//! dedicated Worker through `web-sys`; `write` and `read` at an offset, `flush`, `truncate`,
//! `getSize` and `close`, then reopening it (and, in `verify`, after the page reloaded).
//!
//! Every step is recorded in a JSON report, returned as a string to the worker's JavaScript.

use js_sys::Reflect;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemReadWriteOptions, FileSystemSyncAccessHandle,
    WorkerGlobalScope,
};

const BIG_LEN: usize = 6_100_000;

struct Report {
    steps: Vec<String>,
    ok: bool,
}

impl Report {
    fn new() -> Self {
        Report { steps: Vec::new(), ok: true }
    }
    fn step(&mut self, name: &str, ok: bool, detail: String) {
        if !ok {
            self.ok = false;
        }
        self.steps.push(format!(
            "{{\"step\":{},\"ok\":{},\"detail\":{}}}",
            json_str(name),
            ok,
            json_str(&detail)
        ));
    }
    fn finish(self, phase: &str) -> String {
        format!(
            "{{\"phase\":{},\"ok\":{},\"steps\":[{}]}}",
            json_str(phase),
            self.ok,
            self.steps.join(",")
        )
    }
}

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn err_text(e: &JsValue) -> String {
    let name = Reflect::get(e, &JsValue::from_str("name"))
        .ok()
        .and_then(|v| v.as_string())
        .unwrap_or_default();
    let msg = Reflect::get(e, &JsValue::from_str("message"))
        .ok()
        .and_then(|v| v.as_string())
        .unwrap_or_else(|| format!("{e:?}"));
    format!("{name}: {msg}")
}

fn now_ms() -> f64 {
    let global: WorkerGlobalScope = js_sys::global().unchecked_into();
    global.performance().map(|p| p.now()).unwrap_or(0.0)
}

async fn root_dir() -> Result<FileSystemDirectoryHandle, JsValue> {
    let global: WorkerGlobalScope = js_sys::global().dyn_into()?;
    let storage = global.navigator().storage();
    let dir = JsFuture::from(storage.get_directory()).await?;
    Ok(dir.unchecked_into())
}

async fn sub_dir(
    parent: &FileSystemDirectoryHandle,
    name: &str,
    create: bool,
) -> Result<FileSystemDirectoryHandle, JsValue> {
    let opts = FileSystemGetDirectoryOptions::new();
    opts.set_create(create);
    let d = JsFuture::from(parent.get_directory_handle_with_options(name, &opts)).await?;
    Ok(d.unchecked_into())
}

async fn file(
    dir: &FileSystemDirectoryHandle,
    name: &str,
    create: bool,
) -> Result<FileSystemFileHandle, JsValue> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(create);
    let f = JsFuture::from(dir.get_file_handle_with_options(name, &opts)).await?;
    Ok(f.unchecked_into())
}

async fn open(f: &FileSystemFileHandle) -> Result<FileSystemSyncAccessHandle, JsValue> {
    let h = JsFuture::from(f.create_sync_access_handle()).await?;
    Ok(h.unchecked_into())
}

fn at(offset: f64) -> FileSystemReadWriteOptions {
    let o = FileSystemReadWriteOptions::new();
    o.set_at(offset);
    o
}

fn read_at(h: &FileSystemSyncAccessHandle, offset: f64, len: usize) -> Result<Vec<u8>, JsValue> {
    let mut buf = vec![0u8; len];
    let n = h.read_with_u8_array_and_options(&mut buf, &at(offset))? as usize;
    buf.truncate(n);
    Ok(buf)
}

fn big_pattern() -> Vec<u8> {
    // xorshift, so the content is not compressible and a wrong offset shows.
    let mut x: u32 = 0x9E37_79B9;
    (0..BIG_LEN)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            (x >> 24) as u8
        })
        .collect()
}

/// The content the small file must hold after `write`: "HELLO", zeros up to 4096, then
/// "WOR" (the write of "WORLD" at 4096, truncated to 4099).
fn expected_small() -> Vec<u8> {
    let mut v = vec![0u8; 4099];
    v[..5].copy_from_slice(b"HELLO");
    v[4096..4099].copy_from_slice(b"WOR");
    v
}

macro_rules! check {
    ($rep:expr, $name:expr, $res:expr) => {
        match $res {
            Ok(v) => v,
            Err(e) => {
                $rep.step($name, false, err_text(&e));
                return Ok($rep.finish("aborted"));
            }
        }
    };
}

/// `phase` is "write" (create and exercise the files, then reopen them) or "verify" (after a
/// page reload: the files hold what "write" left). `dir` is the OPFS directory to use.
#[wasm_bindgen]
pub async fn s1q1(phase: String, dir: String) -> Result<String, JsValue> {
    let mut rep = Report::new();
    let root = check!(rep, "getDirectory", root_dir().await);
    let d = check!(rep, "getDirectoryHandle", sub_dir(&root, &dir, phase == "write").await);
    if phase == "write" {
        let fh = check!(rep, "getFileHandle(create)", file(&d, "small.bin", true).await);
        let h = check!(rep, "createSyncAccessHandle", open(&fh).await);
        check!(rep, "truncate(0)", h.truncate_with_f64(0.0));
        let n1 = check!(rep, "write(HELLO, at 0)", h.write_with_u8_array_and_options(b"HELLO", &at(0.0)));
        let n2 = check!(rep, "write(WORLD, at 4096)", h.write_with_u8_array_and_options(b"WORLD", &at(4096.0)));
        rep.step("write returns bytes written", n1 == 5.0 && n2 == 5.0, format!("{n1} {n2}"));
        let size = check!(rep, "getSize", h.get_size());
        rep.step("getSize after writes == 4101", size == 4101.0, format!("{size}"));
        let r = check!(rep, "read(at 4096)", read_at(&h, 4096.0, 5));
        rep.step("read at 4096 == WORLD", r == b"WORLD", format!("{:?}", String::from_utf8_lossy(&r)));
        let gap = check!(rep, "read(gap)", read_at(&h, 5.0, 4091));
        rep.step("gap 5..4096 reads zeros", gap.len() == 4091 && gap.iter().all(|b| *b == 0), format!("len {}", gap.len()));
        check!(rep, "flush", h.flush());
        check!(rep, "truncate(4099)", h.truncate_with_f64(4099.0));
        let size = check!(rep, "getSize", h.get_size());
        rep.step("getSize after truncate == 4099", size == 4099.0, format!("{size}"));
        let r = check!(rep, "read(at 4096, 5) after truncate", read_at(&h, 4096.0, 5));
        rep.step("short read at the end returns 3 bytes WOR", r == b"WOR", format!("{:?}", String::from_utf8_lossy(&r)));
        check!(rep, "flush", h.flush());
        h.close();
        rep.step("close", true, String::new());
        // Reopen, through the same FileSystemFileHandle and through a new one.
        let h = check!(rep, "reopen (same FileSystemFileHandle)", open(&fh).await);
        let size = check!(rep, "getSize", h.get_size());
        let all = check!(rep, "read all", read_at(&h, 0.0, 8192));
        rep.step("reopened content is the expected 4099 bytes", size == 4099.0 && all == expected_small(), format!("size {size}, read {}", all.len()));
        h.close();
        let fh2 = check!(rep, "getFileHandle (no create)", file(&d, "small.bin", false).await);
        let h = check!(rep, "reopen (new FileSystemFileHandle)", open(&fh2).await);
        let all = check!(rep, "read all", read_at(&h, 0.0, 8192));
        rep.step("content through a new FileSystemFileHandle", all == expected_small(), format!("read {}", all.len()));
        h.close();
        // A 6.1 MB file, as the original S1 did with a 100,000-row snapshot.
        let big = big_pattern();
        let fb = check!(rep, "getFileHandle(big, create)", file(&d, "big.bin", true).await);
        let h = check!(rep, "createSyncAccessHandle(big)", open(&fb).await);
        check!(rep, "truncate(0)", h.truncate_with_f64(0.0));
        let t0 = now_ms();
        let n = check!(rep, "write 6.1 MB", h.write_with_u8_array_and_options(&big, &at(0.0)));
        check!(rep, "flush", h.flush());
        let t1 = now_ms();
        let back = check!(rep, "read 6.1 MB", read_at(&h, 0.0, BIG_LEN + 16));
        let t2 = now_ms();
        rep.step(
            "6.1 MB write+flush and read",
            n as usize == BIG_LEN && back == big,
            format!("write+flush {:.1} ms, read {:.1} ms, {} bytes", t1 - t0, t2 - t1, back.len()),
        );
        h.close();
    } else {
        let fh = check!(rep, "getFileHandle (no create)", file(&d, "small.bin", false).await);
        let h = check!(rep, "createSyncAccessHandle", open(&fh).await);
        let size = check!(rep, "getSize", h.get_size());
        let all = check!(rep, "read all", read_at(&h, 0.0, 8192));
        rep.step("after reload: content is the expected 4099 bytes", size == 4099.0 && all == expected_small(), format!("size {size}, read {}", all.len()));
        h.close();
        let fb = check!(rep, "getFileHandle(big)", file(&d, "big.bin", false).await);
        let h = check!(rep, "createSyncAccessHandle(big)", open(&fb).await);
        let t0 = now_ms();
        let back = check!(rep, "read 6.1 MB", read_at(&h, 0.0, BIG_LEN + 16));
        let t1 = now_ms();
        rep.step("after reload: 6.1 MB file intact", back == big_pattern(), format!("read {:.1} ms, {} bytes", t1 - t0, back.len()));
        h.close();
    }
    Ok(rep.finish(&phase))
}
