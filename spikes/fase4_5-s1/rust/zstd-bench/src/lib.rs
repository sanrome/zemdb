//! Plain `extern "C"` exports over `zemdb_storage::snapshot`, for a page that times them.
//!
//! The page writes its input into a buffer from `zb_alloc`, calls `zb_encode` or `zb_decode`,
//! and reads the result through `zb_result_ptr` and `zb_result_len`.

use std::cell::RefCell;
use zemdb_storage::snapshot::{decode_snapshot_envelope, encode_snapshot_envelope};

thread_local! {
    static RESULT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn input<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    // SAFETY: the page passes a buffer it got from `zb_alloc` with that length.
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

/// A buffer of `len` bytes, owned by the page until `zb_free`.
#[no_mangle]
pub extern "C" fn zb_alloc(len: usize) -> *mut u8 {
    let mut v = vec![0u8; len];
    let p = v.as_mut_ptr();
    std::mem::forget(v);
    p
}

#[no_mangle]
pub extern "C" fn zb_free(ptr: *mut u8, len: usize) {
    // SAFETY: `ptr` and `len` come from `zb_alloc`.
    drop(unsafe { Vec::from_raw_parts(ptr, len, len) });
}

/// `encode_snapshot_envelope(payload, true, 3)`: 0 on success, 1 on error.
#[no_mangle]
pub extern "C" fn zb_encode(ptr: *const u8, len: usize) -> u32 {
    match encode_snapshot_envelope(input(ptr, len), true, 3) {
        Ok(v) => {
            RESULT.with(|r| *r.borrow_mut() = v);
            0
        }
        Err(_) => 1,
    }
}

/// `decode_snapshot_envelope(envelope)`: 0 on success, 1 on error.
#[no_mangle]
pub extern "C" fn zb_decode(ptr: *const u8, len: usize) -> u32 {
    match decode_snapshot_envelope(input(ptr, len)) {
        Ok(v) => {
            RESULT.with(|r| *r.borrow_mut() = v);
            0
        }
        Err(_) => 1,
    }
}

#[no_mangle]
pub extern "C" fn zb_result_ptr() -> *const u8 {
    RESULT.with(|r| r.borrow().as_ptr())
}

#[no_mangle]
pub extern "C" fn zb_result_len() -> usize {
    RESULT.with(|r| r.borrow().len())
}

/// Frees the last result.
#[no_mangle]
pub extern "C" fn zb_clear() {
    RESULT.with(|r| *r.borrow_mut() = Vec::new());
}
