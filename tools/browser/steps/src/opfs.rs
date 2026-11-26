//! The files of the harness in the OPFS of the origin, under `harness/`.

use crate::js::{failed, scope};
use wasm_bindgen::{JsCast, JsError, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemReadWriteOptions, FileSystemSyncAccessHandle,
};

/// The largest file the harness reads: its files take a few bytes.
const MAX_FILE: f64 = 1024.0 * 1024.0;

/// The directory of the harness, `harness/` in the OPFS of the origin, created if missing.
pub async fn harness_dir() -> Result<FileSystemDirectoryHandle, JsError> {
    let storage = scope()?.navigator().storage();
    let root = JsFuture::from(storage.get_directory())
        .await
        .map_err(failed("navigator.storage.getDirectory"))?;
    let root: FileSystemDirectoryHandle = root.unchecked_into();
    let options = FileSystemGetDirectoryOptions::new();
    options.set_create(true);
    let dir = JsFuture::from(root.get_directory_handle_with_options("harness", &options))
        .await
        .map_err(failed("getDirectoryHandle(harness)"))?;
    Ok(dir.unchecked_into())
}

/// The file `name` of `dir`, created if missing when `create`; otherwise a missing file is the
/// error of the browser (`NotFoundError`).
pub async fn file_handle(
    dir: &FileSystemDirectoryHandle,
    name: &str,
    create: bool,
) -> Result<FileSystemFileHandle, JsValue> {
    let options = FileSystemGetFileOptions::new();
    options.set_create(create);
    let file = JsFuture::from(dir.get_file_handle_with_options(name, &options)).await?;
    Ok(file.unchecked_into())
}

/// A sync access handle on `file`; if another handle has the file, the error of the browser.
pub async fn open(file: &FileSystemFileHandle) -> Result<FileSystemSyncAccessHandle, JsValue> {
    let handle = JsFuture::from(file.create_sync_access_handle()).await?;
    Ok(handle.unchecked_into())
}

/// Replaces the contents of the file of `handle` with `bytes`, and flushes it.
pub fn write_all(handle: &FileSystemSyncAccessHandle, bytes: &[u8]) -> Result<(), JsError> {
    handle.truncate_with_u32(0).map_err(failed("truncate"))?;
    let options = FileSystemReadWriteOptions::new();
    options.set_at(0.0);
    let written = handle
        .write_with_u8_array_and_options(bytes, &options)
        .map_err(failed("write"))?;
    if written != bytes.len() as f64 {
        return Err(JsError::new(&format!(
            "write wrote {written} of {} bytes",
            bytes.len()
        )));
    }
    handle.flush().map_err(failed("flush"))
}

/// The contents of the file of `handle`.
pub fn read_all(handle: &FileSystemSyncAccessHandle) -> Result<Vec<u8>, JsError> {
    let size = handle.get_size().map_err(failed("getSize"))?;
    if size > MAX_FILE {
        return Err(JsError::new(&format!(
            "a file of the harness of {size} bytes"
        )));
    }
    let mut bytes = vec![0; size as usize];
    let options = FileSystemReadWriteOptions::new();
    options.set_at(0.0);
    let read = handle
        .read_with_u8_array_and_options(&mut bytes, &options)
        .map_err(failed("read"))?;
    if read != size {
        return Err(JsError::new(&format!("read gave {read} of {size} bytes")));
    }
    Ok(bytes)
}
