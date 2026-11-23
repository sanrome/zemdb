//! The suite `tabs`: a second page of the same context shares the OPFS, and terminating the
//! Worker of the first page releases its handles.

use crate::js::{dom_name, failed, now, page, sleep};
use crate::opfs::{file_handle, harness_dir, open, read_all, write_all};
use std::cell::RefCell;
use wasm_bindgen::prelude::*;
use web_sys::FileSystemSyncAccessHandle;

const FILE: &str = "tabs.bin";
/// The load of the page that holds [`FILE`].
const HOLDER: &str = "tabs.holder";

/// How long `tabs_free` waits for the browser to release the handle of a terminated Worker,
/// which it does on its own time, in milliseconds.
const RELEASE_WAIT_MS: f64 = 5000.0;

thread_local! {
    /// The handle that `tabs_hold` keeps open until its Worker ends.
    static HELD: RefCell<Option<FileSystemSyncAccessHandle>> = const { RefCell::new(None) };
}

/// In the first page: writes the id of the run to `harness/tabs.bin` and keeps the file open,
/// until its Worker ends, and writes the load of the page to `harness/tabs.holder`.
#[wasm_bindgen]
pub async fn tabs_hold(run: String) -> Result<(), JsError> {
    let dir = harness_dir().await?;
    let holder = file_handle(&dir, HOLDER, true)
        .await
        .map_err(failed("getFileHandle"))?;
    let handle = open(&holder)
        .await
        .map_err(failed("createSyncAccessHandle"))?;
    let written = write_all(&handle, page()?.as_bytes());
    handle.close();
    written?;
    let file = file_handle(&dir, FILE, true)
        .await
        .map_err(failed("getFileHandle"))?;
    let handle = open(&file)
        .await
        .map_err(failed("createSyncAccessHandle"))?;
    write_all(&handle, run.as_bytes())?;
    if let Some(previous) = HELD.with(|held| held.replace(Some(handle))) {
        previous.close();
    }
    Ok(())
}

/// In the second page, while the first one holds `harness/tabs.bin`: the file is there (the
/// OPFS is the same), and opening it fails.
#[wasm_bindgen]
pub async fn tabs_taken(_run: String) -> Result<(), JsError> {
    let dir = harness_dir().await?;
    let holder = file_handle(&dir, HOLDER, false)
        .await
        .map_err(failed("the second page does not see harness/tabs.holder"))?;
    let handle = open(&holder)
        .await
        .map_err(failed("createSyncAccessHandle"))?;
    let holder = read_all(&handle);
    handle.close();
    if holder? == page()?.as_bytes() {
        return Err(JsError::new(
            "tabs_taken runs in the page that holds harness/tabs.bin, not in a second page",
        ));
    }
    let file = file_handle(&dir, FILE, false)
        .await
        .map_err(failed("the second page does not see harness/tabs.bin"))?;
    match open(&file).await {
        Ok(handle) => {
            handle.close();
            Err(JsError::new(
                "the second page opened harness/tabs.bin while the first one held it",
            ))
        }
        Err(error) if dom_name(&error).as_deref() == Some("NotFoundError") => Err(failed(
            "the second page does not see harness/tabs.bin",
        )(error)),
        Err(_) => Ok(()),
    }
}

/// In the second page, after the first one terminated its Worker: `harness/tabs.bin` opens,
/// and has the id of the run.
#[wasm_bindgen]
pub async fn tabs_free(run: String) -> Result<(), JsError> {
    let file = file_handle(&harness_dir().await?, FILE, false)
        .await
        .map_err(failed("getFileHandle"))?;
    let deadline = now()? + RELEASE_WAIT_MS;
    let handle = loop {
        match open(&file).await {
            Ok(handle) => break handle,
            Err(_) if now()? < deadline => sleep(50).await?,
            Err(error) => return Err(failed(
                "harness/tabs.bin is still taken 5 s after the first page terminated its Worker",
            )(error)),
        }
    };
    let contents = read_all(&handle);
    handle.close();
    if contents? != run.as_bytes() {
        return Err(JsError::new(
            "harness/tabs.bin does not have what the first page wrote",
        ));
    }
    Ok(())
}
