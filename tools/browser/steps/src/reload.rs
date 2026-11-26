//! The suites `reload` and `isolation`: the OPFS keeps what a step wrote across a reload of the
//! page, and another context of the browser does not see it.

use crate::js::{dom_name, failed, page};
use crate::opfs::{file_handle, harness_dir, open, read_all, write_all};
use wasm_bindgen::prelude::*;

/// The id of the run, and the load of the page that wrote it, on two lines.
const FILE: &str = "reload.bin";

/// Writes the id of the run and the load of the page to `harness/reload.bin`, which
/// `reload_read` reads after the page reloads.
#[wasm_bindgen]
pub async fn reload_write(run: String) -> Result<(), JsError> {
    let contents = format!("{run}\n{}", page()?);
    let file = file_handle(&harness_dir().await?, FILE, true)
        .await
        .map_err(failed("getFileHandle"))?;
    let handle = open(&file)
        .await
        .map_err(failed("createSyncAccessHandle"))?;
    let written = write_all(&handle, contents.as_bytes());
    handle.close();
    written
}

/// After the page reloaded: `harness/reload.bin` has the id of the run, which `reload_write`
/// wrote in another load of the page.
#[wasm_bindgen]
pub async fn reload_read(run: String) -> Result<(), JsError> {
    let file = file_handle(&harness_dir().await?, FILE, false)
        .await
        .map_err(failed("after the reload, getFileHandle"))?;
    let (written_run, writer) = read_contents(&file).await?;
    if written_run != run {
        return Err(JsError::new(
            "after the reload, harness/reload.bin does not have what this run wrote",
        ));
    }
    if writer == page()? {
        return Err(JsError::new(
            "the page did not reload: the load of the page that wrote harness/reload.bin reads it",
        ));
    }
    Ok(())
}

/// In another context of the browser, which the runner opens: `harness/reload.bin`, if there,
/// does not have the id of the run.
#[wasm_bindgen]
pub async fn reload_isolated(run: String) -> Result<(), JsError> {
    let file = match file_handle(&harness_dir().await?, FILE, false).await {
        Ok(file) => file,
        Err(error) if dom_name(&error).as_deref() == Some("NotFoundError") => return Ok(()),
        Err(error) => return Err(failed("getFileHandle")(error)),
    };
    let (written_run, _) = read_contents(&file).await?;
    if written_run == run {
        return Err(JsError::new(
            "another context of the browser sees what this run wrote in its OPFS",
        ));
    }
    Ok(())
}

/// The run and the page that `reload_write` wrote to `file`.
async fn read_contents(file: &web_sys::FileSystemFileHandle) -> Result<(String, String), JsError> {
    let handle = open(file).await.map_err(failed("createSyncAccessHandle"))?;
    let contents = read_all(&handle);
    handle.close();
    let contents = String::from_utf8_lossy(&contents?).into_owned();
    let (run, page) = contents.split_once('\n').unwrap_or((&contents, ""));
    Ok((run.to_owned(), page.to_owned()))
}
