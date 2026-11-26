use crate::opfs::{file_handle, harness_dir, open, read_all, write_all};
use wasm_bindgen::{JsCast, JsError, JsValue};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

fn ok<T>(result: Result<T, JsError>) -> T {
    result.map_err(JsValue::from).unwrap()
}

/// The unit tests run in a dedicated Worker, as the steps do, with the OPFS: what a handle
/// writes there, another one reads back.
#[wasm_bindgen_test]
async fn run_in_a_dedicated_worker_with_the_opfs() {
    assert!(js_sys::global().is_instance_of::<web_sys::DedicatedWorkerGlobalScope>());
    let dir = ok(harness_dir().await);
    let file = file_handle(&dir, "unit.bin", true).await.unwrap();
    let handle = open(&file).await.unwrap();
    ok(write_all(&handle, b"unit test"));
    handle.close();
    let handle = open(&file).await.unwrap();
    assert_eq!(ok(read_all(&handle)), b"unit test");
    handle.close();
}
