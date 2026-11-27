//! What the steps take from JavaScript: the scope of the Worker, errors, timers and the clock.

use js_sys::Promise;
use wasm_bindgen::{JsCast, JsError, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{DedicatedWorkerGlobalScope, DomException, WorkerGlobalScope};

/// The global scope of the Worker the steps run in.
pub fn scope() -> Result<WorkerGlobalScope, JsError> {
    js_sys::global()
        .dyn_into()
        .map_err(|_| JsError::new("the steps run in a Worker"))
}

/// The load of the page that started the Worker: the name of the Worker, which each page gives
/// it, a random id of each of its loads (`www/harness.js` and `www/peer.js`).
pub fn page() -> Result<String, JsError> {
    let scope: DedicatedWorkerGlobalScope = js_sys::global()
        .dyn_into()
        .map_err(|_| JsError::new("the steps run in a dedicated Worker"))?;
    let name = scope.name();
    if name.is_empty() {
        return Err(JsError::new(
            "the Worker has no name: the page names it after its load",
        ));
    }
    Ok(name)
}

/// The name and the message of a JavaScript error.
pub fn describe(error: &JsValue) -> String {
    if let Some(error) = error.dyn_ref::<DomException>() {
        return format!("{}: {}", error.name(), error.message());
    }
    if let Some(error) = error.dyn_ref::<js_sys::Error>() {
        return format!(
            "{}: {}",
            String::from(error.name()),
            String::from(error.message())
        );
    }
    format!("{error:?}")
}

/// Makes `error` the failure of `what`.
pub fn failed(what: &str) -> impl FnOnce(JsValue) -> JsError + '_ {
    move |error| JsError::new(&format!("{what}: {}", describe(&error)))
}

/// The name of a `DOMException`; `None` for any other value.
pub fn dom_name(error: &JsValue) -> Option<String> {
    error.dyn_ref::<DomException>().map(DomException::name)
}

/// A promise that resolves to `value` after `ms` milliseconds.
pub fn timer(ms: i32, value: JsValue) -> Result<Promise, JsError> {
    let scope = scope()?;
    let mut set = Ok(());
    let promise = Promise::new(&mut |resolve, _reject| {
        set = scope
            .set_timeout_with_callback_and_timeout_and_arguments_1(&resolve, ms, &value)
            .map(drop);
    });
    set.map_err(failed("setTimeout"))?;
    Ok(promise)
}

/// Sleeps for `ms` milliseconds.
pub async fn sleep(ms: i32) -> Result<(), JsError> {
    JsFuture::from(timer(ms, JsValue::UNDEFINED)?)
        .await
        .map_err(failed("setTimeout"))?;
    Ok(())
}

/// `step`, failed if it does not end within `ms` milliseconds: an operation that hangs fails its
/// step there, saying which step, and not only at the limit of the page for a step.
pub async fn within<T>(
    ms: i32,
    what: &str,
    step: impl std::future::Future<Output = Result<T, JsError>>,
) -> Result<T, JsError> {
    let limit = JsFuture::from(timer(ms, JsValue::UNDEFINED)?);
    match futures::future::select(std::pin::pin!(step), limit).await {
        futures::future::Either::Left((done, _)) => done,
        futures::future::Either::Right(_) => Err(JsError::new(&format!(
            "{what} did not end within {ms} ms: an operation hangs"
        ))),
    }
}

/// The milliseconds since the Worker started (`performance.now()`).
pub fn now() -> Result<f64, JsError> {
    let performance = scope()?
        .performance()
        .ok_or_else(|| JsError::new("the Worker has no performance"))?;
    Ok(performance.now())
}
