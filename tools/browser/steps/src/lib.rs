//! The steps of the browser harness (`tools/browser/README.md`).
//!
//! Compiled to wasm32 with `wasm-bindgen`, for the Worker of the page (`www/worker.js`). Each
//! exported `async` function is a step: it takes the id of the run, which the page keeps across
//! its reloads, and fails with an error that says what failed. The suites, in `www/suites.js`,
//! say in which order the page runs the steps, where it reloads, which steps run in a second
//! page and when it terminates its Worker. The unit tests (`#[cfg(test)]`) run in a dedicated
//! Worker, with `wasm-bindgen-test-runner`.
#![cfg(target_arch = "wasm32")]

mod js;
mod opfs;
mod reload;
mod server;
mod tabs;

use wasm_bindgen::prelude::*;

#[cfg(not(test))]
mod panic {
    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    extern "C" {
        /// Defined by the Worker of the page (`www/worker.js`).
        #[wasm_bindgen(js_name = harnessPanic)]
        fn harness_panic(message: &str);
    }

    /// A panic gives its message to the Worker, which reports it as the error of the step. After
    /// a panic the instance is unusable, so the page ends the run there.
    #[wasm_bindgen(start)]
    pub fn start() {
        std::panic::set_hook(Box::new(|info| harness_panic(&info.to_string())));
    }
}

/// Fails on purpose: with it, the runner checks that a failing step fails the run, with the name
/// of the step (`run.mjs --self-check`).
#[wasm_bindgen]
pub async fn fails_on_purpose(_run: String) -> Result<(), JsError> {
    Err(JsError::new("this step fails on purpose"))
}

/// Panics on purpose: with it, the runner checks that a step that panics fails the run with the
/// message of the panic (`run.mjs --self-check`).
#[wasm_bindgen]
pub async fn panics_on_purpose(_run: String) -> Result<(), JsError> {
    panic!("this step panics on purpose");
}

#[cfg(test)]
#[path = "tests/lib.rs"]
mod tests;
