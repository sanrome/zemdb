// The Worker of a page of the harness: runs the steps of the steps crate (`steps/`), compiled
// to wasm32 and bound with wasm-bindgen into `pkg/`. Each message names a step and gives the
// run id; the answer carries the id of the message, and the error of the step if it failed.
import init, * as steps from './pkg/zemdb_browser_steps.js';

let panic = null;
// The panic hook of the steps crate gives the message of a panic here, before the trap.
self.harnessPanic = (message) => {
  panic = message;
};

// The steps in flight. A panic does not reject the promise of its step: its trap is an uncaught
// error of the Worker, in a microtask of wasm-bindgen-futures. That error fails the steps in
// flight, with the message of the panic; after a panic the instance is unusable, and the page
// ends the run there.
const inFlight = new Set();

function failInFlight(event, error) {
  if (inFlight.size === 0) {
    return;
  }
  for (const id of inFlight) {
    self.postMessage({ id, ok: false, error: panic ?? error });
  }
  inFlight.clear();
  // Reported as the failure of its steps, and not again as an error of the Worker.
  event.preventDefault();
}

self.addEventListener('error', (event) => failInFlight(event, `uncaught ${event.message}`));
self.addEventListener('unhandledrejection', (event) => {
  failInFlight(event, `unhandled rejection: ${describe(event.reason)}`);
});

const ready = init();

self.onmessage = async ({ data: { id, step, run } }) => {
  inFlight.add(id);
  try {
    await ready;
    const run_step = /^[a-z][a-z0-9_]*$/.test(step) ? steps[step] : undefined;
    if (typeof run_step !== 'function') {
      throw new Error(`there is no step ${step}`);
    }
    await run_step(run);
    self.postMessage({ id, ok: true });
  } catch (error) {
    self.postMessage({ id, ok: false, error: panic ?? describe(error) });
  } finally {
    inFlight.delete(id);
  }
};

function describe(error) {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
