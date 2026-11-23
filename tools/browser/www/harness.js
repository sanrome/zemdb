// The page of the harness (README.md): runs the suites that the URL names (`?suites=a,b`; the
// defaults of suites.js without it), with the run id of the URL (`?run=`) or a new one. It keeps
// its progress in sessionStorage across its reloads, and leaves the result in the page: in the
// list of steps, in `data-status` of the body (`pass` or `fail`) and in `window.harnessResult`,
// which the runner reads. A run stops at its first failing step.
import { defaults, suites } from './suites.js';

const STORAGE_KEY = 'zemdb-harness';
// The longest a step may take; the runner has its own limit for the whole run.
const STEP_TIMEOUT_MS = 60_000;
const PEER_OPEN_TIMEOUT_MS = 10_000;
// This load of the page, the name of its Workers: a step tells by it in which page it runs.
const PAGE = `first page ${crypto.randomUUID()}`;

let worker = null;
let peer = null;
let peerReady = null;
let nextId = 1;
const pending = new Map();

window.harnessProgress = { current: null };

window.addEventListener('message', (event) => {
  if (event.origin !== location.origin || peer === null || event.source !== peer) {
    return;
  }
  if (event.data.ready) {
    peerReady?.();
  } else {
    settle(event.data, peer);
  }
});

main().catch((error) => finish(load() ?? fresh(), `the harness failed: ${describe(error)}`));

async function main() {
  let state = load();
  if (state === null || state.url !== location.href) {
    state = fresh();
  }
  const unknown = state.suites.filter((name) => !(name in suites));
  if (unknown.length > 0) {
    return finish(state, `there are no suites ${unknown.join(', ')}`);
  }
  render(state);
  while (state.suite < state.suites.length) {
    const name = state.suites[state.suite];
    const actions = suites[name];
    if (state.action >= actions.length) {
      state.suite += 1;
      state.action = 0;
      continue;
    }
    const action = actions[state.action];
    // A reload goes on with the next action.
    state.action += 1;
    save(state);
    if (action.reload) {
      location.reload();
      return;
    } else if (action.terminate) {
      worker?.terminate();
      worker = null;
    } else if (action.closePeer) {
      peer?.close();
      peer = null;
    } else {
      const step = action.run ?? action.peer;
      const label = `${name}/${step}`;
      window.harnessProgress.current = label;
      render(state, label);
      const started = performance.now();
      let error = null;
      try {
        const target = action.run ? ensureWorker() : await ensurePeer();
        await call(target, step, state.run);
      } catch (failure) {
        error = describe(failure);
      }
      const ms = Math.round(performance.now() - started);
      state.results.push({ step: label, page: action.run ? 'first' : 'second', ok: error === null, error, ms });
      save(state);
      if (error !== null) {
        break;
      }
    }
  }
  finish(state);
}

function fresh() {
  const params = new URLSearchParams(location.search);
  const names = params.get('suites')?.split(',').filter(Boolean) ?? defaults;
  return {
    url: location.href,
    run: params.get('run') ?? randomId(),
    suites: names,
    suite: 0,
    action: 0,
    results: [],
  };
}

function randomId() {
  const bytes = crypto.getRandomValues(new Uint8Array(8));
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

function load() {
  try {
    return JSON.parse(sessionStorage.getItem(STORAGE_KEY));
  } catch {
    return null;
  }
}

function save(state) {
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function ensureWorker() {
  if (worker !== null) {
    return worker;
  }
  worker = new Worker('worker.js', { type: 'module', name: PAGE });
  worker.onmessage = ({ data }) => settle(data, worker);
  // An error that no step caught (a failed allocation, say): the page terminates the Worker,
  // and its steps in flight fail.
  worker.onerror = (event) => {
    worker?.terminate();
    worker = null;
    for (const [id, call] of pending) {
      settle({ id, ok: false, error: `the Worker failed: ${event.message}` }, call.target);
    }
  };
  return worker;
}

async function ensurePeer() {
  if (peer !== null && !peer.closed) {
    return peer;
  }
  const ready = new Promise((resolve, reject) => {
    peerReady = resolve;
    setTimeout(
      () => reject(new Error('the second page did not open: does the browser block pop-ups?')),
      PEER_OPEN_TIMEOUT_MS,
    );
  });
  peer = window.open('peer.html', 'zemdb-harness-peer');
  if (peer === null) {
    throw new Error('the browser blocked the second page, a pop-up');
  }
  await ready;
  return peer;
}

function call(target, step, run) {
  return new Promise((resolve, reject) => {
    const id = nextId;
    nextId += 1;
    pending.set(id, { target, resolve, reject });
    if (target === peer) {
      target.postMessage({ id, step, run }, location.origin);
    } else {
      target.postMessage({ id, step, run });
    }
    setTimeout(
      () => settle({ id, ok: false, error: `timed out after ${STEP_TIMEOUT_MS / 1000} s` }, target),
      STEP_TIMEOUT_MS,
    );
  });
}

// The answer `id`, from `source`: the Worker of the page, or the second page. A step of the
// second page answered by the Worker of this one, or the other way around, fails.
function settle({ id, ok, error }, source) {
  const call = pending.get(id);
  if (call === undefined) {
    return;
  }
  pending.delete(id);
  if (source !== call.target) {
    call.reject(new Error('the step was answered by another page than the one that ran it'));
  } else if (ok) {
    call.resolve();
  } else {
    call.reject(new Error(error));
  }
}

function finish(state, harnessError = null) {
  sessionStorage.removeItem(STORAGE_KEY);
  worker?.terminate();
  peer?.close();
  const failed = state.results.find((result) => !result.ok)
    ?? (harnessError === null ? null : { step: 'harness', error: harnessError });
  const result = {
    status: failed === null ? 'pass' : 'fail',
    run: state.run,
    suites: state.suites,
    results: state.results,
    failed,
  };
  window.harnessResult = result;
  document.body.dataset.status = result.status;
  render(state, null, result);
}

function render(state, current = null, result = null) {
  const list = document.getElementById('steps');
  list.replaceChildren(
    ...state.results.map((step) => {
      const item = document.createElement('li');
      item.className = step.ok ? 'ok' : 'fail';
      const where = step.page === 'second' ? ', in the second page' : '';
      item.textContent = `${step.ok ? 'ok' : 'FAIL'} ${step.step} (${step.ms} ms${where})`
        + (step.ok ? '' : `: ${step.error}`);
      return item;
    }),
  );
  const status = document.getElementById('status');
  if (result === null) {
    status.textContent = `Running ${state.suites.join(', ')}${current === null ? '' : `: ${current}`}…`;
  } else if (result.status === 'pass') {
    status.className = 'ok';
    status.textContent = `PASS: ${state.results.length} steps of ${state.suites.join(', ')} (run ${state.run}).`;
  } else {
    status.className = 'fail';
    status.textContent = `FAIL at ${result.failed.step}: ${result.failed.error}`;
  }
}

function describe(error) {
  return error instanceof Error ? error.message : String(error);
}
