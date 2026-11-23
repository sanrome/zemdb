// A second "tab" of a test: a page with its own dedicated worker, driven by the orchestrator
// through the bus. Opened as a tab (window.open with noopener), by the driver (Safari), or as
// an iframe where neither works.
import * as L from './lib.js';

const P = new URLSearchParams(location.search);
const TOKEN = P.get('token');
const NAME = P.get('name');
L.setLogContext(TOKEN, `${NAME}.page`);
const out = document.getElementById('out');

const alive = await fetch(`/alive?token=${TOKEN}`).then(r => r.json()).catch(() => ({ ok: false }));
if (!alive.ok) {
  out.textContent = 'stale context: its test is over';
} else {
  const kind = window.frameElement ? 'iframe' : (P.get('via') === 'driver' ? 'driver-tab' : 'popup-tab');
  const bus = new L.Bus(TOKEN, `${NAME}.page`);
  const worker = new Worker(`worker.js?token=${TOKEN}&name=${NAME}.w`, { type: 'module' });
  const hiddenSince = { t: document.visibilityState === 'hidden' ? Date.now() : null };
  document.addEventListener('visibilitychange', () => { hiddenSince.t = document.visibilityState === 'hidden' ? Date.now() : null; });
  const end = mode => {
    const t = Date.now();
    bus.emit('ending', { name: NAME, mode, t });
    if (mode === 'terminate') worker.terminate();
    else if (mode === 'closeTab') { if (window.frameElement) window.frameElement.remove(); else window.close(); }
    else if (mode === 'reload') location.reload();
  };
  worker.onmessage = e => {
    const d = e.data;
    if (d?.endNow) end(d.endNow);
    if (d?.tokenRequest) worker.postMessage({ tokenReply: true, id: d.id, t: Date.now(), vis: document.visibilityState, hiddenForMs: hiddenSince.t ? Date.now() - hiddenSince.t : 0 });
  };
  worker.onerror = e => { bus.emit('workerError', { name: NAME, message: e.message }); };
  bus.handlers = {
    end: async ({ mode }) => { setTimeout(() => end(mode), 0); return 'ending'; },
    info: async () => ({ kind, vis: document.visibilityState, hasFocus: document.hasFocus() }),
  };
  out.textContent = `ctx ${NAME} (${kind})`;
  bus.emit('ctxReady', { name: NAME, kind, vis: document.visibilityState });
}
