// The orchestrator: claims its token from the server, runs the test it names (tests.js), and
// logs its result and "done".
import * as L from './lib.js';

const P = new URLSearchParams(location.search);
const TOKEN = P.get('token');
const out = document.getElementById('out');

export class WorkerClient {
  constructor(url) {
    this.w = new Worker(url, { type: 'module' });
    this.seq = 0; this.calls = new Map(); this.listeners = [];
    this.ready = new Promise(res => { this._ready = res; });
    this.w.onmessage = e => {
      const d = e.data;
      if (d?.hello) this._ready();
      if (d?.re !== undefined) { const c = this.calls.get(d.re); this.calls.delete(d.re); if (c) d.ok ? c.res(d.r) : c.rej(Object.assign(new Error(d.err?.message), { name: d.err?.name })); return; }
      for (const l of this.listeners) l(d);
    };
    this.errors = [];
    this.w.onerror = e => { this.errors.push(e.message); };
  }
  async call(cmd, args, timeoutMs = 60000) {
    await this.ready;
    const id = ++this.seq;
    const p = new Promise((res, rej) => this.calls.set(id, { res, rej }));
    this.w.postMessage({ id, cmd, args });
    return L.withTimeout(p, timeoutMs, cmd);
  }
  terminate() { this.w.terminate(); }
}

async function main() {
  const claim = await fetch(`/claim?token=${TOKEN}`).then(r => r.json());
  if (!claim.ok) { out.textContent = `stale page (token ${TOKEN})`; return; }
  const { test, args } = claim;
  L.setLogContext(TOKEN, 'orch');
  out.textContent = `running ${test}`;
  const bus = new L.Bus(TOKEN, 'orch');
  const worker = new WorkerClient(`worker.js?token=${TOKEN}&name=orch.w`);
  let popupWorks = args.noPopups ? false : undefined;
  let driverOpenWorks = args.driverOpen ? undefined : false;
  let nctx = 0;
  const T = {
    token: TOKEN, args, bus, worker, WorkerClient, L,
    log: obj => L.log({ test, ...obj }),
    // Opens a context: a tab if the browser lets the page open one, else one the driver opens,
    // else an iframe. Returns its names on the bus and how it was opened.
    async openCtx(base, prefer) {
      const tryOpen = async (suffix, how, timeoutMs) => {
        const name = `${base}-${++nctx}${suffix}`;
        const url = `ctx.html?token=${TOKEN}&name=${name}${how === 'driver' ? '&via=driver' : ''}`;
        const ready = bus.waitEvent(m => m.event === 'ctxReady' && m.name === name, timeoutMs, `ctxReady ${name}`);
        const hello = bus.waitEvent(m => m.event === 'hello' && m.name === `${name}.w`, timeoutMs + 5000, `hello ${name}`);
        ready.catch(() => {}); hello.catch(() => {});
        if (how === 'popup') window.open(url, '_blank', 'noopener');
        else if (how === 'driver') await fetch(`/driver/open?token=${TOKEN}&url=${encodeURIComponent(url)}`, { method: 'POST' });
        else { const f = document.createElement('iframe'); f.src = url; f.width = 200; f.height = 60; document.body.appendChild(f); }
        const r = await ready; await hello;
        return { name, page: `${name}.page`, w: `${name}.w`, kind: r.kind, vis: r.vis };
      };
      if (prefer !== 'iframe' && popupWorks !== false) {
        try { const c = await tryOpen('p', 'popup', popupWorks ? 15000 : 6000); popupWorks = true; return c; }
        catch (e) { if (popupWorks === undefined) popupWorks = false; else throw e; }
      }
      if (prefer !== 'iframe' && driverOpenWorks !== false) {
        try { const c = await tryOpen('d', 'driver', driverOpenWorks ? 20000 : 15000); driverOpenWorks = true; return c; }
        catch (e) { if (driverOpenWorks === undefined) driverOpenWorks = false; else throw e; }
      }
      return tryOpen('i', 'iframe', 15000);
    },
    async endCtx(c, mode) {
      try { await bus.call(c.page, 'end', { mode }, 3000); } catch (e) { /* already gone */ }
    },
    async ctxGone(c) {
      try { await bus.call(c.page, 'info', {}, 1500); return false; } catch (e) { return true; }
    },
  };
  const mod = await import('./tests.js');
  let result;
  try {
    result = await mod[test](T, args);
    await T.log({ event: 'result', result, popupWorks, driverOpenWorks });
  } catch (e) {
    await T.log({ event: 'error', err: L.errInfo(e), stack: String(e.stack) });
  }
  out.textContent = `done ${test}`;
  if (!args.noDone) await T.log({ event: 'done' });
}
main();
