// Shared helpers of the spike pages and workers (Paso S1 of Fase 4.5). Never merged.

export const IS_WORKER = typeof WorkerGlobalScope !== 'undefined' && self instanceof WorkerGlobalScope;

export function errInfo(e) {
  if (e === undefined || e === null) return { name: String(e), message: '' };
  return {
    name: e.name ?? e.constructor?.name ?? typeof e,
    message: String(e.message ?? e),
    ctor: e.constructor?.name,
  };
}

// --- Logging to the server (it timestamps every line). ---------------------------------

let CTX = { token: null, who: null };
export function setLogContext(token, who) { CTX = { token, who }; }

export async function log(obj) {
  const body = JSON.stringify({ token: CTX.token, who: CTX.who, t: Date.now(), ...obj });
  try {
    await fetch('/log', { method: 'POST', headers: { 'content-type': 'application/json' }, body });
  } catch (e) { /* the server may be killing us */ }
}

// Synchronous POST (workers only): returns once the server answered.
export function logSync(path, obj) {
  const xhr = new XMLHttpRequest();
  xhr.open('POST', path, false);
  xhr.setRequestHeader('content-type', 'application/json');
  xhr.send(JSON.stringify({ token: CTX.token, who: CTX.who, t: Date.now(), ...obj }));
  return xhr.responseText;
}

// Reports with at most one request in flight: what arrives while one is in flight goes in the
// next, so the server learns of each record as soon as possible without a request queue.
export class Reporter {
  constructor(extra) { this.extra = extra; this.pending = []; this.inFlight = false; this.sent = 0; }
  push(rec) { this.pending.push(rec); if (!this.inFlight) this._send(); }
  async _send() {
    if (this.pending.length === 0) { this.inFlight = false; return; }
    this.inFlight = true;
    const batch = this.pending; this.pending = [];
    try {
      await fetch('/log', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ token: CTX.token, who: CTX.who, t: Date.now(), ...this.extra, batch }),
      });
      this.sent += batch.length;
    } catch (e) { /* killed */ }
    this._send();
  }
  async drain() { while (this.inFlight || this.pending.length) await sleep(5); }
}

// --- Time. ------------------------------------------------------------------------------

let timerWorker = null;
let timerSeq = 0;
const timerWaiters = new Map();
// In a page, sleeps run on a worker's timer, which hidden tabs do not throttle like the page's.
export function sleep(ms) {
  if (IS_WORKER || typeof Worker === 'undefined') return new Promise(r => setTimeout(r, ms));
  if (!timerWorker) {
    timerWorker = new Worker(URL.createObjectURL(new Blob(
      ['onmessage = e => setTimeout(() => postMessage(e.data), e.data.ms)'], { type: 'text/javascript' })));
    timerWorker.onmessage = e => { const w = timerWaiters.get(e.data.id); timerWaiters.delete(e.data.id); w?.(); };
  }
  return new Promise(r => { const id = ++timerSeq; timerWaiters.set(id, r); timerWorker.postMessage({ id, ms }); });
}

export function withTimeout(p, ms, what) {
  let t;
  return Promise.race([
    p.finally(() => clearTimeout(t)),
    new Promise((_, rej) => { t = setTimeout(() => rej(Object.assign(new Error(`timeout: ${what}`), { name: 'Timeout' })), ms); }),
  ]);
}

export function stats(xs) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const q = p => s[Math.min(s.length - 1, Math.floor(p * s.length))];
  return { n: s.length, min: s[0], p50: q(0.5), p90: q(0.9), p99: q(0.99), max: s[s.length - 1],
           mean: s.reduce((a, b) => a + b, 0) / s.length };
}

// --- The OPFS. --------------------------------------------------------------------------

export async function opfsRoot() { return navigator.storage.getDirectory(); }

export async function dirPath(path, create = true) {
  let d = await opfsRoot();
  for (const part of path.split('/').filter(Boolean)) d = await d.getDirectoryHandle(part, { create });
  return d;
}

export async function listNames(dir) {
  const out = [];
  for await (const [name, h] of dir.entries()) out.push(`${h.kind === 'directory' ? 'D' : 'F'}:${name}`);
  return out.sort();
}

export function fnv(bytes) {
  let h = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) { h ^= bytes[i]; h = Math.imul(h, 0x01000193); }
  return (h >>> 0).toString(16);
}

// Name, kind, size and hash of each entry (one level of subdirectories).
export async function snapshot(dir, depth = 1) {
  const out = {};
  for await (const [name, h] of dir.entries()) {
    if (h.kind === 'directory') {
      out[name] = depth > 0 ? { kind: 'dir', entries: await snapshot(h, depth - 1) } : { kind: 'dir' };
    } else {
      try {
        const f = await h.getFile();
        const b = new Uint8Array(await f.arrayBuffer());
        out[name] = { kind: 'file', size: b.length, hash: fnv(b) };
      } catch (e) {
        out[name] = { kind: 'file', unreadable: errInfo(e).name };
      }
    }
  }
  return out;
}

// Recognizable contents: a header with the generation and length, a pattern derived from the
// generation, and a trailer; `parseGen` says whether bytes are exactly one generation's.
export function genContent(gen) {
  const len = 8192 + (gen % 13) * 1000;
  const head = new TextEncoder().encode(`GEN:${gen}:LEN:${len}:`);
  const tail = new TextEncoder().encode(`:END:${gen}`);
  const b = new Uint8Array(len);
  b.set(head, 0);
  for (let k = head.length; k < len - tail.length; k++) b[k] = (gen * 31 + k * 7) & 0xff;
  b.set(tail, len - tail.length);
  return b;
}

export function parseGen(b) {
  const text = new TextDecoder().decode(b.subarray(0, 40));
  const m = /^GEN:(\d+):LEN:(\d+):/.exec(text);
  if (!m) return { valid: false, size: b.length, why: 'no header' };
  const gen = Number(m[1]);
  const exp = genContent(gen);
  if (exp.length !== b.length) return { valid: false, gen, size: b.length, why: `length ${b.length} != ${exp.length}` };
  for (let k = 0; k < b.length; k++) if (exp[k] !== b[k]) return { valid: false, gen, size: b.length, why: `differs at ${k}` };
  return { valid: true, gen, size: b.length };
}

export async function readAll(fileHandle) {
  return new Uint8Array(await (await fileHandle.getFile()).arrayBuffer());
}

// Writes `bytes` to a file through a sync access handle, flushes and closes it.
export async function writeFile(dir, name, bytes) {
  const fh = await dir.getFileHandle(name, { create: true });
  const h = await fh.createSyncAccessHandle();
  h.truncate(0);
  h.write(bytes, { at: 0 });
  h.flush();
  h.close();
  return fh;
}

// --- Attempts, for the error matrix. ------------------------------------------------------

export async function attempt(fn) {
  const t0 = performance.now();
  try {
    const v = await fn();
    return { ok: true, ms: +(performance.now() - t0).toFixed(2), value: describe(v) };
  } catch (e) {
    return { ok: false, ms: +(performance.now() - t0).toFixed(2), err: errInfo(e) };
  }
}

function describe(v) {
  if (v === undefined || v === null) return v;
  if (typeof v !== 'object') return v;
  if (typeof FileSystemSyncAccessHandle !== 'undefined' && v instanceof FileSystemSyncAccessHandle) {
    try { v.close(); } catch (e) { /* ignore */ }
    return 'FileSystemSyncAccessHandle (closed again)';
  }
  if (v.kind) return `${v.constructor?.name}(${v.name})`;
  return v.constructor?.name ?? 'object';
}

// --- Messaging between the contexts of a test (pages and workers), by BroadcastChannel. ---

export class Bus {
  constructor(token, me) {
    this.me = me;
    this.ch = new BroadcastChannel(`bus-${token}`);
    this.handlers = {};
    this.waiters = [];
    this.seq = 0;
    this.calls = new Map();
    this.events = [];
    this.ch.onmessage = e => this._on(e.data);
  }
  _on(m) {
    if (m.re !== undefined && m.to === this.me) {
      const c = this.calls.get(m.re); this.calls.delete(m.re);
      if (c) (m.ok ? c.res(m.r) : c.rej(Object.assign(new Error(m.err?.message), { name: m.err?.name, remote: true })));
      return;
    }
    if (m.cmd && m.to === this.me) {
      const h = this.handlers[m.cmd];
      Promise.resolve().then(() => h ? h(m.args ?? {}, m) : Promise.reject(new Error(`no handler ${m.cmd} in ${this.me}`)))
        .then(r => this.ch.postMessage({ re: m.id, to: m.from, ok: true, r }),
              err => this.ch.postMessage({ re: m.id, to: m.from, ok: false, err: errInfo(err) }));
      return;
    }
    if (m.event) {
      // An event goes to the first waiter that wants it, else it is kept for a later waitEvent.
      const w = this.waiters.find(x => x.pred(m));
      if (w) { this.waiters.splice(this.waiters.indexOf(w), 1); w.res(m); } else this.events.push(m);
    }
  }
  call(to, cmd, args, timeoutMs = 30000) {
    const id = `${this.me}#${++this.seq}`;
    const p = new Promise((res, rej) => this.calls.set(id, { res, rej }));
    this.ch.postMessage({ id, from: this.me, to, cmd, args });
    return withTimeout(p, timeoutMs, `${to}.${cmd}`);
  }
  send(to, cmd, args) { this.ch.postMessage({ id: `${this.me}#${++this.seq}`, from: this.me, to, cmd, args }); }
  emit(event, data) { this.ch.postMessage({ event, from: this.me, t: Date.now(), ...data }); }
  waitEvent(pred, timeoutMs = 30000, what = 'event') {
    const found = this.events.find(pred);
    if (found) { this.events.splice(this.events.indexOf(found), 1); return Promise.resolve(found); }
    let w;
    const p = new Promise(res => { w = { pred, res }; this.waiters.push(w); });
    return withTimeout(p, timeoutMs, what).catch(e => { this.waiters.splice(this.waiters.indexOf(w), 1); throw e; });
  }
}
