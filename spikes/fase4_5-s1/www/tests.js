// The page side of each test: what the orchestrator page does, and which contexts it opens.
// Each export takes the orchestrator's toolkit T and the test's arguments from the driver.
import * as L from './lib.js';

// --- S1 questions 3 and 4 (and the capabilities the others need). -------------------------
export async function info(T) {
  const page = {
    userAgent: navigator.userAgent,
    isSecureContext: window.isSecureContext,
    persistInPage: typeof navigator.storage?.persist === 'function',
    visibility: document.visibilityState,
  };
  page.persistedBefore = await navigator.storage.persisted().catch(e => L.errInfo(e));
  page.persist = await L.withTimeout(navigator.storage.persist(), 5000, 'persist()')
    .then(v => ({ value: v }), e => ({ err: L.errInfo(e) }));
  page.persistedAfter = await navigator.storage.persisted().catch(e => L.errInfo(e));
  page.estimate = await navigator.storage.estimate().catch(e => L.errInfo(e));
  page.locksInPage = typeof navigator.locks?.request === 'function';
  const worker = await T.worker.call('capabilities');
  return { page, worker };
}

// --- S1 question 1, from Rust (wasm-bindgen + web-sys) in a module worker. ----------------
function s1q1Worker(phase, dir) {
  return new Promise((resolve, reject) => {
    const w = new Worker(`s1q1-worker.js?phase=${phase}&dir=${dir}`, { type: 'module' });
    w.onmessage = e => { w.terminate(); e.data.ok ? resolve(JSON.parse(e.data.report)) : reject(Object.assign(new Error(e.data.err?.message), e.data.err)); };
    w.onerror = e => { w.terminate(); reject(new Error(`worker error: ${e.message}`)); };
  });
}
export async function s1q1(T, a) {
  const report = await s1q1Worker('write', a.dir);
  await T.log({ event: 'result', result: report });
  // Reload: the same page, with the token of the verifying phase.
  location.href = `run.html?token=${a.verifyToken}`;
  await new Promise(() => {});
}
export async function s1q1verify(T, a) { return s1q1Worker('verify', a.dir); }

// --- The error matrix (S1 extra), S3, and a handle held by another tab (S1 question 3). ---
export async function matrix(T, a) {
  const res = { caps: await T.worker.call('capabilities') };
  res.local = await T.worker.call('matrix', { dir: `matrix-${T.token}` }, 120000);
  res.s3 = await T.worker.call('s3', { dir: `s3-${T.token}` });
  const A = await T.openCtx('A');
  res.otherCtxKind = A.kind;
  const dir = `matrix-${T.token}/cross`;
  await T.bus.call(A.w, 'holdFile', { dir, name: 'x' });
  res.sah_heldByOtherTab = [];
  for (let i = 0; i < 5; i++) res.sah_heldByOtherTab.push(await T.worker.call('tryOpen', { dir, name: 'x' }));
  // Right after the other tab's worker closes its handle.
  await T.bus.call(A.w, 'closeFile', { dir, name: 'x' });
  res.sah_rightAfterOtherTabClosed = await T.worker.call('tryOpen', { dir, name: 'x' });
  // The other tab holds a handle and closes: how long until the file is free.
  await T.bus.call(A.w, 'holdFile', { dir, name: 'y' });
  const tEnd = Date.now();
  await T.endCtx(A, 'closeTab');
  const free = await T.worker.call('openWhenFree', { dir, name: 'y' });
  res.sah_afterOtherTabClosed = { ...free, freeAfterMs: free.ok ? free.t - tEnd : null };
  return res;
}

// --- S1 question 2: flushed and never closed, then terminate / tab closed / SIGKILL. -------
const LEN = 1 << 20;
export async function s1q2(T, a) {
  const out = {};
  const w1 = new T.WorkerClient(`worker.js?token=${T.token}&name=s1q2-term`);
  await w1.call('writeFlushNoClose', { dir: a.dir, name: 'term', seed: 11, len: LEN });
  w1.terminate();
  out.afterTerminate = await T.worker.call('readCheck', { dir: a.dir, name: 'term', seed: 11, len: LEN });
  const A = await T.openCtx('A');
  out.tabKind = A.kind;
  await T.bus.call(A.w, 'writeFlushNoClose', { dir: a.dir, name: 'tab', seed: 22, len: LEN });
  await T.endCtx(A, 'closeTab');
  out.afterTabCloseFree = await T.worker.call('openWhenFree', { dir: a.dir, name: 'tab' });
  out.afterTabClose = await T.worker.call('readCheck', { dir: a.dir, name: 'tab', seed: 22, len: LEN });
  await T.worker.call('writeFlushNoClose', { dir: a.dir, name: 'kill', seed: 33, len: LEN });
  await T.log({ event: 'result', result: out });
  await T.worker.call('killNow', { reason: 's1q2: flushed, not closed' });
}
export async function s1q2check(T, a) {
  return T.worker.call('readCheck', { dir: a.dir, name: 'kill', seed: 33, len: LEN });
}

// --- S5 question 1: series of operations, killed by the server. -----------------------------
export async function series(T, a) {
  return T.worker.call('series', a, 600000);
}
export async function checkSeries(T, a) { return T.worker.call('checkSeries', a); }
export async function rejections(T, a) {
  const r = await T.worker.call('rejections', a);
  await T.log({ event: 'result', result: r });
  await T.worker.call('killNow', { reason: 'after the rejections' });
}
export async function checkRejections(T, a) { return T.worker.call('checkRejections', a); }

// --- S5 question 2: the waiter that gets the lock, and the holder's handles. -----------------
export async function q2(T, a) {
  const rows = [];
  // One waiter tab serves every iteration (a new one only after an error), so each iteration
  // opens a single new tab, the holder's.
  let Bctx = null;
  const waiter = async () => (Bctx ??= await T.openCtx('B'));
  for (const mode of a.modes) {
    for (let i = 0; i < a.iters; i++) {
      const lock = `q2-${T.token}-${mode}-${i}`;
      const dir = `q2-${T.token}/${mode}-${i}`;
      const A = await T.openCtx('A');
      await T.bus.call(A.w, 'q2hold', { lock, dir, n: a.n });
      const B = await waiter();
      const q = await T.bus.call(B.w, 'q2wait', { lock, dir, n: a.n });
      const result = T.bus.waitEvent(m => m.event === 'q2result' && m.lock === lock, 12000, 'q2result');
      const ending = T.bus.waitEvent(m => m.event === 'ending' && (m.from === A.page || m.from === A.w), 10000, 'ending');
      result.catch(() => {}); ending.catch(() => {});
      const tCmd = Date.now();
      if (mode === 'panicClose') T.bus.send(A.w, 'panicClose', {});
      else T.bus.send(A.page, 'end', { mode });
      let r, e;
      try { e = await ending; } catch (err) { e = { t: tCmd, missing: true }; }
      try { r = await result; } catch (err) { r = { error: L.errInfo(err) }; }
      rows.push({ mode, i, aKind: A.kind, bKind: B.kind, queued: q.queued, tEnd: e.t, endMissing: !!e.missing,
                  handoverMs: r.tGrant ? r.tGrant - e.t : null, failures: r.failures, errors: r.errors,
                  retry: r.retry, openMs: r.openMs, error: r.error });
      await T.log({ event: 'q2row', row: rows[rows.length - 1] });
      if (r.error) { await T.endCtx(B, 'closeTab'); Bctx = null; }
      if (mode !== 'closeTab') await T.endCtx(A, 'closeTab');
      await L.sleep(50);
    }
  }
  // In-flight operations: the holder fires moves and removes without awaiting and ends at once.
  const inflight = [];
  for (const mode of a.inflightModes) {
    for (let i = 0; i < a.inflightIters; i++) {
      const lock = `q2f-${T.token}-${mode}-${i}`;
      const dir = `q2f-${T.token}/${mode}-${i}`;
      const A = await T.openCtx('A');
      await T.bus.call(A.w, 'q2hold', { lock, dir, n: 0, inflight: a.inflightOps });
      const B = await waiter();
      await T.bus.call(B.w, 'q2wait', { lock, dir, n: 0, listMs: 1000 });
      const result = T.bus.waitEvent(m => m.event === 'q2result' && m.lock === lock, 12000, 'q2result');
      result.catch(() => {});
      T.bus.send(A.w, 'fireAndEnd', { dir, inflight: a.inflightOps, mode });
      let r;
      try { r = await result; } catch (err) { r = { error: L.errInfo(err) }; }
      const first = r.listing?.first ?? [];
      inflight.push({ mode, i, aKind: A.kind, landedAtGrant: { moved: first.filter(x => x.startsWith('F:mv_')).length,
        removed: a.inflightOps - first.filter(x => x.startsWith('F:r_')).length },
        changesAfterGrant: r.listing?.changes ?? null, lists: r.listing?.lists, error: r.error });
      await T.log({ event: 'q2inflightRow', row: inflight[inflight.length - 1] });
      if (r.error) { await T.endCtx(B, 'closeTab'); Bctx = null; }
      await T.endCtx(A, 'closeTab');
      await L.sleep(50);
    }
  }
  if (Bctx) await T.endCtx(Bctx, 'closeTab');
  return { rows, inflight };
}

// --- S5 question 3: ifAvailable, and waiting. ------------------------------------------------
export async function q3(T, a) {
  const A = await T.openCtx('A');
  const B = await T.openCtx('B');
  const out = { aKind: A.kind, bKind: B.kind, runs: [] };
  for (let i = 0; i < 5; i++) {
    const lock = `q3-${T.token}-${i}`;
    const held = await T.bus.call(A.w, 'lockHold', { lock, holdMs: 1500 });
    const released = T.bus.waitEvent(m => m.event === 'released' && m.lock === lock, 10000, 'released');
    const ifAvail = await T.bus.call(B.w, 'lockIfAvailable', { lock });
    const waited = await T.bus.call(B.w, 'lockWaitTimed', { lock }, 10000);
    const rel = await released;
    out.runs.push({ ifAvailable: ifAvail, waitedMs: waited.waitedMs, grantAfterReleaseMs: waited.tGrant - rel.t, heldAt: held.t });
  }
  out.freeIfAvailable = await T.bus.call(B.w, 'lockIfAvailable', { lock: `q3-${T.token}-free` });
  await T.endCtx(A, 'closeTab'); await T.endCtx(B, 'closeTab');
  return out;
}

// --- S5 question 4: fetch streaming, from the worker. ---------------------------------------
export async function q4(T, a) {
  const out = {};
  const sub = async (key, cmd, args, ms) => {
    try { out[key] = await T.worker.call(cmd, args, ms); } catch (e) { out[key] = { error: L.errInfo(e) }; }
    await T.log({ event: 'q4sub', key, value: out[key] });
  };
  await sub('timed', 'streamTimed', { id: `${T.token}-timed`, chunks: 20, size: 1024, interval: 250 });
  await sub('timedSmall', 'streamTimed', { id: `${T.token}-small`, chunks: 20, size: 16, interval: 100 });
  await sub('abort', 'streamAbort', { id: `${T.token}-abort`, readChunks: 5 });
  await L.sleep(1500);
  await sub('flood', 'streamFlood', { id: `${T.token}-flood`, readBytes: 1 << 20, pauseMs: 8000 }, 60000);
  await L.sleep(1000);
  return out;
}

// --- S5 question 5: a background tab. ---------------------------------------------------------
export async function q5(T, a) {
  const start = Date.now();
  let hiddenSince = document.visibilityState === 'hidden' ? Date.now() : null;
  const vis = () => ({ vis: document.visibilityState, hiddenForMs: hiddenSince ? Date.now() - hiddenSince : 0 });
  document.addEventListener('visibilitychange', () => {
    hiddenSince = document.visibilityState === 'hidden' ? Date.now() : null;
    T.log({ event: 'visibility', vis: document.visibilityState, sinceStartMs: Date.now() - start });
  });
  T.worker.w.addEventListener('message', e => {
    if (e.data?.tokenRequest) T.worker.w.postMessage({ tokenReply: true, id: e.data.id, t: Date.now(), ...vis() });
  });
  await T.worker.call('bgTimers', { totalMs: a.totalMs });
  // The page's own timer, for comparison: a 1 s chain.
  (async () => {
    while (Date.now() - start < a.totalMs) {
      const t0 = performance.now();
      await new Promise(r => setTimeout(r, 1000));
      T.log({ event: 'timer', label: 'page-1s', periodMs: 1000, lateMs: +(performance.now() - t0 - 1000).toFixed(1), ...vis() });
    }
  })();
  await L.sleep(a.visibleMs);
  // A tab in front of this one: by window.open, else by the driver.
  const url = `blank.html?token=${T.token}&closeAfter=${a.hiddenMs}`;
  window.open(url, '_blank', 'noopener');
  await L.sleep(3000);
  if (document.visibilityState !== 'hidden' && a.driverOpen) {
    await fetch(`/driver/open?token=${T.token}&url=${encodeURIComponent(url)}`, { method: 'POST' });
    await L.sleep(3000);
  }
  await T.log({ event: 'visibility-after-open', ...vis() });
  while (Date.now() - start < a.totalMs + 35000) await L.sleep(1000);
  return { ...vis(), totalMs: Date.now() - start };
}

// --- S5 question 6: BroadcastChannel between workers of two tabs; locks. --------------------
export async function q6(T, a) {
  const out = {};
  const A = await T.openCtx('A');
  const B = await T.openCtx('B');
  out.aKind = A.kind; out.bKind = B.kind;
  // Order.
  for (const [label, n, bigEvery] of [['small', 5000, 0], ['withLarge', 2000, 10]]) {
    const chan = `q6o-${T.token}-${label}`;
    await T.bus.call(B.w, 'bcRecvOrder', { chan });
    const res = T.bus.waitEvent(m => m.event === 'orderResult' && m.from === B.w, 60000, 'orderResult');
    await T.bus.call(A.w, 'bcSendOrder', { chan, n, bigEvery }, 60000);
    out[`order_${label}`] = await res;
  }
  // A non-extractable CryptoKey.
  {
    const chan = `q6k-${T.token}`;
    await T.bus.call(B.w, 'bcKeyRecv', { chan });
    const got = [];
    const waitKeys = (async () => { for (let k = 0; k < 3; k++) got.push(await T.bus.waitEvent(m => m.event === 'keyReceived', 5000, 'key')); })();
    out.keySend = await T.bus.call(A.w, 'bcKeySend', { chan });
    await waitKeys.catch(e => { out.keyWaitError = L.errInfo(e); });
    out.keyReceived = got.map(m => ({ kind: m.kind, ...m.r }));
  }
  // Round trips.
  {
    const chan = `q6p-${T.token}`;
    await T.bus.call(B.w, 'bcEcho', { chan });
    out.rtt = await T.bus.call(A.w, 'bcPing', { chan, n: 1000 }, 60000);
  }
  // AbortSignal on a waiting request.
  {
    const lock = `q6a-${T.token}`;
    await T.bus.call(A.w, 'lockHold', { lock, holdMs: 3000 });
    const released = T.bus.waitEvent(m => m.event === 'released' && m.lock === lock, 10000, 'released');
    const ab = await T.bus.call(B.w, 'lockAbort', { lock, abortAfterMs: 300 });
    const next = T.bus.call(B.w, 'lockWaitTimed', { lock }, 10000);
    await released;
    const n = await next;
    await L.sleep(300);
    out.abortSignal = { ...ab, abortedCallbackRan: await T.bus.call(B.w, 'abortRan', {}), laterRequestWaitedMs: n.waitedMs };
  }
  await T.endCtx(A, 'closeTab'); await T.endCtx(B, 'closeTab');
  // Handover when the tab that holds the lock closes (no handles).
  out.handover = [];
  for (let i = 0; i < a.handoverIters; i++) {
    const lock = `q6h-${T.token}-${i}`;
    const H = await T.openCtx('H');
    await T.bus.call(H.w, 'lockHold', { lock });
    const Wt = await T.openCtx('W');
    await T.bus.call(Wt.w, 'lockWaitReport', { lock });
    const granted = T.bus.waitEvent(m => m.event === 'granted' && m.lock === lock, 15000, 'granted');
    const ending = T.bus.waitEvent(m => m.event === 'ending' && m.from === H.page, 5000, 'ending');
    granted.catch(() => {}); ending.catch(() => {});
    T.bus.send(H.page, 'end', { mode: 'closeTab' });
    let e, g;
    try { e = await ending; g = await granted; out.handover.push({ i, kind: H.kind, ms: g.tGrant - e.t }); }
    catch (err) { out.handover.push({ i, kind: H.kind, error: L.errInfo(err) }); }
    await T.endCtx(Wt, 'closeTab');
  }
  out.handoverStats = L.stats(out.handover.filter(h => h.ms !== undefined).map(h => h.ms));
  return out;
}

// --- Chromium only: a lock holder frozen by CDP (Page.setWebLifecycleState). ----------------
export async function freeze(T, a) {
  const out = { runs: [] };
  for (let i = 0; i < a.iters; i++) {
    const lock = `fz-${T.token}-${i}`;
    const H = await T.openCtx('H');
    await T.bus.call(H.w, 'lockHold', { lock, holdMs: a.holdMs });
    const Wt = await T.openCtx('W');
    await T.bus.call(Wt.w, 'lockWaitReport', { lock });
    const granted = T.bus.waitEvent(m => m.event === 'granted' && m.lock === lock, a.holdMs + a.freezeMs + 20000, 'granted');
    const released = T.bus.waitEvent(m => m.event === 'released' && m.lock === lock, a.holdMs + a.freezeMs + 20000, 'released');
    granted.catch(() => {}); released.catch(() => {});
    const tFreeze = Date.now();
    const fr = await fetch(`/driver/freeze?token=${T.token}&match=${encodeURIComponent(`name=${H.name}`)}&ms=${a.freezeMs}`, { method: 'POST' }).then(r => r.json());
    // While frozen, does a ping to the holder's worker get an answer?
    const ping = await T.bus.call(H.w, 'ping', {}, 1500).then(r => ({ answeredAt: r.t - tFreeze }), e => ({ noAnswer: L.errInfo(e).name }));
    let g, rel;
    try { g = await granted; rel = await released; } catch (err) { g = { error: L.errInfo(err) }; }
    out.runs.push({ i, kind: H.kind, freezeResp: fr, pingDuringFreeze: ping, grantedAfterFreezeStartMs: g.tGrant ? g.tGrant - tFreeze : null,
                    releasedAfterFreezeStartMs: rel?.t ? rel.t - tFreeze : null, holdMs: a.holdMs, freezeMs: a.freezeMs });
    await T.endCtx(Wt, 'closeTab'); await T.endCtx(H, 'closeTab');
  }
  return out;
}

// --- Chromium only: a page prerendered by speculation rules, and its Web Locks. -------------
export async function prerender(T, a) {
  const url = `prerendered.html?token=${T.token}`;
  const s = document.createElement('script');
  s.type = 'speculationrules';
  s.textContent = JSON.stringify({ prerender: [{ source: 'list', urls: [url] }] });
  document.head.appendChild(s);
  await L.sleep(a.waitMs);
  await T.log({ event: 'result', result: { navigatingAt: Date.now() } });
  location.href = url;
  await new Promise(() => {});
}

// --- DC9: ruzstd's encoder, timed in the page. ------------------------------------------------
export async function zstd(T, a) {
  const { instance } = await WebAssembly.instantiateStreaming(fetch('zstd/zstd_bench.wasm'));
  const x = instance.exports;
  const fetchBytes = async url => new Uint8Array(await (await fetch(url, { cache: 'no-store' })).arrayBuffer());
  const t0 = performance.now();
  const payload = await fetchBytes('/payload/payload.bin');
  const native = await fetchBytes('/payload/envelope_native.zmsn');
  const fetchMs = performance.now() - t0;
  const put = bytes => { const p = x.zb_alloc(bytes.length); new Uint8Array(x.memory.buffer, p, bytes.length).set(bytes); return p; };
  const result = () => new Uint8Array(x.memory.buffer, x.zb_result_ptr(), x.zb_result_len());
  const pPayload = put(payload);
  const runs = [];
  let own = null;
  for (let r = 0; r < a.runs; r++) {
    const s0 = performance.now();
    const st = x.zb_encode(pPayload, payload.length);
    const ms = performance.now() - s0;
    const out = result();
    runs.push({ run: r + 1, status: st, ms: +ms.toFixed(1), outBytes: out.length });
    if (r === a.runs - 1) own = out.slice();
    x.zb_clear();
  }
  const same = (u, v) => { if (u.length !== v.length) return false; for (let i = 0; i < u.length; i++) if (u[i] !== v[i]) return false; return true; };
  const decode = (bytes, label) => {
    const p = put(bytes);
    const s0 = performance.now();
    const st = x.zb_decode(p, bytes.length);
    const ms = performance.now() - s0;
    const ok = st === 0 && same(result(), payload);
    x.zb_clear(); x.zb_free(p, bytes.length);
    return { label, status: st, ms: +ms.toFixed(1), sameAsPayload: ok, envelopeBytes: bytes.length };
  };
  const decNative = decode(native, 'native envelope (zstd 3)');
  const decOwn = decode(own, 'own envelope (ruzstd Fastest)');
  const up = await fetch(`/upload?token=${T.token}&name=zstd-load${a.load}.zmsn`, { method: 'POST', body: own }).then(r => r.json());
  return { load: a.load, payloadBytes: payload.length, nativeEnvelopeBytes: native.length, fetchMs: +fetchMs.toFixed(0),
           wasmBytes: a.wasmBytes, runs, decNative, decOwn, uploaded: up, userAgent: navigator.userAgent, crossOriginIsolated };
}

// --- Safari: does a WebDriver session keep the OPFS (and see the normal session's)? ---------
export async function opfsMark(T, a) { return T.worker.call('opfsMark', a); }

// --- Safari: a lock holder in a background tab that the browser may suspend. ------------------
export async function bgfreeze(T, a) {
  const lock = `bgf-${T.token}`;
  const A = await T.openCtx('A');
  await T.bus.call(A.w, 'lockHold', { lock });
  T.bus.send(A.w, 'heartbeat', { label: 'holder', periodMs: 5000, totalMs: a.totalMs });
  const B = await T.openCtx('B');
  await T.bus.call(B.w, 'lockWaitReport', { lock });
  T.bus.send(B.w, 'heartbeat', { label: 'waiter', periodMs: 5000, totalMs: a.totalMs });
  const aInfo = await T.bus.call(A.page, 'info', {}).catch(e => L.errInfo(e));
  const bInfo = await T.bus.call(B.page, 'info', {}).catch(e => L.errInfo(e));
  await T.log({ event: 'bgfreeze-setup', aKind: A.kind, bKind: B.kind, aInfo, bInfo, lock });
  await new Promise(() => {}); // the driver reads the heartbeats and the grant from the server's log
}

// --- DC41 follow-up: the holder's in-flight operations against the waiter's client.lock handle. --
export async function dc41(T, a) {
  const rows = [];
  let Bctx = null;
  const waiter = async () => (Bctx ??= await T.openCtx('B'));
  for (const mode of a.modes) {
    for (let i = 0; i < a.iters; i++) {
      const lock = `dc41-${T.token}-${mode}-${i}`;
      const dir = `dc41-${T.token}/${mode}-${i}`;
      const A = await T.openCtx('A');
      await T.bus.call(A.w, 'dc41hold', { lock, dir, inflight: 20, extra: a.extra ?? 0 });
      const B = await waiter();
      const q = await T.bus.call(B.w, 'dc41wait', { lock, dir, listMs: 1000 });
      const result = T.bus.waitEvent(m => m.event === 'dc41result' && m.lock === lock, 15000, 'dc41result');
      result.catch(() => {});
      T.bus.send(A.w, 'dc41fire', { dir, inflight: 20, mode });
      let r;
      try { r = await result; } catch (err) { r = { error: L.errInfo(err) }; }
      const row = { mode, i, aKind: A.kind, queued: q.queued, ...r };
      delete row.event; delete row.from; delete row.t;
      rows.push(row);
      await T.log({ event: 'dc41row', row });
      if (r.error) { await T.endCtx(B, 'closeTab'); Bctx = null; }
      await T.endCtx(A, 'closeTab');
      await L.sleep(50);
    }
  }
  if (Bctx) await T.endCtx(Bctx, 'closeTab');
  return { rows };
}
