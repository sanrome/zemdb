// The dedicated worker of every spike context. Its page, or another context through the bus,
// calls its commands. Never merged.
import * as L from './lib.js';

const P = new URL(self.location.href).searchParams;
const TOKEN = P.get('token');
const NAME = P.get('name');
L.setLogContext(TOKEN, NAME);
const bus = new L.Bus(TOKEN, NAME);

const keep = { handles: new Map(), lockReleases: new Map(), misc: [] };

// ---------------------------------------------------------------------------------------
// Capabilities (S1 questions 3 and 4, in the worker).
// ---------------------------------------------------------------------------------------
async function capabilities() {
  const out = {
    userAgent: navigator.userAgent,
    isSecureContext: self.isSecureContext,
    locksInWorker: typeof navigator.locks?.request === 'function',
    storageInWorker: typeof navigator.storage?.getDirectory === 'function',
    persistInWorker: typeof navigator.storage?.persist === 'function',
    persistedInWorker: typeof navigator.storage?.persisted === 'function',
    syncAccessHandle: typeof FileSystemSyncAccessHandle !== 'undefined',
    moveOnFileHandle: typeof FileSystemFileHandle !== 'undefined' && typeof FileSystemFileHandle.prototype.move === 'function',
    moveOnDirectoryHandle: typeof FileSystemDirectoryHandle !== 'undefined' && typeof FileSystemDirectoryHandle.prototype.move === 'function',
    removeOnHandle: typeof FileSystemHandle !== 'undefined' && typeof FileSystemHandle.prototype.remove === 'function',
    createWritable: typeof FileSystemFileHandle !== 'undefined' && typeof FileSystemFileHandle.prototype.createWritable === 'function',
  };
  try { out.persistedWorker = await navigator.storage.persisted(); } catch (e) { out.persistedWorker = L.errInfo(e); }
  try { out.estimateWorker = await navigator.storage.estimate(); } catch (e) { out.estimateWorker = L.errInfo(e); }
  // Access modes: two handles at once on one file, per mode.
  const dir = await L.dirPath(`caps-${TOKEN}`);
  out.modes = {};
  for (const mode of [undefined, 'readwrite', 'read-only', 'readwrite-unsafe']) {
    const fh = await dir.getFileHandle(`m-${mode}`, { create: true });
    const opts = mode ? { mode } : undefined;
    const first = await L.attempt(() => fh.createSyncAccessHandle(opts).then(h => { keep.misc.push(h); return 'open'; }));
    const second = await L.attempt(() => fh.createSyncAccessHandle(opts));
    let reportedMode = null;
    const h = keep.misc[keep.misc.length - 1];
    if (first.ok) { reportedMode = h.mode ?? '(no mode attribute)'; h.close(); keep.misc.pop(); }
    out.modes[mode ?? 'default'] = { first, second, reportedMode };
  }
  // A read-only handle while a readwrite one is open, and the other way round.
  {
    const fh = await dir.getFileHandle('mix', { create: true });
    const rw = await fh.createSyncAccessHandle();
    out.modes.readOnlyWhileReadwrite = await L.attempt(() => fh.createSyncAccessHandle({ mode: 'read-only' }));
    rw.close();
  }
  return out;
}

// ---------------------------------------------------------------------------------------
// The error matrix (S1 extra) and S3.
// ---------------------------------------------------------------------------------------
async function snapDir(dir) { try { return await L.snapshot(dir); } catch (e) { return { error: L.errInfo(e) }; } }

async function matrix({ dir: dirName }) {
  const root = await L.dirPath(dirName);
  const R = {};
  let n = 0;
  const fresh = async () => root.getDirectoryHandle(`c${++n}`, { create: true });
  const content = new TextEncoder().encode('ORIGINAL');
  const other = new TextEncoder().encode('OTHER');

  // createSyncAccessHandle -----------------------------------------------------------
  { // another handle in the same worker holds the file: same FileSystemFileHandle and a new one
    const d = await fresh();
    const f = await L.writeFile(d, 'a', content);
    const h1 = await f.createSyncAccessHandle();
    R.sah_heldSameWorker_sameFileHandle = await L.attempt(() => f.createSyncAccessHandle());
    const f2 = await d.getFileHandle('a');
    R.sah_heldSameWorker_newFileHandle = await L.attempt(() => f2.createSyncAccessHandle());
    R.sah_heldSameWorker_readOnlyMode = await L.attempt(() => f2.createSyncAccessHandle({ mode: 'read-only' }));
    h1.close();
  }
  { // two createSyncAccessHandle() issued together, before either resolves (10 tries each)
    const runs = [];
    for (let i = 0; i < 10; i++) {
      const d = await fresh();
      const f = await L.writeFile(d, 'a', content);
      const g = i % 2 ? await d.getFileHandle('a') : f;
      const t0 = performance.now();
      const res = await Promise.allSettled([f.createSyncAccessHandle(), g.createSyncAccessHandle()]);
      const ms = +(performance.now() - t0).toFixed(2);
      runs.push({ sameFileHandle: g === f, ms, outcomes: res.map(r => r.status === 'fulfilled' ? 'ok' : L.errInfo(r.reason).name) });
      for (const r of res) if (r.status === 'fulfilled') r.value.close();
    }
    R.sah_concurrentPair = runs;
  }
  { // the entry was removed after the FileSystemFileHandle was obtained
    const d = await fresh();
    const f = await L.writeFile(d, 'a', content);
    await d.removeEntry('a');
    R.sah_entryRemoved = await L.attempt(() => f.createSyncAccessHandle());
    R.sah_entryRemoved.after = await snapDir(d);
  }
  { // the file was moved away through another FileSystemFileHandle
    const d = await fresh();
    const f = await L.writeFile(d, 'a', content);
    const f2 = await d.getFileHandle('a');
    R.sah_movedAwayByOtherHandle_move = await L.attempt(() => f2.move('b'));
    R.sah_movedAwayByOtherHandle = await L.attempt(() => f.createSyncAccessHandle());
    R.sah_movedAwayByOtherHandle.after = await snapDir(d);
    R.sah_movedAwayByOtherHandle.oldHandleName = f.name;
  }
  { // its parent directory was removed (recursively)
    const d = await fresh();
    const sub = await d.getDirectoryHandle('p', { create: true });
    const f = await L.writeFile(sub, 'a', content);
    await d.removeEntry('p', { recursive: true });
    R.sah_parentRemoved = await L.attempt(() => f.createSyncAccessHandle());
    R.sah_parentRemoved.after = await snapDir(d);
  }
  { // right after close() of the previous handle (20 times, same tick)
    const d = await fresh();
    const f = await L.writeFile(d, 'a', content);
    const outcomes = {};
    for (let i = 0; i < 20; i++) {
      const h = await f.createSyncAccessHandle();
      h.close();
      const r = await L.attempt(() => f.createSyncAccessHandle());
      outcomes[r.ok ? 'ok' : r.err.name] = (outcomes[r.ok ? 'ok' : r.err.name] ?? 0) + 1;
    }
    R.sah_rightAfterClose = outcomes;
  }
  { // while a writable stream (createWritable) is open on the file, and the other way round
    const d = await fresh();
    const f = await L.writeFile(d, 'a', content);
    const ws = await L.attempt(() => f.createWritable().then(w => { keep.misc.push(w); return 'writable'; }));
    R.sah_whileWritableOpen = ws.ok ? await L.attempt(() => f.createSyncAccessHandle()) : { skipped: ws };
    if (ws.ok) { try { await keep.misc.pop().abort(); } catch (e) { /* ignore */ } }
    const h = await f.createSyncAccessHandle();
    R.writable_whileSahOpen = await L.attempt(() => f.createWritable());
    h.close();
  }
  { // the file was removed and recreated under the same name after the handle was obtained
    const d = await fresh();
    const f = await L.writeFile(d, 'a', content);
    await d.removeEntry('a');
    await L.writeFile(d, 'a', other);
    R.sah_removedAndRecreated = await L.attempt(async () => {
      const h = await f.createSyncAccessHandle();
      const b = new Uint8Array(16); const k = h.read(b, { at: 0 }); h.close();
      return new TextDecoder().decode(b.subarray(0, k));
    });
  }

  // getFileHandle / getDirectoryHandle, each attempt in a directory of its own ----------
  {
    const names = { missing: 'missing', slash: 'a/b', backslash: 'a\\b', dotdot: '..', dot: '.', empty: '' };
    for (const [k, name] of Object.entries(names)) {
      R[`getFile_${k}`] = await L.attempt(async () => (await fresh()).getFileHandle(name));
      R[`getFile_${k}_create`] = await L.attempt(async () => (await fresh()).getFileHandle(name, { create: true }));
      R[`getDir_${k}`] = await L.attempt(async () => (await fresh()).getDirectoryHandle(name));
      R[`getDir_${k}_create`] = await L.attempt(async () => (await fresh()).getDirectoryHandle(name, { create: true }));
    }
    const d = await fresh();
    await L.writeFile(d, 'afile', content);
    await d.getDirectoryHandle('adir', { create: true });
    R.getFile_onDirectory = await L.attempt(() => d.getFileHandle('adir'));
    R.getFile_onDirectory_create = await L.attempt(() => d.getFileHandle('adir', { create: true }));
    R.getDir_onFile = await L.attempt(() => d.getDirectoryHandle('afile'));
    R.getDir_onFile_create = await L.attempt(() => d.getDirectoryHandle('afile', { create: true }));
    R.getHandles_typeMismatch_after = await snapDir(d);
    // Inside a directory that was removed.
    const sub = await d.getDirectoryHandle('gone', { create: true });
    await d.removeEntry('gone', { recursive: true });
    R.getFile_inRemovedDir = await L.attempt(() => sub.getFileHandle('x'));
    R.getFile_inRemovedDir_create = await L.attempt(() => sub.getFileHandle('x', { create: true }));
    R.getDir_inRemovedDir_create = await L.attempt(() => sub.getDirectoryHandle('y', { create: true }));
    R.list_removedDir = await L.attempt(() => L.listNames(sub));
    R.inRemovedDir_after = await snapDir(d);
  }

  // move() -----------------------------------------------------------------------------
  {
    const d = await fresh();
    const src = await L.writeFile(d, 'src', content);
    const h = await src.createSyncAccessHandle();
    R.move_sourceLocked_1arg = await L.attempt(() => src.move('dst'));
    R.move_sourceLocked_2arg = await L.attempt(() => src.move(d, 'dst'));
    h.close();
    R.move_sourceLocked_after = await snapDir(d);
  }
  {
    const d = await fresh();
    const src = await L.writeFile(d, 'src', content);
    const dst = await L.writeFile(d, 'dst', other);
    const h = await dst.createSyncAccessHandle();
    R.move_destinationLocked_1arg = await L.attempt(() => src.move('dst'));
    R.move_destinationLocked_2arg = await L.attempt(() => src.move(d, 'dst'));
    h.close();
    R.move_destinationLocked_after = await snapDir(d);
  }
  {
    const d = await fresh();
    const src = await L.writeFile(d, 'src', content);
    const ws = await src.createWritable().catch(e => null);
    R.move_sourceWritableOpen = ws ? await L.attempt(() => src.move('dst')) : { skipped: 'no createWritable' };
    if (ws) await ws.abort().catch(() => {});
    R.move_sourceWritableOpen_after = await snapDir(d);
  }
  {
    const d = await fresh();
    const src = await L.writeFile(d, 'src', content);
    await d.removeEntry('src');
    R.move_sourceMissing_1arg = await L.attempt(() => src.move('dst'));
    R.move_sourceMissing_2arg = await L.attempt(() => src.move(d, 'dst'));
    R.move_sourceMissing_after = await snapDir(d);
  }
  {
    const d = await fresh();
    const src = await L.writeFile(d, 'src', content);
    const sub = await d.getDirectoryHandle('gone', { create: true });
    await d.removeEntry('gone');
    R.move_destinationDirMissing = await L.attempt(() => src.move(sub, 'dst'));
    R.move_destinationDirMissing_after = await snapDir(d);
  }
  {
    const d = await fresh();
    const src = await L.writeFile(d, 'src', content);
    await d.getDirectoryHandle('adir', { create: true });
    for (const [k, name] of Object.entries({ slash: 'a/b', dotdot: '..', dot: '.', empty: '' })) {
      R[`move_invalidName_${k}`] = await L.attempt(() => src.move(name));
    }
    R.move_ontoExistingDirectory = await L.attempt(() => src.move('adir'));
    R.move_ontoItself = await L.attempt(() => src.move('src'));
    R.move_invalid_after = await snapDir(d);
  }
  {
    const d = await fresh();
    const sub = await d.getDirectoryHandle('sub', { create: true });
    await L.writeFile(sub, 'x', content);
    R.move_directory = await L.attempt(() => sub.move('sub2'));
    R.move_directory_after = await snapDir(d);
  }

  // removeEntry() ----------------------------------------------------------------------
  {
    const d = await fresh();
    const f = await L.writeFile(d, 'locked', content);
    const h = await f.createSyncAccessHandle();
    R.remove_locked = await L.attempt(() => d.removeEntry('locked'));
    R.remove_lockedViaHandleRemove = typeof f.remove === 'function' ? await L.attempt(() => f.remove()) : { skipped: 'no remove()' };
    h.close();
    R.remove_locked_after = await snapDir(d);
    const f2 = await L.writeFile(d, 'wlocked', content);
    const ws = await f2.createWritable().catch(() => null);
    R.remove_writableOpen = ws ? await L.attempt(() => d.removeEntry('wlocked')) : { skipped: 'no createWritable' };
    if (ws) await ws.abort().catch(() => {});
    R.remove_missing = await L.attempt(() => d.removeEntry('nothere'));
    R.remove_missing_recursive = await L.attempt(() => d.removeEntry('nothere', { recursive: true }));
    for (const [k, name] of Object.entries({ slash: 'a/b', dotdot: '..', dot: '.', empty: '' })) {
      R[`remove_invalidName_${k}`] = await L.attempt(() => d.removeEntry(name));
    }
    const full = await d.getDirectoryHandle('full', { create: true });
    await L.writeFile(full, 'x', content);
    R.remove_nonEmptyDir = await L.attempt(() => d.removeEntry('full'));
    const lockedInside = await full.getFileHandle('x');
    const h2 = await lockedInside.createSyncAccessHandle();
    R.remove_dirWithLockedFile_recursive = await L.attempt(() => d.removeEntry('full', { recursive: true }));
    h2.close();
    R.remove_after = await snapDir(d);
    const sub = await d.getDirectoryHandle('gone', { create: true });
    await L.writeFile(sub, 'y', content);
    await d.removeEntry('gone', { recursive: true });
    R.remove_inRemovedDir = await L.attempt(() => sub.removeEntry('y'));
  }
  return R;
}

// S3: the four questions, in a fresh directory.
async function s3({ dir: dirName }) {
  const root = await L.dirPath(dirName);
  const enc = s => new TextEncoder().encode(s);
  const text = async fh => new TextDecoder().decode(await L.readAll(fh));
  const has = async (d, name) => (await L.listNames(d)).some(x => x.endsWith(`:${name}`));
  const A = {};
  { // Q1: move over an existing file, both forms, and into another directory
    const d = await root.getDirectoryHandle('q1', { create: true });
    const a = await L.writeFile(d, 'a', enc('NEW-A'));
    await L.writeFile(d, 'b', enc('OLD-B'));
    A.q1_move1arg = await L.attempt(() => a.move('b'));
    A.q1_move1arg_result = { b: await text(await d.getFileHandle('b')), aStillThere: await has(d, 'a') };
    const c = await L.writeFile(d, 'c', enc('NEW-C'));
    await L.writeFile(d, 'e', enc('OLD-E'));
    A.q1_move2arg = await L.attempt(() => c.move(d, 'e'));
    A.q1_move2arg_result = { e: await text(await d.getFileHandle('e')), cStillThere: await has(d, 'c') };
    const d2 = await root.getDirectoryHandle('q1other', { create: true });
    await L.writeFile(d2, 'g', enc('OLD-G'));
    const f = await L.writeFile(d, 'f', enc('NEW-F'));
    A.q1_move2arg_otherDir = await L.attempt(() => f.move(d2, 'g'));
    A.q1_move2arg_otherDir_result = { g: await text(await d2.getFileHandle('g')), fStillThere: await has(d, 'f') };
  }
  { // Q2: move a file that has an open handle; move onto a destination that has one
    const d = await root.getDirectoryHandle('q2', { create: true });
    const a = await L.writeFile(d, 'a', enc('A'));
    const b = await L.writeFile(d, 'b', enc('B'));
    let h = await a.createSyncAccessHandle();
    A.q2_moveSourceOpen = await L.attempt(() => a.move('x'));
    h.close();
    A.q2_moveSourceOpen_after = await snapDir(d);
    h = await b.createSyncAccessHandle();
    A.q2_moveOntoOpenDestination = await L.attempt(() => a.move('b'));
    h.close();
    A.q2_moveOntoOpenDestination_after = await snapDir(d);
  }
  { // Q3: removeEntry of a file with an open handle
    const d = await root.getDirectoryHandle('q3', { create: true });
    const a = await L.writeFile(d, 'a', enc('A'));
    const h = await a.createSyncAccessHandle();
    A.q3_removeOpen = await L.attempt(() => d.removeEntry('a'));
    h.close();
    A.q3_removeOpen_after = await snapDir(d);
  }
  { // Q4: the FileSystemFileHandle after its move()
    const d = await root.getDirectoryHandle('q4', { create: true });
    const a = await L.writeFile(d, 'a', enc('A'));
    await a.move('moved');
    A.q4_nameAfterMove = a.name;
    A.q4_getFileAfterMove = await L.attempt(() => text(a));
    A.q4_isSameEntryAsNewName = await L.attempt(async () => a.isSameEntry(await d.getFileHandle('moved')));
    A.q4_sahAfterMove = await L.attempt(async () => {
      const h = await a.createSyncAccessHandle();
      h.write(enc('Z'), { at: 1 }); h.flush(); h.close();
      return text(await d.getFileHandle('moved'));
    });
    A.q4_after = await snapDir(d);
    // And moved twice (the WAL's rotation and its rollback).
    await a.move('a');
    A.q4_movedBack_name = a.name;
    A.q4_movedBack_sah = await L.attempt(async () => { const h = await a.createSyncAccessHandle(); const s = h.getSize(); h.close(); return s; });
  }
  return A;
}

async function holdFile({ dir, name }) {
  const d = await L.dirPath(dir);
  const fh = await d.getFileHandle(name, { create: true });
  const h = await fh.createSyncAccessHandle();
  keep.handles.set(`${dir}/${name}`, h);
  return 'held';
}
async function closeFile({ dir, name }) {
  const h = keep.handles.get(`${dir}/${name}`);
  h.close(); keep.handles.delete(`${dir}/${name}`);
  return Date.now();
}
async function tryOpen({ dir, name }) {
  const d = await L.dirPath(dir);
  const fh = await d.getFileHandle(name, { create: true });
  return L.attempt(() => fh.createSyncAccessHandle());
}
// Retries until the handle opens: how long the file stays taken.
async function openWhenFree({ dir, name, maxMs = 10000 }) {
  const d = await L.dirPath(dir);
  const fh = await d.getFileHandle(name, { create: true });
  const t0 = Date.now(); let tries = 0; let last;
  while (Date.now() - t0 < maxMs) {
    tries++;
    try { const h = await fh.createSyncAccessHandle(); h.close(); return { ok: true, t: Date.now(), tries, last }; }
    catch (e) { last = L.errInfo(e).name; await L.sleep(2); }
  }
  return { ok: false, tries, last };
}

// ---------------------------------------------------------------------------------------
// S1 question 2: flushed, never closed.
// ---------------------------------------------------------------------------------------
function bigPattern(seed, len) {
  const b = new Uint8Array(len); let x = seed >>> 0 || 1;
  for (let i = 0; i < len; i++) { x ^= x << 13; x ^= x >>> 17; x ^= x << 5; b[i] = x & 0xff; }
  return b;
}
async function writeFlushNoClose({ dir, name, seed, len }) {
  const d = await L.dirPath(dir);
  const fh = await d.getFileHandle(name, { create: true });
  const h = await fh.createSyncAccessHandle();
  h.truncate(0);
  h.write(bigPattern(seed, len), { at: 0 });
  h.flush();
  keep.handles.set(`${dir}/${name}`, h);
  return { size: h.getSize() };
}
async function readCheck({ dir, name, seed, len }) {
  const d = await L.dirPath(dir, false);
  const fh = await d.getFileHandle(name);
  const open = await L.attempt(() => fh.createSyncAccessHandle().then(h => { keep.misc.push(h); return 'open'; }));
  if (!open.ok) {
    // Still taken: read through getFile() instead.
    const b = await L.readAll(fh).catch(e => null);
    return { openErr: open.err, viaGetFile: b ? { size: b.length, ok: L.fnv(b) === L.fnv(bigPattern(seed, len)) } : null };
  }
  const h = keep.misc.pop();
  const b = new Uint8Array(h.getSize()); h.read(b, { at: 0 }); h.close();
  return { size: b.length, ok: b.length === len && L.fnv(b) === L.fnv(bigPattern(seed, len)) };
}
function killNow({ reason }) { return L.logSync(`/kill?token=${TOKEN}`, { reason }); }

// ---------------------------------------------------------------------------------------
// S5 question 1: a series of directory operations, killed from the server.
// ---------------------------------------------------------------------------------------
async function series({ dir: dirName, cycles, kinds, killAfterLast }) {
  const d = await L.dirPath(dirName);
  // Setup: the target at generation 0, the files the series removes, the file it flushes.
  await L.writeFile(d, 'target', L.genContent(0));
  for (let i = 1; i <= cycles; i++) if (kinds.includes('remove')) await d.getFileHandle(`r_${i}`, { create: true });
  let fl = null;
  if (kinds.includes('flushNoClose')) fl = await (await d.getFileHandle('fl', { create: true })).createSyncAccessHandle();
  L.logSync('/log', { event: 'series-setup-done', dir: dirName, cycles, kinds });
  const rep = new L.Reporter({ event: 'series-resolved', dir: dirName });
  let seq = 0;
  let tmp = null;
  const t0 = performance.now();
  for (let i = 1; i <= cycles; i++) {
    for (const kind of kinds) {
      switch (kind) {
        case 'tmpCreate': tmp = await d.getFileHandle(`t_${i}`, { create: true }); break;
        case 'tmpWritten': { const h = await tmp.createSyncAccessHandle(); h.write(L.genContent(i), { at: 0 }); h.flush(); h.close(); break; }
        case 'move': if (i % 2) await tmp.move(d, 'target'); else await tmp.move('target'); break;
        case 'createFile': await d.getFileHandle(`f_${i}`, { create: true }); break;
        case 'createDir': await d.getDirectoryHandle(`d_${i}`, { create: true }); break;
        case 'remove': await d.removeEntry(`r_${i}`); break;
        case 'flushNoClose': fl.truncate(0); fl.write(L.genContent(i), { at: 0 }); fl.flush(); break;
      }
      seq++;
      const rec = { seq, kind, i, t: Date.now() };
      if (killAfterLast && i === cycles && kind === kinds[kinds.length - 1]) {
        L.logSync(`/kill?token=${TOKEN}`, { event: 'series-resolved', dir: dirName, batch: [rec], kill: true });
        await L.sleep(60000); // not reached: the server kills the browser before answering
      }
      rep.push(rec);
    }
  }
  const ms = performance.now() - t0;
  await rep.drain();
  await L.log({ event: 'series-finished', dir: dirName, seq, ms });
  return { seq, ms };
}

async function checkSeries({ dir: dirName }) {
  const d = await L.dirPath(dirName, false);
  const entries = {};
  for await (const [name, h] of d.entries()) {
    if (h.kind === 'directory') { entries[name] = { kind: 'dir' }; continue; }
    const b = await L.readAll(h);
    entries[name] = { kind: 'file', size: b.length, ...(b.length ? { gen: L.parseGen(b) } : {}) };
  }
  return entries;
}

// The rejections we can provoke; each is checked for an effect right away and, after the
// browser is killed, on reopening (checkRejections).
async function rejections({ dir: dirName }) {
  const d = await L.dirPath(dirName);
  const enc = s => new TextEncoder().encode(s);
  const R = {};
  const srcL = await L.writeFile(d, 'srcLocked', enc('SRC-LOCKED'));
  keep.misc.push(await srcL.createSyncAccessHandle());
  R.moveSourceLocked = await L.attempt(() => srcL.move(d, 'dstOfLocked'));
  const src2 = await L.writeFile(d, 'src2', enc('SRC2'));
  const dstL = await L.writeFile(d, 'dstLocked', enc('DST-LOCKED'));
  keep.misc.push(await dstL.createSyncAccessHandle());
  R.moveDestinationLocked = await L.attempt(() => src2.move(d, 'dstLocked'));
  const gone = await L.writeFile(d, 'gone', enc('GONE'));
  await d.removeEntry('gone');
  R.moveSourceMissing = await L.attempt(() => gone.move(d, 'dstOfGone'));
  const src4 = await L.writeFile(d, 'src4', enc('SRC4'));
  R.moveInvalidName = await L.attempt(() => src4.move(d, 'a/b'));
  R.moveInvalidNameDotDot = await L.attempt(() => src4.move(d, '..'));
  await d.getDirectoryHandle('adir', { create: true });
  const src5 = await L.writeFile(d, 'src5', enc('SRC5'));
  R.moveOntoDirectory = await L.attempt(() => src5.move(d, 'adir'));
  const goneDir = await d.getDirectoryHandle('goneDir', { create: true });
  await d.removeEntry('goneDir');
  const src6 = await L.writeFile(d, 'src6', enc('SRC6'));
  R.moveIntoMissingDir = await L.attempt(() => src6.move(goneDir, 'x'));
  R.createInvalidName = await L.attempt(() => d.getFileHandle('x/y', { create: true }));
  R.createDirInvalidName = await L.attempt(() => d.getDirectoryHandle('..', { create: true }));
  R.createFileOverDir = await L.attempt(() => d.getFileHandle('adir', { create: true }));
  await L.writeFile(d, 'afile', enc('AFILE'));
  R.createDirOverFile = await L.attempt(() => d.getDirectoryHandle('afile', { create: true }));
  R.createFileInRemovedDir = await L.attempt(() => goneDir.getFileHandle('z', { create: true }));
  R.createDirInRemovedDir = await L.attempt(() => goneDir.getDirectoryHandle('zz', { create: true }));
  const lk = await L.writeFile(d, 'removeLocked', enc('RM-LOCKED'));
  keep.misc.push(await lk.createSyncAccessHandle());
  R.removeLocked = await L.attempt(() => d.removeEntry('removeLocked'));
  R.removeMissing = await L.attempt(() => d.removeEntry('nothere'));
  R.removeInvalidName = await L.attempt(() => d.removeEntry('a/b'));
  R.removeDotDot = await L.attempt(() => d.removeEntry('..'));
  const full = await d.getDirectoryHandle('full', { create: true });
  await L.writeFile(full, 'x', enc('X'));
  R.removeNonEmptyDir = await L.attempt(() => d.removeEntry('full'));
  const wl = await L.writeFile(d, 'removeWritable', enc('RM-WRITABLE'));
  const ws = await wl.createWritable().catch(() => null);
  if (ws) { keep.misc.push(ws); R.removeWritableOpen = await L.attempt(() => d.removeEntry('removeWritable')); }
  const rejected = Object.fromEntries(Object.entries(R).filter(([, v]) => !v.ok));
  const resolved = Object.keys(R).filter(k => R[k].ok);
  await L.sleep(500);
  const stateBefore = await checkRejections({ dir: dirName });
  return { rejected, resolved, stateBefore };
}

async function checkRejections({ dir: dirName }) {
  const d = await L.dirPath(dirName, false);
  const out = {};
  for await (const [name, h] of d.entries()) {
    if (h.kind === 'directory') { out[name] = { kind: 'dir', entries: await L.listNames(h) }; continue; }
    try { out[name] = { kind: 'file', text: new TextDecoder().decode(await L.readAll(h)) }; }
    catch (e) { out[name] = { kind: 'file', unreadable: L.errInfo(e).name }; }
  }
  return out;
}

// ---------------------------------------------------------------------------------------
// S5 question 2: a lock holder with many handles, and the waiter that gets the lock.
// ---------------------------------------------------------------------------------------
async function q2hold({ lock, dir, n, inflight }) {
  const d = await L.dirPath(dir);
  return new Promise((resolve, reject) => {
    navigator.locks.request(lock, async () => {
      try {
        if (inflight) {
          for (let k = 0; k < inflight; k++) {
            await L.writeFile(d, `m_${k}`, new TextEncoder().encode(`M${k}`));
            await d.getFileHandle(`r_${k}`, { create: true });
          }
        }
        for (let k = 0; k < n; k++) {
          const fh = await d.getFileHandle(`h_${k}`, { create: true });
          keep.handles.set(`${dir}/h_${k}`, await fh.createSyncAccessHandle());
        }
        resolve({ held: n, t: Date.now() });
      } catch (e) { reject(e); }
      await new Promise(() => {}); // holds the lock until the worker ends
    });
  });
}

async function q2wait({ lock, dir, n, listMs }) {
  const d = await L.dirPath(dir);
  const fhs = [];
  for (let k = 0; k < n; k++) fhs.push(await d.getFileHandle(`h_${k}`, { create: true }));
  navigator.locks.request(lock, async () => {
    const tGrant = Date.now();
    const res = await Promise.allSettled(fhs.map(f => f.createSyncAccessHandle()));
    const tDone = Date.now();
    const errors = {};
    const failedIdx = [];
    res.forEach((r, k) => { if (r.status === 'rejected') { const nm = L.errInfo(r.reason).name; errors[nm] = (errors[nm] ?? 0) + 1; failedIdx.push(k); } });
    // How long the failed ones stay taken.
    let retry = null;
    if (failedIdx.length) {
      retry = [];
      const t0 = Date.now();
      let left = failedIdx;
      while (left.length && Date.now() - t0 < 5000) {
        await L.sleep(5);
        const again = await Promise.allSettled(left.map(k => fhs[k].createSyncAccessHandle()));
        const still = [];
        again.forEach((r, j) => { if (r.status === 'fulfilled') res[left[j]] = r; else still.push(left[j]); });
        retry.push({ afterMs: Date.now() - tGrant, stillTaken: still.length });
        left = still;
      }
    }
    for (const r of res) if (r.status === 'fulfilled') r.value.close();
    // The directory, listed repeatedly after the grant (the in-flight operations of the holder).
    let listing = null;
    if (listMs) {
      const first = await L.listNames(d);
      const changes = [];
      let prev = first.join('|');
      const t0 = Date.now();
      let lists = 1;
      while (Date.now() - t0 < listMs) {
        await L.sleep(10);
        const cur = (await L.listNames(d)).join('|');
        lists++;
        if (cur !== prev) { changes.push({ atMs: Date.now() - tGrant, names: cur.split('|') }); prev = cur; }
      }
      listing = { first, lists, changes };
    }
    bus.emit('q2result', { lock, tGrant, openMs: tDone - tGrant, failures: failedIdx.length, errors, retry, listing });
  });
  // Returns once the request is in the queue.
  for (let k = 0; k < 200; k++) {
    const q = await navigator.locks.query();
    if (q.pending.some(p => p.name === lock)) return { queued: true };
    await L.sleep(5);
  }
  return { queued: false };
}

// Fires moves and removes without awaiting them, then ends the worker at once.
function fireAndEnd({ dir, inflight, mode }) {
  return L.dirPath(dir).then(async d => {
    const fhs = [];
    for (let k = 0; k < inflight; k++) fhs.push(await d.getFileHandle(`m_${k}`));
    // Nothing is awaited from here on.
    const t = Date.now();
    for (let k = 0; k < inflight; k++) { fhs[k].move(`mv_${k}`).catch(() => {}); d.removeEntry(`r_${k}`).catch(() => {}); }
    if (mode === 'selfClose') { bus.emit('ending', { mode, t }); self.close(); return 'closed'; }
    self.postMessage({ endNow: mode, t });
    return 'fired';
  });
}

function panicClose() {
  const t = Date.now();
  bus.emit('ending', { mode: 'panicClose', t });
  setTimeout(() => { self.close(); throw new Error('simulated panic, after self.close()'); }, 0);
  return 'scheduled';
}

// ---------------------------------------------------------------------------------------
// S5 questions 3 and 6: locks.
// ---------------------------------------------------------------------------------------
async function lockHold({ lock, holdMs }) {
  return new Promise(resolve => {
    navigator.locks.request(lock, async () => {
      resolve({ t: Date.now() });
      if (holdMs === undefined) await new Promise(() => {});
      await L.sleep(holdMs);
      bus.emit('released', { lock, t: Date.now() });
    });
  });
}
async function lockIfAvailable({ lock }) {
  const t0 = performance.now();
  const got = await navigator.locks.request(lock, { ifAvailable: true }, l => (l === null ? null : l.name));
  return { lockArg: got, ms: +(performance.now() - t0).toFixed(2) };
}
async function lockWaitTimed({ lock }) {
  const t0 = Date.now();
  const tGrant = await navigator.locks.request(lock, () => Date.now());
  return { waitedMs: tGrant - t0, tGrant };
}
function lockWaitReport({ lock }) {
  navigator.locks.request(lock, () => { bus.emit('granted', { lock, tGrant: Date.now() }); });
  return navigator.locks.query().then(q => ({ queued: q.pending.some(p => p.name === lock) }));
}
async function lockAbort({ lock, abortAfterMs }) {
  const ac = new AbortController();
  let ran = false;
  const p = navigator.locks.request(lock, { signal: ac.signal }, () => { ran = true; bus.emit('abortedRequestRan', { lock }); });
  await L.sleep(50);
  const before = (await navigator.locks.query()).pending.filter(x => x.name === lock).length;
  await L.sleep(abortAfterMs);
  const tAbort = performance.now();
  ac.abort();
  const outcome = await p.then(() => ({ resolved: true }), e => ({ rejected: L.errInfo(e) }));
  const settleMs = +(performance.now() - tAbort).toFixed(2);
  const after = (await navigator.locks.query()).pending.filter(x => x.name === lock).length;
  keep.abortRan = () => ran;
  return { pendingBefore: before, pendingAfter: after, outcome, settleMs };
}

// BroadcastChannel between the workers of two contexts.
async function bcSendOrder({ chan, n, bigEvery }) {
  const ch = new BroadcastChannel(chan);
  for (let s = 0; s < n; s++) ch.postMessage(bigEvery && s % bigEvery === 0 ? { s, pad: new Uint8Array(65536) } : { s });
  ch.postMessage({ end: n });
  return { sent: n };
}
function bcRecvOrder({ chan }) {
  const ch = new BroadcastChannel(chan);
  let last = -1, outOfOrder = 0, got = 0;
  return new Promise(resolve => {
    ch.onmessage = e => {
      if (e.data.end !== undefined) { ch.close(); bus.emit('orderResult', { expected: e.data.end, got, outOfOrder }); return; }
      got++; if (e.data.s !== last + 1) outOfOrder++; last = e.data.s;
    };
    resolve('listening');
  });
}
async function bcKeySend({ chan }) {
  const ch = new BroadcastChannel(chan);
  const out = {};
  const key = await crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, new TextEncoder().encode('secret plaintext')));
  out.aesPost = await L.attempt(() => ch.postMessage({ kind: 'aes', key, iv, ct }));
  const hmac = await crypto.subtle.generateKey({ name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify']);
  const sig = new Uint8Array(await crypto.subtle.sign('HMAC', hmac, new TextEncoder().encode('msg')));
  out.hmacPost = await L.attempt(() => ch.postMessage({ kind: 'hmac', key: hmac, sig }));
  const ec = await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign', 'verify']);
  out.ecPrivatePost = await L.attempt(() => ch.postMessage({ kind: 'ec', key: ec.privateKey, pub: ec.publicKey }));
  // Also through the worker's own page (postMessage), for comparison.
  out.postToOwnPage = await L.attempt(() => self.postMessage({ keyTest: true, key }));
  return out;
}
function bcKeyRecv({ chan }) {
  const ch = new BroadcastChannel(chan);
  const got = {};
  ch.onmessage = async e => {
    const m = e.data;
    const r = { isCryptoKey: m.key instanceof CryptoKey, extractable: m.key?.extractable, type: m.key?.type, algorithm: m.key?.algorithm?.name };
    try {
      if (m.kind === 'aes') r.decrypted = new TextDecoder().decode(await crypto.subtle.decrypt({ name: 'AES-GCM', iv: m.iv }, m.key, m.ct));
      if (m.kind === 'hmac') r.verified = await crypto.subtle.verify('HMAC', m.key, m.sig, new TextEncoder().encode('msg'));
      if (m.kind === 'ec') { const s = await crypto.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, m.key, new Uint8Array([1])); r.verified = await crypto.subtle.verify({ name: 'ECDSA', hash: 'SHA-256' }, m.pub, s, new Uint8Array([1])); }
    } catch (err) { r.useError = L.errInfo(err); }
    r.exportAttempt = await L.attempt(() => crypto.subtle.exportKey(m.kind === 'ec' ? 'pkcs8' : 'raw', m.key));
    got[m.kind] = r;
    bus.emit('keyReceived', { kind: m.kind, r });
  };
  return 'listening';
}
function bcEcho({ chan }) {
  const ch = new BroadcastChannel(chan);
  ch.onmessage = e => ch.postMessage({ pong: e.data.ping });
  keep.echo = ch;
  return 'echoing';
}
async function bcPing({ chan, n }) {
  const ch = new BroadcastChannel(chan);
  const rtts = [];
  for (let k = 0; k < n; k++) {
    const t0 = performance.now();
    await new Promise(res => { ch.onmessage = e => { if (e.data.pong === k) res(); }; ch.postMessage({ ping: k }); });
    rtts.push(performance.now() - t0);
  }
  ch.close();
  return L.stats(rtts);
}

// ---------------------------------------------------------------------------------------
// S5 question 4: fetch, read from the body's ReadableStream.
// ---------------------------------------------------------------------------------------
async function streamTimed({ id, chunks, size, interval }) {
  const res = await fetch(`/stream?mode=timed&id=${id}&chunks=${chunks}&size=${size}&interval=${interval}`, { cache: 'no-store' });
  const reader = res.body.getReader();
  const reads = [];
  const tStart = Date.now();
  let total = 0;
  let text = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    const t = Date.now();
    total += value.length;
    text += new TextDecoder().decode(value);
    // Each chunk starts with <k:tSend|.
    const marks = [...text.matchAll(/<(\d+):(\d+)\|/g)].map(m => ({ k: +m[1], tSend: +m[2] }));
    reads.push({ t, bytes: value.length, total, chunksSeenSoFar: marks.length });
  }
  const marks = [...text.matchAll(/<(\d+):(\d+)\|/g)].map(m => ({ k: +m[1], tSend: +m[2] }));
  // For each server chunk, the time of the first read that contained it.
  let seen = 0;
  const perChunk = [];
  for (const r of reads) {
    while (seen < r.chunksSeenSoFar) { perChunk.push({ k: marks[seen].k, tSend: marks[seen].tSend, tRecv: r.t, lagMs: r.t - marks[seen].tSend }); seen++; }
  }
  return { status: res.status, httpVersion: res.headers.get('x-proto'), reads: reads.length, total, chunks: marks.length, perChunk, spanMs: Date.now() - tStart };
}

async function streamAbort({ id, readChunks }) {
  const ac = new AbortController();
  const res = await fetch(`/stream?mode=abort&id=${id}`, { signal: ac.signal, cache: 'no-store' });
  const reader = res.body.getReader();
  let got = 0, bytes = 0;
  while (got < readChunks) { const { value } = await reader.read(); got++; bytes += value.length; }
  const pending = reader.read();
  const tAbort = Date.now();
  ac.abort();
  const outcome = await pending.then(r => ({ resolved: { done: r.done, bytes: r.value?.length } }), e => ({ rejected: L.errInfo(e) }));
  const settled = Date.now() - tAbort;
  const after = await reader.read().then(r => ({ resolved: { done: r.done } }), e => ({ rejected: L.errInfo(e) }));
  return { readsBeforeAbort: got, bytesBeforeAbort: bytes, tAbort, pendingRead: outcome, pendingReadSettledMs: settled, nextRead: after };
}

async function streamFlood({ id, readBytes, pauseMs }) {
  const ac = new AbortController();
  const res = await fetch(`/stream?mode=flood&id=${id}`, { signal: ac.signal, cache: 'no-store' });
  const reader = res.body.getReader();
  let bytes = 0;
  while (bytes < readBytes) { const { value } = await reader.read(); bytes += value.length; }
  const tStop = Date.now();
  await L.log({ event: 'flood-stopped-reading', id, bytesRead: bytes, tStop });
  await L.sleep(pauseMs);
  // Resume: does it flow again?
  let more = 0; const tResume = Date.now();
  while (Date.now() - tResume < 2000 && more < (64 << 20)) { const { value, done } = await reader.read(); if (done) break; more += value.length; }
  const resumeMs = Date.now() - tResume;
  ac.abort();
  return { bytesReadBeforePause: bytes, tStop, pauseMs, bytesReadAfterResume: more, resumeMs };
}

// ---------------------------------------------------------------------------------------
// S5 question 5: timers in the dedicated worker of a background tab.
// ---------------------------------------------------------------------------------------
let tokenSeq = 0;
const tokenWaiters = new Map();
self.addEventListener('message', e => {
  if (e.data?.tokenReply) { const w = tokenWaiters.get(e.data.id); tokenWaiters.delete(e.data.id); w?.(e.data); }
});
function askPage() {
  const id = ++tokenSeq; const t0 = performance.now();
  return new Promise(res => {
    tokenWaiters.set(id, d => res({ rttMs: +(performance.now() - t0).toFixed(2), vis: d.vis, hiddenForMs: d.hiddenForMs, pageSeen: d.t }));
    self.postMessage({ tokenRequest: true, id });
  });
}
async function timerChain({ periodMs, label, totalMs }) {
  const tEnd = Date.now() + totalMs;
  while (Date.now() < tEnd) {
    const t0 = performance.now(); const w0 = Date.now();
    await new Promise(r => setTimeout(r, periodMs));
    const lateMs = +(performance.now() - t0 - periodMs).toFixed(1);
    const page = await askPage();
    await L.log({ event: 'timer', label, periodMs, scheduledAt: w0, lateMs, ...page });
  }
  return 'done';
}
function bgTimers({ totalMs }) {
  timerChain({ periodMs: 30000, label: 'worker-30s', totalMs });
  timerChain({ periodMs: 5000, label: 'worker-5s', totalMs });
  return 'started';
}

// ---------------------------------------------------------------------------------------
const handlers = {
  capabilities, matrix, s3, holdFile, closeFile, tryOpen, openWhenFree,
  writeFlushNoClose, readCheck, killNow, series, checkSeries, rejections, checkRejections,
  q2hold, q2wait, fireAndEnd, panicClose,
  lockHold, lockIfAvailable, lockWaitTimed, lockWaitReport, lockAbort,
  abortRan: async () => keep.abortRan?.(),
  bcSendOrder, bcRecvOrder, bcKeySend, bcKeyRecv, bcEcho, bcPing,
  streamTimed, streamAbort, streamFlood, bgTimers,
  ping: async () => ({ t: Date.now() }),
};
bus.handlers = handlers;

self.onmessage = async e => {
  const { id, cmd, args } = e.data ?? {};
  if (!cmd) return;
  try { self.postMessage({ re: id, ok: true, r: await handlers[cmd](args ?? {}) }); }
  catch (err) { self.postMessage({ re: id, ok: false, err: L.errInfo(err) }); }
};
bus.emit('hello', { name: NAME });
self.postMessage({ hello: NAME });
