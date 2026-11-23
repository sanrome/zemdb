// S5 question 4 over HTTP/2 (Chromium only, optional): the same /stream modes as harness.py,
// on node:http2 with a self-signed certificate. Static files from www/; /claim, /alive, /log
// and /kill go to the harness, and the server's own events are POSTed to its /log.
//   node h2server.mjs <harness-port> <h2-port> <cert.pem> <key.pem>
import http2 from 'node:http2';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const [harnessPort, h2Port, certPath, keyPath] = process.argv.slice(2);
const WWW = path.join(path.dirname(fileURLToPath(import.meta.url)), 'www');
const harness = `http://127.0.0.1:${harnessPort}`;
const log = ev => fetch(`${harness}/log`, { method: 'POST', body: JSON.stringify({ proto: 'h2', ...ev }) }).catch(() => {});
const types = { '.js': 'text/javascript', '.html': 'text/html; charset=utf-8', '.wasm': 'application/wasm' };

const server = http2.createSecureServer({ cert: fs.readFileSync(certPath), key: fs.readFileSync(keyPath) });
server.on('stream', async (stream, headers) => {
  const url = new URL(headers[':path'], 'https://localhost');
  stream.on('error', () => {});
  if (['/claim', '/alive', '/log', '/kill', '/upload'].includes(url.pathname)) {
    const chunks = [];
    for await (const c of stream) chunks.push(c);
    const r = await fetch(`${harness}${url.pathname}${url.search}`, { method: headers[':method'], body: headers[':method'] === 'POST' ? Buffer.concat(chunks) : undefined });
    stream.respond({ ':status': r.status, 'content-type': 'application/json', 'cache-control': 'no-store' });
    stream.end(Buffer.from(await r.arrayBuffer()));
    return;
  }
  if (url.pathname === '/stream') return streamMode(stream, url.searchParams);
  const file = path.join(WWW, path.normalize(url.pathname).replace(/^(\.\.[/\\])+/, ''));
  if (!file.startsWith(WWW) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) { stream.respond({ ':status': 404 }); stream.end(); return; }
  stream.respond({ ':status': 200, 'content-type': types[path.extname(file)] ?? 'application/octet-stream', 'cache-control': 'no-store' });
  fs.createReadStream(file).pipe(stream);
});

function streamMode(stream, q) {
  const mode = q.get('mode'), id = q.get('id');
  const t0 = Date.now();
  stream.respond({ ':status': 200, 'content-type': 'application/octet-stream', 'x-proto': 'h2', 'cache-control': 'no-store' });
  let closed = false;
  stream.on('close', () => {
    closed = true;
    log({ event: 'stream-peer-closed', id, how: `stream closed, rstCode ${stream.rstCode}`, t: Date.now(), afterStartMs: Date.now() - t0, sessionStillOpen: !stream.session?.closed });
  });
  if (mode === 'timed') {
    const per = []; let k = 0;
    const n = Number(q.get('chunks') ?? 20), size = Number(q.get('size') ?? 1024), interval = Number(q.get('interval') ?? 250);
    const tick = () => {
      if (closed) return;
      if (k === n) { stream.end(); log({ event: 'stream-sent', id, mode, perChunk: per }); return; }
      const ts = Date.now();
      const head = Buffer.from(`<${k}:${ts}|`);
      stream.write(Buffer.concat([head, Buffer.alloc(Math.max(0, size - head.length), '.')]));
      per.push({ k, tSend: ts }); k++;
      setTimeout(tick, interval);
    };
    tick();
  } else if (mode === 'abort') {
    const iv = setInterval(() => { if (closed) { clearInterval(iv); return; } stream.write(Buffer.from(`<${Date.now()}|`.padEnd(64, '.'))); }, 100);
  } else if (mode === 'flood') {
    const block = Buffer.alloc(65536, 'x');
    let accepted = 0, last = -1, since = Date.now(), stalled = false;
    const mon = setInterval(() => {
      if (closed) { clearInterval(mon); log({ event: 'flood-end', id, sent: accepted, t: Date.now() }); return; }
      if (accepted !== last) { last = accepted; since = Date.now(); if (stalled) { stalled = false; log({ event: 'flood-resumed', id, t: Date.now(), sent: accepted }); } }
      else if (!stalled && Date.now() - since >= 2000) {
        stalled = true;
        log({ event: 'flood-stall', id, t: Date.now(), stalledSinceMs: since, sentAtStall: accepted, streamState: stream.state, sessionState: stream.session?.state });
      }
    }, 100);
    const pump = () => {
      while (!closed) {
        const ok = stream.write(block, () => { accepted += block.length; });
        if (!ok) { stream.once('drain', pump); return; }
      }
    };
    pump();
  }
}
server.listen(Number(h2Port), '127.0.0.1', () => console.log(`h2 on https://localhost:${h2Port}`));
