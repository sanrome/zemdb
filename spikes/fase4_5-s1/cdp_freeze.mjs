// Chromium only: freezes the page whose URL contains <match> with the CDP's
// Page.setWebLifecycleState for <ms> milliseconds, then makes it active again.
// Prints one JSON line when the freeze is applied, then the rest of its log.
//   node cdp_freeze.mjs <cdp-port> <match> <ms>
const [port, match, ms] = process.argv.slice(2);
const list = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
const target = list.find(t => t.type === 'page' && t.url.includes(match));
if (!target) { console.log(JSON.stringify({ ok: false, why: `no page with ${match}`, pages: list.map(t => t.url) })); process.exit(0); }
const ws = new WebSocket(target.webSocketDebuggerUrl);
let id = 0;
const pending = new Map();
ws.onmessage = e => { const m = JSON.parse(e.data); if (m.id && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); } };
const send = (method, params) => new Promise(res => { const i = ++id; pending.set(i, res); ws.send(JSON.stringify({ id: i, method, params })); });
await new Promise(r => { ws.onopen = r; });
const r1 = await send('Page.setWebLifecycleState', { state: 'frozen' });
const tFrozen = Date.now();
console.log(JSON.stringify({ ok: !r1.error, frozenAt: tFrozen, url: target.url, response: r1 }));
await new Promise(r => setTimeout(r, Number(ms)));
const r2 = await send('Page.setWebLifecycleState', { state: 'active' });
console.log(JSON.stringify({ activeAt: Date.now(), frozenForMs: Date.now() - tFrozen, response: r2 }));
ws.close();
