// S1 question 1: the Rust module (wasm-bindgen + web-sys) in a dedicated module worker.
import init, { s1q1 } from './pkg/s1_opfs.js';
const p = new URL(self.location.href).searchParams;
try {
  await init();
  const report = await s1q1(p.get('phase'), p.get('dir'));
  postMessage({ ok: true, report });
} catch (e) {
  postMessage({ ok: false, err: { name: e?.name, message: String(e?.message ?? e) } });
}
