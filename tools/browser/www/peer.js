// The second page of the harness, which the first page opens: runs in its own Worker the steps
// that the first page sends it, and gives it the answers.
const status = document.getElementById('status');

if (!window.opener) {
  status.textContent = 'This page runs the steps that the first page of the harness sends it: open index.html.';
} else {
  const inFlight = new Set();
  // This load of the page names its Worker, as the first page does.
  const worker = new Worker('worker.js', { type: 'module', name: `second page ${crypto.randomUUID()}` });
  worker.onmessage = ({ data }) => {
    inFlight.delete(data.id);
    window.opener.postMessage(data, location.origin);
  };
  worker.onerror = (event) => {
    for (const id of inFlight) {
      window.opener.postMessage(
        { id, ok: false, error: `the Worker of the second page failed: ${event.message}` },
        location.origin,
      );
    }
    inFlight.clear();
  };
  window.addEventListener('message', (event) => {
    if (event.origin !== location.origin || event.source !== window.opener) {
      return;
    }
    inFlight.add(event.data.id);
    worker.postMessage(event.data);
  });
  window.opener.postMessage({ ready: true }, location.origin);
}
