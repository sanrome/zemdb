#!/usr/bin/env python3
"""Spike harness of the Paso S1 of Fase 4.5 (never merged).

One process: an HTTP server (stdlib ThreadingHTTPServer, http://localhost:<port>) that serves
www/, timestamps the JSON the pages POST to /log, kills the browser on /kill, streams on
/stream, hands each page its test on /claim; and a driver that launches the browser with a URL
and a profile directory, waits for the page's "done" (or kills it), and relaunches it.

    python3 harness.py --browser chromium|firefox|safari --out DIR [--tests a,b,...]
    python3 harness.py --serve-only --out DIR        # server only, for manual runs

Chromium runs headful when DISPLAY is set (xvfb-run), Firefox likewise; Safari with
`open -a Safari URL`, which is normal Safari with persistent storage.
"""
import argparse
import base64
import http.server
import json
import os
import random
import select
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
WWW = os.path.join(HERE, 'www')
BULK_EVENTS = {'series-resolved', 'timer'}


def now_ms():
    return time.time() * 1000


# ------------------------------------------------------------------------------------------
# Events and tokens.
# ------------------------------------------------------------------------------------------
class Hub:
    def __init__(self, out_dir):
        self.cv = threading.Condition()
        self.events = []
        self.tokens = {}
        self.out_dir = out_dir
        self.f = open(os.path.join(out_dir, 'events.jsonl'), 'a')
        self.kill_fn = None
        self.open_fn = None
        self.freeze_fn = None
        self.port = None

    def add(self, ev):
        ev['ts'] = round(now_ms(), 1)
        with self.cv:
            self.events.append(ev)
            self.f.write(json.dumps(ev) + '\n')
            self.f.flush()
            self.cv.notify_all()
        if ev.get('event') not in BULK_EVENTS:
            s = json.dumps(ev)
            print('EV ' + (s if len(s) < 4000 else s[:4000] + '...'), flush=True)

    def wait(self, pred, timeout):
        deadline = time.time() + timeout
        i = 0
        with self.cv:
            while True:
                while i < len(self.events):
                    if pred(self.events[i]):
                        return self.events[i]
                    i += 1
                rem = deadline - time.time()
                if rem <= 0:
                    return None
                self.cv.wait(rem)

    def of_token(self, token, event=None):
        with self.cv:
            return [e for e in self.events if e.get('token') == token and (event is None or e.get('event') == event)]

    def new_token(self, test, args):
        token = uuid.uuid4().hex[:12]
        with self.cv:
            self.tokens[token] = {'test': test, 'args': args, 'claimed': False, 'active': True}
        return token

    def claim(self, token):
        with self.cv:
            t = self.tokens.get(token)
            if not t or t['claimed'] or not t['active']:
                return None
            t['claimed'] = True
            return t

    def alive(self, token):
        with self.cv:
            t = self.tokens.get(token)
            return bool(t and t['active'])

    def finish(self, token):
        with self.cv:
            if token in self.tokens:
                self.tokens[token]['active'] = False


HUB = None
PAYLOAD_DIR = None


# ------------------------------------------------------------------------------------------
# The server.
# ------------------------------------------------------------------------------------------
def sock_queues(port):
    """The kernel's queues of the sockets on `port` (both ends are on loopback)."""
    try:
        if sys.platform == 'darwin':
            out = subprocess.run(['netstat', '-an', '-p', 'tcp'], capture_output=True, text=True, timeout=5).stdout
            return [l for l in out.splitlines() if f'.{port} ' in l]
        # /proc/net/tcp: local and remote address:port in hex, then tx_queue:rx_queue.
        rows = []
        hp = f'{port:04X}'
        for path in ('/proc/net/tcp', '/proc/net/tcp6'):
            try:
                lines = open(path).read().splitlines()[1:]
            except OSError:
                continue
            for l in lines:
                f = l.split()
                if f[1].endswith(':' + hp) or f[2].endswith(':' + hp):
                    tx, rx = (int(x, 16) for x in f[4].split(':'))
                    side = 'server' if f[1].endswith(':' + hp) else 'browser'
                    rows.append(f'{side} socket {f[1]}->{f[2]} state {f[3]} send-queue {tx} recv-queue {rx}')
        return rows
    except Exception as e:  # noqa: BLE001
        return [f'error: {e}']


class Handler(http.server.SimpleHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    extensions_map = {**http.server.SimpleHTTPRequestHandler.extensions_map,
                      '.js': 'text/javascript', '.mjs': 'text/javascript', '.wasm': 'application/wasm',
                      '.html': 'text/html; charset=utf-8', '': 'application/octet-stream'}

    def __init__(self, *a, **kw):
        super().__init__(*a, directory=WWW, **kw)

    def log_message(self, fmt, *args):
        pass

    def end_headers(self):
        self.send_header('Cache-Control', 'no-store')
        super().end_headers()

    def _json(self, obj, code=200):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _body(self):
        n = int(self.headers.get('Content-Length') or 0)
        return self.rfile.read(n) if n else b''

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = dict(urllib.parse.parse_qsl(u.query))
        if u.path == '/claim':
            t = HUB.claim(q.get('token'))
            HUB.add({'event': 'claim', 'token': q.get('token'), 'ok': bool(t)})
            return self._json({'ok': True, 'test': t['test'], 'args': t['args']} if t else {'ok': False})
        if u.path == '/alive':
            return self._json({'ok': HUB.alive(q.get('token'))})
        if u.path == '/stream':
            return self._stream(q)
        if u.path.startswith('/payload/'):
            return self._payload(u.path[len('/payload/'):])
        return super().do_GET()

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        q = dict(urllib.parse.parse_qsl(u.query))
        body = self._body()
        if u.path == '/log':
            try:
                ev = json.loads(body or b'{}')
            except ValueError:
                ev = {'raw': body[:200].decode(errors='replace')}
            HUB.add(ev)
            return self._json({})
        if u.path == '/kill':
            ev = json.loads(body or b'{}')
            ev['event'] = ev.get('event') or 'kill-requested'
            ev['kill'] = True
            HUB.add(ev)
            HUB.add({'event': 'kill-start', 'token': q.get('token')})
            if HUB.kill_fn:
                HUB.kill_fn()
            HUB.add({'event': 'killed', 'token': q.get('token')})
            try:
                return self._json({'killed': True})
            except OSError:
                return None
        if u.path == '/upload':
            d = os.path.join(HUB.out_dir, 'uploads')
            os.makedirs(d, exist_ok=True)
            name = os.path.basename(q.get('name', 'upload.bin'))
            with open(os.path.join(d, name), 'wb') as f:
                f.write(body)
            HUB.add({'event': 'upload', 'token': q.get('token'), 'name': name, 'bytes': len(body)})
            return self._json({'bytes': len(body), 'name': name})
        if u.path == '/driver/open':
            ok = HUB.open_fn(q['url']) if HUB.open_fn else False
            HUB.add({'event': 'driver-open', 'token': q.get('token'), 'url': q['url'], 'ok': ok})
            return self._json({'ok': ok})
        if u.path == '/driver/freeze':
            r = HUB.freeze_fn(q['match'], int(q['ms'])) if HUB.freeze_fn else {'ok': False, 'why': 'no CDP'}
            HUB.add({'event': 'driver-freeze', 'token': q.get('token'), **r})
            return self._json(r)
        self.send_error(404)

    def _payload(self, name):
        p = os.path.join(PAYLOAD_DIR or '', os.path.basename(name))
        if not PAYLOAD_DIR or not os.path.exists(p):
            return self.send_error(404)
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Length', str(os.path.getsize(p)))
        self.end_headers()
        with open(p, 'rb') as f:
            shutil.copyfileobj(f, self.wfile, 1 << 20)

    # Q4 -------------------------------------------------------------------------------
    def _chunk(self, data):
        self.wfile.write(b'%x\r\n' % len(data) + data + b'\r\n')
        self.wfile.flush()

    def _peer_closed(self):
        try:
            r, _, _ = select.select([self.connection], [], [], 0)
            if r:
                return self.connection.recv(1, socket.MSG_PEEK) == b''
        except OSError:
            return True
        return False

    def _stream(self, q):
        mode, sid = q.get('mode'), q.get('id')
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Transfer-Encoding', 'chunked')
        self.send_header('X-Proto', 'HTTP/1.1')
        self.end_headers()
        self.close_connection = True
        sent = 0
        t0 = now_ms()
        try:
            if mode == 'timed':
                per = []
                size = int(q.get('size', 1024))
                for k in range(int(q.get('chunks', 20))):
                    ts = int(now_ms())
                    head = f'<{k}:{ts}|'.encode()
                    data = head + b'.' * max(0, size - len(head))
                    self._chunk(data)
                    sent += len(data)
                    per.append({'k': k, 'tSend': ts})
                    time.sleep(int(q.get('interval', 250)) / 1000)
                self.wfile.write(b'0\r\n\r\n')
                self.wfile.flush()
                HUB.add({'event': 'stream-sent', 'id': sid, 'mode': mode, 'perChunk': per, 'bytes': sent})
            elif mode == 'abort':
                end = time.time() + 60
                while time.time() < end:
                    data = f'<{int(now_ms())}|'.encode().ljust(64, b'.')
                    self._chunk(data)
                    sent += len(data)
                    for _ in range(10):
                        if self._peer_closed():
                            HUB.add({'event': 'stream-peer-closed', 'id': sid, 'how': 'FIN seen', 't': now_ms(),
                                     'bytesSent': sent, 'afterStartMs': now_ms() - t0})
                            return
                        time.sleep(0.01)
                HUB.add({'event': 'stream-peer-closed', 'id': sid, 'how': 'never (60 s)', 'bytesSent': sent})
            elif mode == 'flood':
                state = {'sent': 0, 'done': False}
                port = HUB.port
                cport = self.client_address[1]

                def queues():
                    # Only the flood's connection: the lines that name its client port.
                    hexp = f':{cport:04X}'
                    return [l for l in sock_queues(port) if hexp in l or f'.{cport} ' in l]

                def monitor():
                    last, since, stalled = -1, time.time(), False
                    samples = []
                    while not state['done']:
                        time.sleep(0.1)
                        s = state['sent']
                        samples.append((round(now_ms()), s))
                        if s != last:
                            last, since = s, time.time()
                            if stalled:
                                HUB.add({'event': 'flood-resumed', 'id': sid, 't': now_ms(), 'sent': s})
                                stalled = False
                        elif not stalled and time.time() - since >= 2.0:
                            stalled = True
                            HUB.add({'event': 'flood-stall', 'id': sid, 't': now_ms(), 'stalledSinceMs': since * 1000,
                                     'sentAtStall': s, 'clientPort': cport, 'queues': queues()})
                    HUB.add({'event': 'flood-samples', 'id': sid, 'samples': samples[::5]})
                threading.Thread(target=monitor, daemon=True).start()
                block = b'x' * 65536
                try:
                    while state['sent'] < (16 << 30):
                        self._chunk(block)
                        state['sent'] += len(block)
                finally:
                    state['done'] = True
                    HUB.add({'event': 'flood-end', 'id': sid, 'sent': state['sent'], 't': now_ms()})
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            HUB.add({'event': 'stream-peer-closed', 'id': sid, 'how': f'write error: {type(e).__name__}', 't': now_ms(),
                     'bytesSent': sent, 'afterStartMs': now_ms() - t0})


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        # A browser killed mid-request resets its connections: not an error of the harness.
        if not isinstance(sys.exc_info()[1], (ConnectionResetError, BrokenPipeError)):
            super().handle_error(request, client_address)

    allow_reuse_address = True
    request_queue_size = 128


def start_server(port):
    srv = Server(('127.0.0.1', port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    HUB.port = srv.server_address[1]
    return srv


# ------------------------------------------------------------------------------------------
# Browsers.
# ------------------------------------------------------------------------------------------
FIREFOX_PREFS = {
    'browser.shell.checkDefaultBrowser': False,
    'browser.startup.homepage_override.mstone': 'ignore',
    'startup.homepage_welcome_url': '',
    'startup.homepage_welcome_url.additional': '',
    'browser.aboutwelcome.enabled': False,
    'trailhead.firstrun.didSeeAboutWelcome': True,
    'datareporting.policy.dataSubmissionEnabled': False,
    'datareporting.policy.dataSubmissionPolicyBypassNotification': True,
    'toolkit.telemetry.reportingpolicy.firstRun': False,
    'browser.sessionstore.resume_from_crash': False,
    'browser.sessionstore.max_resumed_crashes': 0,
    'toolkit.startup.max_resumed_crashes': -1,
    'browser.startup.page': 0,
    'browser.tabs.warnOnClose': False,
    'browser.warnOnQuit': False,
    'dom.disable_open_during_load': False,
    'dom.popup_maximum': 10000,
    'browser.link.open_newwindow': 3,
    'browser.link.open_newwindow.restriction': 0,
    'browser.tabs.loadDivertedInBackground': False,
    'app.update.enabled': False,
    'app.update.auto': False,
    'browser.safebrowsing.malware.enabled': False,
    'browser.safebrowsing.phishing.enabled': False,
    'extensions.update.enabled': False,
    'browser.newtabpage.enabled': False,
}


class Browser:
    def __init__(self, kind, profile, binary=None, cdp=False):
        self.kind, self.profile, self.binary, self.cdp = kind, profile, binary, cdp
        self.proc = None
        self.logf = None
        self.cdp_port = None
        os.makedirs(profile, exist_ok=True)
        if kind == 'firefox':
            with open(os.path.join(profile, 'user.js'), 'w') as f:
                for k, v in FIREFOX_PREFS.items():
                    f.write(f'user_pref({json.dumps(k)}, {json.dumps(v)});\n')

    def version(self):
        try:
            if self.kind == 'safari':
                p = '/Applications/Safari.app/Contents/Info.plist'
                v = subprocess.run(['defaults', 'read', p, 'CFBundleShortVersionString'], capture_output=True, text=True).stdout.strip()
                b = subprocess.run(['defaults', 'read', p, 'CFBundleVersion'], capture_output=True, text=True).stdout.strip()
                return f'Safari {v} ({b})'
            return subprocess.run([self.binary, '--version'], capture_output=True, text=True, timeout=30).stdout.strip()
        except Exception as e:  # noqa: BLE001
            return f'unknown: {e}'

    def launch(self, url):
        self.logf = open(os.path.join(HUB.out_dir, f'{self.kind}-stderr.log'), 'a')
        if self.kind == 'chromium':
            args = [self.binary, f'--user-data-dir={self.profile}', '--no-first-run', '--no-default-browser-check',
                    '--disable-popup-blocking', '--password-store=basic', '--use-mock-keychain', '--no-sandbox',
                    '--disable-search-engine-choice-screen']
            if self.cdp:
                args += ['--remote-debugging-port=0', '--remote-allow-origins=*']
            args += getattr(self, 'extra', [])
            if not os.environ.get('DISPLAY'):
                args.append('--headless=new')
            args.append(url)
        elif self.kind == 'firefox':
            args = [self.binary, '-profile', self.profile, '-no-remote', '-new-instance']
            if not os.environ.get('DISPLAY'):
                args.append('-headless')
            args.append(url)
        elif self.kind == 'safari':
            subprocess.run(['open', '-a', 'Safari', url], check=False, stdout=self.logf, stderr=self.logf)
            return
        if self.cdp:
            try:
                os.unlink(os.path.join(self.profile, 'DevToolsActivePort'))
            except OSError:
                pass
        self.proc = subprocess.Popen(args, start_new_session=True, stdout=self.logf, stderr=self.logf)
        if self.cdp:
            port_file = os.path.join(self.profile, 'DevToolsActivePort')
            for _ in range(200):
                try:
                    with open(port_file) as f:
                        self.cdp_port = int(f.readline().strip())
                    break
                except (OSError, ValueError):
                    time.sleep(0.05)

    def open_tab(self, url):
        if self.kind == 'safari':
            r = subprocess.run(['open', '-a', 'Safari', url], capture_output=True, text=True)
            return r.returncode == 0
        return False

    def kill(self):
        if self.kind == 'safari':
            subprocess.run(['pkill', '-9', '-x', 'Safari'], check=False)
            subprocess.run(['pkill', '-9', '-f', 'com.apple.WebKit.'], check=False)
            for _ in range(100):
                if subprocess.run(['pgrep', '-x', 'Safari'], capture_output=True).returncode != 0:
                    break
                time.sleep(0.05)
            return
        if self.proc:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                pass
            self.proc = None
        # Stragglers that left the process group.
        subprocess.run(['pkill', '-9', '-f', self.profile], check=False)
        if os.path.exists(os.path.join(self.profile, 'SingletonLock')):
            for n in ('SingletonLock', 'SingletonSocket', 'SingletonCookie'):
                try:
                    os.unlink(os.path.join(self.profile, n))
                except OSError:
                    pass
        time.sleep(0.3)

    def freeze(self, match, ms):
        if self.kind != 'chromium' or not self.cdp_port:
            return {'ok': False, 'why': 'CDP only in Chromium'}
        p = subprocess.Popen([os.environ.get('NODE', 'node'), os.path.join(HERE, 'cdp_freeze.mjs'), str(self.cdp_port), match, str(ms)],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        line = p.stdout.readline().strip()
        threading.Thread(target=lambda: HUB.add({'event': 'cdp-freeze-log', 'out': p.stdout.read()}), daemon=True).start()
        try:
            return json.loads(line)
        except ValueError:
            return {'ok': False, 'why': line}


def screenshot(label):
    """macOS only: a small JPEG of the screen, base64 in the log (for first-run dialogs)."""
    if sys.platform != 'darwin':
        return
    png = os.path.join(HUB.out_dir, f'screen-{label}.png')
    jpg = os.path.join(HUB.out_dir, f'screen-{label}.jpg')
    subprocess.run(['screencapture', '-x', png], check=False)
    subprocess.run(['sips', '-Z', '900', '-s', 'format', 'jpeg', '-s', 'formatOptions', '40', png, '--out', jpg],
                   capture_output=True, check=False)
    if os.path.exists(jpg):
        b = base64.b64encode(open(jpg, 'rb').read()).decode()
        print(f'SCREENSHOT {label} BEGIN', flush=True)
        for i in range(0, len(b), 1000):
            print('SS ' + b[i:i + 1000])
        print(f'SCREENSHOT {label} END', flush=True)


# ------------------------------------------------------------------------------------------
# The driver.
# ------------------------------------------------------------------------------------------
class Driver:
    def __init__(self, browser, summary_path):
        self.b = browser
        self.summary = {}
        self.summary_path = summary_path
        self.first_launch = True

    def url(self, token):
        return f'http://localhost:{HUB.port}/run.html?token={token}'

    def save(self, key, value):
        self.summary[key] = value
        with open(self.summary_path, 'w') as f:
            json.dump(self.summary, f, indent=1)
        s = json.dumps({key: value})
        print('SUMMARY ' + (s if len(s) < 6000 else s[:6000] + '...(full in summary.json)'), flush=True)

    def run(self, test, args, timeout, until=('done',), keep=False):
        """Launches the browser on a page that runs `test`; waits for one of `until` events."""
        token = HUB.new_token(test, args)
        t0 = time.time()
        self.b.launch(self.url(token))
        if self.first_launch:
            self.first_launch = False
            time.sleep(4)
            screenshot('first-launch')
        ev = HUB.wait(lambda e: e.get('token') == token and e.get('event') in until, timeout)
        if ev is None:
            screenshot(f'timeout-{test}')
        results = [e.get('result') for e in HUB.of_token(token, 'result')]
        errors = [e for e in HUB.of_token(token, 'error')]
        if not keep:
            self.b.kill()
        HUB.finish(token)
        how = [{'popupWorks': e.get('popupWorks'), 'driverOpenWorks': e.get('driverOpenWorks')} for e in HUB.of_token(token, 'result')
               if 'popupWorks' in e or 'driverOpenWorks' in e]
        return {'token': token, 'ended': ev.get('event') if ev else 'timeout', 'secs': round(time.time() - t0, 1),
                'results': results, 'errors': errors, 'how': how}

    # --- the tests -------------------------------------------------------------------
    def t_info(self):
        r = self.run('info', {}, 60)
        self.save('info', r)

    def t_s1q1(self):
        dirn = f's1q1-{uuid.uuid4().hex[:6]}'
        verify = HUB.new_token('s1q1verify', {'dir': dirn})
        # The write phase navigates to the verify phase: wait for the verify page's "done".
        token = HUB.new_token('s1q1', {'dir': dirn, 'verifyToken': verify})
        self.b.launch(self.url(token))
        ev = HUB.wait(lambda e: e.get('token') == verify and e.get('event') == 'done', 90)
        write = [e.get('result') for e in HUB.of_token(token, 'result')]
        errs = HUB.of_token(token, 'error') + HUB.of_token(verify, 'error')
        ver = [e.get('result') for e in HUB.of_token(verify, 'result')]
        self.b.kill()
        HUB.finish(token)
        HUB.finish(verify)
        self.save('s1q1', {'write': write, 'verifyAfterReload': ver, 'errors': errs, 'ended': ev and ev.get('event')})

    def t_matrix(self):
        r = self.run('matrix', {'driverOpen': self.b.kind == 'safari'}, 240)
        self.save('matrix', r)

    def t_s1q2(self):
        dirn = f's1q2-{uuid.uuid4().hex[:6]}'
        r1 = self.run('s1q2', {'dir': dirn, 'driverOpen': self.b.kind == 'safari'}, 120, until=('killed', 'error'))
        r2 = self.run('s1q2check', {'dir': dirn}, 60)
        self.save('s1q2', {'beforeKill': r1, 'afterKill': r2})

    def series_once(self, dirn, cycles, kinds, kill_after_last=False, kill_at=None):
        """One series; killed by the page (kill_after_last), at `kill_at` s after its setup, or
        never; then the profile reopened and the directory checked."""
        args = {'dir': dirn, 'cycles': cycles, 'kinds': kinds, 'killAfterLast': kill_after_last}
        token = HUB.new_token('series', args)
        self.b.launch(self.url(token))
        setup = HUB.wait(lambda e: e.get('token') == token and e.get('event') in ('series-setup-done', 'error'), 120)
        info = {'dir': dirn, 'cycles': cycles, 'kinds': kinds}
        if not setup or setup.get('event') != 'series-setup-done':
            self.b.kill()
            HUB.finish(token)
            return {**info, 'error': 'no setup', 'ev': setup}
        if kill_after_last:
            HUB.wait(lambda e: e.get('token') == token and e.get('event') == 'killed', 60)
            info['killedBy'] = 'page, by a synchronous XHR right after the last promise resolved'
        elif kill_at is not None:
            fin = HUB.wait(lambda e: e.get('token') == token and e.get('event') == 'series-finished', kill_at)
            t_kill = now_ms()
            self.b.kill()
            info['killAtMsAfterSetup'] = round(t_kill - setup['ts'])
            info['finishedBeforeKill'] = fin is not None
        else:
            fin = HUB.wait(lambda e: e.get('token') == token and e.get('event') in ('series-finished', 'error'), 600)
            info['finished'] = fin
            self.b.kill()
        HUB.finish(token)
        reported = sorted({r['seq'] for e in HUB.of_token(token, 'series-resolved') for r in e.get('batch', [])})
        info['reportedCount'] = len(reported)
        info['reportedMax'] = reported[-1] if reported else 0
        chk = self.run('checkSeries', {'dir': dirn}, 120)
        state = chk['results'][0] if chk['results'] else None
        if state is None:
            return {**info, 'error': 'no check', 'check': chk}
        return {**info, **analyze_series(cycles, kinds, reported, state)}

    def t_s5q1a(self, reps=3):
        rows = []
        variants = {'move': ['tmpCreate', 'tmpWritten', 'move'], 'createFile': ['createFile'], 'createDir': ['createDir'],
                    'remove': ['remove'], 'flushNoClose': ['flushNoClose']}
        for name, kinds in variants.items():
            for rep in range(reps):
                r = self.series_once(f's5a-{name}-{rep}-{uuid.uuid4().hex[:4]}', 1, kinds, kill_after_last=True)
                r['variant'] = name
                rows.append(r)
                print('S5Q1a', json.dumps({k: r.get(k) for k in ('variant', 'reportedMax', 'visiblePrefix', 'nonPrefixCount', 'reportedButNotVisibleCount', 'targetValid', 'targetGen', 'anomalies')}), flush=True)
        self.save('s5q1a', rows)

    def t_s5q1b(self, iters=20, cycles=300):
        kinds = ['tmpCreate', 'tmpWritten', 'move', 'createFile', 'createDir', 'remove']
        cal = self.series_once(f's5b-cal-{uuid.uuid4().hex[:4]}', cycles, kinds)
        fin = cal.get('finished') or {}
        series_ms = fin.get('ms') or 5000
        rows = []
        for it in range(iters):
            kill_at = random.uniform(0.02, 0.95) * series_ms / 1000
            r = self.series_once(f's5b-{it}-{uuid.uuid4().hex[:4]}', cycles, kinds, kill_at=kill_at)
            r['iter'] = it
            rows.append(r)
            print('S5Q1b', json.dumps({k: v for k, v in r.items() if k != 'visible'})[:800], flush=True)
        self.save('s5q1b', {'calibration': {k: v for k, v in cal.items() if k != 'visible'}, 'seriesMs': series_ms, 'rows': rows})

    def t_s5q1rej(self):
        dirn = f's5rej-{uuid.uuid4().hex[:6]}'
        r1 = self.run('rejections', {'dir': dirn}, 120, until=('killed', 'error'))
        r2 = self.run('checkRejections', {'dir': dirn}, 60)
        before = (r1['results'][0] or {}).get('stateBefore') if r1['results'] else None
        after = r2['results'][0] if r2['results'] else None
        self.save('s5q1rej', {'beforeKill': r1, 'afterKill': r2, 'sameAfterKill': before == after,
                              'diff': diff_states(before, after), 'effectsBeforeKill': rejection_effects(before),
                              'effectsAfterKill': rejection_effects(after)})

    def t_q2(self, iters=20):
        r = self.run('q2', {'modes': ['terminate', 'closeTab', 'reload', 'panicClose'], 'iters': iters, 'n': 50,
                            'inflightModes': ['terminate', 'selfClose', 'closeTab', 'reload'], 'inflightIters': iters,
                            'inflightOps': 20, 'driverOpen': self.b.kind == 'safari'}, 3000)
        self.save('q2', summarize_q2(r))

    def t_q3(self):
        self.save('q3', self.run('q3', {'driverOpen': self.b.kind == 'safari'}, 300))

    def t_q4(self):
        r = self.run('q4', {}, 180)
        server = [e for e in HUB.events if str(e.get('id', '')).startswith(r['token']) and e.get('event') != 'flood-samples']
        self.save('q4', {'page': r, 'server': server})

    def t_q4h2(self):
        """Chromium only: question 4 over HTTP/2 (node:http2, a self-signed certificate)."""
        d = os.path.join(HUB.out_dir, 'h2cert')
        os.makedirs(d, exist_ok=True)
        cert, key = os.path.join(d, 'cert.pem'), os.path.join(d, 'key.pem')
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key, '-out', cert, '-days', '2',
                        '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'], capture_output=True, check=True)
        with socket.socket() as so:
            so.bind(('127.0.0.1', 0))
            h2port = so.getsockname()[1]
        p = subprocess.Popen([os.environ.get('NODE', 'node'), os.path.join(HERE, 'h2server.mjs'), str(HUB.port), str(h2port), cert, key],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(p.stdout.readline().strip(), flush=True)
        self.b.extra = ['--ignore-certificate-errors']
        token = HUB.new_token('q4', {})
        try:
            self.b.launch(f'https://localhost:{h2port}/run.html?token={token}')
            ev = HUB.wait(lambda e: e.get('token') == token and e.get('event') == 'done', 180)
        finally:
            self.b.kill()
            self.b.extra = []
            p.kill()
        HUB.finish(token)
        r = {'token': token, 'ended': ev and ev.get('event'), 'results': [e.get('result') for e in HUB.of_token(token, 'result')],
             'errors': HUB.of_token(token, 'error')}
        server = [e for e in HUB.events if str(e.get('id', '')).startswith(token)]
        self.save('q4h2', {'page': r, 'server': server})

    def t_q5(self, visible_ms=60000, hidden_ms=390000):
        total = visible_ms + hidden_ms + 30000
        r = self.run('q5', {'visibleMs': visible_ms, 'hiddenMs': hidden_ms, 'totalMs': total,
                            'driverOpen': self.b.kind == 'safari'}, total / 1000 + 180)
        timers = [e for e in HUB.of_token(r['token']) if e.get('event') in ('timer', 'visibility', 'visibility-after-open', 'blank-open', 'blank-closing')]
        self.save('q5', {'page': r, 'events': timers, 'summary': summarize_q5(timers)})

    def t_q6(self):
        self.save('q6', self.run('q6', {'handoverIters': 20, 'driverOpen': self.b.kind == 'safari'}, 900))

    def t_freeze(self):
        self.save('freeze', self.run('freeze', {'iters': 3, 'holdMs': 1000, 'freezeMs': 5000}, 300))

    def t_prerender(self):
        r = self.run('prerender', {'waitMs': 4000}, 60)
        evs = [e for e in HUB.of_token(r['token']) if str(e.get('event', '')).startswith('pr-')]
        self.save('prerender', {'page': r, 'events': evs})

    def t_zstd(self):
        wasm = os.path.getsize(os.path.join(WWW, 'zstd', 'zstd_bench.wasm'))
        loads = [self.run('zstd', {'runs': 3, 'load': n, 'wasmBytes': wasm}, 600) for n in (1, 2)]
        self.save('zstd', loads)

    def t_opfs_normal_mark(self):
        self.save('opfsMarkNormal', self.run('opfsMark', {'mark': 'normal-session'}, 60))


def analyze_series(cycles, kinds, reported, state):
    tgt = state.get('target') or {}
    tg = tgt.get('gen') or {}
    tgen = tg.get('gen') if tg.get('valid') else None
    fl = (state.get('fl') or {}).get('gen') or {}
    flgen = fl.get('gen') if fl.get('valid') else None

    def entry(n):
        return state.get(n)

    def vis(kind, i):
        moved = tgen is not None and tgen >= i
        if kind == 'tmpCreate':
            return entry(f't_{i}') is not None or moved
        if kind == 'tmpWritten':
            t = entry(f't_{i}')
            return moved or bool(t and (t.get('gen') or {}).get('valid') and t['gen']['gen'] == i)
        if kind == 'move':
            return moved
        if kind == 'createFile':
            return (entry(f'f_{i}') or {}).get('kind') == 'file'
        if kind == 'createDir':
            return (entry(f'd_{i}') or {}).get('kind') == 'dir'
        if kind == 'remove':
            return entry(f'r_{i}') is None
        if kind == 'flushNoClose':
            return flgen is not None and flgen >= i
        return None

    ops = [(k, i) for i in range(1, cycles + 1) for k in kinds]
    visible = [vis(k, i) for (k, i) in ops]
    prefix = next((j for j, v in enumerate(visible) if not v), len(visible))
    non_prefix = [j + 1 for j in range(prefix, len(visible)) if visible[j]]
    missing = [s for s in reported if not visible[s - 1]]
    anomalies = []
    if tgen is None:
        anomalies.append({'target': tgt or 'missing'})
    for i in range(1, cycles + 1):
        t = entry(f't_{i}')
        if t is not None and tgen is not None and tgen >= i:
            anomalies.append({'tempStillThereAfterItsMove': i})
    if 'flushNoClose' in kinds and flgen is None:
        anomalies.append({'fl': state.get('fl')})
    partial_temps = [n for n, e in state.items() if n.startswith('t_') and not ((e.get('gen') or {}).get('valid'))]
    return {'opsTotal': len(ops), 'visiblePrefix': prefix, 'nonPrefixVisible': non_prefix[:20],
            'nonPrefixCount': len(non_prefix), 'reportedButNotVisible': missing[:20], 'reportedButNotVisibleCount': len(missing),
            'targetValid': tgen is not None, 'targetGen': tgen, 'target': tgt, 'anomalies': anomalies[:20],
            'tempsWithIncompleteContent': partial_temps, 'entries': len(state),
            'inFlightVisible': max(0, prefix - (max(reported) if reported else 0))}


# What the directory of the rejections must hold if no rejected operation had an effect.
REJ_EXPECTED = {'srcLocked': 'SRC-LOCKED', 'src2': 'SRC2', 'dstLocked': 'DST-LOCKED', 'src4': 'SRC4', 'src5': 'SRC5',
                'src6': 'SRC6', 'afile': 'AFILE', 'removeLocked': 'RM-LOCKED', 'removeWritable': 'RM-WRITABLE',
                'adir': [], 'full': ['F:x']}


def rejection_effects(state):
    if state is None:
        return 'missing state'
    out = {'unexpectedEntries': [], 'missingEntries': [], 'changedEntries': []}
    for name, e in state.items():
        if name not in REJ_EXPECTED:
            if name.endswith('.crswap'):
                continue  # Chromium's swap file of the open writable stream
            out['unexpectedEntries'].append({name: e})
        else:
            exp = REJ_EXPECTED[name]
            got = e.get('entries') if isinstance(exp, list) else e.get('text')
            if got != exp:
                out['changedEntries'].append({name: e})
    out['missingEntries'] = [n for n in REJ_EXPECTED if n not in state]
    out['noEffect'] = not (out['unexpectedEntries'] or out['missingEntries'] or out['changedEntries'])
    return out


def diff_states(a, b):
    if a is None or b is None:
        return 'missing state'
    out = {}
    for k in sorted(set(a) | set(b)):
        if a.get(k) != b.get(k):
            out[k] = {'beforeKill': a.get(k), 'afterKill': b.get(k)}
    return out


def summarize_q2(r):
    res = (r.get('results') or [None])[0] or {}
    rows = res.get('rows', [])
    by = {}
    for row in rows:
        m = by.setdefault(row['mode'], {'iters': 0, 'itersWithTakenHandles': 0, 'handlesTaken': 0, 'errors': {},
                                         'handoverMs': [], 'kinds': set(), 'problems': []})
        m['iters'] += 1
        m['kinds'].add(f"A:{row.get('aKind')} B:{row.get('bKind')}")
        if row.get('failures'):
            m['itersWithTakenHandles'] += 1
            m['handlesTaken'] += row['failures']
            for k, v in (row.get('errors') or {}).items():
                m['errors'][k] = m['errors'].get(k, 0) + v
        if row.get('handoverMs') is not None:
            m['handoverMs'].append(row['handoverMs'])
        if row.get('error') or row.get('endMissing') or not row.get('queued'):
            m['problems'].append({k: row.get(k) for k in ('i', 'error', 'endMissing', 'queued')})
    for m in by.values():
        hs = sorted(m['handoverMs'])
        m['handoverMs'] = {'min': hs[0], 'p50': hs[len(hs) // 2], 'max': hs[-1]} if hs else None
        m['kinds'] = sorted(m['kinds'])
    inf = {}
    for row in res.get('inflight', []):
        m = inf.setdefault(row['mode'], {'iters': 0, 'itersWithChangesAfterGrant': 0, 'landedAtGrant': [], 'errors': 0, 'changes': []})
        m['iters'] += 1
        if row.get('changesAfterGrant'):
            m['itersWithChangesAfterGrant'] += 1
            m['changes'].append(row['changesAfterGrant'][:2])
        if row.get('error'):
            m['errors'] += 1
        m['landedAtGrant'].append(row.get('landedAtGrant'))
    return {'run': {k: r[k] for k in ('token', 'ended', 'secs', 'errors', 'how')}, 'ended': r['ended'], 'secs': r['secs'],
            'errors': r['errors'], 'how': r['how'], 'byMode': by, 'inflight': inf, 'rawRows': rows,
            'rawInflight': res.get('inflight')}


def summarize_q5(evs):
    out = {}
    for e in evs:
        if e.get('event') != 'timer':
            continue
        key = f"{e['label']}|{e.get('vis')}|{'hidden>5min' if e.get('hiddenForMs', 0) > 300000 else ('hidden<=5min' if e.get('vis') == 'hidden' else 'visible')}"
        m = out.setdefault(key, {'n': 0, 'lateMs': [], 'rttMs': []})
        m['n'] += 1
        m['lateMs'].append(e.get('lateMs'))
        if e.get('rttMs') is not None:
            m['rttMs'].append(e.get('rttMs'))
    for m in out.values():
        for k in ('lateMs', 'rttMs'):
            xs = sorted(x for x in m[k] if x is not None)
            m[k] = {'min': xs[0], 'p50': xs[len(xs) // 2], 'max': xs[-1]} if xs else None
    return out


# ------------------------------------------------------------------------------------------
def safaridriver_check(drv):
    """Do Safari's WebDriver sessions keep the OPFS between sessions, and see the normal one's?"""
    out = {}
    drv.b.kill()
    sd = subprocess.Popen(['safaridriver', '-p', '4444'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    time.sleep(2)

    def wd(method, path, body=None):
        req = urllib.request.Request(f'http://127.0.0.1:4444{path}', method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read() or b'{}')

    try:
        for n in (1, 2):
            s = wd('POST', '/session', {'capabilities': {'alwaysMatch': {'browserName': 'safari'}}})
            sid = s['value']['sessionId']
            token = HUB.new_token('opfsMark', {'mark': f'webdriver-session-{n}'})
            wd('POST', f'/session/{sid}/url', {'url': drv.url(token)})
            ev = HUB.wait(lambda e, t=token: e.get('token') == t and e.get('event') == 'done', 60)
            res = [e.get('result') for e in HUB.of_token(token, 'result')]
            popup = None
            if n == 1:
                # Does window.open without a user gesture open a tab under WebDriver?
                before = wd('GET', f'/session/{sid}/window/handles')['value']
                wd('POST', f'/session/{sid}/execute/sync', {'script': "window.open('/blank.html', '_blank'); return 1;", 'args': []})
                time.sleep(2)
                after = wd('GET', f'/session/{sid}/window/handles')['value']
                popup = {'handlesBefore': len(before), 'handlesAfter': len(after)}
            out[f'session{n}'] = {'ended': ev and ev.get('event'), 'result': res, 'windowOpenWithoutGesture': popup,
                                  'capabilities': s['value'].get('capabilities')}
            wd('DELETE', f'/session/{sid}')
            time.sleep(1)
    except Exception as e:  # noqa: BLE001
        out['error'] = repr(e)
    finally:
        sd.kill()
        out['safaridriverOutput'] = sd.stdout.read().decode(errors='replace')[-2000:]
    drv.save('safaridriver', out)


def main():
    global HUB, PAYLOAD_DIR
    ap = argparse.ArgumentParser()
    ap.add_argument('--browser', choices=['chromium', 'firefox', 'safari'])
    ap.add_argument('--binary')
    ap.add_argument('--profile')
    ap.add_argument('--out', required=True)
    ap.add_argument('--port', type=int, default=0)
    ap.add_argument('--payload-dir')
    ap.add_argument('--tests', default='info,s1q1,matrix,s1q2,s5q1a,s5q1rej,s5q1b,q3,q4,q6,q2,q5')
    ap.add_argument('--serve-only', action='store_true')
    ap.add_argument('--iters', type=int, default=20)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    HUB = Hub(a.out)
    PAYLOAD_DIR = a.payload_dir
    start_server(a.port)
    print(f'server on http://localhost:{HUB.port}', flush=True)
    if a.serve_only:
        while True:
            time.sleep(3600)
    profile = a.profile or os.path.join(a.out, f'profile-{a.browser}')
    tests = a.tests.split(',')
    b = Browser(a.browser, profile, a.binary, cdp='freeze' in tests)
    HUB.kill_fn = b.kill
    HUB.open_fn = lambda url: b.open_tab(f'http://localhost:{HUB.port}/{url}')
    HUB.freeze_fn = b.freeze
    drv = Driver(b, os.path.join(a.out, 'summary.json'))
    drv.save('env', {'browser': a.browser, 'version': b.version(), 'platform': sys.platform, 'uname': ' '.join(os.uname()),
                     'display': os.environ.get('DISPLAY'), 'tests': tests})
    for t in tests:
        print(f'=== {t} ===', flush=True)
        t0 = time.time()
        try:
            if t == 'safaridriver':
                safaridriver_check(drv)
            elif t == 'q2':
                drv.t_q2(a.iters)
            elif t == 's5q1b':
                drv.t_s5q1b(a.iters)
            else:
                getattr(drv, f't_{t}')()
        except Exception as e:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            drv.save(f'{t}-exception', repr(e))
            b.kill()
        print(f'=== {t}: {time.time() - t0:.0f} s ===', flush=True)
    b.kill()
    print('ALL DONE', flush=True)


if __name__ == '__main__':
    main()
