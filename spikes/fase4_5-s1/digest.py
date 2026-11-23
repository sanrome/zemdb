#!/usr/bin/env python3
"""Prints the answers of a harness run (its summary.json) as short lines, for the job log.

    python3 digest.py RESULTS_DIR [--blob]

With --blob it also prints summary.json and the events (without the bulk ones) as gzip+base64
lines (BLOB ...), to rebuild them from the log.
"""
import base64
import gzip
import json
import os
import sys


def err(v):
    if not isinstance(v, dict):
        return str(v)
    if 'ok' in v and ('err' in v or v['ok'] is True):
        return 'ok' if v['ok'] is True else f"{v['err']['name']}"
    return json.dumps(v)[:200]


def main():
    d = sys.argv[1]
    s = json.load(open(os.path.join(d, 'summary.json')))
    P = lambda *a: print('D', *a)  # noqa: E731
    env = s.get('env', {})
    P('env', env.get('browser'), env.get('version'), env.get('uname'), 'display', env.get('display'))
    for k, v in s.items():
        if k.endswith('-exception'):
            P('EXCEPTION', k, v)
        elif isinstance(v, dict) and 'ended' in v:
            P('ended', k, v.get('ended'), v.get('secs'), 'errors', len(v.get('errors') or []), 'how', v.get('how'))
        elif isinstance(v, dict) and isinstance(v.get('page'), dict):
            P('ended', k, v['page'].get('ended'), v['page'].get('secs'), 'errors', len(v['page'].get('errors') or []), 'how', v['page'].get('how'))
    if 'info' in s:
        r = (s['info'].get('results') or [{}])[0] or {}
        pg, w = r.get('page', {}), r.get('worker', {})
        P('info ended', s['info'].get('ended'), 'UA', pg.get('userAgent'))
        P('info secure', pg.get('isSecureContext'), 'persist() in page', pg.get('persist'), 'persisted before/after',
          pg.get('persistedBefore'), pg.get('persistedAfter'), 'estimate', json.dumps(pg.get('estimate')))
        P('info worker: locks', w.get('locksInWorker'), 'persist in worker', w.get('persistInWorker'), 'persisted in worker',
          w.get('persistedInWorker'), w.get('persistedWorker'), 'estimate', json.dumps(w.get('estimateWorker')))
        P('info worker: SAH', w.get('syncAccessHandle'), 'move on file', w.get('moveOnFileHandle'), 'move on dir',
          w.get('moveOnDirectoryHandle'), 'remove()', w.get('removeOnHandle'), 'createWritable', w.get('createWritable'))
        for m, v in (w.get('modes') or {}).items():
            if 'first' in v:
                P(f'info mode {m}: first {err(v["first"])} second {err(v["second"])} reported {v.get("reportedMode")}')
            else:
                P(f'info mode {m}: {err(v)}')
        for e in s['info'].get('errors', []):
            P('info ERROR', json.dumps(e)[:400])
    if 's1q1' in s:
        r = s['s1q1']
        for ph in ('write', 'verifyAfterReload'):
            for rep in r.get(ph) or []:
                P(f's1q1 {ph} ok={rep.get("ok")}', '; '.join(f"{x['step']}={'ok' if x['ok'] else 'FAIL'} {x['detail']}" for x in rep.get('steps', [])))
        for e in r.get('errors', []):
            P('s1q1 ERROR', json.dumps(e)[:400])
    if 'matrix' in s:
        r = (s['matrix'].get('results') or [{}])[0] or {}
        P('matrix ended', s['matrix'].get('ended'), 'otherCtxKind', r.get('otherCtxKind'))
        for k, v in (r.get('local') or {}).items():
            if k.endswith('_after') or k.endswith('after'):
                P(f'matrix {k}: {json.dumps(v)[:300]}')
            elif isinstance(v, dict) and 'ok' in v and 'ms' in v:
                extra = ''
                if 'after' in v:
                    extra = ' after=' + json.dumps(v['after'])[:200]
                if 'oldHandleName' in v:
                    extra += f" oldHandleName={v['oldHandleName']}"
                msg = '' if v['ok'] else ' | ' + v['err']['message'][:110]
                P(f'matrix {k}: {err(v)} ({v["ms"]} ms){msg}{extra}' + (f' value={v.get("value")}' if v['ok'] else ''))
            else:
                P(f'matrix {k}: {json.dumps(v)[:400]}')
        for k, v in (r.get('s3') or {}).items():
            P(f's3 {k}: {err(v) if isinstance(v, dict) and "ok" in v else json.dumps(v)[:300]}'
              + (f" value={v.get('value')}" if isinstance(v, dict) and v.get('ok') else '')
              + (f" | {v['err']['message'][:110]}" if isinstance(v, dict) and v.get('ok') is False else ''))
        for k in ('sah_heldByOtherTab', 'sah_rightAfterOtherTabClosed', 'sah_afterOtherTabClosed'):
            v = r.get(k)
            if isinstance(v, list):
                P(f'matrix {k}:', [f"{err(x)} {x['ms']}ms" for x in v], '|', (v[0].get('err') or {}).get('message', '')[:120])
            else:
                P(f'matrix {k}: {json.dumps(v)[:300]}')
        for e in s['matrix'].get('errors', []):
            P('matrix ERROR', json.dumps(e)[:400])
    if 's1q2' in s:
        r = s['s1q2']
        b = (r['beforeKill'].get('results') or [{}])[0] or {}
        a = (r['afterKill'].get('results') or [{}])[0]
        P('s1q2 afterTerminate', b.get('afterTerminate'), 'tab', b.get('tabKind'), 'afterTabClose', b.get('afterTabClose'),
          'free', b.get('afterTabCloseFree'), '| after SIGKILL', a, 'ended', r['beforeKill'].get('ended'), r['afterKill'].get('ended'))
        for e in r['beforeKill'].get('errors', []) + r['afterKill'].get('errors', []):
            P('s1q2 ERROR', json.dumps(e)[:400])
    if 's5q1a' in s:
        for row in s['s5q1a']:
            P('s5q1a', row.get('variant'), 'reported', row.get('reportedMax'), 'visiblePrefix', row.get('visiblePrefix'), 'of', row.get('opsTotal'),
              'nonPrefix', row.get('nonPrefixCount'), 'reportedNotVisible', row.get('reportedButNotVisibleCount'),
              'targetValid', row.get('targetValid'), 'gen', row.get('targetGen'), 'anomalies', row.get('anomalies'), row.get('error', ''))
    if 's5q1b' in s:
        r = s['s5q1b']
        c = r.get('calibration', {})
        P('s5q1b calibration seriesMs', r.get('seriesMs'), 'ops', c.get('opsTotal'), 'visible', c.get('visiblePrefix'), 'nonPrefix', c.get('nonPrefixCount'))
        bad = 0
        for row in r.get('rows', []):
            flag = row.get('nonPrefixCount') or row.get('reportedButNotVisibleCount') or not row.get('targetValid') or row.get('anomalies') or row.get('error')
            bad += bool(flag)
            P('s5q1b', row.get('iter'), 'killAt', row.get('killAtMsAfterSetup'), 'finishedBefore', row.get('finishedBeforeKill'),
              'reported', row.get('reportedMax'), 'visiblePrefix', row.get('visiblePrefix'), 'inFlightVisible', row.get('inFlightVisible'),
              'nonPrefix', row.get('nonPrefixCount'), 'reportedNotVisible', row.get('reportedButNotVisibleCount'), 'targetGen', row.get('targetGen'),
              'targetValid', row.get('targetValid'), 'incompleteTemps', row.get('tempsWithIncompleteContent'), 'anomalies', row.get('anomalies'),
              row.get('error', ''), 'FLAG' if flag else '')
        P('s5q1b iterations', len(r.get('rows', [])), 'flagged', bad)
    if 's5q1rej' in s:
        r = s['s5q1rej']
        b = (r['beforeKill'].get('results') or [{}])[0] or {}
        P('s5q1rej rejected:', {k: v['err']['name'] for k, v in (b.get('rejected') or {}).items()})
        P('s5q1rej resolved (not rejected):', b.get('resolved'))
        P('s5q1rej effects before kill', r.get('effectsBeforeKill'))
        P('s5q1rej effects after kill', r.get('effectsAfterKill'), 'sameAfterKill', r.get('sameAfterKill'))
    if 'q2' in s:
        r = s['q2']
        P('q2 run', r.get('run'))
        for m, v in (r.get('byMode') or {}).items():
            P(f'q2 {m}: iters {v["iters"]} itersWithTakenHandles {v["itersWithTakenHandles"]} handlesTaken {v["handlesTaken"]} errors {v["errors"]} handoverMs {v["handoverMs"]} kinds {v["kinds"]} problems {v["problems"][:3]}')
        shown = {}
        for row in r.get('rawRows') or []:
            if row.get('failures') and shown.get(row['mode'], 0) < 6:
                shown[row['mode']] = shown.get(row['mode'], 0) + 1
                P(f"q2 row {row['mode']} {row['i']}: taken {row['failures']} handoverMs {row.get('handoverMs')} retry {json.dumps(row.get('retry'))[:200]}")
        for m in sorted({row['mode'] for row in r.get('rawRows') or []}):
            frees = [row['retry'][-1]['afterMs'] for row in r['rawRows'] if row['mode'] == m and row.get('retry') and row['retry'][-1]['stillTaken'] == 0]
            P(f'q2 {m}: all handles free after (ms since grant) {sorted(frees)}')
        for m, v in (r.get('inflight') or {}).items():
            P(f'q2 inflight {m}: iters {v["iters"]} itersWithChangesAfterGrant {v["itersWithChangesAfterGrant"]} errors {v["errors"]}')
        for row in r.get('rawInflight') or []:
            ch = row.get('changesAfterGrant') or []
            if not ch and not row.get('error'):
                continue
            P(f"q2 inflight row {row['mode']} {row['i']}: kind {row.get('aKind')} atGrant {row.get('landedAtGrant')} changes {[(c['atMs'], sum(1 for n in c['names'] if n.startswith('F:mv_')), sum(1 for n in c['names'] if n.startswith('F:r_'))) for c in ch]} lists {row.get('lists')} {row.get('error') or ''}")
    if 'q3' in s:
        r = (s['q3'].get('results') or [{}])[0] or {}
        P('q3 kinds', r.get('aKind'), r.get('bKind'), 'freeIfAvailable', r.get('freeIfAvailable'))
        for run in r.get('runs', []):
            P('q3 run', json.dumps(run))
        for e in s['q3'].get('errors', []):
            P('q3 ERROR', json.dumps(e)[:400])
    if 'q4' in s:
        r = (s['q4']['page'].get('results') or [{}])[0] or {}
        for k in ('timed', 'timedSmall'):
            t = r.get(k) or {}
            P(f'q4 {k}: reads {t.get("reads")} chunks {t.get("chunks")} bytes {t.get("total")} lagMs {[c["lagMs"] for c in t.get("perChunk", [])]}')
        P('q4 abort', json.dumps(r.get('abort')))
        P('q4 flood', json.dumps(r.get('flood')))
        for e in s['q4'].get('server', []):
            if e.get('event') != 'stream-sent':
                P('q4 server', json.dumps({k: v for k, v in e.items() if k != 'perChunk'})[:900])
        for e in s['q4']['page'].get('errors', []):
            P('q4 ERROR', json.dumps(e)[:400])
    if 'q4h2' in s:
        r = (s['q4h2']['page'].get('results') or [{}])[0] or {}
        for k in ('timed', 'timedSmall'):
            t = r.get(k) or {}
            P(f'q4h2 {k}: proto {t.get("httpVersion")} reads {t.get("reads")} chunks {t.get("chunks")} lagMs {[c["lagMs"] for c in t.get("perChunk", [])]}')
        P('q4h2 abort', json.dumps(r.get('abort')))
        P('q4h2 flood', json.dumps(r.get('flood')))
        for e in s['q4h2'].get('server', []):
            if e.get('event') != 'stream-sent':
                P('q4h2 server', json.dumps({k: v for k, v in e.items() if k != 'perChunk'})[:900])
        for e in s['q4h2']['page'].get('errors', []):
            P('q4h2 ERROR', json.dumps(e)[:400])
    if 'q5' in s:
        r = s['q5']
        P('q5 ended', r['page'].get('ended'), 'result', r['page'].get('results'))
        for e in r.get('events', []):
            if e.get('event') != 'timer':
                P('q5 event', json.dumps({k: e.get(k) for k in ('event', 'vis', 'hiddenForMs', 'sinceStartMs', 't', 'who')}))
        for k, v in (r.get('summary') or {}).items():
            P(f'q5 {k}: {json.dumps(v)}')
        for e in r.get('events', []):
            if e.get('event') == 'timer' and e.get('label') == 'worker-30s':
                P('q5 worker-30s', json.dumps({k: e.get(k) for k in ('lateMs', 'rttMs', 'vis', 'hiddenForMs', 't')}))
    if 'q6' in s:
        r = (s['q6'].get('results') or [{}])[0] or {}
        P('q6 kinds', r.get('aKind'), r.get('bKind'))
        P('q6 order small', r.get('order_small'), 'withLarge', r.get('order_withLarge'))
        P('q6 keySend', json.dumps(r.get('keySend')))
        for k in r.get('keyReceived') or []:
            P('q6 keyReceived', json.dumps(k)[:400])
        if r.get('keyWaitError'):
            P('q6 keyWaitError', r.get('keyWaitError'))
        P('q6 rtt ms', json.dumps(r.get('rtt')))
        P('q6 abortSignal', json.dumps(r.get('abortSignal')))
        P('q6 handover on tab close ms', json.dumps(r.get('handoverStats')), [h.get('ms', h.get('error')) for h in r.get('handover', [])])
        for e in s['q6'].get('errors', []):
            P('q6 ERROR', json.dumps(e)[:400])
    if 'freeze' in s:
        r = (s['freeze'].get('results') or [{}])[0] or {}
        for run in r.get('runs', []):
            P('freeze', json.dumps(run)[:600])
    if 'prerender' in s:
        for e in s['prerender'].get('events', []):
            P('prerender', e.get('event'), e.get('where', ''), 'prerendering', e.get('prerendering'), 'sinceLoadMs', e.get('sinceLoadMs'), 'beforeActivation', e.get('beforeActivation'))
    if 'zstd' in s:
        for ld in s['zstd']:
            r = (ld.get('results') or [{}])[0] or {}
            P('zstd load', r.get('load'), 'payload', r.get('payloadBytes'), 'nativeEnvelope', r.get('nativeEnvelopeBytes'), 'wasm', r.get('wasmBytes'),
              'runs', [(x['ms'], x['outBytes'], x['status']) for x in r.get('runs', [])], 'decNative', r.get('decNative'), 'decOwn', r.get('decOwn'),
              'uploaded', r.get('uploaded'), 'UA', r.get('userAgent'))
            for e in ld.get('errors', []):
                P('zstd ERROR', json.dumps(e)[:400])
    for k in ('opfsMarkNormal', 'safaridriver'):
        if k in s:
            P(k, json.dumps(s[k])[:1500])
    if '--blob' in sys.argv:
        evs = []
        with open(os.path.join(d, 'events.jsonl')) as f:
            for line in f:
                if '"series-resolved"' in line:
                    continue
                evs.append(line)
        blob = json.dumps({'summary': s, 'events': ''.join(evs)}).encode()
        b = base64.b64encode(gzip.compress(blob, 9)).decode()
        print(f'BLOB BEGIN {len(b)}')
        for i in range(0, len(b), 3000):
            print('BLOB ' + b[i:i + 3000])
        print('BLOB END')


if __name__ == '__main__':
    main()
