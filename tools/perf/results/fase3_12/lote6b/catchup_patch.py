#!/usr/bin/env python3
"""Diagnostic, outside the repo: the in-process generator records the length of the catch-up of
every commit acknowledged after the warmup, and prints a histogram to stderr at the end."""
import sys
T = sys.argv[1]
p = T + '/tools/perf/src/main.rs'
s = open(p).read()
def rep(a, b):
    global s
    assert s.count(a) == 1, (a, s.count(a))
    s = s.replace(a, b)
rep("""struct InprocCommitter {""", """static RECORD: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static CATCHUP: [AtomicU64; 1001] = [const { AtomicU64::new(0) }; 1001];
static CATCHUP_MORE: AtomicU64 = AtomicU64::new(0);

fn print_catchups() {
    let counts: Vec<u64> = CATCHUP.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    let n: u64 = counts.iter().sum();
    let sum: u64 = counts.iter().enumerate().map(|(len, c)| len as u64 * c).sum();
    let pct = |q: f64| {
        let mut acc = 0;
        for (len, c) in counts.iter().enumerate() {
            acc += c;
            if acc as f64 >= q * n as f64 { return len; }
        }
        counts.len() - 1
    };
    let max = counts.iter().rposition(|c| *c > 0).unwrap_or(0);
    eprintln!(
        "{{\\"catchups\\":{n},\\"mean\\":{:.1},\\"p50\\":{},\\"p90\\":{},\\"p99\\":{},\\"max\\":{max},\\"at_100\\":{},\\"empty\\":{},\\"has_more\\":{}}}",
        sum as f64 / n.max(1) as f64, pct(0.5), pct(0.9), pct(0.99), counts[100], counts[0],
        CATCHUP_MORE.load(Ordering::Relaxed)
    );
}

struct InprocCommitter {""")
rep("""            Ok(Ok(resp)) => Ok(resp.assigned_seq.get()),""", """            Ok(Ok(resp)) => {
                if RECORD.load(Ordering::Relaxed) {
                    CATCHUP[resp.catchup_ops.len().min(1000)].fetch_add(1, Ordering::Relaxed);
                    if resp.has_more {
                        CATCHUP_MORE.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(resp.assigned_seq.get())
            }""")
rep("""    run("inproc", &run_args, committers, head).await;
    manager.shutdown_all().await;""", """    let warmup = run_args.warmup;
    tokio::spawn(async move {
        tokio::time::sleep(warmup).await;
        RECORD.store(true, Ordering::Relaxed);
    });
    run("inproc", &run_args, committers, head).await;
    RECORD.store(false, Ordering::Relaxed);
    print_catchups();
    manager.shutdown_all().await;""")
open(p, 'w').write(s)
print("patched", p)
