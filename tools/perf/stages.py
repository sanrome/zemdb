#!/usr/bin/env python3
"""Summarizes the runs of run_stages.sh: for each case and number of requests in flight, the
commits per second and, of each stage, its mean per sample, its median and p99, and what it
adds to each commit (its sum over the commits of the run), all in microseconds.

    stages.py OUT.jsonl [--json]
"""
import json
import statistics
import sys
from collections import defaultdict

VALUES = {"group_commits", "catchup_ops", "request_bytes", "response_bytes", "wal_bytes"}
TOTALS = {"actor_total", "server_total"}


def lower(index):
    """Lower bound of the bucket `index` (stage_probe::bucket)."""
    if index < 8:
        return index
    e, sub = divmod(index - 8, 8)
    return (8 + sub) << e


def quantile(buckets, q):
    total = sum(n for _, n in buckets)
    if total == 0:
        return 0.0
    seen = 0
    for index, n in buckets:
        seen += n
        if seen >= q * total:
            # The middle of the bucket.
            return (lower(index) + lower(index + 1)) / 2
    return 0.0


def summarize(path):
    runs = defaultdict(list)
    for line in open(path):
        run = json.loads(line)
        runs[(run["case"], run["result"]["k"])].append(run)
    out = []
    for (case, k), reps in runs.items():
        result = lambda f: statistics.median(r["result"][f] for r in reps)
        row = {
            "case": case,
            "k": k,
            "reps": len(reps),
            "commits_per_sec": result("commits_per_sec"),
            "client_mean_us": result("mean_us"),
            "client_p50_us": result("p50_us"),
            "client_p99_us": result("p99_us"),
            "server_cpu_pct": result("server_cpu_pct"),
            "server_cpu_us_per_commit": statistics.median(
                r["result"]["server_cpu_pct"] / 100 * 1e6 / r["result"]["commits_per_sec"]
                for r in reps
            ),
            "ctx_switches_per_commit": result("server_ctx_switches_per_commit"),
            "stages": {},
        }
        if reps[0]["stages"]:
            for name in reps[0]["stages"]:
                per_rep = []
                for r in reps:
                    s = r["stages"][name]
                    commits = r["stages"]["sequence"]["count"] or 1
                    if s["count"] == 0:
                        continue
                    scale = 1 if name in VALUES else 1000
                    per_rep.append({
                        "mean": s["sum"] / s["count"] / scale,
                        "p50": quantile(s["buckets"], 0.50) / scale,
                        "p99": quantile(s["buckets"], 0.99) / scale,
                        "per_commit": s["sum"] / commits / scale,
                        "per_commit_samples": s["count"] / commits,
                    })
                if per_rep:
                    row["stages"][name] = {
                        f: statistics.median(p[f] for p in per_rep) for f in per_rep[0]
                    }
        out.append(row)
    return out


def main():
    rows = summarize(sys.argv[1])
    if "--json" in sys.argv:
        json.dump(rows, sys.stdout, indent=1)
        return
    for row in rows:
        print(
            f"\n== {row['case']}  k={row['k']}  ({row['reps']} reps)  "
            f"{row['commits_per_sec']:.0f} commits/s  client mean {row['client_mean_us']:.1f} us  "
            f"p50 {row['client_p50_us']:.0f}  p99 {row['client_p99_us']:.0f}  "
            f"server CPU {row['server_cpu_us_per_commit']:.1f} us/commit "
            f"({row['server_cpu_pct']:.0f}%)  ctx {row['ctx_switches_per_commit']:.2f}/commit"
        )
        print(f"{'stage':<16}{'mean':>10}{'p50':>10}{'p99':>10}{'per commit':>12}{'samples/commit':>16}")
        chain = 0.0
        for name, s in row["stages"].items():
            if name not in VALUES and name not in TOTALS:
                chain += s["per_commit"]
            print(
                f"{name:<16}{s['mean']:>10.2f}{s['p50']:>10.2f}{s['p99']:>10.2f}"
                f"{s['per_commit']:>12.2f}{s['per_commit_samples']:>16.3f}"
            )
        print(f"{'sum of stages':<16}{'':>30}{chain:>12.2f}")


if __name__ == "__main__":
    main()
