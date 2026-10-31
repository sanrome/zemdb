#!/usr/bin/env bash
# Summarizes the JSON lines of run_e2.sh: per label, the median of the repetitions.
jq -s -r '
  def median: sort | if length % 2 == 1 then .[length/2|floor] else (.[length/2-1] + .[length/2]) / 2 end;
  def m(f): [.[] | .result | f] | median;
  group_by(.label)
  | sort_by(.[0].kind, .[0].label | (capture("(?<p>.*?)(?<n>[0-9]+)$") // {p: ., n: "0"}) | [.p, (.n | tonumber)])
  | (["label","reps","commits/s","syncs/s","sse/s","commit50","commit99","prop50","prop99","prop999","poolw99","srvCPU","genCPU","RSSMiB","thr","fds","503","diskW/s","diskF/s","busy%","valid"] | @tsv),
    (.[] | [ .[0].label, length,
      (m(.commits_per_sec) | floor), (m(.syncs_per_sec) | floor), (m(.sse_events_per_sec) | floor),
      m(.commit_latency.p50_us), m(.commit_latency.p99_us),
      m(.propagation.p50_us), m(.propagation.p99_us), m(.propagation.p999_us),
      m(.pool_wait.p99_us // 0),
      (m(.server_cpu_pct) | floor), (m(.gen_cpu_pct) | floor),
      (m(.server_rss_kib) / 1024 | floor), m(.server_threads), m(.server_fds),
      m(.unavailable), (m(.disk_writes_per_sec) | floor), (m(.disk_flushes_per_sec) | floor), (m(.disk_busy_pct) | floor),
      (all(.[]; .result.commit_errors == 0 and .result.head_ok and .result.gaps == 0 and .result.sse_errors == 0 and .result.clients_behind_at_end == 0 and .result.gen_cpu_pct < 180)) ] | @tsv)
' "$1"
