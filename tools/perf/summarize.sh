#!/usr/bin/env bash
# Summarizes the JSON lines of run_e1.sh: per measurement and k, the median of the repetitions,
# the spread of commits/s between them, and whether every run was valid.
jq -s -r '
  def median: sort | if length % 2 == 1 then .[length/2|floor] else (.[length/2-1] + .[length/2]) / 2 end;
  group_by([.measurement, .result.k])
  | sort_by(.[0].measurement, .[0].result.k)
  | (["medicion","k","commits/s","spread%","p50us","p99us","p999us","maxus","srvCPU%","genCPU%","RSSKiB","valid"] | @tsv),
    (.[] | . as $g | [.[].result.commits_per_sec] as $t |
      [ $g[0].measurement, $g[0].result.k,
        ($t | median | floor),
        ((($t | max) - ($t | min)) / ($t | median) * 100 | floor),
        ([$g[].result.p50_us] | median), ([$g[].result.p99_us] | median),
        ([$g[].result.p999_us] | median), ([$g[].result.max_us] | median),
        ([$g[].result.server_cpu_pct] | median | floor), ([$g[].result.gen_cpu_pct] | median | floor),
        ([$g[].result.server_hwm_kib] | median),
        (all($g[]; .result.errors == 0 and .result.head_ok)) ] | @tsv)
' "$1"
