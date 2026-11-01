#!/usr/bin/env bash
# The freshness modes of Evaluation 2 (2f), 3 repetitions each, appended to OUT.
#   e2f_all.sh OUT
# a: SSE signals, Ack after each sync (as today); b: SSE signals, periodic Ack; c: SSE events
# carrying the operations (prototype), periodic Ack; d: no SSE, heartbeat every 30 s. Every mode
# sends heartbeats every 30 s, as the SDK will. "fixed": without block_in_place and with a 4 GiB
# response budget (the two fixes of §4.7.5, simulated).
set -euo pipefail
OUT=$1
RUN=$(dirname "$0")/run_e2.sh
export REPS=${REPS:-3}
BIG=4294967296
base() { echo --clients 10 --period 10 --heartbeat 30 --warmup 15 --secs 60 --rooms "$1"; }
mode() {
  local m=$1 r=$2 fixed=$3 label=2f-$1-r$2${3:+-fixed}
  local env=()
  [ -n "$fixed" ] && env+=(ZEMDB_PERF_INLINE_IO=1 ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES=$BIG)
  case $m in
    a) env "${env[@]}" "$RUN" tmpfs "$OUT" "$label" $(base "$r") --sse 1 --ack sync ;;
    b) env "${env[@]}" "$RUN" tmpfs "$OUT" "$label" $(base "$r") --sse 1 --ack periodic ;;
    c) env "${env[@]}" ZEMDB_PERF_SSE_OPS=1 "$RUN" tmpfs "$OUT" "$label" $(base "$r") --sse 1 --ack periodic ;;
    d) env "${env[@]}" "$RUN" tmpfs "$OUT" "$label" $(base "$r") --sse 0 --ack sync ;;
  esac
}
for r in 1000 1500; do
  for m in a b c d; do mode $m $r fixed; done
done
for r in 3000 5000; do mode d $r fixed; done
for m in a b c d; do mode $m 1000 ""; done
