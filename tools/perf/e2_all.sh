#!/usr/bin/env bash
# Every measurement of Evaluation 2, 3 repetitions each, appended to OUT.
#   e2_all.sh OUT
set -euo pipefail
OUT=$1
RUN=$(dirname "$0")/run_e2.sh
export REPS=${REPS:-3}
BIG=4294967296
load() { echo --clients "$1" --period 10 --sse 1 --warmup 15 --secs 60 --rooms "$2"; }

# 2a: rooms open at rest, one registered client each.
for r in 1 100 1000 5000; do
  $RUN tmpfs "$OUT" 2a-r$r --rooms $r --clients 1 --period 0 --warmup 15 --secs 10
done
# 2b: SSE streams open at rest, in 100 rooms.
for c in 10 100; do
  $RUN tmpfs "$OUT" 2b-c$c --rooms 100 --clients $c --period 0 --sse 1 --warmup 15 --secs 10
done
# 2c: load at a human pace, tmpfs.
for r in 100 250 500 750 1000 1500; do
  $RUN tmpfs "$OUT" 2c-r$r $(load 10 $r)
done
# Diagnostic on tmpfs: without block_in_place, with a larger response budget, and both.
for r in 500 1000 1500; do
  ZEMDB_PERF_INLINE_IO=1 ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES=$BIG $RUN tmpfs "$OUT" 2c-both-r$r $(load 10 $r)
done
ZEMDB_PERF_INLINE_IO=1 $RUN tmpfs "$OUT" 2c-inline-r1000 $(load 10 1000)
ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES=$BIG $RUN tmpfs "$OUT" 2c-budget-r1000 $(load 10 1000)
# 2d: the same on the real disk, and rooms of 50 clients.
for r in 100 250 500 750 1000; do
  $RUN disk "$OUT" 2d-r$r $(load 10 $r)
done
for r in 20 50; do
  $RUN disk "$OUT" 2d-c50-r$r $(load 50 $r)
done
# 2e: the real disk without block_in_place (and with the larger budget, so that the 503s do
# not hide it).
for r in 500 1000; do
  ZEMDB_PERF_INLINE_IO=1 ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES=$BIG $RUN disk "$OUT" 2e-both-r$r $(load 10 $r)
done
