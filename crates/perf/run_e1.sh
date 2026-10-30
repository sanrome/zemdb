#!/usr/bin/env bash
# Runs one measurement of Evaluation 1 and appends a JSON line per run to OUT.
#   run_e1.sh tmpfs|disk|inproc OUT
# Environment: KS (default "1 2 4 8 16 32 64"), REPS (3), WARMUP (5), SECS (30),
# SERVER_CPUS (0,1), GEN_CPUS (2,3), TMPFS_ROOT (/dev/shm/zemdb-e1), DISK_ROOT
# (/home/user/zemdb-e1-data), PORT (18080).
set -euo pipefail

MEASUREMENT=$1
OUT=$2
KS=${KS:-"1 2 4 8 16 32 64"}
REPS=${REPS:-3}
WARMUP=${WARMUP:-5}
SECS=${SECS:-30}
SERVER_CPUS=${SERVER_CPUS:-0,1}
GEN_CPUS=${GEN_CPUS:-2,3}
PORT=${PORT:-18080}
BIN=$(cd "$(dirname "$0")/../.." && pwd)/target/release

case $MEASUREMENT in
  tmpfs | inproc) ROOT=${TMPFS_ROOT:-/dev/shm/zemdb-e1} ;;
  disk) ROOT=${DISK_ROOT:-/home/user/zemdb-e1-data} ;;
  *) echo "unknown measurement $MEASUREMENT" >&2; exit 1 ;;
esac

export ZEMDB_AUTH_SECRET=perf_auth_secret_0123456789_0123456789
export ZEMDB_ADMIN_SECRET=perf_admin_secret_0123456789_012345678
export RUST_LOG=warn

for rep in $(seq 1 "$REPS"); do
  for k in $KS; do
    rm -rf "$ROOT" && mkdir -p "$ROOT"
    if [ "$MEASUREMENT" = inproc ]; then
      line=$(taskset -c "$SERVER_CPUS" "$BIN/zemdb-perf" inproc --data-dir "$ROOT/data" \
        --k "$k" --warmup "$WARMUP" --secs "$SECS")
    else
      ZEMDB_DATA_DIR="$ROOT/data" ZEMDB_PORT=$PORT taskset -c "$SERVER_CPUS" \
        "$BIN/zemdb-server" > "$ROOT/server.log" 2>&1 &
      pid=$!
      until (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; do sleep 0.1; done
      line=$(taskset -c "$GEN_CPUS" "$BIN/zemdb-perf" http --addr 127.0.0.1:"$PORT" \
        --room "e1-k$k-r$rep" --k "$k" --warmup "$WARMUP" --secs "$SECS" --server-pid "$pid")
      kill -TERM "$pid" && wait "$pid" || true
    fi
    echo "{\"measurement\":\"$MEASUREMENT\",\"rep\":$rep,\"result\":$line}" | tee -a "$OUT"
  done
done
rm -rf "$ROOT"
