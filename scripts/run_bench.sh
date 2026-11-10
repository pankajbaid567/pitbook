#!/usr/bin/env bash
# Runs the benchmark matrix described in docs/benchmarks-method.md and appends
# raw rows to bench/results/loadgen-<date>.csv. Takes about 25 minutes.
# Run it on an otherwise idle machine on AC power; the load average at the start
# is recorded with every row.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${LOADGEN:-bin/loadgen}
DATE=$(date -u +%Y%m%dT%H%M%SZ)
OUT=bench/results/loadgen-$DATE.csv
POWER=$(pmset -g batt 2>/dev/null | head -1 | sed -E "s/Now drawing from '(.*)'/\1/" || echo unknown)
LOAD=$(uptime | sed -E 's/.*load averages?: *//')
NOTE="power=${POWER}; load_avg_at_start=${LOAD}"
echo "writing $OUT ($NOTE)"
run() { echo "+ $*"; "$BIN" "$@" -out "$OUT" -note "$NOTE"; }

# Saturation: throughput only (latency under a full queue is meaningless).
for m in engine wal-async wal-sync; do
  run -mode $m -workload steady -rate 0 -duration 10s -warmup 3s -reps 5
done
# Headline open-loop runs: 20 s measured after 5 s warm-up, 5 repetitions.
run -mode engine    -workload steady -rate 100000 -duration 20s -warmup 5s -reps 5
run -mode engine    -workload steady -rate 400000 -duration 20s -warmup 5s -reps 5
run -mode wal-async -workload steady -rate 100000 -duration 20s -warmup 5s -reps 5
run -mode wal-sync  -workload steady -rate 10000  -duration 20s -warmup 5s -reps 5
run -mode wal-sync  -workload steady -rate 40000  -duration 20s -warmup 5s -reps 5
# Secondary workloads: 3 repetitions.
run -mode wal-sync  -workload steady -rate 2000   -duration 20s -warmup 5s -reps 3
run -mode engine    -workload deep   -rate 100000 -duration 20s -warmup 5s -reps 3
run -mode wal-async -workload deep   -rate 100000 -duration 20s -warmup 5s -reps 3
run -mode engine    -workload burst  -rate 200000 -duration 20s -warmup 5s -reps 3
run -mode engine    -workload multi  -rate 400000 -duration 20s -warmup 5s -reps 3 -symbols 8
# Recovery time, 1,000,000 commands, with and without a snapshot.
"$BIN" -workload recovery -n 1000000 -out "$OUT" -note "$NOTE"
echo done
