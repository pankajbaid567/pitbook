# Resume claims register

A claim may go on a resume only when its row says **proven**, with the command that reproduces it and the raw file it comes from. Numbers are copied from generated output ([BENCHMARKS.md](../BENCHMARKS.md)), never typed from memory. Dates are 2026-11-10 unless stated.

| # | Claim | Proof command | Raw result | Status |
|---|---|---|---|---|
| 1 | Matching engine equals a naive reference model over **1,000,000** random commands (382,819 fills, 118,607 self-trade cancels, 55,099 FOK rejects), invariants checked after every command | `make prop` | `bench/results/property-*.txt` | **proven** |
| 2 | The property test has teeth: **5 of 5** deliberately injected bugs are caught | `make mutants` | `bench/results/mutation-*.txt` | **proven** |
| 3 | Deterministic replay: identical state hash across repeated replays (20 to 25 per test, any Go map order) | `go test ./internal/engine ./internal/book`, `bin/replay -runs 20` | tests; `cmd/replay` output | **proven** (not 100 replays) |
| 4 | **0 acknowledged commands lost and 0 duplicated fills across 1,000 SIGKILL runs**; 117,609 acknowledged commands found byte-identical in the recovered log, 41,176 fills cross-checked, 3,428 in-flight retries never double-booked | `make crash-test` | `bench/results/crashtest-*.json` | **proven** (process crash only) |
| 5 | The crash harness itself works: it reports `LOST` when fsync is removed and `DOUBLE ORDER` when idempotency is disabled, and it found a real bug (ADR-015) | procedure in `docs/testing.md` | `docs/DESIGN.md` ADR-015, regression test `TestReplaceThatLosesPriorityIsIdempotent` | **proven** |
| 6 | Matching core costs **about 137 ns per command** single-threaded (about 7 million commands/s per core) | `go test -bench BenchmarkSteadyMix ./internal/book` | `bench/results/applybench-*.txt` | **proven** (Apple M3) |
| 7 | Whole pipeline, no log: median **about 670,000 commands/s** at saturation (range 499k to 1.52M across 5 runs on a busy laptop) | `make bench` | `bench/results/loadgen-*.csv` | **proven, with the range** |
| 8 | With fsync before every acknowledgement (group commit): median **about 80,000 commands/s** at saturation; open-loop at 10,000/s: p50 4.2 ms, p99 about 10 ms | `make bench` | `bench/results/loadgen-*.csv` | **proven** (macOS `F_FULLFSYNC`) |
| 9 | A 1,000,000-command log recovers in **0.32 s** (0.26 s from a mid-log snapshot) | `make bench` (recovery step) | `bench/results/loadgen-*-recovery.csv` | **proven** |
| 10 | A client that rebuilds its book from SNAPSHOT + contiguous frames ends with exactly the server's book; slow consumers are resynced without slowing the engine | `go test ./internal/gateway`, `node --experimental-websocket web/scripts/e2e.mjs <url>` | test output | **proven** |
| 11 | A wiped disk recovers identical state from object storage (unit test, and live against the real Worker + R2 on 2026-11-10: recovered seq 736 after a client had seen 704) | `go test ./internal/archive` | test output; deployment record in `docs/deploy.md` | **proven** |
| 12 | Live demo on Cloudflare Workers + Render free tier: https://pitbook.baidpankaj567.workers.dev (checked 2026-11-10: live feed, order entry, CORS and origin checks) | open the link | `docs/deploy.md` | **proven** for the live demo; the crash-recovery button on the hosted instance is **pending** until the object-store mirror env vars are set |
| 13 | GitHub Actions CI | push to GitHub | `.github/workflows/ci.yml` | **pending** (check the Actions tab of the pushed repo; not yet observed) |

## What not to claim
- "Fault tolerant", "highly available", "exchange-grade", "production", "zero downtime". Say **simulator**, **process crash**, **single node**.
- Power-loss or disk-loss durability. SIGKILL keeps the OS page cache.
- Any performance of the hosted instance. All numbers are from one laptop; **tail latencies at 100k/s and above are dominated by scheduler noise on that machine** (control experiment in BENCHMARKS.md), so quote p50, medians and the saturation figures, and quote p99 only together with its configuration.
- The 400,000/s open-loop engine row: the machine could not sustain it (dropped commands). It is in the report as an overload row, not a result.
- A 100-replay determinism figure (the tests run 20 to 25).

## Wording rules
- Write numbers as `value, hardware, configuration`: "137 ns per command, Apple M3, single goroutine, no log".
- Durability claims refer to the local-file WAL (ADR-009). The hosted path is "restores demo state after a restart".
- If a number is re-measured and moves, update the row and the resume line together.

## Resume bullets (every number above is provable; keep them in sync with the table)
**Pitbook – Crash-safe matching engine and live order book (Go)** ( Github ) ( Demo ) &nbsp; November 2026
- Built a deterministic price-time-priority matching engine in Go (limit, market, IOC, FOK, cancel-replace, self-trade prevention); verified against a reference model over **1,000,000** random commands, with **5 of 5** injected bugs caught.
- Event-sourced with a CRC-framed write-ahead log, group-commit fsync and snapshots: **0 acknowledged commands lost or duplicated across 1,000 SIGKILL runs** (117,609 acks verified); the harness also found and fixed a retry-idempotency bug.
- Benchmarked open-loop on an Apple M3: **137 ns/command** in the core, **~670k commands/s** engine-only, **~80k/s** with fsync before every ack; a 1M-command log recovers in **0.3 s**.
- Shipped a live order-book UI (React, WebSocket with gap detection and resync) on Cloudflare Workers; the log is mirrored to R2 so the Render free-tier instance restores its state after a restart.
