# Benchmark method

How the numbers in [BENCHMARKS.md](../BENCHMARKS.md) are produced, and what they do not mean. The method was written before the first number existed so results could not be shaped to fit it; where the harness turned out to need a change, the change is listed under "Changes made after seeing data".

## Rules
1. **Every published number is generated** from a raw CSV in `bench/results/` by `scripts/make_benchmarks.py`. Nothing in `BENCHMARKS.md` is typed by hand.
2. **Environment is recorded in every row:** CPU model, logical CPUs, RAM, OS, Go version, `GOMAXPROCS`, commit hash, AC/battery state and the load average at the start of the run.
3. **Laptop only.** The hosted demo (Render free, a fraction of one CPU) is never used to measure, and the README never quotes a throughput for it.
4. **Open loop.** `cmd/loadgen` schedules command *i* at `start + i/rate` regardless of how fast earlier commands complete, and measures latency from the **scheduled** time to the durable completion callback. A closed loop (send, wait, send) hides queueing delay (coordinated omission). If the queue is full the command is **dropped and counted**, never waited for.
5. **Saturation is reported separately and as throughput only.** With `-rate 0` the generator submits as fast as the queue accepts. Latency under a permanently full queue is meaningless and is not reported.
6. **Three durability configurations, reported separately:**
   - `engine`: book and sequencer, no log. The cost of matching and queueing.
   - `wal-async`: log written and flushed to the OS per batch, **no fsync**. Never used for a durability claim.
   - `wal-sync`: acknowledge only after the group-commit fsync. The configuration the crash test uses. On macOS Go's `File.Sync` issues `F_FULLFSYNC`, which is slow (about 3 ms here); that cost dominates `wal-sync` latency and is stated.
7. **Warm-up excluded.** 5 s warm-up (headline runs: 3 s for saturation), then a 20 s measured window (10 s for saturation). The book is preloaded with 5,000 resting orders (100,000 for `deep`) before the clock starts.
8. **Repetitions:** 5 for the headline rows, 3 for secondary workloads. The table reports the **median** across repetitions for each percentile and the **range of p99**, never the best run.
9. **Percentiles come from a histogram with about 3% resolution** (`internal/metrics`), recorded per command. p99.9 is printed only when there are at least 10,000 samples.
10. **GC is part of the result:** GC cycles, total pause and allocations per command are recorded for the measured window (the allocation figure includes the load generator's own, which is small because requests are pooled).
11. **Failures stay in.** Dropped commands are counted in the table; a row with drops is overloaded and its latency is not a service level.

## Workloads
| Name | Description |
|---|---|
| `steady` | 1 symbol, fixed mid price. 60% passive limits 1 to 100 ticks away from the touch, 25% cancels of (probably) live orders, 15% aggressive orders crossing up to 5 ticks (one third are market orders). 64 participants. About 5,000 resting orders |
| `deep` | Same mix over a 5,000-tick range with 100,000 resting orders. This is the workload that would expose level insertion cost (ADR-005) |
| `burst` | Same mix; alternating seconds at one tenth of the target rate and at the full target rate. Tests queueing and recovery from bursts |
| `multi` | 8 independent symbols, one engine goroutine each, the target rate split evenly. Shows scaling by symbol (ADR-006) |
| `recovery` | Write a log of 1,000,000 commands, crash (no final snapshot), time `engine.Open`, with and without a mid-log snapshot |

Some cancels in `steady` target orders an aggressive command has already filled; the engine rejects them (cheap path). The share is in the `rejects` column of the CSV.

## What the harness cannot do
- **It is a noisy machine.** The runs were made on a normal working laptop (IDE and browser open, `kernel_task` active), not an isolated benchmark host. Repeated identical saturation runs varied by up to 3x. That is why the tables show medians and ranges, and why tail latencies (p99 and above) at high rates are dominated by scheduler stalls, not by the engine's service time. `go test -bench` on `book.Apply` (single goroutine, no scheduler in the path) is included as the cleaner service-time figure.
- **The generator runs in the same process** as the engine and shares its CPUs and GC. At high rates this costs the engine some headroom; it also means the numbers are not network numbers (no HTTP, no WebSocket, no JSON).
- **No fsync-level control.** `wal-sync` measures the laptop SSD with `F_FULLFSYNC`; a Linux server with `fdatasync` on NVMe will differ in either direction.

## Changes made after seeing data
- Open-loop rates were chosen after a saturation probe (engine about 0.8M/s, `wal-sync` about 80k/s on this machine). The 400k/s `engine` row turned out to be overloaded (drops, p99 in the hundreds of milliseconds) and is reported as such rather than removed.
- The run matrix was started once; no runs were discarded. All raw rows are in the CSV.

## What will never be claimed
- Comparisons with other engines, Redis or Kafka (no like-for-like setup).
- "Microsecond latency" without the configuration, rate and percentile.
- Any throughput for the hosted instance.
- A number from a run that was not repeated.
- Durability from `wal-async` or `engine` rows.
