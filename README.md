# Pitbook

**A crash-safe matching engine and live order book, in Go.**

**Live demo: https://pitbook.baidpankaj567.workers.dev** · engine API: https://pitbook-demo.onrender.com (free tier: the first request after idle can take about a minute; the page plays a recorded session meanwhile)

A price-time-priority matching engine you can watch trade in the browser, kill with `SIGKILL`, and bring back with the same book. Event-sourced, deterministic, and built to run on free tiers (Render + Cloudflare).

## What is verified

Every number is generated from a raw file in `bench/results/`; see [BENCHMARKS.md](BENCHMARKS.md) and [docs/resume-claims.md](docs/resume-claims.md). Laptop numbers (Apple M3), single process.

| Claim | Result | Reproduce |
|---|---|---|
| Correct matching | Identical events to a naive reference model over **1,000,000** random commands; invariants checked after every command; **5 of 5** injected bugs caught | `make prop`, `make mutants` |
| Crash safe | **0 lost, 0 duplicated** across **1,000 SIGKILL runs** (117,609 acknowledged commands verified in the recovered log, 41,176 fills cross-checked) | `make crash-test` |
| Fast core | **137 ns** per command (single goroutine); about **670k commands/s** through the whole engine without a log, about **80k/s** with an fsync before every acknowledgement | `make bench` |
| Fast recovery | A 1,000,000-command log rebuilds in **0.32 s** | `make bench` |

Honest limits: these are laptop numbers from a busy machine (the report shows ranges and a noise-floor control), SIGKILL tests a *process* crash and not power loss, and the hosted free-tier instance is for looking at, not for measuring.

## What it does

- Matches limit, market, IOC and FOK orders with strict price-time priority; cancel, cancel-replace (keeps priority only when size shrinks at the same price) and self-trade prevention.
- Keeps the matching core **deterministic**: no clock, no randomness, no I/O. The same command log always produces the same trades, which is what makes replay, the reference-model test and crash recovery provable.
- Writes every command to a CRC-framed log and **fsyncs before acknowledging** (group commit: many commands per fsync). Recovers from a snapshot plus the log suffix; a torn last record is truncated, corruption anywhere else refuses to start.
- Retries are safe: the same `client_order_id` returns `duplicate`, never a second order, even across a crash.
- Streams depth and trades over WebSocket with sequence numbers so a client detects a gap and resyncs; a slow client is resynced and never slows the engine.
- Live UI: ladder, depth chart, tape, order entry, engine stats, and a **"Kill the engine"** button that exits the server abruptly and checks that nothing you saw was lost. If the free-tier host is asleep, a recorded session produced by the same engine plays until it wakes.

## What it is not

A **simulator**: no real money, one venue, one process per book, no FIX, no replication. "Crash-safe" means a process crash loses no acknowledged command and the book is rebuilt identically; it does not cover loss of the disk (the hosted demo mirrors its log to object storage for that, eventually, and says so).

## Architecture

```
 UI (Cloudflare Worker)        demo bots / clients
        │  WebSocket + REST          │
        ▼                            ▼
   ┌──────────────── gateway ────────────────┐
   │ validate · rate limit · idempotency     │
   └───────────────┬─────────────────────────┘
                   │ commands (bounded channel)
                   ▼
   ┌── sequencer: one goroutine per symbol ──┐
   │ seq → append to WAL → apply to book     │   book = pure, deterministic
   │ batch → fsync once → acknowledge        │
   └───────┬──────────────────────┬──────────┘
           │ events (sequenced)   │ WAL segments + snapshots
           ▼                      ▼
   fan-out to subscribers     local disk ──► mirror ──► R2 (via store Worker)
```
Details: [docs/architecture.md](docs/architecture.md). Every decision with its reason: [docs/DESIGN.md](docs/DESIGN.md) (17 short ADRs).

## Quickstart

```bash
go test -race ./...                 # everything except the long runs
make build                          # bin/pitbook, loadgen, replay, crashtest, genfeed
./bin/pitbook --demo --data ./data  # engine + bots on :8080, http://localhost:8080/v1/book/PIT
make crash-test                     # 1,000 SIGKILL runs against the real binary (~3 min)
make demo                           # engine on :8080 and the UI dev server on :5173
./bin/replay -data ./data -symbol PIT -runs 20   # rebuild state from the log, print the hash
```
Go 1.25+. The UI in `web/` needs Node 20+.

## Repository map

| Path | What |
|---|---|
| `internal/book` | deterministic matching core, reference model and property tests |
| `internal/engine` | state machine (book + idempotency), codec, sequencer loop, recovery |
| `internal/wal`, `internal/snapshot` | log segments, CRC framing, torn-tail handling; atomic snapshots |
| `internal/gateway` | REST + WebSocket, market-data hub, rate limits, `/healthz`, `/metrics` |
| `internal/archive` | mirror to / restore from object storage |
| `internal/bots` | seeded market-maker and noise traders for the demo |
| `cmd/pitbook` | the server · `cmd/crashtest` the SIGKILL harness · `cmd/loadgen` open-loop benchmark · `cmd/replay` · `cmd/genfeed` · `cmd/noisefloor` |
| `web/` | React + TypeScript UI |
| `deploy/` | Dockerfile, Render blueprint, store Worker, UI Worker |

## Documents

[prd](docs/prd.md) · [architecture](docs/architecture.md) · [DESIGN (ADRs)](docs/DESIGN.md) · [protocol](docs/protocol.md) · [testing](docs/testing.md) · [benchmark method](docs/benchmarks-method.md) · [deploy](docs/deploy.md) · [development plan](docs/development_plan.md) · [resume claims](docs/resume-claims.md)

## License

MIT.
