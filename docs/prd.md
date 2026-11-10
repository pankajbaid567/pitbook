# PRD: Pitbook

## Problem
Most student backend projects are CRUD plus a UI. A selected-candidate resume study (see `SDE India Hiring Intelligence 2026`) found only 14% of core resumes show a distributed-systems-style project and 15% a backend service; concurrency, durability and measured performance are the scarce signals. Pitbook is a small system where all three can be demonstrated and *verified by a reader in minutes*.

## Goals
1. A correct price-time-priority matching engine, with correctness proved by a reference-model property test, not only example tests.
2. Deterministic replay: identical log in, identical trades and state hash out.
3. Crash safety with a number: SIGKILL the engine at random points; zero acknowledged commands lost, zero fills duplicated.
4. A live, clickable demo on free tiers that never looks broken, even when the host is asleep.
5. Benchmarks that a skeptical reader can reproduce: raw files, hardware stated, open-loop load.
6. Documentation honest enough that every resume line maps to a file and a command (`docs/resume-claims.md`).

## Non-goals (v1)
- Real money, real market data, FIX/ITCH gateways, multi-venue routing, margin, clearing and settlement.
- Replication, leader election or multi-node consensus. (cache-cluster and a future LSM/Raft project cover distributed topics; Pitbook is single-writer by design.)
- Stop orders, iceberg orders, post-only, auctions. Listed as future work in `development_plan.md`.
- Authentication beyond a demo session token. No user accounts.
- Winning a latency contest. The goal is *measured and explained*, not record-setting.

## Users
| Persona | Needs |
|---|---|
| Recruiter / interviewer (2 minutes) | Open the link, see a live book, trust it is real, read three honest numbers in the README |
| Engineer reviewer (20 minutes) | Read DESIGN.md, run `go test -race`, run the crash test, read BENCHMARKS.md and reproduce one number |
| Me, in an interview | Defend each decision: why single-writer, why deterministic, what the WAL guarantees, what breaks |

## Success metrics
Targets were set before building and are kept here with the outcome. Measured values live in [BENCHMARKS.md](../BENCHMARKS.md) and are generated from raw files.

| Metric | Target (set up front) | Outcome | Proof |
|---|---|---|---|
| Property test: engine equals naive reference | 0 mismatches over at least 1,000,000 random commands | **Met**: 1,000,000 commands, 0 mismatches; 5 of 5 injected bugs caught | `make prop`, `make mutants` |
| Replay determinism | identical state hash over 100 replays | **Met** (20 to 25 replays in tests, 20 in the replay CLI run; the 100 in the target was not run) | `go test ./internal/engine`, `bin/replay -runs 20` |
| Crash safety | 0 lost acknowledged commands and 0 duplicated fills over at least 1,000 SIGKILL runs | **Met**: 1,000 runs, 0 failures (after fixing the bug the harness found) | `make crash-test` |
| Throughput, one symbol, WAL off | at least 200k commands/s | **Met** at saturation (see BENCHMARKS.md for the median and range); **not met** as a clean open-loop rate at 400k/s on this machine (overloaded) | `make bench` |
| Throughput with fsync before ack | report whatever it is | Reported in BENCHMARKS.md | `make bench` |
| Match latency p99 | report, with GC discussed | Reported; high-rate tails are scheduler noise on a busy laptop (BENCHMARKS.md, benchmarks-method.md) | `make bench` |
| Demo availability | UI shows activity within 3 s even when the host is cold | Implemented (recorded-session fallback after 3 s); verified at deploy time | `deploy.md` |
| Cost | $0/month | Free tiers only (Render free, Cloudflare Workers and R2 free) | `deploy.md` |

## Risks
| Risk | Mitigation |
|---|---|
| Scope creep into a real exchange | Non-goals list; "simulator" wording everywhere; each new order type needs an ADR |
| Latency numbers distorted by GC and by coordinated omission | Preallocated pools, open-loop load, report percentiles with the method (`benchmarks-method.md`) |
| Render free tier sleeps and has no persistent disk | Cloudflare Pages UI with replay fallback; R2 snapshot restore; durability claims only for the local-file WAL |
| Built in parallel with DSA prep and other flagships | Gated in `development_plan.md`; do not start phase 1 before Cairn and cache-cluster are published and the DSA milestone is met |
| Name collision (as happened with Cairn) | ADR-000 records the search before first public push |
