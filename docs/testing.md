# Testing strategy

The point of the suite is that every claim on the resume is backed by something that can fail. This file says what each layer proves, what it does not, and how to run it. Results are in [BENCHMARKS.md](../BENCHMARKS.md).

## Layers

| Layer | Where | Proves | Does not prove |
|---|---|---|---|
| Rule examples | `internal/book/book_test.go` | Hand-computed scenarios for every rule in ADR-004: price then time priority, trade at the resting price, market / IOC remainder, FOK all-or-nothing and no mutation on reject, self-trade prevention, cancel ownership, replace priority rules, bad fields, snapshot round trip | Anything beyond the examples |
| **Reference-model property test** | `internal/book/property_test.go`, `reference_test.go` | A naive matcher (flat slice, linear scans, 150 lines, shares no code with the book) and the real book produce **identical event streams** for random command streams, and `Book.Check()` (sorted levels, level totals, FIFO order, index consistency, no crossed book) passes after **every** command. `make prop` runs 1,000,000 commands (382,819 fills, 118,607 self-trade cancels, 55,099 FOK rejects in the recorded run); `go test` runs 200,000 | That the reference itself is right. It is small enough to audit, and it is cross-checked by the hand-computed examples |
| **Mutation check** | `scripts/mutation_check.sh` | Five deliberately injected bugs (STP off, crossing off-by-one, FOK ignoring STP, replace keeping priority when size grows, market orders resting) are each **caught** by the property test. This shows the test has teeth | That every possible bug is caught |
| Determinism | `TestDeterministicReplay`, `TestReplayDeterminism` | Replaying one command stream 20 to 25 times (Go randomises map iteration) gives identical events and identical state hashes | Determinism across CPU architectures or Go versions (only macOS arm64 and Linux CI are exercised) |
| Codec | `internal/engine/codec_test.go`, `FuzzDecodeCommand` | The decoder never panics; anything it accepts re-encodes to the same bytes (canonical form) | Semantic validity (the book rejects bad values deterministically) |
| WAL | `internal/wal/wal_test.go`, `FuzzOpenNeverPanics` | Round trip; rotation and trimming; **file cut at every byte offset** recovers exactly the whole records before the cut and stays appendable; flipped bit in the last segment is a torn tail; corruption in an earlier segment or a missing middle segment is refused; read-only replay never modifies files | Behaviour of the OS and disk below `fsync` |
| Snapshots | `internal/snapshot` | Atomic write; every truncation rejected; corrupt newest snapshot falls back to the older one; name/sequence mismatch skipped | n/a |
| Engine | `internal/engine/engine_test.go` | Sequence assignment; idempotent retry (also across a crash); recovery after crash equals live state; **snapshot + suffix recovery equals full replay from sequence 1**; WAL trimming after snapshots; torn tail loses only the torn record; refusal when records are missing; back-pressure (`ErrBusy`); `Close` drains everything accepted; concurrent submitters under `-race` | Power loss |
| Gateway | `internal/gateway/gateway_test.go` | Order flow over REST and WebSocket; validation rejects consume no sequence number; idempotent retry; cancel ownership; rate limits; CORS and origin checks; **a client that rebuilds its view from SNAPSHOT + contiguous frames ends with exactly the server's book**; a stalled subscriber is flagged, resynced with a current snapshot, and never slows the producer; `/healthz` is 503 until recovery completes; crash endpoint is 404 without `--demo` and cooldown-limited with it | Browser behaviour |
| Archive | `internal/archive/archive_test.go` | Store contract (file and HTTP, including auth); restore only into an empty disk; mirror uploads changes and deletes trimmed files; **a wiped disk recovers identical state from the bucket** | Real R2 behaviour and latency (checked at deploy time) |
| Bots | `internal/bots/bots_test.go` | Same seed gives the same market; the market is lively (fills, no errors); the book stays bounded over 20,000 steps | n/a |
| **Crash harness** | `cmd/crashtest`, `make crash-test` | See below | Power loss, disk failure |
| Race detector | `go test -race ./...` | No data races in exercised paths | Paths no test exercises |

## The crash test, precisely
One run:
1. Start the real `pitbook` binary with `--wal sync --keep-all-wal`, a small snapshot interval (100 to 600 commands) and small segments.
2. Four concurrent HTTP clients send a seeded mix of NEW (limit, market, IOC, FOK), CANCEL and REPLACE. Each records every response it receives (these are only sent after fsync).
3. After a random delay the harness sends **SIGKILL** to the process.
4. Restart on the same data directory. Replay the whole log from sequence 1 in the harness and check:
   - every acknowledged command is in the log **at the sequence number the client was told** and is **identical** to the command that was sent (no loss);
   - every fill a client saw equals the fill the replay derives for that trade id; no trade id appears twice (no duplicate fills);
   - the restarted server's `state_hash` and `last_seq` equal the full replay's (snapshot + suffix recovery equals replay);
   - no `(participant, client_order_id)` appears twice as NEW or REPLACE in the log;
5. Re-send every command that was in flight at the kill (no response received) with the same client order id. A command that had reached the log must be answered `duplicate` (never accepted again); one that had not is accepted. Re-verify the log and the state hash afterwards.

The harness was itself validated by breaking the server on purpose: with the fsync removed it reports `LOST: client … was acknowledged at seq N but the recovered log ends at M`; with idempotency disabled it reports `DOUBLE ORDER`. It also found a real bug in the first version (ADR-015). A claim "0 lost, 0 duplicated over N runs" is made only with the N and the seeds in `bench/results/crashtest-*.json`.

## Limits stated up front
- SIGKILL tests a **process** crash. The OS page cache survives it, so power loss and disk failure are not tested. Claims say "process crash", never "power failure".
- Runs are short (a few hundred acknowledged commands); most recoveries replay the log from the start. Snapshot recovery is exercised in a minority of runs and in the engine unit tests.
- Everything runs on macOS (development) and Linux (CI). No other platform is claimed.

## How to run
```bash
go test -race ./...                  # everything except the long runs, about 30 s
make prop                            # 1,000,000-command reference-model proof, about 90 s
./scripts/mutation_check.sh          # the property test must catch every injected bug
make build && make crash-test        # 1,000 SIGKILL runs, about 3 minutes
go test -run xxx -fuzz FuzzDecodeCommand -fuzztime 60s ./internal/engine
```

## Definition of done for a phase
Its exit criteria in `development_plan.md` are met, `go vet` and `go test -race ./...` pass, new behaviour has a test that fails without it, and any number quoted in the docs is generated by a command listed in `resume-claims.md`.
