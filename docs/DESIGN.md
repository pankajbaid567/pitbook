# DESIGN.md: decisions (short ADRs)

Status values: **accepted** (decided, build on it), **proposed** (default chosen, revisit with evidence), **open** (needs an action before it is final).
A decision changes only by adding a new ADR that supersedes it.

## ADR-000: Name and collision check
**Status: accepted. Name: Pitbook** ("pit" as in trading pit, plus order book), Go module `pitbook`. The project was first scaffolded as "Bourse" and renamed on 2026-11-10 after the check below.
Check run 2026-11-10 (GitHub search API + web search; unauthenticated, so counts are approximate and can miss private or very new repos):
- **Bourse: collides, rejected.** 5 repos match "bourse matching engine", including `Yash-Aanand/Bourse` (single-threaded deterministic limit-order-book engine), `samad-zeeshan/Bourse` (deterministic price-time matching engine), `pauti04/bourse` (Rust price-time-priority engine) and `Shashidharahegde/BourseEngine` (Java). Same niche, same pitch.
- **Crossbook: collides, rejected.** `ult666666/crossbook` (deterministic limit order book in Rust) and `frdrckj/crossbook` (Rust matching engine DEX).
- **Parity: too generic, rejected.** Thousands of unrelated repos.
- **Pitbook: clear, chosen.** "pitbook matching engine" returns 0 GitHub repos; the 6 repos named pitbook are unrelated (an F1 wiki, a car-fleet tracker, and similar); a web search found no trading project of that name. Not checked: domain names, trademarks, npm/PyPI/crates names.

## ADR-001: Scope is a simulator
**Status: accepted.** Single venue, no real money, no FIX, no replication. Wording in README and resume avoids "exchange-grade" and "fault-tolerant". Reason: a small claim fully proved is worth more than a large claim partly proved, and the report's own advice is to defend tradeoffs.

## ADR-002: Deterministic core
**Status: accepted.** `internal/book` has no wall-clock time, no randomness, no I/O, no goroutines and no map-iteration-order dependence in anything that affects output. Timestamps and trade IDs are supplied by the sequencer (`seq` doubles as the logical clock; wall time, if shown in the UI, is attached outside the core).
Why: replay, the reference-model property test, crash recovery and snapshot verification all reduce to "same input gives same output". If the core is impure, none of those can be proved.
Consequence: any iteration over a Go map that feeds output must sort first. A lint-style test replays a log twice with Go's map randomisation and compares hashes.

## ADR-003: Integer money
**Status: accepted.** `Price` is int64 ticks, `Qty` is int64 lots. No floats in the engine, the log, or the wire protocol. The gateway rejects prices and quantities that are not multiples of the symbol's tick and lot. Display conversion happens in the UI only.

## ADR-004: Order types and exact semantics (v1)
**Status: accepted.**
- **Limit:** match while the best opposite price crosses; rest the remainder.
- **Market:** match at any price until filled or the book is empty; never rests. An unfilled remainder is cancelled with reason `NO_LIQUIDITY`.
- **IOC:** limit order; unfilled remainder cancelled, never rests.
- **FOK:** executes only if the whole quantity can fill immediately at or better than the limit; otherwise rejected with no fills and no book change. Implemented by a dry-run scan of the opposite side before any mutation.
- **Trade price:** the resting (maker) order's price. The incoming order is the taker.
- **Priority:** price, then arrival sequence. Within a level, strictly FIFO.
- **Cancel:** removes a resting order; unknown or already-filled ID gives a `REJECT` with `UNKNOWN_ORDER`.
- **Cancel-replace:** reducing quantity at the same price keeps priority; changing price or increasing quantity cancels and re-enters at the back of the queue (new sequence).
- **Self-trade prevention:** per `participant_id`; when an incoming order would match a resting order of the same participant, the **incoming** order's remaining quantity is cancelled with reason `SELF_TRADE` (cancel-newest). No self-trade fill is ever produced.
- **Deferred:** post-only, stop, iceberg, auctions.
Why these: they cover the interview-standard set, and each one has a crisp invariant the reference model can check.

## ADR-005: Book data structure
**Status: accepted (measured).** Per side: a slice of price levels kept sorted worst-to-best (so the best level is the last element and popping it is O(1)) plus `map[Price]*level`; each level an intrusive doubly linked FIFO of `*Order`; one `map[OrderID]*Order` index for O(1) cancel. Inserting a new price level is a binary search plus a slice shift, O(levels), which is small and cache-friendly for realistic books.
Decided **not** to add an order pool or a B-tree: the measured allocation rate is about 0.7 to 1.7 allocations per command (one `*Order` per resting order plus the cancel index entry) and GC pauses total about 1 ms per 20 s run (BENCHMARKS.md), so neither is justified yet. The `deep` workload (100,000 resting orders over a 10,000-tick range) is the test that would show level insertion dominating; if its p99 degrades materially against `steady`, revisit with a B-tree and record the comparison here.

## ADR-006: Single-writer sequencer per symbol
**Status: accepted.** One goroutine owns each book; all commands go through a bounded channel. No locks in the matching path.
Alternatives rejected: a global mutex (serialises symbols, adds contention for no benefit) and lock-free sharding inside one book (complexity without a use case at this scale). Throughput scales by adding symbols, not by parallelising one book, which is also how real engines are structured.

## ADR-007: WAL format and group commit
**Status: proposed.**
- Record: `len:u32 | crc32c:u32 | seq:u64 | type:u8 | payload`, little-endian. CRC covers `seq|type|payload`. Payload is a fixed-layout binary command.
- Segments named by first sequence number, rotated by size (default 64 MiB) or record count.
- **Group commit:** the engine takes whatever commands are already queued (up to `MaxBatch`, default 256), appends them all, then `fsync`s once. There is no timer and no artificial wait, so an idle engine adds no latency and a busy one batches naturally. Acks and public events for the batch are released after the fsync. (An earlier draft had a time-based flush; it was dropped because it can only add latency.)
- Modes: `sync` (default; ack after fsync), `async` (ack before fsync, for benchmarks, **never** used for a durability claim), `off` (benchmarks of the pure engine only).
- Parent directory is fsynced on segment creation and rename.
Why binary and CRC32C: small records, hardware-accelerated checksum, and a torn tail is detectable.

## ADR-008: Snapshots
**Status: proposed.** Every N commands (default 100,000) or on clean shutdown, the engine writes `snap-<seq>.snap` via temp file + fsync + rename + directory fsync. Contents: last seq, all resting orders in priority order, next order ID, idempotency table, and a CRC. Snapshot creation copies state at a batch boundary (it never pauses the matching loop for I/O longer than the copy; the file write runs in a separate goroutine from the copy). Old WAL segments fully covered by a snapshot can be deleted.
Verification: after loading a snapshot and replaying the suffix, the state hash must equal a full replay from sequence 1 (tested).

## ADR-009: Durability on free tiers
**Status: accepted (revised).** Render's free web service has no persistent disk. The engine always writes to the local disk. `internal/archive` mirrors the WAL segments and snapshots to a Cloudflare R2 bucket in the background (every 10 s, only changed files, remote files removed when the engine trims them) and, on a container with an empty disk, restores them before the engine opens.
The bucket is reached through a small Cloudflare Worker (`deploy/worker-store`, bearer-token protected, accepts only the key shapes the engine produces) rather than the S3 API: it needs no R2 S3 credentials, no request signing code, and the token is one shared secret that can be rotated. Restore failures stop the process (crash loop, visible in the logs) instead of starting empty, because an empty start would be mirrored over the real data.
The mirror is eventually durable: the active segment can be up to one interval behind. The demo crash button therefore pauses order entry and runs a final sync before exiting (ADR-013). **All durability claims are about the local-file WAL** and are proved by `cmd/crashtest`; the hosted path is described as "restores demo state after a restart". Neon Postgres was considered and rejected: an append-only log fits object storage, and it avoids explaining a second database.

## ADR-010: Client protocol and slow consumers
**Status: accepted.** WebSocket JSON for market data and for order entry from the UI (readable in browser dev tools, which helps a reviewer); the engine-internal command format is binary. REST for health, metrics, snapshots, and order entry for scripts. Every public event carries `seq`; a client that sees `seq` jump asks for a snapshot. Each client has a bounded queue; overflow sends `RESYNC` and drops the stale deltas. The engine never blocks on a client. See [protocol.md](protocol.md).

## ADR-011: Idempotency
**Status: accepted.** `(session_id, client_order_id)` is unique. A repeat returns the original `ACK` (and its current status) without a new command being logged. The idempotency table is part of snapshot state and is rebuilt from the log on recovery, so a client that retries after a crash is still safe.

## ADR-012: Benchmarking method
**Status: accepted.** Open-loop load generation (arrivals follow a schedule independent of response time) to avoid coordinated omission; fixed seeds; hardware, Go version, GOMAXPROCS and commit hash recorded next to the raw CSVs; `BENCHMARKS.md` generated from the CSVs by `scripts/make_benchmarks.py`. Engine-only, WAL-off and WAL-on-fsync are reported separately. Details: [benchmarks-method.md](benchmarks-method.md).

## ADR-013: Demo-only crash endpoint
**Status: accepted (revised).** The UI has a "Kill the engine" button. `POST /demo/crash` exists only with `--demo`, has a **global 60-second cooldown** (a per-visitor token would be public anyway, so the cooldown is the real control), and:
1. sets a pause flag so new orders get `ERR_BUSY` and the bots stop (nothing can be acknowledged after the next step);
2. waits 300 ms for in-flight commands, then runs a final archive sync if an archive is configured;
3. exits with status 137 **without** `engine.Close`: no drain, no final snapshot, no graceful shutdown.
The platform restarts the service, the new container restores the log from the bucket and replays it, and the UI checks that the recovered sequence is at least the last sequence it had displayed. This demonstrates the recovery path; it is not the evidence for crash safety (that is `cmd/crashtest`, which kills at random moments under load). It is documented as a demo feature, not an admin API.

## ADR-014: Language and tooling
**Status: accepted.** Go 1.25, standard library first (`net/http`, `hash/crc32`, `os`, `encoding/json`). One third-party dependency: `github.com/coder/websocket` v1.8.15 (maintained, context-aware, no cgo) for the WebSocket server. No ORM, no framework. UI: React 18 + TypeScript + Vite, no chart library (the depth chart is hand-drawn SVG), served as static assets by a Cloudflare Worker. CI: GitHub Actions (`gofmt`, `go vet`, `go test -race`, UI build).

## ADR-015: Idempotency scope
**Status: accepted.** Idempotency (ADR-011) covers `NEW` and `REPLACE`, the commands that create orders. `CANCEL` is naturally idempotent (a repeat finds nothing and is rejected `UNKNOWN_ORDER`). The outcome of a command is recorded from its first `ACK` or `REJECT` event wherever it sits in the event list.
Origin of that last rule: the crash harness found a double-logged `REPLACE` in about 4% of runs (22 of the first ~500). A replace that loses priority emits `CANCELED` (old order) before its `ACK`, and the first implementation only looked at the first event, so such replaces were never recorded and a retry was sequenced again. Regression test: `TestReplaceThatLosesPriorityIsIdempotent`.
Known limit: the table is unbounded (one 24-byte entry per order ever sent). Fine for a demo; a production design would expire entries after a retry window.

## ADR-016: Participant identity
**Status: accepted.** `participant_id` strings are mapped to the engine's `uint32` by FNV-32a, a pure function, so the mapping needs no state and survives restarts and replays. Two different strings can collide (about 1 in 4 billion per pair); for a simulator with no accounts this is accepted, and it is stated here so nobody mistakes the hash for an identity system.

## ADR-017: Frames are contiguous, even when empty
**Status: accepted.** Every command batch becomes one `EVENTS` frame with `prev_seq`/`last_seq`, including batches with no public events (for example a rejected FOK). Skipping empty frames would make `prev_seq` jump and every client would correctly report a gap. The recorded feed (`cmd/genfeed`) found this the hard way: the first version dropped empty frames and its own contiguity check failed.
