# Architecture

Decisions and their reasons are in [DESIGN.md](DESIGN.md). This file describes the shape of the system.

## Components

| Component | Package | Responsibility | Concurrency |
|---|---|---|---|
| Book | `internal/book` | Matching rules, price-time priority, STP. Pure function of state + command. | None. Single caller. |
| Engine / sequencer | `internal/engine` | Owns one book per symbol: assigns sequence numbers, appends to the WAL, applies commands, emits events. | One goroutine per symbol, fed by a bounded channel |
| WAL | `internal/wal` | Append-only CRC-framed segments, group commit, recovery scan | Writer owned by the engine goroutine; fsync batching |
| Snapshot | `internal/snapshot` | Atomic book snapshot with last sequence and checksum | Written by the engine goroutine between batches |
| Gateway | `internal/gateway` | WebSocket market data, REST commands, session and idempotency handling, per-client queues | One goroutine per connection plus one fan-out goroutine |
| Bots | `internal/bots` | Seeded market makers and noise traders for the live demo | One goroutine per bot, talk to the gateway like any client |
| Metrics | `internal/metrics` | Latency histogram, counters, `/metrics` | Per-worker histograms, merged on read |
| Archive | `internal/archive` | Mirrors WAL segments and snapshots to R2 through the store Worker; restores them on an empty disk | One background goroutine per symbol |
| Recorder | `internal/gateway/recorder.go`, `cmd/genfeed` | Produces the recorded session the UI plays while a sleeping host wakes up | Offline tool |
| UI | `web/` | Ladder, depth chart, tape, order entry, live stats, crash-recovery check | Browser |

## Threading model

```
 conn goroutines ──► [ cmd channel (bounded) ] ──► engine goroutine (per symbol)
                                                    │
                                    1. seq := next()                      (no locks: single owner)
                                    2. wal.Append(seq, cmd)               (buffered)
                                    3. events := book.Apply(seq, cmd)     (pure, deterministic)
                                    4. batch events until fsync point
                                    5. wal.Sync()                         (group commit)
                                    6. publish events (acks, fills, deltas) to fan-out
```

- Nothing in the matching path takes a lock. Shared state is only the channels at the edges.
- A command is **acknowledged to its client only after step 5** (the fsync covering its record). Market-data deltas may be published at the same point; subscribers never see state that could vanish after a crash. This is the property the crash test checks.
- Backpressure: the command channel is bounded. When it is full, the gateway rejects with `ERR_BUSY` instead of queueing without limit.
- Slow consumers: each client has a bounded outbound queue. If it overflows the client is told to resync (`RESYNC` message) and the stale deltas are dropped. The engine never waits for a client.

## Data flow for one order

1. Client sends `NEW` over WebSocket or REST with a `client_order_id`.
2. Gateway validates fields (price > 0, qty > 0, known symbol, tick and lot multiples) and checks the idempotency table. A repeated `(session, client_order_id)` gets the original ack back, not a second order.
3. Gateway pushes the command to the symbol's channel.
4. Engine: sequence, WAL append, `book.Apply`, collect events (`ACK`, `FILL`s, `BOOK_DELTA`s, maybe `CANCELED` for an IOC remainder).
5. After fsync, events are published in sequence order. The submitter receives its `ACK` and `FILL`s; all subscribers receive deltas and trades.

## Matching core

- Per side, price levels sorted best-first. Each level is an intrusive FIFO list of orders (arrival order = time priority).
- `map[OrderID]*Order` gives O(1) cancel; removing from the middle of a FIFO list is O(1) with intrusive links.
- Trades execute at the **resting** order's price. The incoming order is the taker.
- Order objects come from a preallocated pool to keep allocation (and GC) out of the hot path. Whether the pool is worth its complexity is decided by a benchmark, recorded in an ADR.

## Persistence

```
 data/
   wal/
     wal-00000000000000000001.log      first record has seq 1
     wal-00000000000000050001.log      rotated at a size or record threshold
   snap/
     snap-00000000000000040000.snap    state after applying seq 40000, with CRC
```
Recovery:
1. Pick the newest snapshot that passes its CRC (fall back to older, then to empty).
2. Replay WAL records with `seq > snapshot.seq` in order.
3. In the **last** segment, a record that is truncated or fails its CRC marks a torn tail: truncate the file there and continue. In an **earlier** segment, the same condition is real corruption: refuse to start and say so.
4. Verify the state hash equals the one a clean replay of the whole log would give (done in tests and by `cmd/replay`).

Hosted demo: Render's free tier has no persistent disk. The engine still writes only to its local disk; `internal/archive` mirrors new and changed files to a Cloudflare R2 bucket every 10 s (through a small authenticated Worker, `deploy/worker-store`) and, on a container with an empty disk, restores them before the engine opens. If the bucket cannot be read at boot the process exits instead of starting empty (ADR-009). Durability claims are made for the local-file WAL only; the mirror is eventually durable and exists so the demo is not empty after waking.

## Failure modes

| Failure | What happens | How it is tested |
|---|---|---|
| Process killed mid-batch | Unsynced records may be lost; none of them were acknowledged. Restart replays to the last durable record. | `cmd/crashtest`, SIGKILL at random points |
| Torn last record | Detected by length/CRC, truncated, startup continues | WAL unit test that cuts a file at every byte offset |
| Corrupt earlier segment | Startup refuses with a clear error | WAL unit test |
| Duplicate client submit | Original ack returned, no second order | gateway test |
| Slow WebSocket client | Dropped from the live stream and sent `RESYNC`; engine unaffected | gateway test with a stalled reader |
| Client misses a delta | Sequence gap detected client-side, client fetches a snapshot | protocol test, UI behaviour |
| Render instance sleeps | UI plays the recorded session, shows a "waking" banner, reconnects when live | `deploy.md` cold-start check |
| Object store unreachable at boot | Process exits with an error (crash loop, visible in logs) rather than starting empty and overwriting the mirror | `internal/archive` tests, `cmd/pitbook` |
| Loss of the disk | **Not handled.** Out of scope; stated in the README | n/a |

## Observability
`/metrics` (Prometheus text) and `/v1/stats` (JSON, read by the UI) expose: commands, batches and duplicates, queue depth and busy rejects, end-to-end / fsync / queue-wait latency quantiles, average batch size, connected clients and dropped (resynced) frames, goroutines, heap and GC pause totals.
