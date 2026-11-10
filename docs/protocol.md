# Protocol

Contract between clients (UI, bots, scripts) and the gateway, as implemented in `internal/gateway`. Engine-internal binary formats are in ADR-007 and `internal/engine/codec.go`. Version `v0` (changes freely until the project is tagged).

## Conventions
- JSON over WebSocket and REST, `snake_case` fields.
- `price` is an integer number of ticks and `qty` an integer number of lots (ADR-003). Never a float, never a decimal string. `price_scale` in `/v1/symbols` tells a UI how to display ticks (scale 100 = two decimals).
- `client_order_id` is an unsigned 64-bit integer chosen by the client, unique per `participant_id` (ADR-011). `participant_id` is a 1 to 32 character string; the engine sees `FNV-32a(participant_id)`.
- `seq` is the engine's per-symbol, gap-free sequence number of a **command**. All events produced by one command share its `seq` and carry an increasing `idx`. An order's `order_id` equals the `seq` of the command that created it.
- `ts_ms` is wall-clock time attached by the gateway for display. It is not part of the deterministic state.

## Commands

### NEW
```json
{"type":"NEW","symbol":"PIT","participant_id":"alice","client_order_id":1001,
 "side":"buy","order_type":"limit","price":10050,"qty":25}
```
`side`: `buy`/`sell`. `order_type`: `limit`, `market` (no price), `ioc`, `fok`. The gateway validates before sequencing: known symbol, positive bounded `price` and `qty`, tick and lot multiples, id length. A failure returns `REJECT` and **does not consume a sequence number** (tested).

### CANCEL
```json
{"type":"CANCEL","symbol":"PIT","participant_id":"alice","client_order_id":1002,"target_order_id":771204}
```
Only the owner can cancel. A cancel is naturally idempotent: repeating it after success yields `UNKNOWN_ORDER`.

### REPLACE
```json
{"type":"REPLACE","symbol":"PIT","participant_id":"alice","client_order_id":1003,"target_order_id":771204,"price":10055,"qty":20}
```
`qty` is the new open quantity. Same price and not larger: priority kept, same `order_id` returned. Otherwise the old order is cancelled (`REPLACED`) and a new order with a new `order_id` enters at the back of the queue (ADR-004). Replace is idempotent on `client_order_id`.

## Replies to a command
WebSocket: one message `{"type":"REPLY","events":[...]}`. REST: the same body, HTTP 200, or 422 if any event is a `REJECT`. A gateway-level validation failure is `{"type":"REJECT","code":"...","message":"..."}` (422).

| event `type` | fields (besides `seq`, `idx`) |
|---|---|
| `ACK` | `client_order_id`, `order_id`, `side`, `price`, `qty`, `status` = `accepted` or `duplicate` |
| `REJECT` | `client_order_id`, `code` |
| `FILL` | `trade_id`, `price`, `qty`, `maker_order_id`, `taker_order_id`, `taker_side` |
| `CANCELED` | `order_id`, `remaining_qty`, `reason` (`USER`, `IOC_REMAINDER`, `NO_LIQUIDITY`, `SELF_TRADE`, `REPLACED`), `side`, `price` |

A reply contains the events addressed to the submitter (acks, rejects, cancels, and fills of this command). It is sent **only after the command is durable** (fsync of the log). A `duplicate` ACK has `seq` 0: it was answered from the idempotency table and not sequenced.

Reject codes: `BAD_FIELD`, `UNKNOWN_SYMBOL`, `NOT_TICK_MULTIPLE`, `NOT_LOT_MULTIPLE`, `UNKNOWN_ORDER`, `FOK_NOT_FILLABLE`, `ERR_BUSY` (queue full or engine restarting, retry), `RATE_LIMITED`, `INTERNAL`.

## Market data (WebSocket `GET /v1/ws`)

Client to server: `SUBSCRIBE {symbol, depth}`, `SNAPSHOT_REQUEST {symbol, depth}`, `PING`. Server to client:

```json
{"type":"SNAPSHOT","symbol":"PIT","last_seq":4340,"price_scale":100,
 "bids":[[10017,16],[10016,29]],"asks":[[10018,76]],"trades":[{"trade_id":912,"seq":4331,"price":10017,"qty":4,"taker_side":"sell","ts_ms":1700000000000}]}

{"type":"EVENTS","symbol":"PIT","prev_seq":4340,"last_seq":4342,"ts_ms":1700000000060,
 "events":[{"type":"BOOK_DELTA","seq":4341,"idx":0,"side":"sell","price":10018,"qty":70},
           {"type":"FILL","seq":4341,"idx":1,"trade_id":913,"price":10018,"qty":6,"maker_order_id":4310,"taker_order_id":4341,"taker_side":"buy"}]}

{"type":"RESYNC","symbol":"PIT","reason":"SLOW_CONSUMER"}   // a SNAPSHOT follows immediately
```
`BOOK_DELTA.qty` is the **new total** at that price level (`0` removes the level). Public events are only `BOOK_DELTA` and `FILL`; acks and rejects are private.

### Rules clients must follow
1. **Start from the snapshot.** After `SUBSCRIBE` the first message is a `SNAPSHOT` with `last_seq = S`. The next `EVENTS` frame has `prev_seq = S`.
2. **Frames are contiguous.** Each `EVENTS` frame covers the commands in `(prev_seq, last_seq]`, **including commands that produced no public event** (the frame's `events` may be `[]`). Apply a frame only if `prev_seq` equals the last `last_seq` you applied. If it is larger, you have a gap: stop, send `SNAPSHOT_REQUEST` (or reconnect) and resume from the new snapshot. If `last_seq` is not greater than what you have, ignore it (duplicate).
3. **Slow consumers are resynced, not blocked.** Each connection has a bounded queue (256 messages). If it overflows, the server discards the stale frames and queues `RESYNC` plus a fresh `SNAPSHOT`. The engine never waits for a client.
4. **Retry with the same `client_order_id`.** After a timeout or a dropped connection, resend the identical command: the answer is `ACK` with `status:"duplicate"` and the original `order_id`, never a second order. This also holds across an engine crash and restart (the idempotency table is rebuilt from the log).
5. **Do not infer fills from deltas.** Fills arrive as `FILL`.

Limits: 4 KiB per message, 40 messages per second per connection (burst 80), `depth` up to 200. Cross-origin connections are accepted only from the configured `ALLOWED_ORIGIN`.

## REST

| Method and path | Purpose |
|---|---|
| `GET /healthz` | `200` with `status`, `last_seq`, `state_hash` (FNV digest of the full machine state), `recovery` (snapshot used, records replayed, time taken) and, if configured, `archive` counters. `503` while recovering or failed. |
| `GET /metrics` | Prometheus text: commands, batches, duplicates, busy rejects, queue depth, latency quantiles (end to end, fsync, queue wait), WS clients and dropped frames, Go memory and GC |
| `GET /v1/stats` | The same figures as JSON, for the UI |
| `GET /v1/symbols` | Symbol table (`tick`, `lot`, `price_scale`) |
| `GET /v1/book/{symbol}?depth=20` | `SNAPSHOT` shape |
| `GET /v1/trades/{symbol}?limit=100` | Recent fills, newest first (the last 200 are kept) |
| `POST /v1/orders` | Body is a `NEW` (or `CANCEL`/`REPLACE` with `type` set); unknown JSON fields are refused |
| `DELETE /v1/orders/{symbol}/{order_id}?participant_id=..&client_order_id=..` | Cancel |
| `POST /demo/crash` | Only with `--demo`: exits the process with status 137 after refusing new orders and (if configured) a final archive sync; global cooldown 60 s (ADR-013) |

REST order entry is rate limited per client address (default 30/s, burst 60; the rightmost `X-Forwarded-For` entry is used behind a proxy).

## Versioning
Breaking changes will bump the path prefix. While in `v0`, the changelog is the git history of this file.
