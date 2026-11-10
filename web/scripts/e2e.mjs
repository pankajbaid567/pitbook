// End-to-end check of the UI's client logic against a live server (no browser needed):
// compiles src/state.ts, subscribes over WebSocket, applies SNAPSHOT + EVENTS exactly as
// the UI does, and compares the rebuilt book with GET /v1/book at the same sequence.
//   node --experimental-websocket scripts/e2e.mjs http://127.0.0.1:8080 [seconds]
import { execFileSync } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const base = process.argv[2] || "http://127.0.0.1:8080";
const seconds = Number(process.argv[3] || 8);
const out = mkdtempSync(join(tmpdir(), "pitbook-state-"));
execFileSync("npx", ["tsc", "src/state.ts", "--outDir", out, "--module", "esnext", "--target", "es2022", "--moduleResolution", "bundler", "--skipLibCheck"], { stdio: "inherit" });
const { reducer, emptyBook, sortedLevels } = await import(pathToFileURL(join(out, "state.js")).href);

let book = emptyBook("PIT");
const dispatch = (msg) => { book = reducer(book, { type: "msg", msg }); };
let gaps = 0, frames = 0, resyncs = 0;
const ws = new WebSocket(base.replace(/^http/, "ws") + "/v1/ws");
await new Promise((res, rej) => { ws.onopen = res; ws.onerror = () => rej(new Error("ws connect failed")); });
ws.send(JSON.stringify({ type: "SUBSCRIBE", symbol: "PIT", depth: 200 }));
ws.onmessage = (ev) => {
  const m = JSON.parse(ev.data);
  if (m.type === "EVENTS") frames++;
  if (m.type === "RESYNC") resyncs++;
  const before = book.gap;
  dispatch(m);
  if (book.gap && !before) { gaps++; ws.send(JSON.stringify({ type: "SNAPSHOT_REQUEST", symbol: "PIT", depth: 200 })); }
};
await new Promise((r) => setTimeout(r, seconds * 1000));

// Compare with the server at a moment when our view is current: retry until sequences line up.
let ok = false, detail = "";
for (let i = 0; i < 40 && !ok; i++) {
  const srv = await (await fetch(base + "/v1/book/PIT?depth=200")).json();
  await new Promise((r) => setTimeout(r, 5));
  if (srv.last_seq !== book.lastSeq) continue; // bots move the book; wait for an aligned moment
  const mine = (m, bid) => sortedLevels(m, bid, 200);
  const same = (a, b) => a.length === b.length && a.every(([p, q], k) => p === b[k][0] && q === b[k][1]);
  ok = same(mine(book.bids, true), srv.bids) && same(mine(book.asks, false), srv.asks);
  detail = `aligned at seq ${srv.last_seq}: ${srv.bids.length} bid levels, ${srv.asks.length} ask levels`;
}
ws.close();
console.log(JSON.stringify({ frames, gaps, resyncs, trades: book.trades.length, fills: book.fills, lastSeq: book.lastSeq, ok, detail }));
if (!ok || gaps > 0) { console.error("FAIL: client view does not match the server, or a gap occurred"); process.exit(1); }
console.log("OK: UI client logic rebuilt the server's book from SNAPSHOT + contiguous frames");
