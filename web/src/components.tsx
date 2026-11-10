import { useEffect, useMemo, useRef, useState } from "react";
import { crash, getHealth, getProof, getStats, postOrder, OrderReply, Proof, Stats } from "./api";
import { BookState, EventJSON, fmtPrice, sortedLevels } from "./state";
import { Mode } from "./feed";

const fmtInt = (n: number) => n.toLocaleString("en-US");
const fmtUS = (us: number) => (us >= 1000 ? `${(us / 1000).toFixed(us >= 10000 ? 0 : 1)} ms` : `${us.toFixed(us >= 100 ? 0 : 1)} µs`);

export function Header({ book, mode }: { book: BookState; mode: Mode }) {
  const label = mode === "live" ? "LIVE" : mode === "replay" ? "REPLAY · engine asleep, waking" : "CONNECTING";
  return (
    <header className="hdr">
      <div className="brand">
        <svg width="28" height="28" viewBox="0 0 28 28" aria-hidden="true">
          <rect x="2" y="3" width="6" height="22" rx="1.5" fill="var(--bid)" opacity=".85" />
          <rect x="11" y="9" width="6" height="16" rx="1.5" fill="var(--accent)" />
          <rect x="20" y="14" width="6" height="11" rx="1.5" fill="var(--ask)" opacity=".85" />
        </svg>
        <div>
          <h1>Pitbook</h1>
          <p>Crash-safe matching engine · Go · event-sourced · deterministic replay</p>
        </div>
      </div>
      <div className="hdr-right">
        <span className={`chip ${mode}`}><i /> {label}</span>
        <span className="chip plain">{book.symbol}</span>
        <span className="chip plain mono">seq {fmtInt(book.lastSeq)}</span>
      </div>
    </header>
  );
}

export function Ladder({ book }: { book: BookState }) {
  const rows = 11;
  const asks = useMemo(() => sortedLevels(book.asks, false, rows), [book.asks]);
  const bids = useMemo(() => sortedLevels(book.bids, true, rows), [book.bids]);
  const cum = (ls: [number, number][]) => { let c = 0; return ls.map(([p, q]) => [p, q, (c += q)] as [number, number, number]); };
  const aC = cum(asks), bC = cum(bids);
  const max = Math.max(1, aC.at(-1)?.[2] ?? 0, bC.at(-1)?.[2] ?? 0);
  const spread = asks[0] && bids[0] ? asks[0][0] - bids[0][0] : null;
  const last = book.trades.at(-1);
  return (
    <section className="panel ladder" aria-label="Order book">
      <h2>Order book</h2>
      <div className="lhead"><span>Price</span><span>Size</span><span>Total</span></div>
      <div className="rows">
        {[...aC].reverse().map(([p, q, c]) => (
          <div className="row ask" key={p}><b style={{ width: `${(c / max) * 100}%` }} /><span>{fmtPrice(p, book.scale)}</span><span>{fmtInt(q)}</span><span>{fmtInt(c)}</span></div>
        ))}
        <div className="mid">
          <span className={last ? (last.taker_side === "buy" ? "up" : "down") : ""}>{last ? fmtPrice(last.price, book.scale) : "—"}</span>
          <small>spread {spread === null ? "—" : fmtPrice(spread, book.scale)}</small>
        </div>
        {bC.map(([p, q, c]) => (
          <div className="row bid" key={p}><b style={{ width: `${(c / max) * 100}%` }} /><span>{fmtPrice(p, book.scale)}</span><span>{fmtInt(q)}</span><span>{fmtInt(c)}</span></div>
        ))}
      </div>
    </section>
  );
}

export function DepthChart({ book }: { book: BookState }) {
  const W = 640, H = 230, P = 26;
  const bids = sortedLevels(book.bids, true, 30), asks = sortedLevels(book.asks, false, 30);
  if (!bids.length || !asks.length) return <section className="panel"><h2>Market depth</h2><div className="empty">waiting for data…</div></section>;
  const lo = bids.at(-1)![0], hi = asks.at(-1)![0];
  const x = (p: number) => P + ((p - lo) / Math.max(1, hi - lo)) * (W - 2 * P);
  let cb = 0; const bc = bids.map(([p, q]) => [p, (cb += q)] as [number, number]);
  let ca = 0; const ac = asks.map(([p, q]) => [p, (ca += q)] as [number, number]);
  const top = Math.max(cb, ca, 1);
  const y = (c: number) => H - P - (c / top) * (H - 2 * P);
  const area = (pts: [number, number][]) => {
    const line = pts.map(([p, c], i) => `${i ? "L" : "M"}${x(p).toFixed(1)},${y(c).toFixed(1)}`).join(" ");
    return `${line} L${x(pts.at(-1)![0]).toFixed(1)},${H - P} L${x(pts[0][0]).toFixed(1)},${H - P} Z`;
  };
  const midX = (x(bids[0][0]) + x(asks[0][0])) / 2;
  return (
    <section className="panel" aria-label="Market depth chart">
      <h2>Market depth <small>cumulative size, top 30 levels per side</small></h2>
      <svg viewBox={`0 0 ${W} ${H}`} className="chart" role="img" aria-label="Cumulative depth of bids and asks">
        <path d={area(bc)} fill="var(--bid)" fillOpacity=".22" stroke="var(--bid)" strokeWidth="1.5" />
        <path d={area(ac)} fill="var(--ask)" fillOpacity=".22" stroke="var(--ask)" strokeWidth="1.5" />
        <line x1={midX} x2={midX} y1={P / 2} y2={H - P} stroke="var(--muted)" strokeDasharray="3 4" />
        <text x={P} y={H - 8} className="ax">{fmtPrice(lo, book.scale)}</text>
        <text x={W - P} y={H - 8} textAnchor="end" className="ax">{fmtPrice(hi, book.scale)}</text>
        <text x={midX} y={H - 8} textAnchor="middle" className="ax">mid</text>
        <text x={P} y={14} className="ax">{fmtInt(top)} lots</text>
      </svg>
    </section>
  );
}

export function Tape({ book }: { book: BookState }) {
  const t = book.trades.slice(-120);
  const rows = book.trades.slice(-14).reverse();
  let spark = "";
  if (t.length > 1) {
    const lo = Math.min(...t.map((x) => x.price)), hi = Math.max(...t.map((x) => x.price));
    spark = t.map((x, i) => `${(i / (t.length - 1)) * 300},${34 - ((x.price - lo) / Math.max(1, hi - lo)) * 30}`).join(" ");
  }
  return (
    <section className="panel" aria-label="Trade tape">
      <h2>Trades <small>{fmtInt(book.fills)} seen this session</small></h2>
      <svg viewBox="0 0 300 38" className="spark" preserveAspectRatio="none" aria-hidden="true"><polyline points={spark} fill="none" stroke="var(--accent)" strokeWidth="1.6" /></svg>
      <div className="tape">
        {rows.map((x) => (
          <div key={x.trade_id} className={x.taker_side === "buy" ? "up" : "down"}>
            <span>{fmtPrice(x.price, book.scale)}</span><span>{fmtInt(x.qty)}</span><span className="muted">#{x.trade_id}</span>
          </div>
        ))}
        {!rows.length && <div className="empty">no trades yet</div>}
      </div>
    </section>
  );
}

const visitor = (): string => {
  try {
    let v = localStorage.getItem("pitbook-visitor");
    if (!v) { v = "visitor-" + Math.random().toString(36).slice(2, 8); localStorage.setItem("pitbook-visitor", v); }
    return v;
  } catch { return "visitor-anon"; }
};

interface Line { id: number; text: string; kind: "ok" | "bad" | "info" }

const describe = (r: OrderReply, scale: number): string => {
  if (!r.events || !r.events.length) return `${r.code ?? r.type}: ${r.message ?? ""}`;
  return r.events.map((e: EventJSON) => {
    if (e.type === "ACK") return `ACK #${e.order_id} ${e.status}`;
    if (e.type === "FILL") return `FILL ${e.qty} @ ${fmtPrice(e.price!, scale)}`;
    if (e.type === "REJECT") return `REJECT ${e.code}`;
    if (e.type === "CANCELED") return `CANCELED ${e.remaining_qty} (${e.reason})`;
    return e.type;
  }).join(" · ");
};

export function OrderForm({ book, mode }: { book: BookState; mode: Mode }) {
  const [side, setSide] = useState<"buy" | "sell">("buy");
  const [type, setType] = useState<"limit" | "market" | "ioc" | "fok">("limit");
  const [price, setPrice] = useState("");
  const [qty, setQty] = useState("5");
  const [lines, setLines] = useState<Line[]>([]);
  const [busy, setBusy] = useState(false);
  const coid = useRef(Date.now() * 1000);
  const who = useMemo(visitor, []);
  const bestBid = book.bids.size ? Math.max(...book.bids.keys()) : null;
  const bestAsk = book.asks.size ? Math.min(...book.asks.keys()) : null;
  useEffect(() => {
    if (!price && bestBid && bestAsk) setPrice(fmtPrice(Math.round((bestBid + bestAsk) / 2), book.scale));
  }, [bestBid, bestAsk, book.scale, price]);
  const disabled = mode !== "live" || busy;
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    const o = { symbol: book.symbol, participant_id: who, client_order_id: ++coid.current, side, order_type: type,
      price: type === "market" ? 0 : Math.round(parseFloat(price) * book.scale), qty: parseInt(qty, 10) };
    try {
      const { status, body } = await postOrder(o);
      const kind = status === 200 ? "ok" : "bad";
      setLines((l) => [{ id: o.client_order_id, text: `${side.toUpperCase()} ${qty} ${type}${type === "market" ? "" : " @ " + price} → ${describe(body, book.scale)}`, kind } as Line, ...l].slice(0, 6));
    } catch {
      setLines((l) => [{ id: Date.now(), text: "network error", kind: "bad" } as Line, ...l].slice(0, 6));
    }
    setBusy(false);
  };
  return (
    <section className="panel" aria-label="Order entry">
      <h2>Place an order <small>you are {who}</small></h2>
      <form onSubmit={submit} className="oform">
        <div className="seg" role="group" aria-label="Side">
          <button type="button" className={side === "buy" ? "on buy" : ""} onClick={() => setSide("buy")}>Buy</button>
          <button type="button" className={side === "sell" ? "on sell" : ""} onClick={() => setSide("sell")}>Sell</button>
        </div>
        <label>Type
          <select value={type} onChange={(e) => setType(e.target.value as typeof type)}>
            <option value="limit">Limit</option><option value="market">Market</option><option value="ioc">IOC</option><option value="fok">FOK</option>
          </select>
        </label>
        <label>Price
          <input inputMode="decimal" value={type === "market" ? "" : price} disabled={type === "market"} placeholder={type === "market" ? "market" : ""} onChange={(e) => setPrice(e.target.value)} />
        </label>
        <label>Quantity (lots)
          <input inputMode="numeric" value={qty} onChange={(e) => setQty(e.target.value)} />
        </label>
        <button className={`submit ${side}`} disabled={disabled}>{mode !== "live" ? "Engine not live" : busy ? "Sending…" : `${side === "buy" ? "Buy" : "Sell"} ${book.symbol}`}</button>
      </form>
      <ul className="log" aria-live="polite">
        {lines.map((l) => <li key={l.id} className={l.kind}>{l.text}</li>)}
        {!lines.length && <li className="muted">Orders go through the same path as the bots: validate, sequence, write to the log, fsync, match, acknowledge.</li>}
      </ul>
    </section>
  );
}

export function StatsPanel({ mode, symbol }: { mode: Mode; symbol: string }) {
  const [s, setS] = useState<Stats | null>(null);
  const [rate, setRate] = useState<number | null>(null);
  const prev = useRef<{ c: number; t: number } | null>(null);
  useEffect(() => {
    if (mode !== "live") { prev.current = null; return; }
    let stop = false;
    const tick = async () => {
      try {
        const { status, body } = await getStats();
        if (stop || status !== 200) return;
        setS(body);
        const c = body.symbols[symbol]?.commands ?? 0, t = Date.now();
        if (prev.current && t > prev.current.t) setRate(((c - prev.current.c) * 1000) / (t - prev.current.t));
        prev.current = { c, t };
      } catch { /* offline */ }
    };
    void tick();
    const id = window.setInterval(tick, 2000);
    return () => { stop = true; clearInterval(id); };
  }, [mode, symbol]);
  const y = s?.symbols[symbol];
  const cell = (k: string, v: string, hint?: string) => <div className="stat" key={k} title={hint}><small>{k}</small><b>{v}</b></div>;
  return (
    <section className="panel" aria-label="Engine statistics">
      <h2>Engine, live <small>measured on this server, not the benchmark machine</small></h2>
      {y ? (
        <div className="stats">
          {cell("commands/s", rate === null ? "…" : rate.toFixed(1))}
          {cell("total commands", fmtInt(y.commands))}
          {cell("ack latency p50", fmtUS(y.latency_us.total_p50), "submit to durable acknowledgement, includes the fsync")}
          {cell("ack latency p99", fmtUS(y.latency_us.total_p99))}
          {cell("fsync p50", fmtUS(y.latency_us.sync_p50), "one group-commit fsync")}
          {cell("fsync p99", fmtUS(y.latency_us.sync_p99))}
          {cell("avg batch", y.avg_batch.toFixed(1), "commands covered by one fsync")}
          {cell("queue", `${y.queue}/${fmtInt(y.queue_cap)}`)}
          {cell("WS clients", String(y.clients))}
          {cell("dropped frames", String(y.dropped_frames), "slow consumers resynced")}
          {cell("heap", `${s!.go.heap_mb} MB`)}
          {cell("GC pause total", `${s!.go.gc_pause_total_ms} ms`)}
        </div>
      ) : <div className="empty">{mode === "live" ? "loading…" : "available when the engine is live"}</div>}
    </section>
  );
}

type Phase = "idle" | "killing" | "down" | "done" | "error";

export function CrashPanel({ book, mode }: { book: BookState; mode: Mode }) {
  const [phase, setPhase] = useState<Phase>("idle");
  const [msg, setMsg] = useState("");
  const [result, setResult] = useState<{ before: number; after: number; replayed: number; snap: number; tookMs: number; hash: string; down: number; restored?: number } | null>(null);
  const seqRef = useRef(0);
  seqRef.current = book.lastSeq;
  const go = async () => {
    const before = seqRef.current;
    setPhase("killing"); setMsg(""); setResult(null);
    let r;
    try { r = await crash(); } catch { setPhase("error"); setMsg("could not reach the server"); return; }
    if (r.status === 404) { setPhase("error"); setMsg("The crash button is disabled on this server (started without --demo)."); return; }
    if (r.status === 429) { setPhase("error"); setMsg(`On cooldown, try again in ${r.body.retry_after_s ?? "a few"} s.`); return; }
    if (r.status !== 202) { setPhase("error"); setMsg("unexpected response " + r.status); return; }
    setPhase("down");
    const t0 = Date.now();
    let wasDown = false;
    for (let i = 0; i < 180; i++) {
      await new Promise((res) => setTimeout(res, 700));
      try {
        const h = await getHealth();
        if (h.status !== 200) { wasDown = true; continue; }
        if (!wasDown && Date.now() - t0 < 1500) continue; // not killed yet
        const rec = h.body.recovery?.[book.symbol];
        if (!rec) continue;
        setResult({ before, after: rec.last_seq, replayed: rec.replayed, snap: rec.snapshot_seq, tookMs: rec.took_ms, hash: rec.hash,
          down: Math.round((Date.now() - t0) / 1000), restored: h.body.archive?.restored_files });
        setPhase("done");
        return;
      } catch { wasDown = true; }
    }
    setPhase("error"); setMsg("The server did not come back within 2 minutes. Check the host.");
  };
  const ok = result && result.after >= result.before;
  return (
    <section className="panel crash" aria-label="Crash recovery demo">
      <h2>Kill the engine</h2>
      <p className="muted">Exits the server process abruptly (status 137, no graceful shutdown). Everything you have seen on screen was already fsynced to the log before it was shown. The restarted process rebuilds the book by replaying that log.</p>
      <button className="danger" onClick={go} disabled={mode !== "live" || phase === "killing" || phase === "down"}>
        {phase === "down" ? "Engine is down, waiting for recovery…" : phase === "killing" ? "Sending…" : "Kill the engine now"}
      </button>
      {phase === "error" && <p className="bad">{msg}</p>}
      {phase === "done" && result && (
        <div className={`verdict ${ok ? "ok" : "bad"}`} role="status">
          <b>{ok ? "✓ Nothing you saw was lost" : "✗ Recovered state is behind what you saw"}</b>
          <ul>
            <li>Last sequence you saw before the kill: <code>{fmtInt(result.before)}</code></li>
            <li>Sequence recovered from the log: <code>{fmtInt(result.after)}</code></li>
            <li>Rebuilt from snapshot {fmtInt(result.snap)} + {fmtInt(result.replayed)} replayed commands in {result.tookMs} ms</li>
            <li>State hash after recovery: <code>{result.hash}</code></li>
            <li>Down for about {result.down} s (a new container must start and fetch the log){result.restored ? `, ${result.restored} files restored from object storage` : ""}</li>
          </ul>
        </div>
      )}
    </section>
  );
}

export function ProofPanel() {
  const [p, setP] = useState<Proof | null>(null);
  useEffect(() => { void getProof().then(setP); }, []);
  if (!p) return null;
  return (
    <section className="panel proof" aria-label="Verified results">
      <h2>What is verified <small>measured on {p.machine}; reproduce with the commands in the README</small></h2>
      <div className="cards">
        {p.crash && (
          <article><b>{fmtInt(p.crash.runs)} SIGKILL runs, {p.crash.failed} failures</b>
            <span>{fmtInt(p.crash.acked)} acknowledged commands found in the recovered log, {fmtInt(p.crash.fills)} fills cross-checked, {fmtInt(p.crash.retried)} in-flight retries never double-booked.</span><code>make crash-test</code></article>
        )}
        {p.property && (
          <article><b>{fmtInt(p.property.commands)} random commands vs a reference model</b>
            <span>Identical events and invariants after every command; {p.property.mutations_caught} deliberately injected bugs were all caught.</span><code>make prop</code></article>
        )}
        {p.bench && (
          <article><b>{fmtInt(p.bench.engine_saturation)} commands/s, engine only</b>
            <span>{p.bench.wal_sync_p99_ms !== null && p.bench.wal_sync_rate !== null ? `With fsync before every acknowledgement: p99 ${p.bench.wal_sync_p99_ms.toFixed(1)} ms at ${fmtInt(p.bench.wal_sync_rate)}/s (open-loop). ` : ""}{p.bench.recovery_1m_ms !== null ? `Recovery of a 1,000,000-command log: ${(p.bench.recovery_1m_ms / 1000).toFixed(1)} s.` : ""}</span><code>make bench</code></article>
        )}
      </div>
    </section>
  );
}

export function Footer() {
  return (
    <footer className="foot">
      <p>Pitbook is a <b>simulator</b>: no real money, one venue, one process per book. “Crash-safe” means a process crash loses no acknowledged command; it does not mean power-loss or disk-loss tolerance. The free-tier host here is for looking at, not for measuring.</p>
    </footer>
  );
}
