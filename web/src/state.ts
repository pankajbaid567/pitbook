// Client-side book state, rebuilt from SNAPSHOT + contiguous EVENTS frames
// exactly as docs/protocol.md prescribes (gap detection via prev_seq).

export type Side = "buy" | "sell";

export interface Trade {
  trade_id: number;
  seq: number;
  price: number;
  qty: number;
  taker_side: Side;
  ts_ms: number;
}

export interface EventJSON {
  type: string;
  seq: number;
  idx: number;
  side?: Side;
  price?: number;
  qty: number;
  trade_id?: number;
  taker_side?: Side;
  client_order_id?: number;
  order_id?: number;
  status?: string;
  code?: string;
  reason?: string;
  remaining_qty?: number;
  maker_order_id?: number;
  taker_order_id?: number;
}

export type Msg =
  | { type: "SNAPSHOT"; symbol: string; last_seq: number; price_scale: number; bids: [number, number][]; asks: [number, number][]; trades: Trade[] }
  | { type: "EVENTS"; symbol: string; prev_seq: number; last_seq: number; ts_ms: number; events: EventJSON[] }
  | { type: "RESYNC"; symbol: string; reason: string }
  | { type: "PONG" };

export interface BookState {
  symbol: string;
  scale: number;
  bids: Map<number, number>;
  asks: Map<number, number>;
  lastSeq: number;
  trades: Trade[];
  ready: boolean;
  gap: boolean;
  frames: number;
  fills: number;
}

export const emptyBook = (symbol: string): BookState => ({
  symbol, scale: 100, bids: new Map(), asks: new Map(), lastSeq: 0, trades: [], ready: false, gap: false, frames: 0, fills: 0,
});

export type Action = { type: "msg"; msg: Msg } | { type: "reset"; symbol: string };

export function reducer(s: BookState, a: Action): BookState {
  if (a.type === "reset") return emptyBook(a.symbol);
  const m = a.msg;
  switch (m.type) {
    case "SNAPSHOT":
      return {
        ...s, symbol: m.symbol, scale: m.price_scale || s.scale, ready: true, gap: false, lastSeq: m.last_seq,
        bids: new Map(m.bids), asks: new Map(m.asks), trades: [...m.trades].slice(-200),
      };
    case "RESYNC":
      return { ...s, ready: false };
    case "EVENTS": {
      if (!s.ready) return s;
      if (m.last_seq <= s.lastSeq) return s; // duplicate frame
      if (m.prev_seq !== s.lastSeq) return { ...s, gap: true, ready: false };
      let bids = s.bids, asks = s.asks, trades = s.trades, fills = s.fills;
      for (const e of m.events) {
        if (e.type === "BOOK_DELTA") {
          const side = e.side === "buy" ? (bids === s.bids ? (bids = new Map(s.bids)) : bids) : asks === s.asks ? (asks = new Map(s.asks)) : asks;
          if (e.qty === 0) side.delete(e.price!);
          else side.set(e.price!, e.qty);
        } else if (e.type === "FILL") {
          if (trades === s.trades) trades = [...s.trades];
          trades.push({ trade_id: e.trade_id!, seq: e.seq, price: e.price!, qty: e.qty, taker_side: e.taker_side!, ts_ms: m.ts_ms });
          if (trades.length > 200) trades.splice(0, trades.length - 200);
          fills++;
        }
      }
      return { ...s, bids, asks, trades, fills, lastSeq: m.last_seq, frames: s.frames + 1 };
    }
    default:
      return s;
  }
}

export const sortedLevels = (m: Map<number, number>, bid: boolean, depth: number): [number, number][] =>
  [...m.entries()].sort((a, b) => (bid ? b[0] - a[0] : a[0] - b[0])).slice(0, depth);

export const fmtPrice = (p: number, scale: number): string => (p / scale).toFixed(Math.max(0, Math.round(Math.log10(scale))));
