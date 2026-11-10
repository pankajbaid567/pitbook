import { API } from "./feed";
import { EventJSON } from "./state";

export interface Health {
  status: string;
  version?: string;
  last_seq?: Record<string, number>;
  state_hash?: Record<string, string>;
  uptime_s?: number;
  recovery?: Record<string, { snapshot_seq: number; replayed: number; last_seq: number; hash: string; took_ms: number }>;
  archive?: { uploads: number; deletes: number; last_ok_age_s: number; error?: string; restored_files?: number };
}

export interface LatencyUS {
  total_p50: number; total_p99: number; total_max: number; sync_p50: number; sync_p99: number; queue_p50: number; queue_p99: number;
}

export interface SymStats {
  commands: number; batches: number; duplicates: number; busy_rejects: number; last_seq: number; queue: number; queue_cap: number;
  clients: number; dropped_frames: number; latency_us: LatencyUS; avg_batch: number;
}

export interface Stats {
  version: string; demo: boolean; uptime_s: number; symbols: Record<string, SymStats>;
  go: { goroutines: number; heap_mb: number; gc_cycles: number; gc_pause_total_ms: number; gomaxprocs: number };
  ws_connections: number;
}

const j = async <T,>(path: string, init?: RequestInit): Promise<{ status: number; body: T }> => {
  const r = await fetch(API + path, init);
  let body = {} as T;
  try { body = (await r.json()) as T; } catch { /* empty */ }
  return { status: r.status, body };
};

export const getHealth = () => j<Health>("/healthz");
export const getStats = () => j<Stats>("/v1/stats");

export interface OrderReply { type: string; code?: string; message?: string; events?: EventJSON[] }

export interface OrderInput {
  symbol: string; participant_id: string; client_order_id: number; side: "buy" | "sell"; order_type: "limit" | "market" | "ioc" | "fok";
  price: number; qty: number;
}

export const postOrder = (o: OrderInput) =>
  j<OrderReply>("/v1/orders", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ type: "NEW", ...o }) });

export const crash = () => j<{ status?: string; error?: string; retry_after_s?: number }>("/demo/crash", { method: "POST" });

export interface Proof {
  generated: string;
  machine: string;
  crash: { runs: number; failed: number; acked: number; fills: number; retried: number; in_flight: number; date: string } | null;
  property: { commands: number; mutations_caught: number } | null;
  bench: { engine_saturation: number; wal_sync_saturation: number; engine_p99_us: number | null; engine_rate: number | null; wal_sync_p99_ms: number | null; wal_sync_rate: number | null; recovery_1m_ms: number | null } | null;
}

export const getProof = async (): Promise<Proof | null> => {
  try { return (await (await fetch("/proof.json")).json()) as Proof; } catch { return null; }
};
