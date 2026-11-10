#!/usr/bin/env python3
"""Write web/public/proof.json (shown in the UI) from the raw results. Every value is read from bench/results."""
import csv, glob, json, os, re, statistics as st

R = "bench/results"
newest = lambda pat: (sorted(glob.glob(os.path.join(R, pat))) or [None])[-1]
proof = {"generated": "", "machine": "unknown", "crash": None, "property": None, "bench": None}

lg = [p for p in sorted(glob.glob(os.path.join(R, "loadgen-*.csv"))) if not p.endswith("-recovery.csv")]
rows = list(csv.DictReader(open(lg[-1]))) if lg else []
if rows:
    proof["machine"] = f"{rows[0]['cpu']} laptop, {rows[0]['mem_gib']} GiB, {rows[0]['os']}"
    proof["generated"] = rows[0]["ts_utc"][:10]

ct = newest("crashtest-*.json")
if ct:
    j = json.load(open(ct)); t = j["totals"]
    proof["crash"] = {"runs": j["runs"], "failed": j["failed_runs"], "acked": t["acked_commands"], "fills": t["fills_checked"],
                      "retried": t["retried"], "in_flight": t["in_flight_at_kill"], "date": j["date_utc"][:10]}

pr, mu = newest("property-*.txt"), newest("mutation-*.txt")
if pr:
    m = re.search(r"commands=(\d+)", open(pr).read())
    caught = len(re.findall(r": killed$", open(mu).read(), re.M)) if mu else 0
    if m:
        proof["property"] = {"commands": int(m.group(1)), "mutations_caught": caught}

def sat(mode):
    v = [float(r["throughput"]) for r in rows if r["mode"] == mode and float(r["target_rate"]) == 0]
    return st.median(v) if v else None

def p99(mode, rate):
    v = [float(r["p99_us"]) for r in rows if r["mode"] == mode and r["workload"] == "steady" and float(r["target_rate"]) == rate and int(r["dropped"]) == 0]
    return st.median(v) if v else None

if rows:
    rec = newest("loadgen-*-recovery.csv")
    r1m = None
    if rec:
        rr = [r for r in csv.DictReader(open(rec)) if r["variant"] == "no-snapshot"]
        r1m = float(rr[-1]["took_ms"]) if rr else None
    w = p99("wal-sync", 10000.0)
    e = p99("engine", 100000.0)
    proof["bench"] = {"engine_saturation": round(sat("engine") or 0), "wal_sync_saturation": round(sat("wal-sync") or 0),
                      "engine_p99_us": e, "engine_rate": 100000 if e is not None else None,
                      "wal_sync_p99_ms": (w / 1000 if w is not None else None), "wal_sync_rate": 10000 if w is not None else None, "recovery_1m_ms": r1m}
os.makedirs("web/public", exist_ok=True)
json.dump(proof, open("web/public/proof.json", "w"), indent=2)
print("wrote web/public/proof.json")
