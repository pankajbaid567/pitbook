# Deployment (free tier, $0/month)

## Topology

```
 Browser ──► Cloudflare Worker "pitbook"           static UI (web/dist) served as Worker assets, always up,
    │                                                includes the recorded session for cold starts
    │ wss:// and https://  (VITE_API_URL)
    ▼
 Render free web service "pitbook-demo"             Docker, Go binary, --demo (bots + crash button)
    │  boot: restore log from the bucket      ▲ every 10 s: upload new/changed WAL + snapshot files
    ▼                                          │
 Cloudflare Worker "pitbook-store"  ──►  R2 bucket "pitbook-demo"      bearer-token protected, accepts only engine key shapes
```

## Free-tier facts that shape the design
(Re-check the providers' pricing pages when deploying; limits change.)
- **Render free web service:** 512 MB RAM, a fraction of a CPU, spins down after about 15 minutes without inbound requests, takes roughly a minute to start, **no persistent disk**. Hence: the UI lives on Cloudflare and shows a recorded session while the engine wakes; state is mirrored to R2 and restored on boot; nothing is measured here.
- **Cloudflare Workers:** free plan includes static assets and 100,000 requests/day. The mirror sends about one PUT per 10 s per symbol while the engine is awake, plus a LIST only when the file set changes.
- **R2:** free allowance for storage and operations; the demo stores well under 10 MB (small segments, snapshots every 5,000 commands, old segments trimmed and deleted remotely).

## What is deployed (2026-11-10)
| Piece | Where | Status |
|---|---|---|
| Code | `https://github.com/pankajbaid567/pitbook` (public, branch `main`) | pushed |
| R2 bucket `pitbook-demo` | Cloudflare account | created |
| Store Worker `pitbook-store` | `https://pitbook-store.baidpankaj567.workers.dev` | deployed; secret `STORE_TOKEN` set; tested live: 401 without or with a wrong token, 400 for keys that are not engine files, put / get / list / delete correct |
| Engine API, Render service `pitbook-demo` | `https://pitbook-demo.onrender.com` (free plan, Singapore, Docker, auto-deploys on push to `main`) | **live**, `/healthz` ok, bots running. Checked from the outside: CORS only for the UI origin, WebSocket 403 for a foreign origin and 101 for the UI origin, order entry works, and the UI client logic rebuilt the live book exactly (181 frames, 0 gaps) |
| UI Worker `pitbook` | `https://pitbook.baidpankaj567.workers.dev` | **live**, CSP restricted to the API origin |
| Object-store mirror on Render | env `ARCHIVE_URL` + `ARCHIVE_TOKEN` | **not yet enabled** (see below) |

End-to-end recovery over the real network was tested locally against the real Worker and R2: crash button, local disk wiped, restart restored 2 files, replayed 136 commands, recovered sequence 736 after a client had seen 704.

## Enabling the mirror on Render (one step for the owner)
Until `ARCHIVE_URL` and `ARCHIVE_TOKEN` are set on the Render service, the hosted engine runs normally but a restart (including the crash button) starts from an empty book, because Render's free disk is ephemeral. The token is a secret, so it is set by the owner in the Render dashboard (Environment) and not through tooling:
```bash
grep '^ARCHIVE_TOKEN=' ~/.pitbook-prod.env | cut -d= -f2 | tr -d '\n' | pbcopy   # copies it without printing
```
Add `ARCHIVE_TOKEN` (paste) and `ARCHIVE_URL=https://pitbook-store.baidpankaj567.workers.dev` as two environment variables and save (one redeploy). Both must be set together: a server with `ARCHIVE_URL` and no token refuses to start.

## Cold-start check (after enabling the mirror)
Wait about 20 minutes so the free instance sleeps, open the UI: the recorded session plays immediately with the "waking" banner, then the live feed takes over. Press **Kill the engine** and confirm the verdict reads "Nothing you saw was lost".

## Secrets
- `ARCHIVE_TOKEN` / `STORE_TOKEN`: one shared random secret (32 bytes hex), stored in `~/.pitbook-prod.env` (mode 600) and as a Worker secret and a Render environment variable. Rotate by generating a new value, `wrangler secret put STORE_TOKEN`, and updating Render. The bucket holds only synthetic demo data.
- Nothing secret is in the repository. Run a secret scan before the first push (`grep -rniE "(secret|token|password|api[_-]?key)\s*[:=]\s*['\"]?[A-Za-z0-9/+_-]{16,}"` over the tree finds none at the time of writing).

## Security notes
- CORS and WebSocket origin restricted to `ALLOWED_ORIGIN`.
- `/demo/crash` exists only with `--demo` and has a 60-second cooldown **per process** (a restarted process starts a fresh cooldown, so a determined visitor can crash it about once a minute; the demo recovers each time).
- Per-address rate limits on order entry (REST) and per-connection limits (WebSocket); the demo has no accounts and only synthetic data.
- The store Worker accepts only the key shapes the engine writes (`<SYMBOL>/wal/wal-<20 digits>.log`, `<SYMBOL>/snap/snap-<20 digits>.snap`) and compares the bearer token by digest in constant time.
