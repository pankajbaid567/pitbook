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

## What is already deployed (2026-11-10)
| Piece | Where | Status |
|---|---|---|
| R2 bucket `pitbook-demo` | Cloudflare account | created |
| Store Worker `pitbook-store` | `https://pitbook-store.baidpankaj567.workers.dev` | deployed; secret `STORE_TOKEN` set; tested live: 401 without or with a wrong token, 400 for keys that are not engine files, put / get / list / delete correct |
| End-to-end recovery over the real network | local server → Worker → R2 | tested: crash button, local disk wiped, restart restored 2 files, replayed 136 commands, recovered sequence 736 after a client had seen 704 |

## What remains (needs the owner)
Render deploys from a Git repository, so the code must be on GitHub first. The deployment tooling used here has no GitHub credentials (and must not go looking for them), so these two steps are yours:
```bash
# 1. create an empty repo, e.g. github.com/pankajbaid567/pitbook (web UI), then:
cd /Users/pankajbaid/projects/flagships/Pitbook
git remote add origin https://github.com/pankajbaid567/pitbook.git
git push -u origin main
```
After the push, the remaining steps are mechanical and were prepared:
1. **Render:** new web service from the repo, runtime `docker`, `dockerfilePath ./deploy/Dockerfile`, plan `free`, health check `/healthz`, environment `ALLOWED_ORIGIN=https://pitbook.baidpankaj567.workers.dev`, `ARCHIVE_URL=https://pitbook-store.baidpankaj567.workers.dev`, `ARCHIVE_TOKEN=<value of ARCHIVE_TOKEN in ~/.pitbook-prod.env>` (never commit it; `deploy/.env.example` lists names only). `deploy/render.yaml` is the same as a blueprint.
2. **UI Worker:** `cd web && VITE_API_URL=https://<render-url> npm run build && cd ../deploy/worker-ui && npx wrangler deploy`.
3. **Cold-start check:** wait 20 minutes, open the UI: the recorded session plays immediately with the "waking" banner, then the live feed takes over when `/healthz` returns 200. Press the crash button and confirm the verdict panel shows "Nothing you saw was lost".
4. Update `docs/resume-claims.md` row 12 from pending to proven with the URL and date.

## Secrets
- `ARCHIVE_TOKEN` / `STORE_TOKEN`: one shared random secret (32 bytes hex), stored in `~/.pitbook-prod.env` (mode 600) and as a Worker secret and a Render environment variable. Rotate by generating a new value, `wrangler secret put STORE_TOKEN`, and updating Render. The bucket holds only synthetic demo data.
- Nothing secret is in the repository. Run a secret scan before the first push (`grep -rniE "(secret|token|password|api[_-]?key)\s*[:=]\s*['\"]?[A-Za-z0-9/+_-]{16,}"` over the tree finds none at the time of writing).

## Security notes
- CORS and WebSocket origin restricted to `ALLOWED_ORIGIN`.
- `/demo/crash` exists only with `--demo` and has a 60-second cooldown **per process** (a restarted process starts a fresh cooldown, so a determined visitor can crash it about once a minute; the demo recovers each time).
- Per-address rate limits on order entry (REST) and per-connection limits (WebSocket); the demo has no accounts and only synthetic data.
- The store Worker accepts only the key shapes the engine writes (`<SYMBOL>/wal/wal-<20 digits>.log`, `<SYMBOL>/snap/snap-<20 digits>.snap`) and compares the bearer token by digest in constant time.
