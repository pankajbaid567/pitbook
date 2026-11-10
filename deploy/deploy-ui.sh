#!/usr/bin/env bash
# Build the UI against an API origin and deploy it as the "pitbook" Worker (static assets).
#   ./deploy/deploy-ui.sh https://pitbook-demo.onrender.com
set -euo pipefail
API="${1:?usage: deploy-ui.sh <api origin, e.g. https://pitbook-demo.onrender.com>}"
API="${API%/}"
HOST="${API#https://}"
cd "$(dirname "$0")/.."
python3 scripts/make_proof.py >/dev/null
( cd web && VITE_API_URL="$API" npm run build )
cat > web/dist/_headers <<H
/*
  Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self' ${API} wss://${HOST}; base-uri 'none'; frame-ancestors 'none'; form-action 'none'
  X-Content-Type-Options: nosniff
  Referrer-Policy: no-referrer
  Permissions-Policy: camera=(), microphone=(), geolocation=()
/feed.json
  Cache-Control: no-cache
/proof.json
  Cache-Control: no-cache
H
cd deploy/worker-ui && npx wrangler deploy
