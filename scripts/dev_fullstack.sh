#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR/web"
if [[ -f package-lock.json ]]; then
  npm ci
else
  npm install
fi
npm run build

cd "$ROOT_DIR"

export LEADS_SQLITE_PATH="${LEADS_SQLITE_PATH:-/workspaces/Florida_Property_Scraper/leads.sqlite}"

export HOST="0.0.0.0"
export PORT="8000"

exec uvicorn florida_property_scraper.api.app:app \
  --host "$HOST" \
  --port "$PORT" \
  --reload \
  --reload-dir "$ROOT_DIR/src" \
  --reload-exclude "$ROOT_DIR/web/node_modules"
