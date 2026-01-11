#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  if [[ -n "${BACKEND_PID:-}" ]]; then
    kill "$BACKEND_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

uvicorn florida_property_scraper.api.app:app --reload --port 8000 &
BACKEND_PID=$!

npm --prefix web run dev
