#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

if ! curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
  echo "Backend not up. Starting..."
  bash scripts/start_backend_minimal.sh
fi

if ! curl -fsS http://127.0.0.1:5173/ >/dev/null 2>&1; then
  echo "UI not up. Starting..."
  bash scripts/start_ui_minimal.sh
fi

echo "--- proxy /api/health via Vite ---"
curl -sS http://127.0.0.1:5173/api/health | python -m json.tool
