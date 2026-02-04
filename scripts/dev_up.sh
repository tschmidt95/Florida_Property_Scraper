#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

bash scripts/stop_all.sh || true

bash scripts/start_backend_minimal.sh
bash scripts/start_ui_minimal.sh

sleep 2

echo "--- backend log (tail 40) ---"
tail -n 40 .logs/backend_8000.log || true

echo "--- ui log (tail 40) ---"
tail -n 40 .logs/ui_5173.log || true

echo "UI: http://127.0.0.1:5173/"
echo "API: http://127.0.0.1:8000/"
