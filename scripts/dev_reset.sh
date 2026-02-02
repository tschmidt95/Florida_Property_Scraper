#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_LOG="/tmp/api.log"
UI_LOG="/tmp/ui.log"

export PYTHONUNBUFFERED=1
export PYTHONPATH="$ROOT_DIR/src"
export PARCELS_DB_PATH="$ROOT_DIR/data/parcels/parcels.sqlite"
export PA_DB="$ROOT_DIR/leads.sqlite"
export LEADS_SQLITE_PATH="$ROOT_DIR/leads.sqlite"

cd "$ROOT_DIR"

python scripts/kill_port.py 8000 || true
python scripts/kill_port.py 5173 || true

rm -f "$API_LOG" "$UI_LOG"

nohup python -m uvicorn florida_property_scraper.api.app:app \
  --host 0.0.0.0 --port 8000 --workers 1 --log-level info \
  > "$API_LOG" 2>&1 &

(cd web && nohup npm run dev -- --host 0.0.0.0 --port 5173 > "$UI_LOG" 2>&1 &)

printf 'Waiting for API...'
for i in {1..40}; do
  if curl -sS http://127.0.0.1:8000/api/debug/ping >/dev/null; then
    echo " OK"
    break
  fi
  printf '.'
  sleep 0.25
  if [[ "$i" -eq 40 ]]; then
    echo
    echo "FAIL: API did not respond. Tail: $API_LOG"
    tail -n 60 "$API_LOG" || true
    exit 2
  fi
done

printf 'Waiting for UI proxy ping...'
for i in {1..40}; do
  if curl -sS http://127.0.0.1:5173/api/debug/ping >/dev/null; then
    echo " OK"
    break
  fi
  printf '.'
  sleep 0.25
  if [[ "$i" -eq 40 ]]; then
    echo
    echo "FAIL: UI proxy did not respond. Tail: $UI_LOG"
    tail -n 60 "$UI_LOG" || true
    exit 2
  fi
done

API_SHA=$(curl -sS http://127.0.0.1:8000/api/debug/ping 2>/dev/null | python - <<'PY'
import json, sys
try:
  data = json.load(sys.stdin)
  print(data.get('git', {}).get('sha', ''))
except Exception:
  print('')
PY
) || true

echo "API OK (git sha: ${API_SHA:-unknown})"
echo "API log: $API_LOG"
echo "UI log:  $UI_LOG"

echo "PASS dev_reset"
