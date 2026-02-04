#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

bash scripts/start_backend_minimal.sh

health_json=$(curl -sS http://127.0.0.1:8000/api/health)
ping_json=$(curl -sS http://127.0.0.1:8000/api/debug/ping)

echo "--- /api/health ---"
echo "$health_json" | python -m json.tool

echo "--- /api/debug/ping ---"
echo "$ping_json" | python -m json.tool

ok=$(echo "$health_json" | python - <<'PY'
import json,sys
h=json.load(sys.stdin)
ok=bool(h.get('ok'))
sha=str(h.get('sha') or '').strip()
print('1' if ok and sha else '0')
PY
)

if [[ "$ok" != "1" ]]; then
  echo "FAIL backend health"
  exit 2
fi

echo "PASS backend up"
