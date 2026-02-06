#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

# shellcheck disable=SC1091
source "/workspaces/Florida_Property_Scraper/scripts/_curl_json_helper.sh"

health_tmp="$(mktemp)"
ping_tmp="$(mktemp)"
cleanup() {
  rm -f "$health_tmp" "$ping_tmp"
}
trap cleanup EXIT

if ! curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
  echo "FAIL backend not running. Start with: scripts/start_backend_minimal.sh"
  exit 2
fi

curl_json_or_fail "http://127.0.0.1:8000/api/health" "$health_tmp"
curl_json_or_fail "http://127.0.0.1:8000/api/debug/ping" "$ping_tmp"

echo "--- /api/health ---"
python -m json.tool "$health_tmp"

echo "--- /api/debug/ping ---"
python -m json.tool "$ping_tmp"

ok=$(python - <<'PY' "$health_tmp"
import json,sys
h=json.load(open(sys.argv[1], "r", encoding="utf-8"))
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
