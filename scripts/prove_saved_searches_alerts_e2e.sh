#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/_curl_json_helper.sh"

PING_JSON="$TMP_DIR/ping.json"
CREATE_JSON="$TMP_DIR/create_saved_search.json"
ADD_MEMBER_JSON="$TMP_DIR/add_member.json"
ALERTS_JSON="$TMP_DIR/alerts.json"
MARK_JSON="$TMP_DIR/mark_read.json"
READ_JSON="$TMP_DIR/read_alerts.json"

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"
export PA_DB="$LEADS_SQLITE_PATH"

PORT="8011"
BASE_URL="http://127.0.0.1:${PORT}"

UVICORN_LOG="$TMP_DIR/uvicorn_${PORT}.log"
STARTED_BACKEND=0

port_listener_pid() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
    return 0
  fi
  if command -v ss >/dev/null 2>&1; then
    ss -ltnp 2>/dev/null | awk -v p=":${port}" '$0 ~ p { if (match($0, /pid=([0-9]+)/, m)) { print m[1]; exit } }'
    return 0
  fi
  return 1
}

cleanup() {
  if [[ -d "$TMP_DIR" ]]; then
    rm -rf "$TMP_DIR" || true
  fi
  if [[ "${STARTED_BACKEND:-0}" == "1" ]]; then
    listener_pid="$(port_listener_pid "$PORT")"
    if [[ -n "${UVICORN_PID:-}" && -n "$listener_pid" && "$listener_pid" == "$UVICORN_PID" ]]; then
      kill -TERM "$UVICORN_PID" 2>/dev/null || true
      sleep 0.2
      kill -KILL "$UVICORN_PID" 2>/dev/null || true
    fi
  fi
}
trap cleanup EXIT

echo "Starting API on ${BASE_URL} using temp DB: ${LEADS_SQLITE_PATH}"
listener_pid="$(port_listener_pid "$PORT")"
if [[ -n "$listener_pid" ]]; then
  echo "FAIL port ${PORT} already in use (pid=$listener_pid)"
  exit 2
fi
nohup python -m uvicorn florida_property_scraper.api.app:app --host 127.0.0.1 --port "$PORT" >"$UVICORN_LOG" 2>&1 &
UVICORN_PID=$!
STARTED_BACKEND=1

# Wait for health
python - <<PY
import time, urllib.request
url = "${BASE_URL}/health"
for i in range(80):
    try:
        with urllib.request.urlopen(url, timeout=1) as r:
            if r.status == 200:
                print("health: OK")
                raise SystemExit(0)
    except Exception:
        time.sleep(0.2)
raise SystemExit("health: FAILED")
PY

echo

echo "$ curl -sS ${BASE_URL}/api/debug/ping"
curl_json_or_fail "${BASE_URL}/api/debug/ping" "$PING_JSON"
python -m json.tool "$PING_JSON"

echo

echo "$ curl -sS -X POST ${BASE_URL}/api/saved-searches (create saved search)"
curl_json_or_fail "${BASE_URL}/api/saved-searches" "$CREATE_JSON" \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"name":"E2E Saved Search","county":"orange","geometry":{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[0,0]]]},"filters":{},"enrich":false}'

python -m json.tool "$CREATE_JSON"

SAVED_SEARCH_ID="$(python - <<'PY' "$CREATE_JSON"
import json
import sys

obj = json.load(open(sys.argv[1], "r", encoding="utf-8"))
ss = obj.get("saved_search") or {}
print(ss.get("id") or "")
PY
)"

if [ -z "$SAVED_SEARCH_ID" ]; then
  echo "ERROR: missing saved_search.id" >&2
  exit 2
fi

echo

echo "$ curl -sS -X POST ${BASE_URL}/api/saved-searches/${SAVED_SEARCH_ID}/members (add member)"
curl_json_or_fail "${BASE_URL}/api/saved-searches/${SAVED_SEARCH_ID}/members" "$ADD_MEMBER_JSON" \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"county":"orange","parcel_id":"P-E2E-1","source":"manual"}'
python -m json.tool "$ADD_MEMBER_JSON"

echo

echo "$ python - <<'PY' (seed trigger_alerts for parcel)"
python - <<'PY'
import os

from florida_property_scraper.storage import SQLiteStore

db = os.environ.get("LEADS_SQLITE_PATH")
assert db

store = SQLiteStore(db)
try:
    store.upsert_trigger_alert(
        county="orange",
        parcel_id="P-E2E-1",
        alert_key="seller_intent_critical",
        severity=5,
        first_seen_at="2026-01-01T00:00:00+00:00",
        last_seen_at="2026-01-01T00:00:00+00:00",
        status="open",
        trigger_event_ids=[101, 102],
        details={"rule": "critical>=1", "seller_score": 100, "trigger_keys": ["tax_delinquent"]},
    )
finally:
    store.close()

print({"ok": True, "seed_trigger_alert": "PASS"})
PY

echo

echo "$ python -m florida_property_scraper scheduler run --no-saved-searches --no-connectors --no-rollups (sync inbox)"
python -m florida_property_scraper scheduler run \
  --db "$LEADS_SQLITE_PATH" \
  --now "2026-01-01T00:00:01+00:00" \
  --no-saved-searches \
  --no-connectors \
  --no-rollups

echo

echo "$ curl -sS ${BASE_URL}/api/alerts?saved_search_id=${SAVED_SEARCH_ID} (list alerts)"
curl_json_or_fail "${BASE_URL}/api/alerts?saved_search_id=${SAVED_SEARCH_ID}&limit=10" "$ALERTS_JSON"
python -m json.tool "$ALERTS_JSON"

ALERT_ID="$(python - <<'PY' "$ALERTS_JSON"
import json
import sys

obj = json.load(open(sys.argv[1], "r", encoding="utf-8"))
alerts = obj.get("alerts") or []
assert alerts
print(alerts[0].get("id") or "")
PY
)"

if [ -z "$ALERT_ID" ]; then
  echo "ERROR: missing alert id" >&2
  exit 2
fi

echo

echo "$ curl -sS -X POST ${BASE_URL}/api/alerts/${ALERT_ID}/read (mark read)"
curl_json_or_fail "${BASE_URL}/api/alerts/${ALERT_ID}/read" "$MARK_JSON" -X POST
python -m json.tool "$MARK_JSON"

echo

echo "$ curl -sS ${BASE_URL}/api/alerts?saved_search_id=${SAVED_SEARCH_ID}&status=read (list read)"
curl_json_or_fail "${BASE_URL}/api/alerts?saved_search_id=${SAVED_SEARCH_ID}&status=read&limit=10" "$READ_JSON"
python -m json.tool "$READ_JSON"

python - <<'PY' "$READ_JSON"
import json
import sys

obj = json.load(open(sys.argv[1], "r", encoding="utf-8"))
assert obj.get("ok") is True
alerts = obj.get("alerts") or []
assert len(alerts) == 1
assert alerts[0].get("status") == "read"
print({"ok": True, "saved_searches_alerts_e2e": "PASS"})
PY

echo

echo "PASS"
