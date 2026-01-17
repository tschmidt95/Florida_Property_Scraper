#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"
export PA_DB="$LEADS_SQLITE_PATH"

PORT="8011"
BASE_URL="http://127.0.0.1:${PORT}"

UVICORN_LOG="$TMP_DIR/uvicorn_${PORT}.log"

cleanup() {
  if [ -n "${UVICORN_PID:-}" ] && kill -0 "$UVICORN_PID" 2>/dev/null; then
    kill -TERM "$UVICORN_PID" 2>/dev/null || true
    sleep 0.2
    kill -KILL "$UVICORN_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "Starting API on ${BASE_URL} using temp DB: ${LEADS_SQLITE_PATH}"
nohup python -m uvicorn florida_property_scraper.api.app:app --host 127.0.0.1 --port "$PORT" >"$UVICORN_LOG" 2>&1 &
UVICORN_PID=$!

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
curl -sS "${BASE_URL}/api/debug/ping" | python -m json.tool

echo

echo "$ curl -sS -X POST ${BASE_URL}/api/saved-searches (create saved search)"
CREATE_JSON="$(curl -sS -X POST "${BASE_URL}/api/saved-searches" \
  -H 'Content-Type: application/json' \
  -d '{"name":"E2E Saved Search","county":"orange","geometry":{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[0,0]]]},"filters":{},"enrich":false}')"

echo "$CREATE_JSON" | python -m json.tool

SAVED_SEARCH_ID="$(python -c "import json,sys; obj=json.load(sys.stdin); ss=obj.get('saved_search') or {}; print(ss.get('id') or '')" <<<"$CREATE_JSON")"

if [ -z "$SAVED_SEARCH_ID" ]; then
  echo "ERROR: missing saved_search.id" >&2
  exit 2
fi

echo

echo "$ curl -sS -X POST ${BASE_URL}/api/saved-searches/${SAVED_SEARCH_ID}/members (add member)"
ADD_MEMBER_JSON="$(curl -sS -X POST "${BASE_URL}/api/saved-searches/${SAVED_SEARCH_ID}/members" \
  -H 'Content-Type: application/json' \
  -d '{"county":"orange","parcel_id":"P-E2E-1","source":"manual"}')"
echo "$ADD_MEMBER_JSON" | python -m json.tool

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
ALERTS_JSON="$(curl -sS "${BASE_URL}/api/alerts?saved_search_id=${SAVED_SEARCH_ID}&limit=10")"
echo "$ALERTS_JSON" | python -m json.tool

ALERT_ID="$(python -c "import json,sys; obj=json.load(sys.stdin); alerts=obj.get('alerts') or []; assert alerts; print(alerts[0].get('id') or '')" <<<"$ALERTS_JSON")"

if [ -z "$ALERT_ID" ]; then
  echo "ERROR: missing alert id" >&2
  exit 2
fi

echo

echo "$ curl -sS -X POST ${BASE_URL}/api/alerts/${ALERT_ID}/read (mark read)"
MARK_JSON="$(curl -sS -X POST "${BASE_URL}/api/alerts/${ALERT_ID}/read")"
echo "$MARK_JSON" | python -m json.tool

echo

echo "$ curl -sS ${BASE_URL}/api/alerts?saved_search_id=${SAVED_SEARCH_ID}&status=read (list read)"
READ_JSON="$(curl -sS "${BASE_URL}/api/alerts?saved_search_id=${SAVED_SEARCH_ID}&status=read&limit=10")"
echo "$READ_JSON" | python -m json.tool

python -c "import json,sys; obj=json.load(sys.stdin); assert obj.get('ok') is True; alerts=obj.get('alerts') or []; assert len(alerts)==1; assert alerts[0].get('status')=='read'; print({'ok': True, 'saved_searches_alerts_e2e': 'PASS'})" <<<"$READ_JSON"

echo

echo "PASS"
