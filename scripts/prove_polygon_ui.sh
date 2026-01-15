#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
START_BACKEND="${START_BACKEND:-1}"

export BASE_URL

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

wait_ping() {
  local tries="${1:-60}"
  for _ in $(seq 1 "$tries"); do
    if curl -sS "$BASE_URL/api/debug/ping" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

start_backend_if_needed() {
  if [[ "$START_BACKEND" != "1" ]]; then
    return 0
  fi
  if wait_ping 1; then
    return 0
  fi

  echo "== starting backend (8000) =="
  # shellcheck disable=SC1091
  if [[ -f "$ROOT_DIR/.venv/bin/activate" ]]; then
    source "$ROOT_DIR/.venv/bin/activate"
  fi

  export LEADS_SQLITE_PATH="${LEADS_SQLITE_PATH:-$ROOT_DIR/leads.sqlite}"
  export PA_DB="${PA_DB:-$LEADS_SQLITE_PATH}"
  export FPS_USE_FDOR_CENTROIDS="${FPS_USE_FDOR_CENTROIDS:-1}"

  : > "$ROOT_DIR/.uvicorn_8000.log"
  nohup python -m uvicorn florida_property_scraper.api.app:app --host 0.0.0.0 --port 8000 > "$ROOT_DIR/.uvicorn_8000.log" 2>&1 &
  echo $! > "$ROOT_DIR/.uvicorn_8000.pid"

  if ! wait_ping 80; then
    echo "ERROR: backend failed to start"
    tail -n 120 "$ROOT_DIR/.uvicorn_8000.log" || true
    exit 1
  fi
}

start_backend_if_needed

echo "== /api/debug/ping =="
curl -sS "$BASE_URL/api/debug/ping" | python -m json.tool

python -u - <<'PY'
import json
import os
import sys
import urllib.request

BASE_URL = os.environ.get('BASE_URL', 'http://127.0.0.1:8000').rstrip('/')

POLY_LNGLAT = {
  'type': 'Polygon',
  'coordinates': [[
    [-81.395, 28.520],
    [-81.340, 28.520],
    [-81.340, 28.575],
    [-81.395, 28.575],
    [-81.395, 28.520],
  ]],
}

def post_json(url: str, payload: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
    )
    with urllib.request.urlopen(req, timeout=180) as r:
        return json.load(r)

print("== POST /api/parcels/search (known-good polygon) ==")
search_payload = {
    'county': 'orange',
    'live': True,
    'limit': 25,
    'include_geometry': False,
    'polygon_geojson': POLY_LNGLAT,
}
resp = post_json(f"{BASE_URL}/api/parcels/search", search_payload)

records = resp.get('records') or []
summary = resp.get('summary') or {}
count = summary.get('count')
if not isinstance(count, int):
    count = len(records) if isinstance(records, list) else 0

warnings = resp.get('warnings')
if isinstance(warnings, list) and warnings:
    print('warnings:', warnings)

print('count:', count)
if count <= 0:
    raise SystemExit('FAIL: expected count > 0')

sample = records[0] if isinstance(records, list) and records else None
parcel_ids = []
if isinstance(records, list):
    for r in records:
        if isinstance(r, dict) and isinstance(r.get('parcel_id'), str) and r['parcel_id']:
            parcel_ids.append(r['parcel_id'])

if sample and isinstance(sample, dict):
    print('sample_parcel_id:', sample.get('parcel_id'))
    print('sample_owner_name:', (sample.get('owner_name') or '').strip())
    print('sample_address:', (sample.get('situs_address') or sample.get('address') or '').strip())

print("\n== POST /api/parcels/geometry (optional) ==")
geo_payload = {
    'county': 'orange',
    'parcel_ids': parcel_ids[:10],
}
try:
    fc = post_json(f"{BASE_URL}/api/parcels/geometry", geo_payload)
    if not isinstance(fc, dict) or fc.get('type') != 'FeatureCollection':
        raise ValueError('unexpected geometry response')
    feats = fc.get('features')
    if not isinstance(feats, list):
        raise ValueError('unexpected geometry response')
    print('feature_count:', len(feats))
    if len(feats) <= 0:
        raise SystemExit('FAIL: expected feature_count > 0')
except Exception as e:
    print('geometry_check:', f"SKIP ({e})")
PY
