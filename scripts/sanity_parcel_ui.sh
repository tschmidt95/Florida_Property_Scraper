#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

# Start backend (best-effort) and ensure we clean up.
if [ -f .uvicorn_8000.pid ]; then
  pid=$(cat .uvicorn_8000.pid 2>/dev/null || true)
  if [ -n "${pid:-}" ]; then
    kill -TERM "$pid" 2>/dev/null || true
  fi
fi

source .venv/bin/activate
export LEADS_SQLITE_PATH="${LEADS_SQLITE_PATH:-/workspaces/Florida_Property_Scraper/leads.sqlite}"
export PA_DB="${PA_DB:-$LEADS_SQLITE_PATH}"
export FPS_USE_FDOR_CENTROIDS="${FPS_USE_FDOR_CENTROIDS:-1}"

: > .uvicorn_8000.log
nohup python -m uvicorn florida_property_scraper.api.app:app --host 0.0.0.0 --port 8000 > .uvicorn_8000.log 2>&1 &
echo $! > .uvicorn_8000.pid

cleanup() {
  if [ -f .uvicorn_8000.pid ]; then
    pid=$(cat .uvicorn_8000.pid 2>/dev/null || true)
    if [ -n "${pid:-}" ]; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  fi
}
trap cleanup EXIT

python -u - <<PY
import datetime
import json
import time
import urllib.request

BASE_URL = ${BASE_URL@Q}.rstrip('/')

POLY = {
  'type': 'Polygon',
  'coordinates': [[
    [-81.395, 28.520],
    [-81.340, 28.520],
    [-81.340, 28.575],
    [-81.395, 28.575],
    [-81.395, 28.520],
  ]],
}


def get(url: str, timeout: int = 20) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


def post_json(url: str, payload: dict, timeout: int = 120) -> bytes:
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


print('== wait /health ==')
health_url = f"{BASE_URL}/health"
ready = False
last_err = None
for _ in range(60):
    try:
        get(health_url, timeout=5)
        ready = True
        break
    except Exception as e:
        last_err = e
        time.sleep(1)

if not ready:
    print(f"ERROR: server not ready at {BASE_URL} (GET /health failed)")
    print(f"last_error: {last_err}")
    raise SystemExit(2)

print('time_utc:', datetime.datetime.now(datetime.timezone.utc).isoformat())

print('\n== /api/debug/ping ==')
ping = json.loads(get(f"{BASE_URL}/api/debug/ping", timeout=20))
print(json.dumps(ping, indent=2)[:2500])

print('\n== POST /api/parcels/search (polygon) ==')
search_payload = {
    'county': 'orange',
    'live': True,
    'limit': 25,
    'include_geometry': False,
    # MapSearch sends polygon_geojson
    'polygon_geojson': POLY,
}
resp = json.loads(post_json(f"{BASE_URL}/api/parcels/search", search_payload, timeout=120))
recs = resp.get('records') or []
print('records_count:', len(recs))
if not recs:
    print('warnings:', resp.get('warnings'))
    raise SystemExit(1)

# Pick parcel_ids for geometry lookup.
parcel_ids = [r.get('parcel_id') for r in recs if isinstance(r, dict) and r.get('parcel_id')]
parcel_ids = [str(pid) for pid in parcel_ids if pid][:25]
print('sample_parcel_ids:', parcel_ids[:8])

print('\n== Owners (sample) ==')
for r in recs[:5]:
    if not isinstance(r, dict):
        continue
    owner = (r.get('owner_name') or '').strip()
    addr = (r.get('situs_address') or r.get('address') or '').strip()
    pid = (r.get('parcel_id') or '').strip()
    print('-', pid, '|', owner or '—', '|', addr or '—')

print('\n== POST /api/parcels/geometry (requested parcel_ids) ==')
geom_payload = {'county': 'orange', 'parcel_ids': parcel_ids}
fc = json.loads(post_json(f"{BASE_URL}/api/parcels/geometry", geom_payload, timeout=120))
if fc.get('type') != 'FeatureCollection':
    print('unexpected_type:', fc.get('type'))
    raise SystemExit(1)
feats = fc.get('features') or []
print('feature_count:', len(feats))
if not feats:
    print('ERROR: expected parcel geometry features for orange')
    raise SystemExit(1)

# Print one feature properties (safe keys only).
props = (feats[0] or {}).get('properties') if isinstance(feats[0], dict) else None
print('feature0_properties:', json.dumps(props, indent=2)[:800])

raise SystemExit(0)
PY
