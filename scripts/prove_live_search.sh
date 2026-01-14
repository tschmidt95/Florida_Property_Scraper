#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

python - <<PY
import json
import time
import urllib.request
import urllib.error

BASE_URL = ${BASE_URL@Q}.rstrip('/')

def get(url: str, timeout: int = 20) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()

def post_json(url: str, payload: dict, timeout: int = 60) -> bytes:
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

print('== /api/debug/ping ==')
ping_url = f"{BASE_URL}/api/debug/ping"
ping = json.loads(get(ping_url, timeout=20))
print('ok:', ping.get('ok'))
print('git:', ping.get('git'))
print('env:', ping.get('env'))

print('\n== POST /api/parcels/search (live) ==')

payload = {
  'county': 'orange',
  'live': True,
  'limit': 50,
  'include_geometry': False,
  'geometry': {
    'type': 'Polygon',
    'coordinates': [[
      [-81.395, 28.520],
      [-81.340, 28.520],
      [-81.340, 28.575],
      [-81.395, 28.575],
      [-81.395, 28.520],
    ]],
  },
}

search_url = f"{BASE_URL}/api/parcels/search"
resp = json.loads(post_json(search_url, payload, timeout=60))

recs = resp.get('records') or []
sc = ((resp.get('summary') or {}).get('source_counts') or {})
print('count:', len(recs))
print('source_counts:', sc)
if recs:
    print('first_record:')
    print(json.dumps(recs[0], indent=2)[:2000])
else:
    print('error_reason:', resp.get('error_reason'))
    print('warnings:', resp.get('warnings'))

count = len(recs)
live = int(sc.get('live') or 0)

print('\n== POST /api/parcels/search (MapSearch payload: polygon_geojson) ==')
payload2 = {
    'county': 'orange',
    'live': True,
    'limit': 25,
    'include_geometry': False,
    'polygon_geojson': {
        'type': 'Polygon',
        'coordinates': [[
            [-81.395, 28.520],
            [-81.340, 28.520],
            [-81.340, 28.575],
            [-81.395, 28.575],
            [-81.395, 28.520],
        ]],
    },
}
resp2 = json.loads(post_json(search_url, payload2, timeout=60))
recs2 = resp2.get('records') or []
print('count:', len(recs2))
if not recs2:
        print('warnings:', resp2.get('warnings'))

print('\n== POST /api/parcels/search (swapped coords demo; expected 0) ==')
payload3 = {
    **payload2,
    'polygon_geojson': {
        'type': 'Polygon',
        'coordinates': [[
            [28.520, -81.395],
            [28.520, -81.340],
            [28.575, -81.340],
            [28.575, -81.395],
            [28.520, -81.395],
        ]],
    },
}
resp3 = json.loads(post_json(search_url, payload3, timeout=60))
recs3 = resp3.get('records') or []
print('count:', len(recs3))
print('warnings:', resp3.get('warnings'))

raise SystemExit(0 if (count > 0 and live >= 1) else 1)
PY
