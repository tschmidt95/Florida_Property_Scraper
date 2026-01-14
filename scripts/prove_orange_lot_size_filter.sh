#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

python -u - <<PY
import json
import datetime
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

search_url = f"{BASE_URL}/api/parcels/search"

def search(filters=None, timeout=120):
    payload = {
        'county': 'orange',
        'live': True,
        'limit': 200,
        'include_geometry': False,
        'geometry': POLY,
    }
    if filters is not None:
        payload['filters'] = filters
    return json.loads(post_json(search_url, payload, timeout=timeout))


print('\n== POST /api/parcels/search (baseline) ==')
resp0 = search(filters=None, timeout=120)
recs0 = resp0.get('records') or []
print('baseline_records_count:', len(recs0))

if not recs0:
    print('warnings:', resp0.get('warnings'))
    print('error_reason:', resp0.get('error_reason'))
    raise SystemExit(1)

# Find a record with a real lot_size_sqft.
sample = None
for r in recs0:
    v = r.get('lot_size_sqft')
    try:
        v = float(v) if v is not None else None
    except Exception:
        v = None
    if v is not None and v > 0:
        sample = r
        break

if sample is None:
    # Fall back to sqft list.
    for r in recs0:
        sqft = r.get('sqft') or []
        lot = None
        for s in sqft:
            if isinstance(s, dict) and s.get('type') == 'lot':
                try:
                    lot = float(s.get('value'))
                except Exception:
                    lot = None
        if lot is not None and lot > 0:
            sample = dict(r)
            sample['lot_size_sqft'] = lot
            break

if sample is None:
    print('NOTE: No lot size values available in baseline; skipping assertions.')
    raise SystemExit(0)

lot_sqft = float(sample.get('lot_size_sqft') or 0)
lot_acres = sample.get('lot_size_acres')
try:
    lot_acres = float(lot_acres) if lot_acres is not None else (lot_sqft / 43560.0)
except Exception:
    lot_acres = (lot_sqft / 43560.0)

print('sample_parcel_id:', sample.get('parcel_id'))
print('sample_lot_size_sqft:', lot_sqft)
print('sample_lot_size_acres:', lot_acres)

# Choose a threshold that should include the sample.
min_sqft = max(1.0, lot_sqft * 0.5)
max_sqft = lot_sqft * 1.5
min_acres = min_sqft / 43560.0
max_acres = max_sqft / 43560.0

print('\n== POST /api/parcels/search (lot size filter: sqft) ==')
resp_sqft = search(filters={
    'lot_size_unit': 'sqft',
    'min_lot_size': min_sqft,
    'max_lot_size': max_sqft,
}, timeout=120)
recs_sqft = resp_sqft.get('records') or []
print('min_lot_size_sqft:', round(min_sqft, 2))
print('max_lot_size_sqft:', round(max_sqft, 2))
print('filtered_count_sqft:', len(recs_sqft))

bad = []
for r in recs_sqft:
    try:
        v = float(r.get('lot_size_sqft') or 0)
    except Exception:
        v = 0.0
    if v <= 0 or v < min_sqft - 1e-6 or v > max_sqft + 1e-6:
        bad.append((r.get('parcel_id'), r.get('lot_size_sqft')))
        if len(bad) >= 5:
            break

print('mismatch_count_sqft:', len(bad))
if bad:
    print('mismatches_sqft_sample:', bad)
    raise SystemExit(1)

print('\n== POST /api/parcels/search (lot size filter: acres) ==')
resp_acres = search(filters={
    'lot_size_unit': 'acres',
    'min_lot_size': min_acres,
    'max_lot_size': max_acres,
}, timeout=120)
recs_acres = resp_acres.get('records') or []
print('min_lot_size_acres:', round(min_acres, 6))
print('max_lot_size_acres:', round(max_acres, 6))
print('filtered_count_acres:', len(recs_acres))

bad = []
for r in recs_acres:
    try:
        v = r.get('lot_size_acres')
        v = float(v) if v is not None else (float(r.get('lot_size_sqft') or 0) / 43560.0)
    except Exception:
        v = 0.0
    if v <= 0 or v < min_acres - 1e-9 or v > max_acres + 1e-9:
        bad.append((r.get('parcel_id'), r.get('lot_size_acres'), r.get('lot_size_sqft')))
        if len(bad) >= 5:
            break

print('mismatch_count_acres:', len(bad))
if bad:
    print('mismatches_acres_sample:', bad)
    raise SystemExit(1)

print('\nexample_record:')
print(json.dumps(recs_acres[0] if recs_acres else recs_sqft[0], indent=2)[:3500])

raise SystemExit(0)
PY
