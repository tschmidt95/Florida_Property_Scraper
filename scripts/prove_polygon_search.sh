#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

export BASE_URL

python -u - <<'PY'
import json
import os
import time
import urllib.request

BASE_URL = os.environ.get('BASE_URL', 'http://127.0.0.1:8000').rstrip('/')

# A simple polygon around downtown Orlando.
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

# Intentionally swapped points (lat,lng) to demonstrate the common failure mode.
POLY_LATLNG = {
  'type': 'Polygon',
  'coordinates': [[
    [28.520, -81.395],
    [28.520, -81.340],
    [28.575, -81.340],
    [28.575, -81.395],
    [28.520, -81.395],
  ]],
}


def post_search(poly: dict) -> dict:
    payload = {
        'county': 'orange',
        'live': True,
        'limit': 25,
        'include_geometry': False,
        'polygon_geojson': poly,
    }
    req = urllib.request.Request(
        f'{BASE_URL}/api/parcels/search',
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.load(r)


def post_search_filters(poly: dict, filters: dict) -> dict:
  payload = {
    'county': 'orange',
    'live': True,
    'limit': 25,
    'include_geometry': False,
    'polygon_geojson': poly,
    'filters': filters,
  }
  req = urllib.request.Request(
    f'{BASE_URL}/api/parcels/search',
    data=json.dumps(payload).encode('utf-8'),
    headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
  )
  with urllib.request.urlopen(req, timeout=120) as r:
    return json.load(r)


def wait_ping() -> None:
    for _ in range(40):
        try:
            with urllib.request.urlopen(f'{BASE_URL}/api/debug/ping', timeout=5) as r:
                _ = r.read()
            return
        except Exception:
            time.sleep(0.25)
    raise SystemExit(f'backend not responding at {BASE_URL}')


wait_ping()

print('== POST /api/parcels/search (lng,lat) ==')
resp_ok = post_search(POLY_LNGLAT)
recs_ok = resp_ok.get('records') or []
print('records_count:', len(recs_ok))
print('warnings:', resp_ok.get('warnings'))
if len(recs_ok) <= 0:
  raise SystemExit('FAIL: expected records_count > 0 for valid lng/lat polygon')

zoning_opts = resp_ok.get('zoning_options') or []
flu_opts = resp_ok.get('future_land_use_options') or []
print('zoning_options_len:', len(zoning_opts) if isinstance(zoning_opts, list) else 'not_list')
print('future_land_use_options_len:', len(flu_opts) if isinstance(flu_opts, list) else 'not_list')

# Deterministic filter proof: pick a record with lot_size_sqft and filter around it.
sample = None
for r in recs_ok:
  if not isinstance(r, dict):
    continue
  v = r.get('lot_size_sqft')
  try:
    n = float(v) if v is not None else 0.0
  except Exception:
    n = 0.0
  if n > 0:
    sample = (r, n)
    break

if not sample:
  raise SystemExit('FAIL: expected at least one record with lot_size_sqft for filter proof')

r0, lot_sqft = sample
min_sqft = max(1.0, lot_sqft * 0.5)
max_sqft = lot_sqft * 1.5

print('\n== POST /api/parcels/search (filters: lot size sqft) ==')
resp_f = post_search_filters(POLY_LNGLAT, {
  'min_lot_size_sqft': min_sqft,
  'max_lot_size_sqft': max_sqft,
  'lot_size_unit': None,
  'min_lot_size': None,
  'max_lot_size': None,
})
recs_f = resp_f.get('records') or []
print('min_lot_size_sqft:', round(min_sqft, 2))
print('max_lot_size_sqft:', round(max_sqft, 2))
print('records_count:', len(recs_f))
print('warnings:', resp_f.get('warnings'))
if len(recs_f) <= 0:
  raise SystemExit('FAIL: expected filtered records_count > 0 for lot size filter')

print('\n== POST /api/parcels/search (lat,lng swapped; expected 0) ==')
resp_bad = post_search(POLY_LATLNG)
recs_bad = resp_bad.get('records') or []
print('records_count:', len(recs_bad))
print('warnings:', resp_bad.get('warnings'))

# Not strictly required, but this is a helpful guardrail against accidental
# coordinate order regressions.
if len(recs_bad) != 0:
  raise SystemExit('FAIL: expected records_count == 0 for swapped lat/lng polygon')

# Print one example record (safe, already public fields) for sanity.
if recs_ok:
    r0 = recs_ok[0]
    if isinstance(r0, dict):
        print('\n== sample record ==')
        print('parcel_id:', r0.get('parcel_id'))
        print('owner_name:', (r0.get('owner_name') or '').strip())
        print('situs_address:', (r0.get('situs_address') or r0.get('address') or '').strip())
PY

echo
echo "== UI smoke (Playwright): polygon + filters payload =="
node web/scripts/smoke_polygon_run.mjs
