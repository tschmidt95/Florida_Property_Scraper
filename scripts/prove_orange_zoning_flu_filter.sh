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


def norm_choice(v: object) -> str:
    s = str(v or '').strip()
    if not s:
        return 'UNKNOWN'
    return ' '.join(s.upper().split())


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

z_opts = resp0.get('zoning_options') or []
flu_opts = resp0.get('future_land_use_options') or []
print('zoning_options_count:', len(z_opts))
print('future_land_use_options_count:', len(flu_opts))
print('zoning_options_sample:', z_opts[:12])
print('future_land_use_options_sample:', flu_opts[:12])

if not recs0:
    print('warnings:', resp0.get('warnings'))
    print('error_reason:', resp0.get('error_reason'))
    raise SystemExit(1)

# Ensure we have PA data for at least a few parcels to validate filtering.
# (This keeps the proof stable even when PA DB is cold.)
parcel_ids = [r.get('parcel_id') for r in recs0 if r.get('parcel_id')]
parcel_ids = [str(x) for x in parcel_ids if str(x).strip()]
parcel_ids = parcel_ids[:10]

print('\n== POST /api/parcels/enrich (seed) ==')
enrich_url = f"{BASE_URL}/api/parcels/enrich"
enrich_resp = json.loads(post_json(enrich_url, {
  'county': 'orange',
  'parcel_ids': parcel_ids,
  'limit': len(parcel_ids),
}, timeout=180))
print('enrich_count:', enrich_resp.get('count'))

print('\n== POST /api/parcels/search (baseline again, for options) ==')
resp1 = search(filters=None, timeout=120)
recs1 = resp1.get('records') or []
z_opts = resp1.get('zoning_options') or []
flu_opts = resp1.get('future_land_use_options') or []
print('baseline2_records_count:', len(recs1))
print('zoning_options_count:', len(z_opts))
print('future_land_use_options_count:', len(flu_opts))

# Pick a real zoning option.
chosen_z = None
for z in z_opts:
    if str(z).strip() and str(z).strip().upper() != 'UNKNOWN':
        chosen_z = str(z).strip()
        break

if chosen_z is None:
    print('ERROR: no zoning option available to validate zoning_in')
    raise SystemExit(1)

print('\n== POST /api/parcels/search (zoning_in) ==')
resp_z = search(filters={'zoning_in': [chosen_z]}, timeout=120)
recs_z = resp_z.get('records') or []
print('chosen_zoning:', chosen_z)
print('zoning_filtered_count:', len(recs_z))

if not recs_z:
    print('warnings:', resp_z.get('warnings'))
    print('error_reason:', resp_z.get('error_reason'))
    raise SystemExit(1)

bad = []
for r in recs_z:
    zn = norm_choice(r.get('zoning'))
    if zn != norm_choice(chosen_z):
        bad.append((r.get('parcel_id'), r.get('zoning')))
        if len(bad) >= 5:
            break

print('zoning_mismatch_count:', len(bad))
if bad:
    print('zoning_mismatches_sample:', bad[:5])
    raise SystemExit(1)

# Pick a real FLU option if available; otherwise, degrade gracefully.
chosen_f = None
for f in flu_opts:
    if str(f).strip() and str(f).strip().upper() != 'UNKNOWN':
        chosen_f = str(f).strip()
        break

if chosen_f is None:
    print('\nNOTE: No non-UNKNOWN future_land_use options available; skipping FLU filtering assertion.')
    raise SystemExit(0)

print('\n== POST /api/parcels/search (future_land_use_in) ==')
resp_f = search(filters={'future_land_use_in': [chosen_f]}, timeout=120)
recs_f = resp_f.get('records') or []
print('chosen_future_land_use:', chosen_f)
print('flu_filtered_count:', len(recs_f))

if not recs_f:
    print('warnings:', resp_f.get('warnings'))
    print('error_reason:', resp_f.get('error_reason'))
    raise SystemExit(1)

bad = []
for r in recs_f:
    fn = norm_choice(r.get('future_land_use'))
    if fn != norm_choice(chosen_f):
        bad.append((r.get('parcel_id'), r.get('future_land_use')))
        if len(bad) >= 5:
            break

print('flu_mismatch_count:', len(bad))
if bad:
    print('flu_mismatches_sample:', bad[:5])
    raise SystemExit(1)

# Print 1 example record
print('\nexample_record (flu):')
print(json.dumps(recs_f[0], indent=2)[:3500])

raise SystemExit(0)
PY
