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

print('== POST /api/parcels/search (live, baseline) ==')
search_url = f"{BASE_URL}/api/parcels/search"
resp1 = json.loads(post_json(search_url, {
  'county': 'orange',
  'live': True,
  'limit': 50,
  'include_geometry': False,
  'geometry': POLY,
}, timeout=90))

recs1 = resp1.get('records') or []
print('baseline_count:', len(recs1))
if not recs1:
    print('warnings:', resp1.get('warnings'))
    print('error_reason:', resp1.get('error_reason'))
    raise SystemExit(1)

parcel_ids = [r.get('parcel_id') for r in recs1 if r.get('parcel_id')]
parcel_ids = [str(x) for x in parcel_ids if str(x).strip()]

# Prefer parcels likely to have non-zero sales and residential improvements.
def is_candidate(r: dict) -> bool:
    if not isinstance(r, dict):
        return False
    owner = str(r.get('owner_name') or '').upper()
    if any(k in owner for k in ['CITY', 'COUNTY', 'STATE', 'SCHOOL', 'DISTRICT', 'AUTHORITY', 'ORLANDO UTIL', 'HOA', 'ASSOCIATION']):
        return False
    yb = r.get('year_built')
    if isinstance(yb, int) and yb > 0:
        return True
    return True

cands = [r for r in recs1 if is_candidate(r)]
chosen = []
seen = set()
for r in cands:
    pid = str(r.get('parcel_id') or '').strip()
    if pid and pid not in seen:
        chosen.append(pid)
        seen.add(pid)
    if len(chosen) >= 8:
        break
if len(chosen) < 8:
    chosen = parcel_ids[:8]
parcel_ids = chosen
print('sample_parcel_ids:', parcel_ids)

print('\n== POST /api/parcels/enrich (orange OCPA) ==')
enrich_url = f"{BASE_URL}/api/parcels/enrich"
enrich_resp = json.loads(post_json(enrich_url, {
  'county': 'orange',
  'parcel_ids': parcel_ids,
  'limit': len(parcel_ids),
}, timeout=180))

print('enrich_count:', enrich_resp.get('count'))
errs = enrich_resp.get('errors') or {}
try:
        print('enrich_errors_count:', len(list(errs.keys())))
except Exception:
        print('enrich_errors_count: unknown')
print('enrich_errors:')
print(json.dumps(errs, indent=2)[:6000])

print('\n== POST /api/parcels/search (post-enrich) ==')
resp2 = json.loads(post_json(search_url, {
  'county': 'orange',
  'live': True,
  'limit': 50,
  'include_geometry': False,
  'geometry': POLY,
}, timeout=90))
recs2 = resp2.get('records') or []
by_id = {r.get('parcel_id'): r for r in recs2 if r.get('parcel_id')}


def ok_record(r: dict) -> bool:
    if not r:
        return False
    yb = r.get('year_built')
    sqft = r.get('living_area_sqft')
    if sqft is None:
        # fall back to sqft.living
        for s in (r.get('sqft') or []):
            if isinstance(s, dict) and s.get('type') == 'living':
                sqft = s.get('value')
                break
    tv = r.get('total_value')
    lsp = r.get('last_sale_price')

    # Require values present and non-zero.
    if not isinstance(yb, int) or yb <= 0:
        return False
    if not isinstance(sqft, (int, float)) or float(sqft) <= 0:
        return False
    if not isinstance(tv, (int, float)) or float(tv) <= 0:
        return False
    if not isinstance(lsp, (int, float)) or float(lsp) <= 0:
        return False

    # Require OCPA source URL to be present in data_sources.
    sources = r.get('data_sources') or []
    has_ocpa = False
    for s in sources:
        if not isinstance(s, dict):
            continue
        url = str(s.get('url') or '')
        name = str(s.get('name') or '')
        if 'ocpaservices.ocpafl.org' in url or name.lower().startswith('orange_ocpa'):
            has_ocpa = True
            break
    return has_ocpa


passed = []
for pid in parcel_ids:
    r = by_id.get(pid) or {}
    if ok_record(r):
        passed.append(pid)

print('qualified_count:', len(passed))
print('qualified_parcel_ids:', passed)

# Print up to 2 qualified records for the proof artifact.
for pid in passed[:2]:
    print('\nqualified_record:', pid)
    print(json.dumps(by_id.get(pid), indent=2)[:3500])

raise SystemExit(0 if len(passed) >= 2 else 1)
PY
