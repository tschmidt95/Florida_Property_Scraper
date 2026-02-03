#!/usr/bin/env bash
set -euo pipefail

API_URL=${API_URL:-http://127.0.0.1:8000}
PAYLOAD_FILE=/tmp/smoke_payload.json

python - <<'PY' > "$PAYLOAD_FILE"
import json, sys, urllib.request, os
API_URL = "${API_URL}"

def fetch_db_stats():
  with urllib.request.urlopen(f"{API_URL}/api/debug/db_stats?county=seminole", timeout=20) as r:
    return json.loads(r.read().decode('utf-8'))

payload = {
  "county": "seminole",
  "limit": 250,
  "include_geometry": False,
  "explain": True,
}

if os.path.exists('/tmp/ui_req.json'):
  try:
    p = json.load(open('/tmp/ui_req.json','r'))
    if isinstance(p, dict):
      p['county'] = 'seminole'
      p['explain'] = True
      p['limit'] = 250
      if isinstance(p.get('filters'), dict):
        p['filters'].pop('exclude_missing', None)
      payload = p
  except Exception:
    pass
else:
  stats = fetch_db_stats()
  suggested = stats.get('suggested_polygon') if isinstance(stats, dict) else None
  if not isinstance(suggested, dict):
    raise SystemExit('FAIL: no suggested_polygon from /api/debug/db_stats')
  payload['polygon_geojson'] = suggested

json.dump(payload, sys.stdout)
PY

if ! curl -sS "$API_URL/health" >/dev/null 2>&1; then
  if [[ -x "./scripts/dev_reset.sh" ]]; then
    ./scripts/dev_reset.sh
  fi
fi

curl -sS -D /tmp/headers.txt -o /tmp/body.txt \
  -X POST "$API_URL/api/parcels/search?county=seminole&explain=1" \
  -H "Content-Type: application/json" \
  --data-binary "@$PAYLOAD_FILE"

echo "HTTP $(head -n 1 /tmp/headers.txt | tr -d '\r')"

python - <<'PY'
import json, sys
raw = open('/tmp/body.txt','r',encoding='utf-8', errors='ignore').read()
headers = open('/tmp/headers.txt','r',encoding='utf-8', errors='ignore').read()
if not raw.lstrip().startswith('{') or 'application/json' not in headers.lower():
  print('FAIL: response not JSON')
  print(headers)
  print(raw[:200])
  raise SystemExit(1)
resp = json.loads(raw)
records = resp.get('records') or []
count = len(records)
markers = sum(1 for r in records if isinstance(r.get('lat'), (int,float)) and isinstance(r.get('lng'), (int,float)))
explain = resp.get('explain') or {}
stage_counts = explain.get('stage_counts') or {}
print('markers_possible_count:', explain.get('markers_possible_count'))
print('missing_lat_lng_count:', explain.get('missing_lat_lng_count'))
print('stage_counts:', stage_counts)
print('dropped_reasons:', explain.get('dropped_reasons'))
print('missing_field_counts:', explain.get('missing_field_counts'))
if count <= 0:
    raise SystemExit('FAIL: records_count == 0')
if markers < max(1, int(count * 0.95)):
    raise SystemExit(f'FAIL: marker_count too low {markers}/{count}')
if not stage_counts:
    raise SystemExit('FAIL: explain.stage_counts missing')
print(f'base_count={count} markers={markers} stage_counts={stage_counts}')
PY

python - <<'PY'
import json, sys
cases = [
  ("min_sqft", {"min_sqft": 2000}),
  ("min_year_built", {"min_year_built": 2000}),
  ("min_beds", {"min_beds": 3}),
  ("min_baths", {"min_baths": 2}),
  ("min_lot_size_sqft", {"min_lot_size_sqft": 2000}),
]
json.dump(cases, open('/tmp/smoke_filter_cases.json','w'))
PY

base_count=$(python - <<'PY'
import json
raw_base = open('/tmp/body.txt','r',encoding='utf-8', errors='ignore').read()
base = json.loads(raw_base)
print(len(base.get('records') or []))
PY
)

python - <<'PY'
import json, sys
cases = json.load(open('/tmp/smoke_filter_cases.json','r'))
for i, (name, filt) in enumerate(cases):
  payload = json.load(open('/tmp/smoke_payload.json','r'))
  base_filters = payload.get('filters') if isinstance(payload.get('filters'), dict) else {}
  merged = dict(base_filters)
  merged.update(filt)
  payload['filters'] = merged
  payload['explain'] = True
  open(f'/tmp/smoke_payload_filtered_{i}.json','w').write(json.dumps(payload))
PY

idx=0
while read -r name; do
  curl -sS -D "/tmp/headers_filtered_${idx}.txt" -o "/tmp/body_filtered_${idx}.txt" \
  -X POST "$API_URL/api/parcels/search?county=seminole&explain=1" \
  -H "Content-Type: application/json" \
  --data-binary "/tmp/smoke_payload_filtered_${idx}.json"
  echo "HTTP $(head -n 1 /tmp/headers_filtered_${idx}.txt | tr -d '\r')"
  idx=$((idx+1))
done < <(python - <<'PY'
import json
cases = json.load(open('/tmp/smoke_filter_cases.json','r'))
for name, _f in cases:
  print(name)
PY
)

python - <<'PY'
import json
raw_base = open('/tmp/body.txt','r',encoding='utf-8', errors='ignore').read()
base = json.loads(raw_base)
base_count = len(base.get('records') or [])
if base_count <= 0:
  raise SystemExit('FAIL: base_count == 0')

cases = json.load(open('/tmp/smoke_filter_cases.json','r'))
for i, (name, _filt) in enumerate(cases):
  headers = open(f'/tmp/headers_filtered_{i}.txt','r',encoding='utf-8', errors='ignore').read()
  raw = open(f'/tmp/body_filtered_{i}.txt','r',encoding='utf-8', errors='ignore').read()
  if not raw.lstrip().startswith('{') or 'application/json' not in headers.lower():
    print(f'FAIL: filtered response not JSON ({name})')
    print(headers)
    print(raw[:300])
    raise SystemExit(1)
  data = json.loads(raw)
  flt_count = len(data.get('records') or [])
  if flt_count > base_count:
    raise SystemExit(f'FAIL: {name} filtered_count {flt_count} > base_count {base_count}')
  print(f'{name}_filtered_count={flt_count} (base_count={base_count})')

print('PASS smoke_filters')
PY
