#!/usr/bin/env bash
set -euo pipefail

API_URL=${API_URL:-http://127.0.0.1:8000}
PAYLOAD_FILE=/tmp/smoke_payload.json

if [[ -f /tmp/ui_req.json ]]; then
  python - <<'PY' > "$PAYLOAD_FILE"
import json, sys
p = json.load(open('/tmp/ui_req.json','r'))
p['county'] = 'seminole'
p['explain'] = True
p['limit'] = 250
if isinstance(p.get('filters'), dict):
    p['filters'].pop('exclude_missing', None)
json.dump(p, sys.stdout)
PY
else
  python - <<'PY' > "$PAYLOAD_FILE"
import json, sys
payload = {
  "county": "seminole",
  "limit": 250,
  "include_geometry": False,
  "explain": True,
  "polygon_geojson": {
    "type": "Polygon",
    "coordinates": [
      [
        [-81.295, 28.70],
        [-81.20, 28.70],
        [-81.20, 28.64],
        [-81.295, 28.64],
        [-81.295, 28.70]
      ]
    ]
  }
}
json.dump(payload, sys.stdout)
PY
fi

if ! curl -sS "$API_URL/health" >/dev/null 2>&1; then
  if [[ -x "./scripts/dev_reset.sh" ]]; then
    ./scripts/dev_reset.sh
  fi
fi

curl -sS -X POST "$API_URL/api/parcels/search?county=seminole&explain=1" \
  -H "Content-Type: application/json" \
  --data-binary "@$PAYLOAD_FILE" \
  > /tmp/smoke_resp.json

python - <<'PY'
import json, sys
resp = json.load(open('/tmp/smoke_resp.json','r'))
records = resp.get('records') or []
count = len(records)
markers = sum(1 for r in records if isinstance(r.get('lat'), (int,float)) and isinstance(r.get('lng'), (int,float)))
explain = resp.get('explain') or {}
stage_counts = explain.get('stage_counts') or {}
if count <= 0:
    raise SystemExit('FAIL: records_count == 0')
if markers < max(1, int(count * 0.95)):
    raise SystemExit(f'FAIL: marker_count too low {markers}/{count}')
if not stage_counts:
    raise SystemExit('FAIL: explain.stage_counts missing')
print(f'base_count={count} markers={markers} stage_counts={stage_counts}')
PY

python - <<'PY'
import json, subprocess, sys, copy
payload = json.load(open('/tmp/smoke_payload.json','r'))
filters = payload.get('filters') if isinstance(payload.get('filters'), dict) else {}
filters = dict(filters)
filters['min_sqft'] = 2000
payload['filters'] = filters
payload['explain'] = True
open('/tmp/smoke_payload_filtered.json','w').write(json.dumps(payload))
PY

curl -sS -X POST "$API_URL/api/parcels/search?county=seminole&explain=1" \
  -H "Content-Type: application/json" \
  --data-binary /tmp/smoke_payload_filtered.json \
  > /tmp/smoke_resp_filtered.json

python - <<'PY'
import json
base = json.load(open('/tmp/smoke_resp.json','r'))
flt = json.load(open('/tmp/smoke_resp_filtered.json','r'))
base_count = len(base.get('records') or [])
flt_count = len(flt.get('records') or [])
if base_count <= 0:
    raise SystemExit('FAIL: base_count == 0')
if flt_count <= 0:
    raise SystemExit(f'FAIL: filtered_count == 0 (base_count={base_count})')
if flt_count > base_count:
    raise SystemExit(f'FAIL: filtered_count {flt_count} > base_count {base_count}')
print(f'filtered_count={flt_count} (base_count={base_count})')
print('PASS smoke_filters')
PY
