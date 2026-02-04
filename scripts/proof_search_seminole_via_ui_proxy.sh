#!/usr/bin/env bash
set -euo pipefail

BASE="http://127.0.0.1:5173"

stats=$(curl -sS "$BASE/api/debug/db_stats?county=seminole")
polygon=$(echo "$stats" | python - <<'PY'
import json,sys
j=json.load(sys.stdin)
print(json.dumps(j.get('suggested_polygon') or {}))
PY
)

if [[ "$polygon" == "{}" ]]; then
  echo "FAIL: no suggested_polygon"
  exit 2
fi

payload=$(python - <<'PY'
import json,sys
poly=json.loads(sys.argv[1])
print(json.dumps({"county":"seminole","geometry":poly,"limit":50,"include_geometry":False}))
PY
"$polygon")

resp_tmp=$(mktemp)
http_status=$(curl -sS -o "$resp_tmp" -w "%{http_code}" -X POST "$BASE/api/parcels/search" -H "Content-Type: application/json" -d "$payload")

echo "status=$http_status"
python - <<'PY' "$resp_tmp"
import json,sys
path=sys.argv[1]
with open(path, 'r', encoding='utf-8') as f:
    j=json.load(f)
returned=j.get('returned_count') or j.get('summary',{}).get('returned_count') or 0
parcels=j.get('parcels') or j.get('records') or []
ids=[(p.get('parcel_id') or '').strip() for p in parcels if isinstance(p, dict)]
ids=[i for i in ids if i]
print(f"returned_count={returned}")
print("first_3_parcel_ids=" + ", ".join(ids[:3]))
PY

rm -f "$resp_tmp"
