#!/usr/bin/env bash
set -euo pipefail

BASE="http://127.0.0.1:8000"

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

resp=$(curl -sS -X POST "$BASE/api/parcels/search" -H "Content-Type: application/json" -d "$payload")

returned=$(echo "$resp" | python - <<'PY'
import json,sys
j=json.load(sys.stdin)
print(j.get('returned_count') or 0)
PY
)

echo "returned_count=$returned"

if [[ "$returned" -le 0 ]]; then
  echo "FAIL: no records returned"
  exit 2
fi

echo "PASS seminole search"
