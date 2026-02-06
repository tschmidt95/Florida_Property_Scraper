#!/usr/bin/env bash
set -euo pipefail

BASE="http://127.0.0.1:8000"

# shellcheck disable=SC1091
source "/workspaces/Florida_Property_Scraper/scripts/_curl_json_helper.sh"

stats_tmp="$(mktemp)"
polygon_tmp="$(mktemp)"
payload_tmp="$(mktemp)"
resp_tmp="$(mktemp)"
cleanup() {
  rm -f "$stats_tmp" "$polygon_tmp" "$payload_tmp" "$resp_tmp"
}
trap cleanup EXIT

curl_json_or_fail "$BASE/api/debug/db_stats?county=seminole" "$stats_tmp"
python - <<'PY' "$stats_tmp" "$polygon_tmp"
import json
import sys

stats_path = sys.argv[1]
out_path = sys.argv[2]
j = json.load(open(stats_path, "r", encoding="utf-8"))
poly = j.get("suggested_polygon") or {}
with open(out_path, "w", encoding="utf-8") as f:
  json.dump(poly, f)
PY

polygon=$(cat "$polygon_tmp")

if [[ "$polygon" == "{}" ]]; then
  echo "FAIL: no suggested_polygon"
  exit 2
fi

python - <<'PY' "$polygon_tmp" "$payload_tmp"
import json
import sys

poly = json.load(open(sys.argv[1], "r", encoding="utf-8"))
payload = {"county": "seminole", "geometry": poly, "limit": 50, "include_geometry": False}
with open(sys.argv[2], "w", encoding="utf-8") as f:
  json.dump(payload, f)
PY

curl_json_or_fail "$BASE/api/parcels/search" "$resp_tmp" \
  -X POST \
  -H "Content-Type: application/json" \
  --data-binary "@$payload_tmp"
python - <<'PY' "$resp_tmp"
import json,sys
path=sys.argv[1]
with open(path, 'r', encoding='utf-8') as f:
    j=json.load(f)
if isinstance(j, dict) and j.get('ok') is False:
    raise SystemExit("FAIL: ok=false")
returned=j.get('returned_count') or j.get('summary',{}).get('returned_count') or 0
parcels=j.get('parcels') or j.get('records') or []
ids=[(p.get('parcel_id') or '').strip() for p in parcels if isinstance(p, dict)]
ids=[i for i in ids if i]
print(f"returned_count={returned}")
print("first_3_parcel_ids=" + ", ".join(ids[:3]))
PY

rm -f "$resp_tmp"
