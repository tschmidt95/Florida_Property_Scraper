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

returned=$(python - <<'PY' "$resp_tmp"
import json,sys
j=json.load(open(sys.argv[1], "r", encoding="utf-8"))
print(j.get('returned_count') or 0)
PY
)

echo "returned_count=$returned"

if [[ "$returned" -le 0 ]]; then
  echo "FAIL: no records returned"
  exit 2
fi

echo "PASS seminole search"
