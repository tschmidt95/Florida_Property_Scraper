#!/usr/bin/env bash
set -euo pipefail

BASE="http://127.0.0.1:5173"

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

if [[ ! -s "$polygon_tmp" ]] || grep -q '^{\s*}$' "$polygon_tmp"; then
  echo "FAIL: no suggested_polygon"
  cat "$stats_tmp"
  exit 2
fi

python - <<'PY' "$polygon_tmp" "$payload_tmp"
import json
import sys

poly = json.load(open(sys.argv[1], "r", encoding="utf-8"))
payload = {
    "county": "seminole",
    "geometry": poly,
    "limit": 50,
    "include_geometry": False,
    "explain": True,
}
with open(sys.argv[2], "w", encoding="utf-8") as f:
    json.dump(payload, f)
PY

curl_json_or_fail "$BASE/api/parcels/search" "$resp_tmp" \
  -X POST \
  -H "Content-Type: application/json" \
  --data-binary "@$payload_tmp"

python - <<'PY' "$resp_tmp"
import json
import sys

j = json.load(open(sys.argv[1], "r", encoding="utf-8"))
if isinstance(j, dict) and j.get("ok") is False:
    raise SystemExit("FAIL: ok=false")
print("returned_count=", j.get("returned_count", 0))
print("total_count=", j.get("total_count", 0))
fs = j.get("field_stats") or (j.get("explain") or {}).get("field_stats") or {}
keys = sorted(k for k in fs.keys())
print("field_stats_keys=", ", ".join(keys))
PY
