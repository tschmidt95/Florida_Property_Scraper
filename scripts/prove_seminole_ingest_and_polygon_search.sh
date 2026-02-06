#!/bin/bash
set -euo pipefail

# shellcheck disable=SC1091
source "/workspaces/Florida_Property_Scraper/scripts/_curl_json_helper.sh"

# 1. Show git SHA
GIT_SHA=$(git rev-parse --short HEAD)
echo "GIT SHA: $GIT_SHA"

# 2. Show parcel row count for seminole
sqlite3 data/parcels/parcels.sqlite "SELECT COUNT(*) FROM parcels WHERE county='seminole';"

# 3. Run a polygon search around Pineapple Lane (example bbox)
cat <<EOF > /tmp/pineapple_polygon.json
{"type":"Polygon","coordinates":[[[-81.3205,28.7005],[-81.3205,28.6985],[-81.3185,28.6985],[-81.3185,28.7005],[-81.3205,28.7005]]]}
EOF

search_resp="$(mktemp)"
curl_json_or_fail "http://127.0.0.1:8000/api/parcels/search" "$search_resp" \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"county": "seminole", "polygon_geojson": '$(cat /tmp/pineapple_polygon.json)'}'
cat "$search_resp" > /tmp/parcel_search_result.json
rm -f "$search_resp"

COUNT=$(jq '.candidate_count' /tmp/parcel_search_result.json)
echo "Returned candidate_count: $COUNT"

if [ "$COUNT" -gt 0 ]; then
  echo "PASS: Polygon search returns parcels."
else
  echo "FAIL: Polygon search returned 0 parcels."
  exit 1
fi

# 4. Print first 3 results hover_fields and assert owner_name or situs_address is non-empty
jq '.records[0:3][] | {parcel_id, owner_name, situs_address, living_area_sqft}' /tmp/parcel_search_result.json

FIELDS_OK=$(jq '[.records[0:3][] | select((.owner_name != null and .owner_name != "") or (.situs_address != null and .situs_address != ""))] | length' /tmp/parcel_search_result.json)
if [ "$FIELDS_OK" -ge 1 ]; then
  echo "PASS: At least one record has owner_name or situs_address."
else
  echo "FAIL: No records have owner_name or situs_address."
  exit 1
fi

echo "ALL PROOFS PASS"
