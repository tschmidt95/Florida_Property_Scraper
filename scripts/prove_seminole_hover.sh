#!/bin/bash
set -euo pipefail

# shellcheck disable=SC1091
source "/workspaces/Florida_Property_Scraper/scripts/_curl_json_helper.sh"

API_URL="http://127.0.0.1:8000"
COUNTY="seminole"

# 1. Print pa_properties counts by county
sqlite3 leads.sqlite "SELECT county, COUNT(*) FROM pa_properties GROUP BY county;"

# 2. Get a parcel_id from polygon search
cat <<EOF > /tmp/seminole_poly.json
{"type":"Polygon","coordinates":[[[-81.32,28.63],[-81.32,28.65],[-81.30,28.65],[-81.30,28.63],[-81.32,28.63]]]}
EOF

search_resp="$(mktemp)"
curl_json_or_fail "$API_URL/api/parcels/search" "$search_resp" \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"county": "seminole", "polygon_geojson": '$(cat /tmp/seminole_poly.json)', "limit": 1}'
PARCEL_ID=$(jq -r '.records[0].parcel_id' "$search_resp")
rm -f "$search_resp"

echo "Parcel ID: $PARCEL_ID"

# 3. Call hover endpoint and print JSON
hover_resp="$(mktemp)"
curl_json_or_fail "$API_URL/api/parcels/seminole/$PARCEL_ID/hover" "$hover_resp"
jq . "$hover_resp"
rm -f "$hover_resp"
