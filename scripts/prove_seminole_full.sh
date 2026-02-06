#!/bin/bash
set -euo pipefail

# shellcheck disable=SC1091
source "/workspaces/Florida_Property_Scraper/scripts/_curl_json_helper.sh"

API_URL="http://127.0.0.1:8000"
COUNTY="seminole"

# 1. Ping backend
ping_resp="$(mktemp)"
curl_json_or_fail "$API_URL/api/debug/ping" "$ping_resp"
jq . "$ping_resp"
rm -f "$ping_resp"

# 2. Check parcels coverage
coverage_resp="$(mktemp)"
curl_json_or_fail "$API_URL/api/debug/parcels_coverage?county=$COUNTY" "$coverage_resp"
jq . "$coverage_resp"
rm -f "$coverage_resp"

# 3. Optionally, run a polygon search (user can edit BBOX below)
read -p "Run polygon search test? (y/N): " runpoly
if [[ "$runpoly" =~ ^[Yy]$ ]]; then
  # Example: small bbox in Seminole (edit as needed)
  cat <<EOF > /tmp/seminole_poly.json
{
  "type": "Polygon",
  "coordinates": [[
    [-81.3, 28.7],
    [-81.1, 28.7],
    [-81.1, 28.9],
    [-81.3, 28.9],
    [-81.3, 28.7]
  ]]
}
EOF
  search_resp="$(mktemp)"
  curl_json_or_fail "$API_URL/api/parcels/search" "$search_resp" \
    -X POST \
    -H 'Content-Type: application/json' \
    -d '{"county": "seminole", "polygon_geojson": '"$(cat /tmp/seminole_poly.json)"', "limit": 3}'
  jq . "$search_resp"
  rm -f "$search_resp"
else
  echo "To test in UI: set county to 'seminole', draw a polygon anywhere, and Run."
fi
