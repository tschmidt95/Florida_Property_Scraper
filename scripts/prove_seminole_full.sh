#!/bin/bash
set -euo pipefail

API_URL="http://127.0.0.1:8000"
COUNTY="seminole"

# 1. Ping backend
curl -sS "$API_URL/api/debug/ping" | jq .

# 2. Check parcels coverage
curl -sS "$API_URL/api/debug/parcels_coverage?county=$COUNTY" | jq .

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
  curl -sS -X POST "$API_URL/api/parcels/search" \
    -H 'Content-Type: application/json' \
    -d '{"county": "seminole", "polygon_geojson": '"$(cat /tmp/seminole_poly.json)"', "limit": 3}' | jq .
else
  echo "To test in UI: set county to 'seminole', draw a polygon anywhere, and Run."
fi
