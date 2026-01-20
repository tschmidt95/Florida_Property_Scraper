#!/bin/bash
set -euo pipefail

API_URL="http://127.0.0.1:8000"
COUNTY="seminole"

# 1. Print pa_properties counts by county
sqlite3 leads.sqlite "SELECT county, COUNT(*) FROM pa_properties GROUP BY county;"

# 2. Get a parcel_id from polygon search
cat <<EOF > /tmp/seminole_poly.json
{"type":"Polygon","coordinates":[[[-81.32,28.63],[-81.32,28.65],[-81.30,28.65],[-81.30,28.63],[-81.32,28.63]]]}
EOF

PARCEL_ID=$(curl -sS -X POST "$API_URL/api/parcels/search" \
  -H 'Content-Type: application/json' \
  -d '{"county": "seminole", "polygon_geojson": '$(cat /tmp/seminole_poly.json)', "limit": 1}' | jq -r '.records[0].parcel_id')

echo "Parcel ID: $PARCEL_ID"

# 3. Call hover endpoint and print JSON
curl -sS "$API_URL/api/parcels/seminole/$PARCEL_ID/hover" | jq .
