#!/bin/bash
set -euo pipefail

# shellcheck disable=SC1091
source "/workspaces/Florida_Property_Scraper/scripts/_curl_json_helper.sh"

# Show git sha
GIT_SHA=$(git rev-parse --short HEAD)
echo "GIT SHA: $GIT_SHA"

# Show sqlite row count for seminole
sqlite3 data/parcels/parcels.sqlite "SELECT COUNT(*) FROM parcels WHERE county='seminole';"

# Polygon around demo SEM-0001 area
cat <<EOF > /tmp/demo_polygon.json
{"type":"Polygon","coordinates":[[[-81.3700,28.6500],[-81.3680,28.6500],[-81.3680,28.6520],[-81.3700,28.6520],[-81.3700,28.6500]]]}
EOF

demo_resp="$(mktemp)"
curl_json_or_fail "http://127.0.0.1:8000/api/parcels/search" "$demo_resp" \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"county": "seminole", "polygon_geojson": '$(cat /tmp/demo_polygon.json)'}'
cat "$demo_resp" > /tmp/demo_parcel_search.json
rm -f "$demo_resp"

COUNT1=$(jq '.candidate_count' /tmp/demo_parcel_search.json)
echo "Demo area candidate_count: $COUNT1"
if [ "$COUNT1" -ge 1 ]; then
  echo "PASS: Demo polygon returns parcels."
else
  echo "FAIL: Demo polygon returned 0 parcels."
  exit 1
fi

# Polygon around Pineapple Lane
cat <<EOF > /tmp/pineapple_polygon.json
{"type":"Polygon","coordinates":[[[-81.3205,28.7005],[-81.3205,28.6985],[-81.3185,28.6985],[-81.3185,28.7005],[-81.3205,28.7005]]]}
EOF

pineapple_resp="$(mktemp)"
curl_json_or_fail "http://127.0.0.1:8000/api/parcels/search" "$pineapple_resp" \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"county": "seminole", "polygon_geojson": '$(cat /tmp/pineapple_polygon.json)'}'
cat "$pineapple_resp" > /tmp/pineapple_parcel_search.json
rm -f "$pineapple_resp"

COUNT2=$(jq '.candidate_count' /tmp/pineapple_parcel_search.json)
WARN2=$(jq -r '.warnings[]?' /tmp/pineapple_parcel_search.json)
echo "Pineapple Lane candidate_count: $COUNT2"
echo "Warnings: $WARN2"
if [ "$COUNT2" -eq 0 ] && echo "$WARN2" | grep -q "No parcel geometry candidates in DB for this bbox"; then
  echo "PASS: Pineapple Lane returns explicit coverage warning."
else
  echo "FAIL: Pineapple Lane did not return correct warning/candidate_count."
  exit 1
fi

echo "ALL PROOFS PASS"
