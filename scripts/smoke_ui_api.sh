#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
COUNTY="${COUNTY:-seminole}"
MIN_COUNT="${MIN_COUNT:-15}"

python - <<'PY'
import json
import os
import sys
import time
import urllib.request

BASE_URL = os.environ.get('BASE_URL', 'http://127.0.0.1:8000').rstrip('/')
COUNTY = os.environ.get('COUNTY', 'seminole').strip().lower()
MIN_COUNT = int(os.environ.get('MIN_COUNT', '15'))

POLY = {
  "type": "Polygon",
  "coordinates": [[
    [-81.3, 28.7],
    [-81.1, 28.7],
    [-81.1, 28.9],
    [-81.3, 28.9],
    [-81.3, 28.7]
  ]]
}


def wait_ping():
  for _ in range(40):
    try:
      with urllib.request.urlopen(f"{BASE_URL}/api/debug/ping", timeout=5) as r:
        _ = r.read()
      return
    except Exception:
      time.sleep(0.25)
  raise SystemExit("FAIL: backend ping failed")


def post_search():
  payload = {
    "county": COUNTY,
    "polygon_geojson": POLY,
    "limit": 50,
    "include_geometry": False,
  }
  req = urllib.request.Request(
    f"{BASE_URL}/api/parcels/search?county={COUNTY}",
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json", "Accept": "application/json"},
  )
  with urllib.request.urlopen(req, timeout=120) as r:
    return json.load(r)


def get_detail(parcel_id: str):
  url = f"{BASE_URL}/api/parcels/{urllib.parse.quote(parcel_id)}?county={COUNTY}&include_geometry=1"
  with urllib.request.urlopen(url, timeout=60) as r:
    return json.load(r)


wait_ping()
print("== POST /api/parcels/search (seminole polygon) ==")
resp = post_search()
records = resp.get("records") or []
print("records_count:", len(records))
if len(records) < MIN_COUNT:
  raise SystemExit(f"FAIL: records_count {len(records)} < {MIN_COUNT}")

parcel_id = str(records[0].get("parcel_id") or "").strip()
if not parcel_id:
  raise SystemExit("FAIL: missing parcel_id in first record")

print("== GET /api/parcels/{id} detail ==")
detail = get_detail(parcel_id)
required_keys = [
  "parcel_id",
  "county",
  "situs_address",
  "owner_name",
  "owner_mailing_address",
  "year_built",
  "beds",
  "baths",
  "living_area_sqft",
  "lot_size_sqft",
  "lot_size_acres",
  "zoning",
  "future_land_use",
  "assessed_value",
  "taxable_value",
  "just_value",
]
missing = [k for k in required_keys if k not in detail]
if missing:
  raise SystemExit(f"FAIL: missing detail keys: {missing}")

print("PASS smoke_ui_api")
PY
