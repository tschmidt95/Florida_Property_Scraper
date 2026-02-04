import json
import sqlite3
import urllib.request

BASE = "http://127.0.0.1:8000"

lcon = sqlite3.connect("/workspaces/Florida_Property_Scraper/leads.sqlite")
lcur = lcon.cursor()
row = lcur.execute(
    "SELECT parcel_id FROM parcel_trigger_rollups WHERE details_json LIKE '%permit_hvac%' LIMIT 1"
).fetchone()
lcon.close()
if not row:
    raise SystemExit("no permit_hvac rollups found")
parcel_id = row[0]

pcon = sqlite3.connect("/workspaces/Florida_Property_Scraper/data/parcels/parcels.sqlite")
pcur = pcon.cursor()
prow = pcur.execute(
    "SELECT minx, miny, maxx, maxy FROM parcels WHERE parcel_id=? LIMIT 1",
    (parcel_id,),
).fetchone()
pcon.close()
if not prow:
    raise SystemExit("parcel not found in parcels.sqlite")
minx, miny, maxx, maxy = [float(x) for x in prow]
pad = 0.0005
poly = {
    "type": "Polygon",
    "coordinates": [
        [
            [minx - pad, miny - pad],
            [maxx + pad, miny - pad],
            [maxx + pad, maxy + pad],
            [minx - pad, maxy + pad],
            [minx - pad, miny - pad],
        ]
    ],
}


def search(extra=None):
    payload = {
        "polygon_geojson": poly,
        "limit": 200,
        "include_geometry": False,
        "explain": True,
    }
    if extra:
        payload.update(extra)
    req = urllib.request.Request(
        f"{BASE}/api/parcels/search?explain=1",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return json.load(urllib.request.urlopen(req, timeout=30))


base = search()
trigger = search({"trigger_keys": ["permit_hvac"]})
permits = search({"trigger_keys": ["has_permits"]})
print(
    {
        "parcel_id": parcel_id,
        "base_total": base.get("total_count"),
        "trigger_total": trigger.get("total_count"),
        "has_permits_total": permits.get("total_count"),
    }
)
