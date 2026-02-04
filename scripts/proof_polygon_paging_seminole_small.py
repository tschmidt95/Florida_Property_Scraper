import json
import urllib.request

BASE = "http://127.0.0.1:8000"
COUNTY = "seminole"

stats = json.load(urllib.request.urlopen(f"{BASE}/api/debug/db_stats?county={COUNTY}", timeout=30))
lat_range = stats.get("lat_range") or [28.5, 28.7]
lng_range = stats.get("lng_range") or [-81.4, -81.3]
lat_c = (lat_range[0] + lat_range[1]) / 2
lng_c = (lng_range[0] + lng_range[1]) / 2
pad = 0.01
poly = {
    "type": "Polygon",
    "coordinates": [
        [
            [lng_c - pad, lat_c - pad],
            [lng_c + pad, lat_c - pad],
            [lng_c + pad, lat_c + pad],
            [lng_c - pad, lat_c + pad],
            [lng_c - pad, lat_c - pad],
        ]
    ],
}

limit = 50
cursor = None
all_ids = set()
page = 0
total = None

while True:
    page += 1
    payload = {
        "polygon_geojson": poly,
        "limit": limit,
        "include_geometry": False,
        "explain": True,
    }
    if cursor is not None:
        payload["cursor"] = cursor

    req = urllib.request.Request(
        f"{BASE}/api/parcels/search?explain=1",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = json.load(urllib.request.urlopen(req, timeout=30))
    recs = resp.get("records", [])
    for r in recs:
        pid = str(r.get("parcel_id") or "")
        ckey = str(r.get("county") or "")
        if pid:
            all_ids.add(f"{ckey}:{pid}")

    ex = resp.get("explain") or {}
    total = resp.get("total_count", ex.get("total_count", total))
    has_more = resp.get("has_more", ex.get("has_more", False))
    cursor = resp.get("next_cursor")
    print({"page": page, "returned": len(recs), "total": total, "has_more": has_more, "next_cursor": bool(cursor)})
    if not has_more:
        break

print({"unique_ids": len(all_ids), "total_count": total})
