#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import urllib.request

BASE_URL = "http://127.0.0.1:8000"


def _get_json(path: str) -> dict:
    with urllib.request.urlopen(f"{BASE_URL}{path}") as resp:
        return json.loads(resp.read().decode("utf-8"))


def _post_json(path: str, payload: dict) -> dict:
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    stats = _get_json("/api/debug/db_stats?county=seminole")
    poly = stats.get("suggested_polygon")
    if not isinstance(poly, dict):
        print("FAIL: no suggested_polygon")
        return 1

    payload = {
        "county": "seminole",
        "geometry": poly,
        "limit": 200,
        "include_geometry": False,
        "explain": True,
    }
    resp = _post_json("/api/parcels/search", payload)
    records = resp.get("records") or []
    if not isinstance(records, list) or not records:
        print("FAIL: no records returned")
        return 1

    fields = ["beds", "baths", "living_area_sqft", "year_built", "zoning", "future_land_use"]
    coverage = {f: 0 for f in fields}
    for r in records:
        for f in fields:
            v = r.get(f)
            if v is None or v == "" or v == 0:
                continue
            coverage[f] += 1

    print("Coverage (percent of records):")
    for f in fields:
        pct = round((coverage[f] / len(records)) * 100.0, 2)
        print(f"- {f}: {pct}% ({coverage[f]}/{len(records)})")

    print("\nSample records (3):")
    for r in records[:3]:
        sample = {
            "parcel_id": r.get("parcel_id"),
            "beds": r.get("beds"),
            "baths": r.get("baths"),
            "living_area_sqft": r.get("living_area_sqft"),
            "year_built": r.get("year_built"),
            "zoning": r.get("zoning"),
            "future_land_use": r.get("future_land_use"),
            "source_url": r.get("source_url") or r.get("raw_source_url") or r.get("source") or "",
        }
        print(json.dumps(sample, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
