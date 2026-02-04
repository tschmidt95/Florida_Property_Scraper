#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

BASE_URL = "http://127.0.0.1:8000"

FILTER_MAP = {
    "living_area_sqft": ("min_sqft", 100000),
    "year_built": ("min_year_built", 2100),
    "total_value": ("min_value", 1_000_000_000),
    "assessed_value": ("min_assessed_value", 1_000_000_000),
    "land_value": ("min_land_value", 1_000_000_000),
    "building_value": ("min_building_value", 1_000_000_000),
}


def _request(method: str, path: str, payload: dict | None = None):
    url = f"{BASE_URL}{path}"
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
            return resp.getcode(), body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        return exc.code, body


def _parse_json(raw: str):
    try:
        return json.loads(raw)
    except Exception:
        return None


def main() -> int:
    status, body = _request("GET", "/api/debug/db_stats?county=seminole")
    db_stats = _parse_json(body) if status == 200 else None
    if not isinstance(db_stats, dict):
        print("FAIL: /api/debug/db_stats")
        print(body)
        return 1

    polygon = db_stats.get("suggested_polygon")
    if not isinstance(polygon, dict):
        print("FAIL: no suggested_polygon available")
        return 1

    county = "seminole"
    counts = db_stats.get("parcels_count_by_county") or {}
    for k, v in counts.items():
        if v and k:
            county = k
            break

    status, body = _request("GET", f"/api/debug/source_coverage?county={county}")
    coverage = _parse_json(body) if status == 200 else None
    if not isinstance(coverage, dict):
        print("FAIL: /api/debug/source_coverage")
        print(body)
        return 1

    fields = coverage.get("fields") or {}
    selected_field = None
    selected_filter = None
    for field_name, meta in fields.items():
        if field_name not in FILTER_MAP:
            continue
        try:
            pct = float(meta.get("coverage_pct") or 0)
        except Exception:
            pct = 0
        if pct > 0:
            selected_field = field_name
            selected_filter = FILTER_MAP[field_name]
            break

    if not selected_field or not selected_filter:
        print("FAIL: no field with coverage_pct > 0")
        return 1

    filter_key, filter_value = selected_filter

    base_payload = {
        "county": county,
        "geometry": polygon,
        "limit": 200,
    }
    status, body = _request("POST", "/api/parcels/search", base_payload)
    base_resp = _parse_json(body) if status == 200 else None
    if not isinstance(base_resp, dict) or base_resp.get("ok") is not True:
        print("FAIL: base search")
        print(body)
        return 1

    base_total = int(base_resp.get("total_count") or 0)
    if base_total <= 0:
        print("SKIP: no parcels found in base polygon")
        return 0

    filtered_payload = {
        "county": county,
        "geometry": polygon,
        "limit": 200,
        "filters": {filter_key: filter_value},
    }
    status, body = _request("POST", "/api/parcels/search", filtered_payload)
    filtered_resp = _parse_json(body) if status == 200 else None
    if not isinstance(filtered_resp, dict) or filtered_resp.get("ok") is not True:
        print("FAIL: filtered search")
        print(body)
        return 1

    filtered_total = int(filtered_resp.get("total_count") or 0)

    print(f"County: {county}")
    print(f"Field: {selected_field} -> {filter_key}={filter_value}")
    print(f"Base total_count: {base_total}")
    print(f"Filtered total_count: {filtered_total}")
    print("Coverage:")
    print(json.dumps(fields.get(selected_field), indent=2))

    if filtered_total >= base_total:
        print("FAIL: filter did not reduce total_count")
        return 1

    print("OK: filter reduced total_count")
    return 0


if __name__ == "__main__":
    sys.exit(main())
