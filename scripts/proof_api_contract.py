#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

BASE_URL = "http://127.0.0.1:8000"


def _request(method: str, path: str, payload: dict | None = None):
    url = f"{BASE_URL}{path}"
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
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


def _assert_contract(payload: dict):
    required = [
        "ok",
        "records",
        "total_count",
        "returned_count",
        "has_more",
        "next_cursor",
        "explain",
    ]
    for k in required:
        if k not in payload:
            raise AssertionError(f"Missing key: {k}")
    explain = payload.get("explain") or {}
    for k in ["counts", "warnings", "timings_ms", "filter_metrics", "field_stats", "debug_ids_sample"]:
        if k not in explain:
            raise AssertionError(f"Missing explain.{k}")


def main() -> int:
    print("[1/4] /api/debug/ping")
    status, body = _request("GET", "/api/debug/ping")
    ping = _parse_json(body)
    if status != 200 or not isinstance(ping, dict) or not ping.get("ok"):
        print("FAIL: /api/debug/ping")
        print(body)
        return 1
    print("OK: ping")

    print("[2/4] /api/debug/db_stats")
    status, body = _request("GET", "/api/debug/db_stats?county=seminole")
    db_stats = _parse_json(body) if status == 200 else None
    if not isinstance(db_stats, dict):
        print("FAIL: /api/debug/db_stats")
        print(body)
        return 1
    polygon = db_stats.get("suggested_polygon")
    county = "seminole"
    if db_stats.get("parcels_count_by_county"):
        counts = db_stats.get("parcels_count_by_county") or {}
        for k, v in counts.items():
            if v and k:
                county = k
                break

    print("[3/4] /api/parcels/search empty payload")
    status, body = _request("POST", "/api/parcels/search", {})
    if status == 500:
        print("FAIL: /api/parcels/search empty payload returned 500")
        print(body)
        return 1
    if status == 200:
        payload = _parse_json(body)
        if not isinstance(payload, dict):
            print("FAIL: empty payload non-JSON")
            return 1
        _assert_contract(payload)
    elif status == 400:
        payload = _parse_json(body)
        if not isinstance(payload, dict) or payload.get("ok") is not False:
            print("FAIL: bad_request response shape")
            print(body)
            return 1
    else:
        print(f"FAIL: unexpected status {status}")
        print(body)
        return 1
    print("OK: empty payload")

    if isinstance(polygon, dict):
        print("[4/4] /api/parcels/search polygon payload")
        payload = {
            "county": county,
            "geometry": polygon,
            "limit": 5,
        }
        status, body = _request("POST", "/api/parcels/search", payload)
        if status != 200:
            print("FAIL: polygon search")
            print(body)
            return 1
        resp = _parse_json(body)
        if not isinstance(resp, dict):
            print("FAIL: polygon response non-JSON")
            return 1
        _assert_contract(resp)
        print("OK: polygon search contract")

        print("[4/4] /api/parcels/search bad polygon")
        bad_payload = {
            "county": county,
            "geometry": {"type": "Polygon", "coordinates": "bad"},
            "limit": 5,
        }
        status, body = _request("POST", "/api/parcels/search", bad_payload)
        if status != 400:
            print("FAIL: bad polygon expected 400")
            print(body)
            return 1
        err = _parse_json(body)
        if not isinstance(err, dict) or err.get("ok") is not False:
            print("FAIL: bad polygon response shape")
            print(body)
            return 1
        print("OK: bad polygon 400")

        print("[4/4] /api/parcels/search trigger_keys filter")
        trig_payload = {
            "county": county,
            "geometry": polygon,
            "limit": 5,
            "trigger_keys": ["permit_hvac"],
        }
        status, body = _request("POST", "/api/parcels/search", trig_payload)
        if status != 200:
            print("FAIL: trigger_keys search")
            print(body)
            return 1
        resp = _parse_json(body)
        if not isinstance(resp, dict):
            print("FAIL: trigger_keys response non-JSON")
            return 1
        _assert_contract(resp)
        print("OK: trigger_keys search")
    else:
        print("SKIP: no suggested_polygon available")

    return 0


if __name__ == "__main__":
    sys.exit(main())
