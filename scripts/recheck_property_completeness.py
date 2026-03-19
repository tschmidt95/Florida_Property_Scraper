#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8000").rstrip("/")
COUNTY = os.getenv("COUNTY", "seminole").strip().lower()
SEARCH_LIMIT = int(os.getenv("COMPLETENESS_SEARCH_LIMIT", "150"))
RECHECK_LIMIT = int(os.getenv("COMPLETENESS_RECHECK_LIMIT", "80"))
ENRICH_BATCH = int(os.getenv("COMPLETENESS_ENRICH_BATCH", "50"))
REPORT_PATH = Path(os.getenv("COMPLETENESS_REPORT_PATH", "/workspaces/Florida_Property_Scraper/PROOF_PROPERTY_COMPLETENESS_RECHECK.txt"))
REPORT_JSON = Path(os.getenv("COMPLETENESS_REPORT_JSON", "/workspaces/Florida_Property_Scraper/data/property_completeness_recheck.json"))

REQUIRED_FIELDS = [
    "owner_name",
    "owner_mailing_address",
    "living_area_sqft",
    "lot_size_sqft",
    "zoning",
    "future_land_use",
    "total_value",
    "assessed_value",
    "taxable_value",
    "photo_url",
]

COUNTY_CENTERS: dict[str, tuple[float, float, int]] = {
    "seminole": (28.7100, -81.3000, 26000),
    "orange": (28.5400, -81.3800, 28000),
}


def _http_json(method: str, path: str, payload: dict[str, Any] | None = None, timeout: int = 60) -> tuple[int, dict[str, Any]]:
    url = f"{API_BASE}{path}"
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, method=method.upper(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw) if raw.strip() else {}
            if not isinstance(data, dict):
                data = {"raw": data}
            return int(resp.status), data
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(raw) if raw.strip() else {}
            if not isinstance(data, dict):
                data = {"raw": data}
        except Exception:
            data = {"raw": raw}
        return int(e.code), data


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    if isinstance(value, (int, float)):
        try:
            return float(value) <= 0.0
        except Exception:
            return True
    return False


def _search_records(county: str, limit: int) -> list[dict[str, Any]]:
    lat, lng, radius = COUNTY_CENTERS.get(county, (28.7100, -81.3000, 26000))
    payload = {
        "county": county,
        "center": {"lat": lat, "lng": lng},
        "radius_m": radius,
        "limit": max(1, int(limit)),
        "include_geometry": False,
        "filters": {"missing_policy": "lenient"},
    }
    status, data = _http_json("POST", "/api/parcels/search_normalized", payload=payload, timeout=120)
    if status != 200:
        raise RuntimeError(f"search_normalized failed: HTTP {status}")
    records = data.get("records") or []
    if not isinstance(records, list):
        return []
    return [r for r in records if isinstance(r, dict)]


def _missing_fields_for_record(rec: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for field in REQUIRED_FIELDS:
        val = rec.get(field)
        if field == "owner_name" and _is_missing(val):
            pa_owner = rec.get("owner_names")
            if isinstance(pa_owner, list) and any(str(x).strip() for x in pa_owner):
                continue
        if _is_missing(val):
            missing.append(field)
    return missing


def _detail(parcel_id: str, county: str) -> dict[str, Any]:
    path = f"/api/parcels/{urllib.parse.quote(parcel_id)}?county={urllib.parse.quote(county)}&include_fields=1"
    status, data = _http_json("GET", path, timeout=60)
    if status != 200:
        return {}

    result: dict[str, Any] = {}
    pa_raw = data.get("pa")
    merged_raw = data.get("merged_fields")
    pa: dict[str, Any] = pa_raw if isinstance(pa_raw, dict) else {}
    merged: dict[str, Any] = merged_raw if isinstance(merged_raw, dict) else {}

    def _pick(*keys: str) -> Any:
        for k in keys:
            v = data.get(k)
            if not _is_missing(v):
                return v
            if not _is_missing(pa.get(k)):
                return pa.get(k)
            if not _is_missing(merged.get(k)):
                return merged.get(k)
        return None

    result["parcel_id"] = parcel_id
    result["owner_name"] = _pick("owner_name")
    result["owner_mailing_address"] = _pick("owner_mailing_address", "mailing_address")
    result["living_area_sqft"] = _pick("living_area_sqft")
    result["lot_size_sqft"] = _pick("lot_size_sqft")
    result["zoning"] = _pick("zoning")
    result["future_land_use"] = _pick("future_land_use")
    result["total_value"] = _pick("total_value", "just_value")
    result["assessed_value"] = _pick("assessed_value")
    result["taxable_value"] = _pick("taxable_value")
    result["photo_url"] = _pick("photo_url")
    return result


def main() -> int:
    started = datetime.now(timezone.utc).isoformat()

    records = _search_records(COUNTY, SEARCH_LIMIT)
    if not records:
        raise RuntimeError("No records returned for completeness recheck")

    sample = records[: max(1, min(RECHECK_LIMIT, len(records)))]
    before_missing = {}
    to_enrich: list[str] = []
    to_owner_enrich: list[str] = []

    for rec in sample:
        pid = str(rec.get("parcel_id") or "").strip()
        if not pid:
            continue
        missing = _missing_fields_for_record(rec)
        before_missing[pid] = missing
        if missing:
            to_enrich.append(pid)
            if "owner_mailing_address" in missing or "owner_name" in missing:
                to_owner_enrich.append(pid)

    # Batch parcel enrichment first (cache/value/size/photo when available).
    enrich_calls = 0
    enrich_ok = 0
    for i in range(0, len(to_enrich), max(1, ENRICH_BATCH)):
        ids = to_enrich[i : i + max(1, ENRICH_BATCH)]
        if not ids:
            continue
        enrich_calls += 1
        payload = {"county": COUNTY, "parcel_ids": ids, "limit": len(ids)}
        status, data = _http_json("POST", "/api/parcels/enrich", payload=payload, timeout=180)
        if status == 200 and isinstance(data, dict):
            enrich_ok += 1

    # Owner enrichment pass for owner/contact fields.
    owner_calls = 0
    owner_ok = 0
    for pid in to_owner_enrich:
        owner_calls += 1
        path = f"/api/owners/enrich?county={urllib.parse.quote(COUNTY)}&parcel_id={urllib.parse.quote(pid)}"
        status, _data = _http_json("GET", path, timeout=60)
        if status == 200:
            owner_ok += 1

    # Trigger evaluation pass for all touched parcels so trigger rollups update.
    trigger_payload = {
        "county": COUNTY,
        "parcel_ids": to_enrich[:200],
    }
    trigger_status, trigger_data = _http_json("POST", "/api/triggers/evaluate", payload=trigger_payload, timeout=120)
    trigger_results = trigger_data.get("results") if isinstance(trigger_data, dict) else []
    trigger_fired = 0
    if isinstance(trigger_results, list):
        trigger_fired = sum(1 for x in trigger_results if isinstance(x, dict) and bool(x.get("fired")))

    # Re-read parcel details and recalculate missing fields.
    after_missing: dict[str, list[str]] = {}
    worst: list[tuple[str, int, list[str]]] = []
    missing_counter = Counter()

    for rec in sample:
        pid = str(rec.get("parcel_id") or "").strip()
        if not pid:
            continue
        d = _detail(pid, COUNTY)
        missing = _missing_fields_for_record(d)
        after_missing[pid] = missing
        for f in missing:
            missing_counter[f] += 1
        worst.append((pid, len(missing), missing))

    worst.sort(key=lambda x: (-x[1], x[0]))

    total_checked = len(after_missing)
    fully_complete = sum(1 for _pid, _cnt, miss in worst if len(miss) == 0)
    partial = total_checked - fully_complete

    ended = datetime.now(timezone.utc).isoformat()

    report_obj = {
        "started_at": started,
        "ended_at": ended,
        "county": COUNTY,
        "checked": total_checked,
        "fully_complete": fully_complete,
        "partial": partial,
        "enrich_calls": enrich_calls,
        "enrich_ok": enrich_ok,
        "owner_calls": owner_calls,
        "owner_ok": owner_ok,
        "trigger_status": trigger_status,
        "trigger_fired": trigger_fired,
        "missing_by_field": dict(sorted(missing_counter.items())),
        "worst_parcels": [
            {"parcel_id": pid, "missing_count": cnt, "missing_fields": miss}
            for pid, cnt, miss in worst[:25]
        ],
    }

    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(report_obj, indent=2), encoding="utf-8")

    lines = [
        f"Property Completeness Recheck ({COUNTY})",
        f"Started: {started}",
        f"Ended:   {ended}",
        "",
        f"Checked parcels: {total_checked}",
        f"Fully complete: {fully_complete}",
        f"Still partial:  {partial}",
        "",
        "Backfill actions",
        f"- /api/parcels/enrich batches: {enrich_calls} (ok {enrich_ok})",
        f"- /api/owners/enrich calls:    {owner_calls} (ok {owner_ok})",
        f"- /api/triggers/evaluate:      HTTP {trigger_status}, fired={trigger_fired}",
        "",
        "Missing fields after backfill",
    ]

    if missing_counter:
        for field, cnt in sorted(missing_counter.items(), key=lambda kv: (-kv[1], kv[0])):
            pct = round((cnt / total_checked) * 100.0, 2) if total_checked else 0.0
            lines.append(f"- {field}: {cnt}/{total_checked} ({pct}%)")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("Worst parcels after backfill")
    for pid, cnt, miss in worst[:25]:
        lines.append(f"- {pid}: missing={cnt} fields={','.join(miss) if miss else 'none'}")

    lines.append("")
    lines.append(f"JSON report: {REPORT_JSON}")
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({"ok": True, "report": str(REPORT_PATH), "json": str(REPORT_JSON), "checked": total_checked, "fully_complete": fully_complete}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
