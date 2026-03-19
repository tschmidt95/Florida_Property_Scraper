#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8000").rstrip("/")
COUNTY = os.getenv("COUNTY", "seminole").strip().lower()
LEADS_DB = Path(os.getenv("LEADS_SQLITE_PATH", "/workspaces/Florida_Property_Scraper/leads.sqlite"))
MAX_PARCELS = int(os.getenv("MAX_PARCELS", "300"))
OWNER_ENRICH_LIMIT = int(os.getenv("OWNER_ENRICH_LIMIT", "200"))
BATCH_SIZE = int(os.getenv("ENRICH_BATCH_SIZE", "50"))

REPORT_PATH = Path("/workspaces/Florida_Property_Scraper/PROOF_DATA_COMPLETENESS_REPORT.txt")
REPORT_JSON = Path("/workspaces/Florida_Property_Scraper/data/data_completeness_report.json")


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
            try:
                data = json.loads(raw) if raw.strip() else {}
            except Exception:
                data = {"_raw": raw}
            return int(resp.status), data
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(raw) if raw.strip() else {}
        except Exception:
            data = {"_raw": raw}
        return int(e.code), data


def _county_center(county: str) -> tuple[float, float, int]:
    centers = {
        "seminole": (28.71, -81.30, 22000),
        "orange": (28.54, -81.37, 26000),
        "duval": (30.33, -81.65, 30000),
        "hillsborough": (27.98, -82.35, 30000),
    }
    return centers.get(county, (28.71, -81.30, 25000))


def _get_parcel_ids(county: str, max_rows: int) -> list[str]:
    if not LEADS_DB.exists():
        return []
    con = sqlite3.connect(str(LEADS_DB))
    try:
        cur = con.cursor()
        rows = cur.execute(
            "SELECT parcel_id FROM pa_properties WHERE lower(county)=? AND trim(parcel_id)<>'' LIMIT ?",
            (county, max_rows),
        ).fetchall()
        return [str(r[0]).strip() for r in rows if str(r[0] or "").strip()]
    finally:
        con.close()


def _coverage(records: list[dict[str, Any]], fields: list[str]) -> dict[str, Any]:
    total = len(records)
    out: dict[str, Any] = {"total": total, "fields": {}}
    for f in fields:
        present = 0
        for r in records:
            v = r.get(f)
            ok = not (
                v is None
                or (isinstance(v, str) and not v.strip())
                or (isinstance(v, (int, float)) and float(v) == 0.0 and f not in {"beds", "baths"})
            )
            if ok:
                present += 1
        pct = round((present / total) * 100.0, 2) if total else 0.0
        out["fields"][f] = {"present": present, "pct": pct}
    return out


def main() -> int:
    started = datetime.now(timezone.utc).isoformat()

    # 1) Provider configuration/status audit.
    status_code, provider_status = _http_json("GET", f"/api/debug/provider_status?county={urllib.parse.quote(COUNTY)}")
    owner_provider = str(os.getenv("OWNER_ENRICH_PROVIDER", "")).strip() or "not_set"
    pdl_configured = bool(str(os.getenv("PDL_API_KEY", "")).strip())

    # 2) Bulk enrichment run.
    parcel_ids = _get_parcel_ids(COUNTY, MAX_PARCELS)
    batches = [parcel_ids[i : i + BATCH_SIZE] for i in range(0, len(parcel_ids), BATCH_SIZE)]

    enrich_ok = 0
    enrich_errors: list[str] = []
    total_evidence = 0
    total_enriched = 0

    provider_keys = ["seminole_official_records"] if COUNTY == "seminole" else None

    for idx, batch in enumerate(batches, start=1):
        payload: dict[str, Any] = {
            "county": COUNTY,
            "parcel_ids": batch,
            "fixture_mode": COUNTY == "seminole",
        }
        if provider_keys:
            payload["provider_keys"] = provider_keys

        code, data = _http_json("POST", "/api/enrich", payload=payload, timeout=120)
        if code == 200 and bool(data.get("ok")):
            enrich_ok += 1
            total_evidence += len(data.get("evidence") or [])
            total_enriched += len(data.get("enriched") or [])
        else:
            enrich_errors.append(f"batch {idx}: http {code} {str(data)[:220]}")
        time.sleep(0.1)

    # 3) Owner enrichment prefill + completeness report.
    owner_ok = 0
    owner_fail = 0
    owner_with_phone = 0
    owner_with_email = 0
    owner_checked = 0

    for pid in parcel_ids[:OWNER_ENRICH_LIMIT]:
        owner_checked += 1
        code, data = _http_json(
            "GET",
            f"/api/owners/enrich?county={urllib.parse.quote(COUNTY)}&parcel_id={urllib.parse.quote(pid)}",
            timeout=45,
        )
        if code == 200:
            owner_ok += 1
            phones = data.get("phones") or []
            emails = data.get("emails") or []
            if phones:
                owner_with_phone += 1
            if emails:
                owner_with_email += 1
        else:
            owner_fail += 1
        time.sleep(0.03)

    lat, lng, radius_m = _county_center(COUNTY)
    search_payload = {
        "county": COUNTY,
        "center": {"lat": lat, "lng": lng},
        "radius_m": radius_m,
        "limit": 500,
        "include_geometry": False,
        "filters": {"missing_policy": "lenient"},
    }
    scode, sdata = _http_json("POST", "/api/parcels/search", payload=search_payload, timeout=120)
    records = sdata.get("records") if isinstance(sdata, dict) else []
    if not isinstance(records, list):
        records = []

    fields = [
        "owner_name",
        "situs_address",
        "owner_mailing_address",
        "living_area_sqft",
        "lot_size_sqft",
        "beds",
        "baths",
        "year_built",
        "zoning",
        "future_land_use",
        "last_sale_date",
        "last_sale_price",
        "land_value",
        "building_value",
        "total_value",
        "assessed_value",
        "taxable_value",
        "photo_url",
    ]
    coverage = _coverage(records, fields)

    ended = datetime.now(timezone.utc).isoformat()
    report = {
        "started_at": started,
        "ended_at": ended,
        "county": COUNTY,
        "provider_status_http": status_code,
        "provider_status": provider_status,
        "owner_provider_env": owner_provider,
        "pdl_api_key_configured": pdl_configured,
        "bulk_enrichment": {
            "parcel_ids_loaded": len(parcel_ids),
            "batches": len(batches),
            "batches_ok": enrich_ok,
            "total_evidence": total_evidence,
            "total_enriched": total_enriched,
            "errors": enrich_errors,
        },
        "owner_enrichment": {
            "checked": owner_checked,
            "ok": owner_ok,
            "failed": owner_fail,
            "with_phone": owner_with_phone,
            "with_email": owner_with_email,
            "phone_hit_rate_pct": round((owner_with_phone / owner_checked) * 100.0, 2) if owner_checked else 0.0,
            "email_hit_rate_pct": round((owner_with_email / owner_checked) * 100.0, 2) if owner_checked else 0.0,
        },
        "search_http": scode,
        "search_count": len(records),
        "field_coverage": coverage,
    }

    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        f"Data Completeness Report ({COUNTY})",
        f"Started: {started}",
        f"Ended:   {ended}",
        "",
        "Provider Configuration",
        f"- OWNER_ENRICH_PROVIDER: {owner_provider}",
        f"- PDL_API_KEY configured: {pdl_configured}",
        f"- /api/debug/provider_status HTTP: {status_code}",
        "",
        "Bulk Enrichment",
        f"- Parcel IDs loaded: {len(parcel_ids)}",
        f"- Batches: {len(batches)} (ok {enrich_ok})",
        f"- Total evidence rows: {total_evidence}",
        f"- Total enriched parcels reported: {total_enriched}",
    ]
    if enrich_errors:
        lines.append("- Errors:")
        for e in enrich_errors[:10]:
            lines.append(f"  - {e}")
    else:
        lines.append("- Errors: none")

    lines.extend(
        [
            "",
            "Owner Contact Enrichment",
            f"- Checked parcels: {owner_checked}",
            f"- API OK: {owner_ok} / Fail: {owner_fail}",
            f"- With phones: {owner_with_phone} ({report['owner_enrichment']['phone_hit_rate_pct']}%)",
            f"- With emails: {owner_with_email} ({report['owner_enrichment']['email_hit_rate_pct']}%)",
            "",
            "Search Field Coverage (returned records)",
            f"- Search records evaluated: {len(records)}",
        ]
    )

    for f, meta in coverage.get("fields", {}).items():
        lines.append(f"- {f}: {meta['pct']}% ({meta['present']}/{coverage.get('total', 0)})")

    lines.append("")
    lines.append(f"Raw JSON report: {REPORT_JSON}")
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "ok": True,
        "county": COUNTY,
        "parcel_ids": len(parcel_ids),
        "search_count": len(records),
        "report_txt": str(REPORT_PATH),
        "report_json": str(REPORT_JSON),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
