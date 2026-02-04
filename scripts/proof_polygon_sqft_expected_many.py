import json
import os
import sqlite3
import sys
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

BASE = "http://127.0.0.1:8000"
COUNTY = "seminole"
LEADS_DB = "/workspaces/Florida_Property_Scraper/leads.sqlite"
PARCELS_DB = "/workspaces/Florida_Property_Scraper/data/parcels/parcels.sqlite"


def _parse_num(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v if v > 0 else None
    s = str(value).strip().lower()
    if not s or s in {"null", "none"}:
        return None
    s = s.replace(",", "")
    for token in ("sqft", "sq ft", "sf"):
        s = s.replace(token, "")
    num = ""
    for ch in s:
        if ch.isdigit() or ch == "." or (ch == "-" and not num):
            num += ch
    if not num:
        return None
    try:
        v = float(num)
    except Exception:
        return None
    return v if v > 0 else None


def _load_polygon() -> Dict[str, Any]:
    path = "/tmp/last_request.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            poly = payload.get("polygon_geojson") or payload.get("geometry")
            if isinstance(poly, dict) and poly.get("type") == "Polygon":
                return poly
        except Exception:
            pass
    stats = json.load(urllib.request.urlopen(f"{BASE}/api/debug/db_stats?county={COUNTY}", timeout=30))
    poly = stats.get("suggested_polygon")
    if not isinstance(poly, dict):
        raise SystemExit("FAIL: no polygon available")
    return poly


def _polygon_bbox(poly: Dict[str, Any]) -> Tuple[float, float, float, float]:
    coords = poly.get("coordinates") or []
    ring = coords[0] if coords and isinstance(coords[0], list) else []
    xs = [float(p[0]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
    ys = [float(p[1]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
    return min(xs), min(ys), max(xs), max(ys)


def _query_api(filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "polygon_geojson": POLY,
        "limit": 500,
        "include_geometry": False,
        "explain": True,
    }
    if filters:
        payload["filters"] = filters
    req = urllib.request.Request(
        f"{BASE}/api/parcels/search?explain=1",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return json.load(urllib.request.urlopen(req, timeout=60))


def _coverage_pct(resp: Dict[str, Any]) -> float:
    field_stats = resp.get("field_stats") or {}
    fields = field_stats.get("fields") if isinstance(field_stats, dict) else None
    if not isinstance(fields, dict):
        return 0.0
    pct = fields.get("living_area_sqft", {}).get("pct") if isinstance(fields.get("living_area_sqft"), dict) else 0.0
    try:
        return float(pct or 0.0)
    except Exception:
        return 0.0


def _expected_count() -> int:
    minx, miny, maxx, maxy = _polygon_bbox(POLY)

    try:
        from shapely.geometry import shape  # type: ignore
    except Exception:
        shape = None

    poly_shape = None
    if shape is not None:
        try:
            poly_shape = shape(POLY)
        except Exception:
            poly_shape = None

    con = sqlite3.connect(PARCELS_DB)
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT p.parcel_id, p.geom_geojson
        FROM parcels p
        JOIN parcels_rtree r ON r.rowid = p.rowid
        WHERE lower(p.county)=?
          AND r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
        """,
        (COUNTY, maxx, minx, maxy, miny),
    ).fetchall()
    con.close()

    geom_ids: List[str] = []
    for pid, geom_json in rows:
        if not pid:
            continue
        if poly_shape is None:
            geom_ids.append(str(pid))
            continue
        try:
            geom = json.loads(geom_json) if isinstance(geom_json, str) else geom_json
            if not isinstance(geom, dict):
                continue
            g = shape(geom)
            if g.intersects(poly_shape):
                geom_ids.append(str(pid))
        except Exception:
            continue

    if not geom_ids:
        return 0

    map_ids: Dict[str, str] = {}
    lcon = sqlite3.connect(LEADS_DB)
    lcur = lcon.cursor()
    try:
        rows = lcur.execute(
            "SELECT geom_parcel_id, pa_parcel_id FROM parcel_id_map WHERE lower(county)=?",
            (COUNTY,),
        ).fetchall()
        for gpid, ppid in rows:
            if gpid and ppid:
                map_ids[str(gpid)] = str(ppid)
    except Exception:
        pass

    mapped = [map_ids.get(pid, pid) for pid in geom_ids]
    mapped = [pid for pid in mapped if pid]

    expected = 0
    chunk = 900
    for i in range(0, len(mapped), chunk):
        batch = mapped[i : i + chunk]
        placeholders = ",".join(["?"] * len(batch))
        q = f"SELECT PARCEL, LIVING_AREA, TOTAL_SQFT FROM parcel_table1 WHERE PARCEL IN ({placeholders})"
        try:
            for row in lcur.execute(q, batch).fetchall():
                living = _parse_num(row[1]) or _parse_num(row[2])
                if living is not None and 2000 <= living <= 3000:
                    expected += 1
        except Exception:
            continue

    lcon.close()
    return expected


POLY = _load_polygon()

print({"polygon_loaded": True, "county": COUNTY})

resp_base = _query_api()
resp_filter = _query_api({"min_sqft": 2000, "max_sqft": 3000})
resp_lenient = _query_api({"min_sqft": 2000, "max_sqft": 3000, "missing_policy": "lenient"})

out = {
    "base": {
        "total_count": resp_base.get("total_count"),
        "returned_count": resp_base.get("returned_count"),
        "coverage_pct": _coverage_pct(resp_base),
    },
    "sqft_filter": {
        "total_count": resp_filter.get("total_count"),
        "returned_count": resp_filter.get("returned_count"),
        "coverage_pct": _coverage_pct(resp_filter),
    },
    "sqft_filter_lenient": {
        "total_count": resp_lenient.get("total_count"),
        "returned_count": resp_lenient.get("returned_count"),
        "coverage_pct": _coverage_pct(resp_lenient),
    },
}

print(json.dumps(out, indent=2))

expected = _expected_count()
summary = {
    "expected_count": expected,
    "api_counts": {
        "sqft_filter": int(resp_filter.get("returned_count") or 0),
        "sqft_filter_lenient": int(resp_lenient.get("returned_count") or 0),
    },
    "coverage_pct": {
        "sqft_filter": _coverage_pct(resp_filter),
        "sqft_filter_lenient": _coverage_pct(resp_lenient),
    },
}
print("SUMMARY " + json.dumps(summary))
