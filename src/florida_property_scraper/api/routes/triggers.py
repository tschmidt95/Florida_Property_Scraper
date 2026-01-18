from __future__ import annotations

import os
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from florida_property_scraper.storage import SQLiteStore


router = APIRouter(tags=["triggers"])


_DB_ENV_VARS = ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB")


def _get_db_path() -> Path:
    for name in _DB_ENV_VARS:
        value = os.getenv(name)
        if value:
            return Path(value)
    return Path("./leads.sqlite")


class TriggerEventOut(BaseModel):
    id: int
    county: str
    parcel_id: str
    trigger_key: str
    trigger_at: str
    severity: int
    source_connector_key: str
    source_event_type: str
    source_event_id: int | None = None
    details_json: str


class TriggerAlertOut(BaseModel):
    id: int
    county: str
    parcel_id: str
    alert_key: str
    severity: int
    first_seen_at: str
    last_seen_at: str
    status: str
    trigger_event_ids_json: str
    details_json: str


class TriggersByParcelResponse(BaseModel):
    county: str
    parcel_id: str
    trigger_events: list[TriggerEventOut]
    alerts: list[TriggerAlertOut]


@router.get("/triggers/events/by_parcel", response_model=list[TriggerEventOut])
def trigger_events_by_parcel(
    county: str,
    parcel_id: str,
    limit: int = 100,
) -> list[TriggerEventOut]:
    county_key = (county or "").strip().lower()
    pid = (parcel_id or "").strip()
    if not county_key:
        raise HTTPException(status_code=400, detail="county is required")
    if not pid:
        raise HTTPException(status_code=400, detail="parcel_id is required")

    db_path = _get_db_path()
    store = SQLiteStore(str(db_path))
    try:
        events = store.list_trigger_events_for_parcel(county=county_key, parcel_id=pid, limit=int(limit))
    finally:
        store.close()
    return [TriggerEventOut(**e) for e in events]


@router.get("/triggers/alerts/by_parcel", response_model=list[TriggerAlertOut])
def trigger_alerts_by_parcel(
    county: str,
    parcel_id: str,
    status: str = "open",
    limit: int = 50,
) -> list[TriggerAlertOut]:
    county_key = (county or "").strip().lower()
    pid = (parcel_id or "").strip()
    if not county_key:
        raise HTTPException(status_code=400, detail="county is required")
    if not pid:
        raise HTTPException(status_code=400, detail="parcel_id is required")

    db_path = _get_db_path()
    store = SQLiteStore(str(db_path))
    try:
        alerts = store.list_trigger_alerts_for_parcel(
            county=county_key,
            parcel_id=pid,
            status=status,
            limit=int(limit),
        )
    finally:
        store.close()
    return [TriggerAlertOut(**a) for a in alerts]


@router.get("/triggers/by_parcel", response_model=TriggersByParcelResponse)
def triggers_by_parcel(
    county: str,
    parcel_id: str,
    limit_events: int = 100,
    limit_alerts: int = 50,
    status: str = "open",
) -> TriggersByParcelResponse:
    county_key = (county or "").strip().lower()
    pid = (parcel_id or "").strip()
    if not county_key:
        raise HTTPException(status_code=400, detail="county is required")
    if not pid:
        raise HTTPException(status_code=400, detail="parcel_id is required")

    db_path = _get_db_path()
    store = SQLiteStore(str(db_path))
    try:
        events = store.list_trigger_events_for_parcel(county=county_key, parcel_id=pid, limit=limit_events)
        alerts = store.list_trigger_alerts_for_parcel(
            county=county_key,
            parcel_id=pid,
            status=status,
            limit=limit_alerts,
        )
    finally:
        store.close()

    return TriggersByParcelResponse(
        county=county_key,
        parcel_id=pid,
        trigger_events=[TriggerEventOut(**e) for e in events],
        alerts=[TriggerAlertOut(**a) for a in alerts],
    )


class TriggersByParcelsRequest(BaseModel):
    county: str
    parcel_ids: list[str]
    limit_events: int = 50
    limit_alerts: int = 10
    status: str = "open"


@router.post("/triggers/by_parcels")
def triggers_by_parcels(payload: TriggersByParcelsRequest) -> dict:
    county_key = (payload.county or "").strip().lower()
    if not county_key:
        raise HTTPException(status_code=400, detail="county is required")

    parcel_ids = [str(p or "").strip() for p in (payload.parcel_ids or [])]
    parcel_ids = [p for p in parcel_ids if p]
    if not parcel_ids:
        return {"county": county_key, "parcels": {}}

    db_path = _get_db_path()
    store = SQLiteStore(str(db_path))
    try:
        out: dict[str, dict] = {}
        for pid in parcel_ids[:200]:
            events = store.list_trigger_events_for_parcel(
                county=county_key,
                parcel_id=pid,
                limit=int(payload.limit_events),
            )
            alerts = store.list_trigger_alerts_for_parcel(
                county=county_key,
                parcel_id=pid,
                status=payload.status,
                limit=int(payload.limit_alerts),
            )
            out[pid] = {"trigger_events": events, "alerts": alerts}
        return {"county": county_key, "parcels": out}
    finally:
        store.close()


class TriggerRollupOut(BaseModel):
    county: str
    parcel_id: str
    rebuilt_at: str
    last_seen_any: str | None = None
    last_seen_permits: str | None = None
    last_seen_tax: str | None = None
    last_seen_official_records: str | None = None
    last_seen_code_enforcement: str | None = None
    last_seen_courts: str | None = None
    last_seen_gis_planning: str | None = None
    has_permits: int
    has_tax: int
    has_official_records: int
    has_code_enforcement: int
    has_courts: int
    has_gis_planning: int
    count_critical: int
    count_strong: int
    count_support: int
    seller_score: int
    details_json: str


@router.get("/triggers/rollups/by_parcel", response_model=TriggerRollupOut)
def rollups_by_parcel(county: str, parcel_id: str) -> TriggerRollupOut:
    county_key = (county or "").strip().lower()
    pid = (parcel_id or "").strip()
    if not county_key:
        raise HTTPException(status_code=400, detail="county is required")
    if not pid:
        raise HTTPException(status_code=400, detail="parcel_id is required")

    db_path = _get_db_path()
    store = SQLiteStore(str(db_path))
    try:
        row = store.get_rollup_for_parcel(county=county_key, parcel_id=pid)
    finally:
        store.close()

    if not row:
        raise HTTPException(status_code=404, detail="rollup not found")
    return TriggerRollupOut(**row)


class RollupsSearchRequest(BaseModel):
    county: str

    # Polygon/radius inputs mirror /api/parcels/search.
    polygon_geojson: dict | None = None
    radius: dict | None = None
    center: dict | None = None
    radius_m: float | None = None

    # Filters
    min_score: int | None = None
    any_groups: list[str] | None = None
    trigger_groups: list[str] | None = None
    trigger_keys: list[str] | None = None
    tiers: list[str] | None = None

    # Pagination
    limit: int = 250
    offset: int = 0


class RollupsSearchResponse(BaseModel):
    county: str
    candidate_count: int
    returned_count: int
    parcel_ids: list[str]
    rollups: list[TriggerRollupOut]


def _local_parcel_ids_for_geometry(*, county: str, payload: RollupsSearchRequest) -> list[str]:
    from florida_property_scraper.parcels.geometry_search import (
        circle_polygon,
        geometry_bbox,
        intersects,
    )

    county_key = (county or "").strip().lower()
    geometry = payload.polygon_geojson
    if geometry is None:
        geometry = None

    if payload.radius_m is not None or payload.center is not None:
        if not isinstance(payload.center, dict) or not isinstance(payload.radius_m, (int, float)):
            raise HTTPException(
                status_code=400,
                detail="radius search must be {center:{lat,lng}, radius_m:number}",
            )
        lat = payload.center.get("lat")
        lng = payload.center.get("lng")
        if not isinstance(lat, (int, float)) or not isinstance(lng, (int, float)):
            raise HTTPException(status_code=400, detail="center must be {lat:number, lng:number}")
        miles = float(payload.radius_m) / 1609.344
        geometry = circle_polygon(center_lon=float(lng), center_lat=float(lat), miles=miles)
    elif payload.radius is not None:
        if not isinstance(payload.radius, dict):
            raise HTTPException(status_code=400, detail="radius must be an object")
        center = payload.radius.get("center")
        miles = payload.radius.get("miles")
        if (
            not isinstance(center, (list, tuple))
            or len(center) != 2
            or not isinstance(center[0], (int, float))
            or not isinstance(center[1], (int, float))
            or not isinstance(miles, (int, float))
        ):
            raise HTTPException(status_code=400, detail="radius must be {center:[lng,lat], miles:number}")
        geometry = circle_polygon(center_lon=float(center[0]), center_lat=float(center[1]), miles=float(miles))

    if geometry is None:
        raise HTTPException(status_code=400, detail="Provide polygon_geojson or radius")
    if not isinstance(geometry, dict):
        raise HTTPException(status_code=400, detail="geometry must be a GeoJSON geometry object")
    bbox_t = geometry_bbox(geometry)
    if bbox_t is None:
        raise HTTPException(status_code=400, detail="geometry has no coordinates")

    # Enforce offline/deterministic providers here (ignore FDOR live provider).
    if county_key not in {"orange", "seminole"}:
        raise HTTPException(status_code=400, detail=f"geometry rollup search unsupported for county: {county_key}")

    from florida_property_scraper.parcels.geometry_registry import _default_geojson_dir
    from florida_property_scraper.parcels.providers.orange import OrangeProvider
    from florida_property_scraper.parcels.providers.seminole import SeminoleProvider

    geo_dir = _default_geojson_dir()
    if county_key == "orange":
        provider = OrangeProvider(geojson_path=geo_dir / "orange.geojson")
    else:
        provider = SeminoleProvider(geojson_path=geo_dir / "seminole.geojson")
    provider.load()

    candidates = provider.query(bbox_t)
    intersecting = [f for f in candidates if intersects(geometry, f.geometry)]
    # Keep IN-clause bounded
    return [f.parcel_id for f in intersecting[:2000]]


@router.post("/triggers/rollups/search", response_model=RollupsSearchResponse)
def rollups_search(payload: RollupsSearchRequest) -> RollupsSearchResponse:
    county_key = (payload.county or "").strip().lower()
    if not county_key:
        raise HTTPException(status_code=400, detail="county is required")

    candidate_ids = _local_parcel_ids_for_geometry(county=county_key, payload=payload)

    db_path = _get_db_path()
    store = SQLiteStore(str(db_path))
    try:
        groups = set((payload.any_groups or []))
        groups |= set((payload.trigger_groups or []))
        effective_groups = sorted(g for g in groups if str(g or "").strip())

        keys = set((payload.trigger_keys or []))
        effective_keys = sorted(k for k in keys if str(k or "").strip())

        rows = store.search_rollups(
            county=county_key,
            parcel_ids=candidate_ids,
            min_score=payload.min_score,
            require_any_groups=effective_groups or None,
            require_trigger_keys=effective_keys or None,
            require_tiers=payload.tiers,
            limit=int(payload.limit),
            offset=int(payload.offset),
        )
    finally:
        store.close()

    parcel_ids = [str(r.get("parcel_id") or "") for r in rows if str(r.get("parcel_id") or "")]
    return RollupsSearchResponse(
        county=county_key,
        candidate_count=len(candidate_ids),
        returned_count=len(rows),
        parcel_ids=parcel_ids,
        rollups=[TriggerRollupOut(**r) for r in rows],
    )
