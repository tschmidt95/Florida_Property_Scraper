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
