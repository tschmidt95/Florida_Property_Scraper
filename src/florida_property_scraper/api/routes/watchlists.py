from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from florida_property_scraper.storage import SQLiteStore

router = APIRouter(tags=["watchlists"])


_DB_ENV_VARS = ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB")


def _get_db_path() -> str:
    for name in _DB_ENV_VARS:
        value = os.getenv(name)
        if value:
            return value
    return "./leads.sqlite"


class CreateWatchlistBody(BaseModel):
    name: str = Field(default="Watchlist")
    county: str


class CreateSavedSearchBody(BaseModel):
    name: str = Field(default="Saved Search")
    county: str
    geometry: Dict[str, Any]
    filters: Dict[str, Any] = Field(default_factory=dict)
    enrich: bool = False
    sort: Optional[str] = None
    watchlist_id: Optional[str] = None


class AddMemberBody(BaseModel):
    county: str
    parcel_id: str
    source: str = "manual"


class RefreshWatchlistBody(BaseModel):
    connectors: Optional[List[str]] = None
    connector_limit: int = 50



@router.get("/watchlists")
def list_watchlists(county: Optional[str] = None) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        return {"ok": True, "watchlists": store.list_watchlists(county=county)}
    finally:
        store.close()


@router.post("/watchlists")
def create_watchlist(body: CreateWatchlistBody) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        w = store.create_watchlist(name=body.name, county=body.county)
        return {"ok": True, "watchlist": w}
    finally:
        store.close()


@router.get("/watchlists/{watchlist_id}")
def get_watchlist(watchlist_id: str) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        w = store.get_watchlist(watchlist_id=watchlist_id)
        if not w:
            raise HTTPException(status_code=404, detail="watchlist not found")
        members = store.list_watchlist_members(watchlist_id=watchlist_id)
        return {"ok": True, "watchlist": w, "members": members}
    finally:
        store.close()


@router.post("/watchlists/{watchlist_id}/members")
def add_watchlist_member(watchlist_id: str, body: AddMemberBody) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        ok = store.add_parcel_to_watchlist(
            watchlist_id=watchlist_id,
            county=body.county,
            parcel_id=body.parcel_id,
            source=body.source,
        )
        if not ok:
            raise HTTPException(status_code=400, detail="invalid member")
        return {"ok": True}
    finally:
        store.close()


@router.get("/saved-searches")
def list_saved_searches(county: Optional[str] = None) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        return {"ok": True, "saved_searches": store.list_saved_searches(county=county)}
    finally:
        store.close()


@router.post("/saved-searches")
def create_saved_search(body: CreateSavedSearchBody) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        ss = store.create_saved_search(
            name=body.name,
            county=body.county,
            polygon_geojson=body.geometry,
            filters=body.filters,
            enrich=body.enrich,
            sort=body.sort,
            watchlist_id=body.watchlist_id,
        )
        return {"ok": True, "saved_search": ss}
    finally:
        store.close()


@router.post("/saved-searches/{saved_search_id}/run")
def run_saved_search(saved_search_id: str, limit: int = 2000) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        res = store.run_saved_search(saved_search_id=saved_search_id, limit=limit)
        if not res.get("ok"):
            raise HTTPException(status_code=400, detail=res.get("error") or "run failed")
        return res
    finally:
        store.close()


@router.post("/saved-searches/{saved_search_id}/members")
def add_saved_search_member(saved_search_id: str, body: AddMemberBody) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        ok = store.add_member_to_saved_search(
            saved_search_id=saved_search_id,
            county=body.county,
            parcel_id=body.parcel_id,
            source=body.source,
        )
        if not ok:
            raise HTTPException(status_code=400, detail="invalid member")
        return {"ok": True}
    finally:
        store.close()


@router.post("/watchlists/{watchlist_id}/sync-alerts")
def sync_watchlist_alerts(watchlist_id: str) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        res = store.sync_watchlist_inbox_from_trigger_alerts(watchlist_id=watchlist_id)
        if not res.get("ok"):
            raise HTTPException(status_code=400, detail=res.get("error") or "sync failed")
        return res
    finally:
        store.close()


@router.post("/watchlists/{watchlist_id}/refresh")
def refresh_watchlist(watchlist_id: str, body: RefreshWatchlistBody) -> Dict[str, Any]:
    from florida_property_scraper.triggers.engine import run_connector_once
    from florida_property_scraper.triggers.connectors.base import list_connectors, get_connector

    # Ensure builtin connectors are registered.
    import florida_property_scraper.triggers.connectors  # noqa: F401

    store = SQLiteStore(_get_db_path())
    try:
        now = store._utc_now_iso()
        wl = store.get_watchlist(watchlist_id=watchlist_id)
        if not wl:
            raise HTTPException(status_code=404, detail="watchlist not found")
        county = str(wl.get("county") or "").strip().lower()
        if not county:
            raise HTTPException(status_code=400, detail="watchlist county missing")

        available = [k for k in list_connectors() if k != "fake"]
        requested = [str(k or "").strip().lower() for k in (body.connectors or [])]
        requested = [k for k in requested if k]
        connector_keys = requested or available

        results = []
        for ck in connector_keys:
            if ck not in available:
                continue
            connector = get_connector(ck)
            results.append(
                run_connector_once(
                    store=store,
                    connector=connector,
                    county=county,
                    now_iso=now,
                    limit=int(body.connector_limit or 50),
                )
            )

        rollups = store.rebuild_parcel_trigger_rollups(county=county, rebuilt_at=now)
        inbox = store.sync_watchlist_inbox_from_trigger_alerts(watchlist_id=watchlist_id)
        if not inbox.get("ok"):
            raise HTTPException(status_code=400, detail=inbox.get("error") or "inbox sync failed")
        return {
            "ok": True,
            "watchlist_id": watchlist_id,
            "county": county,
            "connectors_ran": len(results),
            "connector_results": results,
            "rollups": rollups,
            "inbox": inbox,
        }
    finally:
        store.close()


@router.get("/alerts")
def list_alerts(
    saved_search_id: Optional[str] = None,
    county: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 200,
) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        alerts = store.list_alerts(
            saved_search_id=saved_search_id,
            county=county,
            status=status,
            limit=limit,
        )
        return {"ok": True, "alerts": alerts}
    finally:
        store.close()


@router.post("/alerts/{alert_id}/read")
def mark_alert_read(alert_id: int) -> Dict[str, Any]:
    store = SQLiteStore(_get_db_path())
    try:
        ok = store.mark_alert_read(alert_id=alert_id)
        if not ok:
            raise HTTPException(status_code=400, detail="invalid alert id")
        return {"ok": True}
    finally:
        store.close()
