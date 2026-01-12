try:
    from fastapi import APIRouter
    from fastapi import HTTPException

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    HTTPException = None
    FASTAPI_AVAILABLE = False

import os
from pathlib import Path

from pydantic import BaseModel

from florida_property_scraper.permits.registry import get_permits_scraper
from florida_property_scraper.storage import SQLiteStore


router = APIRouter(tags=["permits"]) if FASTAPI_AVAILABLE else None


_DB_ENV_VARS = ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB")


def _get_db_path() -> Path:
    for name in _DB_ENV_VARS:
        value = os.getenv(name)
        if value:
            return Path(value)
    return Path("./leads.sqlite")


def _is_live_enabled() -> bool:
    return os.environ.get("LIVE", "0") == "1"


class PermitsSyncRequest(BaseModel):
    county: str
    query: str
    limit: int = 25


class PermitsSyncResponseItem(BaseModel):
    county: str
    parcel_id: str | None = None
    address: str | None = None
    permit_number: str
    permit_type: str | None = None
    status: str | None = None
    issue_date: str | None = None
    final_date: str | None = None
    description: str | None = None
    source: str


if router:

    @router.post("/permits/sync", response_model=list[PermitsSyncResponseItem])
    def permits_sync(payload: PermitsSyncRequest) -> list[PermitsSyncResponseItem]:
        if not _is_live_enabled():
            raise HTTPException(
                status_code=400,
                detail="Live permits sync is disabled. Set LIVE=1 to enable network access.",
            )

        county_key = (payload.county or "").strip().lower()
        q = (payload.query or "").strip()
        try:
            lim = int(payload.limit)
        except Exception:
            lim = 25
        lim = max(1, min(lim, 200))

        scraper = get_permits_scraper(county_key)
        records = scraper.search_permits(q, lim)

        db_path = _get_db_path()
        store = SQLiteStore(str(db_path))
        try:
            store.upsert_many_permits(records)
        finally:
            store.close()

        return [
            PermitsSyncResponseItem(
                county=r.county,
                parcel_id=r.parcel_id,
                address=r.address,
                permit_number=r.permit_number,
                permit_type=r.permit_type,
                status=r.status,
                issue_date=r.issue_date,
                final_date=r.final_date,
                description=r.description,
                source=r.source,
            )
            for r in records
        ]
