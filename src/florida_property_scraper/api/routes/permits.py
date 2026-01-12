from __future__ import annotations

try:
    from fastapi import APIRouter
    from fastapi import HTTPException

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover
    APIRouter = None
    HTTPException = None
    FASTAPI_AVAILABLE = False

from pydantic import BaseModel

from florida_property_scraper.leads_db import open_conn
from florida_property_scraper.permits_db import ensure_permits_schema
from florida_property_scraper.permits_db import upsert_permits
from florida_property_scraper.permits_models import PermitRecord
from florida_property_scraper.scrapers.permits_registry import get_permits_scraper


router = APIRouter(tags=["permits"]) if FASTAPI_AVAILABLE else None


class PermitsSyncRequest(BaseModel):
    county: str
    parcel_ids: list[str]


def _normalize_parcel_ids(parcel_ids: list[str]) -> list[str]:
    out: list[str] = []
    for pid in parcel_ids:
        if not pid:
            continue
        clean = str(pid).strip()
        if clean:
            out.append(clean)
        if len(out) >= 200:
            break
    return out


if router:

    @router.post("/permits/sync", response_model=list[PermitRecord])
    def sync_permits(req: PermitsSyncRequest) -> list[PermitRecord]:
        county = (req.county or "").strip()
        if not county:
            raise HTTPException(status_code=400, detail="county is required")

        parcel_ids = _normalize_parcel_ids(req.parcel_ids or [])
        if not parcel_ids:
            raise HTTPException(status_code=400, detail="parcel_ids is required")

        scraper = get_permits_scraper(county)
        if scraper is None:
            raise HTTPException(
                status_code=400,
                detail=f"No permits scraper available for county '{county}'.",
            )

        permits: list[PermitRecord] = []
        for pid in parcel_ids:
            permits.extend(scraper.fetch_permits(parcel_id=pid))

        with open_conn() as conn:
            ensure_permits_schema(conn)
            upsert_permits(conn, permits)

        return permits
