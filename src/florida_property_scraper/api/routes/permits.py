"""Permits API endpoints."""
try:
    from fastapi import APIRouter, HTTPException

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    HTTPException = None
    FASTAPI_AVAILABLE = False

import os
from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel


router = APIRouter(tags=["permits"]) if FASTAPI_AVAILABLE else None


class PermitsSyncRequest(BaseModel):
    county: str
    query: str
    limit: int = 100


class PermitResponse(BaseModel):
    county: str
    parcel_id: str | None
    address: str | None
    permit_number: str
    permit_type: str | None
    status: str | None
    issue_date: str | None
    final_date: str | None
    description: str | None
    source: str


class PermitsSyncResponse(BaseModel):
    county: str
    query: str
    count: int
    permits: List[PermitResponse]


_DB_ENV_VARS = ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB")


def _get_db_path() -> Path:
    for name in _DB_ENV_VARS:
        value = os.getenv(name)
        if value:
            return Path(value)
    return Path("./leads.sqlite")


if router:

    @router.post("/permits/sync", response_model=PermitsSyncResponse)
    def permits_sync(request: PermitsSyncRequest) -> PermitsSyncResponse:
        """Sync permits from county portal.

        LIVE-gated: requires LIVE=1 environment variable.

        Args:
            request: County, query, and limit

        Returns:
            Synced permits list

        Raises:
            HTTPException: If LIVE!=1 or scraper not available
        """
        if os.getenv("LIVE") != "1":
            raise HTTPException(
                status_code=400,
                detail="Live permit sync requires LIVE=1 environment variable",
            )

        from florida_property_scraper.permits.registry import get_scraper
        from florida_property_scraper.storage import SQLiteStore

        county = request.county.lower().strip()
        scraper = get_scraper(county)

        if not scraper:
            raise HTTPException(
                status_code=404,
                detail=f"Permits scraper not available for county: {county}",
            )

        try:
            # Fetch permits from portal
            permits = scraper.search_permits(request.query, limit=request.limit)

            # Store in database
            db_path = _get_db_path()
            store = SQLiteStore(str(db_path))
            try:
                permit_dicts = [p.to_dict() for p in permits]
                store.upsert_many_permits(permit_dicts)
            finally:
                store.close()

            # Convert to response model
            permit_responses = [
                PermitResponse(
                    county=p.county,
                    parcel_id=p.parcel_id,
                    address=p.address,
                    permit_number=p.permit_number,
                    permit_type=p.permit_type,
                    status=p.status,
                    issue_date=p.issue_date,
                    final_date=p.final_date,
                    description=p.description,
                    source=p.source,
                )
                for p in permits
            ]

            return PermitsSyncResponse(
                county=county,
                query=request.query,
                count=len(permits),
                permits=permit_responses,
            )

        except RuntimeError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error syncing permits: {str(e)}"
            )
