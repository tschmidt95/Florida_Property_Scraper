"""Permits API endpoints."""

try:
    from fastapi import APIRouter, Body, HTTPException

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    Body = None
    HTTPException = None
    FASTAPI_AVAILABLE = False

import os
from pathlib import Path
from typing import Optional

from pydantic import BaseModel


router = APIRouter(tags=["permits"]) if FASTAPI_AVAILABLE else None


class PermitSyncRequest(BaseModel):
    county: str
    query: str
    limit: int = 50


class PermitResponse(BaseModel):
    county: str
    parcel_id: Optional[str] = None
    address: Optional[str] = None
    permit_number: str
    permit_type: Optional[str] = None
    status: Optional[str] = None
    issue_date: Optional[str] = None
    final_date: Optional[str] = None
    description: Optional[str] = None
    source: str


def _get_db_path() -> Path:
    """Get the database path from environment variables."""
    _DB_ENV_VARS = ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB")
    for name in _DB_ENV_VARS:
        value = os.getenv(name)
        if value:
            return Path(value)
    return Path("./leads.sqlite")


if router:

    @router.post("/permits/sync", response_model=list[PermitResponse])
    def sync_permits(request: PermitSyncRequest = Body(...)) -> list[PermitResponse]:
        """
        Sync permits from county portal (requires LIVE=1).

        This endpoint fetches permits from the county building permit portal,
        stores them in the database, and returns the list of permits found.

        Requires LIVE=1 environment variable to be set to enable network requests.

        Args:
            request: PermitSyncRequest with county, query, and limit

        Returns:
            List of PermitResponse objects

        Raises:
            HTTPException: 400 if LIVE!=1, 404 if county not supported, 500 on errors
        """
        # Check LIVE flag
        if os.environ.get("LIVE") != "1":
            raise HTTPException(
                status_code=400,
                detail="Live permit scraping requires LIVE=1 environment variable. "
                "Set LIVE=1 to enable network requests to building permit portals.",
            )

        # Get scraper for county
        try:
            from florida_property_scraper.permits.registry import get_scraper
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="Permits module not available",
            )

        try:
            scraper = get_scraper(request.county)
        except ValueError as e:
            raise HTTPException(
                status_code=404,
                detail=str(e),
            )

        # Fetch permits
        try:
            permit_records = scraper.search_permits(
                query=request.query, limit=request.limit
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to fetch permits: {str(e)}",
            )

        # Store permits in database
        if permit_records:
            try:
                from florida_property_scraper.storage import SQLiteStore

                db_path = _get_db_path()
                store = SQLiteStore(str(db_path))
                try:
                    # Convert PermitRecord objects to dicts
                    permit_dicts = [
                        {
                            "county": p.county,
                            "parcel_id": p.parcel_id,
                            "address": p.address,
                            "permit_number": p.permit_number,
                            "permit_type": p.permit_type,
                            "status": p.status,
                            "issue_date": p.issue_date,
                            "final_date": p.final_date,
                            "description": p.description,
                            "source": p.source,
                            "raw": p.raw,
                        }
                        for p in permit_records
                    ]
                    store.upsert_many_permits(permit_dicts)
                finally:
                    store.close()
            except Exception as e:
                # Log error but still return the permits we fetched
                import warnings

                warnings.warn(f"Failed to store permits in database: {e}")

        # Return permits as response
        return [
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
            for p in permit_records
        ]
