"""Permits API routes."""
import os
from pathlib import Path
from typing import Optional

try:
    from fastapi import APIRouter, HTTPException
    from pydantic import BaseModel

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    BaseModel = None
    HTTPException = None
    FASTAPI_AVAILABLE = False


router = APIRouter(tags=["permits"]) if FASTAPI_AVAILABLE else None


class PermitsSyncRequest(BaseModel):
    """Request model for permits sync endpoint."""

    county: str
    query: str
    limit: int = 50


class PermitsSyncResponse(BaseModel):
    """Response model for permits sync endpoint."""

    county: str
    count: int
    permits: list


def _get_db_path() -> Path:
    """Get database path from environment."""
    for name in ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB"):
        value = os.getenv(name)
        if value:
            return Path(value)
    return Path("./leads.sqlite")


if router:

    @router.post("/permits/sync", response_model=PermitsSyncResponse)
    def sync_permits(request: PermitsSyncRequest) -> PermitsSyncResponse:
        """Sync permits from county portal.

        This endpoint scrapes permits from the county portal and stores them in the database.
        Requires LIVE=1 environment variable to be set.

        Args:
            request: PermitsSyncRequest with county, query, and limit

        Returns:
            PermitsSyncResponse with count and permits list

        Raises:
            HTTPException: If LIVE=1 is not set or scraper not available
        """
        if os.getenv("LIVE") != "1":
            raise HTTPException(
                status_code=400,
                detail="Live permit scraping requires LIVE=1 environment variable to be set",
            )

        from florida_property_scraper.permits.registry import get_permits_scraper
        from florida_property_scraper.storage import SQLiteStore

        # Get scraper for county
        scraper = get_permits_scraper(request.county)
        if scraper is None:
            raise HTTPException(
                status_code=400,
                detail=f"No permits scraper available for county: {request.county}",
            )

        # Scrape permits
        try:
            permits = scraper.search_permits(request.query, limit=request.limit)
        except RuntimeError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Store permits
        db_path = _get_db_path()
        store = SQLiteStore(str(db_path))
        try:
            # Convert PermitRecord objects to dicts
            permit_dicts = []
            for permit in permits:
                # Truncate raw field
                truncated = permit.with_truncated_raw(max_len=10000)
                permit_dicts.append(
                    {
                        "county": truncated.county,
                        "parcel_id": truncated.parcel_id,
                        "address": truncated.address,
                        "permit_number": truncated.permit_number,
                        "permit_type": truncated.permit_type,
                        "status": truncated.status,
                        "issue_date": truncated.issue_date,
                        "final_date": truncated.final_date,
                        "description": truncated.description,
                        "source": truncated.source,
                        "raw": truncated.raw,
                    }
                )
            store.upsert_many_permits(permit_dicts)
        finally:
            store.close()

        # Return minimal response
        return PermitsSyncResponse(
            county=request.county,
            count=len(permits),
            permits=[
                {
                    "permit_number": p.permit_number,
                    "address": p.address,
                    "permit_type": p.permit_type,
                    "status": p.status,
                    "issue_date": p.issue_date,
                }
                for p in permits
            ],
        )
