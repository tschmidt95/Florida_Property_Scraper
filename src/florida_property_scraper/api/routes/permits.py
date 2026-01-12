"""Permits API endpoints."""
import os
from pathlib import Path

try:
    from fastapi import APIRouter, HTTPException

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover
    APIRouter = None
    HTTPException = None
    FASTAPI_AVAILABLE = False

from pydantic import BaseModel

from florida_property_scraper.permits.registry import get_permits_scraper
from florida_property_scraper.storage import SQLiteStore


router = APIRouter(tags=["permits"]) if FASTAPI_AVAILABLE else None


class PermitsSyncRequest(BaseModel):
    """Request model for permits sync endpoint."""

    county: str
    query: str
    limit: int = 50


class PermitResponse(BaseModel):
    """Response model for a single permit."""

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


_DB_ENV_VARS = ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB")


def _get_db_path() -> Path:
    """Get database path from environment."""
    for name in _DB_ENV_VARS:
        value = os.getenv(name)
        if value:
            return Path(value)
    return Path("./leads.sqlite")


if router:

    @router.post("/permits/sync", response_model=list[PermitResponse])
    def permits_sync(request: PermitsSyncRequest) -> list[PermitResponse]:
        """Sync permits from county portal (LIVE-gated).
        
        This endpoint requires LIVE=1 environment variable to be set.
        Fetches permits from the county portal, stores them in the database,
        and returns the list of permits.
        
        Args:
            request: Sync request with county, query, and limit
            
        Returns:
            List of permit records
            
        Raises:
            HTTPException: If LIVE is not set or scraper fails
        """
        if os.getenv("LIVE") != "1":
            raise HTTPException(
                status_code=400,
                detail="Live HTTP is disabled. Set LIVE=1 environment variable to enable permits sync.",
            )
        
        scraper = get_permits_scraper(request.county)
        if not scraper:
            raise HTTPException(
                status_code=404,
                detail=f"No permits scraper available for county: {request.county}",
            )
        
        try:
            permits = scraper.search_permits(request.query, limit=request.limit)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to fetch permits: {str(e)}",
            )
        
        # Store permits in database
        db_path = _get_db_path()
        store = SQLiteStore(str(db_path))
        try:
            permit_dicts = []
            for p in permits:
                # Truncate raw field to avoid excessive storage
                truncated = p.with_truncated_raw(max_len=5000)
                permit_dicts.append({
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
                })
            store.upsert_many_permits(permit_dicts)
        finally:
            store.close()
        
        # Return permit list
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
            for p in permits
        ]
