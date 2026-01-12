"""Permits sync API route."""
try:
    from fastapi import APIRouter, HTTPException
    FASTAPI_AVAILABLE = True
except Exception:
    APIRouter = None
    HTTPException = None
    FASTAPI_AVAILABLE = False

import os
from typing import List, Optional
from pydantic import BaseModel

from florida_property_scraper.permits.registry import get_permits_scraper
from florida_property_scraper.storage import SQLiteStore


router = APIRouter(tags=["permits"]) if FASTAPI_AVAILABLE else None


class PermitsSyncRequest(BaseModel):
    """Request model for permits sync."""
    county: str
    query: str
    limit: Optional[int] = 50


class PermitResponse(BaseModel):
    """Response model for a single permit."""
    county: str
    permit_number: str
    source: str
    parcel_id: Optional[str] = None
    address: Optional[str] = None
    permit_type: Optional[str] = None
    status: Optional[str] = None
    issue_date: Optional[str] = None
    final_date: Optional[str] = None
    description: Optional[str] = None


class PermitsSyncResponse(BaseModel):
    """Response model for permits sync."""
    county: str
    count: int
    permits: List[PermitResponse]


def _get_db_path() -> str:
    """Get database path from environment or default."""
    for name in ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB"):
        value = os.getenv(name)
        if value:
            return value
    return "./leads.sqlite"


if router:
    
    @router.post("/permits/sync", response_model=PermitsSyncResponse)
    def sync_permits(request: PermitsSyncRequest) -> PermitsSyncResponse:
        """Sync permits from county portal.
        
        Requires LIVE=1 environment variable to make actual HTTP requests.
        
        Args:
            request: PermitsSyncRequest with county, query, and optional limit
            
        Returns:
            PermitsSyncResponse with list of permits
            
        Raises:
            HTTPException: If LIVE!=1, county not supported, or scraping fails
        """
        # LIVE gating
        if os.getenv("LIVE") != "1":
            raise HTTPException(
                status_code=400,
                detail="Permits sync requires LIVE=1 environment variable. "
                       "Set LIVE=1 to enable live scraping of county portals."
            )
        
        # Validate request
        if not request.query or not request.query.strip():
            raise HTTPException(status_code=400, detail="Query parameter is required")
        
        county_lower = request.county.lower().strip()
        if not county_lower:
            raise HTTPException(status_code=400, detail="County parameter is required")
        
        # Get scraper
        scraper = get_permits_scraper(county_lower)
        if scraper is None:
            raise HTTPException(
                status_code=404,
                detail=f"Permits scraper not available for county: {request.county}"
            )
        
        # Clamp limit
        limit = max(1, min(request.limit or 50, 200))
        
        # Scrape permits
        try:
            permits = scraper.search_permits(request.query, limit=limit)
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=str(e))
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to scrape permits: {str(e)}"
            )
        
        # Store permits in database
        db_path = _get_db_path()
        store = SQLiteStore(db_path)
        try:
            # Convert PermitRecord objects to dicts
            permit_dicts = [p.to_dict() for p in permits]
            store.upsert_many_permits(permit_dicts)
        finally:
            store.close()
        
        # Convert to response format (with truncated raw data for API response)
        permit_responses = []
        for p in permits:
            permit_responses.append(
                PermitResponse(
                    county=p.county,
                    permit_number=p.permit_number,
                    source=p.source,
                    parcel_id=p.parcel_id,
                    address=p.address,
                    permit_type=p.permit_type,
                    status=p.status,
                    issue_date=p.issue_date,
                    final_date=p.final_date,
                    description=p.description,
                )
            )
        
        return PermitsSyncResponse(
            county=county_lower,
            count=len(permit_responses),
            permits=permit_responses,
        )
