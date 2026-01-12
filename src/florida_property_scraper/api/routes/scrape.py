from __future__ import annotations

try:
    from fastapi import APIRouter
    from fastapi import HTTPException

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    HTTPException = None
    FASTAPI_AVAILABLE = False

from pydantic import BaseModel

from florida_property_scraper.leads_db import ensure_schema
from florida_property_scraper.leads_db import open_conn
from florida_property_scraper.leads_db import upsert_many
from florida_property_scraper.leads_models import SearchResult
from florida_property_scraper.scrapers.registry import get_scraper


router = APIRouter(tags=["scrape"]) if FASTAPI_AVAILABLE else None


class ScrapeRequest(BaseModel):
    county: str
    query: str
    limit: int | None = None


def _clamp_limit(limit: int | None) -> int:
    if limit is None:
        return 50
    try:
        value = int(limit)
    except Exception:
        return 50
    return max(1, min(value, 200))


if router:

    @router.post("/scrape", response_model=list[SearchResult])
    def scrape(req: ScrapeRequest) -> list[SearchResult]:
        query = (req.query or "").strip()
        if not query:
            raise HTTPException(status_code=400, detail="query is required")

        county = (req.county or "").strip()
        if not county:
            raise HTTPException(status_code=400, detail="county is required")

        limit = _clamp_limit(req.limit)

        scraper = get_scraper(county)
        if scraper is None:
            raise HTTPException(
                status_code=400,
                detail=f"No scraper available for county '{county}'.",
            )

        results = scraper.search(query=query, limit=limit)

        with open_conn() as conn:
            ensure_schema(conn)
            upsert_many(conn, results)

        return results
