try:
    from fastapi import APIRouter

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    FASTAPI_AVAILABLE = False

from florida_property_scraper.leads_db import open_conn
from florida_property_scraper.leads_db import search as db_search
from florida_property_scraper.leads_models import SearchResult

router = APIRouter(tags=["search"]) if FASTAPI_AVAILABLE else None


if router:

    @router.get("/search", response_model=list[SearchResult])
    def search(q: str, county: str | None = None, limit: int = 50) -> list[SearchResult]:
        limit = max(1, min(int(limit or 50), 200))
        with open_conn() as conn:
            return db_search(conn, q=q, county=county, limit=limit)
