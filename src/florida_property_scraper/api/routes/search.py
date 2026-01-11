try:
    from fastapi import APIRouter

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    FASTAPI_AVAILABLE = False

from pydantic import BaseModel


router = APIRouter(tags=["search"]) if FASTAPI_AVAILABLE else None


class SearchResult(BaseModel):
    owner: str
    address: str
    county: str
    score: int


if router:

    @router.get("/search", response_model=list[SearchResult])
    def search(q: str = "", county: str = "Orange") -> list[SearchResult]:
        # TEMP: fake data so UI works end-to-end. We'll replace with real search later.
        q = (q or "").strip().lower()
        rows = [
            {
                "owner": "John Smith",
                "address": "123 Main St",
                "county": county,
                "score": 92,
            },
            {
                "owner": "Acme Holdings LLC",
                "address": "45 Lakeview Dr",
                "county": county,
                "score": 88,
            },
            {
                "owner": "Maria Garcia",
                "address": "9 Palm Ave",
                "county": county,
                "score": 81,
            },
        ]
        if not q:
            return rows
        return [r for r in rows if q in r["owner"].lower() or q in r["address"].lower()]
