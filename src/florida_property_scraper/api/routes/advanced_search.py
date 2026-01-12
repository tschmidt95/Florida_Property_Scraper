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

from florida_property_scraper.leads_db import detect_leads_schema
from florida_property_scraper.leads_db import open_conn
from florida_property_scraper.permits_db import ensure_permits_schema
from florida_property_scraper.permits_db import get_last_permit_date_expr
from florida_property_scraper.permits_models import AdvancedSearchResult


router = APIRouter(tags=["search"]) if FASTAPI_AVAILABLE else None


class AdvancedSearchFilters(BaseModel):
    has_parcel_id: bool | None = None
    min_score: int | None = None
    no_permits_in_years: int | None = None


class AdvancedSearchRequest(BaseModel):
    q: str = ""
    counties: list[str] | None = None
    filters: AdvancedSearchFilters | None = None
    limit: int | None = 50
    offset: int | None = 0


def _clamp_limit(limit: int | None) -> int:
    if limit is None:
        return 50
    try:
        value = int(limit)
    except Exception:
        return 50
    return max(1, min(value, 200))


def _clamp_offset(offset: int | None) -> int:
    if offset is None:
        return 0
    try:
        value = int(offset)
    except Exception:
        return 0
    return max(0, value)


if router:

    @router.post("/search/advanced", response_model=list[AdvancedSearchResult])
    def search_advanced(req: AdvancedSearchRequest) -> list[AdvancedSearchResult]:
        q_clean = (req.q or "").strip()
        counties = [c.strip() for c in (req.counties or []) if c and c.strip()]
        limit = _clamp_limit(req.limit)
        offset = _clamp_offset(req.offset)
        filters = req.filters or AdvancedSearchFilters()

        no_permits_in_years = filters.no_permits_in_years
        if no_permits_in_years is not None:
            try:
                no_permits_in_years = int(no_permits_in_years)
            except Exception:
                raise HTTPException(status_code=400, detail="no_permits_in_years must be an int")
            if no_permits_in_years <= 0 or no_permits_in_years > 200:
                raise HTTPException(
                    status_code=400,
                    detail="no_permits_in_years must be between 1 and 200",
                )

        with open_conn() as conn:
            ensure_permits_schema(conn)
            schema = detect_leads_schema(conn)
            if schema.kind != "new":
                raise HTTPException(
                    status_code=400,
                    detail="advanced search requires the new SQLite leads schema",
                )

            where = []
            params: list[object] = []

            if counties:
                where.append(
                    "lower(l.county) IN (" + ",".join(["lower(?)"] * len(counties)) + ")"
                )
                params.extend(counties)

            if q_clean:
                where.append(
                    "(lower(l.owner) LIKE lower(?) OR lower(l.address) LIKE lower(?) OR lower(ifnull(l.parcel_id,'')) LIKE lower(?))"
                )
                like = f"%{q_clean}%"
                params.extend([like, like, like])

            if filters.has_parcel_id is True:
                where.append("l.parcel_id IS NOT NULL AND l.parcel_id != ''")

            if filters.min_score is not None:
                try:
                    min_score = int(filters.min_score)
                except Exception:
                    raise HTTPException(status_code=400, detail="min_score must be an int")
                where.append("l.score >= ?")
                params.append(min_score)

            if no_permits_in_years is not None:
                where.append("l.parcel_id IS NOT NULL AND l.parcel_id != ''")
                where.append(
                    "(ps.last_permit_date IS NULL OR ps.last_permit_date < date('now', '-' || ? || ' years'))"
                )
                params.append(no_permits_in_years)

            where_sql = (" WHERE " + " AND ".join(where)) if where else ""

            sql = (
                "SELECT l.owner, l.address, l.county, l.parcel_id, l.source, l.score, ps.last_permit_date "
                "FROM leads l "
                f"LEFT JOIN ({get_last_permit_date_expr()}) ps "
                "ON ps.county = l.county AND ps.parcel_id = l.parcel_id"
                + where_sql
                + " ORDER BY l.score DESC, l.id DESC LIMIT ? OFFSET ?"
            )

            rows = conn.execute(sql, (*params, limit, offset)).fetchall()

            return [
                AdvancedSearchResult(
                    owner=str(r["owner"] or ""),
                    address=str(r["address"] or ""),
                    county=str(r["county"] or ""),
                    parcel_id=(str(r["parcel_id"]) if r["parcel_id"] else None),
                    source=(str(r["source"]) if r["source"] else None),
                    score=int(r["score"] or 0),
                    last_permit_date=(
                        str(r["last_permit_date"]) if r["last_permit_date"] else None
                    ),
                )
                for r in rows
            ]
