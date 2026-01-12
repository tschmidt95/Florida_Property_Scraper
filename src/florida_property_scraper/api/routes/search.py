try:
    from fastapi import APIRouter

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    FASTAPI_AVAILABLE = False

import os
import sqlite3
from pathlib import Path
from typing import Any

from pydantic import BaseModel


router = APIRouter(tags=["search"]) if FASTAPI_AVAILABLE else None


class SearchResult(BaseModel):
    owner: str
    address: str
    county: str
    score: int
    parcel_id: str | None = None
    source: str | None = None


_DB_ENV_VARS = ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB")


def _get_db_path() -> Path:
    for name in _DB_ENV_VARS:
        value = os.getenv(name)
        if value:
            return Path(value)
    return Path("./leads.sqlite")


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return set()
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    return {str(r[1]) for r in rows}


def _clamp_limit(limit: int | None) -> int:
    try:
        n = int(limit or 50)
    except Exception:
        n = 50
    return max(1, min(n, 200))


def _norm(s: str | None) -> str:
    return (s or "").strip()


def _like_param(q: str) -> str:
    # Parameterized LIKE; callers wrap with LOWER() to keep case-insensitive.
    return f"%{q.lower()}%"


def _search_from_properties(
    *,
    conn: sqlite3.Connection,
    q: str,
    county: str,
    limit: int,
) -> list[SearchResult]:
    cols = _table_columns(conn, "properties")
    has_parcel_id = "parcel_id" in cols
    has_raw_html = "raw_html" in cols
    like = _like_param(q)
    q_lower = q.lower()

    where_parts: list[str] = []
    params: list[Any] = []

    if county:
        where_parts.append("LOWER(TRIM(properties.county)) = LOWER(TRIM(?))")
        params.append(county)

    match_parts: list[str] = [
        "LOWER(owners.name) LIKE ?",
        "LOWER(properties.address) LIKE ?",
    ]
    params.extend([like, like])

    if has_parcel_id:
        match_parts.append("LOWER(properties.parcel_id) LIKE ?")
        params.append(like)

    if has_raw_html:
        match_parts.append("LOWER(properties.raw_html) LIKE ?")
        params.append(like)

    where_parts.append(f"({' OR '.join(match_parts)})")

    parcel_select = "properties.parcel_id" if has_parcel_id else "''"

    # Heuristic score ordering: parcel_id match > owner match > address match > raw_html match.
    score_sql = "CASE "
    score_params: list[Any] = []
    if has_parcel_id:
        score_sql += (
            "WHEN instr(LOWER(COALESCE(properties.parcel_id,'')), ?) > 0 THEN 95 "
        )
        score_params.append(q_lower)
    score_sql += "WHEN instr(LOWER(owners.name), ?) > 0 THEN 90 "
    score_params.append(q_lower)
    score_sql += "WHEN instr(LOWER(properties.address), ?) > 0 THEN 80 "
    score_params.append(q_lower)
    score_sql += "ELSE 70 END"

    sql = f"""
        SELECT
            owners.name AS owner,
            properties.address AS address,
            properties.county AS county,
            {parcel_select} AS parcel_id,
            'properties' AS source,
            {score_sql} AS score
        FROM properties
        JOIN owners ON owners.id = properties.owner_id
        WHERE {" AND ".join(where_parts)}
        ORDER BY score DESC, LOWER(owners.name), LOWER(properties.address)
        LIMIT ?
    """

    # Note: SQLite binds parameters in order of '?' appearance. Our score CASE is in
    # the SELECT list, so its params come before the WHERE params.
    rows = conn.execute(sql, tuple(score_params) + tuple(params) + (limit,)).fetchall()
    out: list[SearchResult] = []
    for row in rows:
        out.append(
            SearchResult(
                owner=str(row["owner"] or ""),
                address=str(row["address"] or ""),
                county=str(row["county"] or ""),
                score=int(row["score"] or 0),
                parcel_id=str(row["parcel_id"]) if row["parcel_id"] else None,
                source=str(row["source"] or "") or None,
            )
        )
    return out


def _search_from_leads(
    *,
    conn: sqlite3.Connection,
    q: str,
    county: str,
    limit: int,
) -> list[SearchResult]:
    cols = _table_columns(conn, "leads")
    q_lower = q.lower()
    like = _like_param(q)

    owner_col = "owner_name" if "owner_name" in cols else None
    county_col = "county" if "county" in cols else None
    mailing_col = "mailing_address" if "mailing_address" in cols else None
    situs_col = "situs_address" if "situs_address" in cols else None
    parcel_col = "parcel_id" if "parcel_id" in cols else None
    source_col = (
        "source_url"
        if "source_url" in cols
        else ("property_url" if "property_url" in cols else None)
    )
    score_col = "lead_score" if "lead_score" in cols else None

    if not owner_col or not county_col:
        return []

    # Choose a single address field for UI.
    address_expr_parts: list[str] = []
    if situs_col:
        address_expr_parts.append(f"NULLIF({situs_col}, '')")
    if mailing_col:
        address_expr_parts.append(f"NULLIF({mailing_col}, '')")
    address_expr = "COALESCE(" + ", ".join(address_expr_parts + ["''"]) + ")"

    where_parts: list[str] = []
    params: list[Any] = []

    if county:
        where_parts.append(f"LOWER(TRIM({county_col})) = LOWER(TRIM(?))")
        params.append(county)

    match_parts: list[str] = [
        f"LOWER({owner_col}) LIKE ?",
        f"LOWER({address_expr}) LIKE ?",
    ]
    params.extend([like, like])

    if parcel_col:
        match_parts.append(f"LOWER({parcel_col}) LIKE ?")
        params.append(like)

    where_parts.append(f"({' OR '.join(match_parts)})")

    parcel_select = parcel_col if parcel_col else "''"
    source_select = source_col if source_col else "''"

    # Prefer stored lead_score if present; otherwise heuristic.
    if score_col:
        score_sql = f"CASE WHEN {score_col} IS NOT NULL THEN MIN(MAX(CAST({score_col} AS INTEGER), 0), 100) ELSE 0 END"
        score_params: list[Any] = []
    else:
        score_sql = "CASE "
        score_params = []
        if parcel_col:
            score_sql += f"WHEN instr(LOWER(COALESCE({parcel_col},'')), ?) > 0 THEN 95 "
            score_params.append(q_lower)
        score_sql += f"WHEN instr(LOWER(COALESCE({owner_col},'')), ?) > 0 THEN 90 "
        score_params.append(q_lower)
        score_sql += f"WHEN instr(LOWER({address_expr}), ?) > 0 THEN 80 "
        score_params.append(q_lower)
        score_sql += "ELSE 70 END"

    sql = f"""
        SELECT
            {owner_col} AS owner,
            {address_expr} AS address,
            {county_col} AS county,
            {parcel_select} AS parcel_id,
            {source_select} AS source,
            {score_sql} AS score
        FROM leads
        WHERE {" AND ".join(where_parts)}
        ORDER BY score DESC, LOWER(owner), LOWER(address)
        LIMIT ?
    """

    # Note: SQLite binds parameters in order of '?' appearance. Our score CASE is in
    # the SELECT list, so its params come before the WHERE params.
    rows = conn.execute(sql, tuple(score_params) + tuple(params) + (limit,)).fetchall()
    out: list[SearchResult] = []
    for row in rows:
        out.append(
            SearchResult(
                owner=str(row["owner"] or ""),
                address=str(row["address"] or ""),
                county=str(row["county"] or ""),
                score=int(row["score"] or 0),
                parcel_id=str(row["parcel_id"]) if row["parcel_id"] else None,
                source=str(row["source"]) if row["source"] else None,
            )
        )
    return out


if router:

    @router.get("/search", response_model=list[SearchResult])
    def search(q: str = "", county: str = "", limit: int = 50) -> list[SearchResult]:
        qn = _norm(q)
        if not qn:
            return []

        cn = _norm(county)
        lim = _clamp_limit(limit)

        db_path = _get_db_path()
        if not db_path.exists():
            # Stable response shape; missing DB returns empty set.
            return []

        conn = _connect(db_path)
        try:
            if _table_exists(conn, "leads"):
                return _search_from_leads(conn=conn, q=qn, county=cn, limit=lim)
            if _table_exists(conn, "properties") and _table_exists(conn, "owners"):
                return _search_from_properties(conn=conn, q=qn, county=cn, limit=lim)
            return []
        finally:
            conn.close()

    from pydantic import BaseModel

    class AdvancedSearchFilters(BaseModel):
        """Filters for advanced search."""

        no_permits_in_years: int | None = None
        permit_status: list[str] | None = None
        permit_types: list[str] | None = None
        city: str | None = None
        zip: str | None = None
        min_score: int | None = None

    class AdvancedSearchRequest(BaseModel):
        """Request model for advanced search."""

        county: str | None = None
        text: str | None = None
        fields: list[str] = []  # owner, address, parcel_id, city, zip
        filters: AdvancedSearchFilters = AdvancedSearchFilters()
        sort: str = "relevance"  # relevance, score_desc, last_permit_oldest, last_permit_newest
        limit: int = 50

    class AdvancedSearchResult(BaseModel):
        """Result model for advanced search."""

        owner: str
        address: str
        county: str
        score: int
        parcel_id: str | None = None
        source: str | None = None
        last_permit_date: str | None = None
        permits_last_15y_count: int = 0
        matched_fields: list[str] = []

    @router.post("/search/advanced", response_model=list[AdvancedSearchResult])
    def advanced_search(request: AdvancedSearchRequest) -> list[AdvancedSearchResult]:
        """Advanced search with field selection and permits enrichment."""
        db_path = _get_db_path()
        if not db_path.exists():
            return []

        conn = _connect(db_path)
        try:
            # Determine which table to search from
            has_leads = _table_exists(conn, "leads")
            has_properties = _table_exists(conn, "properties") and _table_exists(
                conn, "owners"
            )
            has_permits = _table_exists(conn, "permits")

            if not (has_leads or has_properties):
                return []

            # Normalize inputs
            text = _norm(request.text or "")
            county = _norm(request.county or "")
            limit = _clamp_limit(request.limit)
            fields = [f.strip().lower() for f in request.fields if f.strip()]

            if not text and not fields:
                return []

            # Build query based on table
            if has_leads:
                results = _advanced_search_leads(
                    conn=conn,
                    text=text,
                    county=county,
                    fields=fields,
                    filters=request.filters,
                    sort=request.sort,
                    limit=limit,
                    has_permits=has_permits,
                )
            else:
                results = _advanced_search_properties(
                    conn=conn,
                    text=text,
                    county=county,
                    fields=fields,
                    filters=request.filters,
                    sort=request.sort,
                    limit=limit,
                    has_permits=has_permits,
                )

            return results
        finally:
            conn.close()


def _advanced_search_leads(
    *,
    conn: sqlite3.Connection,
    text: str,
    county: str,
    fields: list[str],
    filters: AdvancedSearchFilters,
    sort: str,
    limit: int,
    has_permits: bool,
) -> list[AdvancedSearchResult]:
    """Perform advanced search on leads table."""
    from pydantic import BaseModel

    cols = _table_columns(conn, "leads")

    # Map requested fields to columns
    field_map = {
        "owner": "owner_name",
        "address": ["situs_address", "mailing_address"],
        "parcel_id": "parcel_id",
        "city": None,  # Not available
        "zip": None,  # Not available
    }

    # Build WHERE clause
    where_parts: list[str] = []
    params: list[Any] = []
    matched_fields_map: dict[str, list[str]] = {}

    if county:
        where_parts.append("LOWER(TRIM(leads.county)) = LOWER(TRIM(?))")
        params.append(county)

    if text and fields:
        match_parts: list[str] = []
        for field in fields:
            if field not in field_map:
                continue

            col_names = field_map[field]
            if col_names is None:
                continue

            if isinstance(col_names, str):
                col_names = [col_names]

            for col in col_names:
                if col in cols:
                    match_parts.append(f"LOWER({col}) LIKE ?")
                    params.append(_like_param(text))

        if match_parts:
            where_parts.append(f"({' OR '.join(match_parts)})")

    # Apply filters
    if filters.city:
        # City not available in leads
        pass
    if filters.zip:
        # ZIP not available in leads
        pass
    if filters.min_score and "lead_score" in cols:
        where_parts.append("lead_score >= ?")
        params.append(filters.min_score)

    # Permits filtering
    if has_permits and filters.no_permits_in_years is not None:
        # Join with permits aggregation
        pass  # Complex query - simplified for now

    if not where_parts:
        where_parts.append("1=1")

    # Build SELECT with permits enrichment
    permits_join = ""
    permits_select = "NULL AS last_permit_date, 0 AS permits_last_15y_count"

    if has_permits:
        # Left join to permits aggregation
        permits_join = """
        LEFT JOIN (
            SELECT
                county,
                parcel_id,
                MAX(issue_date) AS last_permit_date,
                COUNT(*) AS permits_last_15y_count
            FROM permits
            WHERE issue_date >= date('now', '-15 years')
            GROUP BY county, parcel_id
        ) AS pa ON pa.county = leads.county AND pa.parcel_id = leads.parcel_id
        """
        permits_select = "pa.last_permit_date, COALESCE(pa.permits_last_15y_count, 0) AS permits_last_15y_count"

        # Apply no_permits_in_years filter
        if filters.no_permits_in_years is not None:
            cutoff_years = filters.no_permits_in_years
            # Match if last_permit_date is NULL OR older than cutoff
            where_parts.append(
                f"(pa.last_permit_date IS NULL OR pa.last_permit_date < date('now', '-{cutoff_years} years'))"
            )

        # Apply permit status filter
        if filters.permit_status:
            # This would require a more complex join
            pass

        # Apply permit types filter
        if filters.permit_types:
            # This would require a more complex join
            pass

    # Build score
    score_sql = "COALESCE(lead_score, 70)"

    # Build ORDER BY
    if sort == "score_desc":
        order_by = "score DESC"
    elif sort == "last_permit_oldest" and has_permits:
        order_by = "pa.last_permit_date ASC NULLS FIRST"
    elif sort == "last_permit_newest" and has_permits:
        order_by = "pa.last_permit_date DESC NULLS LAST"
    else:
        order_by = "score DESC"

    parcel_select = "leads.parcel_id" if "parcel_id" in cols else "''"
    source_select = (
        "leads.source_url" if "source_url" in cols else "leads.property_url"
        if "property_url" in cols
        else "''"
    )
    address_expr = "COALESCE(leads.situs_address, leads.mailing_address, '')"

    sql = f"""
        SELECT
            leads.owner_name AS owner,
            {address_expr} AS address,
            leads.county AS county,
            {parcel_select} AS parcel_id,
            {source_select} AS source,
            {score_sql} AS score,
            {permits_select}
        FROM leads
        {permits_join}
        WHERE {" AND ".join(where_parts)}
        ORDER BY {order_by}
        LIMIT ?
    """

    rows = conn.execute(sql, tuple(params) + (limit,)).fetchall()
    results: list[AdvancedSearchResult] = []

    for row in rows:
        results.append(
            AdvancedSearchResult(
                owner=str(row["owner"] or ""),
                address=str(row["address"] or ""),
                county=str(row["county"] or ""),
                score=int(row["score"] or 0),
                parcel_id=str(row["parcel_id"]) if row["parcel_id"] else None,
                source=str(row["source"]) if row["source"] else None,
                last_permit_date=str(row["last_permit_date"])
                if row["last_permit_date"]
                else None,
                permits_last_15y_count=int(row["permits_last_15y_count"] or 0),
                matched_fields=[],  # TODO: Track matched fields
            )
        )

    return results


def _advanced_search_properties(
    *,
    conn: sqlite3.Connection,
    text: str,
    county: str,
    fields: list[str],
    filters: AdvancedSearchFilters,
    sort: str,
    limit: int,
    has_permits: bool,
) -> list[AdvancedSearchResult]:
    """Perform advanced search on properties table."""
    cols = _table_columns(conn, "properties")

    # Map requested fields to columns
    field_map = {
        "owner": "owners.name",
        "address": "properties.address",
        "parcel_id": "properties.parcel_id" if "parcel_id" in cols else None,
        "city": None,  # Not available
        "zip": None,  # Not available
    }

    # Build WHERE clause
    where_parts: list[str] = []
    params: list[Any] = []

    if county:
        where_parts.append("LOWER(TRIM(properties.county)) = LOWER(TRIM(?))")
        params.append(county)

    if text and fields:
        match_parts: list[str] = []
        for field in fields:
            col = field_map.get(field)
            if col:
                match_parts.append(f"LOWER({col}) LIKE ?")
                params.append(_like_param(text))

        if match_parts:
            where_parts.append(f"({' OR '.join(match_parts)})")

    # Apply filters
    if filters.min_score:
        # Score is computed, apply in HAVING or post-filter
        pass

    if not where_parts:
        where_parts.append("1=1")

    # Build SELECT with permits enrichment
    permits_join = ""
    permits_select = "NULL AS last_permit_date, 0 AS permits_last_15y_count"

    if has_permits:
        # Left join to permits aggregation
        permits_join = """
        LEFT JOIN (
            SELECT
                county,
                parcel_id,
                MAX(issue_date) AS last_permit_date,
                COUNT(*) AS permits_last_15y_count
            FROM permits
            WHERE issue_date >= date('now', '-15 years')
            GROUP BY county, parcel_id
        ) AS pa ON pa.county = properties.county AND pa.parcel_id = properties.parcel_id
        """
        permits_select = "pa.last_permit_date, COALESCE(pa.permits_last_15y_count, 0) AS permits_last_15y_count"

        # Apply no_permits_in_years filter
        if filters.no_permits_in_years is not None:
            cutoff_years = filters.no_permits_in_years
            where_parts.append(
                f"(pa.last_permit_date IS NULL OR pa.last_permit_date < date('now', '-{cutoff_years} years'))"
            )

    # Build score
    score_sql = "70"

    # Build ORDER BY
    if sort == "score_desc":
        order_by = "score DESC"
    elif sort == "last_permit_oldest" and has_permits:
        order_by = "pa.last_permit_date ASC NULLS FIRST"
    elif sort == "last_permit_newest" and has_permits:
        order_by = "pa.last_permit_date DESC NULLS LAST"
    else:
        order_by = "score DESC"

    parcel_select = "properties.parcel_id" if "parcel_id" in cols else "''"

    sql = f"""
        SELECT
            owners.name AS owner,
            properties.address AS address,
            properties.county AS county,
            {parcel_select} AS parcel_id,
            'properties' AS source,
            {score_sql} AS score,
            {permits_select}
        FROM properties
        JOIN owners ON owners.id = properties.owner_id
        {permits_join}
        WHERE {" AND ".join(where_parts)}
        ORDER BY {order_by}
        LIMIT ?
    """

    rows = conn.execute(sql, tuple(params) + (limit,)).fetchall()
    results: list[AdvancedSearchResult] = []

    for row in rows:
        results.append(
            AdvancedSearchResult(
                owner=str(row["owner"] or ""),
                address=str(row["address"] or ""),
                county=str(row["county"] or ""),
                score=int(row["score"] or 0),
                parcel_id=str(row["parcel_id"]) if row["parcel_id"] else None,
                source=str(row["source"] or "") or None,
                last_permit_date=str(row["last_permit_date"])
                if row["last_permit_date"]
                else None,
                permits_last_15y_count=int(row["permits_last_15y_count"] or 0),
                matched_fields=[],  # TODO: Track matched fields
            )
        )

    return results
