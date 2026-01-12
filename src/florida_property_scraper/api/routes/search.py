try:
    from fastapi import APIRouter, Body

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    Body = None
    FASTAPI_AVAILABLE = False

import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel


router = APIRouter(tags=["search"]) if FASTAPI_AVAILABLE else None


class SearchResult(BaseModel):
    owner: str
    address: str
    county: str
    score: int
    parcel_id: str | None = None
    source: str | None = None
    last_permit_date: str | None = None
    permits_last_15y_count: int = 0
    matched_fields: list[str] = []


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


class AdvancedSearchFilters(BaseModel):
    no_permits_in_years: Optional[int] = None
    permit_status: Optional[list[str]] = None
    permit_types: Optional[list[str]] = None
    city: Optional[str] = None
    zip: Optional[str] = None
    min_score: Optional[int] = None


class AdvancedSearchRequest(BaseModel):
    county: Optional[str] = None
    text: Optional[str] = None
    fields: list[str] = ["owner_name", "situs_address", "parcel_id"]
    filters: AdvancedSearchFilters = AdvancedSearchFilters()
    sort: str = "relevance"
    limit: int = 50


def _advanced_search_from_leads(
    *,
    conn: sqlite3.Connection,
    request: AdvancedSearchRequest,
) -> list[SearchResult]:
    """Advanced search implementation for leads table."""
    cols = _table_columns(conn, "leads")

    # Map field names to column names
    field_map = {
        "owner": "owner_name",
        "owner_name": "owner_name",
        "address": "situs_address",
        "situs_address": "situs_address",
        "mailing_address": "mailing_address",
        "parcel_id": "parcel_id",
        "city": "city",
        "zip": "zip",
    }

    # Validate requested fields exist in table
    search_fields = []
    for field in request.fields:
        col_name = field_map.get(field, field)
        if col_name in cols:
            search_fields.append(col_name)

    if not search_fields:
        # No valid fields to search
        return []

    q = _norm(request.text or "")
    q_lower = q.lower()
    like = _like_param(q)
    county = _norm(request.county or "")
    limit = _clamp_limit(request.limit)

    # Build WHERE clause
    where_parts: list[str] = []
    params: list[Any] = []

    if county:
        where_parts.append("LOWER(TRIM(county)) = LOWER(TRIM(?))")
        params.append(county)

    # Text search on selected fields only
    if q:
        match_parts = [
            f"LOWER(COALESCE({field}, '')) LIKE ?" for field in search_fields
        ]
        where_parts.append(f"({' OR '.join(match_parts)})")
        params.extend([like] * len(search_fields))

    # Filters
    if request.filters.city:
        if "city" in cols:
            where_parts.append("LOWER(TRIM(city)) = LOWER(TRIM(?))")
            params.append(request.filters.city)

    if request.filters.zip:
        if "zip" in cols:
            where_parts.append("zip = ?")
            params.append(request.filters.zip)

    # Build base query
    where_clause = " AND ".join(where_parts) if where_parts else "1=1"

    # Score calculation
    score_col = "lead_score" if "lead_score" in cols else None
    if score_col and not q:
        score_sql = f"COALESCE({score_col}, 0)"
        score_params = []
    elif q:
        # Heuristic scoring based on which field matched
        score_sql = "CASE "
        score_params = []
        for i, field in enumerate(search_fields):
            priority = 90 - (i * 5)  # First field gets highest priority
            score_sql += (
                f"WHEN instr(LOWER(COALESCE({field}, '')), ?) > 0 THEN {priority} "
            )
            score_params.append(q_lower)
        score_sql += "ELSE 70 END"
    else:
        score_sql = "50"
        score_params = []

    # Build main query
    owner_col = "owner_name" if "owner_name" in cols else "''"
    situs_col = "situs_address" if "situs_address" in cols else "''"
    mailing_col = "mailing_address" if "mailing_address" in cols else "''"
    parcel_col = "parcel_id" if "parcel_id" in cols else "''"
    source_col = "source_url" if "source_url" in cols else "''"

    address_expr = f"COALESCE(NULLIF({situs_col}, ''), NULLIF({mailing_col}, ''), '')"

    sql = f"""
        SELECT
            {owner_col} AS owner,
            {address_expr} AS address,
            county,
            {parcel_col} AS parcel_id,
            {source_col} AS source,
            {score_sql} AS score,
            parcel_id AS parcel_for_join
        FROM leads
        WHERE {where_clause}
    """

    # Min score filter
    if request.filters.min_score is not None:
        sql += f" AND ({score_sql}) >= ?"
        params.append(request.filters.min_score)

    # Join with permits if needed
    has_permits_table = _table_exists(conn, "permits")
    if has_permits_table:
        # Left join with permits aggregation
        sql = f"""
        WITH base_results AS (
            {sql}
        ),
        permit_agg AS (
            SELECT
                parcel_id,
                county,
                MAX(issue_date) AS last_permit_date,
                COUNT(CASE WHEN issue_date >= date('now', '-15 years') THEN 1 END) AS permits_15y
            FROM permits
            WHERE parcel_id IS NOT NULL AND parcel_id != ''
            GROUP BY county, parcel_id
        )
        SELECT
            br.owner,
            br.address,
            br.county,
            br.parcel_id,
            br.source,
            br.score,
            pa.last_permit_date,
            COALESCE(pa.permits_15y, 0) AS permits_last_15y_count
        FROM base_results br
        LEFT JOIN permit_agg pa ON br.county = pa.county AND br.parcel_for_join = pa.parcel_id
        """

        # Apply no_permits_in_years filter
        if request.filters.no_permits_in_years is not None:
            years = request.filters.no_permits_in_years
            cutoff_date = (datetime.now() - timedelta(days=years * 365)).strftime(
                "%Y-%m-%d"
            )
            sql += " WHERE (pa.last_permit_date IS NULL OR pa.last_permit_date < ?)"
            params.append(cutoff_date)
    else:
        # No permits table - add null columns
        sql = f"""
        WITH base_results AS (
            {sql}
        )
        SELECT
            owner,
            address,
            county,
            parcel_id,
            source,
            score,
            NULL AS last_permit_date,
            0 AS permits_last_15y_count
        FROM base_results
        """

    # Sort
    if request.sort == "score_desc":
        sql += " ORDER BY score DESC"
    elif request.sort == "last_permit_oldest":
        sql += " ORDER BY CASE WHEN last_permit_date IS NULL THEN 1 ELSE 0 END, last_permit_date ASC"
    elif request.sort == "last_permit_newest":
        sql += " ORDER BY last_permit_date DESC NULLS LAST"
    else:  # relevance (default)
        sql += " ORDER BY score DESC"

    sql += " LIMIT ?"
    params.append(limit)

    # Execute query
    all_params = tuple(score_params) + tuple(params)
    rows = conn.execute(sql, all_params).fetchall()

    results: list[SearchResult] = []
    for row in rows:
        # Determine matched fields
        matched = []
        if q:
            for field in search_fields:
                col_val = row.get(field, "")
                if col_val and q_lower in str(col_val).lower():
                    # Map back to requested field name
                    for req_field, col_name in field_map.items():
                        if col_name == field and req_field in request.fields:
                            matched.append(req_field)
                            break

        results.append(
            SearchResult(
                owner=str(row["owner"] or ""),
                address=str(row["address"] or ""),
                county=str(row["county"] or ""),
                score=int(row["score"] or 0),
                parcel_id=str(row["parcel_id"]) if row["parcel_id"] else None,
                source=str(row["source"]) if row.get("source") else None,
                last_permit_date=str(row["last_permit_date"])
                if row.get("last_permit_date")
                else None,
                permits_last_15y_count=int(row.get("permits_last_15y_count", 0)),
                matched_fields=matched,
            )
        )

    return results


if router:

    @router.post("/search/advanced", response_model=list[SearchResult])
    def advanced_search(
        request: AdvancedSearchRequest = Body(...),
    ) -> list[SearchResult]:
        """
        Advanced search with field selection, filters, and permits enrichment.

        - Only searches the fields specified in 'fields' array
        - Supports filters: no_permits_in_years, city, zip, min_score
        - Enriches results with permit data if available
        - Treats null last_permit_date as MATCH for no_permits_in_years filter
        """
        db_path = _get_db_path()
        if not db_path.exists():
            return []

        conn = _connect(db_path)
        try:
            if _table_exists(conn, "leads"):
                return _advanced_search_from_leads(conn=conn, request=request)
            return []
        finally:
            conn.close()
