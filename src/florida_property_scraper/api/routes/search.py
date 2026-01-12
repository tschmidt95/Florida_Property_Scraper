try:
    from fastapi import APIRouter, HTTPException

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    FASTAPI_AVAILABLE = False

import os
import sqlite3
from pathlib import Path
from typing import Any, Optional, List

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


class AdvancedSearchFilters(BaseModel):
    """Filters for advanced search."""
    no_permits_in_years: Optional[int] = None
    permit_status: Optional[List[str]] = None
    permit_types: Optional[List[str]] = None
    city: Optional[str] = None
    zip: Optional[str] = None
    min_score: Optional[int] = None


class AdvancedSearchRequest(BaseModel):
    """Request model for advanced search."""
    county: Optional[str] = None
    text: Optional[str] = None
    fields: List[str] = ["owner", "address", "parcel_id"]
    filters: Optional[AdvancedSearchFilters] = None
    sort: str = "relevance"  # relevance, score_desc, last_permit_oldest, last_permit_newest
    limit: int = 50


class AdvancedSearchResult(BaseModel):
    """Result model for advanced search."""
    owner: str
    address: str
    county: str
    score: int
    parcel_id: Optional[str] = None
    source: Optional[str] = None
    last_permit_date: Optional[str] = None
    permits_last_15y_count: int = 0
    matched_fields: List[str] = []


def _permits_table_exists(conn: sqlite3.Connection) -> bool:
    """Check if permits table exists."""
    return _table_exists(conn, "permits")


def _search_advanced(
    *,
    conn: sqlite3.Connection,
    text: Optional[str],
    fields: List[str],
    county: Optional[str],
    filters: Optional[AdvancedSearchFilters],
    sort: str,
    limit: int,
) -> List[AdvancedSearchResult]:
    """Perform advanced search with field selection and permits enrichment.
    
    Args:
        conn: Database connection
        text: Search text (optional)
        fields: List of fields to search in (owner, address, parcel_id, city, zip)
        county: County filter
        filters: Additional filters
        sort: Sort order
        limit: Maximum results
        
    Returns:
        List of AdvancedSearchResult objects
    """
    # Check if we have a leads table
    if not _table_exists(conn, "leads"):
        return []
    
    cols = _table_columns(conn, "leads")
    
    # Check field availability
    owner_col = "owner_name" if "owner_name" in cols else None
    county_col = "county" if "county" in cols else None
    mailing_col = "mailing_address" if "mailing_address" in cols else None
    situs_col = "situs_address" if "situs_address" in cols else None
    parcel_col = "parcel_id" if "parcel_id" in cols else None
    
    if not owner_col or not county_col:
        return []
    
    # Address expression
    address_expr_parts: list[str] = []
    if situs_col:
        address_expr_parts.append(f"NULLIF({situs_col}, '')")
    if mailing_col:
        address_expr_parts.append(f"NULLIF({mailing_col}, '')")
    address_expr = "COALESCE(" + ", ".join(address_expr_parts + ["''"]) + ")"
    
    # Check if permits table exists
    permits_exist = _permits_table_exists(conn)
    
    # Build query
    where_parts: list[str] = []
    params: list[Any] = []
    
    # County filter
    if county:
        where_parts.append(f"LOWER(TRIM(leads.{county_col})) = LOWER(TRIM(?))")
        params.append(county)
    
    # Text search on selected fields
    if text and text.strip():
        like = _like_param(text)
        match_parts: list[str] = []
        
        if "owner" in fields and owner_col:
            match_parts.append(f"LOWER(leads.{owner_col}) LIKE ?")
            params.append(like)
        
        if "address" in fields:
            match_parts.append(f"LOWER({address_expr}) LIKE ?")
            params.append(like)
        
        if "parcel_id" in fields and parcel_col:
            match_parts.append(f"LOWER(leads.{parcel_col}) LIKE ?")
            params.append(like)
        
        # city and zip would require additional columns in the leads table
        # For now, we skip them if not available
        
        if match_parts:
            where_parts.append(f"({' OR '.join(match_parts)})")
    
    # Add WHERE clause or default to all results if no text
    if not where_parts:
        where_parts.append("1=1")
    
    # Build SELECT with permits enrichment
    parcel_select = f"leads.{parcel_col}" if parcel_col else "''"
    
    if permits_exist:
        # Join with permits aggregation
        select_sql = f"""
            SELECT
                leads.{owner_col} AS owner,
                {address_expr} AS address,
                leads.{county_col} AS county,
                {parcel_select} AS parcel_id,
                MAX(pa.last_permit_date) AS last_permit_date,
                COALESCE(pa.permits_last_15y_count, 0) AS permits_last_15y_count
            FROM leads
            LEFT JOIN (
                SELECT
                    county,
                    parcel_id,
                    MAX(issue_date) AS last_permit_date,
                    COUNT(*) AS permits_last_15y_count
                FROM permits
                WHERE issue_date >= date('now', '-15 years')
                GROUP BY county, parcel_id
            ) AS pa ON pa.county = leads.{county_col} AND pa.parcel_id = {parcel_select}
        """
    else:
        # No permits table, return nulls
        select_sql = f"""
            SELECT
                leads.{owner_col} AS owner,
                {address_expr} AS address,
                leads.{county_col} AS county,
                {parcel_select} AS parcel_id,
                NULL AS last_permit_date,
                0 AS permits_last_15y_count
            FROM leads
        """
    
    # Apply filters
    filter_where_parts: list[str] = []
    
    if filters:
        # no_permits_in_years filter
        if filters.no_permits_in_years is not None and permits_exist:
            years = int(filters.no_permits_in_years)
            # Treat null last_permit_date as MATCH (no permits found)
            # Also match if last permit is older than specified years
            cutoff_date = f"date('now', '-{years} years')"
            # Note: after subquery, reference last_permit_date directly
            filter_where_parts.append(
                f"(last_permit_date IS NULL OR last_permit_date < {cutoff_date})"
            )
        
        # min_score filter (if we had a score field)
        if filters.min_score is not None:
            # For now, we don't have a score in this query
            pass
    
    # Combine WHERE clauses
    all_where = " AND ".join(where_parts)
    
    # Build full query
    if filter_where_parts and permits_exist:
        # Use subquery to filter after aggregation
        sql = f"""
            SELECT * FROM (
                {select_sql}
                WHERE {all_where}
                GROUP BY leads.id
            )
            WHERE {" AND ".join(filter_where_parts)}
        """
    else:
        sql = f"""
            {select_sql}
            WHERE {all_where}
            {"GROUP BY leads.id" if permits_exist else ""}
        """
    
    # Apply sorting
    if sort == "score_desc":
        sql += " ORDER BY 1 DESC"  # Placeholder, we don't have score yet
    elif sort == "last_permit_oldest":
        if permits_exist:
            sql += " ORDER BY CASE WHEN last_permit_date IS NULL THEN 0 ELSE 1 END ASC, last_permit_date ASC"
        else:
            sql += " ORDER BY owner ASC"
    elif sort == "last_permit_newest":
        if permits_exist:
            sql += " ORDER BY CASE WHEN last_permit_date IS NULL THEN 1 ELSE 0 END ASC, last_permit_date DESC"
        else:
            sql += " ORDER BY owner DESC"
    else:  # relevance (default)
        sql += " ORDER BY owner ASC"
    
    sql += f" LIMIT {limit}"
    
    # Execute query
    rows = conn.execute(sql, tuple(params)).fetchall()
    
    # Convert to results
    results: List[AdvancedSearchResult] = []
    for row in rows:
        results.append(
            AdvancedSearchResult(
                owner=str(row["owner"] or ""),
                address=str(row["address"] or ""),
                county=str(row["county"] or ""),
                score=80,  # Placeholder score
                parcel_id=str(row["parcel_id"]) if row["parcel_id"] else None,
                source=None,
                last_permit_date=row["last_permit_date"],
                permits_last_15y_count=int(row["permits_last_15y_count"] or 0),
                matched_fields=fields,  # Return the fields that were searched
            )
        )
    
    return results


if router:
    
    @router.post("/search/advanced", response_model=List[AdvancedSearchResult])
    def advanced_search(request: AdvancedSearchRequest) -> List[AdvancedSearchResult]:
        """Advanced search with field selection and permits enrichment.
        
        Args:
            request: AdvancedSearchRequest with search parameters
            
        Returns:
            List of AdvancedSearchResult objects
        """
        # Validate fields
        valid_fields = {"owner", "address", "parcel_id", "city", "zip"}
        for field in request.fields:
            if field not in valid_fields:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid field: {field}. Valid fields are: {', '.join(valid_fields)}"
                )
        
        # Validate sort
        valid_sorts = {"relevance", "score_desc", "last_permit_oldest", "last_permit_newest"}
        if request.sort not in valid_sorts:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid sort: {request.sort}. Valid values are: {', '.join(valid_sorts)}"
            )
        
        # Clamp limit
        limit = _clamp_limit(request.limit)
        
        # Get database
        db_path = _get_db_path()
        if not db_path.exists():
            return []
        
        conn = _connect(db_path)
        try:
            return _search_advanced(
                conn=conn,
                text=request.text,
                fields=request.fields,
                county=request.county,
                filters=request.filters,
                sort=request.sort,
                limit=limit,
            )
        finally:
            conn.close()
