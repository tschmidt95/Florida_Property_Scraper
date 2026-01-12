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

    class AdvancedSearchFilters(BaseModel):
        no_permits_in_years: int | None = None
        permit_status: list[str] | None = None
        permit_types: list[str] | None = None
        city: str | None = None
        zip: str | None = None
        min_score: int | None = None

    class AdvancedSearchRequest(BaseModel):
        county: str | None = None
        text: str | None = None
        fields: list[str] = ["owner", "address", "parcel_id", "city", "zip"]
        filters: AdvancedSearchFilters = AdvancedSearchFilters()
        sort: str = (
            "relevance"  # relevance, score_desc, last_permit_oldest, last_permit_newest
        )
        limit: int = 50

    class AdvancedSearchResult(BaseModel):
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
    def search_advanced(request: AdvancedSearchRequest) -> list[AdvancedSearchResult]:
        """Advanced search with field selection, filters, and permit enrichment."""
        text = _norm(request.text or "")
        county_filter = _norm(request.county or "")
        limit = _clamp_limit(request.limit)

        db_path = _get_db_path()
        if not db_path.exists():
            return []

        conn = _connect(db_path)
        try:
            # Determine which table to use
            has_leads = _table_exists(conn, "leads")
            has_properties = _table_exists(conn, "properties")
            has_permits = _table_exists(conn, "permits")

            if has_leads:
                return _advanced_search_leads(
                    conn=conn,
                    text=text,
                    county_filter=county_filter,
                    fields=request.fields,
                    filters=request.filters,
                    sort=request.sort,
                    limit=limit,
                    has_permits=has_permits,
                )
            elif has_properties:
                return _advanced_search_properties(
                    conn=conn,
                    text=text,
                    county_filter=county_filter,
                    fields=request.fields,
                    filters=request.filters,
                    sort=request.sort,
                    limit=limit,
                    has_permits=has_permits,
                )
            return []
        finally:
            conn.close()


def _advanced_search_leads(
    *,
    conn: sqlite3.Connection,
    text: str,
    county_filter: str,
    fields: list[str],
    filters: AdvancedSearchFilters,
    sort: str,
    limit: int,
    has_permits: bool,
) -> list[AdvancedSearchResult]:
    """Advanced search on leads table."""
    cols = _table_columns(conn, "leads")

    # Map API fields to DB columns
    field_map = {
        "owner": "owner_name" if "owner_name" in cols else None,
        "address": ["situs_address", "mailing_address"],
        "parcel_id": "parcel_id" if "parcel_id" in cols else None,
        "city": "city" if "city" in cols else None,
        "zip": "zip" if "zip" in cols else None,
    }

    county_col = "county" if "county" in cols else None
    owner_col = field_map.get("owner")
    parcel_col = field_map.get("parcel_id")

    if not owner_col or not county_col:
        return []

    # Build WHERE clause for field search
    where_parts: list[str] = []
    params: list[Any] = []

    if county_filter:
        where_parts.append(f"LOWER(TRIM({county_col})) = LOWER(TRIM(?))")
        params.append(county_filter)

    if text:
        like = _like_param(text)
        match_parts: list[str] = []

        for field in fields:
            db_field = field_map.get(field)
            if not db_field:
                continue

            if isinstance(db_field, list):
                for col in db_field:
                    if col in cols:
                        match_parts.append(f"LOWER({col}) LIKE ?")
                        params.append(like)
            elif db_field in cols:
                match_parts.append(f"LOWER({db_field}) LIKE ?")
                params.append(like)

        if match_parts:
            where_parts.append(f"({' OR '.join(match_parts)})")

    # Apply city/zip filters
    if filters.city and "city" in cols:
        where_parts.append("LOWER(TRIM(city)) = LOWER(TRIM(?))")
        params.append(filters.city)

    if filters.zip and "zip" in cols:
        where_parts.append("zip = ?")
        params.append(filters.zip)

    if not where_parts:
        return []

    # Build address expression
    address_expr_parts: list[str] = []
    if "situs_address" in cols:
        address_expr_parts.append("NULLIF(situs_address, '')")
    if "mailing_address" in cols:
        address_expr_parts.append("NULLIF(mailing_address, '')")
    address_expr = "COALESCE(" + ", ".join(address_expr_parts + ["''"]) + ")"

    # Build score expression
    score_col = "lead_score" if "lead_score" in cols else None
    if score_col:
        score_sql = f"COALESCE({score_col}, 70)"
    else:
        score_sql = "70"

    # Parcel select
    parcel_select = parcel_col if parcel_col else "''"

    sql = f"""
        SELECT
            {owner_col} AS owner,
            {address_expr} AS address,
            {county_col} AS county,
            {parcel_select} AS parcel_id,
            {score_sql} AS score
        FROM leads
        WHERE {" AND ".join(where_parts)}
        LIMIT ?
    """

    rows = conn.execute(sql, tuple(params) + (limit * 2,)).fetchall()

    # Enrich with permits
    results = _enrich_with_permits(conn, rows, filters, has_permits)

    # Apply sort
    results = _apply_sort(results, sort)

    # Apply limit
    return results[:limit]


def _advanced_search_properties(
    *,
    conn: sqlite3.Connection,
    text: str,
    county_filter: str,
    fields: list[str],
    filters: AdvancedSearchFilters,
    sort: str,
    limit: int,
    has_permits: bool,
) -> list[AdvancedSearchResult]:
    """Advanced search on properties table."""
    cols = _table_columns(conn, "properties")
    has_parcel_id = "parcel_id" in cols

    # Build WHERE clause
    where_parts: list[str] = []
    params: list[Any] = []

    if county_filter:
        where_parts.append("LOWER(TRIM(properties.county)) = LOWER(TRIM(?))")
        params.append(county_filter)

    if text:
        like = _like_param(text)
        match_parts: list[str] = []

        if "owner" in fields:
            match_parts.append("LOWER(owners.name) LIKE ?")
            params.append(like)

        if "address" in fields:
            match_parts.append("LOWER(properties.address) LIKE ?")
            params.append(like)

        if "parcel_id" in fields and has_parcel_id:
            match_parts.append("LOWER(properties.parcel_id) LIKE ?")
            params.append(like)

        if match_parts:
            where_parts.append(f"({' OR '.join(match_parts)})")

    if not where_parts:
        return []

    parcel_select = "properties.parcel_id" if has_parcel_id else "''"

    sql = f"""
        SELECT
            owners.name AS owner,
            properties.address AS address,
            properties.county AS county,
            {parcel_select} AS parcel_id,
            85 AS score
        FROM properties
        JOIN owners ON owners.id = properties.owner_id
        WHERE {" AND ".join(where_parts)}
        LIMIT ?
    """

    rows = conn.execute(sql, tuple(params) + (limit * 2,)).fetchall()

    # Enrich with permits
    results = _enrich_with_permits(conn, rows, filters, has_permits)

    # Apply sort
    results = _apply_sort(results, sort)

    # Apply limit
    return results[:limit]


def _enrich_with_permits(
    conn: sqlite3.Connection,
    rows: list,
    filters: AdvancedSearchFilters,
    has_permits: bool,
) -> list[AdvancedSearchResult]:
    """Enrich search results with permit data."""
    import datetime

    results: list[AdvancedSearchResult] = []

    # Calculate cutoff date for 15-year permit count
    cutoff_15y = (datetime.datetime.now() - datetime.timedelta(days=15 * 365)).strftime(
        "%Y-%m-%d"
    )

    for row in rows:
        owner = str(row["owner"] or "")
        address = str(row["address"] or "")
        county = str(row["county"] or "")
        parcel_id = str(row["parcel_id"]) if row["parcel_id"] else None
        score = int(row["score"] or 0)

        last_permit_date: str | None = None
        permits_last_15y_count = 0

        if has_permits and parcel_id:
            # Query permits for this parcel
            permit_rows = conn.execute(
                """
                SELECT issue_date, status, permit_type
                FROM permits
                WHERE county = ? AND parcel_id = ?
                ORDER BY issue_date DESC
                """,
                (county, parcel_id),
            ).fetchall()

            if permit_rows:
                last_permit_date = permit_rows[0]["issue_date"]

            # Count permits in last 15 years
            for p in permit_rows:
                if p["issue_date"] and p["issue_date"] >= cutoff_15y:
                    permits_last_15y_count += 1

            # Apply no_permits_in_years filter
            if filters.no_permits_in_years is not None:
                cutoff = (
                    datetime.datetime.now()
                    - datetime.timedelta(days=filters.no_permits_in_years * 365)
                ).strftime("%Y-%m-%d")

                has_recent_permit = any(
                    p["issue_date"] and p["issue_date"] >= cutoff for p in permit_rows
                )

                # If last_permit_date is null, treat as MATCH
                if last_permit_date is not None and has_recent_permit:
                    continue  # Skip this result

            # Apply permit_status filter
            if filters.permit_status:
                if not any(
                    p["status"]
                    and p["status"].lower()
                    in [s.lower() for s in filters.permit_status]
                    for p in permit_rows
                ):
                    continue

            # Apply permit_types filter
            if filters.permit_types:
                if not any(
                    p["permit_type"]
                    and p["permit_type"].lower()
                    in [t.lower() for t in filters.permit_types]
                    for p in permit_rows
                ):
                    continue

        elif has_permits and address:
            # Fallback to address-based lookup
            permit_rows = conn.execute(
                """
                SELECT issue_date, status, permit_type
                FROM permits
                WHERE county = ? AND LOWER(TRIM(address)) = LOWER(TRIM(?))
                ORDER BY issue_date DESC
                """,
                (county, address),
            ).fetchall()

            if permit_rows:
                last_permit_date = permit_rows[0]["issue_date"]

            for p in permit_rows:
                if p["issue_date"] and p["issue_date"] >= cutoff_15y:
                    permits_last_15y_count += 1

            # Apply filters
            if filters.no_permits_in_years is not None:
                cutoff = (
                    datetime.datetime.now()
                    - datetime.timedelta(days=filters.no_permits_in_years * 365)
                ).strftime("%Y-%m-%d")

                has_recent_permit = any(
                    p["issue_date"] and p["issue_date"] >= cutoff for p in permit_rows
                )

                if last_permit_date is not None and has_recent_permit:
                    continue

            if filters.permit_status:
                if not any(
                    p["status"]
                    and p["status"].lower()
                    in [s.lower() for s in filters.permit_status]
                    for p in permit_rows
                ):
                    continue

            if filters.permit_types:
                if not any(
                    p["permit_type"]
                    and p["permit_type"].lower()
                    in [t.lower() for t in filters.permit_types]
                    for p in permit_rows
                ):
                    continue
        else:
            # No permits table or no parcel_id/address
            # Apply no_permits_in_years filter: null last_permit_date = MATCH
            pass

        # Apply min_score filter
        if filters.min_score is not None and score < filters.min_score:
            continue

        results.append(
            AdvancedSearchResult(
                owner=owner,
                address=address,
                county=county,
                score=score,
                parcel_id=parcel_id,
                source=None,
                last_permit_date=last_permit_date,
                permits_last_15y_count=permits_last_15y_count,
                matched_fields=[],  # TODO: track matched fields
            )
        )

    return results


def _apply_sort(
    results: list[AdvancedSearchResult], sort: str
) -> list[AdvancedSearchResult]:
    """Apply sorting to results."""
    if sort == "score_desc":
        results.sort(key=lambda r: r.score, reverse=True)
    elif sort == "last_permit_oldest":
        results.sort(key=lambda r: r.last_permit_date or "9999-99-99")
    elif sort == "last_permit_newest":
        results.sort(key=lambda r: r.last_permit_date or "0000-00-00", reverse=True)
    else:  # relevance (default)
        results.sort(key=lambda r: r.score, reverse=True)

    return results
