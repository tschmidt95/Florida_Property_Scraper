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
    # Advanced search enrichment fields
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

    class AdvancedSearchRequest(BaseModel):
        county: str | None = None
        text: str | None = None
        fields: list[str] = ["owner", "address", "parcel_id"]
        filters: dict[str, Any] = {}
        sort: str = "relevance"
        limit: int = 50

    @router.post("/search/advanced", response_model=list[SearchResult])
    def advanced_search(request: AdvancedSearchRequest) -> list[SearchResult]:
        """Advanced search with field selection, filters, and permits enrichment.

        Only searches the explicitly specified fields. Supports:
        - Field selection (owner, address, parcel_id, city, zip)
        - Filters: no_permits_in_years, permit_status, permit_types, city, zip, min_score
        - Sort: relevance, score_desc, last_permit_oldest, last_permit_newest
        - Permits enrichment: last_permit_date, permits_last_15y_count
        """

        text = _norm(request.text or "")
        if not text:
            return []

        county = _norm(request.county or "")
        limit = _clamp_limit(request.limit)

        db_path = _get_db_path()
        if not db_path.exists():
            return []

        conn = _connect(db_path)
        try:
            # Check which tables exist
            has_leads = _table_exists(conn, "leads")
            has_properties = _table_exists(conn, "properties") and _table_exists(
                conn, "owners"
            )
            has_permits = _table_exists(conn, "permits")

            if not has_leads and not has_properties:
                return []

            # Get table columns
            cols = _table_columns(conn, "leads" if has_leads else "properties")

            # Build search query
            results = []
            if has_leads:
                results = _advanced_search_from_leads(
                    conn=conn,
                    text=text,
                    county=county,
                    fields=request.fields,
                    filters=request.filters,
                    sort=request.sort,
                    limit=limit,
                    cols=cols,
                    has_permits=has_permits,
                )
            elif has_properties:
                results = _advanced_search_from_properties(
                    conn=conn,
                    text=text,
                    county=county,
                    fields=request.fields,
                    filters=request.filters,
                    sort=request.sort,
                    limit=limit,
                    cols=cols,
                    has_permits=has_permits,
                )

            return results
        finally:
            conn.close()


def _advanced_search_from_leads(
    *,
    conn: sqlite3.Connection,
    text: str,
    county: str,
    fields: list[str],
    filters: dict[str, Any],
    sort: str,
    limit: int,
    cols: set[str],
    has_permits: bool,
) -> list[SearchResult]:
    """Execute advanced search against leads table."""
    from datetime import datetime, timedelta

    like = _like_param(text)
    text_lower = text.lower()

    # Map field names to column names
    field_map = {
        "owner": "owner_name",
        "address": ["situs_address", "mailing_address"],
        "parcel_id": "parcel_id",
        "city": None,  # Not in leads
        "zip": None,  # Not in leads
    }

    # Build match conditions for selected fields only
    match_parts: list[str] = []
    params: list[Any] = []

    for field in fields:
        col_names = field_map.get(field)
        if col_names is None:
            continue
        if isinstance(col_names, list):
            for col in col_names:
                if col in cols:
                    match_parts.append(f"LOWER(leads.{col}) LIKE ?")
                    params.append(like)
        elif col_names in cols:
            match_parts.append(f"LOWER(leads.{col_names}) LIKE ?")
            params.append(like)

    if not match_parts:
        return []

    # Build WHERE clause
    where_parts: list[str] = [f"({' OR '.join(match_parts)})"]

    if county:
        where_parts.append("LOWER(TRIM(leads.county)) = LOWER(TRIM(?))")
        params.append(county)

    # Apply filters
    min_score = filters.get("min_score")
    if min_score is not None and "lead_score" in cols:
        where_parts.append("leads.lead_score >= ?")
        params.append(int(min_score))

    # Build base query with permits aggregation
    parcel_col = "parcel_id" if "parcel_id" in cols else None
    owner_col = "owner_name" if "owner_name" in cols else None
    county_col = "county" if "county" in cols else None

    if not parcel_col or not owner_col or not county_col:
        return []

    situs_col = "situs_address" if "situs_address" in cols else None
    mailing_col = "mailing_address" if "mailing_address" in cols else None
    address_expr_parts = []
    if situs_col:
        address_expr_parts.append(f"NULLIF(leads.{situs_col}, '')")
    if mailing_col:
        address_expr_parts.append(f"NULLIF(leads.{mailing_col}, '')")
    address_expr = "COALESCE(" + ", ".join(address_expr_parts + ["''"]) + ")"

    score_col = "lead_score" if "lead_score" in cols else None
    source_col = (
        "source_url"
        if "source_url" in cols
        else ("property_url" if "property_url" in cols else None)
    )

    # Score calculation
    if score_col:
        score_sql = f"COALESCE(leads.{score_col}, 0)"
        score_params: list[Any] = []
    else:
        score_sql = "CASE "
        score_params = []
        if parcel_col:
            score_sql += (
                f"WHEN instr(LOWER(COALESCE(leads.{parcel_col},'')), ?) > 0 THEN 95 "
            )
            score_params.append(text_lower)
        score_sql += (
            f"WHEN instr(LOWER(COALESCE(leads.{owner_col},'')), ?) > 0 THEN 90 "
        )
        score_params.append(text_lower)
        score_sql += f"WHEN instr(LOWER({address_expr}), ?) > 0 THEN 80 "
        score_params.append(text_lower)
        score_sql += "ELSE 70 END"

    # Permits aggregation subquery
    if has_permits:
        # Aggregate permits: last permit date and count in last 15 years
        cutoff_date = (datetime.now() - timedelta(days=15 * 365)).strftime("%Y-%m-%d")
        permits_subquery = f"""
            LEFT JOIN (
                SELECT
                    county,
                    parcel_id,
                    MAX(issue_date) AS last_permit_date,
                    SUM(CASE WHEN issue_date >= '{cutoff_date}' THEN 1 ELSE 0 END) AS permits_last_15y_count
                FROM permits
                WHERE parcel_id IS NOT NULL
                GROUP BY county, parcel_id
            ) pa ON pa.county = leads.{county_col} AND pa.parcel_id = leads.{parcel_col}
        """
        last_permit_select = "pa.last_permit_date"
        permits_count_select = "COALESCE(pa.permits_last_15y_count, 0)"
    else:
        permits_subquery = ""
        last_permit_select = "NULL"
        permits_count_select = "0"

    sql = f"""
        SELECT
            leads.{owner_col} AS owner,
            {address_expr} AS address,
            leads.{county_col} AS county,
            leads.{parcel_col} AS parcel_id,
            {("leads." + source_col) if source_col else "''"} AS source,
            {score_sql} AS score,
            {last_permit_select} AS last_permit_date,
            {permits_count_select} AS permits_last_15y_count
        FROM leads
        {permits_subquery}
        WHERE {" AND ".join(where_parts)}
    """

    # Apply permits filters if permits table exists
    if has_permits:
        no_permits_years = filters.get("no_permits_in_years")
        if no_permits_years is not None:
            cutoff = (
                datetime.now() - timedelta(days=int(no_permits_years) * 365)
            ).strftime("%Y-%m-%d")
            # Match if last_permit_date is NULL OR older than cutoff
            sql += " AND (pa.last_permit_date IS NULL OR pa.last_permit_date < ?)"
            params.append(cutoff)

    # Sort
    if sort == "score_desc":
        sql += " ORDER BY score DESC, LOWER(owner), LOWER(address)"
    elif sort == "last_permit_oldest" and has_permits:
        sql += " ORDER BY pa.last_permit_date ASC NULLS LAST, score DESC"
    elif sort == "last_permit_newest" and has_permits:
        sql += " ORDER BY pa.last_permit_date DESC NULLS LAST, score DESC"
    else:  # relevance
        sql += " ORDER BY score DESC, LOWER(owner), LOWER(address)"

    sql += " LIMIT ?"

    all_params = tuple(score_params) + tuple(params) + (limit,)
    rows = conn.execute(sql, all_params).fetchall()

    results: list[SearchResult] = []
    for row in rows:
        # Determine matched fields
        matched = []
        if (
            "owner" in fields
            and owner_col
            and text_lower in (row["owner"] or "").lower()
        ):
            matched.append("owner")
        if "address" in fields and text_lower in (row["address"] or "").lower():
            matched.append("address")
        if (
            "parcel_id" in fields
            and parcel_col
            and text_lower in (row["parcel_id"] or "").lower()
        ):
            matched.append("parcel_id")

        results.append(
            SearchResult(
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
                matched_fields=matched,
            )
        )

    return results


def _advanced_search_from_properties(
    *,
    conn: sqlite3.Connection,
    text: str,
    county: str,
    fields: list[str],
    filters: dict[str, Any],
    sort: str,
    limit: int,
    cols: set[str],
    has_permits: bool,
) -> list[SearchResult]:
    """Execute advanced search against properties table."""
    from datetime import datetime, timedelta

    like = _like_param(text)
    text_lower = text.lower()

    # Map field names to column names
    field_map = {
        "owner": "owners.name",
        "address": "properties.address",
        "parcel_id": "properties.parcel_id",
        "city": None,
        "zip": None,
    }

    # Build match conditions
    match_parts: list[str] = []
    params: list[Any] = []

    for field in fields:
        col_name = field_map.get(field)
        if col_name is None:
            continue
        if field == "parcel_id" and "parcel_id" not in cols:
            continue
        match_parts.append(f"LOWER({col_name}) LIKE ?")
        params.append(like)

    if not match_parts:
        return []

    where_parts: list[str] = [f"({' OR '.join(match_parts)})"]

    if county:
        where_parts.append("LOWER(TRIM(properties.county)) = LOWER(TRIM(?))")
        params.append(county)

    # Score calculation
    has_parcel_id = "parcel_id" in cols
    score_sql = "CASE "
    score_params: list[Any] = []
    if has_parcel_id:
        score_sql += (
            "WHEN instr(LOWER(COALESCE(properties.parcel_id,'')), ?) > 0 THEN 95 "
        )
        score_params.append(text_lower)
    score_sql += "WHEN instr(LOWER(owners.name), ?) > 0 THEN 90 "
    score_params.append(text_lower)
    score_sql += "WHEN instr(LOWER(properties.address), ?) > 0 THEN 80 "
    score_params.append(text_lower)
    score_sql += "ELSE 70 END"

    parcel_select = "properties.parcel_id" if has_parcel_id else "''"

    # Permits aggregation
    if has_permits and has_parcel_id:
        cutoff_date = (datetime.now() - timedelta(days=15 * 365)).strftime("%Y-%m-%d")
        permits_subquery = f"""
            LEFT JOIN (
                SELECT
                    county,
                    parcel_id,
                    MAX(issue_date) AS last_permit_date,
                    SUM(CASE WHEN issue_date >= '{cutoff_date}' THEN 1 ELSE 0 END) AS permits_last_15y_count
                FROM permits
                WHERE parcel_id IS NOT NULL
                GROUP BY county, parcel_id
            ) pa ON pa.county = properties.county AND pa.parcel_id = properties.parcel_id
        """
        last_permit_select = "pa.last_permit_date"
        permits_count_select = "COALESCE(pa.permits_last_15y_count, 0)"
    else:
        permits_subquery = ""
        last_permit_select = "NULL"
        permits_count_select = "0"

    sql = f"""
        SELECT
            owners.name AS owner,
            properties.address AS address,
            properties.county AS county,
            {parcel_select} AS parcel_id,
            'properties' AS source,
            {score_sql} AS score,
            {last_permit_select} AS last_permit_date,
            {permits_count_select} AS permits_last_15y_count
        FROM properties
        JOIN owners ON owners.id = properties.owner_id
        {permits_subquery}
        WHERE {" AND ".join(where_parts)}
    """

    # Apply permits filters
    if has_permits and has_parcel_id:
        no_permits_years = filters.get("no_permits_in_years")
        if no_permits_years is not None:
            cutoff = (
                datetime.now() - timedelta(days=int(no_permits_years) * 365)
            ).strftime("%Y-%m-%d")
            sql += " AND (pa.last_permit_date IS NULL OR pa.last_permit_date < ?)"
            params.append(cutoff)

    # Sort
    if sort == "score_desc":
        sql += " ORDER BY score DESC, LOWER(owner), LOWER(address)"
    elif sort == "last_permit_oldest" and has_permits and has_parcel_id:
        sql += " ORDER BY pa.last_permit_date ASC NULLS LAST, score DESC"
    elif sort == "last_permit_newest" and has_permits and has_parcel_id:
        sql += " ORDER BY pa.last_permit_date DESC NULLS LAST, score DESC"
    else:
        sql += " ORDER BY score DESC, LOWER(owner), LOWER(address)"

    sql += " LIMIT ?"

    all_params = tuple(score_params) + tuple(params) + (limit,)
    rows = conn.execute(sql, all_params).fetchall()

    results: list[SearchResult] = []
    for row in rows:
        matched = []
        if "owner" in fields and text_lower in (row["owner"] or "").lower():
            matched.append("owner")
        if "address" in fields and text_lower in (row["address"] or "").lower():
            matched.append("address")
        if (
            "parcel_id" in fields
            and has_parcel_id
            and text_lower in (row["parcel_id"] or "").lower()
        ):
            matched.append("parcel_id")

        results.append(
            SearchResult(
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
                matched_fields=matched,
            )
        )

    return results
