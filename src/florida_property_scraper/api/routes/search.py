try:
    from fastapi import APIRouter
    from fastapi import HTTPException

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    APIRouter = None
    HTTPException = None
    FASTAPI_AVAILABLE = False

import os
import sqlite3
from pathlib import Path
from typing import Any
from datetime import date

from pydantic import BaseModel
from pydantic import Field


router = APIRouter(tags=["search"]) if FASTAPI_AVAILABLE else None


class SearchResult(BaseModel):
    owner: str
    address: str
    county: str
    score: int
    parcel_id: str | None = None
    source: str | None = None


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
    fields: list[str] = Field(default_factory=list)
    filters: AdvancedSearchFilters = Field(default_factory=AdvancedSearchFilters)
    sort: str = "relevance"
    limit: int = 50


class AdvancedSearchResult(SearchResult):
    last_permit_date: str | None = None
    permits_last_15y_count: int = 0
    matched_fields: list[str] = Field(default_factory=list)


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


def _minus_years_iso(years: int) -> str:
    # Robust-ish year subtraction for date comparisons.
    today = date.today()
    try:
        return today.replace(year=today.year - years).isoformat()
    except ValueError:
        # e.g. Feb 29 -> Feb 28
        return today.replace(month=2, day=28, year=today.year - years).isoformat()


def _norm_field_list(fields: list[str]) -> list[str]:
    out: list[str] = []
    for f in fields or []:
        key = str(f or "").strip().lower()
        if not key:
            continue
        if key not in out:
            out.append(key)
    return out


def _build_leads_field_exprs(cols: set[str]) -> dict[str, str]:
    owner_col = "leads.owner_name" if "owner_name" in cols else None
    county_col = "leads.county" if "county" in cols else None
    situs_col = "leads.situs_address" if "situs_address" in cols else None
    mailing_col = "leads.mailing_address" if "mailing_address" in cols else None
    parcel_col = "leads.parcel_id" if "parcel_id" in cols else None
    source_col = (
        "leads.source_url"
        if "source_url" in cols
        else ("leads.property_url" if "property_url" in cols else None)
    )
    score_col = "leads.lead_score" if "lead_score" in cols else None

    if not owner_col or not county_col:
        return {}

    address_parts: list[str] = []
    if situs_col:
        address_parts.append(f"NULLIF({situs_col}, '')")
    if mailing_col:
        address_parts.append(f"NULLIF({mailing_col}, '')")
    address_expr = "COALESCE(" + ", ".join(address_parts + ["''"]) + ")"

    out = {
        "owner": owner_col,
        "county": county_col,
        "address": address_expr,
        "parcel_id": parcel_col or "''",
        "source": source_col or "''",
    }

    # Optional structured fields if present.
    if "city" in cols:
        out["city"] = "leads.city"
    if "zip" in cols:
        out["zip"] = "leads.zip"
    if score_col:
        out["score_col"] = score_col

    return out


def _score_and_matches(
    *, q: str, row: sqlite3.Row, field_values: dict[str, str]
) -> tuple[int, list[str]]:
    qn = (q or "").strip().lower()
    if not qn:
        return (0, [])

    matched: list[str] = []
    for fname, value in field_values.items():
        if qn in (value or "").lower():
            matched.append(fname)

    # Heuristic score ordering: parcel_id > owner > address > others.
    score = 0
    if "parcel_id" in matched:
        score = 95
    elif "owner" in matched:
        score = 90
    elif "address" in matched:
        score = 80
    elif matched:
        score = 70

    # Prefer stored lead_score if available.
    if "score_col" in row.keys():
        try:
            v = row["score_col"]
            if v is not None:
                score = max(0, min(int(v), 100))
        except Exception:
            pass

    return (score, matched)


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

    @router.post("/search/advanced", response_model=list[AdvancedSearchResult])
    def search_advanced(payload: AdvancedSearchRequest) -> list[AdvancedSearchResult]:
        fields = _norm_field_list(payload.fields)
        if not fields:
            raise HTTPException(
                status_code=400, detail="fields must be a non-empty array"
            )

        cn = _norm(payload.county)
        qn = _norm(payload.text)
        lim = _clamp_limit(payload.limit)

        db_path = _get_db_path()
        if not db_path.exists():
            return []

        conn = _connect(db_path)
        try:
            if not _table_exists(conn, "leads"):
                return []

            leads_cols = _table_columns(conn, "leads")
            exprs = _build_leads_field_exprs(leads_cols)
            if not exprs:
                return []

            allowed_fields = {"owner", "address", "parcel_id", "city", "zip"}
            unsupported = [f for f in fields if f not in allowed_fields]
            if unsupported:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported fields: {unsupported}",
                )

            # Ensure requested structured fields exist.
            for f in ("city", "zip"):
                if f in fields and f not in exprs:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Field {f!r} is not available in this database",
                    )

            # Build SELECT.
            select_parts = [
                f"{exprs['owner']} AS owner",
                f"{exprs['address']} AS address",
                f"{exprs['county']} AS county",
                f"{exprs['parcel_id']} AS parcel_id",
                f"{exprs['source']} AS source",
            ]
            if "score_col" in exprs:
                select_parts.append(f"{exprs['score_col']} AS score_col")
            if "city" in exprs:
                select_parts.append(f"{exprs['city']} AS city")
            if "zip" in exprs:
                select_parts.append(f"{exprs['zip']} AS zip")

            has_permits = _table_exists(conn, "permits")
            cutoff_15y = _minus_years_iso(15)

            join_sql = ""
            join_params: list[Any] = []
            if has_permits:
                join_sql = """
                    LEFT JOIN (
                        SELECT
                            LOWER(TRIM(county)) AS p_county,
                            NULLIF(TRIM(parcel_id), '') AS p_parcel_id,
                            MAX(COALESCE(NULLIF(TRIM(final_date),''), NULLIF(TRIM(issue_date),''))) AS last_permit_date,
                            SUM(
                                CASE
                                    WHEN DATE(COALESCE(NULLIF(TRIM(final_date),''), NULLIF(TRIM(issue_date),''))) >= DATE(?) THEN 1
                                    ELSE 0
                                END
                            ) AS permits_last_15y_count
                        FROM permits
                        GROUP BY LOWER(TRIM(county)), NULLIF(TRIM(parcel_id), '')
                    ) pa
                    ON pa.p_county = LOWER(TRIM(leads.county))
                   AND pa.p_parcel_id = NULLIF(TRIM(leads.parcel_id), '')
                """
                join_params.append(cutoff_15y)

                select_parts.append("pa.last_permit_date AS last_permit_date")
                select_parts.append(
                    "COALESCE(pa.permits_last_15y_count, 0) AS permits_last_15y_count"
                )
            else:
                select_parts.append("NULL AS last_permit_date")
                select_parts.append("0 AS permits_last_15y_count")

            where_parts: list[str] = []
            params: list[Any] = []

            if cn:
                where_parts.append("LOWER(TRIM(leads.county)) = LOWER(TRIM(?))")
                params.append(cn)

            # Structured filters.
            if payload.filters.city:
                if "city" not in exprs:
                    raise HTTPException(
                        status_code=400,
                        detail="city filter is not available in this database",
                    )
                where_parts.append(f"LOWER(TRIM({exprs['city']})) = LOWER(TRIM(?))")
                params.append(str(payload.filters.city))
            if payload.filters.zip:
                if "zip" not in exprs:
                    raise HTTPException(
                        status_code=400,
                        detail="zip filter is not available in this database",
                    )
                where_parts.append(f"LOWER(TRIM({exprs['zip']})) = LOWER(TRIM(?))")
                params.append(str(payload.filters.zip))

            # Text match across explicit fields only.
            if qn:
                like = _like_param(qn)
                match_parts: list[str] = []
                for f in fields:
                    if f == "owner":
                        match_parts.append(
                            f"LOWER(COALESCE({exprs['owner']},'')) LIKE ?"
                        )
                        params.append(like)
                    elif f == "address":
                        match_parts.append(f"LOWER({exprs['address']}) LIKE ?")
                        params.append(like)
                    elif f == "parcel_id":
                        match_parts.append(
                            f"LOWER(COALESCE({exprs['parcel_id']},'')) LIKE ?"
                        )
                        params.append(like)
                    elif f == "city":
                        match_parts.append(
                            f"LOWER(COALESCE({exprs['city']},'')) LIKE ?"
                        )
                        params.append(like)
                    elif f == "zip":
                        match_parts.append(f"LOWER(COALESCE({exprs['zip']},'')) LIKE ?")
                        params.append(like)
                where_parts.append(f"({' OR '.join(match_parts)})")

            # Permits-based filters.
            if payload.filters.no_permits_in_years is not None:
                try:
                    years = int(payload.filters.no_permits_in_years)
                except Exception:
                    years = 0
                years = max(0, min(years, 120))
                # If permits aren't available, last_permit_date is always NULL -> match all.
                if has_permits:
                    cutoff_n = _minus_years_iso(years)
                    # Treat NULL last_permit_date as MATCH by default.
                    where_parts.append(
                        "(pa.last_permit_date IS NULL OR DATE(pa.last_permit_date) < DATE(?))"
                    )
                    params.append(cutoff_n)

            if payload.filters.permit_status or payload.filters.permit_types:
                if not has_permits:
                    return []
                status_vals = payload.filters.permit_status or []
                type_vals = payload.filters.permit_types or []

                exists_parts = [
                    "LOWER(TRIM(p.county)) = LOWER(TRIM(leads.county))",
                    "NULLIF(TRIM(p.parcel_id), '') = NULLIF(TRIM(leads.parcel_id), '')",
                ]
                exists_params: list[Any] = []
                if status_vals:
                    placeholders = ",".join(["?"] * len(status_vals))
                    exists_parts.append(f"p.status IN ({placeholders})")
                    exists_params.extend(status_vals)
                if type_vals:
                    placeholders = ",".join(["?"] * len(type_vals))
                    exists_parts.append(f"p.permit_type IN ({placeholders})")
                    exists_params.extend(type_vals)

                where_parts.append(
                    "EXISTS (SELECT 1 FROM permits p WHERE "
                    + " AND ".join(exists_parts)
                    + ")"
                )
                params.extend(exists_params)

            sql = "SELECT " + ", ".join(select_parts) + " FROM leads " + join_sql
            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)

            # Pull more than needed, then compute matched_fields and score in Python.
            sql += " LIMIT ?"
            query_params = tuple(join_params) + tuple(params) + (min(lim * 5, 1000),)
            rows = conn.execute(sql, query_params).fetchall()

            out: list[AdvancedSearchResult] = []
            for row in rows:
                # Build values only for requested fields (no guessing).
                field_values: dict[str, str] = {}
                if "owner" in fields:
                    field_values["owner"] = str(row["owner"] or "")
                if "address" in fields:
                    field_values["address"] = str(row["address"] or "")
                if "parcel_id" in fields:
                    field_values["parcel_id"] = str(row["parcel_id"] or "")
                if "city" in fields:
                    field_values["city"] = (
                        str(row["city"] or "") if "city" in row.keys() else ""
                    )
                if "zip" in fields:
                    field_values["zip"] = (
                        str(row["zip"] or "") if "zip" in row.keys() else ""
                    )

                score, matched = _score_and_matches(
                    q=qn, row=row, field_values=field_values
                )

                min_score = payload.filters.min_score
                if min_score is not None:
                    try:
                        ms = int(min_score)
                    except Exception:
                        ms = 0
                    if score < ms:
                        continue

                out.append(
                    AdvancedSearchResult(
                        owner=str(row["owner"] or ""),
                        address=str(row["address"] or ""),
                        county=str(row["county"] or ""),
                        score=int(score),
                        parcel_id=str(row["parcel_id"]) if row["parcel_id"] else None,
                        source=str(row["source"]) if row["source"] else None,
                        last_permit_date=str(row["last_permit_date"])
                        if row["last_permit_date"]
                        else None,
                        permits_last_15y_count=int(row["permits_last_15y_count"] or 0),
                        matched_fields=matched,
                    )
                )

            sort_key = (payload.sort or "relevance").strip().lower()

            def dt_key(v: str | None) -> str:
                return v or ""

            if sort_key == "score_desc":
                out.sort(key=lambda r: (-r.score, r.owner.lower(), r.address.lower()))
            elif sort_key == "last_permit_oldest":
                out.sort(
                    key=lambda r: (
                        0 if r.last_permit_date is None else 1,
                        dt_key(r.last_permit_date),
                        -r.score,
                    )
                )
            elif sort_key == "last_permit_newest":
                out.sort(
                    key=lambda r: (
                        0 if r.last_permit_date is None else 1,
                        dt_key(r.last_permit_date),
                        r.score,
                    )
                )
                out.reverse()
            else:
                # relevance
                out.sort(
                    key=lambda r: (
                        -len(r.matched_fields),
                        -r.score,
                        r.owner.lower(),
                        r.address.lower(),
                    )
                )

            return out[:lim]
        finally:
            conn.close()
