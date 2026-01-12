from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from florida_property_scraper.leads_models import SearchResult


DEFAULT_DB_PATH = "/workspaces/Florida_Property_Scraper/leads.sqlite"


def get_db_path() -> str:
    path = (os.getenv("LEADS_SQLITE_PATH") or "").strip()
    if path:
        return path
    # Keep default consistent with repo root usage.
    return DEFAULT_DB_PATH


def connect(path: str | None = None) -> sqlite3.Connection:
    db_path = Path(path or get_db_path())
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def open_conn(path: str | None = None):
    conn = connect(path)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


@dataclass(frozen=True)
class LeadsSchema:
    kind: str  # "new" | "legacy" | "missing"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _get_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def detect_leads_schema(conn: sqlite3.Connection) -> LeadsSchema:
    cols = _get_columns(conn, "leads")
    if not cols:
        return LeadsSchema(kind="missing")

    # New schema columns.
    if {"owner", "address", "county", "score"}.issubset(cols):
        return LeadsSchema(kind="new")

    # Legacy schema columns.
    if {"owner_name", "county"}.issubset(cols) and (
        "situs_address" in cols or "mailing_address" in cols
    ):
        return LeadsSchema(kind="legacy")

    # Unknown/other; treat as legacy-ish best effort.
    return LeadsSchema(kind="legacy")


def ensure_schema(conn: sqlite3.Connection) -> None:
    schema = detect_leads_schema(conn)
    if schema.kind != "missing":
        # Do not mutate an existing leads schema; we support searching both.
        return

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL,
            address TEXT NOT NULL,
            county TEXT NOT NULL,
            parcel_id TEXT,
            source TEXT,
            score INTEGER
        )
        """
    )
    # Best-effort uniqueness where parcel_id is known.
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_county_parcel_unique
        ON leads(county, parcel_id)
        WHERE parcel_id IS NOT NULL
        """
    )
    conn.commit()


def _clamp_limit(limit: int | None, *, default: int = 50, cap: int = 200) -> int:
    if limit is None:
        return default
    try:
        value = int(limit)
    except Exception:
        return default
    if value <= 0:
        return default
    return min(value, cap)


def upsert_many(conn: sqlite3.Connection, rows: Iterable[SearchResult]) -> None:
    schema = detect_leads_schema(conn)
    if schema.kind == "missing":
        ensure_schema(conn)
        schema = detect_leads_schema(conn)

    items = list(rows)
    if not items:
        return

    if schema.kind == "new":
        for r in items:
            if r.parcel_id:
                # Check if exists with this county+parcel_id.
                existing = conn.execute(
                    "SELECT id FROM leads WHERE county = ? AND parcel_id = ? LIMIT 1",
                    (r.county, r.parcel_id),
                ).fetchone()
                if existing:
                    # Update existing record.
                    conn.execute(
                        """
                        UPDATE leads
                        SET owner=?, address=?, source=?, score=?
                        WHERE county=? AND parcel_id=?
                        """,
                        (
                            r.owner,
                            r.address,
                            r.source,
                            int(r.score),
                            r.county,
                            r.parcel_id,
                        ),
                    )
                else:
                    # Insert new record.
                    conn.execute(
                        """
                        INSERT INTO leads (owner, address, county, parcel_id, source, score)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            r.owner,
                            r.address,
                            r.county,
                            r.parcel_id,
                            r.source,
                            int(r.score),
                        ),
                    )
            else:
                conn.execute(
                    """
                    INSERT INTO leads (owner, address, county, parcel_id, source, score)
                    VALUES (?, ?, ?, NULL, ?, ?)
                    """,
                    (
                        r.owner,
                        r.address,
                        r.county,
                        r.source,
                        int(r.score),
                    ),
                )
        conn.commit()
        return

    # Legacy schema: insert best-effort into legacy leads table.
    cols = _get_columns(conn, "leads")

    # Choose address destination.
    address_col = "situs_address" if "situs_address" in cols else "mailing_address"
    source_col = "source_url" if "source_url" in cols else (
        "property_url" if "property_url" in cols else None
    )
    score_col = "lead_score" if "lead_score" in cols else None

    # Legacy tables often dedupe on dedupe_key.
    has_dedupe = "dedupe_key" in cols

    for r in items:
        values: dict[str, object] = {
            "county": r.county,
            "owner_name": r.owner,
            address_col: r.address,
            "parcel_id": r.parcel_id,
        }
        if source_col:
            values[source_col] = r.source
        if score_col:
            values[score_col] = int(r.score)
        if has_dedupe:
            # Prefer stable dedupe by county+parcel_id when possible.
            key = f"{r.county.lower()}::{(r.parcel_id or '').strip().lower()}" or None
            if r.parcel_id:
                values["dedupe_key"] = key
            else:
                # Best-effort key when parcel_id missing.
                values["dedupe_key"] = f"{r.county.lower()}::{r.owner.lower()}::{r.address.lower()}"

        insert_cols = [c for c in values.keys() if c in cols]
        if not insert_cols:
            continue

        placeholders = ",".join(["?"] * len(insert_cols))
        columns_sql = ",".join(insert_cols)

        payload = [values[c] for c in insert_cols]
        if has_dedupe and "dedupe_key" in insert_cols:
            conn.execute(
                f"INSERT INTO leads ({columns_sql}) VALUES ({placeholders}) ON CONFLICT(dedupe_key) DO UPDATE SET "
                + ", ".join([f"{c}=excluded.{c}" for c in insert_cols if c != "dedupe_key"]),
                payload,
            )
        else:
            conn.execute(
                f"INSERT INTO leads ({columns_sql}) VALUES ({placeholders})",
                payload,
            )

    conn.commit()


def search(
    conn: sqlite3.Connection,
    *,
    q: str,
    county: str | None = None,
    limit: int | None = None,
) -> list[SearchResult]:
    schema = detect_leads_schema(conn)
    if schema.kind == "missing":
        return []

    q_clean = (q or "").strip()
    county_clean = (county or "").strip()
    lim = _clamp_limit(limit)

    if schema.kind == "new":
        where = []
        params: list[object] = []

        if county_clean:
            where.append("lower(county) = lower(?)")
            params.append(county_clean)

        if q_clean:
            where.append(
                "(lower(owner) LIKE lower(?) OR lower(address) LIKE lower(?) OR lower(ifnull(parcel_id,'')) LIKE lower(?))"
            )
            like = f"%{q_clean}%"
            params.extend([like, like, like])

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        rows = conn.execute(
            "SELECT owner, address, county, parcel_id, source, score FROM leads"
            + where_sql
            + " ORDER BY score DESC, id DESC LIMIT ?",
            (*params, lim),
        ).fetchall()

        return [
            SearchResult(
                owner=str(r["owner"] or ""),
                address=str(r["address"] or ""),
                county=str(r["county"] or ""),
                parcel_id=(str(r["parcel_id"]) if r["parcel_id"] else None),
                source=(str(r["source"]) if r["source"] else None),
                score=int(r["score"] or 0),
            )
            for r in rows
        ]

    # Legacy search.
    cols = _get_columns(conn, "leads")
    owner_col = "owner_name" if "owner_name" in cols else ("owner" if "owner" in cols else None)
    if not owner_col:
        return []

    address_col = "situs_address" if "situs_address" in cols else (
        "mailing_address" if "mailing_address" in cols else None
    )
    parcel_col = "parcel_id" if "parcel_id" in cols else None
    source_col = "source_url" if "source_url" in cols else (
        "property_url" if "property_url" in cols else None
    )
    score_col = "lead_score" if "lead_score" in cols else ("score" if "score" in cols else None)

    select_cols = ["county", owner_col]
    if address_col:
        select_cols.append(address_col)
    if parcel_col:
        select_cols.append(parcel_col)
    if source_col:
        select_cols.append(source_col)
    if score_col:
        select_cols.append(score_col)

    where = []
    params: list[object] = []

    if county_clean and "county" in cols:
        where.append("lower(county) = lower(?)")
        params.append(county_clean)

    if q_clean:
        like = f"%{q_clean}%"
        parts = [f"lower({owner_col}) LIKE lower(?)"]
        params.append(like)
        if address_col:
            parts.append(f"lower({address_col}) LIKE lower(?)")
            params.append(like)
        if parcel_col:
            parts.append(f"lower(ifnull({parcel_col},'')) LIKE lower(?)")
            params.append(like)
        where.append("(" + " OR ".join(parts) + ")")

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    order_col = score_col or owner_col
    sql = (
        f"SELECT {', '.join(select_cols)} FROM leads"
        + where_sql
        + f" ORDER BY {order_col} DESC LIMIT ?"
    )
    rows = conn.execute(sql, (*params, lim)).fetchall()

    out: list[SearchResult] = []
    for r in rows:
        addr = ""
        if address_col and address_col in r.keys():
            addr = str(r[address_col] or "")
        parcel = None
        if parcel_col and parcel_col in r.keys() and r[parcel_col]:
            parcel = str(r[parcel_col])
        source = None
        if source_col and source_col in r.keys() and r[source_col]:
            source = str(r[source_col])
        score = 0
        if score_col and score_col in r.keys() and r[score_col] is not None:
            try:
                score = int(r[score_col])
            except Exception:
                score = 0

        out.append(
            SearchResult(
                owner=str(r[owner_col] or ""),
                address=addr,
                county=str(r["county"] or ""),
                parcel_id=parcel,
                source=source,
                score=score,
            )
        )

    return out
