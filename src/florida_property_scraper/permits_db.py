from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from florida_property_scraper.permits_models import PermitRecord


def ensure_permits_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS permits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            county TEXT NOT NULL,
            parcel_id TEXT NOT NULL,
            permit_id TEXT NOT NULL,
            permit_type TEXT,
            status TEXT,
            issued_date TEXT,
            finaled_date TEXT,
            source TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_permits_county_permit_unique
        ON permits(county, permit_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_permits_county_parcel_issued
        ON permits(county, parcel_id, issued_date)
        """
    )

    conn.commit()


def upsert_permits(conn: sqlite3.Connection, permits: Iterable[PermitRecord]) -> None:
    items = list(permits)
    if not items:
        return

    ensure_permits_schema(conn)

    for p in items:
        existing = conn.execute(
            "SELECT id FROM permits WHERE county = ? AND permit_id = ? LIMIT 1",
            (p.county, p.permit_id),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE permits
                SET parcel_id=?, permit_type=?, status=?, issued_date=?, finaled_date=?, source=?
                WHERE county=? AND permit_id=?
                """,
                (
                    p.parcel_id,
                    p.permit_type,
                    p.status,
                    p.issued_date,
                    p.finaled_date,
                    p.source,
                    p.county,
                    p.permit_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO permits (
                    county, parcel_id, permit_id, permit_type, status, issued_date, finaled_date, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    p.county,
                    p.parcel_id,
                    p.permit_id,
                    p.permit_type,
                    p.status,
                    p.issued_date,
                    p.finaled_date,
                    p.source,
                ),
            )

    conn.commit()


def get_last_permit_date_expr() -> str:
    # Returns a SQL subquery expression that yields (county, parcel_id, last_permit_date)
    return (
        "SELECT county, parcel_id, MAX(issued_date) AS last_permit_date "
        "FROM permits WHERE issued_date IS NOT NULL GROUP BY county, parcel_id"
    )
