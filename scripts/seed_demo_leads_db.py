#!/usr/bin/env python3
"""Seed a demo SQLite leads table for UI development.

This script is intentionally deterministic so screenshots/demos are stable.

DB path resolution order:
  LEADS_SQLITE_PATH -> LEADS_DB -> PA_DB -> /workspaces/Florida_Property_Scraper/leads.sqlite

Idempotent behavior:
  If the `leads` table exists and has >= 200 rows, do nothing.
  Otherwise, (re)create/populate it with ~300 demo rows.
"""

from __future__ import annotations

import os
import random
import sqlite3
from pathlib import Path


_DB_ENV_VARS = ("LEADS_SQLITE_PATH", "LEADS_DB", "PA_DB")
_DEFAULT_DB_PATH = Path("/workspaces/Florida_Property_Scraper/leads.sqlite")

_COUNTIES = [
    "Orange",
    "Seminole",
    "Osceola",
    "Hillsborough",
    "Duval",
    "Broward",
    "Palm Beach",
]


def _get_db_path() -> Path:
    for name in _DB_ENV_VARS:
        value = os.getenv(name)
        if value:
            return Path(value)
    return _DEFAULT_DB_PATH


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _count_rows(conn: sqlite3.Connection, table: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(1) AS n FROM {table}").fetchone()
    except sqlite3.Error:
        return 0
    if not row:
        return 0
    return int(row[0] or 0)


def _create_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY,
            owner TEXT NOT NULL,
            address TEXT NOT NULL,
            county TEXT NOT NULL,
            parcel_id TEXT,
            source TEXT,
            score INTEGER NOT NULL DEFAULT 0
        )
        """
    )


def _build_demo_owner(rng: random.Random) -> str:
    first = [
        "John",
        "Maria",
        "James",
        "Patricia",
        "Robert",
        "Jennifer",
        "Michael",
        "Linda",
        "William",
        "Elizabeth",
        "David",
        "Barbara",
        "Richard",
        "Susan",
        "Joseph",
        "Jessica",
        "Thomas",
        "Sarah",
        "Charles",
        "Karen",
    ]
    last = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Hernandez",
        "Lopez",
        "Gonzalez",
        "Wilson",
        "Anderson",
        "Thomas",
        "Taylor",
        "Moore",
        "Jackson",
        "Martin",
    ]

    # Mix in some entities so results feel more realistic.
    if rng.random() < 0.22:
        nouns = [
            "Holdings",
            "Properties",
            "Investments",
            "Realty",
            "Capital",
            "Development",
            "Enterprises",
            "Group",
        ]
        suffix = ["LLC", "Inc", "LP", "Trust"]
        core = f"{rng.choice(last)} {rng.choice(nouns)}"
        if rng.random() < 0.55:
            core = f"{rng.choice(last)} & {rng.choice(last)} {rng.choice(nouns)}"
        return f"{core} {rng.choice(suffix)}"

    return f"{rng.choice(first)} {rng.choice(last)}"


def _build_demo_address(rng: random.Random) -> str:
    number = rng.randint(10, 9999)
    street = rng.choice(
        [
            "Main",
            "Oak",
            "Pine",
            "Maple",
            "Cedar",
            "Lakeview",
            "Sunset",
            "Palm",
            "Magnolia",
            "Washington",
            "Ridge",
            "River",
            "Park",
            "Highland",
            "Orange",
            "Seminole",
        ]
    )
    st_type = rng.choice(["St", "Ave", "Blvd", "Dr", "Ln", "Ct", "Way"])

    # Optional unit, a common Florida thing.
    unit = ""
    if rng.random() < 0.18:
        unit = f", Apt {rng.randint(1, 30)}"

    return f"{number} {street} {st_type}{unit}"


def _build_demo_parcel_id(rng: random.Random, county: str, i: int) -> str:
    # Deterministic-ish parcel ID format that looks like county assessor IDs.
    county_code = "".join([c for c in county.upper() if c.isalpha()])[:3].ljust(3, "X")
    return f"{county_code}-{rng.randint(10, 99)}-{rng.randint(1000, 9999)}-{i:04d}"


def _build_demo_source(rng: random.Random, county: str) -> str:
    host = rng.choice(
        [
            "https://example-county.gov",
            "https://records.example.gov",
            "https://property.example.org",
        ]
    )
    slug = county.lower().replace(" ", "-")
    return f"{host}/{slug}/search"


def main() -> int:
    db_path = _get_db_path()

    conn = _connect(db_path)
    try:
        if _table_exists(conn, "leads") and _count_rows(conn, "leads") >= 200:
            print(f"leads table already has >= 200 rows; leaving {db_path} unchanged")
            return 0

        _create_table(conn)

        # If the table exists but is too small, reset it for deterministic results.
        conn.execute("DELETE FROM leads")

        rng = random.Random(1337)

        rows: list[tuple[str, str, str, str, str, int]] = []
        target = 300
        for i in range(target):
            county = _COUNTIES[i % len(_COUNTIES)]
            owner = _build_demo_owner(rng)
            address = _build_demo_address(rng)
            parcel_id = _build_demo_parcel_id(rng, county, i)
            source = _build_demo_source(rng, county)

            # Weighted-ish scores so ordering looks real.
            base = rng.randint(10, 95)
            if "LLC" in owner or "Trust" in owner:
                base = min(100, base + 5)
            score = int(base)

            rows.append((owner, address, county, parcel_id, source, score))

        conn.executemany(
            """
            INSERT INTO leads (owner, address, county, parcel_id, source, score)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()

        print(f"seeded {len(rows)} demo leads into {db_path}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
