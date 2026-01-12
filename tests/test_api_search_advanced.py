import sqlite3

from florida_property_scraper.api.app import app


def _init_leads_and_permits(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
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
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_county_parcel_unique
            ON leads(county, parcel_id)
            WHERE parcel_id IS NOT NULL
            """
        )

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

        # Leads
        conn.execute(
            "INSERT INTO leads (owner, address, county, parcel_id, source, score) VALUES (?,?,?,?,?,?)",
            (
                "Recent Permit Owner",
                "1 Future Ln",
                "Seminole",
                "SEM-RECENT",
                "https://example.test/SEM-RECENT",
                50,
            ),
        )
        conn.execute(
            "INSERT INTO leads (owner, address, county, parcel_id, source, score) VALUES (?,?,?,?,?,?)",
            (
                "Old Permit Owner",
                "2 Past St",
                "Seminole",
                "SEM-OLD",
                "https://example.test/SEM-OLD",
                60,
            ),
        )
        conn.execute(
            "INSERT INTO leads (owner, address, county, parcel_id, source, score) VALUES (?,?,?,?,?,?)",
            (
                "No Permit Owner",
                "3 None Ave",
                "Seminole",
                "SEM-NONE",
                "https://example.test/SEM-NONE",
                70,
            ),
        )

        # Permits: one very recent (future) and one very old.
        conn.execute(
            "INSERT INTO permits (county, parcel_id, permit_id, issued_date) VALUES (?,?,?,?)",
            ("Seminole", "SEM-RECENT", "P-RECENT", "2099-01-01"),
        )
        conn.execute(
            "INSERT INTO permits (county, parcel_id, permit_id, issued_date) VALUES (?,?,?,?)",
            ("Seminole", "SEM-OLD", "P-OLD", "2000-01-01"),
        )

        conn.commit()
    finally:
        conn.close()


def test_api_search_advanced_no_permits_filter(tmp_path, monkeypatch):
    if app is None:
        return

    db_path = tmp_path / "leads.sqlite"
    _init_leads_and_permits(str(db_path))

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    from fastapi.testclient import TestClient

    client = TestClient(app)

    r = client.post(
        "/api/search/advanced",
        json={
            "q": "",
            "counties": ["Seminole"],
            "filters": {"no_permits_in_years": 1},
            "limit": 50,
        },
    )
    assert r.status_code == 200
    data = r.json()

    parcel_ids = {row.get("parcel_id") for row in data}
    assert "SEM-RECENT" not in parcel_ids
    assert "SEM-OLD" in parcel_ids
    assert "SEM-NONE" in parcel_ids

    by_parcel = {row["parcel_id"]: row for row in data if row.get("parcel_id")}
    assert by_parcel["SEM-OLD"]["last_permit_date"] == "2000-01-01"
    assert by_parcel["SEM-NONE"]["last_permit_date"] is None
