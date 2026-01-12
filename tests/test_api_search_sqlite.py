import sqlite3

from florida_property_scraper.api.app import app


def _init_new_schema(db_path: str) -> None:
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
            "INSERT INTO leads (owner, address, county, parcel_id, source, score) VALUES (?,?,?,?,?,?)",
            (
                "Jane Doe",
                "123 Test St",
                "Seminole",
                "SEM-123",
                "https://example.test/record/SEM-123",
                90,
            ),
        )
        conn.execute(
            "INSERT INTO leads (owner, address, county, parcel_id, source, score) VALUES (?,?,?,?,?,?)",
            (
                "Other Owner",
                "999 Elsewhere Ave",
                "Orange",
                "ORG-999",
                "https://example.test/record/ORG-999",
                10,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_api_search_reads_sqlite_new_schema(tmp_path, monkeypatch):
    if app is None:
        return

    db_path = tmp_path / "leads.sqlite"
    _init_new_schema(str(db_path))

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    from fastapi.testclient import TestClient

    client = TestClient(app)

    r = client.get("/api/search", params={"q": "jane", "limit": 50})
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["owner"] == "Jane Doe"
    assert data[0]["county"] == "Seminole"
    assert data[0]["parcel_id"] == "SEM-123"

    # County filter.
    r = client.get("/api/search", params={"q": "owner", "county": "Orange"})
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["county"] == "Orange"
