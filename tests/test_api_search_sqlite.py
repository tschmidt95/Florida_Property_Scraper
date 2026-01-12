import sqlite3
from pathlib import Path

import pytest


pytest.importorskip("fastapi")


def _init_properties_db(path: Path, *, include_parcel_id: bool = False) -> None:
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE owners (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
            """
        )

        parcel_col = ", parcel_id TEXT" if include_parcel_id else ""
        cur.execute(
            f"""
            CREATE TABLE properties (
                id INTEGER PRIMARY KEY,
                county TEXT NOT NULL,
                owner_id INTEGER NOT NULL,
                address TEXT NOT NULL{parcel_col},
                FOREIGN KEY(owner_id) REFERENCES owners(id)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _insert_property(
    path: Path,
    *,
    owner_name: str,
    county: str,
    address: str,
    parcel_id: str | None = None,
) -> None:
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO owners (name) VALUES (?)", (owner_name,))
        cur.execute("SELECT id FROM owners WHERE name=?", (owner_name,))
        row = cur.fetchone()
        assert row is not None
        owner_id = int(row[0])

        # Support both schemas (with/without parcel_id)
        cols = [r[1] for r in cur.execute("PRAGMA table_info(properties)").fetchall()]
        if "parcel_id" in cols:
            cur.execute(
                "INSERT INTO properties (county, owner_id, address, parcel_id) VALUES (?, ?, ?, ?)",
                (county, owner_id, address, parcel_id or ""),
            )
        else:
            cur.execute(
                "INSERT INTO properties (county, owner_id, address) VALUES (?, ?, ?)",
                (county, owner_id, address),
            )

        conn.commit()
    finally:
        conn.close()


def _client():
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    from florida_property_scraper.api.routes.search import router

    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app)


def test_api_search_empty_q_returns_empty_list(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"
    _init_properties_db(db_path)
    _insert_property(
        db_path, owner_name="John Smith", county="Orange", address="123 Main St"
    )

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    client = _client()
    r = client.get("/api/search", params={"q": "   ", "county": "Orange"})
    assert r.status_code == 200
    assert r.json() == []


def test_api_search_matches_owner_case_insensitive(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"
    _init_properties_db(db_path)
    _insert_property(
        db_path, owner_name="John Smith", county="Orange", address="123 Main St"
    )

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    client = _client()
    r = client.get("/api/search", params={"q": "SMITH", "county": "Orange"})
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["owner"] == "John Smith"
    assert data[0]["county"] == "Orange"
    assert "score" in data[0]


def test_api_search_matches_address_and_parcel_id(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"
    _init_properties_db(db_path, include_parcel_id=True)

    _insert_property(
        db_path,
        owner_name="Maria Garcia",
        county="Orange",
        address="9 Palm Ave",
        parcel_id="PID-123",
    )

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    client = _client()

    r_addr = client.get("/api/search", params={"q": "palm", "county": "Orange"})
    assert r_addr.status_code == 200
    assert len(r_addr.json()) == 1

    r_pid = client.get("/api/search", params={"q": "pid-123", "county": "Orange"})
    assert r_pid.status_code == 200
    data = r_pid.json()
    assert len(data) == 1
    assert data[0].get("parcel_id") == "PID-123"


def test_api_search_county_filter(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"
    _init_properties_db(db_path)
    _insert_property(
        db_path, owner_name="John Smith", county="Orange", address="123 Main St"
    )
    _insert_property(
        db_path, owner_name="John Smith", county="Broward", address="456 Ocean Dr"
    )

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    client = _client()
    r = client.get("/api/search", params={"q": "smith", "county": "Orange"})
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["address"] == "123 Main St"


def test_api_search_limit_is_capped_to_200(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"
    _init_properties_db(db_path)

    for i in range(250):
        _insert_property(
            db_path,
            owner_name=f"Person {i} Smith",
            county="Orange",
            address=f"{i} Main St",
        )

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    client = _client()
    r = client.get(
        "/api/search", params={"q": "smith", "county": "Orange", "limit": 999}
    )
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 200
