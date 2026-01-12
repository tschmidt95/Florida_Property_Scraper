import sqlite3
from pathlib import Path

import pytest


pytest.importorskip("fastapi")


def _init_leads_and_permits_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                county TEXT,
                owner_name TEXT,
                situs_address TEXT,
                parcel_id TEXT,
                source_url TEXT,
                lead_score INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE permits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                county TEXT NOT NULL,
                parcel_id TEXT,
                address TEXT,
                permit_number TEXT NOT NULL,
                permit_type TEXT,
                status TEXT,
                issue_date TEXT,
                final_date TEXT,
                description TEXT,
                source TEXT,
                raw TEXT
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX ux_permits_county_permit_number ON permits(county, permit_number)"
        )
        conn.commit()
    finally:
        conn.close()


def _insert_lead(
    path: Path,
    *,
    county: str,
    owner_name: str,
    situs_address: str,
    parcel_id: str,
    score: int,
) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "INSERT INTO leads (county, owner_name, situs_address, parcel_id, source_url, lead_score) VALUES (?, ?, ?, ?, ?, ?)",
            (county, owner_name, situs_address, parcel_id, "src://lead", score),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_permit(
    path: Path,
    *,
    county: str,
    parcel_id: str,
    permit_number: str,
    permit_type: str,
    status: str,
    issue_date: str,
    final_date: str | None,
) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            INSERT INTO permits (
                county, parcel_id, address, permit_number, permit_type, status, issue_date, final_date, description, source, raw
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                county,
                parcel_id,
                "addr://" + parcel_id,
                permit_number,
                permit_type,
                status,
                issue_date,
                final_date or "",
                "desc",
                "src://permit",
                "raw",
            ),
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


def test_api_search_advanced_respects_field_selection(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"
    _init_leads_and_permits_db(db_path)

    _insert_lead(
        db_path,
        county="Orange",
        owner_name="John Smith",
        situs_address="123 Main St",
        parcel_id="PID-1",
        score=88,
    )

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))
    client = _client()

    # Search in owner field only
    r_owner = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": "main",
            "fields": ["owner"],
            "filters": {},
            "sort": "relevance",
            "limit": 50,
        },
    )
    assert r_owner.status_code == 200
    assert r_owner.json() == []

    # Search in address only
    r_addr = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": "main",
            "fields": ["address"],
            "filters": {},
            "sort": "relevance",
            "limit": 50,
        },
    )
    assert r_addr.status_code == 200
    data = r_addr.json()
    assert len(data) == 1
    assert data[0]["owner"] == "John Smith"
    assert "address" in data[0]
    assert "matched_fields" in data[0]
    assert data[0]["matched_fields"] == ["address"]


def test_api_search_advanced_no_permits_in_years(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"
    _init_leads_and_permits_db(db_path)

    _insert_lead(
        db_path,
        county="Orange",
        owner_name="Recent Permit",
        situs_address="1 New St",
        parcel_id="PID-1",
        score=50,
    )
    _insert_lead(
        db_path,
        county="Orange",
        owner_name="Old Permit",
        situs_address="2 Old St",
        parcel_id="PID-2",
        score=50,
    )
    _insert_lead(
        db_path,
        county="Orange",
        owner_name="No Permit",
        situs_address="3 None St",
        parcel_id="PID-3",
        score=50,
    )

    _insert_permit(
        db_path,
        county="Orange",
        parcel_id="PID-1",
        permit_number="P-1",
        permit_type="Building",
        status="Closed",
        issue_date="2024-01-01",
        final_date="2024-02-01",
    )
    _insert_permit(
        db_path,
        county="Orange",
        parcel_id="PID-2",
        permit_number="P-2",
        permit_type="Building",
        status="Closed",
        issue_date="2000-01-01",
        final_date="2000-02-01",
    )

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))
    client = _client()

    r = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": None,
            "fields": ["owner"],
            "filters": {"no_permits_in_years": 15},
            "sort": "relevance",
            "limit": 50,
        },
    )
    assert r.status_code == 200
    data = r.json()

    owners = {row["owner"] for row in data}
    assert "Old Permit" in owners
    assert "No Permit" in owners
    assert "Recent Permit" not in owners


def test_api_search_advanced_sorting_by_last_permit_date(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"
    _init_leads_and_permits_db(db_path)

    _insert_lead(
        db_path,
        county="Orange",
        owner_name="A",
        situs_address="A St",
        parcel_id="PID-1",
        score=1,
    )
    _insert_lead(
        db_path,
        county="Orange",
        owner_name="B",
        situs_address="B St",
        parcel_id="PID-2",
        score=1,
    )
    _insert_lead(
        db_path,
        county="Orange",
        owner_name="C",
        situs_address="C St",
        parcel_id="PID-3",
        score=1,
    )

    _insert_permit(
        db_path,
        county="Orange",
        parcel_id="PID-1",
        permit_number="P-1",
        permit_type="Building",
        status="Closed",
        issue_date="2024-01-01",
        final_date="2024-02-01",
    )
    _insert_permit(
        db_path,
        county="Orange",
        parcel_id="PID-2",
        permit_number="P-2",
        permit_type="Building",
        status="Closed",
        issue_date="2000-01-01",
        final_date="2000-02-01",
    )

    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))
    client = _client()

    r_newest = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": "",
            "fields": ["owner"],
            "filters": {},
            "sort": "last_permit_newest",
            "limit": 50,
        },
    )
    assert r_newest.status_code == 200
    data_newest = r_newest.json()
    assert data_newest[0]["parcel_id"] == "PID-1"

    r_oldest = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": "",
            "fields": ["owner"],
            "filters": {},
            "sort": "last_permit_oldest",
            "limit": 50,
        },
    )
    assert r_oldest.status_code == 200
    data_oldest = r_oldest.json()
    # Oldest should include the null last_permit_date first
    assert data_oldest[0]["parcel_id"] == "PID-3"
