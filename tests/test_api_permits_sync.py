import sqlite3
from dataclasses import dataclass

import pytest


pytest.importorskip("fastapi")


def _client():
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    from florida_property_scraper.api.routes.permits import router

    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app)


@dataclass(frozen=True)
class _FakePermit:
    county: str
    parcel_id: str | None
    address: str | None
    permit_number: str
    permit_type: str | None
    status: str | None
    issue_date: str | None
    final_date: str | None
    description: str | None
    source: str
    raw: str | None = None

    def with_truncated_raw(self, *, max_chars: int = 4000):
        return self


class _FakeScraper:
    county = "seminole"

    def search_permits(self, query: str, limit: int):
        return [
            _FakePermit(
                county="seminole",
                parcel_id="PID-1",
                address="123 MAIN ST",
                permit_number="P-123",
                permit_type="Building",
                status="Open",
                issue_date="2024-01-01",
                final_date=None,
                description="Test",
                source="src://permit",
            )
        ]


def test_api_permits_sync_is_live_gated(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"

    # Ensure the DB exists and has schema initialized by SQLiteStore when sync runs.
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))
    monkeypatch.delenv("LIVE", raising=False)

    client = _client()
    r = client.post(
        "/api/permits/sync",
        json={"county": "seminole", "query": "oak", "limit": 10},
    )
    assert r.status_code == 400
    assert "LIVE=1" in r.json().get("detail", "")


def test_api_permits_sync_uses_registry_and_stores(tmp_path, monkeypatch):
    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("LIVE", "1")

    # Monkeypatch the registry lookup inside the route module.
    import florida_property_scraper.api.routes.permits as permits_route

    monkeypatch.setattr(
        permits_route, "get_permits_scraper", lambda county: _FakeScraper()
    )

    client = _client()
    r = client.post(
        "/api/permits/sync",
        json={"county": "seminole", "query": "oak", "limit": 10},
    )
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["permit_number"] == "P-123"

    # Confirm it was stored.
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT county, parcel_id, permit_number FROM permits WHERE permit_number=?",
            ("P-123",),
        ).fetchone()
        assert row is not None
        assert row[0] == "seminole"
    finally:
        conn.close()
