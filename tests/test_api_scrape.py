from __future__ import annotations

import sqlite3

from florida_property_scraper.api.app import app
from florida_property_scraper.leads_models import SearchResult


class _FakeScraper:
    county = "Seminole"

    def search(self, query: str, limit: int):
        return [
            SearchResult(
                owner="Fake Owner",
                address="1 Fake St",
                county="Seminole",
                parcel_id="SEM-FAKE-1",
                source="https://example.test/SEM-FAKE-1",
                score=77,
            )
        ]


def test_api_scrape_uses_scraper_and_persists(tmp_path, monkeypatch):
    if app is None:
        return

    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    # Patch where get_scraper is used (scrape route imports it at module import time).
    import florida_property_scraper.api.routes.scrape as scrape_module

    monkeypatch.setattr(scrape_module, "get_scraper", lambda county: _FakeScraper())

    from fastapi.testclient import TestClient

    client = TestClient(app)

    r = client.post(
        "/api/scrape",
        json={"county": "Seminole", "query": "smith", "limit": 5},
    )
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["owner"] == "Fake Owner"

    # Confirm persisted to sqlite.
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT owner, address, county, parcel_id, source, score FROM leads"
        ).fetchone()
        assert row is not None
        assert row[0] == "Fake Owner"
        assert row[3] == "SEM-FAKE-1"
    finally:
        conn.close()

    # Search API should now find it.
    r = client.get("/api/search", params={"q": "fake", "county": "Seminole"})
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["parcel_id"] == "SEM-FAKE-1"
