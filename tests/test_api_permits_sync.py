import sqlite3

from florida_property_scraper.api.app import app
from florida_property_scraper.permits_models import PermitRecord


class _FakePermitsScraper:
    county = "Seminole"

    def fetch_permits(self, *, parcel_id: str):
        return [
            PermitRecord(
                county="Seminole",
                parcel_id=parcel_id,
                permit_id=f"P-{parcel_id}",
                permit_type="Demo",
                status="Issued",
                issued_date="2001-02-03",
                source=f"https://example.test/permits/{parcel_id}",
            )
        ]


def test_api_permits_sync_persists(tmp_path, monkeypatch):
    if app is None:
        return

    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    import florida_property_scraper.api.routes.permits as permits_module

    monkeypatch.setattr(
        permits_module,
        "get_permits_scraper",
        lambda county: _FakePermitsScraper(),
    )

    from fastapi.testclient import TestClient

    client = TestClient(app)

    r = client.post(
        "/api/permits/sync",
        json={"county": "Seminole", "parcel_ids": ["SEM-123"]},
    )
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["permit_id"] == "P-SEM-123"

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT county, parcel_id, permit_id, issued_date FROM permits"
        ).fetchone()
        assert row is not None
        assert row[0] == "Seminole"
        assert row[1] == "SEM-123"
        assert row[2] == "P-SEM-123"
        assert row[3] == "2001-02-03"
    finally:
        conn.close()
