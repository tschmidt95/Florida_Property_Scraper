"""Test permits sync API endpoint (no live HTTP)."""

import pytest

try:
    from fastapi.testclient import TestClient

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    TestClient = None


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
def test_permits_sync_requires_live_flag(monkeypatch):
    """Test that permits sync endpoint requires LIVE=1."""
    from florida_property_scraper.api.app import app

    # Ensure LIVE is not set
    monkeypatch.delenv("LIVE", raising=False)

    client = TestClient(app)
    response = client.post(
        "/api/permits/sync",
        json={"county": "seminole", "query": "123 Main St", "limit": 10},
    )

    assert response.status_code == 400
    assert "LIVE=1" in response.json()["detail"]


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
def test_permits_sync_with_mock_scraper(monkeypatch, tmp_path):
    """Test permits sync endpoint with mocked scraper."""
    from florida_property_scraper.api.app import app
    from florida_property_scraper.permits.models import PermitRecord

    # Set LIVE=1
    monkeypatch.setenv("LIVE", "1")

    # Set test DB path
    db_path = tmp_path / "test_permits.db"
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    # Mock the scraper to return fake permits
    fake_permits = [
        PermitRecord(
            county="seminole",
            permit_number="BP-2023-001",
            address="123 Test St",
            permit_type="Residential",
            status="ISSUED",
            issue_date="2023-01-15",
            source="https://example.com",
        ),
        PermitRecord(
            county="seminole",
            permit_number="BP-2023-002",
            address="456 Test Ave",
            permit_type="Commercial",
            status="FINALED",
            issue_date="2023-02-20",
            final_date="2023-05-10",
            source="https://example.com",
        ),
    ]

    def mock_search_permits(self, query, limit):
        return fake_permits

    # Patch the scraper
    from florida_property_scraper.permits import seminole

    original_method = seminole.SeminolePermitScraper.search_permits
    monkeypatch.setattr(
        seminole.SeminolePermitScraper, "search_permits", mock_search_permits
    )

    try:
        client = TestClient(app)
        response = client.post(
            "/api/permits/sync",
            json={"county": "seminole", "query": "123 Test St", "limit": 10},
        )

        # Debug: print response details
        print(f"\nResponse status: {response.status_code}")
        print(f"Response text: {response.text}")

        assert response.status_code == 200, (
            f"Expected 200 but got {response.status_code}: {response.text}"
        )
        data = response.json()
        assert len(data) == 2
        assert data[0]["permit_number"] == "BP-2023-001"
        assert data[0]["county"] == "seminole"
        assert data[1]["permit_number"] == "BP-2023-002"

        # Verify permits were stored in database
        from florida_property_scraper.storage import SQLiteStore

        store = SQLiteStore(str(db_path))
        try:
            rows = store.conn.execute(
                "SELECT * FROM permits ORDER BY permit_number"
            ).fetchall()
            assert len(rows) == 2
            assert rows[0]["permit_number"] == "BP-2023-001"
            assert rows[1]["permit_number"] == "BP-2023-002"
        finally:
            store.close()
    finally:
        # Restore original method
        monkeypatch.setattr(
            seminole.SeminolePermitScraper, "search_permits", original_method
        )


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
def test_permits_sync_unsupported_county(monkeypatch):
    """Test permits sync with unsupported county."""
    from florida_property_scraper.api.app import app

    monkeypatch.setenv("LIVE", "1")

    client = TestClient(app)
    response = client.post(
        "/api/permits/sync",
        json={"county": "nonexistent", "query": "test", "limit": 10},
    )

    assert response.status_code == 404
    assert "No permit scraper" in response.json()["detail"]
