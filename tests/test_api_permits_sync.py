"""Test permits sync API endpoint."""
import os
import tempfile
from unittest import mock

import pytest

from florida_property_scraper.permits.models import PermitRecord


def test_permits_sync_requires_live_flag():
    """Test that permits sync returns 400 when LIVE is not set."""
    from florida_property_scraper.api.app import app

    # Ensure LIVE is not set
    old_live = os.environ.get("LIVE")
    if "LIVE" in os.environ:
        del os.environ["LIVE"]

    try:
        from fastapi.testclient import TestClient

        client = TestClient(app)
        response = client.post(
            "/api/permits/sync",
            json={"county": "seminole", "query": "test", "limit": 10},
        )

        assert response.status_code == 400
        assert "LIVE=1" in response.json()["detail"]
    finally:
        if old_live:
            os.environ["LIVE"] = old_live


def test_permits_sync_with_mocked_scraper():
    """Test permits sync with mocked scraper."""
    from florida_property_scraper.api.app import app

    # Mock scraper to return fake permits
    fake_permits = [
        PermitRecord(
            county="seminole",
            parcel_id="123456",
            address="123 Test St",
            permit_number="BP2023-999",
            permit_type="Building",
            status="Issued",
            issue_date="2023-01-01",
            final_date=None,
            description="Test permit",
            source="https://test.example.com",
            raw="<tr>test</tr>",
        ),
        PermitRecord(
            county="seminole",
            parcel_id="789012",
            address="456 Test Ave",
            permit_number="BP2023-888",
            permit_type="Electrical",
            status="Finaled",
            issue_date="2023-02-01",
            final_date="2023-03-01",
            description="Test electrical",
            source="https://test.example.com",
            raw="<tr>test2</tr>",
        ),
    ]

    class MockScraper:
        def search_permits(self, query, limit):
            return fake_permits

    # Set LIVE flag
    os.environ["LIVE"] = "1"

    # Create temp database
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    os.environ["LEADS_SQLITE_PATH"] = db_path

    try:
        from fastapi.testclient import TestClient

        client = TestClient(app)

        # Mock the registry to return our mock scraper
        with mock.patch(
            "florida_property_scraper.api.routes.permits.get_permits_scraper"
        ) as mock_registry:
            mock_registry.return_value = MockScraper()

            response = client.post(
                "/api/permits/sync",
                json={"county": "seminole", "query": "test", "limit": 10},
            )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["permit_number"] == "BP2023-999"
        assert data[1]["permit_number"] == "BP2023-888"

        # Verify permits were stored in database
        from florida_property_scraper.storage import SQLiteStore

        store = SQLiteStore(db_path)
        try:
            # Query permits from database
            rows = store.conn.execute(
                "SELECT * FROM permits WHERE county = ?", ("seminole",)
            ).fetchall()
            assert len(rows) == 2
        finally:
            store.close()

    finally:
        # Cleanup
        if "LIVE" in os.environ:
            del os.environ["LIVE"]
        if "LEADS_SQLITE_PATH" in os.environ:
            del os.environ["LEADS_SQLITE_PATH"]
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_permits_sync_unknown_county():
    """Test that permits sync returns 404 for unknown county."""
    from florida_property_scraper.api.app import app

    os.environ["LIVE"] = "1"

    try:
        from fastapi.testclient import TestClient

        client = TestClient(app)
        response = client.post(
            "/api/permits/sync",
            json={"county": "unknown", "query": "test", "limit": 10},
        )

        assert response.status_code == 404
        assert "No permits scraper" in response.json()["detail"]
    finally:
        if "LIVE" in os.environ:
            del os.environ["LIVE"]
