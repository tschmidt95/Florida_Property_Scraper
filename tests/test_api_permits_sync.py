"""Tests for permits sync API endpoint (no live HTTP)."""
import os
import pytest
from pathlib import Path
from unittest.mock import Mock, patch


pytest.importorskip("fastapi")


def test_permits_sync_requires_live():
    """Test that /api/permits/sync requires LIVE=1."""
    from fastapi.testclient import TestClient
    from florida_property_scraper.api.app import app

    client = TestClient(app)

    # Ensure LIVE is not set
    old_value = os.environ.pop("LIVE", None)
    try:
        response = client.post(
            "/api/permits/sync",
            json={"county": "seminole", "query": "123 Main St", "limit": 10},
        )

        assert response.status_code == 400
        assert "LIVE=1" in response.json()["detail"]
    finally:
        if old_value:
            os.environ["LIVE"] = old_value


def test_permits_sync_unknown_county():
    """Test that /api/permits/sync returns 404 for unknown county."""
    from fastapi.testclient import TestClient
    from florida_property_scraper.api.app import app

    client = TestClient(app)

    # Set LIVE=1
    old_value = os.environ.get("LIVE")
    os.environ["LIVE"] = "1"
    try:
        response = client.post(
            "/api/permits/sync",
            json={"county": "unknown_county", "query": "123 Main St", "limit": 10},
        )

        assert response.status_code == 404
        assert "not available" in response.json()["detail"]
    finally:
        if old_value:
            os.environ["LIVE"] = old_value
        else:
            os.environ.pop("LIVE", None)


def test_permits_sync_with_mock_scraper(tmp_path):
    """Test permits sync with mocked scraper."""
    from fastapi.testclient import TestClient
    from florida_property_scraper.api.app import app
    from florida_property_scraper.permits.models import PermitRecord
    from florida_property_scraper.permits.registry import register_scraper

    # Create mock scraper
    mock_scraper = Mock()
    mock_permits = [
        PermitRecord(
            county="test_county",
            parcel_id="12-34-56",
            address="123 Test St",
            permit_number="TEST-001",
            permit_type="Building",
            status="Issued",
            issue_date="2023-01-15",
            final_date=None,
            description="Test permit",
            source="https://test.example.com",
        ),
        PermitRecord(
            county="test_county",
            parcel_id="12-34-57",
            address="456 Test Ave",
            permit_number="TEST-002",
            permit_type="Electrical",
            status="Final",
            issue_date="2023-02-20",
            final_date="2023-03-10",
            description="Test permit 2",
            source="https://test.example.com",
        ),
    ]
    mock_scraper.search_permits.return_value = mock_permits

    # Register mock scraper
    register_scraper("test_county", mock_scraper)

    client = TestClient(app)

    # Set LIVE=1 and DB path
    old_live = os.environ.get("LIVE")
    old_db = os.environ.get("PA_DB")
    os.environ["LIVE"] = "1"
    db_path = tmp_path / "test_permits.sqlite"
    os.environ["PA_DB"] = str(db_path)

    try:
        response = client.post(
            "/api/permits/sync",
            json={"county": "test_county", "query": "123 Test St", "limit": 10},
        )

        assert response.status_code == 200
        data = response.json()

        assert data["county"] == "test_county"
        assert data["query"] == "123 Test St"
        assert data["count"] == 2
        assert len(data["permits"]) == 2

        # Verify first permit
        p1 = data["permits"][0]
        assert p1["permit_number"] == "TEST-001"
        assert p1["address"] == "123 Test St"
        assert p1["status"] == "Issued"

        # Verify scraper was called
        mock_scraper.search_permits.assert_called_once_with("123 Test St", limit=10)

    finally:
        if old_live:
            os.environ["LIVE"] = old_live
        else:
            os.environ.pop("LIVE", None)
        if old_db:
            os.environ["PA_DB"] = old_db
        else:
            os.environ.pop("PA_DB", None)
