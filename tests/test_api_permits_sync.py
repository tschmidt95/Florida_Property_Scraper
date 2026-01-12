"""Tests for permits sync API endpoint."""
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from florida_property_scraper.permits.models import PermitRecord
from florida_property_scraper.storage import SQLiteStore


def test_permits_sync_requires_live():
    """Test that permits sync returns 400 when LIVE!=1."""
    try:
        from fastapi.testclient import TestClient

        from florida_property_scraper.api.app import app

        client = TestClient(app)

        # Ensure LIVE is not set
        old_live = os.environ.get("LIVE")
        if "LIVE" in os.environ:
            del os.environ["LIVE"]

        try:
            response = client.post(
                "/api/permits/sync",
                json={"county": "seminole", "query": "123 Main St", "limit": 50},
            )
            assert response.status_code == 400
            assert "LIVE=1" in response.json()["detail"]
        finally:
            if old_live is not None:
                os.environ["LIVE"] = old_live
    except ImportError:
        pytest.skip("fastapi not available")


def test_permits_sync_monkeypatched():
    """Test permits sync with monkeypatched scraper."""
    try:
        from fastapi.testclient import TestClient

        from florida_property_scraper.api.app import app

        # Create temp database
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
            db_path = tmp.name

        try:
            # Set environment
            old_live = os.environ.get("LIVE")
            old_db = os.environ.get("LEADS_SQLITE_PATH")
            os.environ["LIVE"] = "1"
            os.environ["LEADS_SQLITE_PATH"] = db_path

            # Create fake permits
            fake_permits = [
                PermitRecord(
                    county="seminole",
                    parcel_id="123-456-789",
                    address="123 Main St",
                    permit_number="BP-2024-001",
                    permit_type="Building",
                    status="Issued",
                    issue_date="2024-01-15",
                    final_date=None,
                    description="Test permit",
                    source="https://example.com",
                    raw="<test>",
                ),
                PermitRecord(
                    county="seminole",
                    parcel_id="987-654-321",
                    address="456 Elm Ave",
                    permit_number="BP-2024-002",
                    permit_type="Electrical",
                    status="Finaled",
                    issue_date="2024-02-20",
                    final_date="2024-03-01",
                    description="Another permit",
                    source="https://example.com",
                    raw="<test2>",
                ),
            ]

            # Mock the scraper
            mock_scraper = MagicMock()
            mock_scraper.search_permits.return_value = fake_permits

            with patch(
                "florida_property_scraper.api.routes.permits.get_permits_scraper",
                return_value=mock_scraper,
            ):
                client = TestClient(app)
                response = client.post(
                    "/api/permits/sync",
                    json={"county": "seminole", "query": "Main St", "limit": 50},
                )

            assert response.status_code == 200
            data = response.json()
            assert data["county"] == "seminole"
            assert data["count"] == 2
            assert len(data["permits"]) == 2
            assert data["permits"][0]["permit_number"] == "BP-2024-001"
            assert data["permits"][1]["permit_number"] == "BP-2024-002"

            # Verify permits were stored
            store = SQLiteStore(db_path)
            try:
                cursor = store.conn.execute(
                    "SELECT COUNT(*) FROM permits WHERE county = ?", ("seminole",)
                )
                count = cursor.fetchone()[0]
                assert count == 2

                # Check specific permit
                cursor = store.conn.execute(
                    "SELECT * FROM permits WHERE permit_number = ?", ("BP-2024-001",)
                )
                row = cursor.fetchone()
                assert row is not None
                assert row["address"] == "123 Main St"
                assert row["permit_type"] == "Building"
            finally:
                store.close()

        finally:
            # Cleanup
            if old_live is not None:
                os.environ["LIVE"] = old_live
            elif "LIVE" in os.environ:
                del os.environ["LIVE"]

            if old_db is not None:
                os.environ["LEADS_SQLITE_PATH"] = old_db
            elif "LEADS_SQLITE_PATH" in os.environ:
                del os.environ["LEADS_SQLITE_PATH"]

            Path(db_path).unlink(missing_ok=True)

    except ImportError:
        pytest.skip("fastapi not available")
