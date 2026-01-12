"""Test permits sync API endpoint."""
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

try:
    from fastapi.testclient import TestClient
    from florida_property_scraper.api.app import app
    from florida_property_scraper.permits.models import PermitRecord
    from florida_property_scraper.storage import SQLiteStore
    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False


@pytest.mark.skipif(not DEPS_AVAILABLE, reason="FastAPI not available")
def test_permits_sync_without_live_env():
    """Test that permits sync returns 400 without LIVE=1."""
    client = TestClient(app)
    
    # Ensure LIVE is not set
    with patch.dict(os.environ, {}, clear=True):
        response = client.post(
            "/api/permits/sync",
            json={"county": "seminole", "query": "123 Main St", "limit": 10}
        )
        
        assert response.status_code == 400
        assert "LIVE=1" in response.json()["detail"]


@pytest.mark.skipif(not DEPS_AVAILABLE, reason="FastAPI not available")
def test_permits_sync_unsupported_county():
    """Test that permits sync returns 404 for unsupported county."""
    client = TestClient(app)
    
    with patch.dict(os.environ, {"LIVE": "1"}):
        # Patch get_permits_scraper to return None
        with patch("florida_property_scraper.api.routes.permits.get_permits_scraper", return_value=None):
            response = client.post(
                "/api/permits/sync",
                json={"county": "unknown", "query": "test", "limit": 10}
            )
            
            assert response.status_code == 404
            assert "not available" in response.json()["detail"]


@pytest.mark.skipif(not DEPS_AVAILABLE, reason="FastAPI not available")
def test_permits_sync_with_mocked_scraper():
    """Test permits sync with mocked scraper."""
    client = TestClient(app)
    
    # Create temporary database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    
    try:
        # Create mock scraper
        mock_scraper = MagicMock()
        mock_permits = [
            PermitRecord(
                county="seminole",
                permit_number="BP2024-001",
                source="http://example.com",
                address="123 Main St",
                permit_type="Building",
                status="Issued",
                issue_date="2024-01-15",
                final_date=None,
                description="Test permit",
                parcel_id="12-34-56",
                raw=None,
            ),
            PermitRecord(
                county="seminole",
                permit_number="BP2024-002",
                source="http://example.com",
                address="456 Oak Dr",
                permit_type="Electrical",
                status="Final",
                issue_date="2024-01-20",
                final_date="2024-02-01",
                description="Test permit 2",
                parcel_id="78-90-12",
                raw=None,
            ),
        ]
        mock_scraper.search_permits.return_value = mock_permits
        
        with patch.dict(os.environ, {"LIVE": "1", "LEADS_SQLITE_PATH": db_path}):
            with patch("florida_property_scraper.api.routes.permits.get_permits_scraper", return_value=mock_scraper):
                response = client.post(
                    "/api/permits/sync",
                    json={"county": "seminole", "query": "Main St", "limit": 10}
                )
                
                assert response.status_code == 200
                data = response.json()
                assert data["county"] == "seminole"
                assert data["count"] == 2
                assert len(data["permits"]) == 2
                
                # Check first permit
                permit1 = data["permits"][0]
                assert permit1["permit_number"] == "BP2024-001"
                assert permit1["address"] == "123 Main St"
                assert permit1["status"] == "Issued"
        
        # Verify permits were stored in database
        store = SQLiteStore(db_path)
        try:
            rows = store.conn.execute("SELECT * FROM permits").fetchall()
            assert len(rows) == 2
            
            # Check first permit in DB
            assert rows[0]["permit_number"] == "BP2024-001"
            assert rows[0]["county"] == "seminole"
            assert rows[0]["address"] == "123 Main St"
        finally:
            store.close()
    
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)


@pytest.mark.skipif(not DEPS_AVAILABLE, reason="FastAPI not available")
def test_permits_sync_empty_query():
    """Test that permits sync returns 400 for empty query."""
    client = TestClient(app)
    
    with patch.dict(os.environ, {"LIVE": "1"}):
        response = client.post(
            "/api/permits/sync",
            json={"county": "seminole", "query": "", "limit": 10}
        )
        
        assert response.status_code == 400
        assert "Query parameter is required" in response.json()["detail"]
