"""Tests for advanced search API endpoint."""
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest


def test_advanced_search_field_selection():
    """Test that field selection actually limits which fields are searched."""
    try:
        from fastapi.testclient import TestClient

        from florida_property_scraper.api.app import app
        from florida_property_scraper.storage import SQLiteStore

        # Create temp database with leads
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
            db_path = tmp.name

        try:
            old_db = os.environ.get("LEADS_SQLITE_PATH")
            os.environ["LEADS_SQLITE_PATH"] = db_path

            store = SQLiteStore(db_path)
            # Insert test lead
            store.upsert_lead(
                {
                    "dedupe_key": "test1",
                    "county": "seminole",
                    "owner_name": "John Smith",
                    "situs_address": "123 Main St",
                    "parcel_id": "ABC-123",
                    "search_query": "test",
                    "captured_at": datetime.now().isoformat(),
                }
            )
            store.upsert_lead(
                {
                    "dedupe_key": "test2",
                    "county": "seminole",
                    "owner_name": "Jane Doe",
                    "situs_address": "456 Elm Ave",
                    "parcel_id": "XYZ-789",
                    "search_query": "test",
                    "captured_at": datetime.now().isoformat(),
                }
            )
            store.close()

            client = TestClient(app)

            # Search only in owner field - should match "John"
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "John",
                    "fields": ["owner"],
                    "limit": 50,
                },
            )
            assert response.status_code == 200
            results = response.json()
            assert len(results) == 1
            assert "John" in results[0]["owner"]
            assert "owner" in results[0]["matched_fields"]

            # Search only in address field - should match "Main"
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "Main",
                    "fields": ["address"],
                    "limit": 50,
                },
            )
            assert response.status_code == 200
            results = response.json()
            assert len(results) == 1
            assert "Main" in results[0]["address"]
            assert "address" in results[0]["matched_fields"]

            # Search only in parcel_id - should match "ABC"
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "ABC",
                    "fields": ["parcel_id"],
                    "limit": 50,
                },
            )
            assert response.status_code == 200
            results = response.json()
            assert len(results) == 1
            assert results[0]["parcel_id"] == "ABC-123"

        finally:
            if old_db is not None:
                os.environ["LEADS_SQLITE_PATH"] = old_db
            elif "LEADS_SQLITE_PATH" in os.environ:
                del os.environ["LEADS_SQLITE_PATH"]
            Path(db_path).unlink(missing_ok=True)

    except ImportError:
        pytest.skip("fastapi not available")


def test_advanced_search_no_permits_filter():
    """Test no_permits_in_years filter with NULL last_permit_date matching."""
    try:
        from fastapi.testclient import TestClient

        from florida_property_scraper.api.app import app
        from florida_property_scraper.storage import SQLiteStore

        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
            db_path = tmp.name

        try:
            old_db = os.environ.get("LEADS_SQLITE_PATH")
            os.environ["LEADS_SQLITE_PATH"] = db_path

            store = SQLiteStore(db_path)

            # Insert leads
            store.upsert_lead(
                {
                    "dedupe_key": "lead1",
                    "county": "seminole",
                    "owner_name": "Owner One",
                    "situs_address": "100 First St",
                    "parcel_id": "P001",
                    "search_query": "test",
                    "captured_at": datetime.now().isoformat(),
                }
            )
            store.upsert_lead(
                {
                    "dedupe_key": "lead2",
                    "county": "seminole",
                    "owner_name": "Owner Two",
                    "situs_address": "200 Second St",
                    "parcel_id": "P002",
                    "search_query": "test",
                    "captured_at": datetime.now().isoformat(),
                }
            )
            store.upsert_lead(
                {
                    "dedupe_key": "lead3",
                    "county": "seminole",
                    "owner_name": "Owner Three",
                    "situs_address": "300 Third St",
                    "parcel_id": "P003",
                    "search_query": "test",
                    "captured_at": datetime.now().isoformat(),
                }
            )

            # Insert permits: P001 has recent permit, P002 has old permit, P003 has no permits
            old_date = (datetime.now() - timedelta(days=20 * 365)).strftime("%Y-%m-%d")
            recent_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

            store.upsert_many_permits(
                [
                    {
                        "county": "seminole",
                        "parcel_id": "P001",
                        "address": "100 First St",
                        "permit_number": "BP-001",
                        "permit_type": "Building",
                        "status": "Finaled",
                        "issue_date": recent_date,
                        "final_date": None,
                        "description": "Recent work",
                        "source": "test",
                        "raw": "",
                    },
                    {
                        "county": "seminole",
                        "parcel_id": "P002",
                        "address": "200 Second St",
                        "permit_number": "BP-002",
                        "permit_type": "Building",
                        "status": "Finaled",
                        "issue_date": old_date,
                        "final_date": None,
                        "description": "Old work",
                        "source": "test",
                        "raw": "",
                    },
                ]
            )

            store.close()

            client = TestClient(app)

            # Search with no_permits_in_years=15
            # Should match P002 (old permit) and P003 (no permits)
            # Should NOT match P001 (recent permit)
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "Owner",
                    "fields": ["owner"],
                    "filters": {"no_permits_in_years": 15},
                    "limit": 50,
                },
            )
            assert response.status_code == 200
            results = response.json()

            # Should get 2 results (P002 and P003)
            assert len(results) == 2

            parcel_ids = {r["parcel_id"] for r in results}
            assert "P002" in parcel_ids, "P002 should match (old permit > 15 years)"
            assert "P003" in parcel_ids, "P003 should match (no permits = NULL)"
            assert "P001" not in parcel_ids, "P001 should NOT match (recent permit)"

            # Verify NULL last_permit_date for P003
            p003_result = [r for r in results if r["parcel_id"] == "P003"][0]
            assert p003_result["last_permit_date"] is None
            assert p003_result["permits_last_15y_count"] == 0

        finally:
            if old_db is not None:
                os.environ["LEADS_SQLITE_PATH"] = old_db
            elif "LEADS_SQLITE_PATH" in os.environ:
                del os.environ["LEADS_SQLITE_PATH"]
            Path(db_path).unlink(missing_ok=True)

    except ImportError:
        pytest.skip("fastapi not available")


def test_advanced_search_sorting():
    """Test last_permit_oldest and last_permit_newest sorting."""
    try:
        from fastapi.testclient import TestClient

        from florida_property_scraper.api.app import app
        from florida_property_scraper.storage import SQLiteStore

        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
            db_path = tmp.name

        try:
            old_db = os.environ.get("LEADS_SQLITE_PATH")
            os.environ["LEADS_SQLITE_PATH"] = db_path

            store = SQLiteStore(db_path)

            # Insert leads with different permit dates
            store.upsert_lead(
                {
                    "dedupe_key": "lead1",
                    "county": "seminole",
                    "owner_name": "Owner A",
                    "situs_address": "100 A St",
                    "parcel_id": "PA",
                    "search_query": "test",
                    "captured_at": datetime.now().isoformat(),
                }
            )
            store.upsert_lead(
                {
                    "dedupe_key": "lead2",
                    "county": "seminole",
                    "owner_name": "Owner B",
                    "situs_address": "200 B St",
                    "parcel_id": "PB",
                    "search_query": "test",
                    "captured_at": datetime.now().isoformat(),
                }
            )
            store.upsert_lead(
                {
                    "dedupe_key": "lead3",
                    "county": "seminole",
                    "owner_name": "Owner C",
                    "situs_address": "300 C St",
                    "parcel_id": "PC",
                    "search_query": "test",
                    "captured_at": datetime.now().isoformat(),
                }
            )

            # Insert permits with different dates
            store.upsert_many_permits(
                [
                    {
                        "county": "seminole",
                        "parcel_id": "PA",
                        "address": "100 A St",
                        "permit_number": "BP-A",
                        "permit_type": "Building",
                        "status": "Finaled",
                        "issue_date": "2020-01-01",  # Oldest
                        "final_date": None,
                        "description": "Test",
                        "source": "test",
                        "raw": "",
                    },
                    {
                        "county": "seminole",
                        "parcel_id": "PB",
                        "address": "200 B St",
                        "permit_number": "BP-B",
                        "permit_type": "Building",
                        "status": "Finaled",
                        "issue_date": "2024-01-01",  # Newest
                        "final_date": None,
                        "description": "Test",
                        "source": "test",
                        "raw": "",
                    },
                    {
                        "county": "seminole",
                        "parcel_id": "PC",
                        "address": "300 C St",
                        "permit_number": "BP-C",
                        "permit_type": "Building",
                        "status": "Finaled",
                        "issue_date": "2022-01-01",  # Middle
                        "final_date": None,
                        "description": "Test",
                        "source": "test",
                        "raw": "",
                    },
                ]
            )

            store.close()

            client = TestClient(app)

            # Test last_permit_oldest sort
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "Owner",
                    "fields": ["owner"],
                    "sort": "last_permit_oldest",
                    "limit": 50,
                },
            )
            assert response.status_code == 200
            results = response.json()
            assert len(results) == 3
            # PA (2020) should be first, PB (2024) should be last
            assert results[0]["parcel_id"] == "PA"
            assert results[-1]["parcel_id"] == "PB"

            # Test last_permit_newest sort
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "Owner",
                    "fields": ["owner"],
                    "sort": "last_permit_newest",
                    "limit": 50,
                },
            )
            assert response.status_code == 200
            results = response.json()
            assert len(results) == 3
            # PB (2024) should be first, PA (2020) should be last
            assert results[0]["parcel_id"] == "PB"
            assert results[-1]["parcel_id"] == "PA"

        finally:
            if old_db is not None:
                os.environ["LEADS_SQLITE_PATH"] = old_db
            elif "LEADS_SQLITE_PATH" in os.environ:
                del os.environ["LEADS_SQLITE_PATH"]
            Path(db_path).unlink(missing_ok=True)

    except ImportError:
        pytest.skip("fastapi not available")
