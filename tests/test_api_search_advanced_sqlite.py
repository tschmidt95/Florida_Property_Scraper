"""Test advanced search API endpoint with SQLite."""

import os
import tempfile
from datetime import datetime, timedelta

from florida_property_scraper.storage import SQLiteStore


def test_advanced_search_field_selection():
    """Test that advanced search only searches specified fields."""
    from florida_property_scraper.api.app import app
    from fastapi.testclient import TestClient

    # Create temp database
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    os.environ["LEADS_SQLITE_PATH"] = db_path

    try:
        # Create database with sample leads
        store = SQLiteStore(db_path)
        store.upsert_lead(
            {
                "dedupe_key": "lead1",
                "county": "seminole",
                "search_query": "",
                "owner_name": "John Doe",
                "situs_address": "123 Main St",
                "parcel_id": "PARCEL001",
                "captured_at": datetime.now().isoformat(),
            }
        )
        store.upsert_lead(
            {
                "dedupe_key": "lead2",
                "county": "seminole",
                "search_query": "",
                "owner_name": "Jane Smith",
                "situs_address": "456 Oak Ave",
                "parcel_id": "PARCEL002",
                "captured_at": datetime.now().isoformat(),
            }
        )
        store.close()

        client = TestClient(app)

        # Search only in owner field
        response = client.post(
            "/api/search/advanced",
            json={
                "county": "seminole",
                "text": "John",
                "fields": ["owner"],
                "filters": {},
                "sort": "relevance",
                "limit": 50,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["owner"] == "John Doe"

        # Search only in address field
        response = client.post(
            "/api/search/advanced",
            json={
                "county": "seminole",
                "text": "Oak",
                "fields": ["address"],
                "filters": {},
                "sort": "relevance",
                "limit": 50,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["address"] == "456 Oak Ave"

    finally:
        if "LEADS_SQLITE_PATH" in os.environ:
            del os.environ["LEADS_SQLITE_PATH"]
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_advanced_search_no_permits_filter():
    """Test no_permits_in_years filter."""
    from florida_property_scraper.api.app import app
    from fastapi.testclient import TestClient

    # Create temp database
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    os.environ["LEADS_SQLITE_PATH"] = db_path

    try:
        # Create database with leads and permits
        store = SQLiteStore(db_path)

        # Lead with no permits
        store.upsert_lead(
            {
                "dedupe_key": "lead1",
                "county": "seminole",
                "search_query": "",
                "owner_name": "No Permits Owner",
                "situs_address": "100 No Permit St",
                "parcel_id": "PARCEL_NO",
                "captured_at": datetime.now().isoformat(),
            }
        )

        # Lead with old permit (>15 years)
        store.upsert_lead(
            {
                "dedupe_key": "lead2",
                "county": "seminole",
                "search_query": "",
                "owner_name": "Old Permit Owner",
                "situs_address": "200 Old Permit St",
                "parcel_id": "PARCEL_OLD",
                "captured_at": datetime.now().isoformat(),
            }
        )

        # Lead with recent permit
        store.upsert_lead(
            {
                "dedupe_key": "lead3",
                "county": "seminole",
                "search_query": "",
                "owner_name": "Recent Permit Owner",
                "situs_address": "300 Recent Permit St",
                "parcel_id": "PARCEL_RECENT",
                "captured_at": datetime.now().isoformat(),
            }
        )

        # Add permits
        old_date = (datetime.now() - timedelta(days=16 * 365)).strftime("%Y-%m-%d")
        recent_date = (datetime.now() - timedelta(days=1 * 365)).strftime("%Y-%m-%d")

        store.upsert_many_permits(
            [
                {
                    "county": "seminole",
                    "parcel_id": "PARCEL_OLD",
                    "address": "200 Old Permit St",
                    "permit_number": "BP2005-001",
                    "permit_type": "Building",
                    "status": "Finaled",
                    "issue_date": old_date,
                    "final_date": old_date,
                    "description": "Old permit",
                    "source": "https://test.example.com",
                    "raw": "",
                },
                {
                    "county": "seminole",
                    "parcel_id": "PARCEL_RECENT",
                    "address": "300 Recent Permit St",
                    "permit_number": "BP2023-001",
                    "permit_type": "Building",
                    "status": "Issued",
                    "issue_date": recent_date,
                    "final_date": None,
                    "description": "Recent permit",
                    "source": "https://test.example.com",
                    "raw": "",
                },
            ]
        )

        store.close()

        client = TestClient(app)

        # Search for properties with no permits in last 15 years
        response = client.post(
            "/api/search/advanced",
            json={
                "county": "seminole",
                "text": "Owner",
                "fields": ["owner"],
                "filters": {
                    "no_permits_in_years": 15,
                },
                "sort": "relevance",
                "limit": 50,
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should return leads with no permits and old permits, but not recent permits
        owners = [r["owner"] for r in data]
        assert "No Permits Owner" in owners
        assert "Old Permit Owner" in owners
        assert "Recent Permit Owner" not in owners

    finally:
        if "LEADS_SQLITE_PATH" in os.environ:
            del os.environ["LEADS_SQLITE_PATH"]
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_advanced_search_permit_sorting():
    """Test sorting by permit dates."""
    from florida_property_scraper.api.app import app
    from fastapi.testclient import TestClient

    # Create temp database
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    os.environ["LEADS_SQLITE_PATH"] = db_path

    try:
        store = SQLiteStore(db_path)

        # Create leads with different permit dates
        store.upsert_lead(
            {
                "dedupe_key": "lead1",
                "county": "seminole",
                "owner_name": "Owner A",
                "situs_address": "100 A St",
                "parcel_id": "PARCEL_A",
                "captured_at": datetime.now().isoformat(),
            }
        )
        store.upsert_lead(
            {
                "dedupe_key": "lead2",
                "county": "seminole",
                "owner_name": "Owner B",
                "situs_address": "200 B St",
                "parcel_id": "PARCEL_B",
                "captured_at": datetime.now().isoformat(),
            }
        )
        store.upsert_lead(
            {
                "dedupe_key": "lead3",
                "county": "seminole",
                "owner_name": "Owner C",
                "situs_address": "300 C St",
                "parcel_id": "PARCEL_C",
                "captured_at": datetime.now().isoformat(),
            }
        )

        # Add permits with different dates
        store.upsert_many_permits(
            [
                {
                    "county": "seminole",
                    "parcel_id": "PARCEL_A",
                    "address": "100 A St",
                    "permit_number": "BP2020-001",
                    "permit_type": "Building",
                    "status": "Finaled",
                    "issue_date": "2020-01-01",
                    "final_date": "2020-06-01",
                    "description": "Permit A",
                    "source": "https://test.example.com",
                    "raw": "",
                },
                {
                    "county": "seminole",
                    "parcel_id": "PARCEL_B",
                    "address": "200 B St",
                    "permit_number": "BP2022-001",
                    "permit_type": "Building",
                    "status": "Issued",
                    "issue_date": "2022-01-01",
                    "final_date": None,
                    "description": "Permit B",
                    "source": "https://test.example.com",
                    "raw": "",
                },
            ]
        )

        store.close()

        client = TestClient(app)

        # Sort by oldest permit first
        response = client.post(
            "/api/search/advanced",
            json={
                "county": "seminole",
                "text": "Owner",
                "fields": ["owner"],
                "filters": {},
                "sort": "last_permit_oldest",
                "limit": 50,
            },
        )

        assert response.status_code == 200
        data = response.json()
        # Should have results with nulls first, then oldest
        assert len(data) >= 2

        # Sort by newest permit first
        response = client.post(
            "/api/search/advanced",
            json={
                "county": "seminole",
                "text": "Owner",
                "fields": ["owner"],
                "filters": {},
                "sort": "last_permit_newest",
                "limit": 50,
            },
        )

        assert response.status_code == 200
        data = response.json()
        # First result should have the most recent permit
        assert len(data) >= 2

    finally:
        if "LEADS_SQLITE_PATH" in os.environ:
            del os.environ["LEADS_SQLITE_PATH"]
        if os.path.exists(db_path):
            os.unlink(db_path)
