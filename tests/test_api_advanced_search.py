"""Test advanced search API with permits enrichment."""
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

try:
    from fastapi.testclient import TestClient

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    TestClient = None


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
def test_advanced_search_field_selection(monkeypatch, tmp_path):
    """Test that advanced search only searches selected fields."""
    from florida_property_scraper.api.app import app
    from florida_property_scraper.storage import SQLiteStore

    # Set test DB path
    db_path = tmp_path / "test_search.db"
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    # Create test data
    store = SQLiteStore(str(db_path))
    try:
        # Insert test leads
        store.upsert_lead(
            {
                "dedupe_key": "lead1",
                "county": "Orange",
                "owner_name": "John Smith",
                "situs_address": "123 Main St",
                "parcel_id": "ABC123",
                "lead_score": 80,
                "captured_at": datetime.now().isoformat(),
            }
        )
        store.upsert_lead(
            {
                "dedupe_key": "lead2",
                "county": "Orange",
                "owner_name": "Jane Doe",
                "situs_address": "456 Smith Ave",
                "parcel_id": "DEF456",
                "lead_score": 75,
                "captured_at": datetime.now().isoformat(),
            }
        )
    finally:
        store.close()

    client = TestClient(app)

    # Search only in owner_name - should find "John Smith" but not "456 Smith Ave"
    response = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": "smith",
            "fields": ["owner_name"],
            "filters": {},
            "sort": "relevance",
            "limit": 50,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["owner"] == "John Smith"

    # Search only in situs_address - should find "456 Smith Ave" but not "John Smith"
    response = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": "smith",
            "fields": ["situs_address"],
            "filters": {},
            "sort": "relevance",
            "limit": 50,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["owner"] == "Jane Doe"


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
def test_advanced_search_no_permits_filter(monkeypatch, tmp_path):
    """Test no_permits_in_years filter behavior."""
    from florida_property_scraper.api.app import app
    from florida_property_scraper.storage import SQLiteStore

    # Set test DB path
    db_path = tmp_path / "test_permits_filter.db"
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    # Create test data
    store = SQLiteStore(str(db_path))
    try:
        # Insert leads
        store.upsert_lead(
            {
                "dedupe_key": "lead1",
                "county": "Orange",
                "owner_name": "Old Property",
                "situs_address": "123 Old St",
                "parcel_id": "OLD123",
                "lead_score": 80,
                "captured_at": datetime.now().isoformat(),
            }
        )
        store.upsert_lead(
            {
                "dedupe_key": "lead2",
                "county": "Orange",
                "owner_name": "Recent Property",
                "situs_address": "456 New Ave",
                "parcel_id": "NEW456",
                "lead_score": 75,
                "captured_at": datetime.now().isoformat(),
            }
        )
        store.upsert_lead(
            {
                "dedupe_key": "lead3",
                "county": "Orange",
                "owner_name": "No Permits",
                "situs_address": "789 None Blvd",
                "parcel_id": "NONE789",
                "lead_score": 70,
                "captured_at": datetime.now().isoformat(),
            }
        )

        # Insert permits
        # Old permit (20 years ago)
        old_date = (datetime.now() - timedelta(days=20 * 365)).strftime("%Y-%m-%d")
        store.upsert_many_permits(
            [
                {
                    "county": "Orange",
                    "parcel_id": "OLD123",
                    "permit_number": "BP-OLD-001",
                    "issue_date": old_date,
                    "source": "test",
                }
            ]
        )

        # Recent permit (5 years ago)
        recent_date = (datetime.now() - timedelta(days=5 * 365)).strftime("%Y-%m-%d")
        store.upsert_many_permits(
            [
                {
                    "county": "Orange",
                    "parcel_id": "NEW456",
                    "permit_number": "BP-NEW-001",
                    "issue_date": recent_date,
                    "source": "test",
                }
            ]
        )
    finally:
        store.close()

    client = TestClient(app)

    # Filter: no permits in last 15 years
    # Should return "Old Property" (permit 20y ago) and "No Permits" (null date)
    response = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": "",
            "fields": ["owner_name", "situs_address"],
            "filters": {"no_permits_in_years": 15},
            "sort": "relevance",
            "limit": 50,
        },
    )

    assert response.status_code == 200
    data = response.json()
    owners = [r["owner"] for r in data]
    assert "Old Property" in owners
    assert "No Permits" in owners
    assert "Recent Property" not in owners


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
def test_advanced_search_sort_by_permit_date(monkeypatch, tmp_path):
    """Test sorting by last permit date."""
    from florida_property_scraper.api.app import app
    from florida_property_scraper.storage import SQLiteStore

    # Set test DB path
    db_path = tmp_path / "test_sort.db"
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

    # Create test data
    store = SQLiteStore(str(db_path))
    try:
        # Insert leads
        for i in range(3):
            store.upsert_lead(
                {
                    "dedupe_key": f"lead{i}",
                    "county": "Orange",
                    "owner_name": f"Property {i}",
                    "situs_address": f"{i}00 Test St",
                    "parcel_id": f"PARCEL{i}",
                    "lead_score": 80,
                    "captured_at": datetime.now().isoformat(),
                }
            )

        # Insert permits with different dates
        dates = [
            (datetime.now() - timedelta(days=10 * 365)).strftime("%Y-%m-%d"),  # 10y
            (datetime.now() - timedelta(days=5 * 365)).strftime("%Y-%m-%d"),  # 5y
            (datetime.now() - timedelta(days=1 * 365)).strftime("%Y-%m-%d"),  # 1y
        ]

        for i, date in enumerate(dates):
            store.upsert_many_permits(
                [
                    {
                        "county": "Orange",
                        "parcel_id": f"PARCEL{i}",
                        "permit_number": f"BP-{i}",
                        "issue_date": date,
                        "source": "test",
                    }
                ]
            )
    finally:
        store.close()

    client = TestClient(app)

    # Sort by last_permit_oldest - should get Property 0 first (10y ago)
    response = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": "",
            "fields": ["owner_name"],
            "filters": {},
            "sort": "last_permit_oldest",
            "limit": 50,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert data[0]["owner"] == "Property 0"

    # Sort by last_permit_newest - should get Property 2 first (1y ago)
    response = client.post(
        "/api/search/advanced",
        json={
            "county": "Orange",
            "text": "",
            "fields": ["owner_name"],
            "filters": {},
            "sort": "last_permit_newest",
            "limit": 50,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert data[0]["owner"] == "Property 2"
