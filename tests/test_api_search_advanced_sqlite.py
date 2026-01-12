"""Test advanced search with SQLite database."""
import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime, timedelta

try:
    from fastapi.testclient import TestClient
    from florida_property_scraper.api.app import app
    from florida_property_scraper.storage import SQLiteStore
    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False


@pytest.mark.skipif(not DEPS_AVAILABLE, reason="FastAPI not available")
def test_advanced_search_field_selection():
    """Test that field selection changes results."""
    client = TestClient(app)
    
    # Create temporary database with test data
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    
    try:
        store = SQLiteStore(db_path)
        
        # Insert test leads
        store.conn.execute(
            """
            INSERT INTO leads (county, owner_name, situs_address, parcel_id)
            VALUES (?, ?, ?, ?)
            """,
            ("seminole", "John Smith", "123 Main St", "12-34-56")
        )
        store.conn.execute(
            """
            INSERT INTO leads (county, owner_name, situs_address, parcel_id)
            VALUES (?, ?, ?, ?)
            """,
            ("seminole", "Jane Doe", "456 Smith Rd", "78-90-12")
        )
        store.conn.commit()
        store.close()
        
        # Search in owner field only - should find "Smith"
        with patch.dict(os.environ, {"LEADS_SQLITE_PATH": db_path}):
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "Smith",
                    "fields": ["owner"],
                    "limit": 50
                }
            )
            
            assert response.status_code == 200
            results = response.json()
            assert len(results) == 1
            assert results[0]["owner"] == "John Smith"
        
        # Search in address field only - should find "Smith Rd"
        with patch.dict(os.environ, {"LEADS_SQLITE_PATH": db_path}):
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "Smith",
                    "fields": ["address"],
                    "limit": 50
                }
            )
            
            assert response.status_code == 200
            results = response.json()
            assert len(results) == 1
            assert "Smith Rd" in results[0]["address"]
        
        # Search in both fields - should find both
        with patch.dict(os.environ, {"LEADS_SQLITE_PATH": db_path}):
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "Smith",
                    "fields": ["owner", "address"],
                    "limit": 50
                }
            )
            
            assert response.status_code == 200
            results = response.json()
            assert len(results) == 2
    
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


@pytest.mark.skipif(not DEPS_AVAILABLE, reason="FastAPI not available")
def test_advanced_search_no_permits_in_years():
    """Test no_permits_in_years filter."""
    client = TestClient(app)
    
    # Create temporary database with test data
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    
    try:
        store = SQLiteStore(db_path)
        
        # Insert test leads
        store.conn.execute(
            """
            INSERT INTO leads (id, county, owner_name, situs_address, parcel_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (1, "seminole", "No Permits Owner", "111 No Permits St", "11-11-11")
        )
        store.conn.execute(
            """
            INSERT INTO leads (id, county, owner_name, situs_address, parcel_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (2, "seminole", "Recent Permit Owner", "222 Recent St", "22-22-22")
        )
        store.conn.execute(
            """
            INSERT INTO leads (id, county, owner_name, situs_address, parcel_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (3, "seminole", "Old Permit Owner", "333 Old St", "33-33-33")
        )
        
        # Insert permits
        # Recent permit (within 15 years)
        recent_date = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y-%m-%d")
        store.conn.execute(
            """
            INSERT INTO permits (county, parcel_id, permit_number, source, issue_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("seminole", "22-22-22", "BP2019-001", "http://example.com", recent_date)
        )
        
        # Old permit (more than 15 years ago)
        old_date = (datetime.now() - timedelta(days=365 * 20)).strftime("%Y-%m-%d")
        store.conn.execute(
            """
            INSERT INTO permits (county, parcel_id, permit_number, source, issue_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("seminole", "33-33-33", "BP2004-001", "http://example.com", old_date)
        )
        
        store.conn.commit()
        store.close()
        
        # Search with no_permits_in_years=15
        # Should match: No Permits Owner (null) and Old Permit Owner (>15 years)
        # Should NOT match: Recent Permit Owner (<15 years)
        with patch.dict(os.environ, {"LEADS_SQLITE_PATH": db_path}):
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "fields": ["owner", "address"],
                    "filters": {
                        "no_permits_in_years": 15
                    },
                    "limit": 50
                }
            )
            
            assert response.status_code == 200
            results = response.json()
            owners = [r["owner"] for r in results]
            
            # Should include properties with no permits or old permits
            assert "No Permits Owner" in owners
            assert "Old Permit Owner" in owners
            # Should NOT include property with recent permit
            assert "Recent Permit Owner" not in owners
    
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


@pytest.mark.skipif(not DEPS_AVAILABLE, reason="FastAPI not available")
def test_advanced_search_sort_by_permit_date():
    """Test sorting by last permit date."""
    client = TestClient(app)
    
    # Create temporary database with test data
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    
    try:
        store = SQLiteStore(db_path)
        
        # Insert test leads with different permit dates
        for i, (name, parcel, permit_year) in enumerate([
            ("Newest Permit", "11-11-11", 2024),
            ("Oldest Permit", "22-22-22", 2010),
            ("Middle Permit", "33-33-33", 2015),
            ("No Permit", "44-44-44", None),
        ], start=1):
            store.conn.execute(
                """
                INSERT INTO leads (id, county, owner_name, situs_address, parcel_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (i, "seminole", name, f"{i * 111} Test St", parcel)
            )
            
            if permit_year:
                store.conn.execute(
                    """
                    INSERT INTO permits (county, parcel_id, permit_number, source, issue_date)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("seminole", parcel, f"BP{permit_year}-001", "http://example.com", f"{permit_year}-01-01")
                )
        
        store.conn.commit()
        store.close()
        
        # Test sort by last_permit_oldest
        with patch.dict(os.environ, {"LEADS_SQLITE_PATH": db_path}):
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "fields": ["owner"],
                    "sort": "last_permit_oldest",
                    "limit": 50
                }
            )
            
            assert response.status_code == 200
            results = response.json()
            # First result should be "No Permit" (null dates come first)
            # or "Oldest Permit" (2010)
            owners = [r["owner"] for r in results]
            assert owners[0] in ["No Permit", "Oldest Permit"]
        
        # Test sort by last_permit_newest
        with patch.dict(os.environ, {"LEADS_SQLITE_PATH": db_path}):
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "fields": ["owner"],
                    "sort": "last_permit_newest",
                    "limit": 50
                }
            )
            
            assert response.status_code == 200
            results = response.json()
            # First result should be "Newest Permit" (2024)
            assert results[0]["owner"] == "Newest Permit"
    
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


from unittest.mock import patch


@pytest.mark.skipif(not DEPS_AVAILABLE, reason="FastAPI not available")
def test_advanced_search_without_permits_table():
    """Test advanced search when permits table doesn't exist."""
    client = TestClient(app)
    
    # Create temporary database with only leads table
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    
    try:
        # Create a minimal database with just leads
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE leads (
                id INTEGER PRIMARY KEY,
                county TEXT,
                owner_name TEXT,
                situs_address TEXT,
                parcel_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO leads (county, owner_name, situs_address, parcel_id)
            VALUES (?, ?, ?, ?)
            """,
            ("seminole", "Test Owner", "123 Test St", "12-34-56")
        )
        conn.commit()
        conn.close()
        
        # Search should work without permits table
        with patch.dict(os.environ, {"LEADS_SQLITE_PATH": db_path}):
            response = client.post(
                "/api/search/advanced",
                json={
                    "county": "seminole",
                    "text": "Test",
                    "fields": ["owner"],
                    "limit": 50
                }
            )
            
            assert response.status_code == 200
            results = response.json()
            assert len(results) == 1
            assert results[0]["owner"] == "Test Owner"
            # Should have null/zero permits enrichment
            assert results[0]["last_permit_date"] is None
            assert results[0]["permits_last_15y_count"] == 0
    
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)
