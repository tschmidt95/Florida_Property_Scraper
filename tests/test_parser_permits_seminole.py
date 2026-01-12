"""Test Seminole permits parser."""
import pytest
from pathlib import Path

from florida_property_scraper.permits.seminole import parse_permits


def test_parse_seminole_permits_from_fixture():
    """Test parsing Seminole permits from fixture HTML."""
    fixture_path = Path(__file__).parent / "fixtures" / "permits" / "seminole_search_result.html"
    assert fixture_path.exists(), f"Fixture not found: {fixture_path}"
    
    with open(fixture_path, "r") as f:
        html = f.read()
    
    source_url = "https://semc-egov.aspgov.com/Click2GovBP/Search?query=test"
    permits = parse_permits(html, source_url)
    
    # Assert at least 2 permits parsed
    assert len(permits) >= 2, f"Expected at least 2 permits, got {len(permits)}"
    
    # Check first permit has key fields
    permit1 = permits[0]
    assert permit1.county == "seminole"
    assert permit1.permit_number is not None
    assert len(permit1.permit_number) > 0
    assert permit1.source == source_url
    
    # Check that at least one permit has address
    addresses = [p.address for p in permits if p.address]
    assert len(addresses) > 0, "At least one permit should have an address"
    
    # Check that at least one permit has status
    statuses = [p.status for p in permits if p.status]
    assert len(statuses) > 0, "At least one permit should have a status"
    
    # Check that at least one permit has permit_type
    types = [p.permit_type for p in permits if p.permit_type]
    assert len(types) > 0, "At least one permit should have a permit_type"


def test_parse_permits_empty_html():
    """Test parsing empty HTML returns empty list."""
    permits = parse_permits("<html></html>", "http://example.com")
    assert permits == []


def test_parse_permits_no_table():
    """Test parsing HTML without table returns empty list."""
    html = "<html><body><h1>No permits found</h1></body></html>"
    permits = parse_permits(html, "http://example.com")
    assert permits == []
