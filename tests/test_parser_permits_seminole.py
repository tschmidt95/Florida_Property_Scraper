"""Tests for Seminole County permits parser (CI-safe, no live HTTP)."""

import pytest
from pathlib import Path


pytest.importorskip("bs4")


def test_seminole_parse_permits_from_fixture():
    """Test parsing Seminole County permits from fixture HTML."""
    from florida_property_scraper.permits.seminole import SeminolePermitScraper

    scraper = SeminolePermitScraper()
    fixture_path = (
        Path(__file__).parent / "fixtures" / "permits" / "seminole_search_result.html"
    )

    assert fixture_path.exists(), f"Fixture not found: {fixture_path}"

    with open(fixture_path, "r") as f:
        content = f.read()

    permits = scraper.parse_permits(content, "https://test.example.com/search")

    # Must parse at least 2 permits
    assert len(permits) >= 2, f"Expected at least 2 permits, got {len(permits)}"

    # Check first permit has required fields
    first_permit = permits[0]
    assert first_permit.county == "seminole"
    assert first_permit.permit_number is not None
    assert len(first_permit.permit_number) > 0
    assert first_permit.source == "https://test.example.com/search"

    # At least one permit should have address
    addresses = [p.address for p in permits if p.address]
    assert len(addresses) > 0, "At least one permit should have an address"

    # At least one permit should have a status
    statuses = [p.status for p in permits if p.status]
    assert len(statuses) > 0, "At least one permit should have a status"

    # At least one permit should have an issue_date
    issue_dates = [p.issue_date for p in permits if p.issue_date]
    assert len(issue_dates) > 0, "At least one permit should have an issue_date"


def test_seminole_parse_permits_validates_permit_number():
    """Test that parser requires permit_number."""
    from florida_property_scraper.permits.seminole import SeminolePermitScraper

    scraper = SeminolePermitScraper()

    # HTML with no permit numbers should return empty list
    html = "<html><body><table><tr><td>No permits here</td></tr></table></body></html>"
    permits = scraper.parse_permits(html, "https://test.example.com")

    assert permits == []


def test_seminole_search_permits_requires_live():
    """Test that search_permits() requires LIVE=1."""
    import os
    from florida_property_scraper.permits.seminole import SeminolePermitScraper

    scraper = SeminolePermitScraper()

    # Ensure LIVE is not set
    old_value = os.environ.pop("LIVE", None)
    try:
        with pytest.raises(RuntimeError, match="LIVE=1"):
            scraper.search_permits("123 Main St")
    finally:
        if old_value:
            os.environ["LIVE"] = old_value


def test_seminole_permit_to_dict():
    """Test PermitRecord.to_dict() conversion."""
    from florida_property_scraper.permits.models import PermitRecord

    permit = PermitRecord(
        county="seminole",
        parcel_id="12-34-56-78",
        address="123 Main St",
        permit_number="BP-2023-001234",
        permit_type="Building",
        status="Issued",
        issue_date="2023-03-15",
        final_date=None,
        description="New construction",
        source="https://example.com",
        raw=None,
    )

    result = permit.to_dict()

    assert result["county"] == "seminole"
    assert result["parcel_id"] == "12-34-56-78"
    assert result["address"] == "123 Main St"
    assert result["permit_number"] == "BP-2023-001234"
    assert result["permit_type"] == "Building"
    assert result["status"] == "Issued"
    assert result["issue_date"] == "2023-03-15"
    assert result["final_date"] is None
