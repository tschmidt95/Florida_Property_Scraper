"""Test Seminole County permits parser (no live HTTP)."""

from pathlib import Path

from florida_property_scraper.permits.seminole import SeminolePermitScraper


def test_parse_seminole_permits_from_fixture():
    """Test that we can parse permits from Seminole fixture HTML."""
    fixture_path = (
        Path(__file__).parent / "fixtures" / "permits" / "seminole_search_result.html"
    )
    assert fixture_path.exists(), f"Fixture not found: {fixture_path}"

    with open(fixture_path, "r") as f:
        html_content = f.read()

    scraper = SeminolePermitScraper()
    permits = scraper.parse_permits(html_content, "https://example.com/test")

    # Verify we parsed at least 2 permits
    assert len(permits) >= 2, f"Expected at least 2 permits, got {len(permits)}"

    # Verify key fields are present in first permit
    first_permit = permits[0]
    assert first_permit.county == "seminole"
    assert first_permit.permit_number is not None
    assert len(first_permit.permit_number) > 0
    assert first_permit.source == "https://example.com/test"

    # Check that at least one permit has an address
    has_address = any(p.address is not None for p in permits)
    assert has_address, "At least one permit should have an address"

    # Check that at least one permit has a status
    has_status = any(p.status is not None for p in permits)
    assert has_status, "At least one permit should have a status"
