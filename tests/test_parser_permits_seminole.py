"""Test Seminole permits parser."""

from pathlib import Path

from florida_property_scraper.permits.seminole import parse_permits


def test_parse_seminole_permits_fixture():
    """Test parsing Seminole permits from fixture."""
    fixture_path = (
        Path(__file__).parent / "fixtures" / "permits" / "seminole_search_result.html"
    )
    html = fixture_path.read_text()
    source_url = "https://semc-egov.aspgov.com/Click2GovBP/Search.aspx"

    permits = parse_permits(html, source_url)

    # Assert at least 2 permits parsed
    assert len(permits) >= 2, f"Expected at least 2 permits, got {len(permits)}"

    # Check first permit
    p1 = permits[0]
    assert p1.county == "seminole"
    assert p1.permit_number == "BP2023-001"
    assert p1.parcel_id == "1234567890"
    assert p1.address == "123 Main St"
    assert p1.permit_type == "Building"
    assert p1.status == "Finaled"
    assert p1.issue_date == "2023-01-15"
    assert p1.final_date == "2023-06-30"
    assert p1.description == "New single family home"
    assert p1.source == source_url

    # Check second permit
    p2 = permits[1]
    assert p2.permit_number == "BP2023-002"
    assert p2.status == "Issued"
