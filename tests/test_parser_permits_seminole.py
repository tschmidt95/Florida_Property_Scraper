"""Tests for Seminole County permits parser."""

from pathlib import Path

from florida_property_scraper.permits.seminole import parse_permits


def test_parse_seminole_permits():
    """Test parsing Seminole County permit search results from fixture."""
    fixture_path = (
        Path(__file__).parent / "fixtures" / "permits" / "seminole_search_result.html"
    )
    assert fixture_path.exists(), f"Fixture not found: {fixture_path}"

    html = fixture_path.read_text()
    source_url = "https://semc-egov.aspgov.com/Click2GovBP/PermitSearch.aspx"

    permits = parse_permits(html, source_url)

    # Assert we parsed at least 2 permits
    assert len(permits) >= 2, f"Expected at least 2 permits, got {len(permits)}"

    # Check first permit
    permit1 = permits[0]
    assert permit1.county == "seminole"
    assert permit1.permit_number == "BP-2023-001234"
    assert permit1.address == "123 MAIN ST"
    assert permit1.permit_type == "Building"
    assert permit1.status == "Issued"
    assert permit1.issue_date == "2023-01-15"
    assert permit1.final_date == "2023-03-20"
    assert "single family" in permit1.description.lower()
    assert permit1.source == source_url

    # Check second permit
    permit2 = permits[1]
    assert permit2.permit_number == "BP-2023-005678"
    assert permit2.address == "456 ELM AVE"
    assert permit2.status == "Finaled"
    assert permit2.issue_date == "2023-02-10"

    # Check third permit (no final date)
    if len(permits) >= 3:
        permit3 = permits[2]
        assert permit3.permit_number == "BP-2024-000111"
        assert permit3.final_date is None or permit3.final_date == ""
