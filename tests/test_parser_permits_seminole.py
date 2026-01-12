from pathlib import Path

from florida_property_scraper.permits.seminole import parse_permits


def test_parse_permits_seminole_fixture_parses_two_rows():
    fixture = (
        Path(__file__).parent
        / "fixtures"
        / "permits"
        / "seminole_search_result.html"
    )
    html = fixture.read_text(encoding="utf-8")
    records = parse_permits(html, source_url="fixture://seminole")

    assert len(records) >= 2

    r0 = records[0]
    assert r0.county == "seminole"
    assert r0.permit_number
    assert r0.address
    assert r0.source == "fixture://seminole"

    # Spot-check known rows
    numbers = {r.permit_number for r in records}
    assert "BP-2024-000123" in numbers
    assert "BP-2023-000987" in numbers
