from __future__ import annotations

from pathlib import Path

from florida_property_scraper.scrapers.seminole import parse_results


def test_parser_seminole_fixture_extracts_owner_address():
    repo_root = Path(__file__).resolve().parents[1]
    fixture = repo_root / "tests" / "fixtures" / "seminole_realistic.html"
    html = fixture.read_text(encoding="utf-8")

    results = parse_results(
        html,
        county="Seminole",
        query="owner",
        base_url="https://www.seminolecountyfl.gov/property-search",
    )

    assert len(results) >= 1
    assert results[0].owner
    assert results[0].address
    assert results[0].county == "Seminole"
