from pathlib import Path

import pytest

from florida_property_scraper.backend.native.parsers import get_parser
from florida_property_scraper.schema import REQUIRED_FIELDS


@pytest.mark.parametrize(
    "county",
    [
        "alachua",
        "broward",
        "seminole",
        "orange",
        "palm_beach",
        "miami_dade",
        "hillsborough",
        "pinellas",
    ],
)
def test_native_parsers_realistic_fixtures(county):
    fixture = Path(f"tests/fixtures/{county}_realistic.html")
    parser = get_parser(county)
    html = fixture.read_text(encoding="utf-8")
    items = parser(html, "file://fixture", county)
    assert len(items) >= 2
    for item in items:
        for field in REQUIRED_FIELDS:
            assert field in item
        assert item["county"] == county
        assert item["owner"]
        assert item["address"]
        assert len(item["raw_html"]) <= 2000
