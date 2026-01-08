from pathlib import Path

from florida_property_scraper.backend.native.native_runner import run_on_fixture
from florida_property_scraper.schema import REQUIRED_FIELDS


def test_native_runner_on_fixture():
    fixture = Path("tests/fixtures/broward_realistic.html")
    items = run_on_fixture("broward", fixture, max_items=2)
    assert items
    for item in items:
        for field in REQUIRED_FIELDS:
            assert field in item
