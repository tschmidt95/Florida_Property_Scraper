from pathlib import Path

from florida_property_scraper.backend.native_adapter import NativeAdapter


def test_native_max_items():
    fixture = Path("tests/fixtures/orange_realistic.html")
    adapter = NativeAdapter()
    items = adapter.search(
        query="Smith",
        start_urls=[f"file://{fixture.resolve()}"],
        spider_name="orange_spider",
        max_items=1,
        live=False,
        county_slug="orange",
    )
    assert len(items) == 1
