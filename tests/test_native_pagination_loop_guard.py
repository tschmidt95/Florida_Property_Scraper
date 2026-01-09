from pathlib import Path

from florida_property_scraper.backend.native.engine import NativeEngine
from florida_property_scraper.backend.native.extract import parse_label_items


def _parser(html, url, county_slug):
    return parse_label_items(html, county_slug)


def _get_next_urls(html, base_url):
    if "page1" in base_url:
        return ["http://example.local/page2"]
    return ["http://example.local/page1"]


def test_native_pagination_loop_guard():
    engine = NativeEngine(max_pages=5)
    fixture_map = {
        "http://example.local/page1": Path(
            "tests/fixtures/broward_paged_1.html"
        ).read_text(encoding="utf-8"),
        "http://example.local/page2": Path(
            "tests/fixtures/broward_paged_2.html"
        ).read_text(encoding="utf-8"),
    }
    _parser.get_next_urls = _get_next_urls
    items = engine.run(
        [{"url": "http://example.local/page1", "method": "GET"}],
        _parser,
        "broward",
        dry_run=True,
        fixture_map=fixture_map,
    )
    assert len(items) == 2
