from pathlib import Path

import pytest
from scrapy.http import TextResponse

from florida_property_scraper.backend.native.parsers import get_parser
from florida_property_scraper.backend.spiders import (
    AlachuaSpider,
    BrowardSpider,
    SeminoleSpider,
    OrangeSpider,
    PalmBeachSpider,
    MiamiDadeSpider,
    HillsboroughSpider,
    PinellasSpider,
)
from florida_property_scraper.schema import REQUIRED_FIELDS


SPIDERS = {
    "alachua": AlachuaSpider,
    "broward": BrowardSpider,
    "seminole": SeminoleSpider,
    "orange": OrangeSpider,
    "palm_beach": PalmBeachSpider,
    "miami_dade": MiamiDadeSpider,
    "hillsborough": HillsboroughSpider,
    "pinellas": PinellasSpider,
}


@pytest.mark.parametrize("county", sorted(SPIDERS.keys()))
def test_backend_parity_realistic_fixtures(county):
    fixture = Path(f"tests/fixtures/{county}_realistic.html")
    html = fixture.read_text(encoding="utf-8")

    spider = SPIDERS[county]()
    response = TextResponse(url="http://example.local", body=html, encoding="utf-8")
    scrapy_items = list(spider.parse(response))

    native_parser = get_parser(county)
    native_items = native_parser(html, "file://fixture", county)

    assert scrapy_items
    assert native_items

    for items in (scrapy_items, native_items):
        for item in items:
            for field in REQUIRED_FIELDS:
                assert field in item
            assert item["county"] == county
