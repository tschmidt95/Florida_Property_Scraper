from pathlib import Path
from urllib.request import pathname2url

from scrapy.http import TextResponse
from florida_property_scraper.backend import spiders as spiders_pkg
from florida_property_scraper.schema import REQUIRED_FIELDS

OrangeSpider = spiders_pkg.orange_spider.OrangeSpider


def test_orange_spider_realistic_fixture():
    sample = Path("tests/fixtures/orange_realistic.html").absolute()
    file_url = "file://" + pathname2url(str(sample))

    html = sample.read_bytes()
    resp = TextResponse(url=file_url, body=html)

    spider = OrangeSpider(start_urls=[file_url])
    items = list(spider.parse(resp))

    assert len(items) >= 2
    for item in items:
        for field in REQUIRED_FIELDS:
            assert field in item
        assert item.get("county") == "orange"
        assert item.get("owner")
        assert item.get("address")
