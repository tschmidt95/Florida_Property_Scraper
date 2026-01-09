from pathlib import Path
from urllib.request import pathname2url

from scrapy.http import TextResponse
from florida_property_scraper.backend import spiders as spiders_pkg
from florida_property_scraper.schema import REQUIRED_FIELDS

BrowardSpider = spiders_pkg.broward_spider.BrowardSpider


def test_broward_spider_realistic_fixture():
    sample = Path("tests/fixtures/broward_realistic.html").absolute()
    file_url = "file://" + pathname2url(str(sample))

    html = sample.read_bytes()
    resp = TextResponse(url=file_url, body=html)

    spider = BrowardSpider(start_urls=[file_url])
    items = list(spider.parse(resp))

    assert len(items) >= 2
    for item in items:
        for field in REQUIRED_FIELDS:
            assert field in item
        assert item.get("county") == "broward"
        assert item.get("owner")
        assert item.get("address")
