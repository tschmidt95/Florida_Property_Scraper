from pathlib import Path
from urllib.request import pathname2url

from scrapy.http import TextResponse
from florida_property_scraper.backend import spiders as spiders_pkg
from florida_property_scraper.schema import REQUIRED_FIELDS

SeminoleSpider = spiders_pkg.seminole_spider.SeminoleSpider


def test_seminole_spider_collects_items():
    sample = Path('tests/fixtures/seminole_sample.html').absolute()
    file_url = 'file://' + pathname2url(str(sample))

    html = sample.read_bytes()
    resp = TextResponse(url=file_url, body=html)

    spider = SeminoleSpider(start_urls=[file_url])
    items = list(spider.parse(resp))

    assert isinstance(items, list)
    assert len(items) >= 2
    for item in items:
        for field in REQUIRED_FIELDS:
            assert field in item
        assert item.get("county") == "seminole"
        assert item.get("owner")
        assert item.get("address")
