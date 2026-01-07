from pathlib import Path
from urllib.request import pathname2url

from scrapy.http import TextResponse
from florida_property_scraper.backend import spiders as spiders_pkg

BrowardSpider = spiders_pkg.broward_spider.BrowardSpider


def test_broward_spider_collects_items():
    sample = Path('tests/fixtures/broward_sample.html').absolute()
    file_url = 'file://' + pathname2url(str(sample))

    html = sample.read_bytes()
    resp = TextResponse(url=file_url, body=html)

    spider = BrowardSpider(start_urls=[file_url])
    items = list(spider.parse(resp))

    assert isinstance(items, list)
    assert len(items) >= 2
    for item in items:
        assert item.get("owner")
        assert item.get("address")
        assert item.get("county") == "broward"
