from pathlib import Path
from urllib.request import pathname2url

from scrapy.http import TextResponse
from florida_property_scraper.backend import spiders as spiders_pkg

AlachuaSpider = spiders_pkg.alachua_spider.AlachuaSpider


def test_alachua_spider_collects_items():
    sample = Path('tests/fixtures/alachua_sample.html').absolute()
    file_url = 'file://' + pathname2url(str(sample))

    html = sample.read_bytes()
    resp = TextResponse(url=file_url, body=html)

    spider = AlachuaSpider(start_urls=[file_url])
    items = list(spider.parse(resp))

    assert isinstance(items, list)
    assert len(items) >= 2
    owners = [it.get('owner') for it in items]
    assert 'Demo Owner A' in owners
    assert 'Demo Owner B' in owners
