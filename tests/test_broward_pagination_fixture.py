from pathlib import Path
from urllib.request import pathname2url

from scrapy.http import TextResponse
from scrapy.http.request import Request
from florida_property_scraper.backend.spiders.broward_spider import BrowardSpider


def test_broward_pagination_fixture():
    page1 = Path("tests/fixtures/broward_paged_1.html").absolute()
    page2 = Path("tests/fixtures/broward_paged_2.html").absolute()
    file_url_1 = "file://" + pathname2url(str(page1))
    file_url_2 = "file://" + pathname2url(str(page2))

    html1 = page1.read_bytes()
    resp1 = TextResponse(url=file_url_1, body=html1)
    spider = BrowardSpider(start_urls=[file_url_1], pagination="next_link")
    outputs = list(spider.parse(resp1))

    items = [o for o in outputs if isinstance(o, dict)]
    reqs = [o for o in outputs if isinstance(o, Request)]
    assert reqs

    html2 = page2.read_bytes()
    resp2 = TextResponse(url=file_url_2, body=html2)
    items.extend(list(spider.parse(resp2)))

    assert len(items) >= 3
