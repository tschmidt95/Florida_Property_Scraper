from scrapy.http import TextResponse

from florida_property_scraper.backend.spiders.broward_spider import BrowardSpider
from florida_property_scraper.schema import REQUIRED_FIELDS


def test_malicious_html_handled():
    payload = (
        "<html><body>"
        "<script>alert(1)</script>"
        "<iframe src='http://evil'></iframe>"
        "<svg onload='alert(1)'></svg>"
        + ("<div>" * 1000)
        + ("</div>" * 1000)
        + "</body></html>"
    )
    resp = TextResponse(url="file://malicious", body=payload.encode())
    spider = BrowardSpider(start_urls=["file://malicious"], debug_html=True)
    items = list(spider.parse(resp))
    assert items
    for item in items:
        for field in REQUIRED_FIELDS:
            assert field in item
        assert len(item.get("raw_html", "")) <= 50000
