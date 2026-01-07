from scrapy.http import TextResponse

from florida_property_scraper.backend.spiders.orange_spider import OrangeSpider


def test_malformed_html_no_crash():
    html = "<html><body><div><span>Owner</span><span>Bad</span>"
    resp = TextResponse(url="file://malformed", body=html.encode())
    spider = OrangeSpider(start_urls=["file://malformed"], debug_html=True)
    items = list(spider.parse(resp))
    assert items
