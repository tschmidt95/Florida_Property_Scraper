from scrapy.http import TextResponse

from florida_property_scraper.backend.spiders.palm_beach_spider import PalmBeachSpider


def test_oversized_response_truncated():
    html = "<html><body>" + ("X" * 60000) + "</body></html>"
    resp = TextResponse(url="file://big", body=html.encode())
    spider = PalmBeachSpider(start_urls=["file://big"], debug_html=True)
    items = list(spider.parse(resp))
    assert items
    assert len(items[0].get("raw_html", "")) <= 2000
