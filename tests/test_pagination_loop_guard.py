from scrapy.http import TextResponse
from scrapy.http.request import Request

from florida_property_scraper.spider_utils import next_page_request


def test_pagination_loop_guard():
    html = b"<html><body><a rel='next' href='http://example.invalid'>Next</a></body></html>"
    resp = TextResponse(url="http://example.invalid", body=html)
    resp.request = Request(resp.url, meta={"page": 1, "visited_pages": {1}})
    req = next_page_request(resp, "next_link", "", 3)
    assert req is None
