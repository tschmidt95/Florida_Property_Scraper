from pathlib import Path
from urllib.request import pathname2url

from scrapy.http import TextResponse
from scrapy.http.request.form import FormRequest

from florida_property_scraper.backend.spiders.seminole_spider import SeminoleSpider
from florida_property_scraper.schema import REQUIRED_FIELDS


def test_seminole_form_submission():
    spider = SeminoleSpider(
        start_urls=["http://example.invalid/form"],
        form_url="http://example.invalid/form",
        form_fields_template={"owner": "{query}"},
        query="Smith",
    )
    requests = list(spider.start_requests())
    assert requests
    assert isinstance(requests[0], FormRequest)
    assert b"Smith" in requests[0].body

    sample = Path("tests/fixtures/seminole_form_response.html").absolute()
    file_url = "file://" + pathname2url(str(sample))
    html = sample.read_bytes()
    resp = TextResponse(url=file_url, body=html)
    items = list(spider.parse(resp))
    assert len(items) >= 2
    for item in items:
        for field in REQUIRED_FIELDS:
            assert field in item
        assert item.get("owner")
        assert item.get("address")
