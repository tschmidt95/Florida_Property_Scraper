from importlib import util
from pathlib import Path

from scrapy.http import TextResponse
from scrapy.http.request import Request
from scrapy.http.request.form import FormRequest

from florida_property_scraper.backend.scrapy_runner import resolve_spider_class
from florida_property_scraper.routers.fl_coverage import FL_COUNTIES
from florida_property_scraper.county_router import get_county_entry
from florida_property_scraper.schema import REQUIRED_FIELDS


def test_county_contract():
    root = Path(__file__).resolve().parents[1]
    live_slugs = [c["slug"] for c in FL_COUNTIES if c.get("status") == "live"]
    for slug in live_slugs:
        entry = get_county_entry(slug)
        spider_key = entry["spider_key"]
        spider_cls = resolve_spider_class(spider_key)
        fixture = root / "tests" / "fixtures" / f"{slug}_sample.html"
        test_path = root / "tests" / f"test_{slug}_spider_integration.py"
        assert fixture.exists()
        assert test_path.exists()
        spec = util.find_spec(f"florida_property_scraper.backend.spiders.{slug}_spider")
        assert spec is not None
        spider = spider_cls(
            start_urls=["file://fixture"],
            pagination=entry.get("pagination"),
            page_param=entry.get("page_param"),
            form_url=entry.get("form_url", ""),
            form_fields_template=entry.get("form_fields_template", {}),
            query="Smith",
        )
        requests = list(spider.start_requests())
        if entry.get("query_param_style") == "form":
            assert any(isinstance(req, FormRequest) for req in requests)
        else:
            assert any(isinstance(req, Request) for req in requests)
        html = fixture.read_bytes()
        resp = TextResponse(url="file://fixture", body=html)
        outputs = list(spider.parse(resp))
        items = [o for o in outputs if isinstance(o, dict)]
        assert items
        for item in items:
            for field in REQUIRED_FIELDS:
                assert field in item
        if entry.get("pagination") == "page_param":
            resp = TextResponse(
                url=f"http://example.invalid/?{entry.get('page_param')}=1",
                body=b"<html><body></body></html>",
            )
            resp.request = Request(resp.url, meta={"page": 1})
            outputs = list(spider.parse(resp))
            assert any(isinstance(o, Request) for o in outputs)
        if entry.get("pagination") == "next_link":
            html = b"<html><body><a rel='next' href='next.html'>Next</a></body></html>"
            resp = TextResponse(url="http://example.invalid", body=html)
            resp.request = Request(resp.url, meta={"page": 1})
            outputs = list(spider.parse(resp))
            assert any(isinstance(o, Request) for o in outputs)
