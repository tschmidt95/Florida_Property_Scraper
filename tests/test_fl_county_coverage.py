from florida_property_scraper.backend.scrapy_runner import resolve_spider_class
from florida_property_scraper.routers.fl_coverage import FL_COUNTIES


def test_fl_county_coverage():
    slugs = [entry["slug"] for entry in FL_COUNTIES]
    assert len(FL_COUNTIES) == 67
    assert len(set(slugs)) == len(slugs)
    required_keys = {
        "slug",
        "display_name",
        "status",
        "capabilities",
        "spider_key",
        "url_template",
    }
    required_capabilities = {
        "supports_query_param",
        "needs_form_post",
        "needs_pagination",
        "needs_js",
        "supports_owner_search",
        "supports_address_search",
    }
    for entry in FL_COUNTIES:
        assert required_keys.issubset(entry.keys())
        assert entry["status"] in ("stub", "live")
        assert required_capabilities.issubset(entry["capabilities"].keys())
        if entry["status"] == "live":
            spider_cls = resolve_spider_class(entry["spider_key"])
            assert spider_cls is not None
