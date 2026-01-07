from florida_property_scraper.routers import registry


def test_state_router_lookup_and_start_urls():
    urls = registry.build_start_urls("fl", "broward", "John Smith")
    assert urls
    assert "John+Smith" in urls[0] or "John%20Smith" in urls[0]


def test_county_entry_schema_contains_capabilities():
    entry = registry.get_entry("fl", "broward")
    assert entry.get("slug") == "broward"
    assert entry.get("spider_key")
    assert "url_template" in entry
    assert "query_param_style" in entry
    assert "supports_query_param" in entry
    assert "needs_form_post" in entry
    assert "needs_pagination" in entry
    assert "needs_js" in entry
