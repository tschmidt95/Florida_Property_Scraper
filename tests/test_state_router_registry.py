from florida_property_scraper.routers.registry import (
    build_start_urls,
    enabled_jurisdictions,
)


def test_state_router_registry_fl():
    jurisdictions = enabled_jurisdictions("fl")
    assert "broward" in jurisdictions
    assert "orange" in jurisdictions


def test_state_router_build_start_urls():
    urls = build_start_urls("fl", "broward", "John Smith")
    assert urls
    assert "John+Smith" in urls[0] or "John%20Smith" in urls[0]
