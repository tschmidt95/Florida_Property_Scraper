from florida_property_scraper.routers import registry


def test_registry_exposes_fl():
    assert registry.get_router("fl") is not None


def test_router_module_contract():
    fl = registry.get_router("fl")
    assert hasattr(fl, "enabled_counties")
    assert hasattr(fl, "enabled_jurisdictions")
    assert hasattr(fl, "build_start_urls")
