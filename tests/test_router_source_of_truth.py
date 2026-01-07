import inspect

import florida_property_scraper.county_router as county_router
import florida_property_scraper.routers.fl as fl_router
import florida_property_scraper.scraper as scraper_module


def test_only_fl_router_has_entries():
    assert hasattr(fl_router, "_ENTRIES")
    assert not hasattr(county_router, "_ENTRIES")


def test_scraper_does_not_import_county_router():
    source = inspect.getsource(scraper_module)
    assert "county_router" not in source
