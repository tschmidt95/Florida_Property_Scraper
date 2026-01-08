from florida_property_scraper.county_router import (
    build_start_urls,
    canonicalize_county_name,
)
from florida_property_scraper.routers.fl_coverage import FL_COUNTIES


def test_canonicalize_county_name_variants():
    assert canonicalize_county_name("Palm Beach") == "palm_beach"
    assert canonicalize_county_name("palm beach") == "palm_beach"
    assert canonicalize_county_name("PALM_BEACH") == "palm_beach"


def test_build_start_urls_for_enabled_counties():
    query = "John Smith"
    live_slugs = [c["slug"] for c in FL_COUNTIES if c.get("status") == "live"]
    for county in live_slugs:
        urls = build_start_urls(county, query)
        assert urls
        if "John+Smith" not in urls[0]:
            assert urls[0].startswith("http")
