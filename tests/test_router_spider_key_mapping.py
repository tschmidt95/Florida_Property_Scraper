from florida_property_scraper.backend.spiders import SPIDERS
from florida_property_scraper.county_router import (
    build_start_urls,
    enabled_counties,
    get_county_entry,
)


def test_router_start_urls_and_spider_keys():
    query = "Smith"
    for slug in enabled_counties():
        urls = build_start_urls(slug, query)
        assert urls
        entry = get_county_entry(slug)
        spider_key = entry.get("spider_key")
        assert spider_key in SPIDERS
