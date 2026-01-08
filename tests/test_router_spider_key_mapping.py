from florida_property_scraper.backend.spiders import SPIDERS
from florida_property_scraper.routers.fl import build_start_urls
from florida_property_scraper.routers.fl import get_entry as get_county_entry
from florida_property_scraper.routers.fl_coverage import FL_COUNTIES


def test_router_start_urls_and_spider_keys():
    query = "Smith"
    live_slugs = [c["slug"] for c in FL_COUNTIES if c.get("status") == "live"]
    for slug in live_slugs:
        urls = build_start_urls(slug, query)
        assert urls
        entry = get_county_entry(slug)
        spider_key = entry.get("spider_key")
        assert spider_key in SPIDERS
