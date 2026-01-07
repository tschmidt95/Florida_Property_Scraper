from florida_property_scraper.county_router import build_start_urls


def test_query_encoding_safe():
    urls = build_start_urls("broward", '"; rm -rf /"')
    assert urls
    assert " " not in urls[0]
