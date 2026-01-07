from florida_property_scraper.backend.scrapy_runner import resolve_spider_name


def test_resolve_spider_name():
    assert resolve_spider_name("broward_spider") == "broward"
    assert resolve_spider_name("Broward_Spider") == "broward"
    assert resolve_spider_name("broward") == "broward"
