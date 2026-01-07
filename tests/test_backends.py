from florida_property_scraper.scraper import FloridaPropertyScraper


def test_demo_mode_uses_scrapy_adapter():
    s = FloridaPropertyScraper(demo=True)
    assert s.adapter is not None
    assert s.adapter.search('anything')
