from florida_property_scraper.scraper import FloridaPropertyScraper
import pytest

def test_demo_mode_allows_no_key_scrapy():
    s = FloridaPropertyScraper(scrapingbee_api_key=None, demo=True, backend='scrapy')
    assert s.adapter is not None
    assert s.adapter.search('anything')

def test_scrapingbee_requires_key():
    with pytest.raises(ValueError):
        FloridaPropertyScraper(scrapingbee_api_key=None, demo=False, backend='scrapingbee')
