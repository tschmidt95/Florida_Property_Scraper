from florida_property_scraper.backend.spiders import SPIDERS


def test_spider_registry_contains_alachua():
    assert 'alachua' in SPIDERS
    assert callable(SPIDERS['alachua'])
