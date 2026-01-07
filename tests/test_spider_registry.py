from florida_property_scraper.backend.spiders import SPIDERS


def test_spider_registry_contains_alachua():
    assert 'alachua' in SPIDERS
    assert callable(SPIDERS['alachua'])


def test_spider_registry_contains_broward():
    assert 'broward' in SPIDERS
    assert callable(SPIDERS['broward'])
    assert 'broward_spider' in SPIDERS
    assert callable(SPIDERS['broward_spider'])
