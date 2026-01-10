from florida_property_scraper.backend.spiders import SPIDERS


def test_spider_registry_contains_alachua():
    assert "alachua" in SPIDERS
    assert callable(SPIDERS["alachua"])


def test_spider_registry_contains_broward():
    assert "broward" in SPIDERS
    assert callable(SPIDERS["broward"])
    assert "broward_spider" in SPIDERS
    assert callable(SPIDERS["broward_spider"])


def test_spider_registry_contains_seminole():
    assert "seminole" in SPIDERS
    assert callable(SPIDERS["seminole"])
    assert "seminole_spider" in SPIDERS
    assert callable(SPIDERS["seminole_spider"])


def test_spider_registry_contains_orange():
    assert "orange" in SPIDERS
    assert callable(SPIDERS["orange"])
    assert "orange_spider" in SPIDERS
    assert callable(SPIDERS["orange_spider"])


def test_spider_registry_contains_palm_beach():
    assert "palm_beach" in SPIDERS
    assert callable(SPIDERS["palm_beach"])
    assert "palm_beach_spider" in SPIDERS
    assert callable(SPIDERS["palm_beach_spider"])
