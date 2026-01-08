from florida_property_scraper.backend.scrapy_runner import resolve_spider_class
from florida_property_scraper.backend.spiders.alachua_spider import AlachuaSpider
from florida_property_scraper.backend.spiders.broward_spider import BrowardSpider
from florida_property_scraper.backend.spiders.duval_spider import DuvalSpider
from florida_property_scraper.backend.spiders.seminole_spider import SeminoleSpider
from florida_property_scraper.backend.spiders.orange_spider import OrangeSpider
from florida_property_scraper.backend.spiders.palm_beach_spider import PalmBeachSpider
from florida_property_scraper.backend.spiders.polk_spider import PolkSpider


def test_resolve_spider_class_names():
    cases = {
        "broward": BrowardSpider,
        "broward_spider": BrowardSpider,
        "seminole": SeminoleSpider,
        "seminole_spider": SeminoleSpider,
        "orange": OrangeSpider,
        "orange_spider": OrangeSpider,
        "palm_beach": PalmBeachSpider,
        "palm_beach_spider": PalmBeachSpider,
        "alachua": AlachuaSpider,
        "alachua_spider": AlachuaSpider,
        "duval": DuvalSpider,
        "duval_spider": DuvalSpider,
        "polk": PolkSpider,
        "polk_spider": PolkSpider,
    }
    for name, cls in cases.items():
        assert resolve_spider_class(name) is cls
