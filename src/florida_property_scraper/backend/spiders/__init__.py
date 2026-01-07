"""Spider registry for available county spiders."""
from .alachua_spider import AlachuaSpider
from .broward_spider import BrowardSpider
from . import broward_spider  # keeps tests that expect spiders_pkg.broward_spider working

SPIDERS = {
    "alachua": AlachuaSpider,
    "alachua_spider": AlachuaSpider,
    "broward": BrowardSpider,
    "broward_spider": BrowardSpider,
}
