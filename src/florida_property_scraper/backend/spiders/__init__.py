"""Spider registry for available county spiders."""
from .alachua_spider import AlachuaSpider
from .broward_spider import BrowardSpider

SPIDERS = {
    "alachua": AlachuaSpider,
    "broward": BrowardSpider,
}
