"""Spider registry for available county spiders."""
from .alachua_spider import AlachuaSpider
from .broward_spider import BrowardSpider
from .hillsborough_spider import HillsboroughSpider
from .miami_dade_spider import MiamiDadeSpider
from .orange_spider import OrangeSpider
from .palm_beach_spider import PalmBeachSpider
from .pinellas_spider import PinellasSpider
from .seminole_spider import SeminoleSpider
from . import broward_spider  # keeps tests that expect spiders_pkg.broward_spider working

SPIDERS = {
    "alachua": AlachuaSpider,
    "alachua_spider": AlachuaSpider,
    "broward": BrowardSpider,
    "broward_spider": BrowardSpider,
    "hillsborough": HillsboroughSpider,
    "hillsborough_spider": HillsboroughSpider,
    "miami_dade": MiamiDadeSpider,
    "miami_dade_spider": MiamiDadeSpider,
    "orange": OrangeSpider,
    "orange_spider": OrangeSpider,
    "palm_beach": PalmBeachSpider,
    "palm_beach_spider": PalmBeachSpider,
    "pinellas": PinellasSpider,
    "pinellas_spider": PinellasSpider,
    "seminole": SeminoleSpider,
    "seminole_spider": SeminoleSpider,
}
