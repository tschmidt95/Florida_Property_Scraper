"""Spider registry for available county spiders."""

import importlib
from typing import Any

from .alachua_spider import AlachuaSpider
from .broward_spider import BrowardSpider
from .duval_spider import DuvalSpider
from .hillsborough_spider import HillsboroughSpider
from .miami_dade_spider import MiamiDadeSpider
from .orange_spider import OrangeSpider
from .palm_beach_spider import PalmBeachSpider
from .pinellas_spider import PinellasSpider
from .polk_spider import PolkSpider
from .seminole_spider import SeminoleSpider

SPIDERS = {
    "alachua": AlachuaSpider,
    "alachua_spider": AlachuaSpider,
    "broward": BrowardSpider,
    "broward_spider": BrowardSpider,
    "duval": DuvalSpider,
    "duval_spider": DuvalSpider,
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
    "polk": PolkSpider,
    "polk_spider": PolkSpider,
    "seminole": SeminoleSpider,
    "seminole_spider": SeminoleSpider,
}


_SUBMODULES = {
    "alachua_spider",
    "broward_spider",
    "duval_spider",
    "hillsborough_spider",
    "miami_dade_spider",
    "orange_spider",
    "palm_beach_spider",
    "pinellas_spider",
    "polk_spider",
    "seminole_spider",
}


def __getattr__(name: str) -> Any:
    """Lazy-load spider submodules for tests and backwards compatibility."""

    if name in _SUBMODULES:
        return importlib.import_module(f"{__name__}.{name}")
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + list(_SUBMODULES))
