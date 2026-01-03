"""Spider registry for available county spiders."""
from .alachua_spider import AlachuaSpider

SPIDERS = {
    "alachua": AlachuaSpider,
}
