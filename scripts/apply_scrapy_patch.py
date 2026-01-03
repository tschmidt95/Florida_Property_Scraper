from pathlib import Path
p=Path.cwd()
# __main__.py
m = p/"src"/"florida_property_scraper"/"__main__.py"
s = m.read_text()
if "--backend" not in s:
    s = s.replace(
        "parser.add_argument('--demo', action='store_true', help='Run demo (no network)')",
        "parser.add_argument('--demo', action='store_true', help='Run demo (no network)')\n"
        "    parser.add_argument('--backend', choices=['scrapy','scrapingbee'], default='scrapy',\n"
        "                        help='Which scraping backend to use (default: scrapy)')\n"
    )
    s = s.replace(
        "api_key = os.environ.get('SCRAPINGBEE_API_KEY')",
        "if args.backend == 'scrapingbee':\n"
        "        api_key = os.environ.get('SCRAPINGBEE_API_KEY')\n"
        "    else:\n"
        "        api_key = None"
    )
    s = s.replace(
        "scraper = FloridaPropertyScraper(scrapingbee_api_key=api_key, timeout=args.timeout, stop_after_first=args.stop_after_first, log_level=args.log_level, demo=args.demo)",
        "scraper = FloridaPropertyScraper(scrapingbee_api_key=api_key, timeout=args.timeout, stop_after_first=args.stop_after_first, log_level=args.log_level, demo=args.demo, backend=args.backend)"
    )
    m.write_text(s)
# scraper.py
m2 = p/"src"/"florida_property_scraper"/"scraper.py"
s2 = m2.read_text()
if "backend='scrapy'" not in s2:
    s2 = s2.replace(
        "def __init__(self, scrapingbee_api_key=None, timeout=30, stop_after_first=False, log_level='INFO', demo=False):",
        "def __init__(self, scrapingbee_api_key=None, timeout=30, stop_after_first=False, log_level='INFO', demo=False, backend='scrapy'):"
    )
    s2 = s2.replace(
        "        if not scrapingbee_api_key:\n            raise ValueError(\"SCRAPINGBEE_API_KEY is not set; provide scrapingbee_api_key or set the SCRAPINGBEE_API_KEY env var\")",
        "        self.backend = backend\n\n        # Only require ScrapingBee key when using that backend (and not demo)\n        if self.backend == 'scrapingbee':\n            if not scrapingbee_api_key and not demo:\n                raise ValueError(\n                    \"SCRAPINGBEE_API_KEY is not set; provide scrapingbee_api_key or set the SCRAPINGBEE_API_KEY env var, \"\n                    \"or run with --demo to use canned demo data\"\n                )\n            self.scrapingbee_api_key = scrapingbee_api_key\n        else:\n            self.scrapingbee_api_key = None\n\n        # Initialize backend adapter (scrapy by default)\n        if self.backend == 'scrapy':\n            from .backend.scrapy_adapter import ScrapyAdapter\n\n            self.adapter = ScrapyAdapter(demo=demo, timeout=timeout)\n        else:\n            from .backend.scrapingbee_adapter import ScrapingBeeAdapter\n\n            self.adapter = ScrapingBeeAdapter(api_key=self.scrapingbee_api_key, demo=demo, timeout=timeout)"
    )
    m2.write_text(s2)
# scaffold adapter
bd = p/"src"/"florida_property_scraper"/"backend"
bd.mkdir(parents=True, exist_ok=True)
adapter = bd/"scrapy_adapter.py"
adapter.write_text(
"""# Simple Scrapy adapter scaffold (demo fixture)
class ScrapyAdapter:
    def __init__(self, demo=False, timeout=None):
        self.demo = demo
        self.timeout = timeout

    def search(self, query, **kwargs):
        if self.demo:
            return [{\"address\": \"123 Demo St\", \"owner\": \"Demo Owner\", \"notes\": \"demo fixture\"}]
        # TODO: implement spider runner (CrawlerRunner/CrawlerProcess)
        return []\n""")
# add tests
tests = p/"tests"
tests.mkdir(exist_ok=True)
tfile = tests/"test_backends.py"
tfile.write_text(
"""from florida_property_scraper.scraper import FloridaPropertyScraper
import pytest

def test_demo_mode_allows_no_key_scrapy():
    s = FloridaPropertyScraper(scrapingbee_api_key=None, demo=True, backend='scrapy')
    assert s.adapter is not None
    assert s.adapter.search('anything')

def test_scrapingbee_requires_key():
    with pytest.raises(ValueError):
        FloridaPropertyScraper(scrapingbee_api_key=None, demo=False, backend='scrapingbee')\n""")
print('Patched: __main__.py, scraper.py; added scrapy_adapter.py and tests/test_backends.py')
