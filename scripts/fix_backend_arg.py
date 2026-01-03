from pathlib import Path
p = Path.cwd()

# __main__.py edits
m = p/"src"/"florida_property_scraper"/"__main__.py"
s = m.read_text()
if "parser.add_argument('--backend'" not in s:
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
    print("patched __main__.py")
else:
    print("__main__.py already patched")

# scraper.py edits
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
    print("patched scraper.py")
else:
    print("scraper.py already patched")
