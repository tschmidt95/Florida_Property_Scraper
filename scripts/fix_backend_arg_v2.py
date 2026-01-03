#!/usr/bin/env python3
from pathlib import Path
import re
p = Path.cwd()

def patch_main():
    f = p/"src"/"florida_property_scraper"/"__main__.py"
    s = f.read_text()
    changed = False

    # add --backend arg after the demo arg
    if "parser.add_argument('--backend'" not in s:
        s, n = re.subn(
            r"(parser\.add_argument\('--demo'.*?\)\s*)",
            r"\1    parser.add_argument('--backend', choices=['scrapy','scrapingbee'], default='scrapy',\n                        help='Which scraping backend to use (default: scrapy)')\n",
            s, flags=re.S
        )
        if n:
            changed = True

    # replace api_key retrieval with conditional based on args.backend
    if "api_key = os.environ.get('SCRAPINGBEE_API_KEY')" in s and "if args.backend == 'scrapingbee':" not in s:
        s = s.replace(
            "api_key = os.environ.get('SCRAPINGBEE_API_KEY')",
            "if args.backend == 'scrapingbee':\n        api_key = os.environ.get('SCRAPINGBEE_API_KEY')\n    else:\n        api_key = None"
        )
        changed = True

    # ensure backend is passed into FloridaPropertyScraper(...) call
    if "FloridaPropertyScraper(" in s and "backend=args.backend" not in s:
        s = s.replace(
            "FloridaPropertyScraper(",
            "FloridaPropertyScraper("
        )
        # add backend in the specific call arguments if we can find the demo arg
        s = s.replace(
            "demo=args.demo)",
            "demo=args.demo, backend=args.backend)"
        )
        changed = True

    if changed:
        f.write_text(s)
        print("patched __main__.py")
    else:
        print("__main__.py already patched or no pattern matched")

def patch_scraper():
    f = p/"src"/"florida_property_scraper"/"scraper.py"
    s = f.read_text()
    changed = False

    # add backend parameter to __init__ signature
    s, n = re.subn(
        r"def __init__\(([^)]*?)demo\s*=\s*False\s*\):",
        r"def __init__(\1demo=False, backend='scrapy'):",
        s
    )
    if n:
        changed = True

    # Replace unconditional ScrapingBee key check with conditional backend check & adapter init
    if "if not scrapingbee_api_key:" in s or "SCRAPINGBEE_API_KEY is not set" in s:
        s = re.sub(
            r"(\s*)if not scrapingbee_api_key:\s*\n\s*raise ValueError\([^\)]*\)",
            (
                r"\1self.backend = backend\n\n"
                r"\1# Only require ScrapingBee key when using that backend (and not demo)\n"
                r"\1if self.backend == 'scrapingbee':\n"
                r"\1    if not scrapingbee_api_key and not demo:\n"
                r"\1        raise ValueError(\n"
                r"\1            \"SCRAPINGBEE_API_KEY is not set; provide scrapingbee_api_key or set the SCRAPINGBEE_API_KEY env var, \"\n"
                r"\1            \"or run with --demo to use canned demo data\"\n"
                r"\1        )\n"
                r"\1    self.scrapingbee_api_key = scrapingbee_api_key\n"
                r"\1else:\n"
                r"\1    self.scrapingbee_api_key = None\n\n"
                r"\1# Initialize backend adapter (scrapy by default)\n"
                r"\1if self.backend == 'scrapy':\n"
                r"\1    from .backend.scrapy_adapter import ScrapyAdapter\n\n"
                r"\1    self.adapter = ScrapyAdapter(demo=demo, timeout=timeout)\n"
                r"\1else:\n"
                r"\1    from .backend.scrapingbee_adapter import ScrapingBeeAdapter\n\n"
                r"\1    self.adapter = ScrapingBeeAdapter(api_key=self.scrapingbee_api_key, demo=demo, timeout=timeout)"
            ),
            s
        )
        changed = True

    if changed:
        f.write_text(s)
        print("patched scraper.py")
    else:
        print("scraper.py already patched or no pattern matched")

if __name__ == '__main__':
    patch_main()
    patch_scraper()
