"""Simple Scrapy adapter with a minimal spider runner.
This is a lightweight implementation that allows running a small Scrapy spider
synchronously via CrawlerProcess and collecting items.

Note: For now the adapter returns canned demo results when `demo=True`.
The non-demo path runs a minimal spider that yields any items parsed by
`parse` (the caller can extend it to use real spiders per-county).
"""

from typing import List, Dict, Any, Optional


class ScrapyAdapter:
    def __init__(self, demo: bool = False, timeout: Optional[int] = None):
        self.demo = demo
        self.timeout = timeout

    def search(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """Run a search for `query` and return a list of result dicts.

        For demo mode this returns a deterministic fixture. For non-demo,
        this method runs a minimal Scrapy spider and returns any items it
        yields. The spider is intentionally simple — it's a placeholder
        that can be replaced with more specific per-county spiders.
        """
        if self.demo:
            return [{"address": "123 Demo St", "owner": "Demo Owner", "notes": "demo fixture"}]

        # Lazy import Scrapy only when needed (keeps tests fast for demo mode)
        try:
            from scrapy.crawler import CrawlerProcess
            from scrapy.spiders import Spider
            from scrapy import Item, Field
        except Exception:  # pragma: no cover - only relevant when running non-demo
            return []

        # Create a dynamic Item class to collect arbitrary fields
        class ResultItem(Item):
            address = Field()
            owner = Field()
            notes = Field()

        results: List[Dict[str, Any]] = []

        # Define a minimal spider that can be parameterized via kwargs
        class GenericSpider(Spider):
            name = "generic_spider"

            def __init__(self, start_urls=None, *a, **kw):
                super().__init__(*a, **kw)
                self.start_urls = start_urls or []

            def parse(self, response):
                # Placeholder parsing: real implementation should extract structured data
                # Here we yield nothing (or could yield a synthetic result if desired)
                return

        # Build start_urls from kwargs if provided (adapter will be extended later)
        start_urls = kwargs.get("start_urls")
        if not start_urls:
            # Nothing to crawl yet
            return []

        process = CrawlerProcess(settings={})
        process.crawl(GenericSpider, start_urls=start_urls)
        process.start()  # blocking

        # Note: extracting items from a CrawlerProcess in-proc requires item pipelines
        # or signals to capture scraped items; for now the spider is a placeholder.
        return results
