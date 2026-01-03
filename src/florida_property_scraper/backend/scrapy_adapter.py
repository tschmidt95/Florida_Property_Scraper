"""Simple Scrapy adapter with a minimal spider runner.
This is a lightweight implementation that runs Scrapy spiders in a subprocess
so the main process can call multiple spiders without Twisted reactor conflicts.

In demo mode the adapter returns a deterministic fixture.
"""

from typing import List, Dict, Any, Optional
import json
import subprocess
import sys


class InMemoryPipeline:
    """
    Pipeline used for subprocess runs. The runner subprocess sets `items_list` and
    prints the collected items as JSON to stdout.
    """
    items_list = None

    @classmethod
    def from_crawler(cls, crawler):
        inst = cls()
        inst.items = cls.items_list if cls.items_list is not None else []
        return inst

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item


class ScrapyAdapter:
    def __init__(self, demo: bool = False, timeout: Optional[int] = None):
        self.demo = demo
        self.timeout = timeout

    def search(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """Run a search for `query` and return a list of result dicts.

        For demo mode this returns a deterministic fixture. Non-demo runs a
        helper subprocess module that executes the Scrapy crawl and emits
        a JSON array of items to stdout. This avoids Twisted reactor reuse
        issues when multiple tests call the adapter.
        """
        if self.demo:
            return [{"address": "123 Demo St", "owner": "Demo Owner", "notes": "demo fixture"}]

        start_urls = kwargs.get("start_urls")
        spider_name = kwargs.get("spider_name")
        if not start_urls:
            return []

        runner_cmd = [
            sys.executable,
            "-m",
            "florida_property_scraper.backend.scrapy_runner",
            "--spider-name",
            spider_name,
            "--start-urls",
            json.dumps(start_urls),
        ]

        try:
            MAX_RETRIES = 3
            delay = 0.05
            proc = None
            items = None

            def _parse_stdout(s):
                if not s:
                    return None
                try:
                    return json.loads(s)
                except Exception:
                    return None

            import sys as _sys
            # Log runner invocation for debugging (flush immediately so pytest captures it)
            print(f"Adapter invoking runner_cmd={runner_cmd}", file=_sys.stderr, flush=True)

            for attempt in range(1, MAX_RETRIES + 1):
                proc = subprocess.run(runner_cmd, capture_output=True, text=True, check=False)
                stdout = proc.stdout.strip()
                stderr = proc.stderr.strip()
                print(f"Adapter runner attempt={attempt} returncode={proc.returncode}", file=_sys.stderr, flush=True)
                print(f"Adapter runner stdout snippet={stdout[:300]}", file=_sys.stderr, flush=True)
                items = _parse_stdout(stdout)
                if isinstance(items, list) and len(items) > 0:
                    return items
                # if runner printed explicit error payload, break early
                try:
                    payload = json.loads(stdout) if stdout else None
                    if isinstance(payload, dict) and payload.get("error"):
                        break
                except Exception:
                    pass
                # small backoff before retrying
                if attempt < MAX_RETRIES:
                    import time
                    time.sleep(delay)
                    delay *= 2

            # If we get here, items is either None, empty list, or there was an error
            # Surface runner stdout/stderr to stderr to aid diagnosis in CI/test logs
            _sys.stderr.write(
                f"Scrapy runner finished with returncode={proc.returncode if proc else 'N/A'}\n"
            )
            _sys.stderr.write(f"Runner STDOUT:\n{stdout}\n")
            if stderr:
                _sys.stderr.write(f"Runner STDERR:\n{stderr}\n")

            if isinstance(items, list):
                return items
            return []
        except Exception:
            return []


class InMemoryPipeline:
    """
    Pipeline used for in-process test/demo runs. Adapter sets `items_list` before crawl.
    """
    items_list = None

    @classmethod
    def from_crawler(cls, crawler):
        inst = cls()
        inst.items = cls.items_list if cls.items_list is not None else []
        return inst

    def process_item(self, item, spider):
        # store a plain dict to make assertions easy
        self.items.append(dict(item))
        return item


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
        spider_name = kwargs.get("spider_name")
        if not start_urls:
            # Nothing to crawl yet
            return []

        # Use an in-memory pipeline to collect items reliably
        items = []



        try:
            # Use a CrawlerProcess with the in-memory pipeline configured
            
            InMemoryPipeline.items_list = items
            settings = {
                'ITEM_PIPELINES': {
                    'florida_property_scraper.backend.scrapy_adapter.InMemoryPipeline': 100,
                }
            }
            process = CrawlerProcess(settings=settings)
        

            # Determine spider class: if spider_name provided, import from spiders package
            if spider_name:
                try:
                    from .spiders import SPIDERS
                    SpiderCls = SPIDERS.get(spider_name)
                except Exception:
                    SpiderCls = None

                if not SpiderCls:
                    try:
                        module_name = f"florida_property_scraper.backend.spiders.{spider_name}_spider"
                        module = __import__(module_name, fromlist=['*'])
                        class_name = ''.join(p.capitalize() for p in spider_name.split('_')) + 'Spider'
                        SpiderCls = getattr(module, class_name)
                    except Exception:
                        SpiderCls = GenericSpider
            else:
                SpiderCls = GenericSpider

            process.crawl(SpiderCls, start_urls=start_urls)
            process.start()  # blocking
        except Exception:  # pragma: no cover - errors when Scrapy isn't available or spider fails
            return []

        return items
