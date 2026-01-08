"""Simple Scrapy adapter that runs the Scrapy runner subprocess.

This implementation prefers running a separate subprocess (via
`scrapy_runner`) to avoid Twisted reactor reuse issues when running
multiple crawls in the same process (e.g., during tests).
"""

from typing import List, Dict, Any, Optional
import json
import subprocess
import sys
import time


class InMemoryPipeline:
    """Pipeline used by the runner subprocess for collecting items in memory.

    The subprocess can set `InMemoryPipeline.items_list` prior to running the
    crawl. The pipeline will append dictified items to that list so the
    subprocess can emit the aggregated results as JSON to stdout.
    """

    items_list = None
    max_items = None

    @classmethod
    def from_crawler(cls, crawler):
        inst = cls()
        inst.items = cls.items_list if cls.items_list is not None else []
        inst.max_items = cls.max_items
        inst.crawler = crawler
        return inst

    def process_item(self, item, spider=None):
        # Keep collecting deterministic output even if the spider yields
        # more than `CLOSESPIDER_ITEMCOUNT` (e.g., multiple items from a
        # single response). Avoid calling deprecated Scrapy engine APIs.
        if self.max_items is not None and len(self.items) >= self.max_items:
            return item
        self.items.append(dict(item))
        return item


class ScrapyAdapter:
    def __init__(
        self,
        demo: bool = False,
        timeout: Optional[int] = None,
        live: bool = False,
    ):
        self.demo = demo
        self.timeout = timeout
        self.live = live

    def search(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """Run a search and return a list of result dicts.

        For demo mode this returns a deterministic fixture.
        For non-demo runs we invoke the runner subprocess which emits JSON
        to stdout.
        """
        from florida_property_scraper.schema import normalize_item

        if self.demo:
            items = [
                {
                    "county": "demo",
                    "address": "123 Demo St",
                    "owner": "Demo Owner",
                    "notes": "demo fixture",
                    "land_size": "",
                    "building_size": "",
                    "bedrooms": "",
                    "bathrooms": "",
                    "zoning": "",
                    "property_class": "",
                    "raw_html": "",
                }
            ]
            max_items = kwargs.get("max_items")
            if max_items:
                items = items[: int(max_items)]
            return [normalize_item(item) for item in items]
        start_urls = kwargs.get("start_urls")
        spider_name = kwargs.get("spider_name") or ""
        max_items = kwargs.get("max_items")
        debug_html = bool(kwargs.get("debug_html"))
        if not self.live:
            if not start_urls:
                return []
            if any(
                isinstance(u, str) and not u.startswith("file://")
                for u in start_urls
            ):
                return []
        else:
            from florida_property_scraper.routers.fl import build_start_urls
            from florida_property_scraper.routers.fl import get_entry as get_county_entry

            slug = (
                spider_name[: -len("_spider")]
                if spider_name.endswith("_spider")
                else spider_name
            )
            entry = get_county_entry(slug)
            if entry.get("spider_key"):
                spider_name = entry["spider_key"]
            if entry.get("query_param_style") == "form":
                kwargs["form_url"] = entry.get("form_url")
                kwargs["form_fields"] = entry.get("form_fields_template")
            kwargs["pagination"] = entry.get("pagination")
            kwargs["page_param"] = entry.get("page_param")
            if not start_urls:
                start_urls = build_start_urls(slug, query)
            if not start_urls:
                return []

        pagination = kwargs.get("pagination")
        page_param = kwargs.get("page_param")
        form_url = kwargs.get("form_url")
        form_fields = kwargs.get("form_fields")

        runner_cmd = [
            sys.executable,
            "-m",
            "florida_property_scraper.backend.scrapy_runner",
            "--spider-name",
            spider_name,
            "--start-urls",
            json.dumps(start_urls),
        ]
        runner_cmd.extend(["--query", query])
        if pagination:
            runner_cmd.extend(["--pagination", pagination])
        if page_param:
            runner_cmd.extend(["--page-param", page_param])
        if form_url:
            runner_cmd.extend(["--form-url", form_url])
        if form_fields:
            runner_cmd.extend(["--form-fields", json.dumps(form_fields)])
        if debug_html:
            runner_cmd.append("--debug-html")
        if max_items:
            runner_cmd.extend(["--max-items", str(int(max_items))])

        MAX_RETRIES = 3
        delay = 0.05
        last_stdout = ""
        last_stderr = ""
        proc = None

        for attempt in range(1, MAX_RETRIES + 1):
            proc = subprocess.run(
                runner_cmd, capture_output=True, text=True, shell=False
            )
            stdout = proc.stdout.strip()
            stderr = proc.stderr.strip()

            # Attempt to parse JSON output
            items = None
            try:
                if stdout:
                    items = json.loads(stdout)
            except Exception:
                items = None

            if isinstance(items, list):
                normalized = [normalize_item(item) for item in items]
                if max_items:
                    return normalized[: int(max_items)]
                return normalized

            # If the runner returned an error payload, stop retrying
            try:
                payload = json.loads(stdout) if stdout else None
                if isinstance(payload, dict) and payload.get("error"):
                    last_stdout = stdout
                    last_stderr = stderr
                    break
            except Exception:
                pass

            last_stdout = stdout
            last_stderr = stderr

            if attempt < MAX_RETRIES:
                time.sleep(delay)
                delay *= 2

        # Surface runner outputs to stderr for easier debugging in CI
        rc = proc.returncode if proc else "N/A"
        sys.stderr.write(f"Scrapy runner finished with returncode={rc}\n")
        sys.stderr.write("Runner STDOUT:\n")
        sys.stderr.write(f"{last_stdout}\n")
        if last_stderr:
            sys.stderr.write(f"Runner STDERR:\n{last_stderr}\n")

        return []
import os as _os

if _os.environ.get("FL_SCRAPER_BACKEND") == "native":
    from florida_property_scraper.backend.native_adapter import NativeAdapter as ScrapyAdapter
