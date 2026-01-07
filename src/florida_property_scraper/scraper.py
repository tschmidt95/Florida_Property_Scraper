from typing import Dict, List, Optional
import time

from florida_property_scraper.backend.scrapy_adapter import ScrapyAdapter
from florida_property_scraper.county_router import (
    build_start_urls,
    canonicalize_county_name,
    enabled_counties,
    get_county_entry,
)


class FloridaPropertyScraper:
    def __init__(
        self,
        timeout: int = 10,
        stop_after_first: bool = True,
        log_level: Optional[str] = None,
        demo: bool = False,
        counties: Optional[List[str]] = None,
        max_items: Optional[int] = None,
        live: bool = False,
        debug_html: bool = False,
        per_county_limit: Optional[int] = None,
        delay_ms: Optional[int] = None,
    ):
        """Create a scraper using the Scrapy backend only."""
        self.timeout = timeout
        self.stop_after_first = stop_after_first
        self.log_level = log_level
        self.demo = demo
        self.live = live
        self.debug_html = debug_html
        self.counties_filter = counties
        self.max_items = max_items
        self.per_county_limit = per_county_limit
        self.delay_ms = delay_ms
        self.failures: List[Dict] = []
        self.last_log_entries: List[Dict] = []
        self.adapter = ScrapyAdapter(demo=demo, timeout=timeout, live=live)

    def search_all_counties(
        self,
        query: str,
        stop_after_first: Optional[bool] = None,
        counties: Optional[List[str]] = None,
        max_items: Optional[int] = None,
        per_county_limit: Optional[int] = None,
        delay_ms: Optional[int] = None,
    ) -> List[Dict]:
        self.failures = []
        if stop_after_first is None:
            stop_after_first = self.stop_after_first
        if counties is None:
            counties = self.counties_filter
        if max_items is None:
            max_items = self.max_items
        if per_county_limit is None:
            per_county_limit = self.per_county_limit
        if delay_ms is None:
            delay_ms = self.delay_ms
        county_limit = per_county_limit if per_county_limit is not None else max_items
        all_results: List[Dict] = []
        self.last_log_entries = []
        if self.demo:
            demo_results = self.adapter.search(
                query,
                start_urls=["file://demo"],
                spider_name="broward_spider",
                max_items=max_items,
            )
            self.last_log_entries.append(
                {
                    "county": "broward",
                    "spider": "broward_spider",
                    "start_urls": ["file://demo"],
                    "items_found": len(demo_results),
                    "status": "success",
                }
            )
            return demo_results
        if counties:
            slugs = [canonicalize_county_name(c) for c in counties if c.strip()]
        else:
            slugs = enabled_counties()
        for idx, slug in enumerate(slugs):
            start_urls = build_start_urls(slug, query)
            if not start_urls:
                self.failures.append(
                    {
                        "county": slug,
                        "query": query,
                        "error": "No start url configured",
                    }
                )
                self.last_log_entries.append(
                    {
                        "county": slug,
                        "spider": get_county_entry(slug).get("spider_key"),
                        "start_urls": [],
                        "items_found": 0,
                        "status": "skipped",
                        "error": "No start url configured",
                    }
                )
                continue
            entry = get_county_entry(slug)
            attempts = 0
            last_error = ""
            results = []
            while attempts < 3:
                attempts += 1
                try:
                    results = self.adapter.search(
                        query,
                        start_urls=start_urls,
                        spider_name=entry.get("spider_key", f"{slug}_spider"),
                        max_items=county_limit,
                        debug_html=self.debug_html,
                    )
                    break
                except Exception as exc:
                    last_error = str(exc)
                    if attempts < 3:
                        time.sleep(0.1 * (2 ** (attempts - 1)))
            if last_error and not results:
                self.failures.append(
                    {
                        "county": slug,
                        "query": query,
                        "error": last_error,
                    }
                )
                self.last_log_entries.append(
                    {
                        "county": slug,
                        "spider": entry.get("spider_key", f"{slug}_spider"),
                        "start_urls": start_urls,
                        "items_found": 0,
                        "status": "failed",
                        "error": last_error,
                    }
                )
                continue
            self.last_log_entries.append(
                {
                    "county": slug,
                    "spider": entry.get("spider_key", f"{slug}_spider"),
                    "start_urls": start_urls,
                    "items_found": len(results),
                    "status": "success",
                }
            )
            if results:
                all_results.extend(results)
                if stop_after_first:
                    break
            if delay_ms and idx < len(slugs) - 1:
                time.sleep(delay_ms / 1000.0)
        return all_results
