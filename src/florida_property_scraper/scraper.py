import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import uuid4

from scrapy import signals
from scrapy.crawler import CrawlerProcess

from florida_property_scraper.backend.scrapy_adapter import ScrapyAdapter
from florida_property_scraper.county_router import (
    build_start_urls,
    canonicalize_county_name,
    enabled_counties,
    get_county_entry,
)
from florida_property_scraper.identity import compute_property_uid
from florida_property_scraper.run_result import RunResult
from florida_property_scraper.scrapy_project.settings import (
    BOT_NAME,
    CONCURRENT_REQUESTS,
    DEFAULT_REQUEST_HEADERS,
    DOWNLOAD_TIMEOUT,
    ITEM_PIPELINES,
    ROBOTSTXT_OBEY,
    SPIDER_MODULES,
    USER_AGENT,
)
from florida_property_scraper.scrapy_project.spiders.county_spider import CountySpider
from florida_property_scraper.storage import SQLiteStore


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
        obey_robots: bool = ROBOTSTXT_OBEY,
        concurrent_requests: int = CONCURRENT_REQUESTS,
        download_timeout: int = DOWNLOAD_TIMEOUT,
    ):
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
        self.obey_robots = obey_robots
        self.concurrent_requests = concurrent_requests
        self.download_timeout = download_timeout

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

    def search(
        self,
        query: str,
        counties: Optional[List[str]] = None,
        output_path: Optional[str] = None,
        output_format: str = "jsonl",
        append_output: bool = True,
        max_items: Optional[int] = None,
        allow_forms: bool = True,
        storage_path: Optional[str] = None,
        webhook_url: Optional[str] = None,
        zoho_sync: bool = False,
    ) -> RunResult:
        run_id = uuid4().hex
        started_at = datetime.now(timezone.utc).isoformat()
        items: List[dict] = []
        errors: List[str] = []
        warnings: List[str] = []
        normalized_counties = None
        if counties:
            normalized_counties = [c for c in counties if c]
        if (webhook_url or zoho_sync) and not output_path:
            warnings.append("Webhook/Zoho sync disabled because output_path is not set.")
        store = None
        if storage_path:
            store = SQLiteStore(storage_path)
            store.record_run_start(
                run_id=run_id,
                started_at=started_at,
                run_type="manual",
                counties=normalized_counties,
                query=query,
            )
        settings = {
            "BOT_NAME": BOT_NAME,
            "SPIDER_MODULES": SPIDER_MODULES,
            "ROBOTSTXT_OBEY": self.obey_robots,
            "CONCURRENT_REQUESTS": self.concurrent_requests,
            "DOWNLOAD_TIMEOUT": self.download_timeout,
            "DEFAULT_REQUEST_HEADERS": DEFAULT_REQUEST_HEADERS,
            "USER_AGENT": USER_AGENT,
            "ITEM_PIPELINES": dict(ITEM_PIPELINES),
            "LOG_LEVEL": self.log_level or "INFO",
            "RUN_ID": run_id,
        }
        if output_path and output_format == "jsonl" and append_output:
            settings["ITEM_PIPELINES"][
                "florida_property_scraper.scrapy_project.pipelines.AppendJsonlPipeline"
            ] = 800
        if storage_path:
            settings["ITEM_PIPELINES"][
                "florida_property_scraper.scrapy_project.pipelines.StoragePipeline"
            ] = 850
            settings["STORAGE_PATH"] = storage_path
        if output_path and (webhook_url or zoho_sync):
            settings["ITEM_PIPELINES"][
                "florida_property_scraper.scrapy_project.pipelines.ExporterPipeline"
            ] = 875
            if webhook_url:
                settings["WEBHOOK_URL"] = webhook_url
            if zoho_sync:
                settings["ZOHO_SYNC"] = True
        if output_path:
            settings["OUTPUT_PATH"] = output_path
            settings["OUTPUT_FORMAT"] = output_format
            settings["APPEND_OUTPUT"] = append_output
            if not (append_output and output_format == "jsonl"):
                settings["FEEDS"] = {output_path: {"format": output_format}}
        process = CrawlerProcess(settings)

        def _on_item_scraped(item, response, spider):
            items.append(dict(item))
            _, _, uid_warnings = compute_property_uid(item)
            warnings.extend(uid_warnings)

        def _on_spider_error(failure, response, spider):
            errors.append(str(failure))

        crawler = process.create_crawler(CountySpider)
        crawler.signals.connect(_on_item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(_on_spider_error, signal=signals.spider_error)
        process.crawl(
            crawler,
            query=query,
            counties=normalized_counties,
            max_items=max_items,
            allow_forms=allow_forms,
        )
        finished_at = started_at
        try:
            process.start()
        except Exception as exc:
            errors.append(str(exc))
            raise
        finally:
            finished_at = datetime.now(timezone.utc).isoformat()
            status = "failed" if errors else "succeeded"
            if store:
                store.record_run_finish(
                    run_id=run_id,
                    finished_at=finished_at,
                    status=status,
                    items_count=len(items),
                    warnings=warnings,
                    errors=errors,
                )
                store.close()
        return RunResult(
            run_id=run_id,
            items=items,
            items_count=len(items),
            started_at=started_at,
            finished_at=finished_at,
            output_path=output_path,
            output_format=output_format if output_path else None,
            storage_path=storage_path,
            counties=normalized_counties,
            query=query,
            errors=errors,
            warnings=warnings,
        )
