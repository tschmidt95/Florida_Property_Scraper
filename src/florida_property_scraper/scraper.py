from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from scrapy import signals
from scrapy.crawler import CrawlerProcess

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
        log_level: str = "INFO",
        obey_robots: bool = True,
        concurrent_requests: int = CONCURRENT_REQUESTS,
        download_timeout: int = DOWNLOAD_TIMEOUT,
    ):
        self.log_level = log_level
        self.obey_robots = obey_robots
        self.concurrent_requests = concurrent_requests
        self.download_timeout = download_timeout

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
            "LOG_LEVEL": self.log_level,
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
