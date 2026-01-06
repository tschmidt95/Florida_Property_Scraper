from typing import List, Optional

from scrapy.crawler import CrawlerProcess

from florida_property_scraper.scrapy_project.pipelines import set_global_collector
from florida_property_scraper.scrapy_project.settings import (
    BOT_NAME,
    CONCURRENT_REQUESTS,
    DEFAULT_REQUEST_HEADERS,
    DOWNLOAD_TIMEOUT,
    ITEM_PIPELINES,
    ROBOTSTXT_OBEY,
    SPIDER_MODULES,
)
from florida_property_scraper.scrapy_project.spiders.county_spider import CountySpider


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
        counties: Optional[str] = None,
        output_path: Optional[str] = None,
        output_format: str = "jsonl",
        append_output: bool = True,
        max_items: Optional[int] = None,
        allow_forms: bool = True,
        storage_path: Optional[str] = None,
        webhook_url: Optional[str] = None,
        zoho_sync: bool = False,
    ) -> List[dict]:
        collector: List[dict] = []
        set_global_collector(collector)
        settings = {
            "BOT_NAME": BOT_NAME,
            "SPIDER_MODULES": SPIDER_MODULES,
            "ROBOTSTXT_OBEY": self.obey_robots,
            "CONCURRENT_REQUESTS": self.concurrent_requests,
            "DOWNLOAD_TIMEOUT": self.download_timeout,
            "DEFAULT_REQUEST_HEADERS": DEFAULT_REQUEST_HEADERS,
            "ITEM_PIPELINES": {
                **ITEM_PIPELINES,
                "florida_property_scraper.scrapy_project.pipelines.AppendJsonlPipeline": 800,
                "florida_property_scraper.scrapy_project.pipelines.StoragePipeline": 850,
                "florida_property_scraper.scrapy_project.pipelines.ExporterPipeline": 875,
                "florida_property_scraper.scrapy_project.pipelines.CollectorPipeline": 900,
            },
            "ITEM_COLLECTOR": collector,
            "LOG_LEVEL": self.log_level,
        }
        if storage_path:
            settings["STORAGE_PATH"] = storage_path
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
        process.crawl(
            CountySpider,
            query=query,
            counties=counties,
            max_items=max_items,
            allow_forms=allow_forms,
        )
        process.start()
        return collector
