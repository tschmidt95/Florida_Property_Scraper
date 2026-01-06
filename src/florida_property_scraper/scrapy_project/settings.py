BOT_NAME = "florida_property_scraper"

SPIDER_MODULES = ["florida_property_scraper.scrapy_project.spiders"]
NEWSPIDER_MODULE = "florida_property_scraper.scrapy_project.spiders"

ROBOTSTXT_OBEY = True

CONCURRENT_REQUESTS = 4
DOWNLOAD_TIMEOUT = 30
DOWNLOAD_DELAY = 0.5

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en",
}

USER_AGENT = "Mozilla/5.0 (compatible; FloridaPropertyScraper/1.0; +https://example.com)"

RETRY_ENABLED = True
RETRY_TIMES = 4
RETRY_HTTP_CODES = [429, 500, 502, 503, 504, 522, 524]

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 10.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

ITEM_PIPELINES = {
    "florida_property_scraper.scrapy_project.pipelines.NormalizePipeline": 100,
}
