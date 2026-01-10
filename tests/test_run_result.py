import scrapy

from florida_property_scraper.scraper import FloridaPropertyScraper


class DummySpider(scrapy.Spider):
    name = "dummy"

    def __init__(
        self, query: str, counties=None, max_items=None, allow_forms=True, **kwargs
    ):
        super().__init__(**kwargs)
        self.query = query

    async def start(self):
        yield scrapy.Request("data:text/plain,ok", callback=self.parse)

    def parse(self, response):
        yield {
            "county": "Test",
            "search_query": self.query,
            "owner_name": "Dummy Owner",
        }


def test_run_result_fields(monkeypatch):
    import florida_property_scraper.scraper as scraper_module

    monkeypatch.setattr(scraper_module, "CountySpider", DummySpider)
    scraper = FloridaPropertyScraper(log_level="ERROR", obey_robots=True)
    result = scraper.search(
        query="SMITH",
        counties=["Test"],
        max_items=1,
        output_path=None,
        storage_path=None,
    )

    assert result.run_id
    assert result.started_at
    assert result.finished_at
    assert result.items_count == len(result.items)
    assert result.items_count == 1
    assert isinstance(result.items, list)
