from florida_property_scraper.backend.scrapy_adapter import ScrapyAdapter
from florida_property_scraper.schema import REQUIRED_FIELDS


def test_schema_fields_present_in_results():
    adapter = ScrapyAdapter(demo=True)
    results = adapter.search(
        "unused",
        start_urls=["file://demo"],
        spider_name="broward_spider",
    )
    assert results
    for item in results:
        for field in REQUIRED_FIELDS:
            assert field in item
