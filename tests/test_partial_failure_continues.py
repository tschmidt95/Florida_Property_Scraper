from florida_property_scraper.scraper import FloridaPropertyScraper


class _AdapterStub:
    def __init__(self):
        self.calls = []

    def search(self, query, **kwargs):
        self.calls.append(kwargs)
        if kwargs.get("spider_name") == "broward_spider":
            raise RuntimeError("boom")
        return [
            {
                "county": "orange",
                "owner": "Owner",
                "address": "123 Main St",
            }
        ]


def test_partial_failure_continues():
    scraper = FloridaPropertyScraper(demo=False)
    stub = _AdapterStub()
    scraper.adapter = stub
    results = scraper.search_all_counties(
        "Smith",
        counties=["broward", "orange"],
    )
    assert len(results) == 1
    assert scraper.failures
    assert scraper.failures[0]["county"] == "broward"
