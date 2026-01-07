from florida_property_scraper.scraper import FloridaPropertyScraper


class _AdapterStub:
    def __init__(self):
        self.calls = []

    def search(self, query, **kwargs):
        self.calls.append((query, kwargs))
        return []


def test_scraper_uses_router_for_start_urls_and_spiders():
    scraper = FloridaPropertyScraper(demo=False)
    stub = _AdapterStub()
    scraper.adapter = stub

    query = "John Smith"
    scraper.search_all_counties(query, counties=["broward", "orange"], max_items=3)

    assert len(stub.calls) == 2
    for called_query, kwargs in stub.calls:
        assert called_query == query
        assert kwargs.get("start_urls")
        assert kwargs.get("spider_name") in {"broward_spider", "orange_spider"}
        assert kwargs.get("max_items") == 3
