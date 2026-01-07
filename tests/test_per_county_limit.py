from florida_property_scraper.scraper import FloridaPropertyScraper


class _AdapterStub:
    def __init__(self):
        self.max_items = None

    def search(self, query, **kwargs):
        self.max_items = kwargs.get("max_items")
        return []


def test_per_county_limit_passed_to_adapter():
    scraper = FloridaPropertyScraper(demo=False, per_county_limit=1)
    stub = _AdapterStub()
    scraper.adapter = stub
    scraper.search_all_counties("Smith", counties=["broward"])
    assert stub.max_items == 1
