from florida_property_scraper.scraper import FloridaPropertyScraper


class _AdapterStub:
    def __init__(self):
        self.calls = []

    def search(self, query, **kwargs):
        self.calls.append(kwargs["spider_name"])
        if kwargs["spider_name"] == "broward_spider":
            raise RuntimeError("crash")
        return [{"county": "orange", "owner": "Owner", "address": "123 Main St"}]


def test_one_county_crash_does_not_stop_others():
    scraper = FloridaPropertyScraper(demo=False)
    scraper.adapter = _AdapterStub()
    results = scraper.search_all_counties("Smith", counties=["broward", "orange"])
    assert results
    assert scraper.failures
