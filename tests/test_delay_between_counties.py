import florida_property_scraper.scraper as scraper_module
from florida_property_scraper.scraper import FloridaPropertyScraper


class _AdapterStub:
    def search(self, query, **kwargs):
        return []


def test_delay_between_counties(monkeypatch):
    delays = []

    def fake_sleep(value):
        delays.append(value)

    monkeypatch.setattr(scraper_module.time, "sleep", fake_sleep)
    scraper = FloridaPropertyScraper(demo=False, delay_ms=100)
    scraper.adapter = _AdapterStub()
    scraper.search_all_counties("Smith", counties=["broward", "orange", "seminole"])
    assert delays == [0.1, 0.1]
