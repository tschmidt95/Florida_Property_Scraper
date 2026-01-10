import florida_property_scraper.scraper as scraper_module
from florida_property_scraper.scraper import FloridaPropertyScraper


class _FlakyAdapter:
    def __init__(self):
        self.calls = 0

    def search(self, query, **kwargs):
        self.calls += 1
        if self.calls < 3:
            raise RuntimeError("fail")
        return []


def test_retry_backoff(monkeypatch):
    delays = []

    def fake_sleep(value):
        delays.append(value)

    monkeypatch.setattr(scraper_module.time, "sleep", fake_sleep)
    scraper = FloridaPropertyScraper(demo=False)
    scraper.adapter = _FlakyAdapter()
    scraper.search_all_counties("Smith", counties=["broward"])
    assert delays == [0.1, 0.2]
