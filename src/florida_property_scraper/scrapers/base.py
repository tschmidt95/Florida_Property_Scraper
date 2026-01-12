from __future__ import annotations

from typing import Protocol

from florida_property_scraper.leads_models import SearchResult


class CountyScraper(Protocol):
    county: str

    def search(self, query: str, limit: int) -> list[SearchResult]:
        raise NotImplementedError
