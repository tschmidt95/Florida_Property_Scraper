from __future__ import annotations

from florida_property_scraper.scrapers.base import CountyScraper
from florida_property_scraper.scrapers.seminole import SeminoleScraper


def _norm(name: str) -> str:
    return (name or "").strip().lower().replace("_", " ")


_REGISTRY: dict[str, CountyScraper] = {
    _norm("Seminole"): SeminoleScraper(),
}


def get_scraper(county: str) -> CountyScraper | None:
    return _REGISTRY.get(_norm(county))


def supported_counties() -> list[str]:
    # Human-friendly names.
    return sorted({s.county for s in _REGISTRY.values()})
