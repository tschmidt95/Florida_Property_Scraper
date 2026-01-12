from __future__ import annotations

from florida_property_scraper.scrapers.permits_base import PermitsScraper


_REGISTRY: dict[str, PermitsScraper] = {}


def register_permits_scraper(county: str, scraper: PermitsScraper) -> None:
    key = (county or "").strip().lower()
    if not key:
        raise ValueError("county is required")
    _REGISTRY[key] = scraper


def get_permits_scraper(county: str) -> PermitsScraper | None:
    key = (county or "").strip().lower()
    if not key:
        return None
    return _REGISTRY.get(key)
