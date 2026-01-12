from __future__ import annotations

from florida_property_scraper.permits.base import PermitsScraper


def get_permits_scraper(county: str) -> PermitsScraper:
    key = (county or "").strip().lower()
    if key in {"seminole", "seminole_county"}:
        from florida_property_scraper.permits.seminole import SeminolePermitsScraper

        return SeminolePermitsScraper()

    raise KeyError(f"No permits scraper registered for county={county!r}")
