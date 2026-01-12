from __future__ import annotations

import os

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


def _maybe_register_fixture_scraper() -> None:
    fixture_path = (os.getenv("PERMITS_FIXTURE_PATH") or "").strip()
    if not fixture_path:
        return
    county = (os.getenv("PERMITS_FIXTURE_COUNTY") or "Seminole").strip() or "Seminole"

    try:
        from florida_property_scraper.scrapers.permits_fixture import FixturePermitsScraper

        register_permits_scraper(
            county,
            FixturePermitsScraper(county=county, fixture_path=fixture_path),
        )
    except Exception:
        # Optional/disabled by default.
        return


_maybe_register_fixture_scraper()
