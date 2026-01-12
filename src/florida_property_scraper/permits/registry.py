"""Registry for county-specific permit scrapers."""
from typing import Dict, Type

from florida_property_scraper.permits.base import PermitScraperBase


_REGISTRY: Dict[str, Type[PermitScraperBase]] = {}


def register_scraper(county: str, scraper_class: Type[PermitScraperBase]) -> None:
    """Register a permit scraper for a county."""
    _REGISTRY[county.lower()] = scraper_class


def get_scraper(county: str) -> PermitScraperBase:
    """Get a permit scraper instance for a county."""
    scraper_class = _REGISTRY.get(county.lower())
    if not scraper_class:
        raise ValueError(f"No permit scraper registered for county: {county}")
    return scraper_class()


def list_counties() -> list[str]:
    """List all counties with registered permit scrapers."""
    return list(_REGISTRY.keys())


# Import and register scrapers
try:
    from florida_property_scraper.permits.seminole import SeminolePermitScraper

    register_scraper("seminole", SeminolePermitScraper)
except ImportError:
    pass
