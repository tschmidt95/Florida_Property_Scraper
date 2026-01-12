"""Registry for county-specific permit scrapers."""

from typing import Dict, Optional

from florida_property_scraper.permits.base import PermitScraperBase


_REGISTRY: Dict[str, PermitScraperBase] = {}


def register_scraper(county: str, scraper: PermitScraperBase) -> None:
    """Register a permit scraper for a county.

    Args:
        county: County name (lowercase)
        scraper: Scraper instance
    """
    _REGISTRY[county.lower()] = scraper


def get_scraper(county: str) -> Optional[PermitScraperBase]:
    """Get a permit scraper for a county.

    Args:
        county: County name (case-insensitive)

    Returns:
        Scraper instance or None if not registered
    """
    return _REGISTRY.get(county.lower())


def list_counties() -> list:
    """List all registered counties."""
    return sorted(_REGISTRY.keys())
