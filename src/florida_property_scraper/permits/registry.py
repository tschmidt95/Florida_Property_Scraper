"""Registry mapping counties to permit scrapers."""

from typing import Optional

from florida_property_scraper.permits.base import PermitsScraper


def get_permits_scraper(county: str) -> Optional[PermitsScraper]:
    """Get a permits scraper for the specified county.

    Args:
        county: County name (case-insensitive)

    Returns:
        PermitsScraper instance or None if county not supported
    """
    county_lower = county.lower().strip()

    if county_lower == "seminole":
        from florida_property_scraper.permits.seminole import SeminolePermitsScraper

        return SeminolePermitsScraper()

    return None
