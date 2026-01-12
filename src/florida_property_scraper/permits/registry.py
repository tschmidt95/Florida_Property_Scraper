"""Permits scraper registry."""
from typing import Optional, Protocol

from florida_property_scraper.permits.models import PermitRecord


class PermitsScraper(Protocol):
    """Protocol for permits scrapers."""

    def search_permits(self, query: str, limit: int = 50) -> list[PermitRecord]:
        """Search for permits matching the query."""
        ...


def get_permits_scraper(county: str) -> Optional[PermitsScraper]:
    """Get permits scraper for a given county.
    
    Args:
        county: County name (lowercase, e.g., 'seminole')
        
    Returns:
        PermitsScraper instance or None if county not supported
    """
    county_lower = county.lower().strip()
    
    if county_lower == "seminole":
        from florida_property_scraper.permits.seminole import SeminolePermitsScraper
        return SeminolePermitsScraper()
    
    return None
