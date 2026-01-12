"""Base interface for permit scrapers."""
from typing import Protocol, List
from florida_property_scraper.permits.models import PermitRecord


class PermitsScraper(Protocol):
    """Protocol for county permit scrapers."""

    def search_permits(self, query: str, limit: int = 50) -> List[PermitRecord]:
        """Search for permits and return list of PermitRecord objects.
        
        Args:
            query: Search query (address, parcel ID, etc.)
            limit: Maximum number of results to return
            
        Returns:
            List of PermitRecord objects
        """
        ...
