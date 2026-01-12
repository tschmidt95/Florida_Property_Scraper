"""Base interface for permits scrapers."""
from abc import ABC, abstractmethod
from typing import List

from florida_property_scraper.permits.models import PermitRecord


class PermitsScraper(ABC):
    """Abstract base class for county-specific permit scrapers."""

    @abstractmethod
    def search_permits(self, query: str, limit: int = 50) -> List[PermitRecord]:
        """Search for permits. Returns list of PermitRecord instances.

        Args:
            query: Search query (e.g., address, parcel_id, or owner name)
            limit: Maximum number of results to return

        Returns:
            List of PermitRecord instances

        Raises:
            RuntimeError: If LIVE=1 is not set when accessing live data
        """
        pass
