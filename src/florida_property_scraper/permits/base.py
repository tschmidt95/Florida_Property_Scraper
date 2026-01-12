"""Base class for permit scrapers."""
from abc import ABC, abstractmethod
from typing import List

from florida_property_scraper.permits.models import PermitRecord


class PermitScraperBase(ABC):
    """Base class for county-specific permit scrapers."""

    @abstractmethod
    def parse_permits(self, content: str, source_url: str) -> List[PermitRecord]:
        """
        Parse permits from HTML/JSON content.

        Args:
            content: Raw HTML or JSON content
            source_url: URL where content was fetched from

        Returns:
            List of PermitRecord objects
        """
        pass

    @abstractmethod
    def search_permits(
        self, query: str, limit: int = 50
    ) -> List[PermitRecord]:
        """
        Search for permits (requires LIVE=1).

        Args:
            query: Search query (address, parcel, etc.)
            limit: Maximum number of permits to return

        Returns:
            List of PermitRecord objects

        Raises:
            RuntimeError: If LIVE!=1
        """
        pass
