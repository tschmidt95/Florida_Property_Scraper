"""Base classes for permit scrapers."""
from abc import ABC, abstractmethod
from typing import List

from florida_property_scraper.permits.models import PermitRecord


class PermitScraperBase(ABC):
    """Base class for county-specific permit scrapers."""

    @abstractmethod
    def parse_permits(self, content: str, source_url: str) -> List[PermitRecord]:
        """Parse permits from HTML/JSON content.

        This method should be pure (no network I/O) for CI-safe testing.

        Args:
            content: HTML or JSON response content
            source_url: Source URL for the content (for record keeping)

        Returns:
            List of PermitRecord objects
        """
        pass

    @abstractmethod
    def search_permits(
        self, query: str, limit: int = 100
    ) -> List[PermitRecord]:
        """Search permits with network requests.

        This method performs live HTTP requests and should only be called
        when LIVE=1 environment variable is set.

        Args:
            query: Search query (address, parcel ID, etc.)
            limit: Maximum number of results

        Returns:
            List of PermitRecord objects
        """
        pass
