"""Seminole County, FL permits scraper.

Phase 1 Implementation:
- County: Seminole County, FL
- Portal: https://semc-egov.aspgov.com/Click2GovBP/
- Target: Building permit search results

This scraper provides CI-safe parsing via parse_permits() and
LIVE-gated network access via search_permits().
"""
import json
import os
import re
import time
from typing import List, Optional

try:
    import requests
    from bs4 import BeautifulSoup

    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False

from florida_property_scraper.permits.base import PermitScraperBase
from florida_property_scraper.permits.models import PermitRecord


class SeminolePermitScraper(PermitScraperBase):
    """Seminole County building permits scraper.

    Portal: https://semc-egov.aspgov.com/Click2GovBP/
    """

    BASE_URL = "https://semc-egov.aspgov.com/Click2GovBP/"
    USER_AGENT = "FloridaPropertyScraper/1.0 (Research; +https://github.com/tschmidt95/Florida_Property_Scraper)"

    def parse_permits(self, content: str, source_url: str) -> List[PermitRecord]:
        """Parse permits from HTML content.

        This is a CI-safe pure parser for testing.

        Args:
            content: HTML content from search results
            source_url: Source URL for record keeping

        Returns:
            List of PermitRecord objects
        """
        if not DEPS_AVAILABLE:
            return []

        permits = []
        soup = BeautifulSoup(content, "html.parser")

        # Look for permit result rows
        # The portal typically shows results in tables or divs
        # This is a generic parser that handles common patterns
        rows = soup.find_all("tr", class_=re.compile(r".*row.*", re.I)) or soup.find_all(
            "div", class_=re.compile(r".*permit.*|.*result.*", re.I)
        )

        for row in rows:
            try:
                permit = self._parse_permit_row(row, source_url)
                if permit:
                    permits.append(permit)
            except Exception:
                # Skip malformed rows
                continue

        return permits

    def _parse_permit_row(
        self, element, source_url: str
    ) -> Optional[PermitRecord]:
        """Parse a single permit row from HTML.

        Args:
            element: BeautifulSoup element (tr or div)
            source_url: Source URL

        Returns:
            PermitRecord or None if parsing fails
        """
        # Extract permit number (required)
        permit_number = None
        permit_link = element.find("a", href=re.compile(r".*permit.*", re.I))
        if permit_link:
            permit_number = permit_link.get_text(strip=True)

        if not permit_number:
            # Try finding by pattern
            text = element.get_text()
            match = re.search(r"\b([A-Z]{2,4}[\-\s]?\d{4,}[\-\s]?\d+)\b", text)
            if match:
                permit_number = match.group(1)

        if not permit_number:
            return None

        # Extract other fields
        cells = element.find_all(["td", "div"])
        address = None
        permit_type = None
        status = None
        issue_date = None
        final_date = None
        description = None

        for i, cell in enumerate(cells):
            cell_text = cell.get_text(strip=True)
            if not cell_text:
                continue

            # Address patterns
            if re.search(
                r"\d+\s+\w+\s+(st|street|ave|avenue|rd|road|blvd|boulevard|dr|drive|ln|lane|way|ct|court|pl|place)",
                cell_text,
                re.I,
            ):
                address = cell_text

            # Status patterns
            if re.search(
                r"\b(issued|approved|final|pending|closed|active|complete)\b",
                cell_text,
                re.I,
            ):
                status = cell_text

            # Date patterns (MM/DD/YYYY or YYYY-MM-DD)
            date_match = re.search(
                r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b",
                cell_text,
            )
            if date_match:
                date_str = date_match.group(1)
                # Normalize to YYYY-MM-DD
                normalized_date = self._normalize_date(date_str)
                if not issue_date:
                    issue_date = normalized_date
                elif not final_date:
                    final_date = normalized_date

            # Type patterns
            if re.search(r"\b(building|electrical|plumbing|mechanical|roofing)\b", cell_text, re.I):
                permit_type = cell_text

        # If no address found, try to extract from full text
        if not address:
            full_text = element.get_text()
            addr_match = re.search(
                r"\d+\s+[\w\s]+(st|street|ave|avenue|rd|road|blvd|dr|ln|way|ct|pl)",
                full_text,
                re.I,
            )
            if addr_match:
                address = addr_match.group(0).strip()

        return PermitRecord(
            county="seminole",
            parcel_id=None,  # Not always available in search results
            address=address,
            permit_number=permit_number,
            permit_type=permit_type,
            status=status,
            issue_date=issue_date,
            final_date=final_date,
            description=description,
            source=source_url,
            raw=None,
        )

    def _normalize_date(self, date_str: str) -> Optional[str]:
        """Normalize date string to YYYY-MM-DD format.

        Args:
            date_str: Date string in various formats

        Returns:
            ISO date string or None
        """
        if not date_str:
            return None

        # Try MM/DD/YYYY or MM-DD-YYYY
        match = re.match(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", date_str)
        if match:
            month, day, year = match.groups()
            if len(year) == 2:
                year = "20" + year if int(year) < 50 else "19" + year
            return f"{year:0>4}-{month:0>2}-{day:0>2}"

        # Try YYYY-MM-DD or YYYY/MM/DD (already normalized)
        match = re.match(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", date_str)
        if match:
            year, month, day = match.groups()
            return f"{year}-{month:0>2}-{day:0>2}"

        return None

    def search_permits(self, query: str, limit: int = 100) -> List[PermitRecord]:
        """Search permits via live HTTP requests.

        LIVE-gated: Only runs when LIVE=1 environment variable is set.

        Args:
            query: Search query (address, parcel ID, etc.)
            limit: Maximum results

        Returns:
            List of PermitRecord objects

        Raises:
            RuntimeError: If LIVE=1 is not set or dependencies not available
        """
        if os.getenv("LIVE") != "1":
            raise RuntimeError(
                "Live permit search requires LIVE=1 environment variable"
            )

        if not DEPS_AVAILABLE:
            raise RuntimeError(
                "requests and beautifulsoup4 required for live permit search"
            )

        # Best-effort robots.txt check
        self._check_robots()

        permits = []
        try:
            # Perform search request with rate limiting
            search_url = f"{self.BASE_URL}PermitSearch.aspx"
            params = {"address": query}

            response = requests.get(
                search_url,
                params=params,
                headers={"User-Agent": self.USER_AGENT},
                timeout=30,
            )
            response.raise_for_status()

            # Rate limit: <= 1 req/sec
            time.sleep(1.1)

            # Parse results
            permits = self.parse_permits(response.text, response.url)

            # Limit results
            if len(permits) > limit:
                permits = permits[:limit]

        except requests.RequestException as e:
            # Log error but don't crash
            print(f"Error searching permits: {e}")

        return permits

    def _check_robots(self) -> None:
        """Best-effort robots.txt check.

        This is a simple check and may not catch all restrictions.
        """
        try:
            robots_url = f"{self.BASE_URL}robots.txt"
            response = requests.get(
                robots_url,
                headers={"User-Agent": self.USER_AGENT},
                timeout=10,
            )
            if response.status_code == 200:
                # Simple check for Disallow: /PermitSearch
                if "Disallow: /PermitSearch" in response.text:
                    raise RuntimeError(
                        "robots.txt disallows /PermitSearch - aborting"
                    )
        except requests.RequestException:
            # If robots.txt doesn't exist or is inaccessible, proceed cautiously
            pass


# Register the scraper
from florida_property_scraper.permits.registry import register_scraper

register_scraper("seminole", SeminolePermitScraper())
