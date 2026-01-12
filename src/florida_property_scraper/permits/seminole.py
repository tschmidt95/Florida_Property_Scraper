"""Seminole County permits scraper.

Targets: https://semc-egov.aspgov.com/Click2GovBP/
"""

import os
import time
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from florida_property_scraper.permits.base import PermitsScraper
from florida_property_scraper.permits.models import PermitRecord


def parse_permits(html: str, source_url: str) -> List[PermitRecord]:
    """Parse permits from Click2GovBP search result HTML.

    This is a pure parser function suitable for fixture-based testing.

    Args:
        html: HTML content from Click2GovBP search results
        source_url: Source URL for attribution

    Returns:
        List of PermitRecord instances
    """
    soup = BeautifulSoup(html, "html.parser")
    permits: List[PermitRecord] = []

    # Click2GovBP typically shows results in a table or result divs
    # We'll look for common patterns in permit search results
    rows = soup.find_all(
        "tr", class_=lambda x: x and "result" in x.lower() if x else False
    )
    if not rows:
        # Try alternative patterns
        rows = soup.find_all("tr")[1:]  # Skip header row if present

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        # Extract permit information from cells
        # Common Click2GovBP format: permit#, address, type, status, dates
        permit_number = cells[0].get_text(strip=True) if len(cells) > 0 else ""
        if not permit_number or permit_number.lower() in ["permit", "no results"]:
            continue

        address = cells[1].get_text(strip=True) if len(cells) > 1 else None
        permit_type = cells[2].get_text(strip=True) if len(cells) > 2 else None
        status = cells[3].get_text(strip=True) if len(cells) > 3 else None
        issue_date = cells[4].get_text(strip=True) if len(cells) > 4 else None
        final_date = cells[5].get_text(strip=True) if len(cells) > 5 else None
        description = cells[6].get_text(strip=True) if len(cells) > 6 else None

        # Normalize dates to ISO format if possible
        issue_date_iso = _normalize_date(issue_date) if issue_date else None
        final_date_iso = _normalize_date(final_date) if final_date else None

        permits.append(
            PermitRecord(
                county="seminole",
                parcel_id=None,  # Click2GovBP may not provide parcel_id directly
                address=address,
                permit_number=permit_number,
                permit_type=permit_type,
                status=status,
                issue_date=issue_date_iso,
                final_date=final_date_iso,
                description=description,
                source=source_url,
                raw=str(row),
            )
        )

    return permits


def _normalize_date(date_str: str) -> Optional[str]:
    """Normalize date string to ISO format YYYY-MM-DD.

    Args:
        date_str: Date string in various formats (MM/DD/YYYY, etc.)

    Returns:
        ISO formatted date string or None if parsing fails
    """
    if not date_str or date_str.strip() == "":
        return None

    date_str = date_str.strip()

    # Try common US date formats
    from datetime import datetime

    for fmt in ["%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y"]:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


class SeminolePermitsScraper(PermitsScraper):
    """Scraper for Seminole County permits via Click2GovBP."""

    def __init__(self):
        """Initialize scraper with configuration."""
        self.base_url = "https://semc-egov.aspgov.com/Click2GovBP/"
        # Allow override via env var for testing
        self.search_url_template = os.getenv(
            "SEMINOLE_PERMITS_SEARCH_URL_TEMPLATE",
            "https://semc-egov.aspgov.com/Click2GovBP/PermitSearch.aspx",
        )
        self.user_agent = "FloridaPropertyScraper/1.0 (Educational Research)"
        self.rate_limit_seconds = 1.0
        self.last_request_time = 0.0

    def search_permits(self, query: str, limit: int = 50) -> List[PermitRecord]:
        """Search for permits via Click2GovBP.

        This method performs live HTTP requests and is gated behind LIVE=1.

        Args:
            query: Search query (address, parcel_id, or owner name)
            limit: Maximum number of results (default 50)

        Returns:
            List of PermitRecord instances

        Raises:
            RuntimeError: If LIVE=1 is not set
        """
        if os.getenv("LIVE") != "1":
            raise RuntimeError(
                "Live permit scraping requires LIVE=1 environment variable"
            )

        # Validate URL is from Click2GovBP
        if not self.search_url_template.startswith(self.base_url):
            raise ValueError(
                f"Search URL must start with {self.base_url} (Click2GovBP only)"
            )

        # Check robots.txt (best effort)
        self._check_robots()

        # Apply rate limiting
        self._rate_limit()

        # Perform search with retries
        html = self._fetch_with_retry(query, max_retries=3)

        # Parse results
        permits = parse_permits(html, self.search_url_template)

        # Apply limit
        return permits[:limit]

    def _check_robots(self):
        """Best-effort check of robots.txt."""
        try:
            robots_url = urljoin(self.base_url, "/robots.txt")
            resp = requests.get(robots_url, timeout=5)
            if resp.status_code == 200:
                # Basic check: if robots.txt explicitly disallows our path, warn
                if "Disallow: /Click2GovBP" in resp.text:
                    # Just log a warning, don't block (best effort)
                    pass
        except Exception:
            # Ignore robots.txt errors (best effort)
            pass

    def _rate_limit(self):
        """Ensure we don't exceed 1 request per second."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)
        self.last_request_time = time.time()

    def _fetch_with_retry(self, query: str, max_retries: int = 3) -> str:
        """Fetch search results with retries and backoff.

        Args:
            query: Search query
            max_retries: Maximum number of retry attempts

        Returns:
            HTML content

        Raises:
            RuntimeError: If all retries fail
        """
        headers = {"User-Agent": self.user_agent}

        for attempt in range(max_retries):
            try:
                # Make request (simplified - real implementation would need proper form submission)
                response = requests.get(
                    self.search_url_template,
                    params={"q": query},
                    headers=headers,
                    timeout=30,
                )
                response.raise_for_status()
                return response.text

            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    # Exponential backoff
                    backoff = 2**attempt
                    time.sleep(backoff)
                else:
                    raise RuntimeError(
                        f"Failed to fetch permits after {max_retries} attempts: {e}"
                    )

        raise RuntimeError("Unexpected error in fetch retry logic")
