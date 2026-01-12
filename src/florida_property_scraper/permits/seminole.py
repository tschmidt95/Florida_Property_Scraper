"""Seminole County permits scraper.

Target portal: https://semc-egov.aspgov.com/Click2GovBP/
Phase 1 deliverable for Seminole County, FL.
"""

import os
import time
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from florida_property_scraper.permits.models import PermitRecord

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    REQUESTS_AVAILABLE = False


def parse_permits(html: str, source_url: str) -> list[PermitRecord]:
    """Parse permits from Seminole Click2GovBP search result HTML.

    This is a pure parser function suitable for fixture-based tests.

    Args:
        html: HTML content of the search results page
        source_url: Source URL for attribution

    Returns:
        List of PermitRecord objects
    """
    soup = BeautifulSoup(html, "html.parser")
    permits: list[PermitRecord] = []

    # Look for common table patterns in Click2GovBP
    # This is a placeholder implementation - actual parsing depends on the HTML structure
    # For now, we'll parse a simple table structure
    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")
        # Skip header row
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 5:
                try:
                    permit = PermitRecord(
                        county="seminole",
                        parcel_id=cells[0].get_text(strip=True)
                        if len(cells) > 0
                        else None,
                        address=cells[1].get_text(strip=True)
                        if len(cells) > 1
                        else None,
                        permit_number=cells[2].get_text(strip=True)
                        if len(cells) > 2
                        else "",
                        permit_type=cells[3].get_text(strip=True)
                        if len(cells) > 3
                        else None,
                        status=cells[4].get_text(strip=True)
                        if len(cells) > 4
                        else None,
                        issue_date=cells[5].get_text(strip=True)
                        if len(cells) > 5
                        else None,
                        final_date=cells[6].get_text(strip=True)
                        if len(cells) > 6
                        else None,
                        description=cells[7].get_text(strip=True)
                        if len(cells) > 7
                        else None,
                        source=source_url,
                        raw=str(row),
                    )
                    if permit.permit_number:
                        permits.append(permit)
                except (IndexError, AttributeError):
                    continue

    return permits


class SeminolePermitsScraper:
    """Seminole County permits scraper for Click2GovBP portal."""

    def __init__(self):
        """Initialize the Seminole permits scraper."""
        self.base_url = "https://semc-egov.aspgov.com/Click2GovBP/"
        # Allow override via environment variable for testing
        self.search_url_template = os.getenv(
            "SEMINOLE_PERMITS_SEARCH_URL_TEMPLATE",
            "https://semc-egov.aspgov.com/Click2GovBP/Search.aspx?query={query}",
        )
        self.user_agent = "FloridaPropertyScraper/1.0 (Educational/Research)"
        self.rate_limit_delay = 1.0  # seconds between requests
        self.last_request_time = 0.0

    def _check_robots(self) -> bool:
        """Best-effort robots.txt check.

        Returns:
            True if allowed (or unable to determine), False if explicitly disallowed
        """
        if not REQUESTS_AVAILABLE:
            return True

        try:
            robots_url = urljoin(self.base_url, "/robots.txt")
            resp = requests.get(robots_url, timeout=5)
            if resp.status_code == 200:
                # Simple check for "Disallow: /"
                if "Disallow: /" in resp.text:
                    return False
        except Exception:
            # Best effort - if we can't check, assume allowed
            pass
        return True

    def _rate_limit(self):
        """Enforce rate limiting of <= 1 req/sec."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()

    def _fetch_with_retry(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Fetch URL with retries and exponential backoff.

        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts

        Returns:
            Response text or None on failure
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is required for live HTTP")

        for attempt in range(max_retries):
            try:
                self._rate_limit()
                resp = requests.get(
                    url,
                    headers={"User-Agent": self.user_agent},
                    timeout=30,
                )
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    backoff = 2**attempt
                    time.sleep(backoff)
                else:
                    raise RuntimeError(
                        f"Failed to fetch {url} after {max_retries} attempts: {e}"
                    )
        return None

    def search_permits(self, query: str, limit: int = 50) -> list[PermitRecord]:
        """Search for permits (LIVE-gated).

        Only performs live HTTP when LIVE=1 environment variable is set.

        Args:
            query: Search query (address, parcel ID, etc.)
            limit: Maximum number of permits to return

        Returns:
            List of PermitRecord objects

        Raises:
            RuntimeError: If LIVE environment variable is not set to "1"
        """
        if os.getenv("LIVE") != "1":
            raise RuntimeError(
                "Live HTTP is disabled. Set LIVE=1 environment variable to enable."
            )

        if not self._check_robots():
            raise RuntimeError("Robots.txt disallows scraping this site")

        search_url = self.search_url_template.format(query=query)
        html = self._fetch_with_retry(search_url)

        if not html:
            return []

        permits = parse_permits(html, search_url)
        return permits[:limit]
