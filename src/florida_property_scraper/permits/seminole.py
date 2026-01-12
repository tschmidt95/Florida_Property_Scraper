"""
Seminole County Permit Scraper.

Target portal: https://semc-egov.aspgov.com/Click2GovBP/
This scraper targets the Click2GovBP building permit portal for Seminole County, FL.
"""

import os
import time
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from florida_property_scraper.permits.base import PermitScraperBase
from florida_property_scraper.permits.models import PermitRecord


class SeminolePermitScraper(PermitScraperBase):
    """
    Seminole County permit scraper for Click2GovBP portal.

    Portal: https://semc-egov.aspgov.com/Click2GovBP/
    """

    BASE_URL = "https://semc-egov.aspgov.com/Click2GovBP/"
    SEARCH_URL = BASE_URL + "Default.aspx"

    def parse_permits(self, content: str, source_url: str) -> List[PermitRecord]:
        """
        Parse permits from Click2GovBP HTML content.

        Args:
            content: HTML content from the search results page
            source_url: URL where content was fetched from

        Returns:
            List of PermitRecord objects
        """
        soup = BeautifulSoup(content, "html.parser")
        permits = []

        # Look for permit records in tables
        # Click2GovBP typically uses GridView or similar table structures
        tables = soup.find_all("table")

        for table in tables:
            # Try to find data rows (skip header rows)
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Check if this looks like a permits table
            header_row = rows[0]
            headers = [
                th.get_text(strip=True).lower()
                for th in header_row.find_all(["th", "td"])
            ]

            # Look for permit-related headers
            has_permit_headers = any(
                keyword in " ".join(headers)
                for keyword in ["permit", "number", "address", "status", "type"]
            )

            if not has_permit_headers:
                continue

            # Parse data rows
            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 3:
                    continue

                cell_texts = [cell.get_text(strip=True) for cell in cells]

                # Extract permit data - this is a generic parser
                # Real implementation would need to match specific column positions
                permit_number = None
                address = None
                permit_type = None
                status = None
                issue_date = None
                final_date = None
                description = None

                # Try to identify columns by content patterns
                for i, text in enumerate(cell_texts):
                    if not text:
                        continue

                    # Permit number pattern (e.g., "BP-2023-12345")
                    if not permit_number and any(
                        prefix in text.upper()
                        for prefix in ["BP-", "BLD-", "PERMIT", "#"]
                    ):
                        permit_number = text
                    # Address pattern (contains numbers and street keywords)
                    elif not address and any(
                        keyword in text.upper()
                        for keyword in [
                            " ST",
                            " RD",
                            " AVE",
                            " BLVD",
                            " DR",
                            " LN",
                            " WAY",
                        ]
                    ):
                        address = text
                    # Status keywords
                    elif not status and any(
                        keyword in text.upper()
                        for keyword in [
                            "ISSUED",
                            "APPROVED",
                            "PENDING",
                            "FINALED",
                            "CLOSED",
                            "ACTIVE",
                        ]
                    ):
                        status = text
                    # Date pattern (MM/DD/YYYY or similar)
                    elif ("/" in text or "-" in text) and len(text) <= 12:
                        if not issue_date:
                            issue_date = text
                        elif not final_date:
                            final_date = text

                # If we found at least a permit number, create a record
                if permit_number:
                    permits.append(
                        PermitRecord(
                            county="seminole",
                            permit_number=permit_number,
                            address=address,
                            permit_type=permit_type,
                            status=status,
                            issue_date=issue_date,
                            final_date=final_date,
                            description=description,
                            source=source_url,
                            raw=str(row),
                        )
                    )

        return permits

    def search_permits(self, query: str, limit: int = 50) -> List[PermitRecord]:
        """
        Search for permits on Click2GovBP portal.

        This method requires LIVE=1 environment variable to be set.
        Implements rate limiting (<= 1 req/sec), User-Agent, retries/backoff,
        and best-effort robots.txt check.

        Args:
            query: Search query (address, parcel ID, etc.)
            limit: Maximum number of permits to return

        Returns:
            List of PermitRecord objects

        Raises:
            RuntimeError: If LIVE!=1
        """
        if os.environ.get("LIVE") != "1":
            raise RuntimeError(
                "Live permit scraping requires LIVE=1 environment variable. "
                "Set LIVE=1 to enable network requests to "
                "https://semc-egov.aspgov.com/Click2GovBP/"
            )

        # Best-effort robots.txt check
        self._check_robots_txt()

        import requests

        headers = {
            "User-Agent": "FloridaPropertyScraper/1.0 (Research/Educational; "
            "+https://github.com/tschmidt95/Florida_Property_Scraper)"
        }

        # Rate limiting: enforce <= 1 req/sec
        time.sleep(1.0)

        # Implement retry logic with exponential backoff
        max_retries = 3
        backoff_factor = 2.0

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    sleep_time = backoff_factor**attempt
                    time.sleep(sleep_time)

                # Make the search request
                # Note: This is a placeholder - actual implementation would need
                # to handle form submission, session management, etc.
                response = requests.get(
                    self.SEARCH_URL,
                    params={"search": query},
                    headers=headers,
                    timeout=30,
                )
                response.raise_for_status()

                # Parse and return permits
                permits = self.parse_permits(response.text, response.url)
                return permits[:limit]

            except Exception as e:
                if attempt == max_retries - 1:
                    raise RuntimeError(
                        f"Failed to fetch permits after {max_retries} attempts: {e}"
                    ) from e

        return []

    def _check_robots_txt(self) -> None:
        """
        Best-effort check of robots.txt.

        This is a minimal implementation that checks if robots.txt exists
        and logs a warning if it disallows scraping.
        """
        try:
            import requests

            robots_url = urljoin(self.BASE_URL, "/robots.txt")
            response = requests.get(robots_url, timeout=5)

            if response.status_code == 200:
                # Simple check - just look for "Disallow: /"
                if "Disallow: /" in response.text:
                    import warnings

                    warnings.warn(
                        f"robots.txt at {robots_url} contains 'Disallow: /' - "
                        "proceeding with caution"
                    )
        except Exception:
            # Best-effort only - don't fail if we can't check robots.txt
            pass
