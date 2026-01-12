"""Seminole County permits scraper.

Target portal: https://semc-egov.aspgov.com/Click2GovBP/
"""
import os
import time
from typing import List, Optional
from urllib.parse import urljoin, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False

from florida_property_scraper.permits.models import PermitRecord


# Portal base URL for Seminole County Click2GovBP system
SEMINOLE_PERMITS_BASE_URL = "https://semc-egov.aspgov.com/Click2GovBP/"


def parse_permits(html: str, source_url: str) -> List[PermitRecord]:
    """Parse permits from HTML response.
    
    This is a pure parser suitable for fixture-based testing.
    
    Args:
        html: HTML content
        source_url: Source URL for this data
        
    Returns:
        List of PermitRecord objects
    """
    if not DEPS_AVAILABLE:
        raise ImportError("beautifulsoup4 and requests are required for permit parsing")
    
    soup = BeautifulSoup(html, 'html.parser')
    permits = []
    
    # Try to find permit records in the HTML
    # This is a placeholder parser - actual implementation depends on the portal structure
    # Looking for common patterns in permit search results
    
    # Try table rows
    rows = soup.find_all('tr')
    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 3:
            continue
            
        # Skip header rows
        if cells[0].name == 'th':
            continue
        
        # Extract permit data from cells
        # This is a basic heuristic parser
        cell_texts = [cell.get_text(strip=True) for cell in cells]
        
        # Skip empty rows
        if not any(cell_texts):
            continue
        
        # Try to identify permit number (usually starts with specific patterns)
        permit_number = None
        for text in cell_texts:
            # Common permit number patterns
            if text and (text.startswith('BP') or text.startswith('BLDG') or 
                        text.startswith('P') or '-' in text) and len(text) <= 30:
                permit_number = text
                break
        
        if not permit_number:
            continue
        
        # Extract other fields
        address = None
        permit_type = None
        status = None
        issue_date = None
        
        for text in cell_texts:
            if not text or text == permit_number:
                continue
            # Address heuristic: contains numbers and common street words
            if not address and any(word in text.lower() for word in ['st', 'dr', 'ave', 'rd', 'ln', 'way', 'blvd', 'ct']):
                address = text
            # Status heuristic
            elif not status and any(word in text.lower() for word in ['issued', 'final', 'pending', 'approved', 'closed', 'open']):
                status = text
            # Type heuristic
            elif not permit_type and any(word in text.lower() for word in ['building', 'electrical', 'plumbing', 'mechanical', 'residential', 'commercial']):
                permit_type = text
            # Date heuristic (contains / or -)
            elif not issue_date and ('/' in text or '-' in text) and len(text) <= 12:
                # Try to normalize to ISO format
                issue_date = _normalize_date(text)
        
        permit = PermitRecord(
            county="seminole",
            permit_number=permit_number,
            source=source_url,
            address=address,
            permit_type=permit_type,
            status=status,
            issue_date=issue_date,
            final_date=None,
            description=None,
            parcel_id=None,
            raw=str(row)[:1000] if row else None,
        )
        permits.append(permit)
    
    return permits


def _normalize_date(date_str: str) -> Optional[str]:
    """Normalize date string to ISO format YYYY-MM-DD.
    
    Args:
        date_str: Date string in various formats
        
    Returns:
        ISO date string or None
    """
    if not date_str or not date_str.strip():
        return None
    
    date_str = date_str.strip()
    
    # Try common formats: MM/DD/YYYY, M/D/YYYY, YYYY-MM-DD
    for sep in ['/', '-']:
        if sep in date_str:
            parts = date_str.split(sep)
            if len(parts) == 3:
                # Try MM/DD/YYYY or M/D/YYYY
                try:
                    if len(parts[0]) <= 2 and len(parts[1]) <= 2:
                        month, day, year = parts
                        if len(year) == 2:
                            year = '20' + year if int(year) <= 50 else '19' + year
                        return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"
                except (ValueError, AttributeError):
                    pass
                
                # Try YYYY-MM-DD
                try:
                    if len(parts[0]) == 4:
                        year, month, day = parts
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except (ValueError, AttributeError):
                    pass
    
    return None


class SeminolePermitsScraper:
    """Scraper for Seminole County permits via Click2GovBP."""
    
    def __init__(self):
        if not DEPS_AVAILABLE:
            raise ImportError("beautifulsoup4 and requests are required for Seminole permits scraper")
        
        self.base_url = os.getenv("SEMINOLE_PERMITS_SEARCH_URL_TEMPLATE", SEMINOLE_PERMITS_BASE_URL)
        self.user_agent = "FloridaPropertyScraper/1.0 (Research; +https://github.com/tschmidt95/Florida_Property_Scraper)"
        self.rate_limit_seconds = 1.0  # <= 1 req/sec
        self.last_request_time = 0.0
    
    def _check_robots_txt(self) -> bool:
        """Best-effort check of robots.txt.
        
        Returns:
            True if allowed or unable to check, False if explicitly disallowed
        """
        try:
            robots_url = urljoin(self.base_url, '/robots.txt')
            response = requests.get(
                robots_url,
                headers={'User-Agent': self.user_agent},
                timeout=5,
            )
            if response.status_code == 200:
                # Simple check: if robots.txt exists and contains "Disallow: /", respect it
                text = response.text.lower()
                if 'disallow: /' in text and 'user-agent: *' in text:
                    return False
        except Exception:
            # Unable to check, assume allowed
            pass
        return True
    
    def _rate_limit(self):
        """Enforce rate limiting."""
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)
        self.last_request_time = time.time()
    
    def _fetch_with_retry(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Fetch URL with retries and exponential backoff.
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retries
            
        Returns:
            Response text or None on failure
        """
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                
                response = requests.get(
                    url,
                    headers={'User-Agent': self.user_agent},
                    timeout=30,
                )
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    # Exponential backoff: 2, 4, 8 seconds
                    backoff = 2 ** (attempt + 1)
                    time.sleep(backoff)
                else:
                    raise RuntimeError(f"Failed to fetch permits after {max_retries} attempts: {e}")
        return None
    
    def search_permits(self, query: str, limit: int = 50) -> List[PermitRecord]:
        """Search for permits and return list of PermitRecord objects.
        
        This method requires LIVE=1 environment variable to make actual HTTP requests.
        
        Args:
            query: Search query (address, parcel ID, etc.)
            limit: Maximum number of results to return
            
        Returns:
            List of PermitRecord objects
            
        Raises:
            PermissionError: If LIVE environment variable is not set to "1"
            RuntimeError: If scraping fails
        """
        # LIVE gating
        if os.getenv("LIVE") != "1":
            raise PermissionError(
                "Live scraping requires LIVE=1 environment variable. "
                "Set LIVE=1 to enable actual HTTP requests to county portals."
            )
        
        # Best-effort robots.txt check
        if not self._check_robots_txt():
            raise PermissionError(
                "robots.txt disallows scraping for this portal. "
                "Please respect the site's robots.txt directives."
            )
        
        # Construct search URL
        # Note: Actual URL construction depends on portal's search interface
        # This is a placeholder - would need to be adapted to actual portal
        search_url = urljoin(self.base_url, f"Search?query={query}")
        
        # Fetch HTML
        html = self._fetch_with_retry(search_url)
        if not html:
            return []
        
        # Parse permits
        permits = parse_permits(html, search_url)
        
        # Apply limit
        return permits[:limit]
