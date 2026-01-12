"""Permits scraping/ingestion.

CI safety: parsers must be testable offline with HTML/JSON fixtures.
Network access must be explicitly LIVE-gated.
"""

from .models import PermitRecord
from .registry import get_permits_scraper

__all__ = ["PermitRecord", "get_permits_scraper"]
