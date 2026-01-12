from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from florida_property_scraper.permits.models import PermitRecord


class PermitsScraper(Protocol):
    """Permits scraper contract.

    - `search_permits` may do LIVE HTTP only when `LIVE=1`.
    - `parse_permits` must be pure and testable offline.
    """

    county: str

    def search_permits(self, query: str, limit: int) -> list[PermitRecord]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class ParseContext:
    source_url: str
    county: str


def parse_permits(content: str, source_url: str, *, county: str) -> list[PermitRecord]:
    """Fallback parser entrypoint.

    County implementations should usually expose their own `parse_permits`.
    """

    raise NotImplementedError(
        "No generic permits parser; use a county-specific permits module"
    )
