from __future__ import annotations

from typing import Protocol

from florida_property_scraper.permits_models import PermitRecord


class PermitsScraper(Protocol):
    county: str

    def fetch_permits(self, *, parcel_id: str) -> list[PermitRecord]:
        ...
