from __future__ import annotations

import json
from pathlib import Path

from florida_property_scraper.permits_models import PermitRecord


class FixturePermitsScraper:
    def __init__(self, *, county: str, fixture_path: str):
        self.county = county
        self._path = Path(fixture_path)

    def fetch_permits(self, *, parcel_id: str) -> list[PermitRecord]:
        if not self._path.exists():
            return []

        data = json.loads(self._path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            return []

        out: list[PermitRecord] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            if str(item.get("parcel_id") or "").strip() != parcel_id:
                continue
            out.append(
                PermitRecord(
                    county=self.county,
                    parcel_id=parcel_id,
                    permit_id=str(item.get("permit_id") or "").strip(),
                    permit_type=(
                        str(item.get("permit_type")).strip()
                        if item.get("permit_type")
                        else None
                    ),
                    status=(
                        str(item.get("status")).strip() if item.get("status") else None
                    ),
                    issued_date=(
                        str(item.get("issued_date")).strip()
                        if item.get("issued_date")
                        else None
                    ),
                    finaled_date=(
                        str(item.get("finaled_date")).strip()
                        if item.get("finaled_date")
                        else None
                    ),
                    source=(str(item.get("source")).strip() if item.get("source") else None),
                )
            )
        return [p for p in out if p.permit_id]
