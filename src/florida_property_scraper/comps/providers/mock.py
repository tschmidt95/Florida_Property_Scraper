from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from .base import ListingProvider
from ..models import ComparableListing, SubjectProperty

class MockProvider(ListingProvider):
    """Fixture-backed deterministic provider.

    Loads listings from: tests/fixtures/comps/mock_listings.json
    """

    name = "mock"

    def __init__(self, fixture_path: str | Path | None = None) -> None:
        if fixture_path is None:
            repo_root = Path(__file__).resolve().parents[4]
            fixture_path = repo_root / "tests" / "fixtures" / "comps" / "mock_listings.json"
        self._fixture_path = Path(fixture_path)

        raw = json.loads(self._fixture_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("mock_listings.json must be a JSON object")

        self._by_county: Dict[str, List[ComparableListing]] = {}
        for county, listings in raw.items():
            if not isinstance(listings, list):
                continue
            parsed: List[ComparableListing] = []
            for item in listings:
                if not isinstance(item, dict):
                    continue
                parsed.append(
                    ComparableListing(
                        id=str(item.get("id")),
                        source=str(item.get("source", "mock")),
                        address=item.get("address"),
                        latitude=item.get("latitude"),
                        longitude=item.get("longitude"),
                        property_type=item.get("property_type"),
                        asking_price=item.get("asking_price"),
                        price_per_sf=item.get("price_per_sf"),
                        building_sf=item.get("building_sf"),
                        year_built=item.get("year_built"),
                        cap_rate=item.get("cap_rate"),
                        url=item.get("url"),
                    )
                )
            self._by_county[str(county).lower()] = parsed

    def search(self, subject: SubjectProperty) -> List[ComparableListing]:
        county_key = (subject.county or "").lower()
        listings = list(self._by_county.get(county_key, []))

        if subject.property_type:
            preferred = [
                l
                for l in listings
                if (l.property_type or "").lower() == subject.property_type.lower()
            ]
            if preferred:
                listings = preferred

        listings.sort(key=lambda l: l.id)
        return listings
