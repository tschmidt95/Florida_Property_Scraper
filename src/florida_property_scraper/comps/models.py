from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class SubjectProperty:
    county: str
    parcel_id: str
    sale_date: date
    sale_price: float

    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    property_type: Optional[str] = None
    building_sf: Optional[float] = None
    land_sf: Optional[float] = None
    year_built: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["sale_date"] = self.sale_date.isoformat()
        return payload


@dataclass(frozen=True)
class ComparableListing:
    id: str
    source: str

    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    property_type: Optional[str] = None
    asking_price: Optional[float] = None
    price_per_sf: Optional[float] = None
    building_sf: Optional[float] = None
    year_built: Optional[int] = None
    cap_rate: Optional[float] = None
    url: Optional[str] = None

    def computed_price_per_sf(self) -> Optional[float]:
        if self.price_per_sf is not None:
            return float(self.price_per_sf)
        if self.asking_price is None or self.building_sf is None:
            return None
        if self.building_sf <= 0:
            return None
        return float(self.asking_price) / float(self.building_sf)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["price_per_sf"] = self.computed_price_per_sf()
        return payload


@dataclass(frozen=True)
class RankedComparable:
    listing: ComparableListing
    score: float
    explanation: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "listing": self.listing.to_dict(),
            "score": float(self.score),
            "explanation": dict(self.explanation),
        }
