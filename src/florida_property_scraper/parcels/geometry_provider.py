from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, Tuple


BBox = Tuple[float, float, float, float]


@dataclass(frozen=True)
class Feature:
    """Internal representation for a parcel feature.

    This stays GeoJSON-like to keep the system MVT/PostGIS-upgradeable later.
    """

    feature_id: str
    county: str
    parcel_id: str
    geometry: Dict[str, Any]

    def to_geojson_feature(self) -> Dict[str, Any]:
        return {
            "type": "Feature",
            "id": self.feature_id,
            "geometry": self.geometry,
            "properties": {
                # Contract: /api/parcels must return only these two keys.
                "parcel_id": self.parcel_id,
                "county": self.county,
            },
        }


class ParcelGeometryProvider(Protocol):
    """Multi-county parcel geometry provider.

    Providers should build an in-memory spatial index on load.
    """

    county: str

    def load(self) -> None: ...

    def query(self, bbox: BBox) -> List[Feature]: ...


def parse_bbox(raw: str) -> BBox:
    parts = [p.strip() for p in (raw or "").split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be minLon,minLat,maxLon,maxLat")
    min_lon, min_lat, max_lon, max_lat = [float(p) for p in parts]
    if max_lon < min_lon or max_lat < min_lat:
        raise ValueError("bbox is invalid")
    return (min_lon, min_lat, max_lon, max_lat)


def feature_id(county: str, parcel_id: str) -> str:
    return f"{county}:{parcel_id}"
