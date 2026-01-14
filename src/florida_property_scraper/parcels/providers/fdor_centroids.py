from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from florida_property_scraper.parcels.geometry_provider import BBox, Feature, ParcelGeometryProvider, feature_id
from florida_property_scraper.parcels.live.fdor_centroids import FDORCentroidClient


def _geojson_point_from_esri(geom: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # ArcGIS point geometry: {x, y}
    try:
        x = geom.get("x")
        y = geom.get("y")
        if x is None or y is None:
            return None
        return {"type": "Point", "coordinates": [float(x), float(y)]}
    except Exception:
        return None


@dataclass
class FDORCentroidsProvider(ParcelGeometryProvider):
    county: str
    client: FDORCentroidClient

    def __init__(self, county: str, client: Optional[FDORCentroidClient] = None) -> None:
        self.county = (county or "").strip().lower()
        self.client = client or FDORCentroidClient()

    def load(self) -> None:
        # No local index; remote service.
        return

    def query(self, bbox: BBox) -> List[Feature]:
        feats = self.client.query_bbox(bbox, limit=2000)
        out: List[Feature] = []
        for f in feats:
            attrs = f.get("attributes") or {}
            pid = str(attrs.get("PARCEL_ID") or "").strip()
            if not pid:
                continue
            gj = _geojson_point_from_esri(f.get("geometry") or {})
            if gj is None:
                continue
            out.append(
                Feature(
                    feature_id=feature_id(self.county, pid),
                    county=self.county,
                    parcel_id=pid,
                    geometry=gj,
                )
            )
        return out
