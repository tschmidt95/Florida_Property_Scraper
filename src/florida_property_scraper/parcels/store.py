from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple

from florida_property_scraper.cache import cache_get, cache_set


BBox = Tuple[float, float, float, float]


def parse_bbox(raw: str) -> BBox:
    parts = [p.strip() for p in (raw or "").split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be minLon,minLat,maxLon,maxLat")
    min_lon, min_lat, max_lon, max_lat = [float(p) for p in parts]
    if max_lon < min_lon or max_lat < min_lat:
        raise ValueError("bbox is invalid")
    return (min_lon, min_lat, max_lon, max_lat)


def _bbox_intersects(a: BBox, b: BBox) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def _geom_bbox(geom: Dict[str, Any]) -> Optional[BBox]:
    gtype = (geom or {}).get("type")
    coords = (geom or {}).get("coordinates")
    if not gtype or coords is None:
        return None

    def _walk_numbers(obj: Any) -> Iterable[Tuple[float, float]]:
        if isinstance(obj, (list, tuple)) and len(obj) == 2 and all(
            isinstance(x, (int, float)) for x in obj
        ):
            yield (float(obj[0]), float(obj[1]))
            return
        if isinstance(obj, (list, tuple)):
            for item in obj:
                yield from _walk_numbers(item)

    xs: List[float] = []
    ys: List[float] = []
    for x, y in _walk_numbers(coords):
        xs.append(x)
        ys.append(y)
    if not xs or not ys:
        return None
    return (min(xs), min(ys), max(xs), max(ys))


class ParcelGeometryStore(Protocol):
    def get_by_bbox(self, *, bbox: BBox, zoom: int, county: Optional[str]) -> Dict[str, Any]:
        ...

    def get_minimal_hover(self, *, county: str, parcel_id: str) -> Dict[str, Any]:
        ...


@dataclass(frozen=True)
class FileGeoJSONParcelGeometryStore:
    """File-based per-county GeoJSON store.

    Directory layout:
      <root>/<county>.geojson

    This is meant as a starter adapter; production can swap this out for
    PostGIS/MVT later without changing endpoint semantics.
    """

    root_dir: Path

    def _path_for_county(self, county: str) -> Path:
        return self.root_dir / f"{county}.geojson"

    def _load_county_fc(self, county: str) -> Dict[str, Any]:
        path = self._path_for_county(county)
        if not path.exists():
            return {"type": "FeatureCollection", "features": []}
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or raw.get("type") != "FeatureCollection":
            return {"type": "FeatureCollection", "features": []}
        if not isinstance(raw.get("features"), list):
            return {"type": "FeatureCollection", "features": []}
        return raw

    def get_by_bbox(self, *, bbox: BBox, zoom: int, county: Optional[str]) -> Dict[str, Any]:
        # Zoom gating here (endpoint also gates; keep store safe on its own).
        if int(zoom) < 15:
            return {"type": "FeatureCollection", "features": []}

        county_key = (county or "").strip().lower()
        if not county_key:
            return {"type": "FeatureCollection", "features": []}

        # Cache key rounds bbox to reduce churn from tiny moves.
        rb = tuple(round(x, 5) for x in bbox)
        cache_key = ("parcels:bbox", county_key, int(zoom), rb)
        cached = cache_get(cache_key)
        if cached is not None:
            return cached

        fc = self._load_county_fc(county_key)
        out_features: List[Dict[str, Any]] = []
        limit = int(os.getenv("PARCELS_LIMIT", "2000"))

        for feat in fc.get("features", []):
            if not isinstance(feat, dict):
                continue
            geom = feat.get("geometry")
            if not isinstance(geom, dict):
                continue
            gb = _geom_bbox(geom)
            if gb is None:
                continue
            if not _bbox_intersects(bbox, gb):
                continue

            props = feat.get("properties") if isinstance(feat.get("properties"), dict) else {}
            parcel_id = (
                props.get("parcel_id")
                or props.get("PARCEL_ID")
                or feat.get("parcel_id")
                or feat.get("PARCEL_ID")
                or ""
            )
            parcel_id = str(parcel_id)
            if not parcel_id:
                continue
            feature_id = f"{county_key}:{parcel_id}"

            out_features.append(
                {
                    "type": "Feature",
                    "id": feature_id,
                    "geometry": geom,
                    "properties": {
                        "parcel_id": parcel_id,
                        "county": county_key,
                    },
                }
            )
            if len(out_features) >= limit:
                break

        payload = {"type": "FeatureCollection", "features": out_features}
        cache_set(cache_key, payload, ttl=30)
        return payload

    def get_minimal_hover(self, *, county: str, parcel_id: str) -> Dict[str, Any]:
        # Hover details are served from PA storage (not geometry store).
        # This store only provides a stable contract placeholder.
        return {
            "parcel_id": str(parcel_id),
            "county": str(county),
            "situs_address": "",
            "owner_name": "",
            "last_sale_date": None,
            "last_sale_price": 0,
            "mortgage_amount": 0,
            "mortgage_lender": "",
        }


def default_geometry_store() -> FileGeoJSONParcelGeometryStore:
    root = os.getenv("PARCEL_GEOJSON_DIR")
    if root:
        return FileGeoJSONParcelGeometryStore(Path(root))

    # Prefer a repo-local data directory if present; tests can override with env.
    repo_root = Path(__file__).resolve().parents[3]
    data_dir = repo_root / "data" / "parcels"
    if data_dir.exists():
        return FileGeoJSONParcelGeometryStore(data_dir)

    # Fall back to fixtures directory (useful for dev/demo).
    fixtures_dir = repo_root / "tests" / "fixtures" / "parcels"
    return FileGeoJSONParcelGeometryStore(fixtures_dir)
