from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Optional, Tuple


BBox = Tuple[float, float, float, float]


def _walk_coords(obj: Any) -> Iterable[Tuple[float, float]]:
    if isinstance(obj, (list, tuple)) and len(obj) == 2 and all(
        isinstance(x, (int, float)) for x in obj
    ):
        yield float(obj[0]), float(obj[1])
        return
    if isinstance(obj, (list, tuple)):
        for it in obj:
            yield from _walk_coords(it)


def geometry_bbox(geometry: Dict[str, Any]) -> Optional[BBox]:
    coords = (geometry or {}).get("coordinates")
    if coords is None:
        return None
    xs: List[float] = []
    ys: List[float] = []
    for x, y in _walk_coords(coords):
        xs.append(x)
        ys.append(y)
    if not xs or not ys:
        return None
    return (min(xs), min(ys), max(xs), max(ys))


def circle_polygon(*, center_lon: float, center_lat: float, miles: float, steps: int = 36) -> Dict[str, Any]:
    """Approximate a radius search as a GeoJSON Polygon.

    Uses a simple spherical earth approximation.
    """

    r_miles = max(float(miles), 0.0)
    steps = max(int(steps), 12)

    # Convert miles to degrees (approx). Latitude: ~69 miles/deg.
    dlat = r_miles / 69.0
    # Longitude degrees shrink with latitude.
    cos_lat = math.cos(math.radians(center_lat))
    dlon = (r_miles / 69.0) / max(cos_lat, 1e-6)

    ring: List[List[float]] = []
    for i in range(steps):
        a = 2.0 * math.pi * (i / steps)
        lon = center_lon + (dlon * math.cos(a))
        lat = center_lat + (dlat * math.sin(a))
        ring.append([float(lon), float(lat)])
    # close ring
    if ring:
        ring.append(ring[0])

    return {"type": "Polygon", "coordinates": [ring]}


def intersects(search_geometry: Dict[str, Any], feature_geometry: Dict[str, Any]) -> bool:
    """Return True if feature_geometry intersects search_geometry.

    Prefers Shapely if available. Falls back to bbox intersection.
    """

    # Attempt shapely fast path.
    try:
        from shapely.geometry import shape as s_shape  # type: ignore[import-not-found]
    except Exception:
        s_shape = None

    if s_shape is not None:
        try:
            return bool(s_shape(feature_geometry).intersects(s_shape(search_geometry)))
        except Exception:
            # Fall through to bbox check.
            pass

    a = geometry_bbox(search_geometry)
    b = geometry_bbox(feature_geometry)
    if a is None or b is None:
        return False
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])
