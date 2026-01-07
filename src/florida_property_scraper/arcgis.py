import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode


def build_where_clause(query: str, address_field: str, parcel_field: str) -> str:
    cleaned = query.strip()
    if cleaned.isdigit() and len(cleaned) >= 6:
        return f"{parcel_field}='{cleaned}'"
    escaped = cleaned.replace("'", "''")
    return f"{address_field} LIKE '%{escaped}%'"


def build_query_url(
    layer_url: str,
    where: str,
    out_fields: List[str],
    return_geometry: bool = False,
    limit: int = 10,
) -> str:
    params = {
        "where": where,
        "outFields": ",".join(out_fields),
        "f": "json",
        "resultRecordCount": limit,
        "returnGeometry": "true" if return_geometry else "false",
    }
    return f"{layer_url}/query?{urlencode(params)}"


def build_geometry_query_url(
    layer_url: str,
    geometry: Dict[str, Any],
    out_fields: Optional[List[str]] = None,
    limit: int = 5,
) -> str:
    params = {
        "geometry": json.dumps(geometry),
        "geometryType": "esriGeometryPolygon",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": ",".join(out_fields or ["*"]),
        "f": "json",
        "resultRecordCount": limit,
    }
    return f"{layer_url}/query?{urlencode(params)}"


def extract_first_field(
    features: List[Dict[str, Any]],
    fields: List[str],
) -> str:
    if not features:
        return ""
    attrs = features[0].get("attributes", {})
    for field in fields:
        value = attrs.get(field)
        if value not in (None, ""):
            return str(value)
    return ""
