from typing import List, Optional


def _geometry_from_feature(feature: dict) -> Optional[dict]:
    if "geometry" in feature and feature["geometry"]:
        return feature["geometry"]
    geometry = feature.get("geometry") or feature.get("geom")
    if geometry:
        return geometry
    lon = feature.get("lon") or feature.get("longitude")
    lat = feature.get("lat") or feature.get("latitude")
    if lon is None or lat is None:
        return None
    return {"type": "Point", "coordinates": [float(lon), float(lat)]}


def to_featurecollection(features: List[dict], county: str) -> dict:
    output = []
    for feature in features or []:
        geometry = _geometry_from_feature(feature)
        if not geometry:
            continue
        properties = feature.get("properties", {})
        parcel_id = (
            properties.get("parcel_id")
            or feature.get("parcel_id")
            or properties.get("PARCEL_ID")
            or feature.get("PARCEL_ID")
            or ""
        )
        address = (
            properties.get("address")
            or feature.get("address")
            or properties.get("SITE_ADDR")
            or ""
        )
        output.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "parcel_id": str(parcel_id),
                    "county": county,
                    "address": str(address),
                },
            }
        )
    return {"type": "FeatureCollection", "features": output}
