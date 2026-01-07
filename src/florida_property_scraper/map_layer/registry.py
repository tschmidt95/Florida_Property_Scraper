import os

from florida_property_scraper.map_layer.arcgis import ArcGISFeatureServerProvider
from florida_property_scraper.map_layer.dev_provider import DevProvider
from florida_property_scraper.map_layer.providers import Provider
from florida_property_scraper.routers.registry import get_entry


class NullProvider:
    def fetch_features(self, bbox, zoom, state, county):
        return []

    def fetch_feature(self, parcel_id, state, county):
        return None


def get_provider(state: str, county: str) -> Provider:
    entry = get_entry(state, county)
    parcel_layer = entry.get("parcel_layer", {}) or {}
    layer_type = parcel_layer.get("type", "none")
    if os.getenv("MAP_PROVIDER") == "dev":
        return DevProvider()
    if layer_type == "arcgis":
        return ArcGISFeatureServerProvider(
            endpoint=parcel_layer.get("endpoint", ""),
            id_field=parcel_layer.get("id_field", "PARCEL_ID"),
            supports_geometry=bool(parcel_layer.get("supports_geometry", True)),
        )
    return DevProvider()
