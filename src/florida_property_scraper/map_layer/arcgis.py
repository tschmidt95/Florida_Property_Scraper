import json
from typing import List, Optional
from urllib.parse import urlencode
from urllib.request import urlopen


class ArcGISFeatureServerProvider:
    def __init__(self, endpoint: str, id_field: str, supports_geometry: bool = True):
        self.endpoint = endpoint.rstrip("/")
        self.id_field = id_field
        self.supports_geometry = supports_geometry

    def _request(self, params: dict) -> dict:
        query = urlencode(params)
        url = f"{self.endpoint}/query?{query}"
        with urlopen(url, timeout=10) as resp:
            payload = resp.read().decode("utf-8")
        return json.loads(payload)

    def fetch_features(
        self,
        bbox: str,
        zoom: int,
        state: str,
        county: str,
    ) -> List[dict]:
        params = {
            "f": "json",
            "where": "1=1",
            "geometry": bbox,
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outFields": "*",
            "outSR": "4326",
            "returnGeometry": "true" if self.supports_geometry else "false",
        }
        data = self._request(params)
        return data.get("features", [])

    def fetch_feature(
        self,
        parcel_id: str,
        state: str,
        county: str,
    ) -> Optional[dict]:
        params = {
            "f": "json",
            "where": f"{self.id_field}='{parcel_id}'",
            "outFields": "*",
            "returnGeometry": "true" if self.supports_geometry else "false",
        }
        data = self._request(params)
        features = data.get("features", [])
        return features[0] if features else None
