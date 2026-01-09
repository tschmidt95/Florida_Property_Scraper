import hashlib
from typing import List, Optional


class DevProvider:
    def fetch_features(
        self, bbox: str, zoom: int, state: str, county: str
    ) -> List[dict]:
        min_lon, min_lat, max_lon, max_lat = [float(v) for v in bbox.split(",")]
        seed = f"{state}:{county}:{bbox}"
        digest = hashlib.md5(seed.encode("utf-8")).hexdigest()
        features = []
        steps = 5
        idx = 0
        for x in range(steps):
            for y in range(steps):
                lon = min_lon + (max_lon - min_lon) * (x + 0.5) / steps
                lat = min_lat + (max_lat - min_lat) * (y + 0.5) / steps
                parcel_id = f"{county}-{digest[:8]}-{idx}"
                features.append(
                    {
                        "geometry": {"type": "Point", "coordinates": [lon, lat]},
                        "properties": {
                            "parcel_id": parcel_id,
                            "address": f"{100 + idx} Demo Rd",
                        },
                    }
                )
                idx += 1
        return features

    def fetch_feature(self, parcel_id: str, state: str, county: str) -> Optional[dict]:
        return {
            "geometry": {"type": "Point", "coordinates": [-81.5, 27.8]},
            "properties": {"parcel_id": parcel_id, "address": "Demo Address"},
        }
