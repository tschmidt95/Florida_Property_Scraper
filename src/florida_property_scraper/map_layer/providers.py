from typing import List, Optional, Protocol


class Provider(Protocol):
    def fetch_features(
        self,
        bbox: str,
        zoom: int,
        state: str,
        county: str,
    ) -> List[dict]: ...

    def fetch_feature(
        self,
        parcel_id: str,
        state: str,
        county: str,
    ) -> Optional[dict]: ...
