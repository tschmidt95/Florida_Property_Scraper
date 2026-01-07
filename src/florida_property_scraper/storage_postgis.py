import os


class PostGISStorage:
    def __init__(self, dsn: str):
        self.dsn = dsn

    @classmethod
    def from_env(cls):
        return cls(os.getenv("POSTGIS_DSN", ""))

    def upsert_parcel_features(self, features):
        return None

    def query_bbox(self, bbox):
        return []

    def get_by_parcel_id(self, parcel_id):
        return None
