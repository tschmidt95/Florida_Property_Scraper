import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from florida_property_scraper.api.geojson import to_featurecollection
from florida_property_scraper.map_layer.registry import get_provider
from florida_property_scraper.routers.registry import enabled_jurisdictions
from florida_property_scraper.storage_postgis import PostGISStorage


app = FastAPI()


def _get_storage():
    if os.getenv("POSTGIS_ENABLED") == "1":
        return PostGISStorage.from_env()
    return None


@app.get("/parcels")
def parcels(state: str, county: str, bbox: str, zoom: int):
    storage = _get_storage()
    if storage:
        features = storage.query_bbox(bbox)
    else:
        provider = get_provider(state, county)
        features = provider.fetch_features(bbox, zoom, state, county)
    return to_featurecollection(features, county)


@app.get("/parcels/{parcel_id}")
def parcel(parcel_id: str, state: str, county: str):
    storage = _get_storage()
    if storage:
        feature = storage.get_by_parcel_id(parcel_id)
    else:
        provider = get_provider(state, county)
        feature = provider.fetch_feature(parcel_id, state, county)
    if not feature:
        raise HTTPException(status_code=404, detail="Parcel not found")
    return feature


@app.get("/counties")
def counties(state: str = "fl"):
    return enabled_jurisdictions(state)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def index():
    base = Path(__file__).resolve().parents[1] / "web" / "map.html"
    return FileResponse(base)


@app.get("/static/{path:path}")
def static_assets(path: str):
    base = Path(__file__).resolve().parents[1] / "web"
    return FileResponse(base / path)
