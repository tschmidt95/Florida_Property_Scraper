from pathlib import Path

try:
    from fastapi import FastAPI
    from fastapi.responses import FileResponse, JSONResponse
    from fastapi.staticfiles import StaticFiles
    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    FastAPI = None
    FileResponse = None
    JSONResponse = None
    StaticFiles = None
    FASTAPI_AVAILABLE = False

from florida_property_scraper.api.geojson import to_featurecollection
from florida_property_scraper.map_layer.registry import get_provider
from florida_property_scraper.routers.registry import get_router


ROOT = Path(__file__).resolve().parents[2]
WEB_DIR = ROOT / "web"
_router = get_router("fl")


def health():
    return {"status": "ok"}


def counties():
    return {"counties": list(_router.enabled_counties())}


app = FastAPI() if FASTAPI_AVAILABLE else None


if app:
    @app.get("/health")
    def health_route():
        return health()

    @app.get("/counties")
    def counties_route():
        return counties()

    @app.get("/parcels")
    def parcels(state: str = "fl", county: str = "broward", bbox: str = "", zoom: int = 12):
        provider = get_provider(state, county)
        features = provider.fetch_features(bbox=bbox, zoom=zoom, state=state, county=county)
        return JSONResponse(to_featurecollection(features, county))

    @app.get("/parcels/{parcel_id}")
    def parcel(parcel_id: str, state: str = "fl", county: str = "broward"):
        provider = get_provider(state, county)
        feature = provider.fetch_feature(parcel_id=parcel_id, state=state, county=county)
        return JSONResponse(feature)

    @app.get("/")
    def index():
        return FileResponse(WEB_DIR / "map.html")

    if WEB_DIR.exists():
        app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
