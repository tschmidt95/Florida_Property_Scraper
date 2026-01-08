from pathlib import Path

try:
    from fastapi import FastAPI
    from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
    from fastapi.staticfiles import StaticFiles
    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    FastAPI = None
    FileResponse = None
    JSONResponse = None
    StreamingResponse = None
    StaticFiles = None
    FASTAPI_AVAILABLE = False

import json
import time

from florida_property_scraper.api.geojson import to_featurecollection
from florida_property_scraper.cache import cache_get, cache_set
from florida_property_scraper.map_layer.registry import get_provider
from florida_property_scraper.routers.registry import get_router


ROOT = Path(__file__).resolve().parents[2]
WEB_DIR = ROOT / "web"
_router = get_router("fl")


def health():
    return {"status": "ok"}


def counties():
    return {"counties": list(_router.enabled_counties())}


def _find_fixture(county):
    candidates = [
        Path("tests/fixtures") / f"{county}_sample.html",
        Path("tests/fixtures") / f"{county}_realistic.html",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def stream_search(state="fl", county="broward", query="", backend="native", mode="fixture", max_items=None, per_county_limit=None, fixture_path=None):
    cache_key = (backend, state, county, query, mode)
    cached = cache_get(cache_key)
    if cached:
        for record in cached["records"]:
            yield json.dumps({"record": record}) + "\n"
        yield json.dumps({"summary": cached["summary"]}) + "\n"
        return
    records = []
    summary = {"records": 0, "seconds": 0.0}
    start = time.perf_counter()
    if backend == "native":
        from florida_property_scraper.backend.native_adapter import NativeAdapter

        adapter = NativeAdapter()
        start_urls = None
        dry_run = mode == "fixture"
        if mode == "fixture":
            fixture = fixture_path or _find_fixture(county)
            if fixture:
                start_urls = [f"file://{fixture.resolve()}"]
        stream = adapter.iter_records(
            query=query,
            start_urls=start_urls,
            spider_name=f"{county}_spider",
            max_items=max_items,
            per_county_limit=per_county_limit,
            live=(mode == "live"),
            county_slug=county,
            state=state,
            dry_run=dry_run,
        )
        for item in stream:
            if "__summary__" in item:
                summary.update(item["__summary__"])
                continue
            records.append(item)
            yield json.dumps({"record": item}) + "\n"
    summary["records"] = len(records)
    summary["seconds"] = round(time.perf_counter() - start, 6)
    cache_set(cache_key, {"records": records, "summary": summary})
    yield json.dumps({"summary": summary}) + "\n"


app = FastAPI() if FASTAPI_AVAILABLE else None


if app:
    @app.get("/health")
    def health_route():
        return health()

    @app.get("/counties")
    def counties_route():
        return counties()

    @app.get("/search/stream")
    def search_stream(state: str = "fl", county: str = "broward", query: str = "", backend: str = "native"):
        generator = stream_search(state=state, county=county, query=query, backend=backend)
        return StreamingResponse(generator, media_type="application/x-ndjson")

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
