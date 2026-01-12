from pathlib import Path

try:
    from fastapi import FastAPI
    from fastapi import Body
    from fastapi import HTTPException
    from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
    from fastapi.staticfiles import StaticFiles

    from florida_property_scraper.api.routes.search import router as search_router

    FASTAPI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    FastAPI = None
    Body = None
    HTTPException = None
    FileResponse = None
    JSONResponse = None
    StreamingResponse = None
    StaticFiles = None
    FASTAPI_AVAILABLE = False

import json
import os
import time

from florida_property_scraper.api.geojson import to_featurecollection
from florida_property_scraper.cache import cache_get, cache_set
from florida_property_scraper.feature_flags import get_flags
from florida_property_scraper.map_layer.registry import get_provider
from florida_property_scraper.parcels.geometry_provider import parse_bbox
from florida_property_scraper.parcels.geometry_registry import (
    get_provider as get_geometry_provider,
)
from florida_property_scraper.routers.registry import get_router
from florida_property_scraper.user_meta.storage import UserMetaSQLite, empty_user_meta


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


def stream_search(
    state="fl",
    county="broward",
    query="",
    backend="native",
    mode="fixture",
    max_items=None,
    per_county_limit=None,
    fixture_path=None,
):
    cache_key = (backend, state, county, query, max_items, per_county_limit, mode)
    use_cache = os.environ.get("CACHE", "1") != "0"
    use_cache_stream = os.environ.get("CACHE_STREAM", "0") == "1"
    if use_cache and use_cache_stream:
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
            if max_items and len(records) >= max_items:
                break
    summary["records"] = len(records)
    summary["seconds"] = round(time.perf_counter() - start, 6)
    if use_cache and use_cache_stream:
        cache_set(cache_key, {"records": records, "summary": summary})
    yield json.dumps({"summary": summary}) + "\n"


app = FastAPI() if FASTAPI_AVAILABLE else None


if app:
    app.include_router(search_router, prefix="/api")

    @app.get("/health")
    def health_route():
        return health()

    @app.get("/counties")
    def counties_route():
        return counties()

    @app.get("/search/stream")
    def search_stream(
        state: str = "fl",
        county: str = "broward",
        query: str = "",
        backend: str = "native",
    ):
        generator = stream_search(
            state=state, county=county, query=query, backend=backend
        )
        return StreamingResponse(generator, media_type="application/x-ndjson")

    @app.get("/parcels")
    def parcels(
        state: str = "fl", county: str = "broward", bbox: str = "", zoom: int = 12
    ):
        provider = get_provider(state, county)
        features = provider.fetch_features(
            bbox=bbox, zoom=zoom, state=state, county=county
        )
        return JSONResponse(to_featurecollection(features, county))

    @app.get("/parcels/{parcel_id}")
    def parcel(parcel_id: str, state: str = "fl", county: str = "broward"):
        provider = get_provider(state, county)
        feature = provider.fetch_feature(
            parcel_id=parcel_id, state=state, county=county
        )
        return JSONResponse(feature)

    @app.get("/api/parcels")
    def api_parcels(bbox: str = "", zoom: int = 12, county: str = ""):
        """Return parcel geometry as GeoJSON FeatureCollection.

        Design notes:
        - GeoJSON-by-bbox now, MVT/PostGIS later.
        - Zoom-gated: returns empty when zoom < 15.
        - In-memory cache keyed by rounded bbox.
        """

        if int(zoom) < 15:
            return JSONResponse({"type": "FeatureCollection", "features": []})
        if not bbox:
            return JSONResponse({"type": "FeatureCollection", "features": []})
        try:
            bbox_t = parse_bbox(bbox)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        county_key = (county or "").strip().lower() or "seminole"

        # Cache the bbox response briefly to avoid hammering providers.
        rb = tuple(round(x, 5) for x in bbox_t)
        cache_key = ("api:parcels", county_key, int(zoom), rb)
        cached = cache_get(cache_key)
        if cached is not None:
            return JSONResponse(cached)

        provider = get_geometry_provider(county_key)
        feats = provider.query(bbox_t)

        # Batch-load PA hover fields for the returned parcel_ids.
        from florida_property_scraper.pa.storage import PASQLite

        db_path = os.getenv("PA_DB", "./leads.sqlite")
        hover_by_parcel: dict[str, dict] = {}
        store = PASQLite(db_path)
        try:
            hover_by_parcel = store.get_hover_fields_many(
                county=county_key,
                parcel_ids=[f.parcel_id for f in feats],
            )
        finally:
            store.close()

        allowed_hover_keys = {
            "situs_address",
            "owner_name",
            "last_sale_date",
            "last_sale_price",
            "mortgage_amount",
        }

        features_out = []
        for f in feats:
            hover = hover_by_parcel.get(f.parcel_id) or {}
            props = {
                "parcel_id": f.parcel_id,
                # Hover whitelist only
                "situs_address": "",
                "owner_name": "",
                "last_sale_date": None,
                "last_sale_price": 0,
                # PA-only: unknown unless explicitly present in PA.
                "mortgage_amount": None,
            }
            for k in allowed_hover_keys:
                if k in hover:
                    props[k] = hover[k]

            features_out.append(
                {
                    "type": "Feature",
                    "id": f.feature_id,
                    "geometry": f.geometry,
                    "properties": props,
                }
            )

        fc = {"type": "FeatureCollection", "features": features_out}
        cache_set(cache_key, fc, ttl=30)
        return JSONResponse(fc)

    @app.post("/api/parcels/search")
    def api_parcels_search(payload: dict = Body(...)):
        """Search parcels by polygon geometry or radius.

        Input:
          {
            county,
            geometry?: <GeoJSON geometry>,
            radius?: {center:[lng,lat], miles:<float>},
            filters?: [{field, op, value}],
            triggers?: [{code, all:[{field, op, value}]}],
            limit?: <int>,
            include_geometry?: <bool>
          }

        PA-only: computed fields and hover fields are derived solely from PA storage.
        Missing fields are treated as unknown and do not match triggers.
        """

        flags = get_flags()
        if not flags.geometry_search:
            raise HTTPException(status_code=404, detail="geometry search is disabled")

        from florida_property_scraper.api.rules import (
            apply_filters,
            compile_filters,
            compile_triggers,
            eval_triggers,
        )
        from florida_property_scraper.parcels.geometry_search import (
            circle_polygon,
            geometry_bbox,
            intersects,
        )
        from florida_property_scraper.pa.storage import PASQLite
        from florida_property_scraper.pa.ui_computed import compute_ui_fields

        county_key = (payload.get("county") or "").strip().lower() or "seminole"
        include_geometry = bool(payload.get("include_geometry", False))
        limit = int(payload.get("limit", 200))
        if limit <= 0:
            limit = 200

        geometry = payload.get("geometry")
        radius = payload.get("radius")
        if geometry and radius:
            raise HTTPException(
                status_code=400, detail="Provide either geometry or radius, not both"
            )

        if radius is not None:
            if not isinstance(radius, dict):
                raise HTTPException(status_code=400, detail="radius must be an object")
            center = radius.get("center")
            miles = radius.get("miles")
            if (
                not isinstance(center, (list, tuple))
                or len(center) != 2
                or not isinstance(center[0], (int, float))
                or not isinstance(center[1], (int, float))
                or not isinstance(miles, (int, float))
            ):
                raise HTTPException(
                    status_code=400,
                    detail="radius must be {center:[lng,lat], miles:number}",
                )
            geometry = circle_polygon(
                center_lon=float(center[0]),
                center_lat=float(center[1]),
                miles=float(miles),
            )

        if not isinstance(geometry, dict):
            raise HTTPException(
                status_code=400, detail="geometry must be a GeoJSON geometry object"
            )
        bbox_t = geometry_bbox(geometry)
        if bbox_t is None:
            raise HTTPException(status_code=400, detail="geometry has no coordinates")

        provider = get_geometry_provider(county_key)
        candidates = provider.query(bbox_t)

        # Filter to true intersections when possible.
        intersecting = [f for f in candidates if intersects(geometry, f.geometry)]

        # Batch-load PA records + hover fields for evaluation.
        db_path = os.getenv("PA_DB", "./leads.sqlite")
        store = PASQLite(db_path)
        try:
            parcel_ids = [f.parcel_id for f in intersecting]
            pa_by_id = store.get_many(county=county_key, parcel_ids=parcel_ids)
            hover_by_id = store.get_hover_fields_many(
                county=county_key, parcel_ids=parcel_ids
            )
        finally:
            store.close()

        filters = compile_filters(payload.get("filters"))

        raw_triggers = payload.get("triggers") if flags.triggers else None
        triggers = compile_triggers(raw_triggers)

        sale_fields = {
            # PA hover fields
            "last_sale_date",
            "last_sale_price",
            # Scraper-derived fields (future)
            "sale_date",
            "sale_price",
            "deed_type",
        }

        results = []
        for feat in intersecting:
            pa = pa_by_id.get(feat.parcel_id)
            pa_dict = pa.to_dict() if pa is not None else None
            computed = compute_ui_fields(pa_dict)
            hover = hover_by_id.get(feat.parcel_id) or {
                "situs_address": "",
                "owner_name": "",
                "last_sale_date": None,
                "last_sale_price": 0,
                "mortgage_amount": None,
            }

            fields: dict[str, object] = {}
            if pa_dict:
                fields.update(pa_dict)
            fields.update(computed)
            fields.update(hover)

            # Optional safety valve: prevent sale-based filtering/triggering.
            if not flags.sale_filtering:
                for k in sale_fields:
                    fields.pop(k, None)

            if not apply_filters(fields, filters):
                continue

            reason_codes = eval_triggers(fields, triggers) if triggers else []
            if triggers and not reason_codes:
                continue

            row = {
                "county": county_key,
                "parcel_id": feat.parcel_id,
                "hover_fields": hover,
                "reason_codes": reason_codes,
            }
            if include_geometry:
                row["geometry"] = feat.geometry
            results.append(row)
            if len(results) >= limit:
                break

        return JSONResponse(
            {"county": county_key, "count": len(results), "results": results}
        )

    @app.get("/api/parcels/{parcel_id}")
    def api_parcel_detail(parcel_id: str, county: str = ""):
        """Return full PA normalized detail + user meta.

        PA-only: this endpoint never enriches outside PA.
        """

        from florida_property_scraper.pa.storage import PASQLite

        county_key = (county or "").strip().lower() or "seminole"
        parcel_key = str(parcel_id)
        db_path = os.getenv("PA_DB", "./leads.sqlite")

        pa_store = PASQLite(db_path)
        try:
            rec = pa_store.get(county=county_key, parcel_id=parcel_key)
        finally:
            pa_store.close()

        pa = rec.to_dict() if rec is not None else None

        user_db = os.getenv("USER_META_DB", db_path)
        meta_store = UserMetaSQLite(user_db)
        try:
            meta = meta_store.get(county=county_key, parcel_id=parcel_key)
        finally:
            meta_store.close()

        from florida_property_scraper.pa.ui_computed import compute_ui_fields

        computed = compute_ui_fields(pa)

        payload = {
            "county": county_key,
            "parcel_id": parcel_key,
            "pa": pa,
            "computed": computed,
            "user_meta": meta.to_dict()
            if meta is not None
            else empty_user_meta(county=county_key, parcel_id=parcel_key),
        }
        return JSONResponse(payload)

    @app.get("/api/parcels/{parcel_id}/meta")
    def api_parcel_meta_get(parcel_id: str, county: str = ""):
        county_key = (county or "").strip().lower() or "seminole"
        parcel_key = str(parcel_id)
        db_path = os.getenv("PA_DB", "./leads.sqlite")
        user_db = os.getenv("USER_META_DB", db_path)

        meta_store = UserMetaSQLite(user_db)
        try:
            meta = meta_store.get(county=county_key, parcel_id=parcel_key)
        finally:
            meta_store.close()
        return JSONResponse(
            meta.to_dict()
            if meta is not None
            else empty_user_meta(county=county_key, parcel_id=parcel_key)
        )

    @app.put("/api/parcels/{parcel_id}/meta")
    def api_parcel_meta_put(parcel_id: str, payload: dict, county: str = ""):
        county_key = (county or "").strip().lower() or "seminole"
        parcel_key = str(parcel_id)
        db_path = os.getenv("PA_DB", "./leads.sqlite")
        user_db = os.getenv("USER_META_DB", db_path)

        starred = bool(payload.get("starred", False))
        tags = payload.get("tags", [])
        notes = str(payload.get("notes", "") or "")
        lists_v = payload.get("lists", [])

        meta_store = UserMetaSQLite(user_db)
        try:
            meta = meta_store.upsert(
                county=county_key,
                parcel_id=parcel_key,
                starred=starred,
                tags=tags,
                notes=notes,
                lists=lists_v,
            )
        finally:
            meta_store.close()
        return JSONResponse(meta.to_dict())

    @app.get("/api/parcels/{county}/{parcel_id}/hover")
    def api_parcel_hover(county: str, parcel_id: str):
        """Return minimal PA-only hover fields.

        Mortgage fields are always blank/0 unless PA explicitly provides them.
        """

        from florida_property_scraper.pa.storage import PASQLite

        county_key = (county or "").strip().lower()
        parcel_key = str(parcel_id)
        db_path = os.getenv("PA_DB", "./leads.sqlite")

        cache_key = ("pa:hover", county_key, parcel_key)
        cached = cache_get(cache_key)
        if cached is not None:
            return JSONResponse(cached)

        store = PASQLite(db_path)
        try:
            rec = store.get(county=county_key, parcel_id=parcel_key)
        finally:
            store.close()

        owner_name = ""
        situs_address = ""
        last_sale_date = None
        last_sale_price = 0
        if rec is not None:
            situs_address = rec.situs_address or ""
            owner_name = "; ".join([n for n in (rec.owner_names or []) if n])
            last_sale_date = rec.last_sale_date
            last_sale_price = float(rec.last_sale_price or 0)

        payload = {
            "parcel_id": parcel_key,
            "county": county_key,
            "situs_address": situs_address,
            "owner_name": owner_name,
            "last_sale_date": last_sale_date,
            "last_sale_price": last_sale_price,
            # PA-only: unknown unless explicitly present in PA.
            "mortgage_amount": None,
            "mortgage_lender": "",
        }
        cache_set(cache_key, payload, ttl=30)
        return JSONResponse(payload)

    @app.get("/")
    def root():
        return {"status": "ok", "message": "API running"}

    if WEB_DIR.exists():
        app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
