from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import json
import logging
import os
import re
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any

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


REPO_ROOT = Path(__file__).resolve().parents[3]
WEB_DIST = REPO_ROOT / "web" / "dist"
_router = get_router("fl")
assert _router is not None


class ParcelsGeometryRequest(BaseModel):
    county: str
    parcel_ids: list[str]


def health():
    return {"status": "ok"}


def counties():
    assert _router is not None
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


app = FastAPI()


if app:
    from florida_property_scraper.api.routes.search import router as search_router
    from florida_property_scraper.api.routes.permits import router as permits_router
    from florida_property_scraper.api.routes.lookup import router as lookup_router
    from florida_property_scraper.api.routes.triggers import router as triggers_router
    from florida_property_scraper.api.routes.watchlists import router as watchlists_router

    assert search_router is not None
    assert permits_router is not None
    assert lookup_router is not None
    assert triggers_router is not None
    assert watchlists_router is not None

    app.include_router(search_router, prefix="/api")
    app.include_router(permits_router, prefix="/api")
    app.include_router(lookup_router, prefix="/api")
    app.include_router(triggers_router, prefix="/api")
    app.include_router(watchlists_router, prefix="/api")

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

    @app.post("/api/parcels/geometry")
    def api_parcels_geometry(payload: ParcelsGeometryRequest = Body(...)):
        """Return parcel boundary geometry for a set of parcel_ids.

        Input:
          { county, parcel_ids: [...] }

        Output:
          GeoJSON FeatureCollection with properties:
            {parcel_id, county, situs_address, owner_name}

        Notes:
        - Best-effort; counties without support return 404.
        - Never loads full-county geometry; only requested parcel_ids.
        """

        county_key = (payload.county or "").strip().lower()
        parcel_ids = [str(pid).strip() for pid in (payload.parcel_ids or []) if str(pid).strip()]
        parcel_ids = parcel_ids[:50]

        if not parcel_ids:
            return JSONResponse({"type": "FeatureCollection", "features": []})

        # Orange first (expand later).
        if county_key not in {"orange"}:
            raise HTTPException(
                status_code=404,
                detail="Parcel geometry not available for this county yet",
            )

        from florida_property_scraper.parcels.live.fdor_parcel_polygons import FDORParcelPolygonClient
        from florida_property_scraper.pa.storage import PASQLite

        client = FDORParcelPolygonClient()
        geom_by_id = client.fetch_parcel_geometries(parcel_ids)

        db_path = os.getenv("PA_DB", "./leads.sqlite")
        hover_by_id: dict[str, dict] = {}
        store = PASQLite(db_path)
        try:
            hover_by_id = store.get_hover_fields_many(county=county_key, parcel_ids=parcel_ids)
        finally:
            store.close()

        features_out: list[dict] = []
        for pid in parcel_ids:
            geom = geom_by_id.get(pid)
            if not geom:
                continue
            hover = hover_by_id.get(pid) or {}
            props = {
                "parcel_id": pid,
                "county": county_key,
                "situs_address": str(hover.get("situs_address") or ""),
                "owner_name": str(hover.get("owner_name") or ""),
            }
            features_out.append({"type": "Feature", "geometry": geom, "properties": props})

        return JSONResponse({"type": "FeatureCollection", "features": features_out})

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

        search_id = uuid.uuid4().hex[:12]

        debug_response_enabled = payload.get("debug") is True
        debug_timing_ms: dict[str, int] | None = None
        debug_counts: dict[str, Any] | None = None
        _timing_mark = None
        _timing_last = None
        if debug_response_enabled:
            try:
                import time as _time

                debug_timing_ms = {}
                debug_counts = {}
                _timing_mark = _time.perf_counter()
                _timing_last = _timing_mark

                def _mark(stage: str) -> None:
                    nonlocal _timing_last
                    if debug_timing_ms is None or _timing_last is None:
                        return
                    now = _time.perf_counter()
                    debug_timing_ms[str(stage)] = int(round((now - _timing_last) * 1000.0))
                    _timing_last = now
            except Exception:
                debug_timing_ms = None
                debug_counts = None
                _timing_mark = None
                _timing_last = None

                def _mark(stage: str) -> None:
                    return
        else:

            def _mark(stage: str) -> None:
                return

        pre_warnings: list[str] = []

        def _parse_date_any(v: object) -> date | None:
            if v is None:
                return None
            if isinstance(v, date) and not isinstance(v, datetime):
                return v
            if isinstance(v, datetime):
                try:
                    return v.date()
                except Exception:
                    return None
            if not isinstance(v, str):
                return None
            s = v.strip()
            if not s:
                return None
            if "T" in s:
                try:
                    return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
                except Exception:
                    return None
            try:
                return date.fromisoformat(s)
            except Exception:
                pass
            import re as _re

            m = _re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
            if m:
                try:
                    mm = int(m.group(1))
                    dd = int(m.group(2))
                    yy = int(m.group(3))
                    return date(yy, mm, dd)
                except Exception:
                    return None
            return None

        def _normalize_last_sale_date_range(filters_obj: object) -> tuple[object, bool]:
            if not isinstance(filters_obj, dict):
                return filters_obj, False

            if "last_sale_date_start" not in filters_obj and "last_sale_date_end" not in filters_obj:
                return filters_obj, False

            d0_raw = filters_obj.get("last_sale_date_start")
            d1_raw = filters_obj.get("last_sale_date_end")
            d0 = _parse_date_any(d0_raw)
            d1 = _parse_date_any(d1_raw)

            swapped = False
            if d0 is not None and d1 is not None and d0 > d1:
                swapped = True
                d0, d1 = d1, d0

            out = dict(filters_obj)
            if "last_sale_date_start" in out:
                if d0 is not None:
                    out["last_sale_date_start"] = d0.isoformat()
                else:
                    out.pop("last_sale_date_start", None)
            if "last_sale_date_end" in out:
                if d1 is not None:
                    out["last_sale_date_end"] = d1.isoformat()
                else:
                    out.pop("last_sale_date_end", None)

            if out == filters_obj:
                return filters_obj, swapped
            return out, swapped

        raw_filters0 = payload.get("filters")
        raw_filters_norm, swapped_last_sale_range = _normalize_last_sale_date_range(raw_filters0)
        if swapped_last_sale_range:
            pre_warnings.append("Swapped date range")
        if raw_filters_norm is not raw_filters0:
            payload["filters"] = raw_filters_norm

        # If normalization removed everything, drop filters entirely.
        if isinstance(payload.get("filters"), dict) and not payload.get("filters"):
            payload.pop("filters", None)

        normalized_filters: dict[str, Any] | None = None
        if debug_response_enabled:
            nf: dict[str, Any] = {}
            if isinstance(payload.get("filters"), dict):
                for k, v in (payload.get("filters") or {}).items():
                    if v is None:
                        continue
                    if isinstance(v, str) and not v.strip():
                        continue
                    if isinstance(v, list) and len(v) == 0:
                        continue
                    nf[str(k)] = v
            normalized_filters = {
                "filters": nf,
                "swapped_last_sale_date_range": bool(swapped_last_sale_range),
            }

        _mark("normalize_filters")

        debug_enabled = bool(payload.get("debug", False)) or (
            str(os.getenv("FPS_SEARCH_DEBUG", "")).strip().lower() in {"1", "true", "yes"}
        )

        def _append_search_debug(event: dict) -> None:
            """Append a single JSON event line to a local debug log.

            Default behavior: log to stdout (uvicorn logs) to avoid leaving untracked
            debug artifacts in the repo.

            Optional: set FPS_SEARCH_DEBUG_LOG to a filepath to also append JSONL.
            """

            if not debug_enabled:
                return

            try:
                import logging

                logger = logging.getLogger("fps.search")
                log_path = str(os.getenv("FPS_SEARCH_DEBUG_LOG", "") or "").strip()
                event_out = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "search_id": search_id,
                    **(event or {}),
                }
                line = json.dumps(event_out, ensure_ascii=False, default=str)
                logger.info(line)

                if log_path:
                    with open(log_path, "a", encoding="utf-8") as f:
                        f.write(line + "\n")
            except Exception:
                return

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
        if "live" in payload:
            live = bool(payload.get("live", False))
        else:
            live = (
                os.getenv("FPS_USE_FDOR_CENTROIDS", "").strip() in {"1", "true", "True"}
                and county_key in {"orange", "seminole"}
            )
        include_geometry = bool(payload.get("include_geometry", False))
        limit = int(payload.get("limit", 200))
        if limit <= 0:
            limit = 200

        # Guardrail: never allow unbounded result sets.
        if limit > 250:
            limit = 250

        # Guardrail: live mode can be expensive if/when implemented.
        if live and limit > 250:
            limit = 250

        _mark("parse_payload")

        if debug_counts is not None:
            debug_counts.update(
                {
                    "county": county_key,
                    "live": bool(live),
                    "limit": int(limit),
                    "include_geometry": bool(include_geometry),
                    "has_filters": isinstance(payload.get("filters"), dict) and bool(payload.get("filters")),
                    "enrich": payload.get("enrich", None),
                    "enrich_limit": payload.get("enrich_limit", None),
                }
            )

        # Accept multiple input shapes.
        # - geometry: GeoJSON geometry
        # - polygon_geojson: GeoJSON Polygon geometry
        # - polygon: legacy alias
        geometry = payload.get("geometry")
        if geometry is None:
            geometry = payload.get("polygon_geojson")
        if geometry is None:
            geometry = payload.get("polygon")
        radius = payload.get("radius")
        radius_m = payload.get("radius_m")
        center_obj = payload.get("center")

        _mark("parse_geometry")

        if geometry and radius:
            raise HTTPException(
                status_code=400, detail="Provide either geometry or radius, not both"
            )

        if geometry and (radius_m is not None or center_obj is not None):
            raise HTTPException(
                status_code=400, detail="Provide either geometry or radius, not both"
            )

        if radius_m is not None or center_obj is not None:
            # Newer shape: {center:{lat,lng}, radius_m:number}
            if not isinstance(center_obj, dict) or not isinstance(radius_m, (int, float)):
                raise HTTPException(
                    status_code=400,
                    detail="radius search must be {center:{lat,lng}, radius_m:number}",
                )
            lat = center_obj.get("lat")
            lng = center_obj.get("lng")
            if not isinstance(lat, (int, float)) or not isinstance(lng, (int, float)):
                raise HTTPException(
                    status_code=400,
                    detail="center must be {lat:number, lng:number}",
                )
            miles = float(radius_m) / 1609.344
            geometry = circle_polygon(center_lon=float(lng), center_lat=float(lat), miles=miles)

        elif radius is not None:
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

        if geometry is None:
            raise HTTPException(status_code=400, detail="Draw polygon or radius first")

        if not isinstance(geometry, dict):
            raise HTTPException(
                status_code=400, detail="geometry must be a GeoJSON geometry object"
            )
        bbox_t = geometry_bbox(geometry)
        if bbox_t is None:
            raise HTTPException(status_code=400, detail="geometry has no coordinates")


        # --- BEGIN PATCH: Use parcels.sqlite + RTree for polygon search ---
        import sqlite3
        from shapely.geometry import shape
        import json as _json
        from pathlib import Path
        from types import SimpleNamespace

        intersecting = []
        provider_warnings = []
        candidates = []
        if county_key == "seminole" and geometry:
            db_path = str(Path(__file__).resolve().parents[3] / "data" / "parcels" / "parcels.sqlite")
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                minx, miny, maxx, maxy = bbox_t
                q = """
                    SELECT p.parcel_id, p.geom_geojson
                    FROM parcels_rtree r
                    JOIN parcels p ON p.rowid = r.rowid
                    WHERE r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ? AND p.county = ?
                """
                rows = conn.execute(q, (maxx, minx, maxy, miny, county_key)).fetchall()
                poly = shape(geometry) if isinstance(geometry, dict) else geometry
                for row in rows:
                    try:
                        parcel_geom = _json.loads(row["geom_geojson"])
                        parcel_shape = shape(parcel_geom)
                        if poly.intersects(parcel_shape):
                            intersecting.append(SimpleNamespace(parcel_id=row["parcel_id"], geometry=parcel_shape))
                    except Exception:
                        continue
                conn.close()
            except Exception as e:
                provider_warnings.append(f"parcels_sqlite_error:{e}")
        else:
            # fallback to original provider logic for other counties
            provider = get_geometry_provider(county_key)
            provider_is_live = provider.__class__.__name__ == "FDORCentroidsProvider"
            fdor_enabled = os.getenv("FPS_USE_FDOR_CENTROIDS", "").strip() in {
                "1",
                "true",
                "True",
            }
            try:
                candidates = provider.query(bbox_t)
            except Exception as e:
                candidates = []
                try:
                    provider_warnings.append(f"geometry_provider_error:{type(e).__name__}")
                except Exception:
                    provider_warnings.append("geometry_provider_error")
                if provider_is_live and county_key in {"orange", "seminole"}:
                    try:
                        from florida_property_scraper.parcels.geometry_registry import _default_geojson_dir
                        from florida_property_scraper.parcels.providers.orange import OrangeProvider
                        from florida_property_scraper.parcels.providers.seminole import SeminoleProvider
                        geo_dir = _default_geojson_dir()
                        if county_key == "orange":
                            fallback = OrangeProvider(geojson_path=geo_dir / "orange.geojson")
                        else:
                            fallback = SeminoleProvider(geojson_path=geo_dir / "seminole.geojson")
                        fallback.load()
                        candidates = fallback.query(bbox_t)
                        provider_warnings.append("geometry_provider_fallback:local_geojson")
                    except Exception:
                        pass
            _mark("candidate_query")
            # Filter to true intersections when possible.
            intersecting = [f for f in candidates if intersects(geometry, f.geometry)]
        # --- END PATCH ---

        _mark("geometry_filter")

        _append_search_debug(
            {
                "event": "request",
                "county": county_key,
                "payload": payload,
                "bbox": bbox_t,
                "candidates_count": len(candidates),
                "intersecting_count": len(intersecting),
            }
        )

        warnings: list[str] = []
        if provider_warnings:
            warnings.extend(provider_warnings)
        if pre_warnings:
            warnings.extend(pre_warnings)
        if not candidates:
            warnings.append("No parcel candidates returned for bbox")
        if candidates and not intersecting:
            warnings.append("No parcels intersected the drawn geometry")

        def _centroid_lat_lng(geom: Any) -> tuple[float, float]:
            # Best-effort centroid using bbox center; avoids heavy deps.
            try:
                bbox = geometry_bbox(geom)
                if bbox is not None:
                    minx, miny, maxx, maxy = bbox
                    lng = (float(minx) + float(maxx)) / 2.0
                    lat = (float(miny) + float(maxy)) / 2.0
                    return lat, lng
            except Exception:
                pass
            return 0.0, 0.0

        # Batch-load PA records + hover fields for evaluation.
        # If live=true, best-effort enrich missing parcel_ids into PA before continuing.
        db_path = os.getenv("PA_DB", "./leads.sqlite")
        store = PASQLite(db_path)
        enriched_live_ids: set[str] = set()
        live_error_reason: str | None = None
        zoning_options: list[str] = []
        future_land_use_options: list[str] = []
        field_stats: dict[str, Any] = {
            "scanned": 0,
            "present": {
                "living_area_sqft": 0,
                "lot_size_sqft": 0,
                "lot_size_acres": 0,
                "zoning": 0,
                "future_land_use": 0,
            },
        }
        try:
            # Optional pre-filter: restrict to a known parcel_id allow-list.
            # This is used by trigger rollup filters (separate endpoint precomputes IDs).
            allowed_ids: set[str] | None = None
            raw_allow = payload.get("parcel_id_in")
            if isinstance(raw_allow, list) and raw_allow:
                allowed = [str(x or "").strip() for x in raw_allow]
                allowed = [x for x in allowed if x]
                if allowed:
                    allowed_ids = set(allowed[:2000])
                    intersecting = [f for f in intersecting if f.parcel_id in allowed_ids]

            parcel_ids = [f.parcel_id for f in intersecting]

            if debug_counts is not None:
                debug_counts["parcel_id_in_count"] = int(len(allowed_ids) if allowed_ids is not None else 0)

            def _norm_choice(v: object) -> str:
                s = str(v or "").strip()
                if not s:
                    return "UNKNOWN"
                return " ".join(s.upper().split())

            # Optional: SQL-side filtering against cached columns.
            # Only applies when the request is not asking us to enrich missing data.
            # If enrich=true, we need to consider parcels not yet cached.
            raw_filters = payload.get("filters")
            if not isinstance(raw_filters, dict):
                raw_filters = {}
            explicit_enrich = payload.get("enrich", None)
            enrich_requested = bool(explicit_enrich) if explicit_enrich is not None else False

            enrich_disabled_by_candidate_cap = False
            try:
                if enrich_requested and len(intersecting) > 1500:
                    enrich_requested = False
                    enrich_disabled_by_candidate_cap = True
                    warnings.append("enrich_disabled_candidate_cap")
            except Exception:
                enrich_disabled_by_candidate_cap = False

            # Baseline (unfiltered) option lists must be computed from the polygon/radius
            # candidates, regardless of any attribute filters.
            baseline_parcel_ids = list(parcel_ids)

            # Load cached rows up front so we can determine which live IDs are missing.
            pa_by_id = store.get_many(county=county_key, parcel_ids=parcel_ids)

            # If any attribute filters are present and the UI did not explicitly
            # disable enrichment, enable best-effort enrichment.
            compiled_filters: list[Any] = []
            try:
                compiled_filters = compile_filters(raw_filters)
                filters_present = len(compiled_filters) > 0
            except Exception:
                compiled_filters = []
                filters_present = False

            # STRICT attribute filtering mode:
            # - When the user supplies any attribute filters (sqft/acres/beds/baths/year/zoning/FLU/etc),
            #   missing values MUST fail the filter.
            # - Soft-missing is only allowed for polygon-only browsing (no attribute filters).
            strict_attribute_filters = bool(filters_present)

            if explicit_enrich is None and filters_present:
                enrich_requested = True

            try:
                if enrich_requested and len(intersecting) > 1500:
                    enrich_requested = False
                    enrich_disabled_by_candidate_cap = True
                    if "enrich_disabled_candidate_cap" not in warnings:
                        warnings.append("enrich_disabled_candidate_cap")
            except Exception:
                pass

            if live:
                from florida_property_scraper.pa.schema import PAProperty

                def _merge_sources(
                    existing: list[dict] | None,
                    add: list[dict],
                ) -> list[dict]:
                    out: list[dict] = []
                    seen: set[tuple[str, str]] = set()
                    for src in (existing or []) + (add or []):
                        if not isinstance(src, dict):
                            continue
                        name = str(src.get("name") or "").strip()
                        url = str(src.get("url") or "").strip()
                        if not url:
                            continue
                        key = (name, url)
                        if key in seen:
                            continue
                        seen.add(key)
                        out.append({"name": name, "url": url})
                    return out

                def _as_float(v: object) -> float:
                    import re

                    if v is None:
                        return 0.0
                    if isinstance(v, (int, float)):
                        return float(v)
                    if not isinstance(v, str):
                        return 0.0
                    s = v.strip()
                    if not s:
                        return 0.0
                    s = s.replace(",", "")
                    m = re.search(r"[-+]?\d*\.?\d+", s)
                    if not m:
                        return 0.0
                    try:
                        return float(m.group(0))
                    except Exception:
                        return 0.0

                def _as_int(v: object) -> int:
                    return int(round(_as_float(v)))

                missing_ids = [pid for pid in parcel_ids if pid not in pa_by_id]

                # Guardrail: when FDOR is enabled and the geometry provider is FDOR,
                # do NOT fall back to demo data. Treat the request county as a hint
                # only; bbox+geometry is authoritative.
                if provider_is_live and fdor_enabled:
                    enrich_ids = missing_ids[: min(limit, 250)]
                else:
                    live_cap = int(os.getenv("LIVE_PARCEL_ENRICH_LIMIT", "40"))
                    if live_cap < 0:
                        live_cap = 0
                    enrich_ids = missing_ids[: min(live_cap, limit)]
                if enrich_ids:
                    # Preferred live path for Orange/Seminole: FDOR statewide centroids.
                    if (
                        fdor_enabled and (county_key in {"orange", "seminole"} or provider_is_live)
                    ):
                        from florida_property_scraper.parcels.live.fdor_centroids import (
                            FDORCentroidClient,
                        )

                        client = FDORCentroidClient()
                        try:
                            rows = client.fetch_parcels(
                                enrich_ids,
                                include_geometry=True,
                            )
                            for pid, row in rows.items():
                                pa_rec = PAProperty(
                                    county=county_key,
                                    parcel_id=str(pid),
                                    situs_address=row.situs_address or "",
                                    owner_names=[row.owner_name] if row.owner_name else [],
                                    land_use_code=row.land_use_code or "",
                                    use_type=row.land_use_code or "",
                                    land_sf=float(row.land_sqft or 0),
                                    year_built=int(row.year_built or 0),
                                    last_sale_date=row.last_sale_date,
                                    last_sale_price=float(row.last_sale_price or 0),
                                    zip=row.situs_zip or "",
                                    latitude=row.lat,
                                    longitude=row.lon,
                                    source_url=row.raw_source_url,
                                    parser_version="fdor_centroids:v1",
                                    sources=[
                                        {
                                            "name": "fdor_centroids",
                                            "url": row.raw_source_url,
                                        }
                                    ],
                                )
                                try:
                                    store.upsert(pa_rec)
                                    enriched_live_ids.add(str(pid))
                                except Exception:
                                    continue
                        except Exception as e:
                            # In FDOR live mode, do not silently return demo rows.
                            if provider_is_live and fdor_enabled:
                                live_error_reason = f"fdor_fetch_failed: {e}"

                        # Refresh after best-effort enrichment.
                        pa_by_id = store.get_many(
                            county=county_key, parcel_ids=parcel_ids
                        )
                    else:
                        import re

                        from florida_property_scraper.backend.native_adapter import (
                            NativeAdapter,
                        )

                        adapter = NativeAdapter()
                        for pid in enrich_ids:
                            try:
                                items = adapter.search(
                                    pid,
                                    live=True,
                                    county_slug=county_key,
                                    state="fl",
                                    max_items=1,
                                )
                            except Exception:
                                continue

                            if not items:
                                continue

                            item = items[0] if isinstance(items[0], dict) else {}
                            owner = str(item.get("owner") or "").strip()
                            address = str(item.get("address") or "").strip()
                            zoning_v = str(item.get("zoning") or "").strip()
                            property_class_v = str(item.get("property_class") or "").strip()

                            pa_rec = PAProperty(
                                county=county_key,
                                parcel_id=str(pid),
                                situs_address=address,
                                owner_names=[owner] if owner else [],
                                zoning=zoning_v,
                                property_class=property_class_v,
                                land_sf=_as_float(item.get("land_size")),
                                building_sf=_as_float(item.get("building_size")),
                                living_sf=_as_float(item.get("building_size")),
                                bedrooms=_as_int(item.get("bedrooms")),
                                bathrooms=_as_float(item.get("bathrooms")),
                                land_use_code=str(item.get("land_use_code") or "").strip(),
                                use_type=str(item.get("use_type") or "").strip(),
                                zip=str(item.get("zip") or "").strip(),
                                year_built=_as_int(item.get("year_built")),
                                source_url=str(item.get("source_url") or "").strip(),
                                extracted_at=str(item.get("extracted_at") or "").strip(),
                                parser_version=str(item.get("parser_version") or "").strip(),
                                sources=(
                                    [
                                        {
                                            "name": str(item.get("parser_version") or "pa_source"),
                                            "url": str(item.get("source_url") or "").strip(),
                                        }
                                    ]
                                    if str(item.get("source_url") or "").strip()
                                    else []
                                ),
                            )
                            try:
                                store.upsert(pa_rec)
                                enriched_live_ids.add(str(pid))
                            except Exception:
                                continue

                        # Refresh after best-effort enrichment.
                        pa_by_id = store.get_many(
                            county=county_key, parcel_ids=parcel_ids
                        )

                # Optional: inline enrichment via OCPA.
                # This can be slow / blocked and has caused long-hanging search requests.
                # Default to OFF unless filters require OCPA-only fields (ex: living area).
                inline_ocpa_pref = payload.get("inline_ocpa", None)
                inline_ocpa_enabled = False
                try:
                    disable_inline = os.getenv("FPS_DISABLE_INLINE_OCPA", "").strip() in {
                        "1",
                        "true",
                        "True",
                    }
                except Exception:
                    disable_inline = False

                ocpa_sensitive_fields = {
                    # Not available from FDOR centroids.
                    "living_area_sqft",
                    "beds",
                    "baths",
                    "zoning",
                    "zoning_norm",
                    "future_land_use_norm",
                    "total_value",
                    "land_value",
                    "building_value",
                }
                needs_ocpa_fields = any(
                    getattr(c, "field", None) in ocpa_sensitive_fields
                    for c in (compiled_filters or [])
                )

                try:
                    if inline_ocpa_pref is not None:
                        inline_ocpa_enabled = bool(inline_ocpa_pref)
                    else:
                        inline_ocpa_enabled = (
                            os.getenv("FPS_INLINE_OCPA", "").strip() in {"1", "true", "True"}
                        ) or (bool(enrich_requested) and bool(needs_ocpa_fields))
                except Exception:
                    inline_ocpa_enabled = False

                if disable_inline:
                    inline_ocpa_enabled = False

                inline_enrich = bool(enrich_requested) and bool(inline_ocpa_enabled)
                if inline_enrich and county_key == "orange" and fdor_enabled:
                    inline_cap = int(payload.get("enrich_limit", 5) or 0)
                    if inline_cap < 0:
                        inline_cap = 0
                    # When the caller didn't explicitly request inline OCPA but filters
                    # require it (e.g. living area), raise the cap to improve chances
                    # of returning non-empty filtered results.
                    if inline_ocpa_pref is None and needs_ocpa_fields:
                        try:
                            inline_cap = max(
                                inline_cap,
                                int(
                                    min(
                                        max(limit * (6 if strict_attribute_filters else 4), 50),
                                        250,
                                    )
                                ),
                            )
                        except Exception:
                            inline_cap = max(inline_cap, 50)
                    inline_cap = min(inline_cap, 250)

                    # Guardrail: inline enrichment can be slow (OCPA HTML + rate limiting).
                    # Never let it block the search request indefinitely.
                    try:
                        import time as _time

                        # In strict mode we prefer correctness over speed.
                        # OCPA HTML fetches are slow enough that 60s can be too tight to
                        # find any strict matches in a large polygon.
                        default_budget = "120" if strict_attribute_filters else "20"
                        inline_budget_s = float(
                            os.getenv("INLINE_ENRICH_BUDGET_S", default_budget) or default_budget
                        )
                    except Exception:
                        inline_budget_s = 120.0 if strict_attribute_filters else 20.0
                    inline_deadline = None
                    if inline_cap > 0 and inline_budget_s > 0:
                        try:
                            inline_deadline = _time.time() + float(inline_budget_s)
                        except Exception:
                            inline_deadline = None

                    # Prefer enriching candidates that have a chance to match
                    # cheap/non-OCPA filters (notably lot size), so we don't waste
                    # OCPA requests on obviously-ineligible parcels.
                    eligible_ids: list[str] = [pid for pid in parcel_ids if pid]

                    def _pa_lot_sqft(pid: str) -> float:
                        pa_tmp = pa_by_id.get(pid)
                        if pa_tmp is None:
                            return 0.0
                        try:
                            land_sf = float(getattr(pa_tmp, "land_sf", 0) or 0)
                        except Exception:
                            land_sf = 0.0
                        try:
                            land_acres = float(getattr(pa_tmp, "land_acres", 0) or 0)
                        except Exception:
                            land_acres = 0.0
                        if land_sf <= 0 and land_acres > 0:
                            land_sf = land_acres * 43560.0
                        return float(land_sf or 0)

                    if isinstance(raw_filters, dict):
                        min_lot_sqft_v: float | None = None
                        max_lot_sqft_v: float | None = None

                        try:
                            v = raw_filters.get("min_lot_size_sqft")
                            if v is not None:
                                min_lot_sqft_v = float(_as_float(v))
                        except Exception:
                            min_lot_sqft_v = None
                        try:
                            v = raw_filters.get("max_lot_size_sqft")
                            if v is not None:
                                max_lot_sqft_v = float(_as_float(v))
                        except Exception:
                            max_lot_sqft_v = None

                        # UI shorthand: acres.
                        if min_lot_sqft_v is None:
                            try:
                                v = raw_filters.get("min_acres")
                                if v is not None:
                                    a = float(_as_float(v))
                                    if a > 0:
                                        min_lot_sqft_v = a * 43560.0
                            except Exception:
                                pass
                        if max_lot_sqft_v is None:
                            try:
                                v = raw_filters.get("max_acres")
                                if v is not None:
                                    a = float(_as_float(v))
                                    if a > 0:
                                        max_lot_sqft_v = a * 43560.0
                            except Exception:
                                pass

                        # Legacy unit+value.
                        if min_lot_sqft_v is None and max_lot_sqft_v is None:
                            try:
                                unit = str(raw_filters.get("lot_size_unit") or "").strip().lower()
                                min_lot = raw_filters.get("min_lot_size")
                                max_lot = raw_filters.get("max_lot_size")
                                if unit == "acres":
                                    if min_lot is not None:
                                        min_lot_sqft_v = float(_as_float(min_lot)) * 43560.0
                                    if max_lot is not None:
                                        max_lot_sqft_v = float(_as_float(max_lot)) * 43560.0
                                elif unit:
                                    if min_lot is not None:
                                        min_lot_sqft_v = float(_as_float(min_lot))
                                    if max_lot is not None:
                                        max_lot_sqft_v = float(_as_float(max_lot))
                            except Exception:
                                pass

                        # NOTE: do NOT pre-filter eligible_ids here.
                        # Filtering semantics are centralized in `compile_filters` + `apply_filters`
                        # against normalized `fields` (to avoid divergent behavior between stages).

                        # Heuristic ordering: when sqft filters are present, prioritize parcels
                        # that look like improved residential (year_built present + lower land_use_code).
                        try:
                            wants_sqft = False
                            try:
                                wants_sqft = bool(_as_float(raw_filters.get("min_sqft")) or _as_float(raw_filters.get("max_sqft")))
                            except Exception:
                                wants_sqft = False

                            if wants_sqft and eligible_ids:
                                def _sort_key(pid: str) -> tuple[int, int, int]:
                                    pa_tmp = pa_by_id.get(pid)
                                    yb = 0
                                    try:
                                        yb = int(getattr(pa_tmp, "year_built", 0) or 0)
                                    except Exception:
                                        yb = 0
                                    luc = 9999
                                    try:
                                        s = str(getattr(pa_tmp, "land_use_code", "") or "").strip()
                                        if s.isdigit():
                                            luc = int(s)
                                    except Exception:
                                        luc = 9999
                                    # year_built=0 tends to be vacant/unknown; push it later.
                                    has_yb = 1 if yb > 0 else 0
                                    return (0 if luc < 100 else 1, 0 if has_yb else 1, luc)

                                eligible_ids = sorted(eligible_ids, key=_sort_key)
                        except Exception:
                            pass

                    ids_to_enrich: list[str] = []
                    for pid in eligible_ids:
                        existing = pa_by_id.get(pid)
                        if existing is None:
                            ids_to_enrich.append(pid)
                            continue
                        try:
                            needs_living = float(existing.living_sf or 0) <= 0 and float(existing.building_sf or 0) <= 0
                        except Exception:
                            needs_living = True
                        needs_zoning = not str(getattr(existing, "zoning", "") or "").strip()
                        needs_flu = not str(getattr(existing, "future_land_use", "") or "").strip()
                        if needs_living or needs_zoning or needs_flu:
                            ids_to_enrich.append(pid)
                        if len(ids_to_enrich) >= inline_cap:
                            break
                    if ids_to_enrich:
                        try:
                            from florida_property_scraper.parcels.live.fdor_centroids import (
                                FDORCentroidClient,
                            )
                            from florida_property_scraper.pa.providers.orange_ocpa import (
                                enrich_parcel as ocpa_enrich,
                            )

                            client = FDORCentroidClient()
                            fdor_rows = client.fetch_parcels(ids_to_enrich, include_geometry=True)
                            # Early-stop once we have *some* strict matches.
                            # Avoid a full O(n^2) rescan after every upsert; just evaluate
                            # the newly enriched record and keep a running count.
                            strict_match_target = 0
                            strict_match_count = 0
                            if strict_attribute_filters:
                                try:
                                    # Prefer returning at least a handful of correct matches quickly
                                    # over spending the entire budget trying to fill the whole `limit`.
                                    strict_match_target = max(1, min(int(limit or 0), 5))
                                except Exception:
                                    strict_match_target = 1

                            def _fields_for_pa(_pa: Any) -> dict[str, object]:
                                fields_tmp: dict[str, object] = {}
                                try:
                                    fields_tmp.update(_pa.to_dict())
                                except Exception:
                                    return fields_tmp
                                try:
                                    fields_tmp.update(compute_ui_fields(fields_tmp))
                                except Exception:
                                    pass

                                # Stable filter aliases.
                                try:
                                    living = float(_pa.living_sf or 0) or float(_pa.building_sf or 0) or 0.0
                                    fields_tmp["living_area_sqft"] = living if living > 0 else None
                                except Exception:
                                    fields_tmp["living_area_sqft"] = None
                                try:
                                    land_sf = float(_pa.land_sf or 0) or 0.0
                                    land_acres = float(_pa.land_acres or 0) or 0.0
                                    lot_sqft = land_sf
                                    if lot_sqft <= 0 and land_acres > 0:
                                        lot_sqft = land_acres * 43560.0
                                    fields_tmp["lot_size_sqft"] = lot_sqft if lot_sqft > 0 else None
                                except Exception:
                                    fields_tmp["lot_size_sqft"] = None
                                try:
                                    lot_acres = float(_pa.land_acres or 0) or 0.0
                                    if lot_acres <= 0:
                                        lot_sqft_any = fields_tmp.get("lot_size_sqft")
                                        if isinstance(lot_sqft_any, (int, float, str)):
                                            lot_sqft = float(lot_sqft_any) or 0.0
                                        else:
                                            lot_sqft = 0.0
                                        lot_acres = (lot_sqft / 43560.0) if lot_sqft > 0 else 0.0
                                    fields_tmp["lot_size_acres"] = lot_acres if lot_acres > 0 else None
                                except Exception:
                                    fields_tmp["lot_size_acres"] = None
                                try:
                                    fields_tmp["zoning_norm"] = _norm_choice(_pa.zoning)
                                except Exception:
                                    fields_tmp["zoning_norm"] = "UNKNOWN"
                                try:
                                    flu_raw = str(getattr(_pa, "future_land_use", "") or "").strip()
                                    fields_tmp["future_land_use_norm"] = _norm_choice(flu_raw)
                                except Exception:
                                    fields_tmp["future_land_use_norm"] = "UNKNOWN"
                                try:
                                    pt = (_pa.use_type or _pa.land_use_code or "").strip()
                                    fields_tmp["property_type"] = pt or None
                                except Exception:
                                    fields_tmp["property_type"] = None

                                return fields_tmp

                            def _is_strict_match(_pa: Any) -> bool:
                                if strict_match_target <= 0:
                                    return False
                                try:
                                    return bool(apply_filters(_fields_for_pa(_pa), compiled_filters))
                                except Exception:
                                    return False

                            if strict_match_target > 0:
                                try:
                                    # Seed the counter from whatever is already cached.
                                    for _pid in eligible_ids:
                                        _pa0 = pa_by_id.get(_pid)
                                        if _pa0 is None:
                                            continue
                                        if _is_strict_match(_pa0):
                                            strict_match_count += 1
                                            if strict_match_count >= strict_match_target:
                                                break
                                except Exception:
                                    strict_match_count = 0

                            for pid in ids_to_enrich:
                                if inline_deadline is not None:
                                    try:
                                        if _time.time() > inline_deadline:
                                            warnings.append("inline_enrich_budget_exhausted")
                                            break
                                    except Exception:
                                        pass
                                existing = pa_by_id.get(pid)
                                row = fdor_rows.get(pid)
                                base_sources: list[dict] = []
                                if row is not None:
                                    base_sources.append({"name": "fdor_centroids", "url": row.raw_source_url})
                                try:
                                    ocpa = ocpa_enrich(str(pid))
                                    if isinstance(ocpa, dict) and ocpa.get("error_reason"):
                                        try:
                                            er = str(ocpa.get("error_reason") or "").strip() or "unknown"
                                        except Exception:
                                            er = "unknown"
                                        warnings.append(f"inline_ocpa_enrich_error_reason:{pid}:{er}")
                                        continue
                                    ocpa_url = str(ocpa.get("source_url") or "").strip()
                                    ocpa_sources = (
                                        [{"name": "orange_ocpa", "url": ocpa_url}] if ocpa_url else []
                                    )
                                    merged_sources = _merge_sources(
                                        getattr(existing, "sources", None) if existing else None,
                                        _merge_sources(base_sources, ocpa_sources),
                                    )

                                    owner_name = str(ocpa.get("owner_name") or "").strip()
                                    owner_names = [owner_name] if owner_name else (existing.owner_names if existing else [])

                                    land_use = str(ocpa.get("land_use") or "").strip()
                                    zoning_v = str(ocpa.get("zoning") or "").strip()
                                    future_land_use_v = str(ocpa.get("future_land_use") or "").strip()
                                    property_type_v = str(ocpa.get("property_type") or "").strip()
                                    year_built = int(ocpa.get("year_built") or 0)
                                    living_sf = float(ocpa.get("living_area_sqft") or 0)
                                    bedrooms = int(ocpa.get("beds") or 0)
                                    bathrooms = float(ocpa.get("baths") or 0)
                                    land_value = float(ocpa.get("land_value") or 0)
                                    improvement_value = float(ocpa.get("building_value") or 0)
                                    total_value = float(ocpa.get("total_value") or 0)

                                    pa_rec = PAProperty(
                                        county=county_key,
                                        parcel_id=str(pid),
                                        situs_address=str(ocpa.get("situs_address") or "").strip()
                                        or ((row.situs_address if row is not None else "") or (existing.situs_address if existing else "")),
                                        mailing_address=str(ocpa.get("mailing_address") or "").strip()
                                        or (existing.mailing_address if existing else ""),
                                        owner_names=owner_names,
                                        land_use_code=land_use or (existing.land_use_code if existing else ""),
                                        use_type=(property_type_v or land_use) or (existing.use_type if existing else ""),
                                        zoning=zoning_v or (existing.zoning if existing else ""),
                                        future_land_use=future_land_use_v or (existing.future_land_use if existing else ""),
                                        land_sf=float((row.land_sqft if row is not None else (existing.land_sf if existing else 0)) or 0),
                                        year_built=year_built or (existing.year_built if existing else 0),
                                        living_sf=living_sf or (existing.living_sf if existing else 0),
                                        bedrooms=bedrooms or (existing.bedrooms if existing else 0),
                                        bathrooms=bathrooms or (existing.bathrooms if existing else 0),
                                        land_value=land_value or (existing.land_value if existing else 0),
                                        improvement_value=improvement_value or (existing.improvement_value if existing else 0),
                                        just_value=total_value or (existing.just_value if existing else 0),
                                        assessed_value=total_value or (existing.assessed_value if existing else 0),
                                        last_sale_date=ocpa.get("last_sale_date")
                                        or (existing.last_sale_date if existing else None),
                                        last_sale_price=float(ocpa.get("last_sale_price") or 0)
                                        or (existing.last_sale_price if existing else 0),
                                        zip=(row.situs_zip if row is not None else (existing.zip if existing else "")) or "",
                                        latitude=(row.lat if row is not None else (existing.latitude if existing else None)),
                                        longitude=(row.lon if row is not None else (existing.longitude if existing else None)),
                                        source_url=ocpa_url
                                        or (existing.source_url if existing else (row.raw_source_url if row is not None else "")),
                                        parser_version="orange_ocpa:v1",
                                        sources=merged_sources,
                                        field_provenance=(ocpa.get("field_provenance") or {}),
                                    )
                                    store.upsert(pa_rec)
                                    enriched_live_ids.add(str(pid))
                                    pa_by_id[str(pid)] = pa_rec

                                    # Early-stop to avoid needless enrichment.
                                    if strict_match_target > 0:
                                        try:
                                            if _is_strict_match(pa_rec):
                                                strict_match_count += 1
                                            if strict_match_count >= strict_match_target:
                                                break
                                        except Exception:
                                            pass
                                except Exception as e:
                                    warnings.append(f"inline_ocpa_enrich_failed:{pid}:{e}")

                            pa_by_id = store.get_many(county=county_key, parcel_ids=parcel_ids)
                        except Exception as e:
                            warnings.append(f"inline_ocpa_batch_failed:{e}")

            # Compute baseline (unfiltered) option lists AFTER best-effort enrichment.
            # Otherwise the first run in a new area can return empty option arrays.
            baseline_pa_by_id = store.get_many(county=county_key, parcel_ids=baseline_parcel_ids)

            def _looks_like_code(s: str) -> bool:
                import re

                t = (s or "").strip()
                if not t:
                    return True
                # Treat purely numeric / slash-y strings as non-human (ex: "01/001", "089").
                if re.fullmatch(r"[0-9\s\-/\.]+", t):
                    return True
                return False

            def _candidate_coverage() -> dict[str, dict[str, object]]:
                total = int(len(baseline_parcel_ids) or 0)
                present = {
                    "living_area_sqft": 0,
                    "lot_size_sqft": 0,
                    "lot_size_acres": 0,
                    "zoning": 0,
                    "future_land_use": 0,
                }
                for pid in baseline_parcel_ids:
                    pa = baseline_pa_by_id.get(pid)
                    if pa is None:
                        continue
                    try:
                        living = float(pa.living_sf or 0) or float(pa.building_sf or 0) or 0.0
                        if living > 0:
                            present["living_area_sqft"] += 1
                    except Exception:
                        pass
                    try:
                        land_sf = float(pa.land_sf or 0) or 0.0
                        land_acres = float(pa.land_acres or 0) or 0.0
                        if land_sf > 0 or land_acres > 0:
                            present["lot_size_sqft"] += 1
                            present["lot_size_acres"] += 1
                    except Exception:
                        pass
                    try:
                        if str(getattr(pa, "zoning", "") or "").strip():
                            present["zoning"] += 1
                    except Exception:
                        pass
                    try:
                        if str(getattr(pa, "future_land_use", "") or "").strip():
                            present["future_land_use"] += 1
                    except Exception:
                        pass

                out: dict[str, dict[str, object]] = {}
                for k, v in present.items():
                    frac = (float(v) / float(total)) if total > 0 else 0.0
                    out[k] = {
                        "present": int(v),
                        "total": int(total),
                        "coverage": frac,
                    }
                return out

            field_stats["coverage_candidates"] = _candidate_coverage()

            def _baseline_options(field_name: str) -> list[str]:
                values: set[str] = set()
                for pa in baseline_pa_by_id.values():
                    try:
                        raw = getattr(pa, field_name, "")
                    except Exception:
                        raw = ""
                    s = str(raw or "").strip()
                    if not s:
                        continue
                    if field_name == "future_land_use" and _looks_like_code(s):
                        continue
                    values.add(_norm_choice(s))
                return sorted(values)

            zoning_options = _baseline_options("zoning")
            future_land_use_options = _baseline_options("future_land_use")

            # When strict filters are enabled, include explicit coverage warnings for
            # fields the user is attempting to filter on.
            if strict_attribute_filters:
                required: set[str] = set()
                for c in compiled_filters or []:
                    try:
                        required.add(str(getattr(c, "field", "") or ""))
                    except Exception:
                        continue

                # Map normalized filter fields back to their PA source fields.
                required_pa_fields: set[str] = set()
                for f in required:
                    if f in {"zoning_norm"}:
                        required_pa_fields.add("zoning")
                    elif f in {"future_land_use_norm"}:
                        required_pa_fields.add("future_land_use")
                    elif f in {"living_area_sqft", "lot_size_sqft"}:
                        required_pa_fields.add(f)

                cov = field_stats.get("coverage_candidates") or {}
                if isinstance(cov, dict):
                    for f in sorted(required_pa_fields):
                        stats_f = cov.get(f)
                        if not isinstance(stats_f, dict):
                            continue
                        p = stats_f.get("present")
                        t = stats_f.get("total")
                        if isinstance(p, int) and isinstance(t, int) and t > 0:
                            warnings.append(f"coverage:{f}:{p}/{t}")

            # Optional: SQL-side filtering against cached columns.
            # Only applies when the request is not asking us to enrich missing data.
            # If enrich=true, we need to consider parcels not yet cached.
            #
            # IMPORTANT: run this AFTER the best-effort live enrichment so we don't
            # accidentally filter away all candidates simply because they were not
            # yet cached at the moment the request started.
            # IMPORTANT: numeric filtering semantics are centralized in
            # `compile_filters` + `apply_filters` over normalized in-memory fields.
            # Keep SQL prefiltering disabled to avoid drift (ex: living_sf vs building_sf,
            # lot sqft computed from land_sf vs land_acres, etc.).
            if False and isinstance(raw_filters, dict) and parcel_ids and not enrich_requested:
                where_parts: list[str] = []
                where_params: list[object] = []

                def _add_num(col: str, op: str, v: object) -> None:
                    try:
                        if v is None:
                            return
                        if isinstance(v, (int, float)):
                            num = float(v)
                        else:
                            s = str(v).strip().replace(",", "")
                            if not s:
                                return
                            num = float(s)
                        where_parts.append(f"{col} {op} ?")
                        where_params.append(num)
                    except Exception:
                        return

                def _add_text_contains(col: str, v: object) -> None:
                    if v is None:
                        return
                    s = str(v).strip()
                    if not s:
                        return
                    where_parts.append(f"LOWER({col}) LIKE ?")
                    where_params.append(f"%{s.lower()}%")

                # Ranges
                _add_num("living_sf", ">=", raw_filters.get("min_sqft"))
                _add_num("living_sf", "<=", raw_filters.get("max_sqft"))
                _add_num("year_built", ">=", raw_filters.get("min_year_built"))
                _add_num("year_built", "<=", raw_filters.get("max_year_built"))
                _add_num("bedrooms", ">=", raw_filters.get("min_beds"))
                _add_num("bathrooms", ">=", raw_filters.get("min_baths"))

                _add_num("just_value", ">=", raw_filters.get("min_value"))
                _add_num("just_value", "<=", raw_filters.get("max_value"))
                _add_num("land_value", ">=", raw_filters.get("min_land_value"))
                _add_num("land_value", "<=", raw_filters.get("max_land_value"))
                _add_num("improvement_value", ">=", raw_filters.get("min_building_value"))
                _add_num("improvement_value", "<=", raw_filters.get("max_building_value"))

                # Text
                _add_text_contains("zoning", raw_filters.get("zoning"))
                _add_text_contains("use_type", raw_filters.get("property_type"))

                # Dates (ISO strings compare lexicographically)
                d0 = raw_filters.get("last_sale_date_start")
                d1 = raw_filters.get("last_sale_date_end")
                if isinstance(d0, str) and d0.strip():
                    where_parts.append("last_sale_date >= ?")
                    where_params.append(d0.strip())
                if isinstance(d1, str) and d1.strip():
                    where_parts.append("last_sale_date <= ?")
                    where_params.append(d1.strip())

                if where_parts:
                    where_sql = " AND ".join(where_parts)
                    matching_ids = set(
                        store.filter_cached_ids(
                            county=county_key,
                            parcel_ids=parcel_ids,
                            where_sql=where_sql,
                            params=where_params,
                            limit=len(parcel_ids),
                        )
                    )
                    if matching_ids:
                        intersecting = [f for f in intersecting if f.parcel_id in matching_ids]
                        parcel_ids = [f.parcel_id for f in intersecting]
                    else:
                        intersecting = []
                        parcel_ids = []

                # Refresh after SQL filtering.
                pa_by_id = store.get_many(county=county_key, parcel_ids=parcel_ids)

            hover_by_id = store.get_hover_fields_many(county=county_key, parcel_ids=parcel_ids)

            # Strict mode: missing values must fail attribute filters. Soft-missing is
            # reserved for polygon-only browsing (no attribute filters).
        finally:
            store.close()

        _mark("enrich")

        # Compile filters (supports both list-form and object-form).
        raw_filters = payload.get("filters")
        filters = compile_filters(raw_filters)

        _mark("compile_filters")

        # Opt-in debug summary for filter parsing/normalization.
        if debug_enabled:
            try:
                raw_filter_keys = None
                if isinstance(raw_filters, dict):
                    raw_filter_keys = sorted([str(k) for k in raw_filters.keys()])
                compiled_summary = []
                for f in (filters or []):
                    try:
                        compiled_summary.append(
                            {
                                "field": getattr(f, "field", None),
                                "op": getattr(f, "op", None),
                                "value": getattr(f, "value", None),
                            }
                        )
                    except Exception:
                        continue
                _append_search_debug(
                    {
                        "event": "filters",
                        "sort": payload.get("sort"),
                        "raw_filter_keys": raw_filter_keys,
                        "raw_filters": raw_filters if isinstance(raw_filters, dict) else None,
                        "compiled_filters": compiled_summary,
                    }
                )
            except Exception:
                pass

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
        records = []
        source_counts: dict[str, int] = {"live": 0, "cache": 0}
        legacy_source_counts: dict[str, int] = {
            "local": 0,
            "live": 0,
            "geojson": 0,
            "missing": 0,
        }

        def _prov(source_name: str, url: str) -> dict:
            return {"source": source_name, "url": url}

        def _pa_field_source(pa_obj: object) -> tuple[str, str]:
            try:
                pv = str(getattr(pa_obj, "parser_version", "") or "")
                su = str(getattr(pa_obj, "source_url", "") or "")
                if pv.startswith("orange_ocpa"):
                    return "orange_ocpa", su
                if pv.startswith("fdor_centroids"):
                    return "fdor_centroids", su
                return "pa_db", su
            except Exception:
                return "pa_db", ""

        filter_stage_counts: dict[str, int] = {
            "intersecting": len(intersecting),
            "skipped_no_pa_fdor_live": 0,
            "with_pa": 0,
            "filter_failed": 0,
            "trigger_failed": 0,
            "emitted": 0,
        }

        def _conf_meta(
            value: object,
            *,
            source: str | None,
            missing_reason: str | None = None,
        ) -> dict[str, object]:
            present = value not in (None, "")
            if not present:
                return {
                    "source": source,
                    "confidence": 0.0,
                    "reason": missing_reason or "missing",
                }
            # Keep scoring deliberately coarse for now.
            if source in {"pa_db", "orange_ocpa", "fdor_centroids"}:
                c = 0.9
            elif source:
                c = 0.6
            else:
                c = 0.5
            return {"source": source, "confidence": float(c), "reason": None}

        for feat in intersecting:
            pa = pa_by_id.get(feat.parcel_id)
            pa_dict = pa.to_dict() if pa is not None else None

            # Guardrail: if FDOR live mode is requested and enabled, do not emit
            # demo rows. Instead, skip rows we couldn't enrich and surface a reason.
            if live and provider_is_live and fdor_enabled and pa_dict is None:
                if live_error_reason is None:
                    live_error_reason = "fdor_no_attributes_for_some_parcels"
                filter_stage_counts["skipped_no_pa_fdor_live"] += 1
                continue

            if pa_dict is not None:
                filter_stage_counts["with_pa"] += 1
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

            # Soft-missing behavior is disabled when attribute filters are present.

            # Provide stable, UI-friendly aliases for filtering.
            # These keys are treated as authoritative only when PA has a record.
            if pa is not None:
                try:
                    living = float(pa.living_sf or 0) or float(pa.building_sf or 0) or 0.0
                    fields["living_area_sqft"] = living if living > 0 else None
                except Exception:
                    fields["living_area_sqft"] = None
                try:
                    land_sf = float(pa.land_sf or 0) or 0.0
                    land_acres = float(pa.land_acres or 0) or 0.0
                    lot_sqft = land_sf
                    if lot_sqft <= 0 and land_acres > 0:
                        lot_sqft = land_acres * 43560.0
                    fields["lot_size_sqft"] = lot_sqft if lot_sqft > 0 else None
                except Exception:
                    fields["lot_size_sqft"] = None
                try:
                    lot_acres = float(pa.land_acres or 0) or 0.0
                    if lot_acres <= 0:
                        lot_sqft_any = fields.get("lot_size_sqft")
                        if isinstance(lot_sqft_any, (int, float, str)):
                            lot_sqft = float(lot_sqft_any) or 0.0
                        else:
                            lot_sqft = 0.0
                        lot_acres = (lot_sqft / 43560.0) if lot_sqft > 0 else 0.0
                    fields["lot_size_acres"] = lot_acres if lot_acres > 0 else None
                except Exception:
                    fields["lot_size_acres"] = None
                try:
                    b = int(pa.bedrooms or 0)
                    fields["beds"] = b if b > 0 else None
                except Exception:
                    fields["beds"] = None
                try:
                    ba = float(pa.bathrooms or 0)
                    fields["baths"] = ba if ba > 0 else None
                except Exception:
                    fields["baths"] = None
                try:
                    yb = int(pa.year_built or 0)
                    fields["year_built"] = yb if yb > 0 else None
                except Exception:
                    fields["year_built"] = None
                try:
                    lv = float(pa.land_value or 0)
                    fields["land_value"] = lv if lv > 0 else None
                except Exception:
                    fields["land_value"] = None
                try:
                    iv = float(pa.improvement_value or 0)
                    fields["building_value"] = iv if iv > 0 else None
                except Exception:
                    fields["building_value"] = None
                try:
                    tv = float(pa.just_value or 0)
                    fields["total_value"] = tv if tv > 0 else None
                except Exception:
                    fields["total_value"] = None
                # `property_type` is treated as the PA use_type / land_use_code label.
                try:
                    pt = (pa.use_type or pa.land_use_code or "").strip()
                    fields["property_type"] = pt or None
                except Exception:
                    fields["property_type"] = None

                try:
                    fields["zoning_norm"] = _norm_choice(pa.zoning)
                except Exception:
                    fields["zoning_norm"] = "UNKNOWN"
                try:
                    flu_raw = str(getattr(pa, "future_land_use", "") or "").strip()
                    fields["future_land_use_norm"] = _norm_choice(flu_raw)
                except Exception:
                    fields["future_land_use_norm"] = "UNKNOWN"

            # Optional safety valve: prevent sale-based filtering/triggering.
            if not flags.sale_filtering:
                for k in sale_fields:
                    fields.pop(k, None)

            # Track field availability for UI warnings/debug.
            try:
                field_stats["scanned"] += 1
                if fields.get("living_area_sqft") not in (None, "", 0):
                    field_stats["present"]["living_area_sqft"] += 1
                if fields.get("lot_size_sqft") not in (None, "", 0):
                    field_stats["present"]["lot_size_sqft"] += 1
                if fields.get("lot_size_acres") not in (None, "", 0):
                    field_stats["present"]["lot_size_acres"] += 1
                if fields.get("year_built") not in (None, "", 0):
                    field_stats["present"]["year_built"] += 1
                if str(fields.get("zoning") or "").strip():
                    field_stats["present"]["zoning"] += 1
                if str(fields.get("future_land_use") or "").strip():
                    field_stats["present"]["future_land_use"] += 1
            except Exception:
                pass

            # Capture one representative candidate BEFORE filters are applied.
            if "sample_candidate" not in field_stats:
                try:
                    raw_subset: dict[str, object] = {"parcel_id": feat.parcel_id}
                    if pa is not None:
                        raw_subset.update(
                            {
                                "land_sf": getattr(pa, "land_sf", None),
                                "land_acres": getattr(pa, "land_acres", None),
                                "living_sf": getattr(pa, "living_sf", None),
                                "building_sf": getattr(pa, "building_sf", None),
                                "bedrooms": getattr(pa, "bedrooms", None),
                                "bathrooms": getattr(pa, "bathrooms", None),
                                "year_built": getattr(pa, "year_built", None),
                                "zoning": getattr(pa, "zoning", None),
                                "future_land_use": getattr(pa, "future_land_use", None),
                                "use_type": getattr(pa, "use_type", None),
                                "land_use_code": getattr(pa, "land_use_code", None),
                                "parser_version": getattr(pa, "parser_version", None),
                                "source_url": getattr(pa, "source_url", None),
                            }
                        )

                    norm_subset = {
                        "living_area_sqft": fields.get("living_area_sqft"),
                        "lot_size_sqft": fields.get("lot_size_sqft"),
                        "lot_size_acres": fields.get("lot_size_acres"),
                        "beds": fields.get("beds"),
                        "baths": fields.get("baths"),
                        "year_built": fields.get("year_built"),
                        "zoning": fields.get("zoning"),
                        "zoning_norm": fields.get("zoning_norm"),
                        "future_land_use_norm": fields.get("future_land_use_norm"),
                        "property_type": fields.get("property_type"),
                    }

                    field_stats["sample_candidate"] = {
                        "raw": raw_subset,
                        "normalized": norm_subset,
                    }
                except Exception:
                    pass

            if not apply_filters(fields, filters):
                filter_stage_counts["filter_failed"] += 1
                continue

            reason_codes = eval_triggers(fields, triggers) if triggers else []
            if triggers and not reason_codes:
                filter_stage_counts["trigger_failed"] += 1
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

            # Enriched record payload for the modern UI.
            # New source contract:
            # - cache: we have a PA DB record already
            # - live: record was fetched live this request OR the geometry provider is live
            if pa_dict is None:
                # Product rule: never emit fake/demo records. If we don't have PA data,
                # we cannot apply reliable attribute filters nor display real details.
                continue

            if live and provider_is_live and fdor_enabled:
                # If the request is explicitly live and we're using the FDOR provider,
                # treat the record as live even when attributes came from PA cache.
                # (The geometry + authoritative parcel IDs are still from the live source.)
                source = "live"
            else:
                source = "live" if feat.parcel_id in enriched_live_ids else "cache"

            legacy_source = "local" if pa_dict else ("live" if live else "missing")

            source_counts[source] = int(source_counts.get(source, 0)) + 1
            legacy_source_counts[legacy_source] = int(
                legacy_source_counts.get(legacy_source, 0)
            ) + 1

            owner_name = hover.get("owner_name") or ""
            situs_address = hover.get("situs_address") or ""
            owner_mailing_address = ""
            homestead_flag = None
            zoning = ""
            land_use = ""
            future_land_use = ""
            property_class = ""
            living_area_sqft = None
            lot_size_sqft = None
            lot_size_acres = None
            beds = None
            baths = None
            year_built = None
            last_sale_date = hover.get("last_sale_date")
            last_sale_price = hover.get("last_sale_price")

            if pa is not None:
                owner_name = "; ".join([n for n in (pa.owner_names or []) if n]) or owner_name
                situs_address = pa.situs_address or situs_address
                owner_mailing_address = ", ".join(
                    [
                        str(getattr(pa, "mailing_address", "") or "").strip(),
                        " ".join(
                            [
                                str(getattr(pa, "mailing_city", "") or "").strip(),
                                str(getattr(pa, "mailing_state", "") or "").strip(),
                                str(getattr(pa, "mailing_zip", "") or "").strip(),
                            ]
                        ).strip(),
                    ]
                ).replace(" ,", ",").strip(" ,")
                zoning = (pa.zoning or "").strip()
                future_land_use = (pa.future_land_use or "").strip()
                land_use = (pa.use_type or pa.land_use_code or "").strip()
                property_class = (pa.property_class or "").strip()
                living_area_sqft = float(pa.living_sf or 0) or None
                if living_area_sqft is None:
                    living_area_sqft = float(pa.building_sf or 0) or None
                lot_size_sqft = float(pa.land_sf or 0) or None
                lot_size_acres = float(pa.land_acres or 0) or None
                if lot_size_acres is None and lot_size_sqft is not None:
                    try:
                        lot_size_acres = float(lot_size_sqft) / 43560.0
                    except Exception:
                        lot_size_acres = None
                beds = int(pa.bedrooms) if int(pa.bedrooms or 0) > 0 else None
                baths = float(pa.bathrooms) if float(pa.bathrooms or 0) > 0 else None
                year_built = int(pa.year_built) if int(pa.year_built or 0) > 0 else None
                last_sale_date = pa.last_sale_date or last_sale_date
                last_sale_price = float(pa.last_sale_price or 0) or last_sale_price

                try:
                    ex = getattr(pa, "exemptions", None)
                    if isinstance(ex, (list, tuple)):
                        homestead_flag = any("HOMESTEAD" in str(x or "").upper() for x in ex)
                except Exception:
                    homestead_flag = None

                land_value = float(pa.land_value or 0) or None
                building_value = float(pa.improvement_value or 0) or None
                total_value = float(pa.just_value or 0) or None
            else:
                land_value = None
                building_value = None
                total_value = None

            zoning_out = zoning.strip() or None
            zoning_reason = None
            if zoning_out is None:
                zoning_reason = "not_provided_by_source"

            sqft: list[dict] = []
            if living_area_sqft is not None:
                sqft.append({"type": "living", "value": float(living_area_sqft)})
            if lot_size_sqft is not None:
                sqft.append({"type": "lot", "value": float(lot_size_sqft)})

            provenance: dict[str, dict] = {}
            if pa is not None:
                psrc, purl = _pa_field_source(pa)
                # Record-level url is the most useful right now; per-field where possible.
                if situs_address:
                    provenance["situs_address"] = _prov(psrc, purl)
                if owner_name:
                    provenance["owner_name"] = _prov(psrc, purl)
                if land_use:
                    provenance["land_use"] = _prov(psrc, purl)
                if zoning_out:
                    provenance["zoning"] = _prov(psrc, purl)
                if year_built is not None:
                    provenance["year_built"] = _prov(psrc, purl)
                if living_area_sqft is not None:
                    provenance["sqft.living"] = _prov(psrc, purl)
                if lot_size_sqft is not None:
                    provenance["sqft.lot"] = _prov(psrc, purl)
                if last_sale_date:
                    provenance["last_sale_date"] = _prov(psrc, purl)
                if last_sale_price:
                    provenance["last_sale_price"] = _prov(psrc, purl)
                if land_value is not None:
                    provenance["land_value"] = _prov(psrc, purl)
                if building_value is not None:
                    provenance["building_value"] = _prov(psrc, purl)
                if total_value is not None:
                    provenance["total_value"] = _prov(psrc, purl)
            elif legacy_source == "geojson":
                provenance["situs_address"] = _prov("geojson_file", "")
                provenance["owner_name"] = _prov("geojson_file", "")
                provenance["last_sale_date"] = _prov("geojson_file", "")
                provenance["last_sale_price"] = _prov("geojson_file", "")

            data_sources = []
            field_provenance = {}
            raw_source_url = ""
            photo_url = None
            mortgage_lender = None
            mortgage_amount = None
            mortgage_date = None
            if pa is not None:
                raw_source_url = str(pa.source_url or "")
                data_sources = getattr(pa, "sources", None) or []
                field_provenance = getattr(pa, "field_provenance", None) or {}
                try:
                    photo_url = str(getattr(pa, "photo_url", "") or "").strip() or None
                except Exception:
                    photo_url = None
                try:
                    mortgage_lender = str(getattr(pa, "mortgage_lender", "") or "").strip() or None
                except Exception:
                    mortgage_lender = None
                try:
                    mortgage_amount = float(getattr(pa, "mortgage_amount", 0) or 0) or None
                except Exception:
                    mortgage_amount = None
                try:
                    mortgage_date = str(getattr(pa, "mortgage_date", "") or "").strip() or None
                except Exception:
                    mortgage_date = None

            rec = {
                "record_version": 1,
                "parcel_id": feat.parcel_id,
                "county": county_key,
                "situs_address": situs_address.strip() or None,
                "owner_name": owner_name.strip() or None,
                "owner_mailing_address": owner_mailing_address.strip() or None,
                "homestead_flag": homestead_flag,
                "property_type": land_use.strip() or None,
                "land_use": land_use,
                "future_land_use": future_land_use.strip() or None,
                "beds": beds,
                "baths": baths,
                "year_built": year_built,
                "last_sale_date": last_sale_date,
                "last_sale_price": last_sale_price,
                "source": source,
                "zoning": zoning_out,
                "zoning_reason": zoning_reason,
                "sqft": sqft,
                "raw_source_url": raw_source_url,
                "data_sources": data_sources,
                "provenance": provenance,
                "field_provenance": field_provenance,
                "photo_url": photo_url,
                "mortgage_lender": mortgage_lender,
                "mortgage_amount": mortgage_amount,
                "mortgage_date": mortgage_date,
                "land_value": land_value,
                "building_value": building_value,
                "total_value": total_value,
                # Back-compat fields (older UI code paths)
                "address": situs_address,
                "flu": land_use,
                "property_class": property_class,
                "living_area_sqft": living_area_sqft,
                "lot_size_sqft": lot_size_sqft,
                "lot_size_acres": lot_size_acres,
            }
            lat, lng = _centroid_lat_lng(feat.geometry)
            rec["lat"] = lat
            rec["lng"] = lng

            # Stable confidence metadata for the unified record contract.
            try:
                psrc, purl = _pa_field_source(pa) if pa is not None else (None, "")
                conf_fields = {
                    "parcel_id": _conf_meta(feat.parcel_id, source=psrc),
                    "county": _conf_meta(county_key, source=psrc),
                    "situs_address": _conf_meta(rec.get("situs_address"), source=psrc),
                    "lat": _conf_meta(lat, source=psrc),
                    "lng": _conf_meta(lng, source=psrc),
                    "property_type": _conf_meta(rec.get("property_type"), source=psrc),
                    "living_area_sqft": _conf_meta(living_area_sqft, source=psrc),
                    "beds": _conf_meta(beds, source=psrc),
                    "baths": _conf_meta(baths, source=psrc),
                    "year_built": _conf_meta(year_built, source=psrc),
                    "lot_size_sqft": _conf_meta(lot_size_sqft, source=psrc),
                    "zoning": _conf_meta(zoning_out, source=psrc, missing_reason=zoning_reason),
                    "future_land_use": _conf_meta(rec.get("future_land_use"), source=psrc),
                    "owner_name": _conf_meta(rec.get("owner_name"), source=psrc),
                    "owner_mailing_address": _conf_meta(
                        rec.get("owner_mailing_address"),
                        source=psrc,
                    ),
                    "homestead_flag": _conf_meta(homestead_flag, source=psrc),
                    "last_sale_date": _conf_meta(last_sale_date, source=psrc),
                    "last_sale_price": _conf_meta(last_sale_price, source=psrc),
                }
                rec["data_confidence"] = {"fields": conf_fields, "record_source_url": purl}
            except Exception:
                rec["data_confidence"] = {"fields": {}}

            if include_geometry:
                rec["geometry"] = feat.geometry
            records.append(rec)

            filter_stage_counts["emitted"] += 1

            if len(results) >= limit:
                break

        _mark("apply_filters")

        if live_error_reason:
            warnings.append(f"live_error_reason: {live_error_reason}")

        # Deterministic post-filter sorting for the UI record list.
        sort_key = str(payload.get("sort") or "").strip().lower()
        if sort_key:
            try:
                from datetime import date as _date, datetime as _datetime
                import re as _re

                def _as_date(v: object) -> _date | None:
                    if v is None:
                        return None
                    if isinstance(v, _date) and not isinstance(v, _datetime):
                        return v
                    if isinstance(v, _datetime):
                        try:
                            return v.date()
                        except Exception:
                            return None
                    if not isinstance(v, str):
                        return None
                    s = v.strip()
                    if not s:
                        return None
                    if "T" in s:
                        try:
                            return _datetime.fromisoformat(s.replace("Z", "+00:00")).date()
                        except Exception:
                            return None
                    try:
                        return _date.fromisoformat(s)
                    except Exception:
                        pass

                    m = _re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
                    if m:
                        try:
                            mm = int(m.group(1))
                            dd = int(m.group(2))
                            yy = int(m.group(3))
                            return _date(yy, mm, dd)
                        except Exception:
                            return None

                    return None

                if sort_key == "last_sale_date_desc":
                    def _k(r: dict) -> tuple[bool, int]:
                        d = _as_date(r.get("last_sale_date"))
                        if d is None:
                            return True, 0
                        try:
                            return False, -int(d.toordinal())
                        except Exception:
                            return True, 0

                    records.sort(key=_k)
                elif sort_key == "year_built_desc":
                    records.sort(
                        key=lambda r: (
                            r.get("year_built") is None,
                            -int(r.get("year_built") or 0),
                        )
                    )
                elif sort_key == "sqft_desc":
                    records.sort(
                        key=lambda r: (
                            r.get("living_area_sqft") is None,
                            -float(r.get("living_area_sqft") or 0.0),
                        )
                    )
            except Exception:
                pass

        _mark("sort")

        # Flag whether we stopped early due to the limit.
        records_truncated = bool(len(results) >= limit and len(intersecting) > len(results))

        debug_flags: dict[str, Any] | None = None
        if debug_response_enabled:
            debug_flags = {
                "county": county_key,
                "limit": int(limit),
                "include_geometry": bool(include_geometry),
                "sort": str(payload.get("sort") or ""),
                "enrich_enabled": bool(payload.get("enrich", False)) if payload.get("enrich", None) is not None else False,
                "records_truncated": bool(records_truncated),
            }

        if debug_counts is not None:
            try:
                debug_counts.update(
                    {
                        "candidate_count": int(len(intersecting)),
                        "filtered_count": int(len(records)),
                        "returned_count": int(len(records)),
                        "records_truncated": bool(records_truncated),
                    }
                )
            except Exception:
                pass

        _mark("serialize")

        _append_search_debug(
            {
                "event": "result",
                "county": county_key,
                "candidate_count": len(intersecting),
                "filtered_count": len(records),
                "warnings": warnings,
                "field_stats": field_stats,
                "filter_stage_counts": filter_stage_counts,
            }
        )

        return JSONResponse(
            {
                "search_id": search_id,
                # Backwards-compatible keys
                "county": county_key,
                "count": len(results),
                "results": results,
                # New UI payload
                "zoning_options": zoning_options,
                "future_land_use_options": future_land_use_options,
                "summary": {
                    "count": len(records),
                    "candidate_count": len(intersecting),
                    "filtered_count": len(records),
                    "source_counts": source_counts,
                    "source_counts_legacy": legacy_source_counts,
                },
                "records": records,
                "records_truncated": bool(records_truncated),
                "warnings": warnings,
                "field_stats": field_stats,
                "filter_stage_counts": filter_stage_counts,
                "error_reason": live_error_reason,
                **({"normalized_filters": normalized_filters} if debug_response_enabled else {}),
                **(
                    {
                        "debug_timing_ms": debug_timing_ms or {},
                        "debug_counts": debug_counts or {},
                        "debug_flags": debug_flags or {},
                    }
                    if debug_response_enabled
                    else {}
                ),
            }
        )

    @app.post("/api/parcels/enrich")
    def api_parcels_enrich(payload: dict = Body(...)):
        """Batch-enrich parcels into the PA cache.

        This is intended to be called after geometry search.

        Input:
          { county: "orange"|"seminole", parcel_ids: ["..."] }

        Output:
          { county, count, records, errors }
        """

        county_key = (payload.get("county") or "").strip().lower() or "seminole"
        ids = payload.get("parcel_ids") or payload.get("parcel_id") or []
        if isinstance(ids, str):
            ids = [ids]
        if not isinstance(ids, list):
            raise HTTPException(status_code=400, detail="parcel_ids must be a list")

        parcel_ids = [str(x).strip() for x in ids if str(x).strip()]
        limit = int(payload.get("limit", 50))
        if limit <= 0:
            limit = 50
        parcel_ids = parcel_ids[: min(limit, 250)]

        if county_key not in {"orange", "seminole"}:
            raise HTTPException(
                status_code=400,
                detail="enrich currently supports orange and seminole only",
            )

        from florida_property_scraper.pa.schema import PAProperty
        from florida_property_scraper.pa.storage import PASQLite

        def _merge_sources(
            existing: list[dict] | None,
            add: list[dict],
        ) -> list[dict]:
            out: list[dict] = []
            seen: set[tuple[str, str]] = set()
            for src in (existing or []) + (add or []):
                if not isinstance(src, dict):
                    continue
                name = str(src.get("name") or "").strip()
                url = str(src.get("url") or "").strip()
                if not url:
                    continue
                key = (name, url)
                if key in seen:
                    continue
                seen.add(key)
                out.append({"name": name, "url": url})
            return out

        rows: dict[str, Any] = {}
        if county_key != "orange":
            if os.getenv("FPS_USE_FDOR_CENTROIDS", "").strip() not in {"1", "true", "True"}:
                raise HTTPException(
                    status_code=400,
                    detail="live enrichment disabled (set FPS_USE_FDOR_CENTROIDS=1)",
                )

            from florida_property_scraper.parcels.live.fdor_centroids import (
                FDORCentroidClient,
            )

            client = FDORCentroidClient()
            rows = client.fetch_parcels(parcel_ids, include_geometry=True)

        db_path = os.getenv("PA_DB", "./leads.sqlite")
        store = PASQLite(db_path)
        errors: dict[str, Any] = {}
        try:
            for pid in parcel_ids:
                row = rows.get(pid)
                if row is None and county_key != "orange":
                    errors[pid] = "not_found_in_fdor_centroids"
                    continue

                existing = None
                try:
                    existing = store.get(county=county_key, parcel_id=str(pid))
                except Exception:
                    existing = None

                base_sources: list[dict] = []
                if row is not None:
                    base_sources.append({"name": "fdor_centroids", "url": row.raw_source_url})

                if county_key == "orange":
                    # Orange: authoritative enrichment via OCPA.
                    try:
                        from florida_property_scraper.pa.providers.orange_ocpa import (
                            enrich_parcel,
                        )

                        ocpa = enrich_parcel(str(pid))
                        if isinstance(ocpa, dict) and ocpa.get("error_reason"):
                            errors[pid] = ocpa
                            # Fall back to FDOR-derived fields when available.
                            if row is None:
                                continue
                            raise RuntimeError(f"ocpa_error_reason:{ocpa.get('error_reason')}")
                        ocpa_url = str(ocpa.get("source_url") or "").strip()
                        ocpa_sources = (
                            [{"name": "orange_ocpa", "url": ocpa_url}] if ocpa_url else []
                        )

                        merged_sources = _merge_sources(
                            getattr(existing, "sources", None) if existing else None,
                            _merge_sources(base_sources, ocpa_sources),
                        )

                        owner_name = str(ocpa.get("owner_name") or "").strip()
                        owner_names = [owner_name] if owner_name else []

                        situs_address = str(ocpa.get("situs_address") or "").strip() or (
                            (row.situs_address if row is not None else "")
                        )
                        mailing_address = str(ocpa.get("mailing_address") or "").strip()

                        land_use = str(ocpa.get("land_use") or "").strip()
                        zoning_v = str(ocpa.get("zoning") or "").strip()
                        property_type_v = str(ocpa.get("property_type") or "").strip()

                        year_built = int(ocpa.get("year_built") or 0)
                        living_sf = float(ocpa.get("living_area_sqft") or 0)

                        bedrooms = int(ocpa.get("beds") or 0)
                        bathrooms = float(ocpa.get("baths") or 0)

                        land_value = float(ocpa.get("land_value") or 0)
                        improvement_value = float(ocpa.get("building_value") or 0)
                        total_value = float(ocpa.get("total_value") or 0)

                        last_sale_date = ocpa.get("last_sale_date")
                        last_sale_price = float(ocpa.get("last_sale_price") or 0)

                        photo_url = str(ocpa.get("photo_url") or "").strip()
                        mortgage_lender = str(ocpa.get("mortgage_lender") or "").strip()
                        mortgage_amount = float(ocpa.get("mortgage_amount") or 0)
                        mortgage_date = str(ocpa.get("mortgage_date") or "").strip()

                        pa_rec = PAProperty(
                            county=county_key,
                            parcel_id=str(pid),
                            situs_address=situs_address or "",
                            mailing_address=mailing_address or "",
                            owner_names=owner_names,
                            land_use_code=land_use or "",
                            use_type=(property_type_v or land_use) or "",
                            zoning=zoning_v or "",
                            land_sf=float((row.land_sqft if row is not None else 0) or 0),
                            year_built=year_built,
                            living_sf=living_sf,
                            bedrooms=bedrooms,
                            bathrooms=bathrooms,
                            land_value=land_value,
                            improvement_value=improvement_value,
                            just_value=total_value,
                            assessed_value=total_value,
                            last_sale_date=last_sale_date,
                            last_sale_price=last_sale_price,
                            photo_url=photo_url,
                            mortgage_lender=mortgage_lender,
                            mortgage_amount=mortgage_amount,
                            mortgage_date=mortgage_date,
                            zip=(row.situs_zip if row is not None else "") or "",
                            latitude=(row.lat if row is not None else None),
                            longitude=(row.lon if row is not None else None),
                            source_url=ocpa_url or (row.raw_source_url if row is not None else ""),
                            parser_version="orange_ocpa:v1",
                            sources=merged_sources,
                            field_provenance=(ocpa.get("field_provenance") or {}),
                        )
                    except Exception as e:
                        errors.setdefault(pid, {"error_reason": "exception", "hint": str(e)})
                        # Best-effort fallback to FDOR-derived fields when available.
                        if row is None:
                            continue

                        pa_rec = PAProperty(
                            county=county_key,
                            parcel_id=str(pid),
                            situs_address=row.situs_address or "",
                            owner_names=[row.owner_name] if row.owner_name else [],
                            land_use_code=row.land_use_code or "",
                            use_type=row.land_use_code or "",
                            land_sf=float(row.land_sqft or 0),
                            year_built=int(row.year_built or 0),
                            last_sale_date=row.last_sale_date,
                            last_sale_price=float(row.last_sale_price or 0),
                            zip=row.situs_zip or "",
                            latitude=row.lat,
                            longitude=row.lon,
                            source_url=row.raw_source_url,
                            parser_version="fdor_centroids:v1",
                            sources=_merge_sources(
                                getattr(existing, "sources", None) if existing else None,
                                base_sources,
                            ),
                        )
                else:
                    # Default enrichment path: FDOR statewide centroids.
                    assert row is not None
                    pa_rec = PAProperty(
                        county=county_key,
                        parcel_id=str(pid),
                        situs_address=row.situs_address or "",
                        owner_names=[row.owner_name] if row.owner_name else [],
                        land_use_code=row.land_use_code or "",
                        use_type=row.land_use_code or "",
                        land_sf=float(row.land_sqft or 0),
                        year_built=int(row.year_built or 0),
                        last_sale_date=row.last_sale_date,
                        last_sale_price=float(row.last_sale_price or 0),
                        zip=row.situs_zip or "",
                        latitude=row.lat,
                        longitude=row.lon,
                        source_url=row.raw_source_url,
                        parser_version="fdor_centroids:v1",
                        sources=_merge_sources(
                            getattr(existing, "sources", None) if existing else None,
                            base_sources,
                        ),
                    )
                try:
                    store.upsert(pa_rec)
                except Exception as e:
                    errors[pid] = {"error_reason": "cache_upsert_failed", "hint": str(e)}

            pa_by_id = store.get_many(county=county_key, parcel_ids=parcel_ids)
        finally:
            store.close()

        def _pa_field_source(pa_obj: object) -> tuple[str, str]:
            try:
                pv = str(getattr(pa_obj, "parser_version", "") or "")
                su = str(getattr(pa_obj, "source_url", "") or "")
                if pv.startswith("orange_ocpa"):
                    return "orange_ocpa", su
                if pv.startswith("fdor_centroids"):
                    return "fdor_centroids", su
                return "pa_db", su
            except Exception:
                return "pa_db", ""

        def _prov(source_name: str, url: str) -> dict:
            return {"source": source_name, "url": url}

        records: list[dict] = []
        for pid in parcel_ids:
            pa = pa_by_id.get(pid)
            if pa is None:
                continue

            psrc, purl = _pa_field_source(pa)
            owner_name = "; ".join([n for n in (pa.owner_names or []) if n])
            situs_address = pa.situs_address or ""
            land_use = (pa.use_type or pa.land_use_code or "").strip()

            # Values
            land_value = float(pa.land_value or 0) or None
            building_value = float(pa.improvement_value or 0) or None
            total_value = float(pa.just_value or 0) or None

            sqft: list[dict] = []
            living = float(pa.living_sf or 0) or None
            if living is None:
                living = float(pa.building_sf or 0) or None
            lot = float(pa.land_sf or 0) or None
            lot_acres = float(pa.land_acres or 0) or None
            if lot_acres is None and lot is not None:
                try:
                    lot_acres = float(lot) / 43560.0
                except Exception:
                    lot_acres = None
            if living is not None:
                sqft.append({"type": "living", "value": float(living)})
            if lot is not None:
                sqft.append({"type": "lot", "value": float(lot)})

            provenance: dict[str, dict] = {}
            if situs_address:
                provenance["situs_address"] = _prov(psrc, purl)
            if owner_name:
                provenance["owner_name"] = _prov(psrc, purl)
            if land_use:
                provenance["land_use"] = _prov(psrc, purl)
            if pa.year_built:
                provenance["year_built"] = _prov(psrc, purl)
            if pa.last_sale_date:
                provenance["last_sale_date"] = _prov(psrc, purl)
            if pa.last_sale_price:
                provenance["last_sale_price"] = _prov(psrc, purl)
            if land_value is not None:
                provenance["land_value"] = _prov(psrc, purl)
            if building_value is not None:
                provenance["building_value"] = _prov(psrc, purl)
            if total_value is not None:
                provenance["total_value"] = _prov(psrc, purl)
            if living is not None:
                provenance["sqft.living"] = _prov(psrc, purl)
            if lot is not None:
                provenance["sqft.lot"] = _prov(psrc, purl)

            data_sources = getattr(pa, "sources", None) or []
            field_provenance = getattr(pa, "field_provenance", None) or {}

            photo_url = None
            mortgage_lender = None
            mortgage_amount = None
            mortgage_date = None
            try:
                photo_url = str(getattr(pa, "photo_url", "") or "").strip() or None
            except Exception:
                photo_url = None
            try:
                mortgage_lender = str(getattr(pa, "mortgage_lender", "") or "").strip() or None
            except Exception:
                mortgage_lender = None
            try:
                mortgage_amount = float(getattr(pa, "mortgage_amount", 0) or 0) or None
            except Exception:
                mortgage_amount = None
            try:
                mortgage_date = str(getattr(pa, "mortgage_date", "") or "").strip() or None
            except Exception:
                mortgage_date = None

            zoning_out = (pa.zoning or "").strip() or None
            zoning_reason = None if zoning_out else "not_provided_by_source"

            records.append(
                {
                    "parcel_id": pid,
                    "county": county_key,
                    "situs_address": situs_address,
                    "owner_name": owner_name,
                    "land_use": land_use,
                    "zoning": zoning_out,
                    "zoning_reason": zoning_reason,
                    "sqft": sqft,
                    "lot_size_sqft": lot,
                    "lot_size_acres": lot_acres,
                    "beds": int(pa.bedrooms) if int(pa.bedrooms or 0) > 0 else None,
                    "baths": float(pa.bathrooms) if float(pa.bathrooms or 0) > 0 else None,
                    "year_built": int(pa.year_built) if int(pa.year_built or 0) > 0 else None,
                    "last_sale_date": pa.last_sale_date,
                    "last_sale_price": float(pa.last_sale_price or 0) or None,
                    "land_value": land_value,
                    "building_value": building_value,
                    "total_value": total_value,
                    "photo_url": photo_url,
                    "mortgage_lender": mortgage_lender,
                    "mortgage_amount": mortgage_amount,
                    "mortgage_date": mortgage_date,
                    "source": "cache",
                    "raw_source_url": purl,
                    "data_sources": data_sources,
                    "provenance": provenance,
                    "field_provenance": field_provenance,
                    "lat": pa.latitude,
                    "lng": pa.longitude,
                }
            )

        return JSONResponse(
            {
                "county": county_key,
                "count": len(records),
                "records": records,
                "errors": errors,
            }
        )

    @app.get("/api/debug/ping")
    def debug_ping():
        sha = os.getenv("APP_GIT_SHA") or ""
        branch = os.getenv("APP_GIT_BRANCH") or ""
        if not sha or not branch:
            try:
                import subprocess

                if not sha:
                    p = subprocess.run(
                        ["git", "rev-parse", "--short", "HEAD"],
                        cwd=str(REPO_ROOT),
                        text=True,
                        capture_output=True,
                    )
                    if p.returncode == 0:
                        sha = (p.stdout or "").strip()
                if not branch:
                    p = subprocess.run(
                        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                        cwd=str(REPO_ROOT),
                        text=True,
                        capture_output=True,
                    )
                    if p.returncode == 0:
                        branch = (p.stdout or "").strip()
            except Exception:
                pass
        return {
            "ok": True,
            "server_time": datetime.now(timezone.utc).isoformat(),
            "git": {"sha": sha or "dev", "branch": branch or "unknown"},
            "env": {
                "FPS_USE_FDOR_CENTROIDS": os.getenv("FPS_USE_FDOR_CENTROIDS", ""),
                "PA_DB": os.getenv("PA_DB", ""),
                "LEADS_SQLITE_PATH": os.getenv("LEADS_SQLITE_PATH", ""),
                "APP_GIT_SHA": os.getenv("APP_GIT_SHA", ""),
                "APP_GIT_BRANCH": os.getenv("APP_GIT_BRANCH", ""),
            },
        }

    @app.get("/api/debug/parcels_coverage")
    def debug_parcels_coverage(county: str):
        import sqlite3, os
        db_path = os.getenv("PARCELS_DB_PATH", "data/parcels/parcels.sqlite")
        db_path_abs = os.path.abspath(db_path)
        has_data = False
        row_count = 0
        bbox = {"minx": None, "miny": None, "maxx": None, "maxy": None}
        if os.path.exists(db_path_abs):
            con = sqlite3.connect(db_path_abs)
            cur = con.cursor()
            row_count = cur.execute("select count(*) from parcels where county=?", (county.lower(),)).fetchone()[0]
            if row_count > 0:
                has_data = True
                minx, miny, maxx, maxy = cur.execute(
                    "select min(minx), min(miny), max(maxx), max(maxy) from parcels where county=?",
                    (county.lower(),),
                ).fetchone()
                bbox = {"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy}
            con.close()
        return {"county": county.lower(), "db_path_used": db_path_abs, "row_count": row_count, "bbox": bbox, "has_data": has_data}


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

    def _spa_index_response():
        index = WEB_DIST / "index.html"
        if index.exists():
            return FileResponse(
                str(index),
                media_type="text/html",
                headers={"Cache-Control": "no-store"},
            )
        return {"status": "ok", "message": "API running (web/dist missing)"}

    @app.get("/")
    def root():
        # Serve the built SPA (web/dist). Fall back to JSON if missing.
        return _spa_index_response()

    @app.head("/", include_in_schema=False)
    def root_head():
        # Ensure HEAD / works for curl -I / (avoid 405 Method Not Allowed).
        # Return an empty 200 response; GET / serves the actual index.html.
        index = WEB_DIST / "index.html"
        if index.exists():
            return Response(status_code=200, media_type="text/html")
        return Response(status_code=200)

    if (WEB_DIST / "assets").exists():
        app.mount(
            "/assets",
            StaticFiles(directory=str(WEB_DIST / "assets")),
            name="assets",
        )

    @app.get("/{full_path:path}")
    def spa_fallback(full_path: str):
        # SPA fallback: serve index.html for any unknown, non-API, non-docs path.
        # Keep /api/*, /docs, /openapi.json, /health intact.
        path = (full_path or "").lstrip("/")
        reserved_prefixes = (
            "api/",
            "assets/",
        )
        reserved_exact = {
            "api",
            "assets",
            "docs",
            "openapi.json",
            "redoc",
            "health",
        }
        if path in reserved_exact or any(path.startswith(p) for p in reserved_prefixes):
            raise HTTPException(status_code=404, detail="Not Found")

        index = WEB_DIST / "index.html"
        if index.exists():
            return FileResponse(
                str(index),
                media_type="text/html",
                headers={"Cache-Control": "no-store"},
            )
        raise HTTPException(status_code=404, detail="web UI not built")

    # Ensure leads DB exists on startup
    from florida_property_scraper.db.init import init_db

    @app.on_event("startup")
    def _ensure_leads_db():
        logger = logging.getLogger("fps.startup")
        logger.warning(
            "startup env: FPS_USE_FDOR_CENTROIDS=%s PA_DB=%s APP_GIT_SHA=%s APP_GIT_BRANCH=%s",
            os.getenv("FPS_USE_FDOR_CENTROIDS", ""),
            os.getenv("PA_DB", ""),
            os.getenv("APP_GIT_SHA", ""),
            os.getenv("APP_GIT_BRANCH", ""),
        )
        init_db(os.getenv("LEADS_SQLITE_PATH", "./leads.sqlite"))

    _watchlists_scheduler_task = {"task": None}

    @app.on_event("startup")
    async def _start_watchlists_scheduler():
        import asyncio

        enabled = os.getenv("FPS_WATCHLIST_SCHEDULER", "0").strip() == "1"
        if not enabled:
            return

        interval_s = int(float(os.getenv("FPS_WATCHLIST_INTERVAL_S", "3600") or 3600))
        interval_s = max(30, interval_s)
        connector_limit = int(float(os.getenv("FPS_TRIGGER_CONNECTOR_LIMIT", "50") or 50))
        connector_limit = max(1, min(connector_limit, 500))

        logger = logging.getLogger("fps.watchlists")
        logger.warning(
            "watchlists scheduler enabled: interval_s=%s connector_limit=%s",
            interval_s,
            connector_limit,
        )

        async def _loop():
            from florida_property_scraper.storage import SQLiteStore
            from florida_property_scraper.triggers.engine import run_connector_once, utc_now_iso
            from florida_property_scraper.triggers.connectors.base import get_connector, list_connectors

            # Ensure builtin connectors are registered.
            import florida_property_scraper.triggers.connectors  # noqa: F401

            db_path = os.getenv("LEADS_SQLITE_PATH", "./leads.sqlite")

            while True:
                try:
                    now = utc_now_iso()
                    store = SQLiteStore(db_path)
                    try:
                        # 1) Run enabled saved searches to keep watchlist membership fresh.
                        ss_rows = store.conn.execute(
                            "SELECT id FROM saved_searches WHERE is_enabled=1 ORDER BY updated_at DESC"
                        ).fetchall()
                        for r in ss_rows[:50]:
                            sid = str(r["id"] or "").strip()
                            if not sid:
                                continue
                            store.run_saved_search(saved_search_id=sid, now_iso=now, limit=2000)

                        # 2) Refresh triggers for counties that have enabled saved searches.
                        wl_rows = store.conn.execute(
                            "SELECT DISTINCT county FROM saved_searches WHERE is_enabled=1"
                        ).fetchall()
                        counties = [str(r["county"] or "").strip().lower() for r in wl_rows]
                        counties = [c for c in counties if c]

                        connector_keys = [k for k in list_connectors() if k != "fake"]
                        for county in counties:
                            for ck in connector_keys:
                                try:
                                    run_connector_once(
                                        store=store,
                                        connector=get_connector(ck),
                                        county=county,
                                        now_iso=now,
                                        limit=connector_limit,
                                    )
                                except Exception as e:
                                    logger.warning("connector %s failed for %s: %s", ck, county, e)

                            # Rollups are county-scoped.
                            try:
                                store.rebuild_parcel_trigger_rollups(county=county, rebuilt_at=now)
                            except Exception as e:
                                logger.warning("rollups rebuild failed for %s: %s", county, e)

                        # 3) Sync inbox for enabled saved searches.
                        for r in ss_rows[:100]:
                            sid = str(r["id"] or "").strip()
                            if not sid:
                                continue
                            store.sync_saved_search_inbox_from_trigger_alerts(saved_search_id=sid, now_iso=now)
                    finally:
                        store.close()
                except Exception as e:
                    logger.warning("scheduler tick failed: %s", e)

                await asyncio.sleep(interval_s)

        _watchlists_scheduler_task["task"] = asyncio.create_task(_loop())

    @app.on_event("shutdown")
    async def _stop_watchlists_scheduler():
        import asyncio

        t = _watchlists_scheduler_task.get("task")
        if t is not None:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
