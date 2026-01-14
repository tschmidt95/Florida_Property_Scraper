from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import json
import logging
import os
import time
from datetime import datetime, timezone
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

    assert search_router is not None
    assert permits_router is not None
    assert lookup_router is not None

    app.include_router(search_router, prefix="/api")
    app.include_router(permits_router, prefix="/api")
    app.include_router(lookup_router, prefix="/api")

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

        # Guardrail: live mode can be expensive if/when implemented.
        if live and limit > 250:
            limit = 250

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

        provider = get_geometry_provider(county_key)
        provider_is_live = provider.__class__.__name__ == "FDORCentroidsProvider"
        fdor_enabled = os.getenv("FPS_USE_FDOR_CENTROIDS", "").strip() in {
            "1",
            "true",
            "True",
        }
        candidates = provider.query(bbox_t)

        # Filter to true intersections when possible.
        intersecting = [f for f in candidates if intersects(geometry, f.geometry)]

        warnings: list[str] = []
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
        try:
            parcel_ids = [f.parcel_id for f in intersecting]

            # Optional: SQL-side filtering against cached columns.
            # Only applies when the request is not asking us to enrich missing data.
            # If enrich=true, we need to consider parcels not yet cached.
            raw_filters = payload.get("filters")
            enrich_requested = bool(payload.get("enrich", False))
            if isinstance(raw_filters, dict) and parcel_ids and not enrich_requested:
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
                    # Filter intersecting candidates down to cached matches.
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

            pa_by_id = store.get_many(county=county_key, parcel_ids=parcel_ids)

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

                # Optional: inline enrichment of the first N results.
                inline_enrich = bool(payload.get("enrich", False))
                if inline_enrich and county_key == "orange" and fdor_enabled:
                    inline_cap = int(payload.get("enrich_limit", 5) or 0)
                    if inline_cap < 0:
                        inline_cap = 0
                    inline_cap = min(inline_cap, 25)
                    ids_to_enrich = [pid for pid in parcel_ids if pid][:inline_cap]
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
                            for pid in ids_to_enrich:
                                existing = pa_by_id.get(pid)
                                row = fdor_rows.get(pid)
                                base_sources: list[dict] = []
                                if row is not None:
                                    base_sources.append({"name": "fdor_centroids", "url": row.raw_source_url})
                                try:
                                    ocpa = ocpa_enrich(str(pid))
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
                                except Exception as e:
                                    warnings.append(f"inline_ocpa_enrich_failed:{pid}:{e}")

                            pa_by_id = store.get_many(county=county_key, parcel_ids=parcel_ids)
                        except Exception as e:
                            warnings.append(f"inline_ocpa_batch_failed:{e}")

            hover_by_id = store.get_hover_fields_many(county=county_key, parcel_ids=parcel_ids)
        finally:
            store.close()

        # Compile filters (supports both list-form and object-form).
        raw_filters = payload.get("filters")
        filters = compile_filters(raw_filters)

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

        for feat in intersecting:
            pa = pa_by_id.get(feat.parcel_id)
            pa_dict = pa.to_dict() if pa is not None else None

            # Guardrail: if FDOR live mode is requested and enabled, do not emit
            # demo rows. Instead, skip rows we couldn't enrich and surface a reason.
            if live and provider_is_live and fdor_enabled and pa_dict is None:
                if live_error_reason is None:
                    live_error_reason = "fdor_no_attributes_for_some_parcels"
                continue
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

            # Provide stable, UI-friendly aliases for filtering.
            # These keys are treated as authoritative only when PA has a record.
            if pa is not None:
                try:
                    living = float(pa.living_sf or 0) or float(pa.building_sf or 0) or 0.0
                    fields["living_area_sqft"] = living if living > 0 else None
                except Exception:
                    fields["living_area_sqft"] = None
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
            zoning = ""
            land_use = ""
            property_class = ""
            living_area_sqft = None
            lot_size_sqft = None
            beds = None
            baths = None
            year_built = None
            last_sale_date = hover.get("last_sale_date")
            last_sale_price = hover.get("last_sale_price")

            if pa is not None:
                owner_name = "; ".join([n for n in (pa.owner_names or []) if n]) or owner_name
                situs_address = pa.situs_address or situs_address
                zoning = (pa.zoning or "").strip()
                land_use = (pa.use_type or pa.land_use_code or "").strip()
                property_class = (pa.property_class or "").strip()
                living_area_sqft = float(pa.living_sf or 0) or None
                if living_area_sqft is None:
                    living_area_sqft = float(pa.building_sf or 0) or None
                lot_size_sqft = float(pa.land_sf or 0) or None
                beds = int(pa.bedrooms) if int(pa.bedrooms or 0) > 0 else None
                baths = float(pa.bathrooms) if float(pa.bathrooms or 0) > 0 else None
                year_built = int(pa.year_built) if int(pa.year_built or 0) > 0 else None
                last_sale_date = pa.last_sale_date or last_sale_date
                last_sale_price = float(pa.last_sale_price or 0) or last_sale_price

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
            if pa is not None:
                raw_source_url = str(pa.source_url or "")
                data_sources = getattr(pa, "sources", None) or []
                field_provenance = getattr(pa, "field_provenance", None) or {}

            rec = {
                "parcel_id": feat.parcel_id,
                "county": county_key,
                "situs_address": situs_address,
                "owner_name": owner_name,
                "land_use": land_use,
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
                "land_value": land_value,
                "building_value": building_value,
                "total_value": total_value,
                # Back-compat fields (older UI code paths)
                "address": situs_address,
                "flu": land_use,
                "property_class": property_class,
                "living_area_sqft": living_area_sqft,
                "lot_size_sqft": lot_size_sqft,
            }
            lat, lng = _centroid_lat_lng(feat.geometry)
            rec["lat"] = lat
            rec["lng"] = lng
            if include_geometry:
                rec["geometry"] = feat.geometry
            records.append(rec)

            if len(results) >= limit:
                break

        if live_error_reason:
            warnings.append(f"live_error_reason: {live_error_reason}")

        return JSONResponse(
            {
                # Backwards-compatible keys
                "county": county_key,
                "count": len(results),
                "results": results,
                # New UI payload
                "summary": {
                    "count": len(records),
                    "source_counts": source_counts,
                    "source_counts_legacy": legacy_source_counts,
                },
                "records": records,
                "warnings": warnings,
                "error_reason": live_error_reason,
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
                    "beds": int(pa.bedrooms) if int(pa.bedrooms or 0) > 0 else None,
                    "baths": float(pa.bathrooms) if float(pa.bathrooms or 0) > 0 else None,
                    "year_built": int(pa.year_built) if int(pa.year_built or 0) > 0 else None,
                    "last_sale_date": pa.last_sale_date,
                    "last_sale_price": float(pa.last_sale_price or 0) or None,
                    "land_value": land_value,
                    "building_value": building_value,
                    "total_value": total_value,
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
