from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import json
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
        live = bool(payload.get("live", False))
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
        try:
            parcel_ids = [f.parcel_id for f in intersecting]
            pa_by_id = store.get_many(county=county_key, parcel_ids=parcel_ids)

            if live:
                import re

                from florida_property_scraper.backend.native_adapter import NativeAdapter
                from florida_property_scraper.pa.schema import PAProperty

                def _as_float(v: object) -> float:
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
                live_cap = int(os.getenv("LIVE_PARCEL_ENRICH_LIMIT", "40"))
                if live_cap < 0:
                    live_cap = 0
                enrich_ids = missing_ids[: min(live_cap, limit)]
                if enrich_ids:
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
                        )
                        try:
                            store.upsert(pa_rec)
                        except Exception:
                            continue

                    # Refresh after best-effort enrichment.
                    pa_by_id = store.get_many(county=county_key, parcel_ids=parcel_ids)

            hover_by_id = store.get_hover_fields_many(county=county_key, parcel_ids=parcel_ids)
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
        records = []
        source_counts: dict[str, int] = {"local": 0, "live": 0, "geojson": 0, "missing": 0}
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

            # Enriched record payload for the modern UI.
            source = "local" if pa_dict else ("live" if live else "missing")

            # Best-effort: Orange geojson includes a lightweight meta cache we can use
            # when PA DB doesn't have a record (still not full PA).
            if source == "missing" and getattr(provider, "county", "") == "orange":
                meta = getattr(provider, "_meta", {}).get(feat.parcel_id) if hasattr(provider, "_meta") else None
                if isinstance(meta, dict):
                    source = "geojson"
                    if not hover.get("situs_address"):
                        hover["situs_address"] = meta.get("situs") or ""
                    if not hover.get("owner_name"):
                        hover["owner_name"] = meta.get("owner") or ""
                    if not hover.get("last_sale_date"):
                        hover["last_sale_date"] = meta.get("sale_date")
                    if not hover.get("last_sale_price"):
                        try:
                            hover["last_sale_price"] = float(meta.get("sale_price") or 0)
                        except Exception:
                            pass

            source_counts[source] = int(source_counts.get(source, 0)) + 1

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

            rec = {
                "parcel_id": feat.parcel_id,
                "county": county_key,
                "address": situs_address,
                "situs_address": situs_address,
                "owner_name": owner_name,
                "property_class": property_class,
                "land_use": land_use,
                "flu": land_use,
                "zoning": zoning,
                "living_area_sqft": living_area_sqft,
                "lot_size_sqft": lot_size_sqft,
                "beds": beds,
                "baths": baths,
                "year_built": year_built,
                "last_sale_date": last_sale_date,
                "last_sale_price": last_sale_price,
                "source": source,
            }
            lat, lng = _centroid_lat_lng(feat.geometry)
            rec["lat"] = lat
            rec["lng"] = lng
            if include_geometry:
                rec["geometry"] = feat.geometry
            records.append(rec)

            if len(results) >= limit:
                break

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
                },
                "records": records,
                "warnings": warnings,
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
        init_db(os.getenv("LEADS_SQLITE_PATH", "./leads.sqlite"))
