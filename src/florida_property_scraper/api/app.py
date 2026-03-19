from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Response, Request
from fastapi.exceptions import RequestValidationError
from fastapi.exception_handlers import (
    http_exception_handler as _default_http_exception_handler,
    request_validation_exception_handler as _default_validation_exception_handler,
)
from fastapi.responses import RedirectResponse, FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import json
import hashlib
import logging
import os
import re
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any

from florida_property_scraper.debug.process_watch import install_process_watch, record_shutdown_event
from florida_property_scraper.api.geojson import to_featurecollection
from florida_property_scraper.cache import cache_get, cache_set
from florida_property_scraper.feature_flags import get_flags
from florida_property_scraper.map_layer.registry import get_provider
from florida_property_scraper.parcels.geometry_provider import parse_bbox
from florida_property_scraper.parcels.geometry_registry import (
    get_provider as get_geometry_provider,
)
from florida_property_scraper.enrichment.providers.registry import (
    get_property_provider,
    get_property_providers,
)
from florida_property_scraper.enrichment.providers.model import compute_request_fingerprint
from florida_property_scraper.triggers.evidence_rules import evaluate_triggers_from_evidence
from florida_property_scraper.routers.registry import get_router
from florida_property_scraper.registry import get_county as get_registry_county
from florida_property_scraper.registry import normalize_county_slug, registry_payload
from florida_property_scraper.user_meta.storage import UserMetaSQLite, empty_user_meta


REPO_ROOT = Path(__file__).resolve().parents[3]
WEB_DIST = REPO_ROOT / "web" / "dist"
_router = get_router("fl")
assert _router is not None

DEFAULT_PARCELS_DB = str(REPO_ROOT / "data" / "parcels" / "parcels.sqlite")
DEFAULT_LEADS_DB = str(REPO_ROOT / "leads.sqlite")

_APP_START_TS = time.time()

_LAST_ERROR_ID: str | None = None
_LAST_ERROR_TIME: str | None = None

_RUNTIME_AUDIT_STATE: dict[str, Any] = {
    "watchlists": {
        "enabled": False,
        "interval_s": None,
        "last_started_at": None,
        "last_completed_at": None,
        "last_duration_ms": None,
        "last_error": None,
        "tick_count": 0,
        "last_summary": {},
    },
    "statewide_refresh": {
        "enabled": False,
        "interval_s": None,
        "last_started_at": None,
        "last_completed_at": None,
        "last_duration_ms": None,
        "last_error": None,
        "tick_count": 0,
        "preloaded_once": False,
        "last_summary": {},
    },
}

install_process_watch()


def _resolve_db_path(env_key: str, default_path: str) -> str:
    raw = os.getenv(env_key) or default_path
    if not raw:
        raw = default_path
    p = Path(str(raw))
    if not p.is_absolute():
        p = REPO_ROOT / p
    return str(p)


def _db_exists(path: str) -> bool:
    try:
        return bool(path and Path(path).exists())
    except Exception:
        return False


def _table_exists(conn: Any, table_name: str) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (str(table_name),),
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _record_last_error(error_id: str) -> None:
    global _LAST_ERROR_ID
    global _LAST_ERROR_TIME
    _LAST_ERROR_ID = str(error_id)
    _LAST_ERROR_TIME = datetime.now(timezone.utc).isoformat()


def _runtime_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _runtime_parse_iso(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _runtime_age_seconds(value: str | None) -> int | None:
    dt = _runtime_parse_iso(value)
    if dt is None:
        return None
    try:
        age = int((datetime.now(timezone.utc) - dt).total_seconds())
    except Exception:
        return None
    if age < 0:
        return 0
    return age


def _search_explain_contract(
    *,
    counts: dict[str, int] | None = None,
    warnings: list[str] | None = None,
    timings_ms: dict[str, int] | None = None,
    filter_metrics: list[dict[str, Any]] | None = None,
    field_stats: dict[str, Any] | None = None,
    debug_ids_sample: list[str] | None = None,
    error_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "counts": counts or {},
        "warnings": warnings or [],
        "timings_ms": timings_ms or {},
        "filter_metrics": filter_metrics or [],
        "field_stats": field_stats or {},
        "debug_ids_sample": debug_ids_sample or [],
    }
    if error_id:
        payload["error_id"] = str(error_id)
    if details:
        payload["details"] = details
    return payload


def _search_empty_payload(
    *,
    warnings: list[str] | None = None,
    error_id: str | None = None,
    search_id: str | None = None,
    correlation_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    warn = warnings or []
    counts = {
        "total_count": 0,
        "returned_count": 0,
        "candidate_count": 0,
        "filtered_count": 0,
    }
    explain = _search_explain_contract(
        counts=counts,
        warnings=warn,
        timings_ms={},
        filter_metrics=[],
        field_stats={},
        debug_ids_sample=[],
        error_id=error_id,
        details=details,
    )
    return {
        "ok": True,
        "data_quality": {"mode": "evidence_only"},
        "hover_fields_mode": "evidence_only",
        "search_id": search_id or (error_id or ""),
        "correlation_id": correlation_id or (error_id or ""),
        "records": [],
        "total_count": 0,
        "returned_count": 0,
        "has_more": False,
        "next_cursor": None,
        "warnings": warn,
        "explain": explain,
    }
def _bad_request_response(detail: str, hint: str = "") -> JSONResponse:
    return JSONResponse(
        {
            "ok": False,
            "error": "bad_request",
            "detail": str(detail),
            "hint": str(hint),
        },
        status_code=400,
    )


class ParcelsGeometryRequest(BaseModel):
    county: str
    parcel_ids: list[str]


class EnrichRequest(BaseModel):
    county: str
    parcel_ids: list[str]
    providers: list[str] | None = None
    provider_keys: list[str] | None = None
    dry_run: bool | None = None
    fixture_mode: bool | None = None


class ManualEvidenceItem(BaseModel):
    field: str
    value: Any
    source_type: str
    source_url: str
    confidence_label: str | None = None
    fetched_at: str | None = None
    content_hash: str | None = None
    extract_method: str | None = None
    raw_reference: str | None = None
    raw_snippet: str | None = None


class ManualIngestRequest(BaseModel):
    county: str
    parcel_id: str
    provider_key: str | None = None
    fetched_at: str | None = None
    evidence: list[ManualEvidenceItem]


class ManualEvidenceIn(BaseModel):
    field: str
    value: Any
    source_url: str
    source_label: str | None = None
    source_type: str
    confidence_label: str
    extract_method: str
    content_hash: str | None = None
    raw_reference: str | None = None
    fetched_at: str | None = None


class ManualIngestRequest(BaseModel):
    county: str
    parcel_id: str
    provider_key: str | None = None
    evidence: list[ManualEvidenceIn]
    fetched_at: str | None = None


class EvidenceSourceOut(BaseModel):
    source_type: str | None = None
    url: str | None = None
    label: str | None = None


class EvidenceOut(BaseModel):
    provider_key: str
    provider_id: str
    provider_name: str
    county: str
    parcel_id: str
    field: str
    value: Any
    confidence: float
    confidence_label: str
    source: EvidenceSourceOut
    fetched_at: str
    retrieved_at: str
    content_hash: str
    extract_method: str
    raw_reference: str | None = None
    raw_ref: int | None = None


class ArtifactRefOut(BaseModel):
    raw_id: int | None = None
    url: str
    content_type: str | None = None
    sha256: str
    body_bytes: int
    storage_path: str | None = None


class ProviderResultOut(BaseModel):
    ok: bool
    provider_key: str
    county: str
    parcel_id: str | None = None
    status: str
    fetched_at: str
    evidence_ids: list[int]
    raw_artifacts: list[ArtifactRefOut]
    warnings: list[str]
    errors: list[str]


class EnrichResponse(BaseModel):
    ok: bool
    mode: str
    dry_run: bool
    enriched: list[str]
    evidence: list[EvidenceOut]
    merged_fields: dict[str, dict[str, Any]]
    provider_results: list[ProviderResultOut]


class TriggerEvaluateRequest(BaseModel):
    county: str
    parcel_ids: list[str]


class TriggerEvaluateItem(BaseModel):
    trigger_key: str
    trigger_id: str | None = None
    parcel_id: str
    fired: bool
    severity: int
    reason: str
    evidence_ids: list[int]
    fields_used: list[str]
    evaluated_at: str


class TriggerEvaluateResponse(BaseModel):
    ok: bool
    county: str
    results: list[TriggerEvaluateItem]


class ProviderCatalogEntryOut(BaseModel):
    provider_id: str
    category: str
    method: str
    supports_counties: list[str]
    target_ids: list[str]
    base_url: str
    supported_fields: list[str]
    rate_limit: dict[str, int] | None = None
    status: str
    notes: str | None = None


class ProviderCatalogResponse(BaseModel):
    ok: bool
    county: str
    providers: list[ProviderCatalogEntryOut]


class ManualIngestResponse(BaseModel):
    ok: bool
    provider_key: str
    parcel_id: str
    evidence_ids: list[int]
    warnings: list[str]
    errors: list[str]


class ProviderStatusEntry(BaseModel):
    provider_id: str
    category: str
    base_url: str
    status: str
    last_run_at: str | None = None
    last_status: str | None = None
    last_error: str | None = None


class ProviderStatusResponse(BaseModel):
    ok: bool
    county: str
    providers: list[ProviderStatusEntry]


def _compact_fields(fields: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in (fields or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[str(k)] = v
    return out


def _maybe_url(value: Any) -> str | None:
    try:
        s = str(value or "").strip()
        if not s:
            return None
        if s.startswith("http://") or s.startswith("https://"):
            return s
    except Exception:
        return None
    return None


def _evidence_hash(
    *,
    county: str,
    parcel_id: str,
    provider_id: str,
    source: str,
    source_url: str | None,
    fields: dict[str, Any],
) -> str:
    payload = {
        "county": str(county or "").strip().lower(),
        "parcel_id": str(parcel_id or "").strip(),
        "provider_id": str(provider_id or "").strip().lower(),
        "source": str(source or "").strip().lower(),
        "source_url": str(source_url or "").strip(),
        "fields": fields or {},
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _trigger_result_hash(
    *,
    county: str,
    parcel_id: str,
    trigger_id: str,
    reason: str,
    evidence_ids: list[int],
) -> str:
    payload = {
        "county": str(county or "").strip().lower(),
        "parcel_id": str(parcel_id or "").strip(),
        "trigger_id": str(trigger_id or "").strip(),
        "reason": str(reason or "").strip(),
        "evidence_ids": sorted({int(x) for x in evidence_ids if int(x) > 0}),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _run_with_retries(
    fn,
    *,
    retries: int = 2,
    sleep_s: float = 0.25,
    logger: logging.Logger | None = None,
    label: str = "",
) -> Any:
    last_err: Exception | None = None
    for attempt in range(int(retries) + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if logger is not None:
                logger.warning("retry %s (%s/%s): %s", label, attempt + 1, retries + 1, e)
            if attempt < int(retries):
                time.sleep(float(sleep_s) * float(attempt + 1))
    if last_err is not None:
        raise last_err
    raise RuntimeError("retry failed")


def _safe_error_response(*, error: str, detail: str, status_code: int = 500) -> JSONResponse:
    trace_id = uuid.uuid4().hex[:12]
    _record_last_error(trace_id)
    return JSONResponse(
        {
            "ok": False,
            "error": str(error),
            "detail": str(detail),
            "correlation_id": trace_id,
        },
        status_code=status_code,
    )


def _fixture_mode_default() -> bool:
    return str(os.getenv("FPS_PROVIDER_FIXTURES", "1")).strip().lower() in {"1", "true", "yes"}


def _health_payload() -> dict[str, Any]:
    sha = os.getenv("APP_GIT_SHA") or ""
    ok = True
    try:
        if not sha:
            import subprocess

            p = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(REPO_ROOT),
                text=True,
                capture_output=True,
            )
            if p.returncode == 0:
                sha = (p.stdout or "").strip()
    except Exception:
        sha = sha or ""

    payload: dict[str, Any] = {
        "status": "ok",
        "ok": bool(ok),
        "sha": sha or "dev",
        "uptime_s": int(max(0, time.time() - _APP_START_TS)),
    }
    return payload


def health():
    return _health_payload()


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

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://0.0.0.0:5173",
    ],
    allow_origin_regex=(
        r"(^https?://.*\.(app\.github\.dev|preview\.app\.github\.dev|githubpreview\.dev)$"
        r"|^https?://localhost:5173$"
        r"|^https?://127\.0\.0\.1:5173$"
        r"|^https?://0\.0\.0\.0:5173$)"
    ),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _include_error_stack(request: Request | None) -> bool:
    try:
        if str(os.getenv("FPS_DEBUG_ERRORS", "")).strip().lower() in {"1", "true", "yes"}:
            return True
    except Exception:
        pass
    if request is None:
        return False
    try:
        q = str(request.query_params.get("debug") or "").strip().lower()
        if q in {"1", "true", "yes"}:
            return True
    except Exception:
        return False
    return False


def _error_payload(
    *,
    message: str,
    exc_type: str,
    path: str,
    trace_id: str,
    stack: str | None,
) -> dict[str, object]:
    error_obj: dict[str, object] = {
        "message": message,
        "type": exc_type,
        "path": path,
        "trace_id": trace_id,
    }
    if stack:
        error_obj["traceback"] = stack
    return {"error": error_obj}


def _should_wrap_error(request: Request | None) -> bool:
    if request is None:
        return False
    try:
        path = str(request.url.path or "")
    except Exception:
        return False
    if path.startswith("/api/parcels/search"):
        return True
    if path.startswith("/api/parcels/"):
        return True
    return False


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if not _should_wrap_error(request):
        return await _default_http_exception_handler(request, exc)
    try:
        msg = exc.detail if isinstance(exc.detail, str) else json.dumps(exc.detail)
    except Exception:
        msg = str(exc.detail)
    try:
        path = str(request.url.path or "")
    except Exception:
        path = ""
    if path.startswith("/api/parcels/search"):
        if int(exc.status_code or 400) == 400:
            return _bad_request_response(
                str(msg),
                hint="Check geometry/radius/center payload",
            )
        return JSONResponse(
            {
                "ok": False,
                "error": "http_error",
                "detail": str(msg),
                "hint": "",
            },
            status_code=int(exc.status_code) if exc.status_code else 400,
        )
    stack = None
    if _include_error_stack(request):
        import traceback

        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    trace_id = uuid.uuid4().hex[:12]
    return JSONResponse(
        _error_payload(
            message=str(msg),
            exc_type=exc.__class__.__name__,
            path=str(request.url.path),
            trace_id=trace_id,
            stack=stack,
        ),
        status_code=int(exc.status_code) if exc.status_code else 400,
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    if not _should_wrap_error(request):
        return await _default_validation_exception_handler(request, exc)
    try:
        path = str(request.url.path or "")
    except Exception:
        path = ""
    if path.startswith("/api/parcels/search"):
        return _bad_request_response("Validation error", hint="Invalid or missing JSON body")
    stack = None
    if _include_error_stack(request):
        import traceback

        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    trace_id = uuid.uuid4().hex[:12]
    return JSONResponse(
        _error_payload(
            message="Validation error",
            exc_type=exc.__class__.__name__,
            path=str(request.url.path),
            trace_id=trace_id,
            stack=stack,
        ),
        status_code=422,
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    if not _should_wrap_error(request):
        return JSONResponse(
            {"detail": "Internal Server Error"},
            status_code=500,
        )
    try:
        path = str(request.url.path or "")
    except Exception:
        path = ""
    if path.startswith("/api/parcels/search"):
        error_id = uuid.uuid4().hex[:12]
        _record_last_error(error_id)
        logging.getLogger("fps.api").exception("Unhandled search exception %s", error_id)
        return JSONResponse(
            _search_empty_payload(
                warnings=["internal_error"],
                error_id=error_id,
                search_id=error_id,
                correlation_id=error_id,
                details={"path": path},
            ),
            status_code=200,
        )
    explain_enabled = False
    try:
        q = str(request.query_params.get("explain") or "").strip().lower()
        if q in {"1", "true", "yes"}:
            explain_enabled = True
    except Exception:
        explain_enabled = False
    if not explain_enabled:
        try:
            raw_body = await request.body()
            if raw_body:
                try:
                    payload = json.loads(raw_body)
                    if payload.get("explain") is True:
                        explain_enabled = True
                    if not explain_enabled:
                        raw_explain = str(payload.get("explain") or "").strip().lower()
                        if raw_explain in {"1", "true", "yes"}:
                            explain_enabled = True
                except Exception:
                    pass
        except Exception:
            pass
    if explain_enabled and str(os.getenv("FPS_EXPLAIN_ERRORS", "1")).strip() != "0":
        logging.getLogger("fps.api").exception("Unhandled exception (explain)")
        import traceback

        return JSONResponse(
            {
                "ok": False,
                "error": str(exc),
                "where": str(request.url.path),
                "trace": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            },
            status_code=200,
        )
    logging.getLogger("fps.api").exception("Unhandled exception")
    stack = None
    if _include_error_stack(request):
        import traceback

        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    trace_id = uuid.uuid4().hex[:12]
    _record_last_error(trace_id)
    return JSONResponse(
        _error_payload(
            message=str(exc),
            exc_type=exc.__class__.__name__,
            path=str(request.url.path),
            trace_id=trace_id,
            stack=stack,
        ),
        status_code=500,
    )


if app:
    logger = logging.getLogger("fps.api")

    search_router = None
    permits_router = None
    lookup_router = None
    triggers_router = None
    watchlists_router = None

    try:
        from florida_property_scraper.api.routes.search import router as search_router
    except Exception as e:
        logger.exception("search router import failed: %s", e)
        search_router = None
    try:
        from florida_property_scraper.api.routes.permits import router as permits_router
    except Exception as e:
        logger.exception("permits router import failed: %s", e)
        permits_router = None
    try:
        from florida_property_scraper.api.routes.lookup import router as lookup_router
    except Exception as e:
        logger.exception("lookup router import failed: %s", e)
        lookup_router = None
    try:
        from florida_property_scraper.api.routes.triggers import router as triggers_router
    except Exception as e:
        logger.exception("triggers router import failed: %s", e)
        triggers_router = None
    try:
        from florida_property_scraper.api.routes.watchlists import router as watchlists_router
    except Exception as e:
        logger.exception("watchlists router import failed: %s", e)
        watchlists_router = None

    if search_router is not None:
        app.include_router(search_router, prefix="/api")
    else:
        logger.warning("search router missing; /api/parcels/search will be unavailable")

    if permits_router is not None:
        app.include_router(permits_router, prefix="/api")
    else:
        logger.warning("permits router missing; /api/permits endpoints unavailable")

    if lookup_router is not None:
        app.include_router(lookup_router, prefix="/api")
    else:
        logger.warning("lookup router missing; /api/lookup endpoints unavailable")

    if triggers_router is not None:
        app.include_router(triggers_router, prefix="/api")
    else:
        logger.warning("triggers router missing; /api/triggers endpoints unavailable")

    if watchlists_router is not None:
        app.include_router(watchlists_router, prefix="/api")
    else:
        logger.warning("watchlists router missing; /api/watchlists endpoints unavailable")

    @app.get("/health")
    def health_route():
        return health()

    @app.get("/api/health")
    def api_health_route():
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

        db_path = _resolve_db_path("PA_DB", DEFAULT_LEADS_DB)
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
        """

        county_key = (payload.county or "").strip().lower()
        parcel_ids = [str(pid).strip() for pid in (payload.parcel_ids or []) if str(pid).strip()]
        parcel_ids = parcel_ids[:50]

        if not parcel_ids:
            return JSONResponse({"type": "FeatureCollection", "features": []})

        # Supported counties:
        # - seminole: read geometry from parcels.sqlite (pre-ingested GeoJSON)
        # - orange: live FDOR polygon fetch (existing behavior)
        if county_key == "seminole":
            import sqlite3, json as _json
            from florida_property_scraper.pa.storage import PASQLite

            parcels_db = os.getenv("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
            if not parcels_db:
                raise HTTPException(status_code=500, detail="PARCELS_DB_PATH is not set")

            con = sqlite3.connect(parcels_db)
            cur = con.cursor()
            q = (
                "SELECT parcel_id, geom_geojson "
                "FROM parcels "
                "WHERE county=? AND parcel_id IN ({})"
            ).format(",".join(["?"] * len(parcel_ids)))
            rows = cur.execute(q, ["seminole", *parcel_ids]).fetchall()
            con.close()

            geom_by_id = {}
            for pid, gj in rows:
                if not gj:
                    continue
                try:
                    geom_by_id[str(pid)] = _json.loads(gj)
                except Exception:
                    pass

            db_path = _resolve_db_path("PA_DB", DEFAULT_LEADS_DB)
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

        if county_key != "orange":
            raise HTTPException(status_code=404, detail="Parcel geometry not available for this county yet")

        from florida_property_scraper.parcels.live.fdor_parcel_polygons import FDORParcelPolygonClient
        from florida_property_scraper.pa.storage import PASQLite

        client = FDORParcelPolygonClient()
        geom_by_id = client.fetch_parcel_geometries(parcel_ids)

        db_path = _resolve_db_path("PA_DB", DEFAULT_LEADS_DB)
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

    def _search_payload_from_query(request: Request) -> dict[str, Any]:
        payload: dict[str, Any] = {}

        def _get(name: str) -> str:
            return str(request.query_params.get(name) or "").strip()

        def _get_int(name: str) -> int | None:
            raw = _get(name)
            if not raw:
                return None
            try:
                return int(raw)
            except Exception:
                return None

        payload["county"] = _get("county")
        if _get("include_geometry"):
            payload["include_geometry"] = _get("include_geometry").lower() in {"1", "true", "yes"}

        limit = _get_int("limit")
        if limit is not None:
            payload["limit"] = limit
        offset = _get_int("offset")
        if offset is not None:
            payload["offset"] = offset

        for key in ("cursor", "next_cursor", "sort", "q", "query", "search_text"):
            val = _get(key)
            if val:
                payload[key] = val

        for json_key in ("geometry", "polygon_geojson", "radius", "center"):
            raw = _get(json_key)
            if not raw:
                continue
            try:
                payload[json_key] = json.loads(raw)
            except Exception:
                continue

        return payload

    @app.get("/api/parcels/search")
    def api_parcels_search_get(request: Request):
        payload = _search_payload_from_query(request)
        has_geometry = any(k in payload for k in ("geometry", "polygon_geojson", "radius", "center"))
        has_text = bool(str(payload.get("search_text") or payload.get("q") or payload.get("query") or "").strip())
        if not has_geometry and not has_text:
            return JSONResponse(
                _search_empty_payload(
                    warnings=["missing_geometry"],
                    details={"hint": "Provide geometry/radius or q/search_text."},
                )
            )
        return _api_parcels_search_impl(request, payload)

    @app.get("/api/parcels/search_normalized")
    def api_parcels_search_normalized_get(request: Request):
        payload = _search_payload_from_query(request)
        has_geometry = any(k in payload for k in ("geometry", "polygon_geojson", "radius", "center"))
        has_text = bool(str(payload.get("search_text") or payload.get("q") or payload.get("query") or "").strip())
        if not has_geometry and not has_text:
            return JSONResponse(
                _search_empty_payload(
                    warnings=["missing_geometry"],
                    details={"hint": "Provide geometry/radius or q/search_text."},
                )
            )
        payload["normalized"] = True
        return _api_parcels_search_impl(request, payload)

    @app.post("/api/parcels/search")
    def api_parcels_search(request: Request, payload: dict = Body(default={})): 
        try:
            return _api_parcels_search_impl(request, payload)
        except HTTPException:
            raise
        except BaseException as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise
            error_id = uuid.uuid4().hex[:12]
            correlation_id = ""
            try:
                corr_payload = str(payload.get("correlation_id") or payload.get("request_id") or "").strip()
                corr_query = str(request.query_params.get("correlation_id") or "").strip()
                corr_header = str(
                    request.headers.get("x-correlation-id")
                    or request.headers.get("x-request-id")
                    or ""
                ).strip()
                correlation_id = corr_payload or corr_query or corr_header or error_id
            except Exception:
                correlation_id = error_id
            try:
                import logging

                logging.getLogger("fps.search").exception("search_failed error_id=%s", error_id)
            except Exception:
                pass
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "error": {
                        "type": "server_error",
                        "code": "server_error",
                        "message": str(exc)[:500],
                    },
                    "error_code": "server_error",
                    "correlation_id": correlation_id,
                    "detail": str(exc)[:500],
                    "hint": "See .logs/backend_signals.log and server logs for the error_id.",
                    "where": "api_parcels_search",
                    "error_id": error_id,
                },
            )

    @app.post("/api/parcels/search_normalized")
    def api_parcels_search_normalized(request: Request, payload: dict = Body(default={})): 
        try:
            payload = payload if isinstance(payload, dict) else {}
            payload["normalized"] = True
            return _api_parcels_search_impl(request, payload)
        except HTTPException:
            raise
        except BaseException as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise
            error_id = uuid.uuid4().hex[:12]
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "error": {
                        "type": "server_error",
                        "code": "server_error",
                        "message": str(exc)[:500],
                    },
                    "error_code": "server_error",
                    "correlation_id": error_id,
                    "detail": str(exc)[:500],
                    "hint": "See .logs/backend_signals.log and server logs for the error_id.",
                    "where": "api_parcels_search_normalized",
                    "error_id": error_id,
                },
            )

    def _api_parcels_search_impl(request: Request, payload: dict = Body(default={})): 
        # WRITE_UI_REQ_JSON: debug dump last UI payload to /tmp/ui_req.json
        try:
            import json as _json
            open("/tmp/last_request.json", "w", encoding="utf-8").write(_json.dumps(payload))
        except Exception:
            pass
        if payload is None or not isinstance(payload, dict):
            return _bad_request_response("payload must be a JSON object", hint="Send an object body")
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


        def _explain_error_response(exc: Exception, where: str) -> JSONResponse:
            detail = f"{where}: {exc}" if where else str(exc)
            return _bad_request_response(detail, hint="Check polygon/radius/center payload")

        debug_ids_sample: list[str] = []

        # Ensure these are ALWAYS defined (used later in multiple branches)
        provider_is_live = False
        fdor_enabled = os.getenv("FPS_USE_FDOR_CENTROIDS", "").strip() in {"1","true","True"}
        signal_filter_active = False


        search_id = uuid.uuid4().hex[:12]

        correlation_id = ""
        try:
            corr_payload = str(payload.get("correlation_id") or payload.get("request_id") or "").strip()
            corr_query = str(request.query_params.get("correlation_id") or "").strip()
            corr_header = str(
                request.headers.get("x-correlation-id")
                or request.headers.get("x-request-id")
                or ""
            ).strip()
            correlation_id = corr_payload or corr_query or corr_header or search_id
        except Exception:
            correlation_id = search_id

        explain_enabled = False
        try:
            if payload.get("explain") is True:
                explain_enabled = True
            if not explain_enabled:
                raw_explain = str(payload.get("explain") or "").strip().lower()
                if raw_explain in {"1", "true", "yes"}:
                    explain_enabled = True
            if not explain_enabled and request is not None:
                q = str(request.query_params.get("explain") or "").strip().lower()
                if q in {"1", "true", "yes"}:
                    explain_enabled = True
        except Exception:
            explain_enabled = False

        debug_response_enabled = payload.get("debug") is True
        debug_timing_ms: dict[str, int] | None = None
        debug_counts: dict[str, Any] | None = None
        _timing_mark = None
        _timing_last = None
        if debug_response_enabled or explain_enabled:
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

        def _canonical_lot_sizes(land_sf_raw: object, land_acres_raw: object) -> tuple[float | None, float | None]:
            def _pos_num(v: object) -> float | None:
                try:
                    if v is None:
                        return None
                    n = float(v)
                    if n <= 0:
                        return None
                    return n
                except Exception:
                    return None

            lot_sqft = _pos_num(land_sf_raw)
            lot_acres = _pos_num(land_acres_raw)

            if lot_sqft is None and lot_acres is None:
                return None, None
            if lot_sqft is None and lot_acres is not None:
                return lot_acres * 43560.0, lot_acres
            if lot_acres is None and lot_sqft is not None:
                return lot_sqft, (lot_sqft / 43560.0)

            # Both values exist: reconcile obvious unit drift before emitting/filtering.
            expected_sqft = float(lot_acres or 0) * 43560.0
            if expected_sqft <= 0:
                return lot_sqft, lot_acres

            ratio = float(lot_sqft or 0) / expected_sqft
            if 0.8 <= ratio <= 1.25:
                return lot_sqft, lot_acres

            # Common ingest bug: land_sf carries acres value. Convert to sqft.
            same_units = abs(float(lot_sqft or 0) - float(lot_acres or 0)) <= max(0.1, 0.02 * float(lot_acres or 0))
            tiny_sqft_vs_acres = float(lot_sqft or 0) <= 50.0 and float(lot_acres or 0) >= 0.5
            if same_units or tiny_sqft_vs_acres:
                return expected_sqft, lot_acres

            # Fallback: trust sqft and derive acres deterministically.
            return lot_sqft, (float(lot_sqft or 0) / 43560.0)

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
                    "correlation_id": correlation_id,
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
            apply_filters_explain,
            compile_filters,
            compile_triggers,
            eval_condition,
            eval_triggers,
            effective_living_sqft,
            normalize_property_type,
        )
        from florida_property_scraper.parcels.geometry_search import (
            circle_polygon,
            geometry_bbox,
            intersects,
            match_geometry,
        )
        from florida_property_scraper.pa.storage import PASQLite
        from florida_property_scraper.pa.ui_computed import compute_ui_fields

        raw_county = (payload.get("county") or "").strip().lower()
        raw_filters_for_text = payload.get("filters") if isinstance(payload.get("filters"), dict) else {}
        raw_search_text = ""
        try:
            raw_search_text = str(
                raw_filters_for_text.get("search_text")
                or raw_filters_for_text.get("q")
                or payload.get("search_text")
                or payload.get("q")
                or payload.get("query")
                or ""
            )
        except Exception:
            raw_search_text = ""
        search_text = " ".join(raw_search_text.split()).strip()
        text_only_mode = False

        parcels_db_path = _resolve_db_path("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
        leads_db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
        parcels_db_ok = _db_exists(parcels_db_path)
        leads_db_ok = _db_exists(leads_db_path)

        if not parcels_db_ok:
            pre_warnings.append("parcels_db_missing")
        if not leads_db_ok:
            pre_warnings.append("leads_db_missing")

        geojson_dir_env = str(os.getenv("PARCEL_GEOJSON_DIR", "") or "").strip()
        geojson_dir = Path(geojson_dir_env) if geojson_dir_env else (REPO_ROOT / "data" / "parcels")
        use_geojson_override = bool(geojson_dir_env)
        geojson_available = False
        try:
            geojson_available = any(geojson_dir.glob("*.geojson"))
        except Exception:
            geojson_available = False

        if not raw_county and not parcels_db_ok and not geojson_available:
            pre_warnings.append("multi_county_not_available")
            return JSONResponse(
                _search_empty_payload(
                    warnings=pre_warnings,
                    search_id=search_id,
                    correlation_id=correlation_id,
                    details={"reason": "no_geometry_sources"},
                ),
                status_code=200,
            )

        def _available_geometry_counties() -> list[str]:
            counties: set[str] = set()
            try:
                data_dir = geojson_dir
                db_path = Path(parcels_db_path)
                if db_path.exists():
                    try:
                        import sqlite3 as _sqlite3

                        conn = _sqlite3.connect(str(db_path))
                        conn.row_factory = _sqlite3.Row
                        rows = conn.execute("SELECT DISTINCT county FROM parcels").fetchall()
                        for row in rows:
                            c = str(row["county"] or "").strip().lower()
                            if c:
                                counties.add(c)
                        conn.close()
                    except Exception:
                        pass
                for p in data_dir.glob("*.geojson"):
                    c = p.stem.strip().lower()
                    if c:
                        counties.add(c)
            except Exception:
                pass
            if not counties:
                return ["seminole"]
            return sorted(counties)

        county_keys = [raw_county] if raw_county else _available_geometry_counties()
        if not county_keys:
            county_keys = ["seminole"]
        live_env_enabled = os.getenv("FPS_USE_FDOR_CENTROIDS", "").strip() in {
            "1",
            "true",
            "True",
        }
        # When live mode is requested without a county, treat it as a statewide
        # search and route through FDOR centroid coverage.
        if live_env_enabled and not raw_county and bool(payload.get("live", False)):
            county_keys = ["statewide"]
        multi_county = bool(len(county_keys) > 1 or not raw_county)
        county_key = raw_county or county_keys[0] or "seminole"
        county_label = "all" if multi_county else county_key
        requested_live = False
        if "live" in payload:
            requested_live = bool(payload.get("live", False))
        else:
            requested_live = live_env_enabled
        live = bool(requested_live)
        include_geometry = bool(payload.get("include_geometry", False))
        limit = None
        try:
            raw_limit = payload.get("limit")
            if raw_limit is not None and str(raw_limit).strip() != "":
                limit = int(raw_limit)
        except Exception:
            limit = None
        if limit is not None and limit <= 0:
            limit = None

        max_limit = int(os.getenv("FPS_SEARCH_MAX_LIMIT", "2000") or 2000)
        if max_limit <= 0:
            max_limit = 2000

        # Guardrail: never allow unbounded result sets.
        if limit is not None and limit > max_limit:
            limit = max_limit

        # Guardrail: live mode can be expensive if/when implemented.
        if live and limit is not None and limit > max_limit:
            limit = max_limit

        cursor = None
        offset = None
        try:
            raw_cursor = payload.get("cursor") or payload.get("next_cursor")
            if raw_cursor is not None:
                cursor = str(raw_cursor).strip() or None
        except Exception:
            cursor = None
        try:
            raw_offset = payload.get("offset")
            if raw_offset is not None and str(raw_offset).strip() != "":
                offset = int(raw_offset)
        except Exception:
            offset = None

        _mark("parse_payload")

        if debug_counts is not None:
            debug_counts.update(
                {
                    "county": county_label,
                    "live": bool(live),
                    "limit": int(limit) if limit is not None else int(max_limit),
                    "include_geometry": bool(include_geometry),
                    "has_filters": isinstance(payload.get("filters"), dict) and bool(payload.get("filters")),
                    "has_search_text": bool(search_text),
                    "enrich": payload.get("enrich", None),
                    "enrich_limit": payload.get("enrich_limit", None),
                    "polygon_match_mode": str(payload.get("polygon_match_mode") or "intersects"),
                }
            )

        try:
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
        except Exception as e:
            if explain_enabled and str(os.getenv("FPS_EXPLAIN_ERRORS", "1")).strip() != "0":
                return _explain_error_response(e, "parse_geometry")
            raise

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

        if geometry is None and search_text:
            text_only_mode = True
            # World bounds polygon lets us reuse the same candidate/filter pipeline.
            geometry = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-180.0, -90.0],
                        [180.0, -90.0],
                        [180.0, 90.0],
                        [-180.0, 90.0],
                        [-180.0, -90.0],
                    ]
                ],
            }

        if geometry is None:
            return _bad_request_response(
                "Missing geometry",
                hint="Provide geometry/radius or search_text/q for text-based search",
            )

        if not isinstance(geometry, dict):
            raise HTTPException(
                status_code=400, detail="geometry must be a GeoJSON geometry object"
            )
        try:
            gtype = str(geometry.get("type") or "").strip().lower()
            if gtype not in {"polygon", "multipolygon", "point"}:
                raise HTTPException(
                    status_code=400,
                    detail="geometry must be Polygon, MultiPolygon, or Point",
                )
            if "coordinates" not in geometry:
                raise HTTPException(status_code=400, detail="geometry has no coordinates")
        except HTTPException:
            raise
        except Exception as e:
            if explain_enabled and str(os.getenv("FPS_EXPLAIN_ERRORS", "1")).strip() != "0":
                return _explain_error_response(e, "validate_geometry")
            raise
        geometry_type = gtype
        bbox_t = geometry_bbox(geometry)
        if bbox_t is None:
            raise HTTPException(status_code=400, detail="geometry has no coordinates")

        # AUTO-SWAP LAT/LNG: UI sometimes sends [lat,lng] instead of GeoJSON [lng,lat]
        # Florida sanity: lon ~ -87..-79, lat ~ 24..31
        try:
            minx, miny, maxx, maxy = bbox_t
            if (24 <= float(minx) <= 31) and (-87 <= float(miny) <= -79) and (24 <= float(maxx) <= 31) and (-87 <= float(maxy) <= -79):
                bbox_t = (miny, minx, maxy, maxx)
        except Exception:
            pass


        # --- BEGIN PATCH: Use parcels.sqlite + RTree for polygon search ---
        import sqlite3
        import json as _json
        from types import SimpleNamespace

        intersecting = []
        provider_warnings = []
        candidates = []
        seminole_db = parcels_db_path
        seminole_db_exists = bool(seminole_db and os.path.exists(seminole_db))
        if use_geojson_override:
            # Deterministic test/dev mode: prefer explicit geojson directory over
            # workspace-wide parcels.sqlite sources.
            seminole_db_exists = False
        seminole_sql_count = None
        sqlite_counties: set[str] = set()

        def _wrap_candidate(pid: str, geom: object, ckey: str) -> SimpleNamespace:
            return SimpleNamespace(parcel_id=pid, geometry=geom, county=ckey)

        if text_only_mode and seminole_db_exists:
            db_path = seminole_db
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                leads_join = ""
                try:
                    if leads_db_ok and leads_db_path and os.path.exists(leads_db_path):
                        conn.execute("ATTACH DATABASE ? AS leads", (str(leads_db_path),))
                        leads_join = (
                            " LEFT JOIN leads.pa_properties lp"
                            " ON LOWER(COALESCE(lp.county,'')) = LOWER(COALESCE(p.county,''))"
                            " AND lp.parcel_id = p.parcel_id"
                        )
                except Exception:
                    leads_join = ""
                q = """
                    SELECT p.parcel_id, p.geom_geojson, p.county
                    FROM parcels p
                    LEFT JOIN parcels_pa pa
                      ON pa.county = p.county AND pa.parcel_id = p.parcel_id
                    WHERE 1=1
                """
                if leads_join:
                    q = q.replace("WHERE 1=1", f"{leads_join} WHERE 1=1")
                params: list[object] = []
                if not multi_county and county_key and county_key != "statewide":
                    q += " AND p.county = ?"
                    params.append(county_key)
                elif county_keys:
                    placeholders = ",".join(["?"] * len(county_keys))
                    q += f" AND p.county IN ({placeholders})"
                    params.extend(county_keys)

                tokens = [t.strip().lower() for t in re.split(r"\s+", search_text) if t.strip()]
                if not tokens:
                    tokens = [search_text.lower()]
                tokens = tokens[:6]

                for tok in tokens:
                    like = f"%{tok}%"
                    token_clause = (
                        " AND ("
                        "LOWER(p.parcel_id) LIKE ? OR "
                        "LOWER(COALESCE(pa.owner_name,'')) LIKE ? OR "
                        "LOWER(COALESCE(pa.situs_address,'')) LIKE ? OR "
                        "LOWER(COALESCE(pa.mailing_address,'')) LIKE ?"
                    )
                    if leads_join:
                        token_clause += " OR LOWER(COALESCE(lp.record_json,'')) LIKE ?"
                    token_clause += ")"
                    q += token_clause
                    token_params = [like, like, like, like]
                    if leads_join:
                        token_params.append(like)
                    params.extend(token_params)

                text_candidate_cap = int(min(max((limit or 500) * 25, 2500), 20000))
                q += " LIMIT ?"
                params.append(text_candidate_cap)

                rows = conn.execute(q, params).fetchall()
                seminole_sql_count = len(rows)
                for row in rows:
                    try:
                        parcel_geom = _json.loads(row["geom_geojson"])
                        ckey = str(row["county"] or "").strip().lower() or county_key
                        sqlite_counties.add(ckey)
                        candidates.append(_wrap_candidate(row["parcel_id"], parcel_geom, ckey))
                    except Exception:
                        continue
                conn.close()
            except Exception as e:
                provider_warnings.append(f"parcels_sqlite_error:{e}")

        elif geometry and seminole_db_exists:
            db_path = seminole_db
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                minx, miny, maxx, maxy = bbox_t
                q = """
                    SELECT p.parcel_id, p.geom_geojson, p.county
                    FROM parcels_rtree r
                    JOIN parcels p ON p.rowid = r.rowid
                    WHERE r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
                """
                params = [maxx, minx, maxy, miny]
                if not multi_county and county_key:
                    q += " AND p.county = ?"
                    params.append(county_key)
                elif county_keys:
                    placeholders = ",".join(["?"] * len(county_keys))
                    q += f" AND p.county IN ({placeholders})"
                    params.extend(county_keys)
                rows = conn.execute(q, params).fetchall()
                seminole_sql_count = len(rows)
                for row in rows:
                    try:
                        parcel_geom = _json.loads(row["geom_geojson"])
                        ckey = str(row["county"] or "").strip().lower() or county_key
                        sqlite_counties.add(ckey)
                        candidates.append(_wrap_candidate(row["parcel_id"], parcel_geom, ckey))
                    except Exception:
                        continue
                conn.close()
            except Exception as e:
                provider_warnings.append(f"parcels_sqlite_error:{e}")

        remaining_counties = [c for c in county_keys if c not in sqlite_counties]
        if multi_county:
            for ck in remaining_counties:
                try:
                    if live:
                        from florida_property_scraper.parcels.providers.fdor_centroids import (
                            FDORCentroidsProvider,
                        )

                        provider = FDORCentroidsProvider(county=ck)
                        provider.load()
                    else:
                        provider = get_geometry_provider(ck)
                    provider_is_live = provider.__class__.__name__ == "FDORCentroidsProvider"
                    fdor_enabled = live or os.getenv("FPS_USE_FDOR_CENTROIDS", "").strip() in {
                        "1",
                        "true",
                        "True",
                    }
                    cand = provider.query(bbox_t)
                    for f in cand:
                        candidates.append(_wrap_candidate(f.parcel_id, f.geometry, ck))
                except Exception as e:
                    try:
                        provider_warnings.append(f"geometry_provider_error:{type(e).__name__}")
                    except Exception:
                        provider_warnings.append("geometry_provider_error")
                    if provider_is_live and ck in {"orange", "seminole"}:
                        try:
                            from florida_property_scraper.parcels.geometry_registry import _default_geojson_dir
                            from florida_property_scraper.parcels.providers.orange import OrangeProvider
                            from florida_property_scraper.parcels.providers.seminole import SeminoleProvider
                            geo_dir = _default_geojson_dir()
                            if ck == "orange":
                                fallback = OrangeProvider(geojson_path=geo_dir / "orange.geojson")
                            else:
                                fallback = SeminoleProvider(geojson_path=geo_dir / "seminole.geojson")
                            fallback.load()
                            cand = fallback.query(bbox_t)
                            for f in cand:
                                candidates.append(_wrap_candidate(f.parcel_id, f.geometry, ck))
                            provider_warnings.append("geometry_provider_fallback:local_geojson")
                        except Exception:
                            pass
        elif not candidates:
            # fallback to original provider logic for other single counties
            if live:
                from florida_property_scraper.parcels.providers.fdor_centroids import (
                    FDORCentroidsProvider,
                )

                provider = FDORCentroidsProvider(county=county_key)
                provider.load()
            else:
                provider = get_geometry_provider(county_key)
            provider_is_live = provider.__class__.__name__ == "FDORCentroidsProvider"
            fdor_enabled = live or os.getenv("FPS_USE_FDOR_CENTROIDS", "").strip() in {
                "1",
                "true",
                "True",
            }
            try:
                cand = provider.query(bbox_t)
                candidates = [_wrap_candidate(f.parcel_id, f.geometry, county_key) for f in cand]
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
                        cand = fallback.query(bbox_t)
                        candidates = [_wrap_candidate(f.parcel_id, f.geometry, county_key) for f in cand]
                        provider_warnings.append("geometry_provider_fallback:local_geojson")
                    except Exception:
                        pass
            _mark("candidate_query")
        # Apply geometry clipping after attribute filters.
        intersecting = list(candidates)
        # --- END PATCH ---

        _mark("geometry_filter")

        candidate_ids_sample: list[str] = []
        try:
            candidate_ids_sample = [str(f.parcel_id) for f in intersecting[:10] if getattr(f, "parcel_id", None)]
        except Exception:
            candidate_ids_sample = []

        _append_search_debug(
            {
                "event": "request",
                "county": county_label,
                "payload": payload,
                "bbox": bbox_t,
                "candidates_count": len(candidates),
                "intersecting_count": len(intersecting),
            }
        )

        try:
            log_path = "/tmp/api.log"
            line = json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "trace_id": correlation_id,
                    "event": "search_request",
                    "county": county_key,
                    "bbox": bbox_t,
                    "candidates_count": len(candidates),
                    "seminole_db": seminole_db if county_key == "seminole" else None,
                    "seminole_db_exists": bool(seminole_db_exists) if county_key == "seminole" else None,
                    "seminole_sql_count": seminole_sql_count,
                },
                default=str,
            )
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

        warnings: list[str] = []
        if provider_warnings:
            warnings.extend(provider_warnings)
        if pre_warnings:
            warnings.extend(pre_warnings)
        if not candidates:
            warnings.append("No parcel candidates returned for bbox")

        def _centroid_lat_lng(geom: Any) -> tuple[float, float]:
            # Best-effort centroid without heavy deps.
            try:
                g = geom
                if isinstance(g, dict):
                    gtype = str(g.get("type") or "").lower()
                    coords = g.get("coordinates")
                else:
                    gtype = ""
                    coords = None

                def _ring_area(ring: list) -> float:
                    area = 0.0
                    if not ring or len(ring) < 3:
                        return area
                    for i in range(len(ring)):
                        x1, y1 = ring[i][0], ring[i][1]
                        x2, y2 = ring[(i + 1) % len(ring)][0], ring[(i + 1) % len(ring)][1]
                        area += (float(x1) * float(y2)) - (float(x2) * float(y1))
                    return area / 2.0

                def _ring_centroid(ring: list) -> tuple[float, float] | None:
                    if not ring or len(ring) < 3:
                        return None
                    a = _ring_area(ring)
                    if a == 0:
                        return None
                    cx = 0.0
                    cy = 0.0
                    for i in range(len(ring)):
                        x1, y1 = float(ring[i][0]), float(ring[i][1])
                        x2, y2 = float(ring[(i + 1) % len(ring)][0]), float(ring[(i + 1) % len(ring)][1])
                        cross = (x1 * y2) - (x2 * y1)
                        cx += (x1 + x2) * cross
                        cy += (y1 + y2) * cross
                    cx /= (6.0 * a)
                    cy /= (6.0 * a)
                    return cx, cy

                if gtype == "point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
                    lng = float(coords[0])
                    lat = float(coords[1])
                    return lat, lng

                def _centroid_from_rings(rings: list) -> tuple[float, float] | None:
                    best = None
                    best_area = 0.0
                    for ring in rings:
                        if not isinstance(ring, list) or len(ring) < 3:
                            continue
                        area = abs(_ring_area(ring))
                        if area <= 0:
                            continue
                        if area > best_area:
                            cent = _ring_centroid(ring)
                            if cent is not None:
                                best = cent
                                best_area = area
                    return best

                if gtype == "polygon" and isinstance(coords, list) and coords:
                    cent = _centroid_from_rings(coords)
                    if cent is not None:
                        return float(cent[1]), float(cent[0])

                if gtype == "multipolygon" and isinstance(coords, list) and coords:
                    best = None
                    best_area = 0.0
                    for poly in coords:
                        if not isinstance(poly, list) or not poly:
                            continue
                        cent = _centroid_from_rings(poly)
                        if cent is None:
                            continue
                        area = 0.0
                        try:
                            area = abs(_ring_area(poly[0]))
                        except Exception:
                            area = 0.0
                        if area > best_area:
                            best_area = area
                            best = cent
                    if best is not None:
                        return float(best[1]), float(best[0])

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
        db_path = _resolve_db_path("PA_DB", leads_db_path)
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
                "beds": 0,
                "baths": 0,
                "year_built": 0,
                "owner_name": 0,
                "owner_mailing_address": 0,
                "total_value": 0,
                "land_value": 0,
                "building_value": 0,
                "assessed_value": 0,
                "last_sale_date": 0,
                "last_sale_price": 0,
                "property_type": 0,
                "ownership_years": 0,
                "zoning": 0,
                "future_land_use": 0,
            },
        }
        rollups_by_id: dict[tuple[str, str], dict] = {}
        rollup_keys_by_id: dict[tuple[str, str], set[str]] = {}
        rollups_loaded = False
        parcel_table1_by_id: dict[tuple[str, str], dict] = {}
        parcels_pa_by_id: dict[tuple[str, str], dict] = {}
        pa_raw_by_id: dict[tuple[str, str], str] = {}
        pa_address_map_by_county: dict[str, dict[str, str]] = {}
        pa_raw_by_address_by_county: dict[str, dict[str, str]] = {}
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

            parcel_ids_by_county: dict[str, list[str]] = {}
            for f in intersecting:
                ckey = str(getattr(f, "county", "") or county_key).strip().lower() or county_key
                parcel_ids_by_county.setdefault(ckey, []).append(f.parcel_id)
            parcel_ids = [pid for ids in parcel_ids_by_county.values() for pid in ids]

            parcel_id_map_by_county: dict[str, dict[str, str]] = {}
            parcel_id_map_norm_by_county: dict[str, dict[str, str]] = {}
            pa_parcel_norm_by_county: dict[str, dict[str, str]] = {}
            parcel_id_map_norm_numeric_by_county: dict[str, dict[str, str]] = {}
            pa_parcel_norm_numeric_by_county: dict[str, dict[str, str]] = {}

            def _norm_pid(value: str) -> str:
                try:
                    s = str(value or "").strip().upper()
                    s = re.sub(r"[^A-Z0-9]", "", s)
                    s = s.lstrip("0") or s
                    return s
                except Exception:
                    return ""

            def _norm_pid_numeric(value: str) -> str:
                try:
                    s = str(value or "").strip()
                    s = re.sub(r"[^0-9]", "", s)
                    s = s.lstrip("0") or s
                    return s
                except Exception:
                    return ""

            def _load_parcel_id_map(ckey: str, geom_ids: list[str]) -> dict[str, str]:
                out: dict[str, str] = {}
                norm_out: dict[str, str] = {}
                norm_out_numeric: dict[str, str] = {}
                try:
                    import sqlite3 as _sqlite3

                    db_path = leads_db_path
                    if leads_db_ok and db_path and os.path.exists(db_path):
                        con = _sqlite3.connect(db_path)
                    con = _sqlite3.connect(leads_path)
                    try:
                        cur = con.cursor()
                        rows = cur.execute(
                            "SELECT geom_parcel_id, pa_parcel_id FROM parcel_id_map WHERE lower(county)=?",
                            (ckey,),
                        ).fetchall()
                        for row in rows:
                            try:
                                gpid = str(row[0] or "").strip()
                                ppid = str(row[1] or "").strip()
                                if gpid and ppid:
                                    out[gpid] = ppid
                                    norm = _norm_pid(gpid)
                                    if norm and norm not in norm_out:
                                        norm_out[norm] = ppid
                                        norm_num = _norm_pid_numeric(gpid)
                                        if norm_num and norm_num not in norm_out_numeric:
                                            norm_out_numeric[norm_num] = ppid
                                if ppid:
                                    out.setdefault(ppid, ppid)
                                    p_norm = _norm_pid(ppid)
                                    if p_norm and p_norm not in norm_out:
                                        norm_out[p_norm] = ppid
                                    p_norm_num = _norm_pid_numeric(ppid)
                                    if p_norm_num and p_norm_num not in norm_out_numeric:
                                        norm_out_numeric[p_norm_num] = ppid
                            except Exception:
                                continue
                    finally:
                        try:
                            con.close()
                        except Exception:
                            pass
                except Exception:
                    return {}
                parcel_id_map_norm_by_county[ckey] = norm_out
                parcel_id_map_norm_numeric_by_county[ckey] = norm_out_numeric
                return out

            def _pa_id_for_geom(ckey: str, geom_id: str) -> str:
                mapped = parcel_id_map_by_county.get(ckey, {}).get(geom_id)
                if mapped:
                    return mapped
                norm = _norm_pid(geom_id)
                if norm:
                    mapped = parcel_id_map_norm_by_county.get(ckey, {}).get(norm)
                    if mapped:
                        return mapped
                    mapped = pa_parcel_norm_by_county.get(ckey, {}).get(norm)
                    if mapped:
                        return mapped
                norm_num = _norm_pid_numeric(geom_id)
                if norm_num:
                    mapped = parcel_id_map_norm_numeric_by_county.get(ckey, {}).get(norm_num)
                    if mapped:
                        return mapped
                    mapped = pa_parcel_norm_numeric_by_county.get(ckey, {}).get(norm_num)
                    if mapped:
                        return mapped
                return geom_id

            def _pa_ids_for_geom_ids(ckey: str, geom_ids: list[str]) -> list[str]:
                ids: list[str] = []
                for gid in geom_ids:
                    pid = _pa_id_for_geom(ckey, gid)
                    if pid:
                        ids.append(pid)
                return list({pid for pid in ids if pid})

            for ckey, ids in parcel_ids_by_county.items():
                parcel_id_map_by_county[ckey] = _load_parcel_id_map(ckey, ids)

            try:
                import sqlite3 as _sqlite3

                db_path = leads_db_path
                if leads_db_ok and db_path and os.path.exists(db_path):
                    con = _sqlite3.connect(db_path)
                    con.row_factory = _sqlite3.Row
                    try:
                        for ckey in parcel_ids_by_county.keys():
                            rows = con.execute(
                                "SELECT parcel_id FROM pa_properties WHERE lower(county)=? OR lower(county) LIKE ?",
                                (ckey, f"%{ckey}%"),
                            ).fetchall()
                            norm_map: dict[str, str] = {}
                            for row in rows:
                                try:
                                    pid = str(row["parcel_id"] or "").strip()
                                    if not pid:
                                        continue
                                    norm = _norm_pid(pid)
                                    if norm and norm not in norm_map:
                                        norm_map[norm] = pid
                                    norm_num = _norm_pid_numeric(pid)
                                    if norm_num and norm_num not in pa_parcel_norm_numeric_by_county.get(ckey, {}):
                                        pa_parcel_norm_numeric_by_county.setdefault(ckey, {})[norm_num] = pid
                                except Exception:
                                    continue
                            pa_parcel_norm_by_county[ckey] = norm_map
                    finally:
                        con.close()
            except Exception:
                pa_parcel_norm_by_county = {}

            try:
                import sqlite3 as _sqlite3

                db_path = leads_db_path
                if leads_db_ok and db_path and os.path.exists(db_path):
                    con = _sqlite3.connect(db_path)
                    con.row_factory = _sqlite3.Row
                    try:
                        for ckey, ids in parcel_ids_by_county.items():
                            if not ids:
                                continue
                            pa_ids = _pa_ids_for_geom_ids(ckey, ids)
                            if not pa_ids:
                                continue
                            chunk = 900
                            for i in range(0, len(pa_ids), chunk):
                                batch = pa_ids[i : i + chunk]
                                placeholders = ",".join(["?"] * len(batch))
                                rows = con.execute(
                                    f"SELECT parcel_id, record_json FROM pa_properties WHERE (lower(county)=? OR lower(county) LIKE ?) AND parcel_id IN ({placeholders})",
                                    [ckey, f"%{ckey}%", *batch],
                                ).fetchall()
                                for row in rows:
                                    try:
                                        pid = str(row["parcel_id"] or "").strip()
                                        raw = row["record_json"]
                                        if pid and raw:
                                            pa_raw_by_id[(ckey, pid)] = str(raw)
                                    except Exception:
                                        continue
                    finally:
                        con.close()
            except Exception:
                pa_raw_by_id = {}

            early_trigger_keys = payload.get("trigger_keys")
            early_trigger_groups = payload.get("trigger_groups") or payload.get("trigger_any_groups") or payload.get("signal_groups")
            early_trigger_tiers = payload.get("trigger_tiers") or payload.get("tiers")
            early_trigger_min_score = payload.get("trigger_min_score", payload.get("min_score"))
            signal_filter_active = bool(
                (isinstance(early_trigger_keys, (list, tuple, set)) and len(early_trigger_keys) > 0)
                or (isinstance(early_trigger_groups, (list, tuple, set)) and len(early_trigger_groups) > 0)
                or (isinstance(early_trigger_tiers, (list, tuple, set)) and len(early_trigger_tiers) > 0)
                or early_trigger_min_score is not None
            )

            try:
                from florida_property_scraper.storage import SQLiteStore

                db_path = leads_db_path
                if leads_db_ok and db_path and os.path.exists(db_path):
                    rstore = SQLiteStore(str(db_path))
                    try:
                        rollups_total = 0
                        try:
                            rollups_total = int(
                                rstore.conn.execute("SELECT count(*) FROM parcel_trigger_rollups").fetchone()[0]
                            )
                        except Exception:
                            rollups_total = 0

                        for ckey, ids in parcel_ids_by_county.items():
                            if not ids:
                                continue
                            rollups = rstore.get_rollups_for_parcels(county=ckey, parcel_ids=ids)
                            for pid, rec in rollups.items():
                                if not isinstance(rec, dict):
                                    continue
                                key = (ckey, pid)
                                rollups_by_id[key] = rec
                                try:
                                    details = rec.get("details_json")
                                    if isinstance(details, str) and details:
                                        data = json.loads(details)
                                        keys = data.get("trigger_keys") or []
                                        if isinstance(keys, (list, tuple)):
                                            rollup_keys_by_id[key] = {str(k).strip().lower() for k in keys if str(k).strip()}
                                except Exception:
                                    pass

                        rollups_loaded = True
                    finally:
                        rstore.close()
                else:
                    rollups_loaded = False
            except Exception:
                rollups_loaded = False

            try:
                import sqlite3 as _sqlite3

                db_path = leads_db_path
                if leads_db_ok and db_path and os.path.exists(db_path):
                    conn = _sqlite3.connect(db_path)
                    conn.row_factory = _sqlite3.Row
                    try:
                        has_table = conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name='parcel_table1'"
                        ).fetchone()
                        if has_table:
                            for ckey, ids in parcel_ids_by_county.items():
                                if ckey != "seminole":
                                    continue
                                if not ids:
                                    continue
                                chunk = 900
                                for i in range(0, len(ids), chunk):
                                    batch = ids[i : i + chunk]
                                    placeholders = ",".join(["?"] * len(batch))
                                    rows = conn.execute(
                                        f"SELECT PARCEL, OWNER, ADD1, ADD2, CITY, STATE, ZIP, ZIP4, "
                                        f"PAD_NUM, PAD_DIR, PAD_NAME, PAD_STREET, APPR_BLDG, APPR_LAND, "
                                        f"TOTAL_JUST_VALUE, TOTAL_ASSESSED_VALUE, LIVING_AREA, TOTAL_SQFT, "
                                        f"BASE_YR_BLT, HMST_YEAR_GRANTED "
                                        f"FROM parcel_table1 WHERE PARCEL IN ({placeholders})",
                                        batch,
                                    ).fetchall()
                                    for row in rows:
                                        try:
                                            pid = str(row["PARCEL"] or "").strip()
                                            if not pid:
                                                continue
                                            parcel_table1_by_id[(ckey, pid)] = dict(row)
                                        except Exception:
                                            continue
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass
            except Exception:
                parcel_table1_by_id = {}

            try:
                import sqlite3 as _sqlite3

                if parcels_db_ok and parcels_db_path and os.path.exists(parcels_db_path):
                    con = _sqlite3.connect(parcels_db_path)
                    con.row_factory = _sqlite3.Row
                    try:
                        has_table = con.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name='parcels_pa'"
                        ).fetchone()
                        if has_table:
                            for ckey, ids in parcel_ids_by_county.items():
                                if not ids:
                                    continue
                                chunk = 900
                                for i in range(0, len(ids), chunk):
                                    batch = ids[i : i + chunk]
                                    placeholders = ",".join(["?"] * len(batch))
                                    rows = con.execute(
                                        f"SELECT county, parcel_id, owner_name, situs_address, mailing_address, "
                                        f"living_area_sqft, beds, baths, year_built, last_sale_date, last_sale_price "
                                        f"FROM parcels_pa WHERE county=? AND parcel_id IN ({placeholders})",
                                        [ckey, *batch],
                                    ).fetchall()
                                    for row in rows:
                                        try:
                                            pid = str(row["parcel_id"] or "").strip()
                                            cval = str(row["county"] or ckey).strip().lower() or ckey
                                            if not pid:
                                                continue
                                            parcels_pa_by_id[(cval, pid)] = dict(row)
                                        except Exception:
                                            continue
                    finally:
                        try:
                            con.close()
                        except Exception:
                            pass
            except Exception:
                parcels_pa_by_id = {}

            if debug_counts is not None:
                debug_counts["parcel_id_in_count"] = int(len(allowed_ids) if allowed_ids is not None else 0)

            def _norm_choice(v: object) -> str:
                s = str(v or "").strip()
                if not s:
                    return "UNKNOWN"
                return " ".join(s.upper().split())

            def _pt1_num(row: dict, key: str) -> float | None:
                try:
                    raw = row.get(key)
                    if raw is None:
                        return None
                    s = str(raw).strip()
                    if not s or s.upper() == "NULL":
                        return None
                    return float(s)
                except Exception:
                    return None

            def _pt1_int(row: dict, key: str) -> int | None:
                try:
                    raw = row.get(key)
                    if raw is None:
                        return None
                    s = str(raw).strip()
                    if not s or s.upper() == "NULL":
                        return None
                    return int(float(s))
                except Exception:
                    return None

            def _pt1_text(row: dict, key: str) -> str:
                try:
                    s = str(row.get(key) or "").strip()
                    if not s or s.upper() == "NULL":
                        return ""
                    return s
                except Exception:
                    return ""

            def _pt1_situs(row: dict) -> str:
                parts = [
                    _pt1_text(row, "PAD_NUM"),
                    _pt1_text(row, "PAD_DIR"),
                    _pt1_text(row, "PAD_NAME"),
                    _pt1_text(row, "PAD_STREET"),
                ]
                return " ".join([p for p in parts if p]).strip()

            def _pt1_mailing(row: dict) -> str:
                parts = [
                    _pt1_text(row, "ADD1"),
                    _pt1_text(row, "ADD2"),
                    " ".join(
                        [
                            _pt1_text(row, "CITY"),
                            _pt1_text(row, "STATE"),
                            _pt1_text(row, "ZIP"),
                            _pt1_text(row, "ZIP4"),
                        ]
                    ).strip(),
                ]
                return ", ".join([p for p in parts if p]).strip()

            # Optional: SQL-side filtering against cached columns.
            # Only applies when the request is not asking us to enrich missing data.
            # If enrich=true, we need to consider parcels not yet cached.
            raw_filters = payload.get("filters")
            if not isinstance(raw_filters, dict):
                raw_filters = {}
            # Search never auto-enriches; enrichment is explicit via /api/parcels/enrich.
            explicit_enrich = None
            enrich_requested = False

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
            if multi_county:
                pa_by_id = {}
                for ckey, ids in parcel_ids_by_county.items():
                    if not ids:
                        continue
                    pa_ids = _pa_ids_for_geom_ids(ckey, ids)
                    pa_by_pa = store.get_many(county=ckey, parcel_ids=pa_ids)
                    for gid in ids:
                        pid = _pa_id_for_geom(ckey, gid)
                        pa_rec = pa_by_pa.get(pid)
                        if pa_rec is not None:
                            pa_by_id[(ckey, gid)] = pa_rec
            else:
                pa_ids = _pa_ids_for_geom_ids(county_key, parcel_ids)
                pa_by_pa = store.get_many(county=county_key, parcel_ids=pa_ids)
                pa_by_id = {
                    gid: pa_by_pa.get(_pa_id_for_geom(county_key, gid))
                    for gid in parcel_ids
                    if pa_by_pa.get(_pa_id_for_geom(county_key, gid)) is not None
                }

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
            missing_policy = "lenient"
            try:
                if isinstance(raw_filters, dict):
                    mp = str(raw_filters.get("missing_policy") or "").strip().lower()
                    if mp in {"lenient", "strict"}:
                        missing_policy = mp
            except Exception:
                missing_policy = "lenient"

            exclude_missing = bool(missing_policy == "strict")
            strict_attribute_filters = bool(filters_present and exclude_missing)

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
                    live_cap = int(os.getenv("LIVE_PARCEL_ENRICH_LIMIT", "1000") or 1000)
                    if live_cap < 0:
                        live_cap = 0
                    enrich_ids = missing_ids[: min(live_cap, limit)]
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
                        lot_sqft, _lot_acres = _canonical_lot_sizes(
                            getattr(pa_tmp, "land_sf", None),
                            getattr(pa_tmp, "land_acres", None),
                        )
                        return float(lot_sqft or 0.0)

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
                                    lot_sqft, lot_acres = _canonical_lot_sizes(
                                        getattr(_pa, "land_sf", None),
                                        getattr(_pa, "land_acres", None),
                                    )
                                    fields_tmp["lot_size_sqft"] = lot_sqft if lot_sqft > 0 else None
                                except Exception:
                                    fields_tmp["lot_size_sqft"] = None
                                try:
                                    lot_sqft_any = fields_tmp.get("lot_size_sqft")
                                    lot_sqft, lot_acres = _canonical_lot_sizes(
                                        lot_sqft_any,
                                        getattr(_pa, "land_acres", None),
                                    )
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

                            pa_ids = _pa_ids_for_geom_ids(county_key, parcel_ids)
                            pa_by_pa = store.get_many(county=county_key, parcel_ids=pa_ids)
                            pa_by_id = {
                                gid: pa_by_pa.get(_pa_id_for_geom(county_key, gid))
                                for gid in parcel_ids
                                if pa_by_pa.get(_pa_id_for_geom(county_key, gid)) is not None
                            }
                        except Exception as e:
                            warnings.append(f"inline_ocpa_batch_failed:{e}")

            # Compute baseline (unfiltered) option lists AFTER best-effort enrichment.
            # Otherwise the first run in a new area can return empty option arrays.
            baseline_pa_by_id = {}
            if multi_county:
                for ckey, ids in parcel_ids_by_county.items():
                    if not ids:
                        continue
                    pa_ids = _pa_ids_for_geom_ids(ckey, ids)
                    pa_by_pa = store.get_many(county=ckey, parcel_ids=pa_ids)
                    for gid in ids:
                        pid = _pa_id_for_geom(ckey, gid)
                        pa = pa_by_pa.get(pid)
                        if pa is not None:
                            baseline_pa_by_id[(ckey, gid)] = pa
            else:
                pa_ids = _pa_ids_for_geom_ids(county_key, baseline_parcel_ids)
                pa_by_pa = store.get_many(county=county_key, parcel_ids=pa_ids)
                for gid in baseline_parcel_ids:
                    pid = _pa_id_for_geom(county_key, gid)
                    pa = pa_by_pa.get(pid)
                    if pa is not None:
                        baseline_pa_by_id[gid] = pa

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
                if multi_county:
                    total = int(sum(len(v) for v in parcel_ids_by_county.values()) or 0)
                else:
                    total = int(len(baseline_parcel_ids) or 0)
                present = {
                    "living_area_sqft": 0,
                    "lot_size_sqft": 0,
                    "lot_size_acres": 0,
                    "zoning": 0,
                    "future_land_use": 0,
                }
                if multi_county:
                    iter_ids = [(ckey, pid) for ckey, ids in parcel_ids_by_county.items() for pid in ids]
                else:
                    iter_ids = [(county_key, pid) for pid in baseline_parcel_ids]

                for ckey, pid in iter_ids:
                    pa = baseline_pa_by_id.get((ckey, pid)) if multi_county else baseline_pa_by_id.get(pid)
                    parcels_pa_row = (
                        parcels_pa_by_id.get((ckey, pid)) if parcels_pa_by_id else None
                    )
                    parcel_row = (
                        parcel_table1_by_id.get((ckey, pid)) if parcel_table1_by_id else None
                    )

                    living_val = None
                    try:
                        if pa is not None:
                            living_val = float(pa.living_sf or 0) or float(pa.building_sf or 0) or 0.0
                    except Exception:
                        living_val = None
                    if not living_val:
                        try:
                            living_val = _pt1_num(parcels_pa_row, "living_area_sqft") if parcels_pa_row else None
                        except Exception:
                            living_val = None
                    if not living_val:
                        try:
                            living_val = _pt1_num(parcel_row, "LIVING_AREA") if parcel_row else None
                            if living_val is None:
                                living_val = _pt1_num(parcel_row, "TOTAL_SQFT") if parcel_row else None
                        except Exception:
                            living_val = None
                    if living_val and living_val > 0:
                        present["living_area_sqft"] += 1

                    try:
                        if pa is not None:
                            land_sf = float(pa.land_sf or 0) or 0.0
                            land_acres = float(pa.land_acres or 0) or 0.0
                            if land_sf > 0 or land_acres > 0:
                                present["lot_size_sqft"] += 1
                                present["lot_size_acres"] += 1
                    except Exception:
                        pass
                    try:
                        if pa is not None and str(getattr(pa, "zoning", "") or "").strip():
                            present["zoning"] += 1
                    except Exception:
                        pass
                    try:
                        if pa is not None and str(getattr(pa, "future_land_use", "") or "").strip():
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
            try:
                cov = field_stats.get("coverage_candidates") or {}
                fields_out: dict[str, dict[str, float | int]] = {}
                if isinstance(cov, dict):
                    for k, v in cov.items():
                        if not isinstance(v, dict):
                            continue
                        present = int(v.get("present", 0) or 0)
                        total = int(v.get("total", 0) or 0)
                        pct = float(v.get("coverage", 0.0) or 0.0)
                        fields_out[str(k)] = {"present": present, "total": total, "pct": pct}
                field_stats["fields"] = fields_out
            except Exception:
                pass

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
                    if multi_county:
                        next_intersecting: list[Any] = []
                        for ckey, ids in parcel_ids_by_county.items():
                            if not ids:
                                continue
                            pa_ids = _pa_ids_for_geom_ids(ckey, ids)
                            matching_pa_ids = set(
                                store.filter_cached_ids(
                                    county=ckey,
                                    parcel_ids=pa_ids,
                                    where_sql=where_sql,
                                    params=where_params,
                                    limit=len(pa_ids),
                                )
                            )
                            matching_geom = {
                                gid for gid in ids if _pa_id_for_geom(ckey, gid) in matching_pa_ids
                            }
                            if matching_geom:
                                next_intersecting.extend(
                                    [
                                        f
                                        for f in intersecting
                                        if getattr(f, "county", ckey) == ckey and f.parcel_id in matching_geom
                                    ]
                                )
                        intersecting = next_intersecting
                        parcel_ids_by_county = {}
                        for f in intersecting:
                            ckey = str(getattr(f, "county", "") or county_key).strip().lower() or county_key
                            parcel_ids_by_county.setdefault(ckey, []).append(f.parcel_id)
                        parcel_ids = [pid for ids in parcel_ids_by_county.values() for pid in ids]
                    else:
                        pa_ids = _pa_ids_for_geom_ids(county_key, parcel_ids)
                        matching_pa_ids = set(
                            store.filter_cached_ids(
                                county=county_key,
                                parcel_ids=pa_ids,
                                where_sql=where_sql,
                                params=where_params,
                                limit=len(pa_ids),
                            )
                        )
                        matching_geom = {
                            gid
                            for gid in parcel_ids
                            if _pa_id_for_geom(county_key, gid) in matching_pa_ids
                        }
                        if matching_geom:
                            intersecting = [f for f in intersecting if f.parcel_id in matching_geom]
                            parcel_ids = [f.parcel_id for f in intersecting]
                        else:
                            intersecting = []
                            parcel_ids = []

                # Refresh after SQL filtering.
                if multi_county:
                    pa_by_id = {}
                    for ckey, ids in parcel_ids_by_county.items():
                        if not ids:
                            continue
                        pa_ids = _pa_ids_for_geom_ids(ckey, ids)
                        pa_by_pa = store.get_many(county=ckey, parcel_ids=pa_ids)
                        for gid in ids:
                            pid = _pa_id_for_geom(ckey, gid)
                            pa = pa_by_pa.get(pid)
                            if pa is not None:
                                pa_by_id[(ckey, gid)] = pa
                else:
                    pa_ids = _pa_ids_for_geom_ids(county_key, parcel_ids)
                    pa_by_pa = store.get_many(county=county_key, parcel_ids=pa_ids)
                    pa_by_id = {
                        gid: pa_by_pa.get(_pa_id_for_geom(county_key, gid))
                        for gid in parcel_ids
                        if pa_by_pa.get(_pa_id_for_geom(county_key, gid)) is not None
                    }

            if multi_county:
                hover_by_id = {}
                for ckey, ids in parcel_ids_by_county.items():
                    if not ids:
                        continue
                    pa_ids = _pa_ids_for_geom_ids(ckey, ids)
                    hover_by_pa = store.get_hover_fields_many(county=ckey, parcel_ids=pa_ids)
                    for gid in ids:
                        pid = _pa_id_for_geom(ckey, gid)
                        hv = hover_by_pa.get(pid)
                        if hv is not None:
                            hover_by_id[(ckey, gid)] = hv
            else:
                pa_ids = _pa_ids_for_geom_ids(county_key, parcel_ids)
                hover_by_pa = store.get_hover_fields_many(county=county_key, parcel_ids=pa_ids)
                hover_by_id = {
                    gid: hover_by_pa.get(_pa_id_for_geom(county_key, gid))
                    for gid in parcel_ids
                    if hover_by_pa.get(_pa_id_for_geom(county_key, gid)) is not None
                }

            # Fallback: if PA rows exist under a different county label, attempt
            # a conservative match by parcel_id + county alias. This avoids
            # dropping beds/baths/zoning when data is present but the county
            # string differs (e.g., "Seminole County").
            try:
                import re as _re
                import sqlite3 as _sqlite3
                from florida_property_scraper.pa.normalize import apply_defaults

                def _norm_county(value: str) -> str:
                    return _re.sub(r"[^a-z]", "", str(value or "").lower())

                def _county_matches(raw_county: str, target: str, raw_json: dict | None) -> bool:
                    norm_target = _norm_county(target)
                    if not norm_target:
                        return False
                    norm_raw = _norm_county(raw_county)
                    if norm_raw and (norm_raw == norm_target or norm_raw.startswith(norm_target) or norm_target in norm_raw):
                        return True
                    if isinstance(raw_json, dict):
                        for key in ("county", "county_name", "countycode", "county_code"):
                            try:
                                v = raw_json.get(key)
                            except Exception:
                                v = None
                            if v and _county_matches(v, target, None):
                                return True
                    return False

                def _hover_from_pa(rec) -> dict:
                    owner_name = "; ".join([n for n in (rec.owner_names or []) if n])
                    return {
                        "situs_address": rec.situs_address or "",
                        "owner_name": owner_name,
                        "last_sale_date": rec.last_sale_date,
                        "last_sale_price": float(rec.last_sale_price or 0),
                        "year_built": int(rec.year_built or 0) if rec.year_built is not None else 0,
                        "beds": int(rec.bedrooms or 0) if rec.bedrooms is not None else 0,
                        "baths": float(rec.bathrooms or 0) if rec.bathrooms is not None else 0.0,
                        "living_sf": float(rec.living_sf or 0),
                        "land_sf": float(rec.land_sf or 0),
                        "land_acres": float(rec.land_acres or 0),
                        "zoning": rec.zoning or "",
                        "future_land_use": rec.future_land_use or "",
                        "land_value": float(rec.land_value or 0),
                        "improvement_value": float(rec.improvement_value or 0),
                        "assessed_value": float(rec.assessed_value or 0),
                        "taxable_value": float(rec.taxable_value or 0),
                        "just_value": float(rec.just_value or 0),
                        "mortgage_amount": float(rec.mortgage_amount or 0) if rec.mortgage_amount is not None else None,
                        "mortgage_date": rec.mortgage_date or "",
                        "mortgage_lender": rec.mortgage_lender or "",
                        "longitude": float(rec.longitude) if rec.longitude is not None else None,
                        "latitude": float(rec.latitude) if rec.latitude is not None else None,
                        "use_type": rec.use_type or "",
                        "land_use_code": rec.land_use_code or "",
                    }

                missing_by_county: dict[str, list[tuple[str, str]]] = {}

                def _has_pa(ckey: str, gid: str) -> bool:
                    if multi_county:
                        return (ckey, gid) in pa_by_id
                    return gid in pa_by_id

                for ckey, ids in parcel_ids_by_county.items():
                    for gid in ids:
                        if _has_pa(ckey, gid):
                            continue
                        pid = _pa_id_for_geom(ckey, gid)
                        if pid:
                            missing_by_county.setdefault(ckey, []).append((gid, pid))

                missing_pa_ids = {
                    pid
                    for pairs in missing_by_county.values()
                    for (_, pid) in pairs
                    if pid
                }

                if missing_pa_ids:
                    db_path = os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
                    if db_path and os.path.exists(db_path):
                        con = _sqlite3.connect(db_path)
                        con.row_factory = _sqlite3.Row
                        try:
                            all_rows: dict[str, list[tuple[str, str, dict | None]]] = {}
                            chunk = 900
                            all_ids = list(missing_pa_ids)
                            for i in range(0, len(all_ids), chunk):
                                batch = all_ids[i : i + chunk]
                                placeholders = ",".join(["?"] * len(batch))
                                rows = con.execute(
                                    f"SELECT parcel_id, county, record_json FROM pa_properties WHERE parcel_id IN ({placeholders})",
                                    batch,
                                ).fetchall()
                                for row in rows:
                                    try:
                                        pid = str(row["parcel_id"] or "").strip()
                                        if not pid:
                                            continue
                                        raw = row["record_json"]
                                        raw_json = None
                                        try:
                                            raw_json = json.loads(raw) if raw else None
                                        except Exception:
                                            raw_json = None
                                        all_rows.setdefault(pid, []).append(
                                            (str(row["county"] or "").strip(), str(raw or ""), raw_json)
                                        )
                                    except Exception:
                                        continue

                            for ckey, pairs in missing_by_county.items():
                                for gid, pid in pairs:
                                    if _has_pa(ckey, gid):
                                        continue
                                    rows = all_rows.get(pid, [])
                                    if not rows:
                                        continue
                                    chosen = None
                                    for row_county, raw_str, raw_json in rows:
                                        if _county_matches(row_county, ckey, raw_json):
                                            chosen = (row_county, raw_str, raw_json)
                                            break
                                    if chosen is None:
                                        continue
                                    _, raw_str, raw_json = chosen
                                    if not raw_json:
                                        continue
                                    try:
                                        pa_any = apply_defaults(raw_json)
                                    except Exception:
                                        continue
                                    if multi_county:
                                        pa_by_id[(ckey, gid)] = pa_any
                                        hover_by_id[(ckey, gid)] = _hover_from_pa(pa_any)
                                    else:
                                        pa_by_id[gid] = pa_any
                                        hover_by_id[gid] = _hover_from_pa(pa_any)
                                    if raw_str:
                                        pa_raw_by_id[(ckey, pid)] = raw_str
                        finally:
                            con.close()
            except Exception:
                pass

            # Strict mode: missing values must fail attribute filters. Soft-missing is
            # reserved for polygon-only browsing (no attribute filters).
        finally:
            store.close()

        _mark("enrich")

        # Compile filters (supports both list-form and object-form).
        raw_filters = payload.get("filters")
        filters = compile_filters(raw_filters)
        applied_where_clauses: list[str] = []

        try:
            for f in (filters or []):
                field_name = str(getattr(f, "field", "") or "").strip()
                op_raw = str(getattr(f, "op", "") or "").strip().lower()
                value = getattr(f, "value", None)
                if not field_name or not op_raw:
                    continue
                if op_raw in {"in", "in_list"} and isinstance(value, (list, tuple, set)):
                    n = len([x for x in value if x is not None])
                    if n <= 0:
                        continue
                    applied_where_clauses.append(f"{field_name} IN ({', '.join(['?'] * n)})")
                    continue
                if op_raw == "contains":
                    applied_where_clauses.append(f"LOWER({field_name}) LIKE ?")
                    continue
                if op_raw in {"=", "==", "equals"}:
                    applied_where_clauses.append(f"{field_name} = ?")
                    continue
                if op_raw in {"!=", "not_equals"}:
                    applied_where_clauses.append(f"{field_name} != ?")
                    continue
                if op_raw in {">", "gt", ">=", "gte", "<", "lt", "<=", "lte"}:
                    op_norm = op_raw
                    if op_raw == "gt":
                        op_norm = ">"
                    elif op_raw == "gte":
                        op_norm = ">="
                    elif op_raw == "lt":
                        op_norm = "<"
                    elif op_raw == "lte":
                        op_norm = "<="
                    applied_where_clauses.append(f"{field_name} {op_norm} ?")
                    continue
                applied_where_clauses.append(f"{field_name} {op_raw} ?")
        except Exception:
            applied_where_clauses = []

        filter_fields: list[str] = []
        try:
            for f in (filters or []):
                name = str(getattr(f, "field", "") or "").strip()
                if name:
                    filter_fields.append(name)
        except Exception:
            filter_fields = []

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
                        "applied_where_clauses": applied_where_clauses,
                    }
                )
            except Exception:
                pass

        raw_triggers = payload.get("triggers") if flags.triggers else None
        triggers = compile_triggers(raw_triggers)

        raw_trigger_keys = payload.get("trigger_keys")
        trigger_keys: list[str] = []
        try:
            if isinstance(raw_trigger_keys, (list, tuple, set)):
                for item in raw_trigger_keys:
                    key = str(item or "").strip().lower()
                    if key and key not in trigger_keys:
                        trigger_keys.append(key)
        except Exception:
            trigger_keys = []

        raw_trigger_groups = (
            payload.get("trigger_groups")
            or payload.get("trigger_any_groups")
            or payload.get("signal_groups")
        )
        trigger_groups: list[str] = []
        try:
            if isinstance(raw_trigger_groups, (list, tuple, set)):
                for item in raw_trigger_groups:
                    key = str(item or "").strip().lower()
                    if key and key not in trigger_groups:
                        trigger_groups.append(key)
        except Exception:
            trigger_groups = []

        raw_trigger_tiers = payload.get("trigger_tiers") or payload.get("tiers")
        trigger_tiers: list[str] = []
        try:
            if isinstance(raw_trigger_tiers, (list, tuple, set)):
                for item in raw_trigger_tiers:
                    key = str(item or "").strip().lower()
                    if key and key not in trigger_tiers:
                        trigger_tiers.append(key)
        except Exception:
            trigger_tiers = []

        trigger_min_score = None
        try:
            raw_min_score = payload.get("trigger_min_score", payload.get("min_score"))
            if raw_min_score is not None and str(raw_min_score).strip() != "":
                trigger_min_score = int(float(raw_min_score))
        except Exception:
            trigger_min_score = None

        supported_signal_keys = {"absentee_owner", "homestead"}
        signal_keys = [k for k in trigger_keys if k in supported_signal_keys]

        sale_fields = {
            # PA hover fields
            "last_sale_date",
            "last_sale_price",
            # Scraper-derived fields (future)
            "sale_date",
            "sale_price",
            "deed_type",
        }

        # signal_filter_active computed earlier for rollup loading; keep value consistent.

        results_all: list[dict] = []
        records_all: list[dict] = []
        source_counts: dict[str, int] = {"live": 0, "cache": 0, "missing": 0}
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

        def _norm_addr(value: object) -> str:
            try:
                s = str(value or "").strip().upper()
            except Exception:
                return ""
            if not s:
                return ""
            s = re.sub(r"[^A-Z0-9 ]+", " ", s)
            s = re.sub(r"\s+", " ", s).strip()
            return s

        def _ensure_pa_address_map(ckey: str) -> None:
            if ckey in pa_address_map_by_county:
                return
            pa_address_map_by_county[ckey] = {}
            pa_raw_by_address_by_county[ckey] = {}
            try:
                import sqlite3 as _sqlite3

                db_path = os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
                if not db_path or not os.path.exists(db_path):
                    return
                con = _sqlite3.connect(db_path)
                con.row_factory = _sqlite3.Row
                try:
                    rows = con.execute(
                        "SELECT parcel_id, record_json FROM pa_properties WHERE lower(county)=? OR lower(county) LIKE ?",
                        (ckey, f"%{ckey}%"),
                    ).fetchall()
                    for row in rows:
                        try:
                            pid = str(row["parcel_id"] or "").strip()
                            raw = row["record_json"]
                            if not pid or not raw:
                                continue
                            try:
                                data = json.loads(raw)
                            except Exception:
                                continue
                            addr = str(
                                data.get("situs_address")
                                or data.get("property_address")
                                or data.get("site_address")
                                or ""
                            ).strip()
                            if not addr:
                                continue
                            street = addr.split(",")[0].strip()
                            for candidate in {addr, street}:
                                norm = _norm_addr(candidate)
                                if not norm:
                                    continue
                                if norm not in pa_address_map_by_county[ckey]:
                                    pa_address_map_by_county[ckey][norm] = pid
                                    pa_raw_by_address_by_county[ckey][norm] = raw
                        except Exception:
                            continue
                finally:
                    con.close()
            except Exception:
                return

        filter_stage_counts: dict[str, int] = {
            "intersecting": 0,
            "skipped_no_pa_fdor_live": 0,
            "with_pa": 0,
            "filter_failed": 0,
            "geometry_failed": 0,
            "trigger_failed": 0,
            "emitted": 0,
        }

        filter_drop_reasons: dict[str, int] = {}
        trigger_drop_reasons: dict[str, int] = {}

        stage_counts: dict[str, int] = {
            "candidates": int(len(intersecting)),
            "with_pa": 0,
            "filter_passed": 0,
            "geometry_passed": 0,
            "geometry_failed": 0,
            "latlng_available": 0,
            "returned": 0,
        }

        missing_field_counts: dict[str, int] = {}

        filter_explain_counts: list[dict[str, Any]] = []
        if explain_enabled and filters:
            try:
                for f in filters:
                    filter_explain_counts.append(
                        {
                            "field": getattr(f, "field", None),
                            "op": getattr(f, "op", None),
                            "value": getattr(f, "value", None),
                            "candidates": 0,
                            "dropped_missing": 0,
                            "dropped_value": 0,
                            "passed": 0,
                        }
                    )
            except Exception:
                filter_explain_counts = []

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

        def merge_non_null(dst: dict[str, object], src: dict | None) -> None:
            if not isinstance(src, dict):
                return

            def _is_empty(field: str, value: object) -> bool:
                if value is None:
                    return True
                if isinstance(value, str):
                    return not value.strip()
                if isinstance(value, (list, tuple, set, dict)):
                    return len(value) == 0
                if field in {
                    "living_area_sqft",
                    "lot_size_sqft",
                    "lot_size_acres",
                    "beds",
                    "baths",
                    "year_built",
                    "total_value",
                    "land_value",
                    "building_value",
                    "assessed_value",
                }:
                    try:
                        return float(value) <= 0
                    except Exception:
                        return True
                return False

            for k, v in src.items():
                key = str(k)
                if _is_empty(key, v):
                    continue
                if _is_empty(key, dst.get(key)):
                    dst[key] = v

        after_filter_ids_sample: list[str] = []
        after_geom_ids_sample: list[str] = []

        for feat in intersecting:
            feat_county = str(getattr(feat, "county", "") or county_key).strip().lower() or county_key
            feat_pa_parcel_id = _pa_id_for_geom(feat_county, feat.parcel_id)
            pa = pa_by_id.get((feat_county, feat.parcel_id)) if multi_county else pa_by_id.get(feat.parcel_id)
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
                stage_counts["with_pa"] += 1
            computed = compute_ui_fields(pa_dict)
            hover_key = (feat_county, feat.parcel_id) if multi_county else feat.parcel_id
            hover = hover_by_id.get(hover_key) or {
                "situs_address": "",
                "owner_name": "",
                "last_sale_date": None,
                "last_sale_price": 0,
                "mortgage_amount": None,
            }

            fields: dict[str, object] = {}
            merge_non_null(fields, pa_dict)
            merge_non_null(fields, computed)
            merge_non_null(fields, hover)

            try:
                pt_norm = normalize_property_type(
                    fields.get("property_type"),
                    fields.get("use_type"),
                    fields.get("land_use"),
                    fields.get("land_use_code"),
                    fields.get("property_class"),
                )
                if pt_norm:
                    fields["property_type_norm"] = pt_norm
                    fields["property_type"] = pt_norm
            except Exception:
                pass

            parcel_row = parcel_table1_by_id.get((feat_county, feat.parcel_id)) if parcel_table1_by_id else None
            if parcel_row:
                try:
                    if not fields.get("owner_name"):
                        owner_name_pt1 = _pt1_text(parcel_row, "OWNER")
                        if owner_name_pt1:
                            fields["owner_name"] = owner_name_pt1
                    if not fields.get("situs_address"):
                        situs_pt1 = _pt1_situs(parcel_row)
                        if situs_pt1:
                            fields["situs_address"] = situs_pt1
                    if fields.get("living_area_sqft") in (None, "", 0):
                        la = _pt1_num(parcel_row, "LIVING_AREA")
                        if la is None:
                            la = _pt1_num(parcel_row, "TOTAL_SQFT")
                        if la:
                            fields["living_area_sqft"] = la
                    if fields.get("year_built") in (None, "", 0):
                        yb = _pt1_int(parcel_row, "BASE_YR_BLT")
                        if yb and yb > 0:
                            fields["year_built"] = yb
                    if fields.get("total_value") in (None, "", 0):
                        tv = _pt1_num(parcel_row, "TOTAL_JUST_VALUE")
                        if tv:
                            fields["total_value"] = tv
                    if fields.get("assessed_value") in (None, "", 0):
                        av = _pt1_num(parcel_row, "TOTAL_ASSESSED_VALUE")
                        if av:
                            fields["assessed_value"] = av
                    if fields.get("land_value") in (None, "", 0):
                        lv = _pt1_num(parcel_row, "APPR_LAND")
                        if lv:
                            fields["land_value"] = lv
                    if fields.get("building_value") in (None, "", 0):
                        bv = _pt1_num(parcel_row, "APPR_BLDG")
                        if bv:
                            fields["building_value"] = bv
                except Exception:
                    pass

            if pa is None and parcel_row:
                try:
                    _ensure_pa_address_map(feat_county)
                    addr = _pt1_situs(parcel_row)
                    if addr:
                        norm = _norm_addr(addr)
                        pa_pid = pa_address_map_by_county.get(feat_county, {}).get(norm)
                        if pa_pid:
                            feat_pa_parcel_id = pa_pid
                            try:
                                pa = store.get(county=feat_county, parcel_id=pa_pid)
                            except Exception:
                                pa = None
                            pa_dict = pa.to_dict() if pa is not None else None
                            raw = pa_raw_by_address_by_county.get(feat_county, {}).get(norm)
                            if raw:
                                pa_raw_by_id[(feat_county, feat_pa_parcel_id)] = raw
                except Exception:
                    pass

            parcels_pa_row = (
                parcels_pa_by_id.get((feat_county, feat.parcel_id)) if parcels_pa_by_id else None
            )
            if parcels_pa_row:
                try:
                    if not fields.get("owner_name"):
                        owner_name_pp = _pt1_text(parcels_pa_row, "owner_name")
                        if owner_name_pp:
                            fields["owner_name"] = owner_name_pp
                    if not fields.get("situs_address"):
                        situs_pp = _pt1_text(parcels_pa_row, "situs_address")
                        if situs_pp:
                            fields["situs_address"] = situs_pp
                    if fields.get("living_area_sqft") in (None, "", 0):
                        la_pp = _pt1_num(parcels_pa_row, "living_area_sqft")
                        if la_pp:
                            fields["living_area_sqft"] = la_pp
                    if fields.get("beds") in (None, "", 0):
                        b_pp = _pt1_int(parcels_pa_row, "beds")
                        if b_pp and b_pp > 0:
                            fields["beds"] = b_pp
                    if fields.get("baths") in (None, "", 0):
                        ba_pp = _pt1_num(parcels_pa_row, "baths")
                        if ba_pp and ba_pp > 0:
                            fields["baths"] = ba_pp
                    if fields.get("year_built") in (None, "", 0):
                        yb_pp = _pt1_int(parcels_pa_row, "year_built")
                        if yb_pp and yb_pp > 0:
                            fields["year_built"] = yb_pp
                    if fields.get("last_sale_date") in (None, "", 0):
                        ls_pp = _pt1_text(parcels_pa_row, "last_sale_date")
                        if ls_pp:
                            fields["last_sale_date"] = ls_pp
                    if fields.get("last_sale_price") in (None, "", 0):
                        lp_pp = _pt1_num(parcels_pa_row, "last_sale_price")
                        if lp_pp:
                            fields["last_sale_price"] = lp_pp
                except Exception:
                    pass

            if not exclude_missing and filter_fields:
                missing_ok_fields = set(filter_fields)
                if not flags.sale_filtering:
                    missing_ok_fields = {f for f in missing_ok_fields if f not in sale_fields}
                fields["__missing_ok_fields"] = list(missing_ok_fields)

            if explain_enabled and filter_fields:
                for fname in filter_fields:
                    v = fields.get(fname) if fname in fields else None
                    if v is None or v == "":
                        missing_field_counts[fname] = int(missing_field_counts.get(fname, 0)) + 1

            try:
                oy = fields.get("ownership_years")
                if isinstance(oy, (int, float)) and oy > 0:
                    field_stats["present"]["ownership_years"] += 1
            except Exception:
                pass

            # Soft-missing behavior is controlled by missing_policy (lenient keeps missing fields).

            # Provide stable, UI-friendly aliases for filtering.
            # These keys are treated as authoritative only when PA has a record.
            if pa is not None:
                def _set_field_if_present(field: str, value: object) -> None:
                    if value is None:
                        return
                    if isinstance(value, str) and not value.strip():
                        return
                    if field in {"living_area_sqft", "lot_size_sqft", "lot_size_acres", "beds", "baths", "year_built"}:
                        try:
                            if float(value) <= 0:
                                return
                        except Exception:
                            return
                    if fields.get(field) in (None, "", 0):
                        fields[field] = value

                try:
                    living = float(pa.living_sf or 0) or float(pa.building_sf or 0) or 0.0
                    _set_field_if_present("living_area_sqft", living)
                except Exception:
                    pass
                try:
                    lot_sqft, _lot_acres = _canonical_lot_sizes(
                        getattr(pa, "land_sf", None),
                        getattr(pa, "land_acres", None),
                    )
                    _set_field_if_present("lot_size_sqft", lot_sqft)
                except Exception:
                    pass
                try:
                    lot_sqft_any = fields.get("lot_size_sqft")
                    _lot_sqft, lot_acres = _canonical_lot_sizes(
                        lot_sqft_any,
                        getattr(pa, "land_acres", None),
                    )
                    _set_field_if_present("lot_size_acres", lot_acres)
                except Exception:
                    pass
                try:
                    b = int(pa.bedrooms or 0)
                    _set_field_if_present("beds", b)
                except Exception:
                    pass
                try:
                    ba = float(pa.bathrooms or 0)
                    _set_field_if_present("baths", ba)
                except Exception:
                    pass
                try:
                    yb = int(pa.year_built or 0)
                    _set_field_if_present("year_built", yb)
                except Exception:
                    pass
                try:
                    lv = float(pa.land_value or 0)
                    _set_field_if_present("land_value", lv)
                except Exception:
                    pass
                try:
                    iv = float(pa.improvement_value or 0)
                    _set_field_if_present("building_value", iv)
                except Exception:
                    pass
                try:
                    tv = float(pa.just_value or 0)
                    _set_field_if_present("total_value", tv)
                except Exception:
                    pass
                try:
                    av = float(pa.assessed_value or 0)
                    _set_field_if_present("assessed_value", av)
                except Exception:
                    pass
                try:
                    tv = float(pa.taxable_value or 0)
                    _set_field_if_present("taxable_value", tv)
                except Exception:
                    pass
                # `property_type` is treated as the PA use_type / land_use_code label.
                try:
                    pt_raw = (pa.use_type or pa.land_use_code or "").strip()
                    pt_norm = normalize_property_type(pt_raw, pa.property_class)
                    if pt_norm:
                        fields["property_type_norm"] = pt_norm
                        fields["property_type"] = pt_norm
                    else:
                        fields["property_type"] = pt_raw or None
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

            try:
                normalized_living = effective_living_sqft(fields)
                if normalized_living is not None:
                    fields["living_area_sqft"] = normalized_living
                elif fields.get("living_area_sqft") in (None, "", 0):
                    fields["living_area_sqft"] = None
            except Exception:
                pass

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
                if fields.get("beds") not in (None, "", 0):
                    field_stats["present"]["beds"] += 1
                if fields.get("baths") not in (None, "", 0):
                    field_stats["present"]["baths"] += 1
                if fields.get("year_built") not in (None, "", 0):
                    field_stats["present"]["year_built"] += 1
                if fields.get("total_value") not in (None, "", 0):
                    field_stats["present"]["total_value"] += 1
                if fields.get("land_value") not in (None, "", 0):
                    field_stats["present"]["land_value"] += 1
                if fields.get("building_value") not in (None, "", 0):
                    field_stats["present"]["building_value"] += 1
                if fields.get("assessed_value") not in (None, "", 0):
                    field_stats["present"]["assessed_value"] += 1
                if str(fields.get("last_sale_date") or "").strip():
                    field_stats["present"]["last_sale_date"] += 1
                last_sale_price_any = fields.get("last_sale_price")
                if last_sale_price_any not in (None, "", 0):
                    field_stats["present"]["last_sale_price"] += 1
                owner_name_any = fields.get("owner_name") or hover.get("owner_name")
                if str(owner_name_any or "").strip():
                    field_stats["present"]["owner_name"] += 1
                owner_mailing_any = fields.get("owner_mailing_address")
                if not owner_mailing_any and parcels_pa_row:
                    owner_mailing_any = _pt1_text(parcels_pa_row, "mailing_address")
                if not owner_mailing_any and parcel_row:
                    owner_mailing_any = _pt1_mailing(parcel_row)
                if str(owner_mailing_any or "").strip():
                    field_stats["present"]["owner_mailing_address"] += 1
                if str(fields.get("property_type") or "").strip():
                    field_stats["present"]["property_type"] += 1
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

            try:
                search_parts: list[str] = [
                    str(feat.parcel_id or "").strip(),
                    str(feat_pa_parcel_id or "").strip(),
                    str(feat_county or "").strip(),
                    str(fields.get("owner_name") or "").strip(),
                    str(fields.get("situs_address") or "").strip(),
                    str(fields.get("owner_mailing_address") or "").strip(),
                    str(fields.get("property_type") or "").strip(),
                    str(fields.get("zoning") or "").strip(),
                    str(fields.get("future_land_use") or "").strip(),
                    str(fields.get("last_sale_date") or "").strip(),
                    str(fields.get("beds") or "").strip(),
                    str(fields.get("baths") or "").strip(),
                    str(fields.get("year_built") or "").strip(),
                    str(fields.get("living_area_sqft") or "").strip(),
                    str(fields.get("lot_size_sqft") or "").strip(),
                    str(fields.get("lot_size_acres") or "").strip(),
                    str(fields.get("total_value") or "").strip(),
                    str(fields.get("land_value") or "").strip(),
                    str(fields.get("building_value") or "").strip(),
                    str(fields.get("assessed_value") or "").strip(),
                    str(fields.get("taxable_value") or "").strip(),
                ]
                fields["search_text_blob"] = " | ".join([p for p in search_parts if p])
            except Exception:
                pass

            if explain_enabled and filters and filter_explain_counts:
                missing_ok_raw = fields.get("__missing_ok_fields")
                missing_ok: set[str] = set()
                if isinstance(missing_ok_raw, (list, tuple, set)):
                    missing_ok = {str(x) for x in missing_ok_raw if str(x)}

                for idx, f in enumerate(filters):
                    if idx >= len(filter_explain_counts):
                        break
                    counters = filter_explain_counts[idx]
                    counters["candidates"] = int(counters.get("candidates", 0)) + 1
                    present = f.field in fields and fields.get(f.field) is not None
                    if not present:
                        if f.field in missing_ok:
                            counters["passed"] = int(counters.get("passed", 0)) + 1
                        else:
                            counters["dropped_missing"] = int(counters.get("dropped_missing", 0)) + 1
                        continue
                    try:
                        if eval_condition(fields, f):
                            counters["passed"] = int(counters.get("passed", 0)) + 1
                        else:
                            counters["dropped_value"] = int(counters.get("dropped_value", 0)) + 1
                    except Exception:
                        counters["dropped_value"] = int(counters.get("dropped_value", 0)) + 1

            if explain_enabled and filters:
                passed, reason = apply_filters_explain(fields, filters)
                if not passed:
                    filter_stage_counts["filter_failed"] += 1
                    if reason:
                        filter_drop_reasons[reason] = int(filter_drop_reasons.get(reason, 0)) + 1
                    continue
            else:
                if not apply_filters(fields, filters):
                    filter_stage_counts["filter_failed"] += 1
                    continue

            stage_counts["filter_passed"] += 1
            if len(after_filter_ids_sample) < 10:
                try:
                    after_filter_ids_sample.append(str(feat.parcel_id))
                except Exception:
                    pass

            # Geometry clipping AFTER attribute filters.
            geom_ok = False
            polygon_match_mode = str(payload.get("polygon_match_mode") or "intersects").strip().lower()
            if polygon_match_mode not in {"intersects", "centroid_inside", "contains"}:
                polygon_match_mode = "intersects"
            if text_only_mode:
                geom_ok = True
            else:
                try:
                    geom_ok = bool(feat.geometry) and match_geometry(geometry, feat.geometry, polygon_match_mode)
                except Exception:
                    geom_ok = False

            if not geom_ok:
                filter_stage_counts["geometry_failed"] += 1
                stage_counts["geometry_failed"] += 1
                if explain_enabled:
                    reason = "geometry:outside_polygon"
                    if polygon_match_mode == "centroid_inside":
                        reason = "geometry:centroid_outside"
                    elif polygon_match_mode == "contains":
                        reason = "geometry:not_contained"
                    filter_drop_reasons[reason] = int(filter_drop_reasons.get(reason, 0)) + 1
                continue

            stage_counts["geometry_passed"] += 1
            if len(after_geom_ids_sample) < 10:
                try:
                    after_geom_ids_sample.append(str(feat.parcel_id))
                except Exception:
                    pass

            reason_codes = eval_triggers(fields, triggers) if triggers else []
            if triggers and not reason_codes:
                filter_stage_counts["trigger_failed"] += 1
                if explain_enabled:
                    trigger_drop_reasons["no_trigger_match"] = (
                        int(trigger_drop_reasons.get("no_trigger_match", 0)) + 1
                    )
                continue

            # Enriched record payload for the modern UI.
            # New source contract:
            # - cache: we have a PA DB record already
            # - live: record was fetched live this request OR the geometry provider is live
            if pa_dict is None:
                source = "missing"
            elif live and provider_is_live and fdor_enabled:
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

            def _norm_num(value: object) -> float | None:
                try:
                    if value is None:
                        return None
                    if isinstance(value, (int, float)):
                        return float(value)
                    s = str(value).strip()
                    if not s or s.upper() == "NULL":
                        return None
                    return float(s)
                except Exception:
                    return None

            owner_name = str(fields.get("owner_name") or hover.get("owner_name") or "").strip()
            situs_address = str(fields.get("situs_address") or hover.get("situs_address") or "").strip()
            owner_mailing_address = ""
            homestead_flag = None
            zoning = str(fields.get("zoning") or "").strip()
            land_use = str(
                fields.get("use_type") or fields.get("land_use_code") or fields.get("land_use") or ""
            ).strip()
            future_land_use = str(fields.get("future_land_use") or "").strip()
            property_class = str(fields.get("property_class") or "").strip()
            living_area_sqft = None
            lot_size_sqft = None
            lot_size_acres = None
            beds = None
            baths = None
            year_built = None
            last_sale_date = fields.get("last_sale_date") or hover.get("last_sale_date")
            last_sale_price = fields.get("last_sale_price") or hover.get("last_sale_price")
            land_value = None
            building_value = None
            total_value = None
            assessed_value = None
            taxable_value = None

            field_sources: dict[str, str] = {}

            def _set_source(field_name: str, source_name: str, value: object) -> None:
                if value is None:
                    return
                if isinstance(value, str) and not value.strip():
                    return
                if field_name not in field_sources:
                    field_sources[field_name] = source_name

            def _apply_record_json(raw: object) -> None:
                if raw is None:
                    return
                try:
                    if isinstance(raw, str) and raw.strip():
                        data = json.loads(raw)
                    elif isinstance(raw, dict):
                        data = raw
                    else:
                        return
                except Exception:
                    return

                data_lower = {str(k).strip().lower(): v for k, v in data.items()} if isinstance(data, dict) else {}

                def _get(*keys: str) -> object | None:
                    for k in keys:
                        key = str(k).strip()
                        if key in data and data[key] not in (None, ""):
                            return data[key]
                        lkey = key.lower()
                        if lkey in data_lower and data_lower[lkey] not in (None, ""):
                            return data_lower[lkey]
                    return None

                nonlocal owner_name
                nonlocal situs_address
                nonlocal owner_mailing_address
                nonlocal homestead_flag
                nonlocal zoning
                nonlocal future_land_use
                nonlocal land_use
                nonlocal property_class
                nonlocal living_area_sqft
                nonlocal lot_size_sqft
                nonlocal lot_size_acres
                nonlocal beds
                nonlocal baths
                nonlocal year_built
                nonlocal last_sale_date
                nonlocal last_sale_price
                nonlocal land_value
                nonlocal building_value
                nonlocal total_value
                nonlocal assessed_value
                nonlocal taxable_value

                owner_raw = _get("owner_name", "owner", "owner_names")
                if owner_raw and not owner_name:
                    if isinstance(owner_raw, (list, tuple)):
                        owner_name = "; ".join([str(x).strip() for x in owner_raw if str(x).strip()])
                    else:
                        owner_name = str(owner_raw).strip()
                    if owner_name:
                        _set_source("owner_name", "pa_properties.record_json", owner_name)

                situs_raw = _get("situs_address", "property_address", "site_address")
                if situs_raw and not situs_address:
                    situs_address = str(situs_raw).strip()
                    if situs_address:
                        _set_source("situs_address", "pa_properties.record_json", situs_address)

                mailing_raw = _get("mailing_address", "owner_mailing_address", "mail_address")
                if mailing_raw and not owner_mailing_address:
                    owner_mailing_address = str(mailing_raw).strip()
                    if owner_mailing_address:
                        _set_source("owner_mailing_address", "pa_properties.record_json", owner_mailing_address)

                zoning_raw = _get("zoning")
                if zoning_raw and not zoning:
                    zoning = str(zoning_raw).strip()
                    if zoning:
                        _set_source("zoning", "pa_properties.record_json", zoning)

                flu_raw = _get("future_land_use", "future_landuse", "future_land_use_code")
                if flu_raw and not future_land_use:
                    future_land_use = str(flu_raw).strip()
                    if future_land_use:
                        _set_source("future_land_use", "pa_properties.record_json", future_land_use)

                land_use_raw = _get("land_use", "land_use_code", "use_type", "property_type")
                if land_use_raw and not land_use:
                    land_use = str(land_use_raw).strip()
                    if land_use:
                        _set_source("property_type", "pa_properties.record_json", land_use)

                property_class_raw = _get("property_class")
                if property_class_raw and not property_class:
                    property_class = str(property_class_raw).strip()

                la_raw = _get("living_area_sqft", "living_sf", "living_area", "heated_area", "building_sf")
                if living_area_sqft is None:
                    la = _norm_num(la_raw)
                    if la and la > 0:
                        living_area_sqft = la
                        _set_source("living_area_sqft", "pa_properties.record_json", living_area_sqft)

                lot_raw = _get("lot_size_sqft", "land_sqft", "land_sf")
                if lot_size_sqft is None:
                    ls = _norm_num(lot_raw)
                    if ls and ls > 0:
                        lot_size_sqft = ls
                        _set_source("lot_size_sqft", "pa_properties.record_json", lot_size_sqft)

                acres_raw = _get("lot_size_acres", "land_acres")
                if lot_size_acres is None:
                    la = _norm_num(acres_raw)
                    if la and la > 0:
                        lot_size_acres = la
                        _set_source("lot_size_acres", "pa_properties.record_json", lot_size_acres)

                beds_raw = _get("beds", "bedrooms", "bedroom")
                if beds is None:
                    b = _norm_num(beds_raw)
                    if b and b > 0:
                        beds = int(b)
                        _set_source("beds", "pa_properties.record_json", beds)

                baths_raw = _get("baths", "bathrooms", "bathroom")
                if baths is None:
                    ba = _norm_num(baths_raw)
                    if ba and ba > 0:
                        baths = float(ba)
                        _set_source("baths", "pa_properties.record_json", baths)

                yb_raw = _get("year_built", "yr_built", "built_year")
                if year_built is None:
                    yb = _norm_num(yb_raw)
                    if yb and yb > 0:
                        year_built = int(yb)
                        _set_source("year_built", "pa_properties.record_json", year_built)

                last_sale_raw = _get("last_sale_date", "sale_date")
                if not last_sale_date and last_sale_raw:
                    last_sale_date = str(last_sale_raw).strip()
                    if last_sale_date:
                        _set_source("last_sale_date", "pa_properties.record_json", last_sale_date)

                last_sale_price_raw = _get("last_sale_price", "sale_price")
                if not last_sale_price:
                    lp = _norm_num(last_sale_price_raw)
                    if lp and lp > 0:
                        last_sale_price = lp
                        _set_source("last_sale_price", "pa_properties.record_json", last_sale_price)

                just_raw = _get("just_value", "total_value", "total_just_value")
                if total_value is None:
                    tv = _norm_num(just_raw)
                    if tv and tv > 0:
                        total_value = tv
                        _set_source("total_value", "pa_properties.record_json", total_value)

                assessed_raw = _get("assessed_value", "total_assessed_value")
                if assessed_value is None:
                    av = _norm_num(assessed_raw)
                    if av and av > 0:
                        assessed_value = av
                        _set_source("assessed_value", "pa_properties.record_json", assessed_value)

                taxable_raw = _get("taxable_value")
                if taxable_value is None:
                    tv = _norm_num(taxable_raw)
                    if tv and tv > 0:
                        taxable_value = tv
                        _set_source("taxable_value", "pa_properties.record_json", taxable_value)

                land_value_raw = _get("land_value")
                if land_value is None:
                    lv = _norm_num(land_value_raw)
                    if lv and lv > 0:
                        land_value = lv
                        _set_source("land_value", "pa_properties.record_json", land_value)

                bldg_value_raw = _get("improvement_value", "building_value")
                if building_value is None:
                    bv = _norm_num(bldg_value_raw)
                    if bv and bv > 0:
                        building_value = bv
                        _set_source("building_value", "pa_properties.record_json", building_value)

                homestead_raw = _get("homestead", "homestead_flag", "homestead_exemption")
                if homestead_flag is None and homestead_raw is not None:
                    try:
                        if isinstance(homestead_raw, bool):
                            homestead_flag = homestead_raw
                        else:
                            s = str(homestead_raw).strip().lower()
                            homestead_flag = s in {"1", "true", "yes", "y"}
                    except Exception:
                        homestead_flag = None

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
                _set_source("owner_mailing_address", "pa_properties", owner_mailing_address)

                zoning = (pa.zoning or zoning).strip()
                future_land_use = (pa.future_land_use or future_land_use).strip()
                land_use = (pa.use_type or pa.land_use_code or land_use).strip()
                property_class = (pa.property_class or property_class).strip()

                living_area_sqft = _norm_num(pa.living_sf) or _norm_num(pa.building_sf)
                if living_area_sqft is not None and living_area_sqft > 0:
                    _set_source("living_area_sqft", "pa_properties", living_area_sqft)

                lot_size_sqft, lot_size_acres = _canonical_lot_sizes(pa.land_sf, pa.land_acres)
                if lot_size_sqft is not None and lot_size_sqft > 0:
                    _set_source("lot_size_sqft", "pa_properties", lot_size_sqft)
                if lot_size_acres is not None and lot_size_acres > 0:
                    _set_source("lot_size_acres", "pa_properties", lot_size_acres)

                beds = int(pa.bedrooms) if int(pa.bedrooms or 0) > 0 else None
                baths = float(pa.bathrooms) if float(pa.bathrooms or 0) > 0 else None
                if beds is not None:
                    _set_source("beds", "pa_properties", beds)
                if baths is not None:
                    _set_source("baths", "pa_properties", baths)

                year_built = int(pa.year_built) if int(pa.year_built or 0) > 0 else None
                if year_built is not None:
                    _set_source("year_built", "pa_properties", year_built)

                last_sale_date = pa.last_sale_date or last_sale_date
                last_sale_price = float(pa.last_sale_price or 0) or last_sale_price
                if last_sale_date:
                    _set_source("last_sale_date", "pa_properties", last_sale_date)
                if last_sale_price:
                    _set_source("last_sale_price", "pa_properties", last_sale_price)

                try:
                    ex = getattr(pa, "exemptions", None)
                    if isinstance(ex, (list, tuple)):
                        homestead_flag = any("HOMESTEAD" in str(x or "").upper() for x in ex)
                except Exception:
                    homestead_flag = None

                land_value = _norm_num(pa.land_value)
                building_value = _norm_num(pa.improvement_value)
                total_value = _norm_num(pa.just_value)
                assessed_value = _norm_num(pa.assessed_value)
                taxable_value = _norm_num(pa.taxable_value)
                if land_value is not None and land_value > 0:
                    _set_source("land_value", "pa_properties", land_value)
                if building_value is not None and building_value > 0:
                    _set_source("building_value", "pa_properties", building_value)
                if total_value is not None and total_value > 0:
                    _set_source("total_value", "pa_properties", total_value)
                if assessed_value is not None and assessed_value > 0:
                    _set_source("assessed_value", "pa_properties", assessed_value)
                if taxable_value is not None and taxable_value > 0:
                    _set_source("taxable_value", "pa_properties", taxable_value)

                _apply_record_json(pa_raw_by_id.get((feat_county, feat_pa_parcel_id)))

            if parcels_pa_row:
                try:
                    if not owner_mailing_address:
                        mailing_pp = _pt1_text(parcels_pa_row, "mailing_address")
                        if mailing_pp:
                            owner_mailing_address = mailing_pp
                            _set_source("owner_mailing_address", "parcels_pa", owner_mailing_address)
                    if living_area_sqft is None:
                        la_pp = _pt1_num(parcels_pa_row, "living_area_sqft")
                        if la_pp and la_pp > 0:
                            living_area_sqft = la_pp
                            _set_source("living_area_sqft", "parcels_pa", living_area_sqft)
                    if beds is None:
                        b_pp = _pt1_int(parcels_pa_row, "beds")
                        if b_pp and b_pp > 0:
                            beds = b_pp
                            _set_source("beds", "parcels_pa", beds)
                    if baths is None:
                        ba_pp = _pt1_num(parcels_pa_row, "baths")
                        if ba_pp and ba_pp > 0:
                            baths = float(ba_pp)
                            _set_source("baths", "parcels_pa", baths)
                    if year_built is None:
                        yb_pp = _pt1_int(parcels_pa_row, "year_built")
                        if yb_pp and yb_pp > 0:
                            year_built = yb_pp
                            _set_source("year_built", "parcels_pa", year_built)
                    if not last_sale_date:
                        ls_pp = _pt1_text(parcels_pa_row, "last_sale_date")
                        if ls_pp:
                            last_sale_date = ls_pp
                            _set_source("last_sale_date", "parcels_pa", last_sale_date)
                    if not last_sale_price:
                        lp_pp = _pt1_num(parcels_pa_row, "last_sale_price")
                        if lp_pp and lp_pp > 0:
                            last_sale_price = lp_pp
                            _set_source("last_sale_price", "parcels_pa", last_sale_price)
                except Exception:
                    pass

            if parcel_row:
                try:
                    if not owner_mailing_address:
                        owner_mailing_address = _pt1_mailing(parcel_row) or owner_mailing_address
                        _set_source("owner_mailing_address", "parcel_table1", owner_mailing_address)
                    if living_area_sqft is None:
                        living_area_sqft = _pt1_num(parcel_row, "LIVING_AREA") or _pt1_num(parcel_row, "TOTAL_SQFT")
                        if living_area_sqft:
                            _set_source("living_area_sqft", "parcel_table1", living_area_sqft)
                    if year_built is None:
                        yb = _pt1_int(parcel_row, "BASE_YR_BLT")
                        year_built = yb if yb and yb > 0 else None
                        if year_built is not None:
                            _set_source("year_built", "parcel_table1", year_built)
                    if land_value is None:
                        land_value = _pt1_num(parcel_row, "APPR_LAND")
                        if land_value:
                            _set_source("land_value", "parcel_table1", land_value)
                    if building_value is None:
                        building_value = _pt1_num(parcel_row, "APPR_BLDG")
                        if building_value:
                            _set_source("building_value", "parcel_table1", building_value)
                    if total_value is None:
                        total_value = _pt1_num(parcel_row, "TOTAL_JUST_VALUE")
                        if total_value:
                            _set_source("total_value", "parcel_table1", total_value)
                    if assessed_value is None:
                        assessed_value = _pt1_num(parcel_row, "TOTAL_ASSESSED_VALUE")
                        if assessed_value:
                            _set_source("assessed_value", "parcel_table1", assessed_value)
                    hmst = _pt1_text(parcel_row, "HMST_YEAR_GRANTED")
                    if hmst:
                        homestead_flag = True
                except Exception:
                    pass

            if zoning:
                _set_source("zoning", "pa_properties", zoning)
            if future_land_use:
                _set_source("future_land_use", "pa_properties", future_land_use)
            if land_use:
                _set_source("property_type", "pa_properties", land_use)

            rollup = rollups_by_id.get((feat_county, feat.parcel_id)) if rollups_loaded else None
            rollup_keys = rollup_keys_by_id.get((feat_county, feat.parcel_id), set())

            signals = {
                "absentee_owner": False,
                "homestead": bool(homestead_flag) if homestead_flag is not None else False,
                "has_permits": bool(rollup.get("has_permits")) if isinstance(rollup, dict) else False,
                "has_official_records": bool(rollup.get("has_official_records")) if isinstance(rollup, dict) else False,
                "has_tax_events": bool(rollup.get("has_tax")) if isinstance(rollup, dict) else False,
                "has_code_enforcement": bool(rollup.get("has_code_enforcement")) if isinstance(rollup, dict) else False,
                "has_courts": bool(rollup.get("has_courts")) if isinstance(rollup, dict) else False,
                "has_gis_planning": bool(rollup.get("has_gis_planning")) if isinstance(rollup, dict) else False,
                "has_property_appraiser": bool(rollup.get("has_gis_planning")) if isinstance(rollup, dict) else False,
            }
            try:
                mail_norm = _norm_addr(owner_mailing_address)
                situs_norm = _norm_addr(situs_address)
                if mail_norm and situs_norm and mail_norm != situs_norm:
                    signals["absentee_owner"] = True
            except Exception:
                pass

            if signal_filter_active:
                key_match = True
                group_match = True
                tier_match = True
                score_match = True

                if trigger_keys:
                    key_match = False
                    for k in trigger_keys:
                        if k in signals and bool(signals.get(k)):
                            key_match = True
                            break
                        if k in rollup_keys:
                            key_match = True
                            break

                if trigger_groups:
                    group_match = False
                    ownership_active = bool(signals.get("absentee_owner")) or bool(signals.get("homestead"))
                    for g in trigger_groups:
                        if g == "permits" and signals.get("has_permits"):
                            group_match = True
                            break
                        if g in {"official_records", "records"} and signals.get("has_official_records"):
                            group_match = True
                            break
                        if g == "tax" and signals.get("has_tax_events"):
                            group_match = True
                            break
                        if g in {"code", "code_enforcement"} and signals.get("has_code_enforcement"):
                            group_match = True
                            break
                        if g == "courts" and signals.get("has_courts"):
                            group_match = True
                            break
                        if g in {"gis", "gis_planning", "appraiser", "property_appraiser"} and signals.get("has_gis_planning"):
                            group_match = True
                            break
                        if g == "ownership" and ownership_active:
                            group_match = True
                            break

                if trigger_tiers:
                    tier_match = False
                    if isinstance(rollup, dict):
                        count_critical = int(rollup.get("count_critical") or 0)
                        count_strong = int(rollup.get("count_strong") or 0)
                        count_support = int(rollup.get("count_support") or 0)
                        for t in trigger_tiers:
                            if t == "critical" and count_critical > 0:
                                tier_match = True
                                break
                            if t == "strong" and count_strong > 0:
                                tier_match = True
                                break
                            if t == "support" and count_support > 0:
                                tier_match = True
                                break

                if trigger_min_score is not None:
                    score_match = False
                    if isinstance(rollup, dict):
                        try:
                            score_match = int(rollup.get("seller_score") or 0) >= int(trigger_min_score)
                        except Exception:
                            score_match = False

                if not (key_match and group_match and tier_match and score_match):
                    filter_stage_counts["trigger_failed"] += 1
                    if explain_enabled:
                        trigger_drop_reasons["signals:no_match"] = (
                            int(trigger_drop_reasons.get("signals:no_match", 0)) + 1
                        )
                    continue

            rollup_payload = None
            if isinstance(rollup, dict):
                try:
                    rollup_payload = {
                        "seller_score": int(rollup.get("seller_score") or 0),
                        "count_critical": int(rollup.get("count_critical") or 0),
                        "count_strong": int(rollup.get("count_strong") or 0),
                        "count_support": int(rollup.get("count_support") or 0),
                        "has_permits": int(rollup.get("has_permits") or 0),
                        "has_official_records": int(rollup.get("has_official_records") or 0),
                        "has_tax": int(rollup.get("has_tax") or 0),
                        "has_code_enforcement": int(rollup.get("has_code_enforcement") or 0),
                        "has_courts": int(rollup.get("has_courts") or 0),
                        "has_gis_planning": int(rollup.get("has_gis_planning") or 0),
                        "trigger_keys": sorted(rollup_keys) if rollup_keys else [],
                    }
                except Exception:
                    rollup_payload = None

            signal_keys_out: list[str] = []
            try:
                for k, v in (signals or {}).items():
                    if bool(v):
                        signal_keys_out.append(str(k))
            except Exception:
                signal_keys_out = []
            try:
                for k in rollup_keys:
                    if k not in signal_keys_out:
                        signal_keys_out.append(k)
            except Exception:
                pass

            zoning_out = zoning.strip() or None
            zoning_reason = None
            if zoning_out is None:
                zoning_reason = "not_provided_by_source"

            property_type_norm_out = None
            try:
                property_type_norm_out = str(fields.get("property_type_norm") or "").strip() or None
            except Exception:
                property_type_norm_out = None
            property_type_raw_out = None
            try:
                property_type_raw_out = str(land_use or property_class or "").strip() or None
            except Exception:
                property_type_raw_out = None

            try:
                lot_size_sqft, lot_size_acres = _canonical_lot_sizes(lot_size_sqft, lot_size_acres)
            except Exception:
                pass

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

            pa_parcel_id = feat_pa_parcel_id
            mapped_id = None
            try:
                mapped_id = parcel_id_map_by_county.get(feat_county, {}).get(feat.parcel_id)
            except Exception:
                mapped_id = None
            mapping_status = "mapped" if mapped_id and mapped_id != feat.parcel_id else "missing"
            if mapped_id is None:
                mapping_status = "missing"
            if mapped_id and mapped_id == feat.parcel_id:
                mapping_status = "identity"
            if mapped_id and mapped_id != feat.parcel_id:
                _set_source("pa_parcel_id", "parcel_id_map", pa_parcel_id)
            elif mapped_id is None:
                _set_source("pa_parcel_id", "identity", pa_parcel_id)
            rec = {
                "record_version": 1,
                "parcel_id": feat.parcel_id,
                "pa_parcel_id": pa_parcel_id if pa_parcel_id != feat.parcel_id else None,
                "county": feat_county,
                "situs_address": situs_address.strip() or None,
                "owner_name": owner_name.strip() or None,
                "owner_mailing_address": owner_mailing_address.strip() or None,
                "homestead_flag": homestead_flag,
                "property_type": property_type_norm_out or property_type_raw_out,
                "property_type_raw": property_type_raw_out,
                "land_use": land_use,
                "future_land_use": future_land_use.strip() or None,
                "beds": beds,
                "baths": baths,
                "year_built": year_built,
                "ownership_years": fields.get("ownership_years"),
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
                "signals": signals,
                "signal_keys": signal_keys_out,
                "rollup": rollup_payload,
                "land_value": land_value,
                "building_value": building_value,
                "total_value": total_value,
                "assessed_value": assessed_value,
                "taxable_value": taxable_value,
                "source_coverage": {
                    "fields": field_sources,
                    "sources": sorted({v for v in field_sources.values() if v}),
                    "mapping_status": mapping_status,
                    "mapping_source": "parcel_id_map" if mapped_id else None,
                },
                "field_provenance": field_sources,
                # Back-compat fields (older UI code paths)
                "address": situs_address,
                "flu": land_use,
                "property_class": property_class,
                "living_area_sqft": living_area_sqft,
                "lot_size_sqft": lot_size_sqft,
                "lot_size_acres": lot_size_acres,
            }
            lat = None
            lng = None
            if pa is not None:
                try:
                    lat = float(getattr(pa, "latitude", None)) if getattr(pa, "latitude", None) is not None else None
                except Exception:
                    lat = None
                try:
                    lng = float(getattr(pa, "longitude", None)) if getattr(pa, "longitude", None) is not None else None
                except Exception:
                    lng = None
            if lat is None or lng is None:
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

            row = {
                "county": feat_county,
                "parcel_id": feat.parcel_id,
                "hover_fields": hover,
                "reason_codes": reason_codes,
            }
            if include_geometry:
                row["geometry"] = feat.geometry
            results_all.append(row)

            records_all.append(rec)
            try:
                if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
                    if float(lat) != 0.0 or float(lng) != 0.0:
                        stage_counts["latlng_available"] += 1
            except Exception:
                pass

            filter_stage_counts["emitted"] += 1

        filter_stage_counts["intersecting"] = int(stage_counts.get("geometry_passed", 0))
        if candidates and stage_counts.get("geometry_passed", 0) == 0:
            warnings.append("No parcels intersected the drawn geometry")

        _mark("apply_filters")

        try:
            scanned = int(field_stats.get("scanned", 0) or 0)
            present = field_stats.get("present") or {}
            fields_out: dict[str, dict[str, float | int]] = {}
            missing: dict[str, int] = {}
            coverage: dict[str, float] = {}
            if isinstance(present, dict):
                for key, val in present.items():
                    p = int(val or 0)
                    t = int(scanned)
                    pct = (float(p) / float(t)) if t > 0 else 0.0
                    missing[str(key)] = max(0, t - p)
                    coverage[str(key)] = pct
                    fields_out[str(key)] = {
                        "present": p,
                        "missing": max(0, t - p),
                        "total": t,
                        "pct": pct,
                    }
            field_stats["fields"] = fields_out
            field_stats["coverage_candidates"] = {
                "total": int(scanned),
                "present": {str(k): int(v or 0) for k, v in (present or {}).items()},
                "missing": missing,
                "coverage": coverage,
                "fields": fields_out,
            }
        except Exception:
            pass

        stage_counts["returned"] = int(len(records_all))

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

                    records_all.sort(key=_k)
                elif sort_key == "year_built_desc":
                    records_all.sort(
                        key=lambda r: (
                            r.get("year_built") is None,
                            -int(r.get("year_built") or 0),
                        )
                    )
                elif sort_key == "sqft_desc":
                    records_all.sort(
                        key=lambda r: (
                            r.get("living_area_sqft") is None,
                            -float(r.get("living_area_sqft") or 0.0),
                        )
                    )
            except Exception:
                pass

        _mark("sort")

        def _pid(item: dict) -> str:
            try:
                pid = str(item.get("parcel_id") or "").strip()
                ckey = str(item.get("county") or county_key).strip().lower()
                return f"{ckey}:{pid}" if ckey else pid
            except Exception:
                return ""

        if not sort_key:
            def _source_rank(item: dict) -> int:
                src = str(item.get("source") or "").strip().lower()
                if src == "cache":
                    return 0
                if src == "live":
                    return 1
                if src == "missing":
                    return 3
                return 2

            def _completeness_score(item: dict) -> int:
                score = 0
                for field in (
                    "owner_name",
                    "owner_mailing_address",
                    "living_area_sqft",
                    "lot_size_sqft",
                    "zoning",
                    "future_land_use",
                    "total_value",
                    "assessed_value",
                    "taxable_value",
                    "photo_url",
                ):
                    v = item.get(field)
                    if v is None:
                        continue
                    if isinstance(v, str) and not v.strip():
                        continue
                    if isinstance(v, (int, float)) and float(v) <= 0.0:
                        continue
                    if isinstance(v, (list, tuple, set, dict)) and len(v) == 0:
                        continue
                    score += 1
                return score

            records_all.sort(
                key=lambda r: (
                    _source_rank(r),
                    -_completeness_score(r),
                    _pid(r),
                )
            )

        try:
            results_map = {str(r.get("parcel_id") or "").strip(): r for r in results_all}
            ordered_results = []
            for rec in records_all:
                pid = _pid(rec)
                row = results_map.get(pid)
                if row is not None:
                    ordered_results.append(row)
            if ordered_results:
                results_all = ordered_results
        except Exception:
            pass

        cursor_mode = "index" if bool(sort_key) else "pid"

        start_index = 0
        if cursor:
            try:
                if cursor_mode == "index":
                    start_index = max(0, int(cursor) + 1)
                else:
                    for i, item in enumerate(records_all):
                        if _pid(item) > cursor:
                            start_index = i
                            break
                    else:
                        start_index = len(records_all)
            except Exception:
                start_index = 0
        elif offset is not None:
            start_index = max(0, int(offset))

        effective_limit = int(limit) if limit is not None else int(len(records_all))
        end_index = start_index + effective_limit
        records = records_all[start_index:end_index]
        results = results_all[start_index:end_index]

        total_count = int(len(records_all))
        returned_count = int(len(records))
        has_more = bool(start_index + returned_count < total_count)
        if has_more and records:
            if cursor_mode == "index":
                next_cursor = str(start_index + returned_count - 1)
            else:
                next_cursor = _pid(records[-1])
        else:
            next_cursor = None

        # Flag whether we stopped early due to the limit.
        records_truncated = bool(has_more)

        # Recompute field_stats from the same records returned in this response.
        def _compute_field_stats_from_records(rows: list[dict]) -> dict[str, Any]:
            fields = [
                "living_area_sqft",
                "lot_size_sqft",
                "lot_size_acres",
                "beds",
                "baths",
                "year_built",
                "owner_name",
                "owner_mailing_address",
                "total_value",
                "land_value",
                "building_value",
                "assessed_value",
                "last_sale_date",
                "last_sale_price",
                "property_type",
                "ownership_years",
                "zoning",
                "future_land_use",
            ]
            scanned = int(len(rows))
            present: dict[str, int] = {k: 0 for k in fields}

            def _num(value: object) -> float | None:
                try:
                    if value is None:
                        return None
                    if isinstance(value, (int, float)):
                        v = float(value)
                        return v if v > 0 else None
                    s = str(value).strip()
                    if not s or s.upper() == "NULL":
                        return None
                    s = s.replace(",", "")
                    m = re.search(r"[-+]?\d*\.?\d+", s)
                    if not m:
                        return None
                    v = float(m.group(0))
                    return v if v > 0 else None
                except Exception:
                    return None

            def _present(field: str, value: object) -> bool:
                if value is None:
                    return False
                if isinstance(value, str):
                    return bool(value.strip())
                if field in {"beds", "baths", "year_built", "ownership_years"}:
                    try:
                        v = _num(value)
                        return bool(v and v > 0)
                    except Exception:
                        return False
                if field in {"living_area_sqft", "lot_size_sqft", "lot_size_acres", "total_value", "land_value", "building_value", "assessed_value", "last_sale_price"}:
                    try:
                        v = _num(value)
                        return bool(v and v > 0)
                    except Exception:
                        return False
                return True

            for rec in rows:
                for key in fields:
                    if _present(key, rec.get(key)):
                        present[key] += 1

            missing: dict[str, int] = {k: max(0, scanned - int(present[k])) for k in fields}
            coverage: dict[str, float] = {
                k: (float(present[k]) / float(scanned)) if scanned > 0 else 0.0 for k in fields
            }

            detail: dict[str, dict[str, float | int]] = {}
            for key in fields:
                detail[key] = {
                    "present": int(present[key]),
                    "missing": int(missing[key]),
                    "total": int(scanned),
                    "pct": float(coverage[key]),
                }

            return {
                "scanned": scanned,
                "present": present,
                "missing": missing,
                "coverage": coverage,
                "fields": detail,
            }

        field_stats_candidates = field_stats if isinstance(field_stats, dict) else {}
        field_stats_returned = _compute_field_stats_from_records(records_all)
        field_stats_returned["coverage_candidates"] = field_stats_candidates.get("coverage_candidates")
        field_stats_returned["coverage_returned"] = {
            "total": int(field_stats_returned.get("scanned", 0) or 0),
            "present": field_stats_returned.get("present") or {},
            "missing": field_stats_returned.get("missing") or {},
            "coverage": field_stats_returned.get("coverage") or {},
            "fields": field_stats_returned.get("fields") or {},
        }
        field_stats_returned["missing_policy_effect"] = str(missing_policy)
        if "sample_candidate" in field_stats_candidates:
            field_stats_returned["sample_candidate"] = field_stats_candidates.get("sample_candidate")
        # Return merged-record coverage for UI filter availability.
        field_stats = field_stats_returned

        completeness_gate: dict[str, Any] = {
            "status": "not_evaluated",
            "sample_size": int(field_stats_returned.get("scanned", 0) or 0),
            "min_sample": 20,
            "checks": [],
            "failed_checks": [],
        }
        try:
            coverage_map = (
                (field_stats_returned.get("coverage_returned") or {}).get("coverage")
                if isinstance(field_stats_returned.get("coverage_returned"), dict)
                else {}
            )
            if not isinstance(coverage_map, dict):
                coverage_map = {}
            sample_size = int(field_stats_returned.get("scanned", 0) or 0)
            min_sample = 20
            thresholds: dict[str, float] = {
                "owner_name": 0.95,
                "owner_mailing_address": 0.60,
                "total_value": 0.70,
            }

            checks: list[dict[str, Any]] = []
            failed_checks: list[dict[str, Any]] = []
            if sample_size >= min_sample:
                for field_key, threshold in thresholds.items():
                    cov_val = float(coverage_map.get(field_key, 0.0) or 0.0)
                    check = {
                        "field": field_key,
                        "coverage": round(cov_val, 4),
                        "threshold": float(threshold),
                        "status": "pass" if cov_val >= threshold else "fail",
                    }
                    checks.append(check)
                    if check["status"] == "fail":
                        failed_checks.append(check)
                completeness_gate = {
                    "status": "fail" if failed_checks else "pass",
                    "sample_size": sample_size,
                    "min_sample": min_sample,
                    "checks": checks,
                    "failed_checks": failed_checks,
                }
            else:
                completeness_gate = {
                    "status": "insufficient_sample",
                    "sample_size": sample_size,
                    "min_sample": min_sample,
                    "checks": checks,
                    "failed_checks": failed_checks,
                }

            for failed in failed_checks:
                f = str(failed.get("field") or "")
                c = float(failed.get("coverage") or 0.0)
                t = float(failed.get("threshold") or 0.0)
                token = f"completeness_low:{f}:{int(round(c * 100))}%<{int(round(t * 100))}%"
                if token not in warnings:
                    warnings.append(token)
        except Exception:
            pass

        debug_flags: dict[str, Any] | None = None
        if debug_response_enabled:
            debug_flags = {
                "county": county_label,
                "correlation_id": correlation_id,
                "limit": int(effective_limit),
                "include_geometry": bool(include_geometry),
                "sort": str(payload.get("sort") or ""),
                "enrich_enabled": bool(payload.get("enrich", False)) if payload.get("enrich", None) is not None else False,
                "records_truncated": bool(records_truncated),
                "explain": bool(explain_enabled),
                "missing_policy": str(missing_policy),
                "applied_where_clauses": applied_where_clauses,
                "resolved_filter_payload": raw_filters if isinstance(raw_filters, dict) else None,
            }

        if debug_counts is not None:
            try:
                debug_counts.update(
                    {
                        "candidate_count": int(len(intersecting)),
                        "filtered_count": int(total_count),
                        "returned_count": int(returned_count),
                        "records_truncated": bool(records_truncated),
                    }
                )
            except Exception:
                pass

        _mark("serialize")

        _append_search_debug(
            {
                "event": "result",
                "county": county_label,
                "candidate_count": len(intersecting),
                "filtered_count": int(total_count),
                "warnings": warnings,
                "field_stats": field_stats,
                "filter_stage_counts": filter_stage_counts,
            }
        )

        try:
            log_path = "/tmp/api.log"
            line = json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "trace_id": correlation_id,
                    "event": "search_result",
                    "county": county_label,
                    "records_count": int(total_count),
                    "markers_possible_count": int(stage_counts.get("latlng_available", 0)),
                    "after_attr_filters": int(stage_counts.get("filter_passed", 0)),
                    "after_geom_clip": int(stage_counts.get("geometry_passed", 0)),
                },
                default=str,
            )
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

        response_headers = {"X-Correlation-Id": correlation_id} if correlation_id else None
        try:
            debug_ids_sample = []
            for rec in records[:10]:
                pid = str(rec.get("parcel_id") or "").strip()
                if pid:
                    debug_ids_sample.append(pid)
        except Exception:
            debug_ids_sample = []

        explain_payload = None
        if explain_enabled:
            dropped_reasons = {}
            dropped_reasons.update(filter_drop_reasons)
            dropped_reasons.update(trigger_drop_reasons)
            markers_possible = int(stage_counts.get("latlng_available", 0))
            records_count = int(total_count)
            scanned = int(field_stats.get("scanned", 0) or 0)
            present = field_stats.get("present") or {}

            provider_status_payload = None
            try:
                provider_status_payload = _provider_status_payload(county_key)
            except Exception:
                provider_status_payload = None

            time_ms_total = 0
            try:
                if isinstance(debug_timing_ms, dict):
                    time_ms_total = int(sum(int(v or 0) for v in debug_timing_ms.values()))
            except Exception:
                time_ms_total = 0

            def _pct(key: str) -> float:
                if scanned <= 0:
                    return 0.0
                try:
                    return round((float(present.get(key, 0) or 0) / float(scanned)) * 100.0, 2)
                except Exception:
                    return 0.0

            explain_payload = {
                "records_count": records_count,
                "total_count": int(total_count),
                "returned_count": int(returned_count),
                "has_more": bool(has_more),
                "next_cursor": next_cursor,
                "polygon_match_mode": str(payload.get("polygon_match_mode") or "intersects"),
                "diagnostics": {
                    "geometry_type": geometry_type,
                    "bbox": bbox_t,
                    "county_resolved": county_label,
                    "candidate_count": int(stage_counts.get("candidates", 0)),
                    "filtered_count": int(stage_counts.get("filter_passed", 0)),
                    "dropped_by_filter": filter_explain_counts,
                    "sample_ids": {
                        "candidates": candidate_ids_sample,
                        "after_filters": after_filter_ids_sample,
                        "after_geometry": after_geom_ids_sample,
                        "returned": debug_ids_sample,
                    },
                },
                "markers_possible_count": markers_possible,
                "missing_lat_lng_count": max(0, records_count - markers_possible),
                "stage_counts": {
                    "candidates": int(stage_counts.get("candidates", 0)),
                    "after_attr_filters": int(stage_counts.get("filter_passed", 0)),
                    "after_geom_clip": int(stage_counts.get("geometry_passed", 0)),
                    "final": int(records_count),
                },
                "candidates_count": int(stage_counts.get("candidates", 0)),
                "after_attr_filters_count": int(stage_counts.get("filter_passed", 0)),
                "after_geom_clip_count": int(stage_counts.get("geometry_passed", 0)),
                "final_count": int(records_count),
                "stage_counts_raw": stage_counts,
                "dropped_reasons": dropped_reasons,
                "missing_field_counts": missing_field_counts,
                "field_coverage": {
                    "percent_with_beds": _pct("beds"),
                    "percent_with_sqft": _pct("living_area_sqft"),
                    "percent_with_year_built": _pct("year_built"),
                    "percent_with_value": _pct("total_value"),
                },
                "filter_metrics": filter_explain_counts,
                "filters": raw_filters if isinstance(raw_filters, dict) else raw_filters,
                "missing_policy": str(missing_policy),
                "filter_stage_counts": filter_stage_counts,
                "provider_status": provider_status_payload,
                "time_ms": int(time_ms_total),
            }

        explain_contract = _search_explain_contract(
            counts={
                "total_count": int(total_count),
                "returned_count": int(returned_count),
                "candidate_count": int(stage_counts.get("candidates", 0)),
                "filtered_count": int(stage_counts.get("filter_passed", 0)),
            },
            warnings=warnings,
            timings_ms=debug_timing_ms or {},
            filter_metrics=filter_explain_counts if explain_enabled else [],
            field_stats=field_stats_returned if field_stats_returned else field_stats,
            debug_ids_sample=debug_ids_sample,
            details=explain_payload,
        )

        data_quality_payload = {
            "mode": "evidence_only",
            "completeness_gate": completeness_gate,
        }

        return JSONResponse(
            {
                "ok": True,
                "data_quality": data_quality_payload,
                "hover_fields_mode": "evidence_only",
                "search_id": search_id,
                "correlation_id": correlation_id,
                # Backwards-compatible keys
                "county": county_label,
                "count": len(results),
                "results": results,
                # New UI payload
                "zoning_options": zoning_options,
                "future_land_use_options": future_land_use_options,
                "summary": {
                    "count": int(total_count),
                    "returned_count": int(returned_count),
                    "total_count": int(total_count),
                    "candidate_count": len(intersecting),
                    "filtered_count": int(total_count),
                    "source_counts": source_counts,
                    "source_counts_legacy": legacy_source_counts,
                },
                "records": records,
                "total_count": int(total_count),
                "returned_count": int(returned_count),
                "has_more": bool(has_more),
                "next_cursor": next_cursor,
                "records_truncated": bool(records_truncated),
                "warnings": warnings,
                "field_stats": field_stats,
                "field_stats_returned": field_stats_returned,
                "filter_stage_counts": filter_stage_counts,
                "error_reason": live_error_reason,
                "explain": explain_contract,
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
            },
            headers=response_headers,
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

        max_per_minute = payload.get("max_per_minute", None)
        try:
            max_per_minute = int(max_per_minute) if max_per_minute is not None else 0
        except Exception:
            max_per_minute = 0
        if max_per_minute is None or max_per_minute < 0:
            max_per_minute = 0
        throttle_s = (60.0 / float(max_per_minute)) if max_per_minute and max_per_minute > 0 else 0.0

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
        requested = int(len(parcel_ids))
        cached = 0
        fetched_ok = 0
        fetched_failed = 0
        skipped = 0
        failures: list[dict[str, Any]] = []
        start_ts = None
        try:
            import time as _time

            start_ts = _time.time()
        except Exception:
            start_ts = None
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

        db_path = _resolve_db_path("PA_DB", DEFAULT_LEADS_DB)
        store = PASQLite(db_path)
        errors: dict[str, Any] = {}
        try:
            last_fetch_at = None
            for pid in parcel_ids:
                row = rows.get(pid)
                if row is None and county_key != "orange":
                    errors[pid] = "not_found_in_fdor_centroids"
                    fetched_failed += 1
                    failures.append({"parcel_id": pid, "error": "not_found_in_fdor_centroids"})
                    continue

                existing = None
                try:
                    existing = store.get(county=county_key, parcel_id=str(pid))
                except Exception:
                    existing = None

                if existing is not None:
                    cached += 1
                    skipped += 1
                    continue

                base_sources: list[dict] = []
                if row is not None:
                    base_sources.append({"name": "fdor_centroids", "url": row.raw_source_url})

                if county_key == "orange":
                    # Orange: authoritative enrichment via OCPA.
                    try:
                        if throttle_s > 0:
                            try:
                                import time as _time

                                if last_fetch_at is not None:
                                    delta = _time.time() - float(last_fetch_at)
                                    if delta < throttle_s:
                                        _time.sleep(float(throttle_s) - delta)
                                last_fetch_at = _time.time()
                            except Exception:
                                pass
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
                        fetched_failed += 1
                        failures.append({"parcel_id": pid, "error": "exception", "hint": str(e)})
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
                    fetched_ok += 1
                except Exception as e:
                    errors[pid] = {"error_reason": "cache_upsert_failed", "hint": str(e)}
                    fetched_failed += 1
                    failures.append({"parcel_id": pid, "error": "cache_upsert_failed", "hint": str(e)})

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
            assessed_value = float(pa.assessed_value or 0) or None
            taxable_value = float(pa.taxable_value or 0) or None

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
                    "assessed_value": assessed_value,
                    "taxable_value": taxable_value,
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

        elapsed_s = None
        try:
            if start_ts is not None:
                import time as _time

                elapsed_s = float(_time.time() - float(start_ts))
        except Exception:
            elapsed_s = None

        return JSONResponse(
            {
                "county": county_key,
                "count": len(records),
                "requested": requested,
                "cached": cached,
                "fetched_ok": fetched_ok,
                "fetched_failed": fetched_failed,
                "skipped": skipped,
                "failures": failures,
                "elapsed_s": elapsed_s,
                "records": records,
                "errors": errors,
            }
        )

    @app.get("/api/debug/ping")
    def debug_ping():
        logger = logging.getLogger("fps.debug")
        sha = os.getenv("APP_GIT_SHA") or ""
        branch = os.getenv("APP_GIT_BRANCH") or ""
        try:
            if not sha or not branch:
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
        except Exception as e:
            logger.warning("debug ping git lookup failed: %s", e)

        now_iso = datetime.now(timezone.utc).isoformat()
        parcels_db_path = os.getenv("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
        leads_db_path = os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
        parcels_db_ok = False
        leads_db_ok = False
        try:
            parcels_db_ok = bool(Path(parcels_db_path).exists())
        except Exception:
            parcels_db_ok = False
        try:
            leads_db_ok = bool(Path(leads_db_path).exists())
        except Exception:
            leads_db_ok = False
        parcels_count = 0
        rollups_count = 0
        try:
            if parcels_db_ok:
                import sqlite3 as _sqlite3

                con = _sqlite3.connect(parcels_db_path)
                cur = con.cursor()
                parcels_count = int(cur.execute("SELECT count(*) FROM parcels").fetchone()[0])
                con.close()
        except Exception:
            parcels_count = 0
        try:
            if leads_db_ok:
                import sqlite3 as _sqlite3

                lcon = _sqlite3.connect(leads_db_path)
                lcur = lcon.cursor()
                rollups_count = int(lcur.execute("SELECT count(*) FROM parcel_trigger_rollups").fetchone()[0])
                lcon.close()
        except Exception:
            rollups_count = 0
        return {
            "ok": True,
            "version": os.getenv("APP_VERSION", "dev"),
            "time": now_iso,
            "server_time": now_iso,
            "db_ok": bool(parcels_db_ok),
            "parcels_db_ok": bool(parcels_db_ok),
            "leads_db_ok": bool(leads_db_ok),
            "parcels_db_path": os.getenv("PARCELS_DB_PATH", DEFAULT_PARCELS_DB),
            "leads_db_path": os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB),
            "parcels_count": parcels_count,
            "rollups_count": rollups_count,
            "git": {"sha": sha or "dev", "branch": branch or "unknown"},
            "env": {
                "FPS_USE_FDOR_CENTROIDS": os.getenv("FPS_USE_FDOR_CENTROIDS", ""),
                "PA_DB": os.getenv("PA_DB", ""),
                "LEADS_SQLITE_PATH": os.getenv("LEADS_SQLITE_PATH", ""),
                "APP_GIT_SHA": os.getenv("APP_GIT_SHA", ""),
                "APP_GIT_BRANCH": os.getenv("APP_GIT_BRANCH", ""),
            },
        }

    @app.get("/api/registry/counties")
    def registry_counties():
        return {"ok": True, "counties": registry_payload()}

    @app.post("/api/geo/resolve_county")
    def resolve_county(payload: dict = Body(...)):
        logger = logging.getLogger("fps.geo")
        lat = None
        lng = None

        if isinstance(payload, dict) and payload.get("county"):
            slug = normalize_county_slug(str(payload.get("county") or ""))
            if not slug or get_registry_county(slug) is None:
                return {"ok": False, "county": None, "counties": [], "source": "registry", "error": "unknown_county"}
            return {"ok": True, "county": slug, "counties": [slug], "source": "registry"}

        try:
            if isinstance(payload, dict):
                lat_val = payload.get("lat")
                lng_val = payload.get("lng")
                if isinstance(lat_val, (int, float, str)):
                    lat = float(lat_val)
                if isinstance(lng_val, (int, float, str)):
                    lng = float(lng_val)
                center = payload.get("center") or {}
                if lat is None and isinstance(center, dict):
                    c_lat = center.get("lat")
                    if isinstance(c_lat, (int, float, str)):
                        lat = float(c_lat)
                if lng is None and isinstance(center, dict):
                    c_lng = center.get("lng")
                    if isinstance(c_lng, (int, float, str)):
                        lng = float(c_lng)
        except Exception:
            lat = None
            lng = None

        polygon = payload.get("polygon_geojson") if isinstance(payload, dict) else None

        def _centroid_from_polygon(poly: object) -> tuple[float, float] | None:
            try:
                if not isinstance(poly, dict):
                    return None
                if str(poly.get("type") or "").lower() != "polygon":
                    return None
                coords = poly.get("coordinates")
                if not isinstance(coords, list) or not coords:
                    return None
                ring = coords[0]
                if not isinstance(ring, list) or len(ring) < 3:
                    return None
                minx = min(float(p[0]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2)
                maxx = max(float(p[0]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2)
                miny = min(float(p[1]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2)
                maxy = max(float(p[1]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2)
                return (float(miny + maxy) / 2.0, float(minx + maxx) / 2.0)
            except Exception:
                return None

        def _bbox_from_polygon(poly: object) -> tuple[float, float, float, float] | None:
            try:
                if not isinstance(poly, dict):
                    return None
                if str(poly.get("type") or "").lower() != "polygon":
                    return None
                coords = poly.get("coordinates")
                if not isinstance(coords, list) or not coords:
                    return None
                ring = coords[0]
                if not isinstance(ring, list) or len(ring) < 3:
                    return None
                xs = [float(p[0]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
                ys = [float(p[1]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
                if not xs or not ys:
                    return None
                return (min(xs), min(ys), max(xs), max(ys))
            except Exception:
                return None

        polygon_bbox = _bbox_from_polygon(polygon) if polygon is not None else None
        if (lat is None or lng is None) and polygon is not None:
            cent = _centroid_from_polygon(polygon)
            if cent is not None:
                lat, lng = cent

        if lat is None or lng is None:
            return {"ok": True, "county": None, "counties": [], "source": "none"}

        db_path = Path(_resolve_db_path("PARCELS_DB_PATH", DEFAULT_PARCELS_DB))
        if not db_path.exists():
            return {"ok": True, "county": None, "counties": [], "source": "missing_parcels_db"}

        try:
            import sqlite3 as _sqlite3

            conn = _sqlite3.connect(str(db_path))
            conn.row_factory = _sqlite3.Row
            if polygon_bbox is not None:
                minx, miny, maxx, maxy = polygon_bbox
                rows = conn.execute(
                    """
                                        SELECT DISTINCT county FROM parcels
                                        WHERE rowid IN (
                                            SELECT rowid FROM parcels_rtree
                                            WHERE minx <= ? AND maxx >= ? AND miny <= ? AND maxy >= ?
                                        )
                    """,
                    (float(maxx), float(minx), float(maxy), float(miny)),
                ).fetchall()
                counties = [str(r["county"] or "").strip().lower() for r in rows if r and r["county"]]
                county = counties[0] if len(counties) == 1 else ""
            else:
                cur = conn.execute(
                    """
                                        SELECT county FROM parcels
                                        WHERE rowid IN (
                                            SELECT rowid FROM parcels_rtree
                                            WHERE minx <= ? AND maxx >= ? AND miny <= ? AND maxy >= ?
                                        )
                    LIMIT 1
                    """,
                    (float(lng), float(lng), float(lat), float(lat)),
                )
                row = cur.fetchone()
                county = str(row["county"] or "").strip().lower() if row else ""

                counties = []
                try:
                    rows = conn.execute(
                        """
                                                SELECT DISTINCT county FROM parcels
                                                WHERE rowid IN (
                                                    SELECT rowid FROM parcels_rtree
                                                    WHERE minx <= ? AND maxx >= ? AND miny <= ? AND maxy >= ?
                                                )
                        """,
                        (float(lng), float(lng), float(lat), float(lat)),
                    ).fetchall()
                    counties = [str(r["county"] or "").strip().lower() for r in rows if r and r["county"]]
                except Exception:
                    counties = []
            conn.close()
            county = normalize_county_slug(county) if county else ""
            if county and get_registry_county(county) is None:
                county = ""
            counties = [normalize_county_slug(c) for c in counties if normalize_county_slug(c)]
            counties = [c for c in counties if get_registry_county(c) is not None]
            return {"ok": True, "county": county or None, "counties": counties, "source": "parcels_rtree"}
        except Exception as e:
            logger.warning("resolve_county failed: %s", e)
            return {"ok": True, "county": None, "counties": [], "source": "error"}

    @app.post("/api/resolve_county")
    def resolve_county_alias(payload: dict = Body(...)):
        return resolve_county(payload)

    @app.get("/api/signals/catalog")
    def signals_catalog():
        signals = [
            {"key": "absentee_owner", "label": "Absentee owner", "group": "Ownership", "tier": "strong", "implemented": True},
            {"key": "homestead", "label": "Homestead", "group": "Ownership", "tier": "support", "implemented": True},
            {"key": "permit_demolition", "label": "Permit: demolition", "group": "Permits", "tier": "critical", "implemented": True},
            {"key": "permit_structural", "label": "Permit: structural", "group": "Permits", "tier": "critical", "implemented": True},
            {"key": "permit_roof", "label": "Permit: roof", "group": "Permits", "tier": "strong", "implemented": True},
            {"key": "permit_hvac", "label": "Permit: HVAC", "group": "Permits", "tier": "strong", "implemented": True},
            {"key": "permit_electrical", "label": "Permit: electrical", "group": "Permits", "tier": "strong", "implemented": True},
            {"key": "permit_plumbing", "label": "Permit: plumbing", "group": "Permits", "tier": "strong", "implemented": True},
            {"key": "permit_pool", "label": "Permit: pool", "group": "Permits", "tier": "strong", "implemented": True},
            {"key": "permit_fire", "label": "Permit: fire", "group": "Permits", "tier": "strong", "implemented": True},
            {"key": "permit_sitework", "label": "Permit: sitework", "group": "Permits", "tier": "strong", "implemented": True},
            {"key": "permit_tenant_improvement", "label": "Permit: tenant improvement", "group": "Permits", "tier": "strong", "implemented": True},
            {"key": "permit_remodel", "label": "Permit: remodel", "group": "Permits", "tier": "strong", "implemented": True},
            {"key": "permit_generator", "label": "Permit: generator", "group": "Permits", "tier": "support", "implemented": True},
            {"key": "permit_windows", "label": "Permit: windows", "group": "Permits", "tier": "support", "implemented": True},
            {"key": "permit_doors", "label": "Permit: doors", "group": "Permits", "tier": "support", "implemented": True},
            {"key": "permit_solar", "label": "Permit: solar", "group": "Permits", "tier": "support", "implemented": True},
            {"key": "permit_fence", "label": "Permit: fence", "group": "Permits", "tier": "support", "implemented": True},
            {"key": "permit_sign", "label": "Permit: sign", "group": "Permits", "tier": "support", "implemented": True},
            {"key": "lis_pendens", "label": "Lis pendens", "group": "Official Records", "tier": "critical", "implemented": True},
            {"key": "foreclosure_filing", "label": "Foreclosure: filing", "group": "Official Records", "tier": "critical", "implemented": True},
            {"key": "foreclosure_judgment", "label": "Foreclosure: judgment", "group": "Official Records", "tier": "critical", "implemented": True},
            {"key": "foreclosure", "label": "Foreclosure (generic)", "group": "Official Records", "tier": "critical", "implemented": True},
            {"key": "deed_recorded", "label": "Deed recorded", "group": "Official Records", "tier": "strong", "implemented": True},
            {"key": "deed_warranty", "label": "Deed: warranty", "group": "Official Records", "tier": "strong", "implemented": True},
            {"key": "deed_quitclaim", "label": "Deed: quitclaim", "group": "Official Records", "tier": "strong", "implemented": True},
            {"key": "mortgage_recorded", "label": "Mortgage recorded", "group": "Official Records", "tier": "support", "implemented": True},
            {"key": "mortgage_satisfaction", "label": "Mortgage satisfaction", "group": "Official Records", "tier": "strong", "implemented": True},
            {"key": "mortgage_assignment", "label": "Mortgage assignment", "group": "Official Records", "tier": "support", "implemented": True},
            {"key": "mechanics_lien", "label": "Mechanic's lien", "group": "Liens", "tier": "strong", "implemented": True},
            {"key": "hoa_lien", "label": "HOA lien", "group": "Liens", "tier": "strong", "implemented": True},
            {"key": "irs_tax_lien", "label": "IRS tax lien", "group": "Liens", "tier": "strong", "implemented": True},
            {"key": "state_tax_lien", "label": "State tax lien", "group": "Liens", "tier": "strong", "implemented": True},
            {"key": "code_enforcement_lien", "label": "Code enforcement lien", "group": "Liens", "tier": "critical", "implemented": True},
            {"key": "judgment_lien", "label": "Judgment lien", "group": "Liens", "tier": "strong", "implemented": True},
            {"key": "utility_lien", "label": "Utility lien", "group": "Liens", "tier": "strong", "implemented": True},
            {"key": "lien_recorded", "label": "Lien recorded (generic)", "group": "Liens", "tier": "critical", "implemented": True},
            {"key": "delinquent_tax", "label": "Delinquent tax", "group": "Tax Collector", "tier": "critical", "implemented": True},
            {"key": "tax_certificate_issued", "label": "Tax certificate issued", "group": "Tax Collector", "tier": "strong", "implemented": True},
            {"key": "tax_certificate_redeemed", "label": "Tax certificate redeemed", "group": "Tax Collector", "tier": "strong", "implemented": True},
            {"key": "payment_plan_started", "label": "Payment plan started", "group": "Tax Collector", "tier": "strong", "implemented": True},
            {"key": "payment_plan_defaulted", "label": "Payment plan defaulted", "group": "Tax Collector", "tier": "strong", "implemented": True},
            {"key": "tax_deed_application", "label": "Tax deed application", "group": "Tax Collector", "tier": "critical", "implemented": True},
            {"key": "code_case_opened", "label": "Code case opened", "group": "Code Enforcement", "tier": "strong", "implemented": True},
            {"key": "unsafe_structure", "label": "Unsafe structure", "group": "Code Enforcement", "tier": "critical", "implemented": True},
            {"key": "condemnation", "label": "Condemnation", "group": "Code Enforcement", "tier": "critical", "implemented": True},
            {"key": "demolition_order", "label": "Demolition order", "group": "Code Enforcement", "tier": "critical", "implemented": True},
            {"key": "abatement_order", "label": "Abatement order", "group": "Code Enforcement", "tier": "critical", "implemented": True},
            {"key": "board_hearing_set", "label": "Board hearing set", "group": "Code Enforcement", "tier": "strong", "implemented": True},
            {"key": "fines_imposed", "label": "Fines imposed", "group": "Code Enforcement", "tier": "strong", "implemented": True},
            {"key": "reinspection_failed", "label": "Reinspection failed", "group": "Code Enforcement", "tier": "strong", "implemented": True},
            {"key": "repeat_violation", "label": "Repeat violation", "group": "Code Enforcement", "tier": "strong", "implemented": True},
            {"key": "probate_opened", "label": "Probate opened", "group": "Courts", "tier": "critical", "implemented": True},
            {"key": "divorce_filed", "label": "Divorce filed", "group": "Courts", "tier": "critical", "implemented": True},
            {"key": "eviction_filing", "label": "Eviction filing", "group": "Courts", "tier": "critical", "implemented": True},
        ]
        return {"ok": True, "signals": signals}

    @app.get("/api/debug/parcels_coverage")
    def debug_parcels_coverage(county: str):
        import sqlite3, os
        db_path_abs = _resolve_db_path("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
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

    @app.get("/api/debug/db_stats")
    def debug_db_stats(county: str):
        import sqlite3

        county_key = (county or "").strip().lower() or "seminole"
        db_path_abs = _resolve_db_path("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
        out: dict[str, Any] = {
            "ok": True,
            "county": county_key,
            "parcels_db_ok": _db_exists(db_path_abs),
            "parcels_db_path": db_path_abs,
            "warnings": [],
            "parcels_count_by_county": {},
            "lat_range": None,
            "lng_range": None,
            "suggested_polygon": None,
        }

        if not out["parcels_db_ok"]:
            out["warnings"].append("parcels_db_missing")
            return out

        con = sqlite3.connect(db_path_abs)
        try:
            cur = con.cursor()
            if not _table_exists(con, "parcels"):
                out["warnings"].append("parcels_table_missing")
                return out

            try:
                rows = cur.execute(
                    "SELECT county, count(*) FROM parcels GROUP BY county"
                ).fetchall()
                out["parcels_count_by_county"] = {
                    str(r[0] or "").strip().lower(): int(r[1]) for r in rows if r
                }
            except Exception:
                out["parcels_count_by_county"] = {}

            cols = [r[1] for r in cur.execute("pragma table_info(parcels)").fetchall()]
            colset = {c.lower() for c in cols}
            county_col = None
            parcel_id_col = None
            for cand in ("county", "county_name", "county_cd"):
                if cand in colset:
                    county_col = cand
                    break
            for cand in ("parcel_id", "parcelid", "apn", "folio", "strap"):
                if cand in colset:
                    parcel_id_col = cand
                    break

            pairs = [
                ("lat", "lng"),
                ("latitude", "longitude"),
                ("centroid_lat", "centroid_lng"),
                ("centroid_y", "centroid_x"),
                ("y", "x"),
            ]
            lat_col = None
            lng_col = None
            for a, b in pairs:
                if a in colset and b in colset:
                    lat_col, lng_col = a, b
                    break

            has_bbox = all(k in colset for k in ("minx", "miny", "maxx", "maxy"))
            if not lat_col or not lng_col:
                if not has_bbox:
                    out["warnings"].append("no_spatial_fields")
                    return out

            row = None
            latlng_source = None
            if lat_col and lng_col:
                row = cur.execute(
                    f"select min({lng_col}), min({lat_col}), max({lng_col}), max({lat_col}) "
                    f"from parcels where {county_col or 'county'}=? and {lat_col} is not null and {lng_col} is not null",
                    (county_key,),
                ).fetchone()
                latlng_source = f"{lat_col},{lng_col}"
            elif has_bbox:
                row = cur.execute(
                    f"select min(minx), min(miny), max(maxx), max(maxy) from parcels where {county_col or 'county'}=?",
                    (county_key,),
                ).fetchone()
                latlng_source = "minx/miny/maxx/maxy"

            if not row or row[0] is None or row[1] is None or row[2] is None or row[3] is None:
                out["warnings"].append("no_spatial_rows")
                return out

            min_lng, min_lat, max_lng, max_lat = [float(x) for x in row]
            out["lng_range"] = [min_lng, max_lng]
            out["lat_range"] = [min_lat, max_lat]
            pad = max((max_lng - min_lng) * 0.05, 0.002)
            out["suggested_polygon"] = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [min_lng + pad, min_lat + pad],
                        [max_lng - pad, min_lat + pad],
                        [max_lng - pad, max_lat - pad],
                        [min_lng + pad, max_lat - pad],
                        [min_lng + pad, min_lat + pad],
                    ]
                ],
            }

            out["notes"] = {
                "county_field": county_col,
                "parcel_id_field": parcel_id_col,
                "lat_field": lat_col,
                "lng_field": lng_col,
                "spatial_source": latlng_source,
            }
        finally:
            con.close()

        return out


    @app.get("/api/debug/source_coverage")
    def debug_source_coverage(county: str):
        import sqlite3

        county_key = (county or "").strip().lower() or "seminole"
        parcels_db_path = _resolve_db_path("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
        leads_db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
        out: dict[str, Any] = {
            "ok": True,
            "county": county_key,
            "parcels_db_ok": _db_exists(parcels_db_path),
            "leads_db_ok": _db_exists(leads_db_path),
            "counts": {},
            "fields": {},
            "available_trigger_keys": [],
            "available_signal_keys": [],
            "available_signal_groups": [],
        }

        if out["parcels_db_ok"]:
            try:
                con = sqlite3.connect(parcels_db_path)
                try:
                    cur = con.cursor()
                    if _table_exists(con, "parcels"):
                        out["counts"]["parcels"] = int(
                            cur.execute("SELECT count(*) FROM parcels WHERE county=?", (county_key,)).fetchone()[0]
                        )
                finally:
                    con.close()
            except Exception:
                out["counts"]["parcels"] = 0

        if out["leads_db_ok"]:
            try:
                con = sqlite3.connect(leads_db_path)
                con.row_factory = sqlite3.Row
                try:
                    cur = con.cursor()

                    def _count(table: str, where: str = "", params: tuple[Any, ...] = ()):  # type: ignore
                        try:
                            if not _table_exists(con, table):
                                return 0
                            sql = f"SELECT count(*) FROM {table}"
                            if where:
                                sql += f" WHERE {where}"
                            return int(cur.execute(sql, params).fetchone()[0])
                        except Exception:
                            return 0

                    out["counts"]["parcel_id_map"] = _count("parcel_id_map", "county=?", (county_key,))
                    out["counts"]["pa_properties"] = _count("pa_properties", "county=?", (county_key,))
                    out["counts"]["parcel_trigger_rollups"] = _count("parcel_trigger_rollups", "county=?", (county_key,))
                    out["counts"]["permits"] = _count("permits", "county=?", (county_key,))
                    out["counts"]["official_records"] = _count("official_records", "county=?", (county_key,))
                    out["counts"]["tax_collector_events"] = _count("tax_collector_events", "county=?", (county_key,))
                    out["counts"]["code_enforcement_events"] = _count("code_enforcement_events", "county=?", (county_key,))
                    out["counts"]["trigger_events"] = _count("trigger_events", "county=?", (county_key,))

                    pa_total = out["counts"].get("pa_properties", 0)
                    pt1_total = _count("parcel_table1")

                    def _coverage(count: int, total: int) -> float:
                        if total <= 0:
                            return 0.0
                        return round((float(count) / float(total)) * 100.0, 2)

                    def _field(field_name: str, count: int, total: int, source: str, notes: str = ""):
                        out["fields"][field_name] = {
                            "coverage_pct": _coverage(count, total),
                            "source": source,
                            "sample_size": int(total),
                            "notes": notes,
                        }

                    beds_count = _count("pa_properties", "county=? AND bedrooms > 0", (county_key,))
                    baths_count = _count("pa_properties", "county=? AND bathrooms > 0", (county_key,))
                    zoning_count = _count("pa_properties", "county=? AND zoning != ''", (county_key,))
                    flu_count = _count("pa_properties", "county=? AND future_land_use != ''", (county_key,))
                    ptype_count = _count("pa_properties", "county=? AND use_type != ''", (county_key,))
                    year_count = _count("pa_properties", "county=? AND year_built > 0", (county_key,))
                    last_sale_count = _count("pa_properties", "county=? AND last_sale_date is not null AND last_sale_date != ''", (county_key,))
                    living_count = _count("pa_properties", "county=? AND (living_sf > 0 OR building_sf > 0)", (county_key,))
                    total_value_count = _count("pa_properties", "county=? AND just_value > 0", (county_key,))
                    assessed_count = _count("pa_properties", "county=? AND assessed_value > 0", (county_key,))
                    land_value_count = _count("pa_properties", "county=? AND land_value > 0", (county_key,))
                    building_value_count = _count("pa_properties", "county=? AND improvement_value > 0", (county_key,))

                    pt1_living = _count("parcel_table1", "LIVING_AREA is not null AND LIVING_AREA != ''")
                    pt1_total_sqft = _count("parcel_table1", "TOTAL_SQFT is not null AND TOTAL_SQFT != ''")
                    pt1_year = _count("parcel_table1", "BASE_YR_BLT is not null AND BASE_YR_BLT != ''")
                    pt1_total_value = _count("parcel_table1", "TOTAL_JUST_VALUE is not null AND TOTAL_JUST_VALUE != ''")
                    pt1_assessed = _count("parcel_table1", "TOTAL_ASSESSED_VALUE is not null AND TOTAL_ASSESSED_VALUE != ''")
                    pt1_land = _count("parcel_table1", "APPR_LAND is not null AND APPR_LAND != ''")
                    pt1_building = _count("parcel_table1", "APPR_BLDG is not null AND APPR_BLDG != ''")

                    if living_count > 0:
                        _field("living_area_sqft", living_count, pa_total, "pa_properties")
                    elif pt1_living > 0 or pt1_total_sqft > 0:
                        _field(
                            "living_area_sqft",
                            max(pt1_living, pt1_total_sqft),
                            pt1_total,
                            "parcel_table1",
                            notes="parcel_table1 has no county column; sample_size is table-wide",
                        )
                    else:
                        _field("living_area_sqft", 0, pa_total or pt1_total, "none", notes="no source data")

                    if year_count > 0:
                        _field("year_built", year_count, pa_total, "pa_properties")
                    elif pt1_year > 0:
                        _field("year_built", pt1_year, pt1_total, "parcel_table1", notes="table-wide sample")
                    else:
                        _field("year_built", 0, pa_total or pt1_total, "none", notes="no source data")

                    if total_value_count > 0:
                        _field("total_value", total_value_count, pa_total, "pa_properties")
                    elif pt1_total_value > 0:
                        _field("total_value", pt1_total_value, pt1_total, "parcel_table1", notes="table-wide sample")
                    else:
                        _field("total_value", 0, pa_total or pt1_total, "none", notes="no source data")

                    if assessed_count > 0:
                        _field("assessed_value", assessed_count, pa_total, "pa_properties")
                    elif pt1_assessed > 0:
                        _field("assessed_value", pt1_assessed, pt1_total, "parcel_table1", notes="table-wide sample")
                    else:
                        _field("assessed_value", 0, pa_total or pt1_total, "none", notes="no source data")

                    if land_value_count > 0:
                        _field("land_value", land_value_count, pa_total, "pa_properties")
                    elif pt1_land > 0:
                        _field("land_value", pt1_land, pt1_total, "parcel_table1", notes="table-wide sample")
                    else:
                        _field("land_value", 0, pa_total or pt1_total, "none", notes="no source data")

                    if building_value_count > 0:
                        _field("building_value", building_value_count, pa_total, "pa_properties")
                    elif pt1_building > 0:
                        _field("building_value", pt1_building, pt1_total, "parcel_table1", notes="table-wide sample")
                    else:
                        _field("building_value", 0, pa_total or pt1_total, "none", notes="no source data")

                    _field("beds", beds_count, pa_total, "pa_properties", notes="pa_properties.bedrooms")
                    _field("baths", baths_count, pa_total, "pa_properties", notes="pa_properties.bathrooms")
                    _field("zoning", zoning_count, pa_total, "pa_properties", notes="pa_properties.zoning")
                    _field("future_land_use", flu_count, pa_total, "pa_properties", notes="pa_properties.future_land_use")
                    _field("property_type", ptype_count, pa_total, "pa_properties", notes="pa_properties.use_type")
                    _field("last_sale_date", last_sale_count, pa_total, "pa_properties", notes="pa_properties.last_sale_date")

                    _field("owner_name", 0, pa_total, "pa_properties.record_json", notes="not computed in SQL")
                    _field("address", 0, pa_total, "pa_properties.record_json", notes="not computed in SQL")
                    _field("owner_mailing_address", 0, pa_total, "pa_properties.record_json", notes="not computed in SQL")

                    keys: set[str] = set()
                    if _table_exists(con, "parcel_trigger_rollups"):
                        for row in cur.execute(
                            "SELECT details_json FROM parcel_trigger_rollups WHERE county=? LIMIT 2000",
                            (county_key,),
                        ).fetchall():
                            try:
                                raw = row[0]
                                if not raw:
                                    continue
                                data = json.loads(raw)
                                klist = data.get("trigger_keys") or []
                                if isinstance(klist, (list, tuple)):
                                    for k in klist:
                                        s = str(k or "").strip()
                                        if s:
                                            keys.add(s)
                            except Exception:
                                continue

                    rollup_flags = []
                    if _count("parcel_trigger_rollups", "county=? AND has_permits=1", (county_key,)) > 0:
                        rollup_flags.append("has_permits")
                    if _count("parcel_trigger_rollups", "county=? AND has_tax=1", (county_key,)) > 0:
                        rollup_flags.append("has_tax_events")
                    if _count("parcel_trigger_rollups", "county=? AND has_official_records=1", (county_key,)) > 0:
                        rollup_flags.append("has_official_records")
                    if _count("parcel_trigger_rollups", "county=? AND has_code_enforcement=1", (county_key,)) > 0:
                        rollup_flags.append("has_code_enforcement")
                    if _count("parcel_trigger_rollups", "county=? AND has_courts=1", (county_key,)) > 0:
                        rollup_flags.append("has_courts")
                    if _count("parcel_trigger_rollups", "county=? AND has_gis_planning=1", (county_key,)) > 0:
                        rollup_flags.append("has_gis_planning")

                    derived_signals = []
                    if out["counts"].get("pa_properties", 0) > 0:
                        derived_signals.extend(["absentee_owner", "homestead", "entity_owner"])

                    out["available_trigger_keys"] = sorted(keys)
                    out["available_signal_keys"] = sorted(set(keys) | set(rollup_flags) | set(derived_signals))
                    groups_raw = [
                        "permits" if "has_permits" in rollup_flags else "",
                        "tax" if "has_tax_events" in rollup_flags else "",
                        "official_records" if "has_official_records" in rollup_flags else "",
                        "code_enforcement" if "has_code_enforcement" in rollup_flags else "",
                        "courts" if "has_courts" in rollup_flags else "",
                        "gis" if "has_gis_planning" in rollup_flags else "",
                        "ownership" if derived_signals else "",
                    ]
                    out["available_signal_groups"] = sorted({g for g in groups_raw if g})
                finally:
                    con.close()
            except Exception:
                pass

        return out

    def _select_provider_entries(
        *,
        county_key: str,
        providers: list[str] | None,
        default_categories: list[str],
    ) -> list[ProviderCatalogEntryOut]:
        from florida_property_scraper.providers.catalog import get_provider_catalog

        entries = get_provider_catalog(county_key)
        by_id = {e.provider_id: e for e in entries}
        by_cat: dict[str, list] = {}
        for e in entries:
            by_cat.setdefault(e.category, []).append(e)

        selected: list = []
        if providers:
            for p in providers:
                key = str(p or "").strip().lower()
                if not key:
                    continue
                if key in by_id:
                    selected.append(by_id[key])
                    continue
                if key in by_cat:
                    selected.extend(by_cat[key])
        else:
            for cat in default_categories:
                selected.extend(by_cat.get(cat, []))

        deduped: dict[str, ProviderCatalogEntryOut] = {}
        for e in selected:
            try:
                deduped[e.provider_id] = ProviderCatalogEntryOut(
                    provider_id=e.provider_id,
                    category=e.category,
                    method=e.method,
                    supports_counties=list(e.supports_counties or []),
                    target_ids=list(e.target_ids or []),
                    base_url=e.base_url,
                    supported_fields=list(e.supported_fields),
                    rate_limit=e.rate_limit,
                    status=e.status,
                    notes=e.notes,
                )
            except Exception:
                continue
        return list(deduped.values())

    def _fetch_evidence_for_provider(
        *,
        store: Any,
        county_key: str,
        parcel_ids: list[str],
        provider: ProviderCatalogEntryOut,
        now_iso: str,
        logger: logging.Logger,
    ) -> list[dict[str, Any]]:
        if not parcel_ids:
            return []

        placeholders = ",".join(["?"] * len(parcel_ids))
        params = [county_key, *parcel_ids]

        evidence: list[dict[str, Any]] = []
        category = str(provider.category or "").strip().lower()
        source = ""
        sql = ""

        if category == "permits":
            source = "permit"
            sql = f"SELECT * FROM permits WHERE county=? AND parcel_id IN ({placeholders})"
        elif category == "official_records":
            source = "official_record"
            sql = f"SELECT * FROM official_records WHERE county=? AND parcel_id IN ({placeholders})"
        elif category == "tax":
            source = "tax"
            sql = f"SELECT * FROM tax_collector_events WHERE county=? AND parcel_id IN ({placeholders})"
        elif category == "code_enforcement":
            source = "code_enforcement"
            sql = f"SELECT * FROM code_enforcement_events WHERE county=? AND parcel_id IN ({placeholders})"
        else:
            return []

        rows = store.conn.execute(sql, params).fetchall()
        for row in rows:
            rec = dict(row)
            parcel_id = str(rec.get("parcel_id") or "").strip()
            if not parcel_id:
                continue

            fields: dict[str, Any] = {}
            if category == "permits":
                fields = _compact_fields(
                    {
                        "permit_number": rec.get("permit_number"),
                        "permit_type": rec.get("permit_type"),
                        "status": rec.get("status"),
                        "issue_date": rec.get("issue_date"),
                        "final_date": rec.get("final_date"),
                        "description": rec.get("description"),
                        "address": rec.get("address"),
                    }
                )
            elif category == "official_records":
                fields = _compact_fields(
                    {
                        "doc_type": rec.get("doc_type"),
                        "rec_date": rec.get("rec_date"),
                        "parties": rec.get("parties"),
                        "book_page_or_instrument": rec.get("book_page_or_instrument"),
                        "consideration": rec.get("consideration"),
                        "owner_name": rec.get("owner_name"),
                        "address": rec.get("address"),
                    }
                )
            elif category == "tax":
                fields = _compact_fields(
                    {
                        "event_type": rec.get("event_type"),
                        "event_date": rec.get("event_date"),
                        "amount_due": rec.get("amount_due"),
                        "status": rec.get("status"),
                        "description": rec.get("description"),
                    }
                )
            elif category == "code_enforcement":
                fields = _compact_fields(
                    {
                        "event_type": rec.get("event_type"),
                        "event_date": rec.get("event_date"),
                        "case_number": rec.get("case_number"),
                        "status": rec.get("status"),
                        "description": rec.get("description"),
                        "fine_amount": rec.get("fine_amount"),
                        "lien_amount": rec.get("lien_amount"),
                    }
                )

            source_url = _maybe_url(rec.get("source"))
            evidence_hash = _evidence_hash(
                county=county_key,
                parcel_id=parcel_id,
                provider_id=provider.provider_id,
                source=source,
                source_url=source_url,
                fields=fields,
            )

            evidence_id = store.upsert_enrichment_evidence(
                county=county_key,
                parcel_id=parcel_id,
                provider_id=provider.provider_id,
                source=source,
                source_url=source_url,
                retrieved_at=now_iso,
                fields=fields,
                raw=rec,
                evidence_hash=evidence_hash,
            )

            if evidence_id is None:
                logger.warning("evidence insert failed: %s %s", provider.provider_id, parcel_id)
                continue

            evidence.append(
                {
                    "id": evidence_id,
                    "parcel_id": parcel_id,
                    "provider_id": provider.provider_id,
                    "source": source,
                    "source_url": source_url,
                    "retrieved_at": now_iso,
                    "fields": fields,
                }
            )
        return evidence

    def _evaluate_triggers_from_evidence(
        *,
        county_key: str,
        evidence_rows: list[dict[str, Any]],
        now_iso: str,
    ) -> list[dict[str, Any]]:
        by_key: dict[tuple[str, str], dict[str, Any]] = {}

        def _add_trigger(
            *,
            parcel_id: str,
            trigger_id: str,
            reason: str,
            evidence_id: int,
        ) -> None:
            k = (parcel_id, trigger_id)
            entry = by_key.get(k)
            if entry is None:
                entry = {
                    "parcel_id": parcel_id,
                    "trigger_id": trigger_id,
                    "reason": reason,
                    "evidence_ids": set(),
                    "evaluated_at": now_iso,
                }
                by_key[k] = entry
            try:
                entry["evidence_ids"].add(int(evidence_id))
            except Exception:
                pass

        for ev in evidence_rows:
            parcel_id = str(ev.get("parcel_id") or "").strip()
            if not parcel_id:
                continue
            ev_id = int(ev.get("id") or 0)
            if ev_id <= 0:
                continue

            source = str(ev.get("source") or "").strip().lower()
            fields = ev.get("fields") or {}

            if source == "permit":
                _add_trigger(
                    parcel_id=parcel_id,
                    trigger_id="permit_filed",
                    reason=f"permit_number={fields.get('permit_number') or ''}".strip(),
                    evidence_id=ev_id,
                )
                ptype = str(fields.get("permit_type") or "").strip().lower()
                if "hvac" in ptype:
                    _add_trigger(
                        parcel_id=parcel_id,
                        trigger_id="permit_hvac",
                        reason=f"permit_type={fields.get('permit_type')}",
                        evidence_id=ev_id,
                    )
                if "roof" in ptype:
                    _add_trigger(
                        parcel_id=parcel_id,
                        trigger_id="permit_roof",
                        reason=f"permit_type={fields.get('permit_type')}",
                        evidence_id=ev_id,
                    )
                if "electrical" in ptype or "electric" in ptype:
                    _add_trigger(
                        parcel_id=parcel_id,
                        trigger_id="permit_electrical",
                        reason=f"permit_type={fields.get('permit_type')}",
                        evidence_id=ev_id,
                    )

            if source == "official_record":
                doc_type = str(fields.get("doc_type") or "").strip().lower()
                if "deed" in doc_type or "warranty" in doc_type or "quitclaim" in doc_type:
                    _add_trigger(
                        parcel_id=parcel_id,
                        trigger_id="ownership_change",
                        reason=f"doc_type={fields.get('doc_type')}",
                        evidence_id=ev_id,
                    )

            if source == "tax":
                event_type = str(fields.get("event_type") or "").strip().lower()
                status = str(fields.get("status") or "").strip().lower()
                if "delinquent" in event_type or "delinquent" in status:
                    _add_trigger(
                        parcel_id=parcel_id,
                        trigger_id="tax_delinquent",
                        reason=f"event_type={fields.get('event_type')}",
                        evidence_id=ev_id,
                    )

            if source == "code_enforcement":
                event_type = str(fields.get("event_type") or "").strip().lower()
                if "case_opened" in event_type or "case opened" in event_type or "code_case_opened" in event_type:
                    _add_trigger(
                        parcel_id=parcel_id,
                        trigger_id="code_case_opened",
                        reason=f"event_type={fields.get('event_type')}",
                        evidence_id=ev_id,
                    )

        out: list[dict[str, Any]] = []
        for entry in by_key.values():
            ev_ids = sorted(int(x) for x in (entry.get("evidence_ids") or []) if int(x) > 0)
            out.append(
                {
                    "parcel_id": entry.get("parcel_id"),
                    "trigger_id": entry.get("trigger_id"),
                    "reason": entry.get("reason") or "",
                    "evidence_ids": ev_ids,
                    "evaluated_at": entry.get("evaluated_at") or now_iso,
                }
            )
        return out

    @app.get("/api/providers/catalog", response_model=ProviderCatalogResponse)
    def providers_catalog(county: str = "seminole") -> ProviderCatalogResponse:
        try:
            county_key = (county or "").strip().lower() or "seminole"
            providers = _select_provider_entries(
                county_key=county_key,
                providers=None,
                default_categories=[
                    "permits",
                    "official_records",
                    "tax",
                    "code_enforcement",
                    "courts",
                    "liens",
                    "utilities",
                    "manual",
                ],
            )
            implemented = {p.key for p in get_property_providers(county=county_key) if p.implemented}
            out: list[ProviderCatalogEntryOut] = []
            for p in providers:
                status = "implemented" if p.provider_id in implemented else "not_implemented"
                out.append(
                    ProviderCatalogEntryOut(
                        provider_id=p.provider_id,
                        category=p.category,
                        method=p.method,
                        supports_counties=p.supports_counties,
                        target_ids=p.target_ids,
                        base_url=p.base_url,
                        supported_fields=p.supported_fields,
                        rate_limit=p.rate_limit,
                        status=status,
                        notes=p.notes,
                    )
                )
            return ProviderCatalogResponse(ok=True, county=county_key, providers=out)
        except Exception as exc:
            return _safe_error_response(error="providers_catalog_failed", detail=str(exc), status_code=500)

    @app.get("/api/providers/status", response_model=ProviderStatusResponse)
    def providers_status(county: str = "seminole") -> ProviderStatusResponse:
        from florida_property_scraper.storage import SQLiteStore

        try:
            county_key = (county or "").strip().lower() or "seminole"
            providers = _select_provider_entries(
                county_key=county_key,
                providers=None,
                default_categories=[
                    "permits",
                    "official_records",
                    "tax",
                    "code_enforcement",
                    "courts",
                    "liens",
                    "utilities",
                ],
            )

            db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
            store = SQLiteStore(db_path)
            try:
                out: list[ProviderStatusEntry] = []
                for p in providers:
                    rows = store.list_provider_fetch_log(county=county_key, provider_key=p.provider_id, limit=1)
                    last = rows[0] if rows else None
                    status = "unknown"
                    last_status = None
                    last_error = None
                    last_run_at = None
                    if isinstance(last, dict):
                        last_run_at = last.get("fetched_at")
                        ok_flag = int(last.get("ok") or 0)
                        last_status = "ok" if ok_flag == 1 else "error"
                        last_error = last.get("error")
                        status = last_status
                    out.append(
                        ProviderStatusEntry(
                            provider_id=p.provider_id,
                            category=p.category,
                            base_url=p.base_url,
                            status=status,
                            last_run_at=last_run_at,
                            last_status=last_status,
                            last_error=last_error,
                        )
                    )
            finally:
                store.close()
            return ProviderStatusResponse(ok=True, county=county_key, providers=out)
        except Exception as exc:
            return _safe_error_response(error="providers_status_failed", detail=str(exc), status_code=500)

    @app.post("/api/providers/manual_ingest", response_model=ManualIngestResponse)
    def manual_ingest(payload: ManualIngestRequest) -> ManualIngestResponse:
        from florida_property_scraper.storage import SQLiteStore
        from florida_property_scraper.enrichment.providers.model import (
            compute_content_hash,
            confidence_score_from_label,
            normalize_confidence_label,
        )

        try:
            county_key = (payload.county or "").strip().lower()
            parcel_id = str(payload.parcel_id or "").strip()
            if not county_key or not parcel_id:
                return _safe_error_response(error="bad_request", detail="county and parcel_id are required", status_code=400)

            provider_key = str(payload.provider_key or "manual_ingest").strip().lower()
            if not provider_key:
                provider_key = "manual_ingest"

            evidence_items = payload.evidence or []
            if not evidence_items:
                return ManualIngestResponse(
                    ok=True,
                    provider_key=provider_key,
                    parcel_id=parcel_id,
                    evidence_ids=[],
                    warnings=["no_evidence"],
                    errors=[],
                )

            now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            fetched_at = str(payload.fetched_at or "").strip() or now_iso

            db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
            store = SQLiteStore(db_path)
            evidence_ids: list[int] = []
            warnings: list[str] = []
            errors: list[str] = []

            try:
                for ev in evidence_items:
                    field = str(ev.field or "").strip()
                    source_url = str(ev.source_url or "").strip()
                    source_type = str(ev.source_type or "").strip().lower()
                    if not field or not source_url or not source_type:
                        warnings.append("skipped_evidence_missing_required_fields")
                        continue

                    confidence_label = normalize_confidence_label(ev.confidence_label)
                    confidence_score = confidence_score_from_label(confidence_label)
                    content_hash = str(ev.content_hash or "").strip() or compute_content_hash(
                        field=field,
                        value=ev.value,
                        source_url=source_url,
                    )

                    ev_id = store.upsert_provider_evidence(
                        provider_key=provider_key,
                        provider_name="Manual Ingest",
                        county=county_key,
                        parcel_id=parcel_id,
                        field=field,
                        value=ev.value,
                        confidence=confidence_score,
                        confidence_label=confidence_label,
                        source_type=source_type,
                        source_url=source_url,
                        fetched_at=fetched_at,
                        retrieved_at=str(ev.fetched_at or "").strip() or fetched_at,
                        content_hash=content_hash,
                        extract_method=str(ev.extract_method or "").strip() or "manual_ingest",
                        raw_reference=str(ev.raw_reference or "").strip() or None,
                        raw_snippet=str(ev.raw_snippet or "").strip() or None,
                        raw_id=None,
                    )
                    if ev_id:
                        evidence_ids.append(ev_id)
            finally:
                store.close()

            if evidence_ids:
                store = SQLiteStore(db_path)
                try:
                    store.log_provider_fetch(
                        provider_key=provider_key,
                        county=county_key,
                        parcel_id=parcel_id,
                        request_fingerprint=compute_request_fingerprint(
                            provider_key=provider_key,
                            county=county_key,
                            parcel_id=parcel_id,
                            url=f"manual_ingest://{parcel_id}",
                        ),
                        url=f"manual_ingest://{parcel_id}",
                        http_status=200,
                        fetched_at=fetched_at,
                        duration_ms=0,
                        ok=True,
                        error=None,
                    )
                finally:
                    store.close()

            return ManualIngestResponse(
                ok=True,
                provider_key=provider_key,
                parcel_id=parcel_id,
                evidence_ids=evidence_ids,
                warnings=warnings,
                errors=errors,
            )
        except Exception as exc:
            return _safe_error_response(error="manual_ingest_failed", detail=str(exc), status_code=500)

    @app.get("/api/debug/provider_evidence")
    def debug_provider_evidence(county: str = "seminole", parcel_id: str = "", limit: int = 50):
        from florida_property_scraper.storage import SQLiteStore

        county_key = (county or "").strip().lower()
        pid = str(parcel_id or "").strip()
        if not county_key or not pid:
            return _bad_request_response("county and parcel_id are required")

        db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
        store = SQLiteStore(db_path)
        try:
            rows = store.list_provider_evidence_for_parcels(county=county_key, parcel_ids=[pid])
        finally:
            store.close()

        lim = max(1, min(int(limit or 50), 200))
        trimmed = []
        for row in rows[:lim]:
            trimmed.append(
                {
                    "id": row.get("id"),
                    "provider_key": row.get("provider_key"),
                    "provider_name": row.get("provider_name"),
                    "field": row.get("field"),
                    "value": row.get("value"),
                    "confidence": row.get("confidence"),
                    "confidence_label": row.get("confidence_label"),
                    "fetched_at": row.get("fetched_at"),
                    "retrieved_at": row.get("retrieved_at"),
                    "source_url": row.get("source_url"),
                    "source_type": row.get("source_type"),
                    "raw_reference": row.get("raw_reference"),
                    "raw_snippet": row.get("raw_snippet"),
                }
            )

        return {
            "ok": True,
            "county": county_key,
            "parcel_id": pid,
            "count": len(rows),
            "evidence": trimmed,
        }

    @app.post("/api/enrich", response_model=EnrichResponse)
    def enrich(payload: EnrichRequest) -> EnrichResponse:
        from florida_property_scraper.storage import SQLiteStore
        from florida_property_scraper.enrichment.providers.model import EvidenceItem

        try:
            logger = logging.getLogger("fps.enrich")
            county_key = (payload.county or "").strip().lower()
            if not county_key:
                return _safe_error_response(error="bad_request", detail="county is required", status_code=400)

            parcel_ids = [str(p or "").strip() for p in (payload.parcel_ids or [])]
            parcel_ids = [p for p in parcel_ids if p]
            if not parcel_ids:
                return EnrichResponse(
                    ok=True,
                    mode="evidence_only",
                    dry_run=bool(payload.dry_run),
                    enriched=[],
                    evidence=[],
                    merged_fields={},
                    provider_results=[],
                )

            if len(parcel_ids) > 500:
                logger.warning("enrich: parcel_ids capped (got %s)", len(parcel_ids))
                parcel_ids = parcel_ids[:500]

            provider_keys = payload.provider_keys or payload.providers or []
            provider_keys = [str(p or "").strip().lower() for p in provider_keys if str(p or "").strip()]

            providers: list[tuple[str, Any]] = []
            if provider_keys:
                for key in provider_keys:
                    provider = get_property_provider(provider_key=key)
                    providers.append((key, provider))
            else:
                providers = [(p.key, p) for p in get_property_providers(county=county_key)]

            dry_run = bool(payload.dry_run)
            fixture_mode = bool(payload.fixture_mode) if payload.fixture_mode is not None else _fixture_mode_default()

            now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
            store = SQLiteStore(db_path)

            provider_results: list[ProviderResultOut] = []
            evidence_out: list[EvidenceOut] = []

            try:
                for pid in parcel_ids:
                    for provider_key, provider in providers:
                        if provider is None:
                            provider_results.append(
                                ProviderResultOut(
                                    ok=True,
                                    provider_key=provider_key,
                                    county=county_key,
                                    parcel_id=pid,
                                    status="not_supported",
                                    fetched_at=now_iso,
                                    evidence_ids=[],
                                    raw_artifacts=[],
                                    warnings=[],
                                    errors=[],
                                )
                            )
                            continue

                        start_ts = time.perf_counter()
                        result = provider.fetch(
                            county=county_key,
                            parcel_id=pid,
                            dry_run=dry_run,
                            fixture_mode=fixture_mode,
                            store=store,
                        )
                        elapsed_ms = int((time.perf_counter() - start_ts) * 1000)

                        evidence_ids: list[int] = []
                        local_warnings: list[str] = []

                        for ev in result.evidence:
                            if not isinstance(ev, EvidenceItem):
                                continue
                            if not ev.field or not ev.content_hash:
                                local_warnings.append("invalid_evidence_missing_field_or_hash")
                                continue
                            if str(ev.provider_id or "").strip().lower() != str(result.provider_key or "").strip().lower():
                                local_warnings.append("invalid_evidence_provider_id_mismatch")
                                continue
                            if str(ev.county or "").strip().lower() != county_key:
                                local_warnings.append("invalid_evidence_county_mismatch")
                                continue
                            if str(ev.parcel_id or "").strip() != pid:
                                local_warnings.append("invalid_evidence_parcel_mismatch")
                                continue
                            if not str(ev.source.source_type or "").strip():
                                local_warnings.append("invalid_evidence_missing_source_type")
                                continue
                            if not str(ev.confidence_label or "").strip():
                                local_warnings.append("invalid_evidence_missing_confidence")
                                continue
                            provider_name = str(ev.provider_name or "").strip() or str(result.provider_key or "").strip()
                            ev_id = store.upsert_provider_evidence(
                                provider_key=result.provider_key,
                                provider_name=provider_name,
                                county=county_key,
                                parcel_id=pid,
                                field=ev.field,
                                value=ev.value,
                                confidence=ev.confidence_score,
                                confidence_label=ev.confidence_label,
                                source_type=ev.source.source_type,
                                source_url=ev.source.url,
                                fetched_at=ev.fetched_at,
                                retrieved_at=ev.retrieved_at,
                                content_hash=ev.content_hash,
                                extract_method=ev.extract_method,
                                raw_reference=ev.raw_reference,
                                raw_snippet=ev.raw_snippet,
                                raw_id=ev.raw_ref,
                            )
                            if ev_id:
                                evidence_ids.append(ev_id)
                                evidence_out.append(
                                    EvidenceOut(
                                        provider_key=result.provider_key,
                                        provider_id=ev.provider_id,
                                        provider_name=provider_name,
                                        county=county_key,
                                        parcel_id=pid,
                                        field=ev.field,
                                        value=ev.value,
                                        confidence=ev.confidence_score,
                                        confidence_label=ev.confidence_label,
                                        source=EvidenceSourceOut(
                                            source_type=ev.source.source_type,
                                            url=ev.source.url,
                                            label=ev.source.label,
                                        ),
                                        fetched_at=ev.fetched_at,
                                        retrieved_at=ev.retrieved_at,
                                        content_hash=ev.content_hash,
                                        extract_method=ev.extract_method,
                                        raw_reference=ev.raw_reference,
                                        raw_ref=ev.raw_ref,
                                    )
                                )

                        provider_results.append(
                            ProviderResultOut(
                                ok=result.ok,
                                provider_key=result.provider_key,
                                county=county_key,
                                parcel_id=pid,
                                status=result.status,
                                fetched_at=result.fetched_at,
                                evidence_ids=evidence_ids,
                                raw_artifacts=[
                                    ArtifactRefOut(
                                        raw_id=a.raw_id,
                                        url=a.url,
                                        content_type=a.content_type,
                                        sha256=a.sha256,
                                        body_bytes=a.body_bytes,
                                        storage_path=a.storage_path,
                                    )
                                    for a in (result.raw_artifacts or [])
                                ],
                                warnings=list(result.warnings or []) + local_warnings,
                                errors=list(result.errors or []),
                            )
                        )

                        store.log_provider_fetch(
                            provider_key=result.provider_key,
                            county=county_key,
                            parcel_id=pid,
                            request_fingerprint=compute_request_fingerprint(
                                provider_key=result.provider_key,
                                county=county_key,
                                parcel_id=pid,
                                url=result.raw_artifacts[0].url if result.raw_artifacts else "fixture://" + result.provider_key,
                            ),
                            url=result.raw_artifacts[0].url if result.raw_artifacts else "fixture://" + result.provider_key,
                            http_status=200 if result.ok else None,
                            fetched_at=result.fetched_at,
                            duration_ms=elapsed_ms,
                            ok=result.ok,
                            error="; ".join(result.errors) if result.errors else None,
                        )
            finally:
                store.close()

            store = SQLiteStore(db_path)
            merged_fields: dict[str, dict[str, Any]] = {}
            try:
                min_conf_label = str(os.getenv("FPS_EVIDENCE_MIN_CONFIDENCE", "low")).strip().lower() or "low"
                rows = store.list_provider_evidence_for_parcels(county=county_key, parcel_ids=parcel_ids)
                by_parcel: dict[str, list[dict[str, Any]]] = {}
                for row in rows:
                    pid = str(row.get("parcel_id") or "").strip()
                    if not pid:
                        continue
                    by_parcel.setdefault(pid, []).append(row)
                for pid in parcel_ids:
                    merged, evidence_ids = store.build_enriched_fields(
                        evidence_rows=by_parcel.get(pid, []),
                        min_confidence_label=min_conf_label,
                    )
                    merged_fields[pid] = merged
                    store.save_parcel_enrichment_snapshot(
                        county=county_key,
                        parcel_id=pid,
                        snapshot_at=now_iso,
                        merged_fields=merged,
                        evidence_ids=evidence_ids,
                    )
            finally:
                store.close()

            enriched_ids = sorted([pid for pid, fields in merged_fields.items() if fields])

            return EnrichResponse(
                ok=True,
                mode="evidence_only",
                dry_run=dry_run,
                enriched=enriched_ids,
                evidence=evidence_out,
                merged_fields=merged_fields,
                provider_results=provider_results,
            )
        except Exception as exc:
            return _safe_error_response(error="enrich_failed", detail=str(exc), status_code=500)

    @app.post("/api/triggers/evaluate", response_model=TriggerEvaluateResponse)
    def triggers_evaluate(payload: TriggerEvaluateRequest) -> TriggerEvaluateResponse:
        from florida_property_scraper.storage import SQLiteStore

        try:
            logger = logging.getLogger("fps.triggers")
            county_key = (payload.county or "").strip().lower()
            if not county_key:
                return _safe_error_response(error="bad_request", detail="county is required", status_code=400)

            parcel_ids = [str(p or "").strip() for p in (payload.parcel_ids or [])]
            parcel_ids = [p for p in parcel_ids if p]
            if not parcel_ids:
                return TriggerEvaluateResponse(ok=True, county=county_key, results=[])

            if len(parcel_ids) > 500:
                logger.warning("triggers evaluate: parcel_ids capped (got %s)", len(parcel_ids))
                parcel_ids = parcel_ids[:500]

            now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
            store = SQLiteStore(db_path)
            try:
                evidence_rows = store.list_provider_evidence_for_parcels(
                    county=county_key,
                    parcel_ids=parcel_ids,
                )
            finally:
                store.close()

            trigger_rows = evaluate_triggers_from_evidence(
                county=county_key,
                evidence_rows=evidence_rows,
                parcel_ids=parcel_ids,
                now_iso=now_iso,
            )

            results: list[TriggerEvaluateItem] = []
            to_persist: list[dict[str, Any]] = []
            for row in trigger_rows:
                evidence_ids = [int(x) for x in (row.evidence_ids or []) if int(x) > 0]
                if row.fired:
                    result_hash = _trigger_result_hash(
                        county=county_key,
                        parcel_id=str(row.parcel_id or ""),
                        trigger_id=str(row.trigger_key or ""),
                        reason=str(row.reason or ""),
                        evidence_ids=evidence_ids,
                    )
                    to_persist.append(
                        {
                            "parcel_id": row.parcel_id,
                            "trigger_id": row.trigger_key,
                            "reason": row.reason,
                            "evidence_ids": evidence_ids,
                            "evaluated_at": row.evaluated_at,
                            "result_hash": result_hash,
                        }
                    )
                results.append(
                    TriggerEvaluateItem(
                        trigger_key=row.trigger_key,
                        trigger_id=row.trigger_key,
                        parcel_id=row.parcel_id,
                        fired=row.fired,
                        severity=row.severity,
                        reason=row.reason,
                        evidence_ids=evidence_ids,
                        fields_used=row.fields_used,
                        evaluated_at=row.evaluated_at,
                    )
                )

            if results:
                store = SQLiteStore(_resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB))
                try:
                    run_id = f"trigger_eval:{county_key}:{uuid.uuid4().hex[:8]}"
                    store.upsert_trigger_results(run_id=run_id, county=county_key, results=to_persist)
                finally:
                    store.close()

            for r in results:
                if r.fired:
                    logger.info("trigger fired: %s %s", r.parcel_id, r.trigger_key)

            return TriggerEvaluateResponse(ok=True, county=county_key, results=results)
        except Exception as exc:
            return _safe_error_response(error="trigger_eval_failed", detail=str(exc), status_code=500)

    def _provider_status_payload(county_key: str) -> dict[str, Any]:
        from florida_property_scraper.providers.catalog import get_provider_catalog, get_targets

        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        targets = get_targets(county_key)
        providers_map: dict[str, list[object]] = {}
        for target in targets:
            providers_map.setdefault(target.kind, []).append(target)

        parcels_db_path = _resolve_db_path("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
        leads_db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
        parcels_db_ok = _db_exists(parcels_db_path)
        leads_db_ok = _db_exists(leads_db_path)

        parcels_ok = False
        appraiser_ok = False
        try:
            if parcels_db_ok:
                import sqlite3 as _sqlite3

                con = _sqlite3.connect(parcels_db_path)
                parcels_ok = _table_exists(con, "parcels")
                con.close()
        except Exception:
            parcels_ok = False

        try:
            if leads_db_ok:
                import sqlite3 as _sqlite3

                con = _sqlite3.connect(leads_db_path)
                appraiser_ok = _table_exists(con, "pa_properties")
                con.close()
        except Exception:
            appraiser_ok = False

        def _provider_entry(p: object) -> dict[str, Any]:
            try:
                return {
                    "kind": str(getattr(p, "kind")),
                    "base_url": str(getattr(p, "base_url")),
                    "notes": getattr(p, "notes", None),
                }
            except Exception:
                return {}

        def _status_from_catalog(providers: list, *, default_reason: str) -> dict[str, Any]:
            if not providers:
                return {
                    "status": "DOWN",
                    "reason": "no_provider_catalog",
                    "last_checked": now,
                    "providers": [],
                    "mode": "planned",
                }
            return {
                "status": "DEGRADED",
                "reason": default_reason,
                "last_checked": now,
                "providers": [_provider_entry(p) for p in providers],
                "mode": "planned",
            }

        catalog_entries = list(get_provider_catalog(county_key) or [])

        def _status_from_category(category: str, fallback_targets: list | None = None) -> dict[str, Any]:
            entries = [e for e in catalog_entries if str(getattr(e, "category", "") or "").strip().lower() == category]
            provider_entries = [
                {
                    "provider_id": str(getattr(e, "provider_id", "") or ""),
                    "method": str(getattr(e, "method", "") or ""),
                    "status": str(getattr(e, "status", "") or ""),
                    "base_url": str(getattr(e, "base_url", "") or ""),
                    "notes": getattr(e, "notes", None),
                }
                for e in entries
            ]

            if not entries:
                if fallback_targets:
                    return {
                        "status": "DEGRADED",
                        "reason": "target_only",
                        "last_checked": now,
                        "providers": [_provider_entry(p) for p in fallback_targets],
                        "mode": "planned",
                    }
                return {
                    "status": "DOWN",
                    "reason": "no_provider_catalog",
                    "last_checked": now,
                    "providers": [],
                    "mode": "planned",
                }

            statuses = {str(getattr(e, "status", "") or "").strip().lower() for e in entries}
            if "implemented" in statuses:
                return {
                    "status": "OK",
                    "reason": "implemented",
                    "last_checked": now,
                    "providers": provider_entries,
                    "mode": "live",
                }
            if "planned" in statuses or "not_implemented" in statuses:
                return {
                    "status": "DEGRADED",
                    "reason": "planned_only",
                    "last_checked": now,
                    "providers": provider_entries,
                    "mode": "planned",
                }

            return {
                "status": "DEGRADED",
                "reason": "catalog_only",
                "last_checked": now,
                "providers": provider_entries,
                "mode": "planned",
            }

        categories: dict[str, Any] = {
            "parcels": {
                "status": "OK" if parcels_ok else "DOWN",
                "reason": "parcels_sqlite" if parcels_ok else "parcels_db_missing",
                "last_checked": now,
                "providers": [],
                "mode": "live" if parcels_ok else "planned",
            },
            "appraiser": {
                "status": "OK" if appraiser_ok else "DOWN",
                "reason": "pa_properties" if appraiser_ok else "pa_db_missing",
                "last_checked": now,
                "providers": [_provider_entry(p) for p in providers_map.get("pa", [])],
                "mode": "live" if appraiser_ok else "planned",
            },
            "tax": _status_from_catalog(providers_map.get("tax", []), default_reason="catalog_only"),
            "permits": _status_from_catalog(providers_map.get("permits", []), default_reason="catalog_only"),
            "courts": _status_from_catalog(providers_map.get("clerk", []), default_reason="catalog_only"),
            "deeds": _status_from_category("deeds", providers_map.get("clerk", [])),
            "official_records": _status_from_category("official_records", providers_map.get("clerk", [])),
            "code_enforcement": _status_from_category("code_enforcement"),
            "utilities": _status_from_category("utilities"),
            "liens": _status_from_category("liens"),
            "photos": _status_from_category("photos", providers_map.get("pa", [])),
        }

        reference_sources: list[dict[str, Any]] = []
        seen_refs: set[tuple[str, str, str]] = set()

        for p in targets:
            kind = str(getattr(p, "kind", "") or "").strip().lower()
            base_url = str(getattr(p, "base_url", "") or "").strip()
            if not kind and not base_url:
                continue
            key = (kind, base_url, "target")
            if key in seen_refs:
                continue
            seen_refs.add(key)
            reference_sources.append(
                {
                    "source_type": "target",
                    "category": kind,
                    "provider_id": f"{county_key}:{kind}",
                    "url": base_url,
                    "notes": getattr(p, "notes", None),
                }
            )

        for entry in get_provider_catalog(county_key):
            base_url = str(getattr(entry, "base_url", "") or "").strip()
            provider_id = str(getattr(entry, "provider_id", "") or "").strip()
            category = str(getattr(entry, "category", "") or "").strip().lower()
            key = (provider_id, base_url, "catalog")
            if key in seen_refs:
                continue
            seen_refs.add(key)
            reference_sources.append(
                {
                    "source_type": "provider_catalog",
                    "category": category,
                    "provider_id": provider_id,
                    "status": str(getattr(entry, "status", "") or ""),
                    "url": base_url,
                    "notes": getattr(entry, "notes", None),
                    "supported_fields": list(getattr(entry, "supported_fields", []) or []),
                }
            )

        return {
            "ok": True,
            "county": county_key,
            "checked_at": now,
            "categories": categories,
            "providers": [_provider_entry(p) for p in targets],
            "reference_sources": reference_sources,
        }

    @app.get("/api/debug/provider_status")
    def debug_provider_status(county: str = "seminole"):
        county_key = (county or "").strip().lower() or "seminole"
        return _provider_status_payload(county_key)

    @app.get("/api/debug/runtime_audit")
    def debug_runtime_audit(county: str = "seminole"):
        import sqlite3

        county_key = (county or "").strip().lower() or "seminole"
        now_iso = _runtime_now_iso()

        runtime_watch = dict(_RUNTIME_AUDIT_STATE.get("watchlists") or {})
        runtime_refresh = dict(_RUNTIME_AUDIT_STATE.get("statewide_refresh") or {})

        watch_age_s = _runtime_age_seconds(runtime_watch.get("last_completed_at"))
        refresh_age_s = _runtime_age_seconds(runtime_refresh.get("last_completed_at"))

        watch_interval = int(runtime_watch.get("interval_s") or 3600)
        refresh_interval = int(runtime_refresh.get("interval_s") or 3600)
        watch_fresh_s = max(3600, int(watch_interval * 1.5))
        refresh_fresh_s = max(3600, int(refresh_interval * 1.5))

        watch_fresh = (watch_age_s is not None) and (watch_age_s <= watch_fresh_s)
        refresh_fresh = (refresh_age_s is not None) and (refresh_age_s <= refresh_fresh_s)

        evidence_db = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
        preload_stats: dict[str, Any] = {
            "db_path": evidence_db,
            "db_exists": _db_exists(evidence_db),
            "provider_evidence_count": 0,
            "enrichment_snapshot_count": 0,
            "trigger_results_count": 0,
            "latest_evidence_at": None,
            "latest_snapshot_at": None,
            "latest_trigger_evaluated_at": None,
            "fresh_within_90m": False,
        }

        def _safe_scalar(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
            try:
                row = conn.execute(sql, params).fetchone()
                if not row:
                    return None
                return row[0]
            except Exception:
                return None

        if preload_stats["db_exists"]:
            try:
                con = sqlite3.connect(evidence_db)
                try:
                    if _table_exists(con, "provider_evidence"):
                        preload_stats["provider_evidence_count"] = int(
                            _safe_scalar(
                                con,
                                "SELECT COUNT(*) FROM provider_evidence WHERE lower(county)=?",
                                (county_key,),
                            )
                            or 0
                        )
                        preload_stats["latest_evidence_at"] = _safe_scalar(
                            con,
                            "SELECT MAX(COALESCE(retrieved_at, fetched_at)) FROM provider_evidence WHERE lower(county)=?",
                            (county_key,),
                        )

                    if _table_exists(con, "parcel_enrichment_snapshot"):
                        preload_stats["enrichment_snapshot_count"] = int(
                            _safe_scalar(
                                con,
                                "SELECT COUNT(*) FROM parcel_enrichment_snapshot WHERE lower(county)=?",
                                (county_key,),
                            )
                            or 0
                        )
                        preload_stats["latest_snapshot_at"] = _safe_scalar(
                            con,
                            "SELECT MAX(snapshot_at) FROM parcel_enrichment_snapshot WHERE lower(county)=?",
                            (county_key,),
                        )

                    if _table_exists(con, "trigger_results"):
                        preload_stats["trigger_results_count"] = int(
                            _safe_scalar(
                                con,
                                "SELECT COUNT(*) FROM trigger_results WHERE lower(county)=?",
                                (county_key,),
                            )
                            or 0
                        )
                        preload_stats["latest_trigger_evaluated_at"] = _safe_scalar(
                            con,
                            "SELECT MAX(evaluated_at) FROM trigger_results WHERE lower(county)=?",
                            (county_key,),
                        )
                finally:
                    con.close()
            except Exception:
                pass

        latest_data_ts = max(
            [
                _runtime_age_seconds(preload_stats.get("latest_evidence_at"))
                if _runtime_age_seconds(preload_stats.get("latest_evidence_at")) is not None
                else -1,
                _runtime_age_seconds(preload_stats.get("latest_snapshot_at"))
                if _runtime_age_seconds(preload_stats.get("latest_snapshot_at")) is not None
                else -1,
                _runtime_age_seconds(preload_stats.get("latest_trigger_evaluated_at"))
                if _runtime_age_seconds(preload_stats.get("latest_trigger_evaluated_at")) is not None
                else -1,
            ]
        )
        preload_stats["fresh_within_90m"] = bool(latest_data_ts >= 0 and latest_data_ts <= 5400)

        warnings: list[str] = []
        if runtime_watch.get("enabled") and not watch_fresh:
            warnings.append("watchlists_scheduler_stale")
        if runtime_refresh.get("enabled") and not refresh_fresh:
            warnings.append("statewide_refresh_scheduler_stale")
        if runtime_refresh.get("enabled") and not runtime_refresh.get("preloaded_once"):
            warnings.append("statewide_preload_not_completed")
        if int(preload_stats.get("provider_evidence_count") or 0) <= 0:
            warnings.append("provider_evidence_empty")
        if int(preload_stats.get("enrichment_snapshot_count") or 0) <= 0:
            warnings.append("enrichment_snapshot_empty")

        ready = len(warnings) == 0

        return {
            "ok": True,
            "ready": bool(ready),
            "checked_at": now_iso,
            "uptime_s": int(time.time() - _APP_START_TS),
            "county": county_key,
            "hourly_policy": {
                "target_interval_s": 3600,
                "watchlists_interval_s": watch_interval,
                "statewide_refresh_interval_s": refresh_interval,
            },
            "schedulers": {
                "watchlists": {
                    **runtime_watch,
                    "last_age_s": watch_age_s,
                    "fresh": bool(watch_fresh),
                    "fresh_threshold_s": watch_fresh_s,
                },
                "statewide_refresh": {
                    **runtime_refresh,
                    "last_age_s": refresh_age_s,
                    "fresh": bool(refresh_fresh),
                    "fresh_threshold_s": refresh_fresh_s,
                },
            },
            "preload": preload_stats,
            "warnings": warnings,
        }

    @app.post("/api/debug/runtime_repair")
    def debug_runtime_repair(county: str = "seminole"):
        county_key = (county or "").strip().lower() or "seminole"
        now_iso = _runtime_now_iso()
        out: dict[str, Any] = {
            "ok": True,
            "county": county_key,
            "started_at": now_iso,
            "statewide_refresh": None,
            "errors": [],
        }

        try:
            from florida_property_scraper.scheduler.statewide_refresh import run_statewide_refresh_tick

            db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
            batch_size = int(float(os.getenv("FPS_STATEWIDE_REFRESH_BATCH_SIZE", "100") or 100))
            max_parcels = int(
                float(os.getenv("FPS_STATEWIDE_REFRESH_MAX_PARCELS_PER_COUNTY", "1000") or 1000)
            )
            result = run_statewide_refresh_tick(
                db_path=db_path,
                counties=[county_key],
                batch_size=batch_size,
                max_parcels_per_county=max_parcels,
                min_confidence_label=str(os.getenv("FPS_EVIDENCE_MIN_CONFIDENCE", "low") or "low"),
            )
            out["statewide_refresh"] = result
            _RUNTIME_AUDIT_STATE["statewide_refresh"].update(
                {
                    "last_completed_at": _runtime_now_iso(),
                    "last_error": None,
                    "preloaded_once": True,
                }
            )
        except Exception as exc:
            out["ok"] = False
            out["errors"].append(f"statewide_refresh_failed:{exc}")
            _RUNTIME_AUDIT_STATE["statewide_refresh"]["last_error"] = str(exc)

        out["finished_at"] = _runtime_now_iso()
        out["audit"] = debug_runtime_audit(county=county_key)
        return out

    @app.get("/api/debug/process")
    def debug_process():
        import sys

        start_dt = datetime.fromtimestamp(_APP_START_TS, tz=timezone.utc).isoformat()
        sha = os.getenv("APP_GIT_SHA") or os.getenv("GIT_SHA") or ""
        branch = os.getenv("APP_GIT_BRANCH") or os.getenv("GIT_BRANCH") or ""
        if not sha:
            try:
                head_path = REPO_ROOT / ".git" / "HEAD"
                if head_path.exists():
                    head = head_path.read_text(encoding="utf-8").strip()
                    if head.startswith("ref:"):
                        ref = head.split("ref:", 1)[1].strip()
                        branch = branch or ref.rsplit("/", 1)[-1]
                        ref_path = REPO_ROOT / ".git" / ref
                        if ref_path.exists():
                            sha = ref_path.read_text(encoding="utf-8").strip()
                    else:
                        sha = head
            except Exception:
                pass

        return {
            "ok": True,
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "cwd": os.getcwd(),
            "argv": list(sys.argv),
            "python": sys.executable,
            "start_time": start_dt,
            "git": {"sha": sha, "branch": branch},
        }

    @app.get("/api/debug/sample_record")
    def debug_sample_record(county: str, parcel_id: str):
        import sqlite3

        county_key = (county or "").strip().lower()
        geom_pid = str(parcel_id or "").strip()
        if not county_key or not geom_pid:
            raise HTTPException(status_code=400, detail="county and parcel_id are required")

        leads_db_path = os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
        parcels_db_path = os.getenv("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)

        mapping_row = None
        pa_parcel_id = geom_pid
        pa_row = None
        record_json = None
        record_json_parsed = None
        pt1_row = None
        parcels_pa_row = None

        if os.path.exists(leads_db_path):
            con = sqlite3.connect(leads_db_path)
            con.row_factory = sqlite3.Row
            try:
                mapping_row = con.execute(
                    "SELECT * FROM parcel_id_map WHERE lower(county)=? AND geom_parcel_id=? LIMIT 1",
                    (county_key, geom_pid),
                ).fetchone()
                if mapping_row:
                    mapping_row = dict(mapping_row)
                if mapping_row and mapping_row.get("pa_parcel_id"):
                    pa_parcel_id = str(mapping_row["pa_parcel_id"])
                pa_row = con.execute(
                    "SELECT * FROM pa_properties WHERE lower(county)=? AND parcel_id=? LIMIT 1",
                    (county_key, pa_parcel_id),
                ).fetchone()
                if pa_row:
                    pa_row = dict(pa_row)
                    record_json = pa_row.get("record_json")
                    try:
                        record_json_parsed = json.loads(record_json) if record_json else None
                    except Exception:
                        record_json_parsed = None
                pt1_row = con.execute(
                    "SELECT * FROM parcel_table1 WHERE PARCEL=? LIMIT 1",
                    (pa_parcel_id,),
                ).fetchone()
                if pt1_row:
                    pt1_row = dict(pt1_row)
            finally:
                con.close()

        if os.path.exists(parcels_db_path):
            pcon = sqlite3.connect(parcels_db_path)
            pcon.row_factory = sqlite3.Row
            try:
                parcels_pa_row = pcon.execute(
                    "SELECT * FROM parcels_pa WHERE county=? AND parcel_id=? LIMIT 1",
                    (county_key, geom_pid),
                ).fetchone()
                if parcels_pa_row:
                    parcels_pa_row = dict(parcels_pa_row)
            finally:
                pcon.close()

        def _norm_num(value: object) -> float | None:
            try:
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return float(value)
                s = str(value).strip()
                if not s or s.upper() == "NULL":
                    return None
                return float(s)
            except Exception:
                return None

        merged: dict[str, object] = {"parcel_id": geom_pid, "pa_parcel_id": pa_parcel_id, "county": county_key}
        sources: dict[str, str] = {}

        def _set(field: str, value: object, source: str) -> None:
            if value is None:
                return
            if isinstance(value, str) and not value.strip():
                return
            if merged.get(field) not in (None, ""):
                return
            merged[field] = value
            sources[field] = source

        if pa_row:
            _set("beds", _norm_num(pa_row.get("bedrooms")), "pa_properties")
            _set("baths", _norm_num(pa_row.get("bathrooms")), "pa_properties")
            _set("living_area_sqft", _norm_num(pa_row.get("living_sf")) or _norm_num(pa_row.get("building_sf")), "pa_properties")
            _set("year_built", _norm_num(pa_row.get("year_built")), "pa_properties")
            _set("zoning", pa_row.get("zoning"), "pa_properties")
            _set("future_land_use", pa_row.get("future_land_use"), "pa_properties")
            _set("last_sale_date", pa_row.get("last_sale_date"), "pa_properties")
            _set("last_sale_price", _norm_num(pa_row.get("last_sale_price")), "pa_properties")
            _set("assessed_value", _norm_num(pa_row.get("assessed_value")), "pa_properties")
            _set("taxable_value", _norm_num(pa_row.get("taxable_value")), "pa_properties")
            _set("just_value", _norm_num(pa_row.get("just_value")), "pa_properties")

        if record_json_parsed:
            def _get(*keys: str):
                for k in keys:
                    if k in record_json_parsed and record_json_parsed[k] not in (None, ""):
                        return record_json_parsed[k]
                return None

            _set("beds", _norm_num(_get("beds", "bedrooms")), "pa_properties.record_json")
            _set("baths", _norm_num(_get("baths", "bathrooms")), "pa_properties.record_json")
            _set(
                "living_area_sqft",
                _norm_num(_get("living_area_sqft", "living_sf", "living_area", "heated_area", "building_sf")),
                "pa_properties.record_json",
            )
            _set("year_built", _norm_num(_get("year_built", "yr_built")), "pa_properties.record_json")
            _set("zoning", _get("zoning"), "pa_properties.record_json")
            _set("future_land_use", _get("future_land_use"), "pa_properties.record_json")
            _set("last_sale_date", _get("last_sale_date", "sale_date"), "pa_properties.record_json")
            _set("last_sale_price", _norm_num(_get("last_sale_price", "sale_price")), "pa_properties.record_json")
            _set("assessed_value", _norm_num(_get("assessed_value")), "pa_properties.record_json")
            _set("taxable_value", _norm_num(_get("taxable_value")), "pa_properties.record_json")
            _set("just_value", _norm_num(_get("just_value", "total_value")), "pa_properties.record_json")

        if pt1_row:
            _set("living_area_sqft", _norm_num(pt1_row.get("LIVING_AREA")) or _norm_num(pt1_row.get("TOTAL_SQFT")), "parcel_table1")
            _set("year_built", _norm_num(pt1_row.get("BASE_YR_BLT")), "parcel_table1")
            _set("just_value", _norm_num(pt1_row.get("TOTAL_JUST_VALUE")), "parcel_table1")
            _set("assessed_value", _norm_num(pt1_row.get("TOTAL_ASSESSED_VALUE")), "parcel_table1")
            _set("land_value", _norm_num(pt1_row.get("APPR_LAND")), "parcel_table1")
            _set("improvement_value", _norm_num(pt1_row.get("APPR_BLDG")), "parcel_table1")

        if parcels_pa_row:
            _set("beds", _norm_num(parcels_pa_row.get("beds")), "parcels_pa")
            _set("baths", _norm_num(parcels_pa_row.get("baths")), "parcels_pa")
            _set("living_area_sqft", _norm_num(parcels_pa_row.get("living_area_sqft")), "parcels_pa")
            _set("year_built", _norm_num(parcels_pa_row.get("year_built")), "parcels_pa")
            _set("last_sale_date", parcels_pa_row.get("last_sale_date"), "parcels_pa")
            _set("last_sale_price", _norm_num(parcels_pa_row.get("last_sale_price")), "parcels_pa")

        return {
            "county": county_key,
            "geom_parcel_id": geom_pid,
            "pa_parcel_id": pa_parcel_id,
            "mapping_row": dict(mapping_row) if mapping_row else None,
            "pa_properties": dict(pa_row) if pa_row else None,
            "record_json": record_json_parsed,
            "parcel_table1": dict(pt1_row) if pt1_row else None,
            "parcels_pa": dict(parcels_pa_row) if parcels_pa_row else None,
            "merged_record": merged,
            "merged_sources": sources,
        }

    @app.get("/api/owners/enrich")
    def owners_enrich(county: str, parcel_id: str):
        from florida_property_scraper.enrichment.providers.registry import get_owner_enrichment_provider
        from florida_property_scraper.pa.storage import PASQLite
        from florida_property_scraper.storage import SQLiteStore
        import sqlite3

        county_key = (county or "").strip().lower()
        pid = str(parcel_id or "").strip()
        if not county_key or not pid:
            raise HTTPException(status_code=400, detail="county and parcel_id are required")

        def _resolve_pa_parcel_id() -> str:
            try:
                import sqlite3 as _sqlite3

                leads_path = os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
                if not leads_path or not os.path.exists(leads_path):
                    return pid
                con = _sqlite3.connect(leads_path)
                try:
                    row = con.execute(
                        "SELECT pa_parcel_id FROM parcel_id_map WHERE county=? AND geom_parcel_id=?",
                        (county_key, pid),
                    ).fetchone()
                    if row and row[0]:
                        return str(row[0])
                finally:
                    con.close()
            except Exception:
                return pid
            return pid

        pa_parcel_id = _resolve_pa_parcel_id()

        pa_store = PASQLite(_resolve_db_path("PA_DB", DEFAULT_LEADS_DB))
        try:
            pa = pa_store.get(county=county_key, parcel_id=pa_parcel_id)
        finally:
            pa_store.close()

        owner_name = ""
        mailing_address = ""
        lead_phones: list[str] = []
        lead_emails: list[str] = []
        pa_missing = pa is None

        if pa is not None:
            owner_name = "; ".join([n for n in (pa.owner_names or []) if n]).strip()
            mailing_address = ", ".join(
                [
                    str(pa.mailing_address or "").strip(),
                    " ".join(
                        [
                            str(pa.mailing_city or "").strip(),
                            str(pa.mailing_state or "").strip(),
                            str(pa.mailing_zip or "").strip(),
                        ]
                    ).strip(),
                ]
            ).replace(" ,", ",").strip(" ,")

        def _parse_contact_list(value: Any) -> list[str]:
            if value is None:
                return []
            if isinstance(value, list):
                return [str(v).strip() for v in value if str(v).strip()]
            s = str(value).strip()
            if not s:
                return []
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except Exception:
                pass
            return [x.strip() for x in s.split(",") if x.strip()]

        try:
            leads_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
            if os.path.exists(leads_path):
                con = sqlite3.connect(leads_path)
                con.row_factory = sqlite3.Row
                try:
                    row = con.execute(
                        """
                        SELECT owner_name, mailing_address, contact_phones, contact_emails
                        FROM leads
                        WHERE lower(county)=? AND (parcel_id=? OR parcel_id=?)
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (county_key, pid, pa_parcel_id),
                    ).fetchone()
                    if row:
                        if not owner_name:
                            owner_name = str(row["owner_name"] or "").strip()
                        if not mailing_address:
                            mailing_address = str(row["mailing_address"] or "").strip()
                        lead_phones = _parse_contact_list(row["contact_phones"])
                        lead_emails = _parse_contact_list(row["contact_emails"])
                finally:
                    con.close()
        except Exception:
            pass

        provider = get_owner_enrichment_provider()
        provider_name = provider.name if provider is not None else ""
        provider_configured = bool(provider is not None and provider.is_configured())

        store = SQLiteStore(os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB))
        try:
            cached = None
            if provider_name:
                cached = store.get_owner_enrichment(
                    county=county_key,
                    parcel_id=pa_parcel_id,
                    provider=provider_name,
                )

            if cached:
                return JSONResponse(
                    {
                        "county": county_key,
                        "parcel_id": pid,
                        "pa_parcel_id": pa_parcel_id if pa_parcel_id != pid else None,
                        "owner_name": owner_name or None,
                        "owner_mailing_address": mailing_address or None,
                        "provider": provider_name,
                        "configured": provider_configured,
                        "status": cached.get("status") or ("pa_missing_cache" if pa_missing else "cache"),
                        "phones": cached.get("phones") or lead_phones,
                        "emails": cached.get("emails") or lead_emails,
                        "cache_hit": True,
                    }
                )

            if not provider_configured:
                status = "not_configured"
                if pa_missing and (lead_phones or lead_emails):
                    status = "local_cache_only"
                elif pa_missing:
                    status = "pa_record_missing"
                return JSONResponse(
                    {
                        "county": county_key,
                        "parcel_id": pid,
                        "pa_parcel_id": pa_parcel_id if pa_parcel_id != pid else None,
                        "owner_name": owner_name or None,
                        "owner_mailing_address": mailing_address or None,
                        "provider": provider_name,
                        "configured": False,
                        "status": status,
                        "phones": lead_phones,
                        "emails": lead_emails,
                        "cache_hit": False,
                    }
                )

            assert provider is not None
            result = provider.enrich(
                owner_name=owner_name,
                mailing_address=mailing_address,
                county=county_key,
                parcel_id=pa_parcel_id,
            )
            store.upsert_owner_enrichment(
                county=county_key,
                parcel_id=pa_parcel_id,
                provider=result.provider,
                status=result.status,
                owner_name=owner_name,
                mailing_address=mailing_address,
                phones=result.phones,
                emails=result.emails,
                raw=result.raw,
            )

            return JSONResponse(
                {
                    "county": county_key,
                    "parcel_id": pid,
                    "pa_parcel_id": pa_parcel_id if pa_parcel_id != pid else None,
                    "owner_name": owner_name or None,
                    "owner_mailing_address": mailing_address or None,
                    "provider": result.provider,
                    "configured": True,
                    "status": result.status,
                    "phones": result.phones,
                    "emails": result.emails,
                    "cache_hit": False,
                }
            )
        finally:
            store.close()


    @app.get("/api/parcels/{parcel_id}")
    def api_parcel_detail(
        parcel_id: str,
        county: str = "",
        include_geometry: bool = False,
        include_fields: bool = False,
    ):
        """Return full PA normalized detail + user meta.

        PA-only: this endpoint never enriches outside PA.
        """

        from florida_property_scraper.pa.storage import PASQLite

        county_key = (county or "").strip().lower() or "seminole"

        # include_geometry: best-effort geometry attach (Seminole uses parcels.sqlite)
        geom = None
        if include_geometry and county_key == "seminole":
            try:
                import sqlite3, json as _json
                parcels_db = os.getenv("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
                if parcels_db:
                    con = sqlite3.connect(parcels_db)
                    cur = con.cursor()
                    row = cur.execute(
                        "SELECT geom_geojson FROM parcels WHERE county=? AND parcel_id=?",
                        ("seminole", str(parcel_id).strip()),
                    ).fetchone()
                    con.close()
                    if row and row[0]:
                        geom = _json.loads(row[0])
            except Exception:
                geom = None

        parcel_key = str(parcel_id)
        db_path = _resolve_db_path("PA_DB", DEFAULT_LEADS_DB)

        pa_store = PASQLite(db_path)
        try:
            rec = pa_store.get(county=county_key, parcel_id=parcel_key)

            # Seminole bridge: geometry parcel_id -> PA parcel_id via parcel_id_map (address-built)
            if rec is None and county_key == "seminole":
                try:
                    import sqlite3 as _sqlite3
                    _con = _sqlite3.connect(db_path)
                    _cur = _con.cursor()
                    _row = _cur.execute(
                        "SELECT pa_parcel_id FROM parcel_id_map WHERE county=? AND geom_parcel_id=? LIMIT 1",
                        ("seminole", parcel_key),
                    ).fetchone()
                    _con.close()
                    if _row and _row[0]:
                        rec = pa_store.get(county=county_key, parcel_id=str(_row[0]))
                except Exception:
                    pass
        finally:
            pa_store.close()

        pa = rec.to_dict() if rec is not None else None

        enrichment_snapshot = None
        merged_fields: dict[str, Any] = {}
        evidence_ids: list[int] = []
        owner_enrichment = None
        try:
            from florida_property_scraper.storage import SQLiteStore

            evidence_db = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
            enrichment_pid = parcel_key
            if isinstance(pa, dict):
                pa_pid = str(pa.get("parcel_id") or "").strip()
                if pa_pid:
                    enrichment_pid = pa_pid
            store = SQLiteStore(evidence_db)
            try:
                enrichment_snapshot = store.get_latest_parcel_enrichment_snapshot(
                    county=county_key,
                    parcel_id=enrichment_pid,
                )
                if not isinstance(enrichment_snapshot, dict) or not isinstance(
                    enrichment_snapshot.get("merged_fields"), dict
                ):
                    # Fallback: build a fresh snapshot directly from provider evidence
                    # so parcel detail remains complete even before a scheduler refresh.
                    ev_rows = store.list_provider_evidence_for_parcels(
                        county=county_key,
                        parcel_ids=[enrichment_pid],
                    )
                    if ev_rows:
                        min_conf_label = (
                            str(os.getenv("FPS_EVIDENCE_MIN_CONFIDENCE", "low")).strip().lower() or "low"
                        )
                        merged, ev_ids = store.build_enriched_fields(
                            evidence_rows=ev_rows,
                            min_confidence_label=min_conf_label,
                        )
                        if merged:
                            snapshot_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                            # Save the snapshot if possible, but always use merged_fields regardless
                            try:
                                store.save_parcel_enrichment_snapshot(
                                    county=county_key,
                                    parcel_id=enrichment_pid,
                                    snapshot_at=snapshot_at,
                                    merged_fields=merged,
                                    evidence_ids=ev_ids,
                                )
                            except Exception:
                                pass  # snapshot save failed, but we still use merged
                            enrichment_snapshot = {
                                "merged_fields": merged,
                                "evidence_ids": ev_ids,
                                "snapshot_at": snapshot_at,
                            }
                owner_enrichment = store.get_latest_owner_enrichment(
                    county=county_key,
                    parcel_id=enrichment_pid,
                )
            finally:
                store.close()
        except Exception as _enrichment_exc:
            import traceback
            try:
                import logging
                logging.getLogger("fps.parcel_detail").exception("enrichment fallback failed: %s", _enrichment_exc)
            except Exception:
                pass
            enrichment_snapshot = None
            owner_enrichment = None

        if isinstance(enrichment_snapshot, dict):
            merged_fields = enrichment_snapshot.get("merged_fields") or {}
            evidence_ids = enrichment_snapshot.get("evidence_ids") or []

        def _is_missing_value(field: str, value: object) -> bool:
            if value is None:
                return True
            if isinstance(value, str):
                return not value.strip()
            if isinstance(value, bool):
                return False
            if isinstance(value, (int, float)):
                try:
                    return float(value) == 0.0
                except Exception:
                    return True
            if isinstance(value, (list, tuple, set, dict)):
                return len(value) == 0
            return False

        def _overlay_missing_fields(dst: dict[str, Any], src: dict[str, Any]) -> None:
            for k, v in (src or {}).items():
                key = str(k)
                if _is_missing_value(key, dst.get(key)) and not _is_missing_value(key, v):
                    dst[key] = v

        if merged_fields:
            if pa is None:
                pa = {}
            if isinstance(pa, dict):
                _overlay_missing_fields(pa, merged_fields)

        user_db = os.getenv("USER_META_DB", db_path)
        meta_store = UserMetaSQLite(user_db)
        try:
            meta = meta_store.get(county=county_key, parcel_id=parcel_key)
        finally:
            meta_store.close()

        from florida_property_scraper.pa.ui_computed import compute_ui_fields

        computed = compute_ui_fields(pa)

        owner_enrichment_payload = {
            "provider": (owner_enrichment or {}).get("provider"),
            "status": (owner_enrichment or {}).get("status"),
            "phones": (owner_enrichment or {}).get("phones") or [],
            "emails": (owner_enrichment or {}).get("emails") or [],
            "owner_name": (owner_enrichment or {}).get("owner_name"),
            "mailing_address": (owner_enrichment or {}).get("mailing_address"),
            "updated_at": (owner_enrichment or {}).get("updated_at"),
        }

        profile_canonical: dict[str, Any] = {}
        if isinstance(pa, dict):
            profile_canonical.update(pa)
        if isinstance(merged_fields, dict):
            _overlay_missing_fields(profile_canonical, merged_fields)

        owner_name_from_contacts = str(owner_enrichment_payload.get("owner_name") or "").strip()
        if owner_name_from_contacts and _is_missing_value(
            "owner_name", profile_canonical.get("owner_name")
        ):
            profile_canonical["owner_name"] = owner_name_from_contacts

        phones = owner_enrichment_payload.get("phones") or []
        if phones and _is_missing_value("owner_phone", profile_canonical.get("owner_phone")):
            profile_canonical["owner_phone"] = str(phones[0] or "").strip()

        emails = owner_enrichment_payload.get("emails") or []
        if emails and _is_missing_value("owner_email", profile_canonical.get("owner_email")):
            profile_canonical["owner_email"] = str(emails[0] or "").strip()

        if _is_missing_value("mailing_address", profile_canonical.get("mailing_address")):
            mailing_from_owner = str(owner_enrichment_payload.get("mailing_address") or "").strip()
            if mailing_from_owner:
                profile_canonical["mailing_address"] = mailing_from_owner

        for k, v in (computed or {}).items():
            key = str(k)
            if _is_missing_value(key, profile_canonical.get(key)) and not _is_missing_value(key, v):
                profile_canonical[key] = v

        # Canonical profile uses UI-facing aliases; keep PA field names for back-compat
        # while ensuring completeness checks see equivalent populated fields.
        canonical_aliases: tuple[tuple[str, tuple[str, ...]], ...] = (
            ("owner_name", ("owner_name", "owner_names")),
            ("beds", ("beds", "bedrooms")),
            ("baths", ("baths", "bathrooms")),
            ("living_area_sqft", ("living_area_sqft", "living_sf", "building_sf")),
            ("lot_size_sqft", ("lot_size_sqft", "land_sf")),
            ("lot_size_acres", ("lot_size_acres", "land_acres")),
            ("building_value", ("building_value", "improvement_value")),
            ("total_value", ("total_value", "just_value")),
        )
        for target_key, source_keys in canonical_aliases:
            if not _is_missing_value(target_key, profile_canonical.get(target_key)):
                continue
            for source_key in source_keys:
                source_val = profile_canonical.get(source_key)
                if _is_missing_value(source_key, source_val):
                    continue
                if target_key == "owner_name" and source_key == "owner_names":
                    if isinstance(source_val, list):
                        joined = "; ".join([str(x).strip() for x in source_val if str(x).strip()])
                        if joined:
                            profile_canonical[target_key] = joined
                            break
                    continue
                profile_canonical[target_key] = source_val
                break

        completeness_checklist = [
            "parcel_id",
            "situs_address",
            "situs_city",
            "situs_state",
            "situs_zip",
            "owner_name",
            "mailing_address",
            "mailing_city",
            "mailing_state",
            "mailing_zip",
            "beds",
            "baths",
            "year_built",
            "living_area_sqft",
            "lot_size_sqft",
            "lot_size_acres",
            "zoning",
            "future_land_use",
            "last_sale_date",
            "last_sale_price",
            "just_value",
            "assessed_value",
            "taxable_value",
            "land_value",
            "building_value",
            "total_value",
            "owner_phone",
            "owner_email",
        ]
        completeness_missing = [
            k for k in completeness_checklist if _is_missing_value(k, profile_canonical.get(k))
        ]
        completeness_present = [k for k in completeness_checklist if k not in completeness_missing]
        completeness_pct = 0
        if completeness_checklist:
            completeness_pct = int(
                round(
                    (len(completeness_present) / float(len(completeness_checklist))) * 100.0,
                    0,
                )
            )

        coverage_payload = {
            "required_fields": completeness_checklist,
            "present_fields": completeness_present,
            "missing_fields": completeness_missing,
            "present_count": int(len(completeness_present)),
            "required_count": int(len(completeness_checklist)),
            "completeness_pct": int(completeness_pct),
        }

        property_profile = {
            "canonical": profile_canonical,
            "pa_fields": pa or {},
            "enrichment_fields": merged_fields if isinstance(merged_fields, dict) else {},
            "computed_fields": computed or {},
            "owner_enrichment": owner_enrichment_payload,
        }

        payload = {
            "county": county_key,
            "parcel_id": parcel_key,
            "pa": pa or {},
            "computed": computed or {},
            "user_meta": meta.to_dict()
            if meta is not None
            else empty_user_meta(county=county_key, parcel_id=parcel_key),
            "merged_fields": merged_fields if isinstance(merged_fields, dict) else {},
            "evidence_ids": [int(x) for x in (evidence_ids or []) if int(x) > 0],
            "owner_enrichment": owner_enrichment_payload,
            "property_profile": property_profile,
            "coverage": coverage_payload,
        }

        if not include_fields:
            return JSONResponse(payload)

        result = dict(payload)

        # Seminole fallback: populate situs fields from sem_addr_index when PA record is missing
        if county_key == "seminole" and (pa is None):
            try:
                import sqlite3 as _sqlite3
                _con = _sqlite3.connect(db_path)
                _cur = _con.cursor()
                _row = _cur.execute(
                    "SELECT situs_address, city, zip FROM sem_addr_index WHERE parcel_id=? LIMIT 1",
                    (parcel_key,),
                ).fetchone()
                _con.close()
                if _row:
                    _addr, _city, _zip = _row
                    result["situs_address"] = _addr or ""
                    result["situs_city"] = _city or ""
                    result["situs_state"] = "FL"
                    result["situs_zip"] = _zip or ""
            except Exception:
                pass


        if isinstance(result, dict):
            if geom is not None:
                result["geometry"] = geom
            pa = result.get("pa") or {}

            # UI_FLATTEN_FROM_PA: map PA fields to UI keys
            def _n(v):
                return v if v not in (None,"",[],{}) else None

            # Beds/Baths (UI uses beds/baths, PA uses bedrooms/bathrooms)
            b = _n((pa.get("beds") if isinstance(pa, dict) else None))
            if b is None: b = _n((pa.get("bedrooms") if isinstance(pa, dict) else None))
            try:
                b = int(float(b)) if b is not None else None
            except Exception:
                pass
            if b is not None:
                result.setdefault("beds", b)

            ba = _n((pa.get("baths") if isinstance(pa, dict) else None))
            if ba is None: ba = _n((pa.get("bathrooms") if isinstance(pa, dict) else None))
            try:
                ba = float(ba) if ba is not None else None
            except Exception:
                pass
            if ba is not None:
                result.setdefault("baths", ba)

            # Values (UI uses just/assessed/taxable)
            for k in ("just_value","assessed_value","taxable_value"):
                v = _n((pa.get(k) if isinstance(pa, dict) else None))
                try:
                    v = float(v) if v is not None else None
                except Exception:
                    pass
                if v is not None:
                    result.setdefault(k, v)

        # UI_TOPLEVEL_FROM_PA_FALLBACKS: populate UI top-level fields from PA with common key fallbacks
        pa_dict = pa if isinstance(pa, dict) else {}

        def _first(*vals):
            for v in vals:
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                return v
            return None

        # living area / sqft
        la = _first(
            pa_dict.get("living_area_sqft"),
            pa_dict.get("living_area"),
            pa_dict.get("living_sf"),
            pa_dict.get("heated_area"),
            pa_dict.get("building_sf"),
            pa_dict.get("gross_area"),
            pa_dict.get("building_area"),
            pa_dict.get("sqft"),
        )
        if la is not None and result.get("living_area_sqft") in (None, ""):
            try:
                result["living_area_sqft"] = int(float(la))
            except Exception:
                result["living_area_sqft"] = la

        # lot size
        ls = _first(pa_dict.get("lot_size_sqft"), pa_dict.get("lot_sqft"), pa_dict.get("land_sqft"))
        if ls is not None and result.get("lot_size_sqft") in (None, ""):
            try:
                result["lot_size_sqft"] = int(float(ls))
            except Exception:
                result["lot_size_sqft"] = ls

        # acreage
        la_ac = _first(pa_dict.get("lot_size_acres"), pa_dict.get("land_acres"))
        if la_ac is not None and result.get("lot_size_acres") in (None, ""):
            try:
                result["lot_size_acres"] = float(la_ac)
            except Exception:
                result["lot_size_acres"] = la_ac

        # mailing address (single string OR parts)
        maddr = _first(pa_dict.get("mailing_address"), pa_dict.get("mail_address"))
        if (not maddr):
            ms = _first(pa_dict.get("mailing_street"), pa_dict.get("mailing_addr1"), pa_dict.get("mail_addr1"))
            mc = _first(pa_dict.get("mailing_city"), pa_dict.get("mail_city"))
            mst = _first(pa_dict.get("mailing_state"), pa_dict.get("mail_state"))
            mz = _first(pa_dict.get("mailing_zip"), pa_dict.get("mail_zip"))
            parts = [x for x in [ms, mc, mst, mz] if x not in (None,"")]
            if parts:
                maddr = ", ".join([str(x) for x in parts])

        if maddr and (result.get("mailing_address") in (None,"")):
            result["mailing_address"] = str(maddr)

        # ensure owner_names/year_built bubble up if UI reads top-level
        if result.get("owner_names") in (None, [], "") and pa_dict.get("owner_names"):
            result["owner_names"] = pa_dict.get("owner_names")
        if result.get("year_built") in (None, "") and pa_dict.get("year_built") is not None:
            result["year_built"] = pa_dict.get("year_built")

        # FLATTEN_PA_TOPLEVEL: expose key PA fields at top-level for UI panels
        # (Use setdefault so Seminole fallback / earlier values are not overwritten)
        _pa = pa_dict
        for _k in (
            "owner_names",
            "mailing_address",
            "mailing_city",
            "mailing_state",
            "mailing_zip",
            "year_built",
            "living_area_sqft",
            "lot_size_sqft",
            "lot_size_acres",
            "zoning",
            "future_land_use",
            "property_class",
            "use_type",
            "land_value",
            "building_value",
            "total_value",
            "assessed_value",
            "taxable_value",
            "owner_phone",
            "owner_email",
        ):
            try:
                result.setdefault(_k, _pa.get(_k))
            except Exception:
                pass

        owner_enrich_data = result.get("owner_enrichment") or {}
        owner_phones = owner_enrich_data.get("phones") or []
        owner_emails = owner_enrich_data.get("emails") or []
        if owner_phones:
            result.setdefault("owner_phone", str(owner_phones[0] or "").strip())
        if owner_emails:
            result.setdefault("owner_email", str(owner_emails[0] or "").strip())

        result.setdefault("situs_address", pa_dict.get("situs_address") or "")
        result.setdefault("situs_city", pa_dict.get("situs_city") or "")
        result.setdefault("situs_state", pa_dict.get("situs_state") or "")
        result.setdefault("situs_zip", pa_dict.get("situs_zip") or "")
        owners = pa_dict.get("owner_names") or []
        result["owner_name"] = (owners[0] if owners else (pa_dict.get("owner_name") or ""))
        if not str(result.get("owner_name") or "").strip():
            result["owner_name"] = str(owner_enrich_data.get("owner_name") or "").strip()
        if result.get("mailing_address") in (None, ""):
            result["mailing_address"] = str(owner_enrich_data.get("mailing_address") or "").strip()
        result["last_sale_date"] = pa_dict.get("last_sale_date") or ""
        result["last_sale_price"] = pa_dict.get("last_sale_price") or 0
        result["just_value"] = pa_dict.get("just_value") or 0

        # Source URLs and missing fields for UI
        try:
            source_urls = []
            if isinstance(pa, dict):
                su = str(pa.get("source_url") or "").strip()
                if su:
                    source_urls.append(su)
                sources = pa.get("sources") or []
                if isinstance(sources, list):
                    for s in sources:
                        if not isinstance(s, dict):
                            continue
                        u = str(s.get("url") or "").strip()
                        if u:
                            source_urls.append(u)
            if source_urls:
                result["source_urls"] = list(dict.fromkeys(source_urls))
        except Exception:
            pass

        try:
            checklist = coverage_payload.get("required_fields") or []
            missing = []
            for k in checklist:
                v = result.get(k)
                if v is None or v == "" or v == []:
                    missing.append(k)
            result["missing_fields"] = missing
            present = [k for k in checklist if k not in missing]
            result["coverage"] = {
                "required_fields": checklist,
                "present_fields": present,
                "missing_fields": missing,
                "present_count": int(len(present)),
                "required_count": int(len(checklist)),
                "completeness_pct": int(
                    round((len(present) / float(len(checklist))) * 100.0, 0)
                )
                if checklist
                else 0,
            }
        except Exception:
            pass

        profile_payload = result.get("property_profile") or {}
        canonical = profile_payload.get("canonical")
        if not isinstance(canonical, dict):
            canonical = {}
        for key in (result.get("coverage") or {}).get("required_fields") or []:
            k = str(key)
            if _is_missing_value(k, canonical.get(k)) and not _is_missing_value(k, result.get(k)):
                canonical[k] = result.get(k)
        profile_payload["canonical"] = canonical
        result["property_profile"] = profile_payload

        return JSONResponse(result)

    @app.get("/api/parcels/{parcel_id}/meta")
    def api_parcel_meta_get(parcel_id: str, county: str = ""):
        county_key = (county or "").strip().lower() or "seminole"
        parcel_key = str(parcel_id)
        db_path = _resolve_db_path("PA_DB", DEFAULT_LEADS_DB)
        user_db = _resolve_db_path("USER_META_DB", db_path)

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
        db_path = _resolve_db_path("PA_DB", DEFAULT_LEADS_DB)
        user_db = _resolve_db_path("USER_META_DB", db_path)

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
        hover = {}
        """Return minimal PA-only hover fields.

        Mortgage fields are always blank/0 unless PA explicitly provides them.
        """

        from florida_property_scraper.pa.storage import PASQLite

        county_key = (county or "").strip().lower()
        parcel_key = str(parcel_id)
        db_path = _resolve_db_path("PA_DB", DEFAULT_LEADS_DB)

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
    try:
        from florida_property_scraper.db.init import init_db
    except Exception as e:
        init_db = None
        logging.getLogger("fps.startup").warning("init_db import failed: %s", e)

    @app.on_event("startup")
    def _ensure_leads_db():
        logger = logging.getLogger("fps.startup")
        parcels_db_path = _resolve_db_path("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
        leads_db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
        parcels_db_ok = _db_exists(parcels_db_path)
        leads_db_ok = _db_exists(leads_db_path)
        logger.warning(
            "startup env: FPS_USE_FDOR_CENTROIDS=%s PA_DB=%s APP_GIT_SHA=%s APP_GIT_BRANCH=%s",
            os.getenv("FPS_USE_FDOR_CENTROIDS", ""),
            os.getenv("PA_DB", ""),
            os.getenv("APP_GIT_SHA", ""),
            os.getenv("APP_GIT_BRANCH", ""),
        )
        try:
            parcels_count = 0
            if parcels_db_ok:
                import sqlite3 as _sqlite3

                con = _sqlite3.connect(parcels_db_path)
                cur = con.cursor()
                if _table_exists(con, "parcels"):
                    parcels_count = int(cur.execute("SELECT count(*) FROM parcels").fetchone()[0])
                con.close()
            leads_count = 0
            if leads_db_ok:
                import sqlite3 as _sqlite3

                con = _sqlite3.connect(leads_db_path)
                cur = con.cursor()
                if _table_exists(con, "pa_properties"):
                    leads_count = int(cur.execute("SELECT count(*) FROM pa_properties").fetchone()[0])
                con.close()
            logger.warning(
                "startup self-check: parcels_db=%s exists=%s count=%s leads_db=%s exists=%s pa_count=%s",
                parcels_db_path,
                parcels_db_ok,
                parcels_count,
                leads_db_path,
                leads_db_ok,
                leads_count,
            )
        except Exception as e:
            logger.warning("startup self-check failed: %s", e)
        if init_db is None:
            logger.warning("init_db unavailable; skipping leads DB initialization")
            return
        try:
            init_db(os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB))
        except Exception as e:
            logger.warning("init_db failed: %s", e)

        def _maybe_seed_demo_parcels() -> None:
            try:
                import sqlite3 as _sqlite3
                import json as _json

                parcels_db = os.getenv("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
                if not parcels_db:
                    return
                Path(parcels_db).parent.mkdir(parents=True, exist_ok=True)
                con = _sqlite3.connect(parcels_db)
                try:
                    cur = con.cursor()
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS parcels (
                          county TEXT NOT NULL,
                          parcel_id TEXT NOT NULL,
                          geom_geojson TEXT,
                          minx REAL, miny REAL, maxx REAL, maxy REAL
                        )
                        """
                    )
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_parcels_county ON parcels(county)")
                    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_parcels_county_pid ON parcels(county, parcel_id)")
                    cur.execute(
                        """
                        CREATE VIRTUAL TABLE IF NOT EXISTS parcels_rtree
                        USING rtree(id, minx, maxx, miny, maxy)
                        """
                    )
                    cnt = int(cur.execute("SELECT count(*) FROM parcels").fetchone()[0])
                    if cnt > 0:
                        return

                    base_lng = -81.35
                    base_lat = 28.70
                    step = 0.002
                    created = 0
                    for i in range(50):
                        row = i // 10
                        col = i % 10
                        minx = base_lng + col * step
                        miny = base_lat + row * step
                        maxx = minx + step * 0.8
                        maxy = miny + step * 0.8
                        geom = {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [minx, miny],
                                    [maxx, miny],
                                    [maxx, maxy],
                                    [minx, maxy],
                                    [minx, miny],
                                ]
                            ],
                        }
                        pid = f"DEMO{i:03d}"
                        cur.execute(
                            "INSERT OR IGNORE INTO parcels(county, parcel_id, geom_geojson, minx, miny, maxx, maxy) VALUES (?,?,?,?,?,?,?)",
                            ("seminole", pid, _json.dumps(geom), minx, miny, maxx, maxy),
                        )
                        rowid = cur.lastrowid
                        if rowid:
                            cur.execute(
                                "INSERT OR IGNORE INTO parcels_rtree(id, minx, maxx, miny, maxy) VALUES (?,?,?,?,?)",
                                (rowid, minx, maxx, miny, maxy),
                            )
                            created += 1
                    con.commit()
                    logger.warning("seeded demo parcels: %s", created)
                finally:
                    con.close()
            except Exception as e:
                logger.warning("demo parcels seed failed: %s", e)

        def _maybe_seed_demo_rollups() -> None:
            try:
                import sqlite3 as _sqlite3
                import json as _json

                leads_db = os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
                if not leads_db:
                    return
                con = _sqlite3.connect(leads_db)
                try:
                    cur = con.cursor()
                    cnt = int(cur.execute("SELECT count(*) FROM parcel_trigger_rollups").fetchone()[0])
                    if cnt > 0:
                        try:
                            rollup_ids = [
                                str(r[0])
                                for r in cur.execute(
                                    "SELECT parcel_id FROM parcel_trigger_rollups WHERE county='seminole' LIMIT 50"
                                ).fetchall()
                                if r and r[0]
                            ]
                            parcels_db = os.getenv("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
                            if parcels_db and Path(parcels_db).exists() and rollup_ids:
                                pcon = _sqlite3.connect(parcels_db)
                                try:
                                    placeholders = ",".join(["?"] * len(rollup_ids))
                                    row = pcon.execute(
                                        f"SELECT parcel_id FROM parcels WHERE county='seminole' AND geom_geojson IS NOT NULL AND parcel_id IN ({placeholders}) LIMIT 1",
                                        rollup_ids,
                                    ).fetchone()
                                    if row and row[0]:
                                        return
                                finally:
                                    pcon.close()
                        except Exception:
                            return
                        # Rollups exist but don't align with parcels geometry; reseed.
                        cur.execute("DELETE FROM parcel_trigger_rollups")

                    parcels_db = os.getenv("PARCELS_DB_PATH", DEFAULT_PARCELS_DB)
                    parcel_ids = []
                    try:
                        if parcels_db and Path(parcels_db).exists():
                            pcon = _sqlite3.connect(parcels_db)
                            try:
                                rows = pcon.execute(
                                    "SELECT parcel_id FROM parcels WHERE county='seminole' AND geom_geojson IS NOT NULL LIMIT 10"
                                ).fetchall()
                                parcel_ids = [str(r[0]) for r in rows if r and r[0]]
                            finally:
                                pcon.close()
                    except Exception:
                        parcel_ids = []
                    if not parcel_ids:
                        parcel_ids = [f"DEMO{i:03d}" for i in range(10)]

                    now = datetime.now(timezone.utc).isoformat()
                    for i, pid in enumerate(parcel_ids):
                        details = {
                            "trigger_keys": ["permit_hvac"] if i % 2 == 0 else ["lis_pendens"],
                            "groups": ["permits"] if i % 2 == 0 else ["official_records"],
                        }
                        cur.execute(
                            """
                            INSERT OR REPLACE INTO parcel_trigger_rollups (
                              county, parcel_id, rebuilt_at, last_seen_any,
                              last_seen_permits, last_seen_tax, last_seen_official_records,
                              last_seen_code_enforcement, last_seen_courts, last_seen_gis_planning,
                              has_permits, has_tax, has_official_records, has_code_enforcement,
                              has_courts, has_gis_planning, count_critical, count_strong, count_support,
                              seller_score, details_json
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                "seminole",
                                pid,
                                now,
                                now,
                                now if i % 2 == 0 else None,
                                None,
                                now if i % 2 == 1 else None,
                                None,
                                None,
                                None,
                                1 if i % 2 == 0 else 0,
                                0,
                                1 if i % 2 == 1 else 0,
                                0,
                                0,
                                0,
                                1 if i % 2 == 1 else 0,
                                1 if i % 2 == 0 else 0,
                                0,
                                45 + i,
                                _json.dumps(details),
                            ),
                        )
                    con.commit()
                    logger.warning("seeded demo rollups: %s", len(parcel_ids))
                finally:
                    con.close()
            except Exception as e:
                logger.warning("demo rollups seed failed: %s", e)

        # Demo seeding disabled: production must use real data sources only.

    _watchlists_scheduler_task: dict[str, Any] = {"task": None}
    _statewide_refresh_scheduler_task: dict[str, Any] = {"task": None}

    @app.on_event("startup")
    async def _start_watchlists_scheduler():
        import asyncio

        enabled = os.getenv("FPS_WATCHLIST_SCHEDULER", "1").strip() == "1"
        _RUNTIME_AUDIT_STATE["watchlists"]["enabled"] = bool(enabled)
        if not enabled:
            return

        interval_s = int(float(os.getenv("FPS_WATCHLIST_INTERVAL_S", "3600") or 3600))
        interval_s = max(30, interval_s)
        _RUNTIME_AUDIT_STATE["watchlists"]["interval_s"] = int(interval_s)
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

            db_path = os.getenv("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)

            while True:
                tick_started_at = _runtime_now_iso()
                tick_t0 = time.perf_counter()
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

                        _RUNTIME_AUDIT_STATE["watchlists"].update(
                            {
                                "last_started_at": tick_started_at,
                                "last_completed_at": _runtime_now_iso(),
                                "last_duration_ms": int((time.perf_counter() - tick_t0) * 1000),
                                "last_error": None,
                                "tick_count": int(_RUNTIME_AUDIT_STATE["watchlists"].get("tick_count") or 0) + 1,
                                "last_summary": {
                                    "saved_searches_checked": len(ss_rows[:50]),
                                    "saved_searches_inbox_synced": len(ss_rows[:100]),
                                    "counties_with_enabled_searches": len(counties),
                                    "connectors_per_county": len(connector_keys),
                                },
                            }
                        )
                    finally:
                        store.close()
                except Exception as e:
                    _RUNTIME_AUDIT_STATE["watchlists"].update(
                        {
                            "last_started_at": tick_started_at,
                            "last_completed_at": _runtime_now_iso(),
                            "last_duration_ms": int((time.perf_counter() - tick_t0) * 1000),
                            "last_error": str(e),
                            "tick_count": int(_RUNTIME_AUDIT_STATE["watchlists"].get("tick_count") or 0) + 1,
                        }
                    )
                    logger.warning("scheduler tick failed: %s", e)

                await asyncio.sleep(interval_s)

        try:
            _watchlists_scheduler_task["task"] = asyncio.create_task(_loop())
        except Exception as e:
            logger.warning("watchlists scheduler failed to start: %s", e)

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

    @app.on_event("startup")
    async def _start_statewide_refresh_scheduler():
        import asyncio

        enabled = os.getenv("FPS_STATEWIDE_REFRESH_SCHEDULER", "1").strip() == "1"
        _RUNTIME_AUDIT_STATE["statewide_refresh"]["enabled"] = bool(enabled)
        if not enabled:
            return

        interval_s = int(float(os.getenv("FPS_STATEWIDE_REFRESH_INTERVAL_S", "3600") or 3600))
        interval_s = max(30, interval_s)
        _RUNTIME_AUDIT_STATE["statewide_refresh"]["interval_s"] = int(interval_s)
        batch_size = int(float(os.getenv("FPS_STATEWIDE_REFRESH_BATCH_SIZE", "100") or 100))
        max_parcels = int(float(os.getenv("FPS_STATEWIDE_REFRESH_MAX_PARCELS_PER_COUNTY", "1000") or 1000))
        counties_raw = str(os.getenv("FPS_STATEWIDE_REFRESH_COUNTIES", "")).strip()
        county_filter = [c.strip().lower() for c in counties_raw.split(",") if c.strip()] if counties_raw else None

        logger = logging.getLogger("fps.statewide_refresh")
        logger.warning(
            "statewide refresh scheduler enabled: interval_s=%s batch_size=%s max_parcels_per_county=%s counties=%s",
            interval_s,
            batch_size,
            max_parcels,
            county_filter or "all",
        )

        async def _loop():
            from florida_property_scraper.scheduler.statewide_refresh import run_statewide_refresh_tick

            db_path = _resolve_db_path("LEADS_SQLITE_PATH", DEFAULT_LEADS_DB)
            while True:
                tick_started_at = _runtime_now_iso()
                tick_t0 = time.perf_counter()
                try:
                    result = run_statewide_refresh_tick(
                        db_path=db_path,
                        counties=county_filter,
                        batch_size=batch_size,
                        max_parcels_per_county=max_parcels,
                        min_confidence_label=str(os.getenv("FPS_EVIDENCE_MIN_CONFIDENCE", "low") or "low"),
                    )
                    stats = result.get("stats") if isinstance(result, dict) else {}
                    logger.warning(
                        "statewide refresh tick: counties=%s parcels=%s evidence=%s triggers=%s",
                        (stats or {}).get("counties_processed", 0),
                        (stats or {}).get("parcels_considered", 0),
                        (stats or {}).get("evidence_rows_written", 0),
                        (stats or {}).get("trigger_results_written", 0),
                    )
                    _RUNTIME_AUDIT_STATE["statewide_refresh"].update(
                        {
                            "last_started_at": tick_started_at,
                            "last_completed_at": _runtime_now_iso(),
                            "last_duration_ms": int((time.perf_counter() - tick_t0) * 1000),
                            "last_error": None,
                            "tick_count": int(_RUNTIME_AUDIT_STATE["statewide_refresh"].get("tick_count") or 0) + 1,
                            "preloaded_once": True,
                            "last_summary": {
                                "counties_processed": int((stats or {}).get("counties_processed", 0) or 0),
                                "parcels_considered": int((stats or {}).get("parcels_considered", 0) or 0),
                                "parcels_enriched": int((stats or {}).get("parcels_enriched", 0) or 0),
                                "evidence_rows_written": int((stats or {}).get("evidence_rows_written", 0) or 0),
                                "trigger_results_written": int((stats or {}).get("trigger_results_written", 0) or 0),
                            },
                        }
                    )
                except Exception as e:
                    _RUNTIME_AUDIT_STATE["statewide_refresh"].update(
                        {
                            "last_started_at": tick_started_at,
                            "last_completed_at": _runtime_now_iso(),
                            "last_duration_ms": int((time.perf_counter() - tick_t0) * 1000),
                            "last_error": str(e),
                            "tick_count": int(_RUNTIME_AUDIT_STATE["statewide_refresh"].get("tick_count") or 0) + 1,
                        }
                    )
                    logger.warning("statewide refresh tick failed: %s", e)

                await asyncio.sleep(interval_s)

        try:
            _statewide_refresh_scheduler_task["task"] = asyncio.create_task(_loop())
        except Exception as e:
            logger.warning("statewide refresh scheduler failed to start: %s", e)

    @app.on_event("shutdown")
    async def _stop_statewide_refresh_scheduler():
        import asyncio

        t = _statewide_refresh_scheduler_task.get("task")
        if t is not None:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    @app.on_event("shutdown")
    async def _log_shutdown_event():
        try:
            record_shutdown_event("fastapi_shutdown")
        except Exception:
            pass

# --- alias: county in path ---
@app.get("/api/parcels/{county}/{parcel_id}/detail")
def api_parcel_detail_alias(county: str, parcel_id: str):
    hover = locals().get('hover') or locals().get('hover_fields') or locals().get('hover_data') or {}
    return RedirectResponse(url=f"/api/parcels/{parcel_id}?county={county}", status_code=307)


# --- ensure parcel dynamic routes are last ---
try:
    routes = app.router.routes
    dynamic_routes = []
    for r in list(routes):
        path = getattr(r, "path", "") or ""
        if path.startswith("/api/parcels/") and "{" in path:
            dynamic_routes.append(r)

    if dynamic_routes:
        for r in dynamic_routes:
            try:
                routes.remove(r)
            except ValueError:
                continue
        insert_at = 0
        for i, r in enumerate(list(routes)):
            path = getattr(r, "path", "") or ""
            if path.startswith("/api/parcels/"):
                insert_at = i + 1
        for idx, r in enumerate(dynamic_routes):
            routes.insert(insert_at + idx, r)
except Exception:
    pass


# --- ensure spa_fallback is last ---
try:
    routes = app.router.routes  # FastAPI / Starlette router
    idx = None
    for i, r in enumerate(list(routes)):
        if getattr(r, "name", "") == "spa_fallback" or getattr(r, "path", "") == "/{full_path:path}":
            idx = i
            break
    if idx is not None:
        routes.append(routes.pop(idx))
except Exception:
    pass


# --- ensure spa_fallback is last ---
try:
    routes = app.router.routes  # FastAPI / Starlette router
    idx = None
    for i, r in enumerate(list(routes)):
        if getattr(r, "name", "") == "spa_fallback" or getattr(r, "path", "") == "/{full_path:path}":
            idx = i
            break
    if idx is not None:
        routes.append(routes.pop(idx))
except Exception:
    pass
