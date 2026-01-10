from __future__ import annotations

import os

import pytest


def _reset_flags(monkeypatch, **env):
    from florida_property_scraper.feature_flags import reset_flags_cache

    for k, v in env.items():
        if v is None:
            monkeypatch.delenv(k, raising=False)
        else:
            monkeypatch.setenv(k, str(v))
    reset_flags_cache()


def test_flag_strict_schema_validation_toggles_native_engine(monkeypatch):
    from florida_property_scraper.backend.native.engine import NativeEngine

    url = "http://example.local/page"
    start_requests = [{"url": url, "method": "GET"}]

    def bad_parser(_html: str, _final_url: str, _county: str):
        return [
            {
                "county": "seminole",
                "state": "fl",
                "jurisdiction": "seminole",
                "owner": "",
                "address": "",
                "raw_html": "",
            }
        ]

    engine = NativeEngine()

    # Default: strict validation OFF -> drops invalid records, no raise.
    _reset_flags(monkeypatch, FPS_FEATURE_STRICT_SCHEMA_VALIDATION=None)
    out = list(
        engine.iter_records(
            start_requests,
            bad_parser,
            "seminole",
            allowed_hosts=None,
            log_fn=None,
            dry_run=True,
            fixture_map={url: "<html></html>"},
        )
    )
    out = [r for r in out if "__summary__" not in r]
    assert out == []

    # Strict validation ON -> raises.
    _reset_flags(monkeypatch, FPS_FEATURE_STRICT_SCHEMA_VALIDATION="1")
    with pytest.raises(ValueError):
        list(
            engine.iter_records(
                start_requests,
                bad_parser,
                "seminole",
                allowed_hosts=None,
                log_fn=None,
                dry_run=True,
                fixture_map={url: "<html></html>"},
            )
        )


def test_flag_geometry_search_disables_endpoint(monkeypatch, tmp_path):
    from florida_property_scraper.api.app import app

    if app is None:
        pytest.skip("fastapi not installed")

    _reset_flags(monkeypatch, FPS_FEATURE_GEOMETRY_SEARCH="0")

    # Ensure parcel geojson uses fixtures.
    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    # Isolate PA DB.
    monkeypatch.setenv("PA_DB", str(tmp_path / "leads.sqlite"))

    from fastapi.testclient import TestClient

    client = TestClient(app)
    payload = {
        "county": "seminole",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [0, 0], [0, 0], [0, 0]]],
        },
    }
    r = client.post("/api/parcels/search", json=payload)
    assert r.status_code == 404


def test_flag_triggers_toggles_filtering(monkeypatch, tmp_path):
    from florida_property_scraper.api.app import app

    if app is None:
        pytest.skip("fastapi not installed")

    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)
    monkeypatch.setenv("PA_DB", str(tmp_path / "leads.sqlite"))

    from florida_property_scraper.pa.normalize import apply_defaults
    from florida_property_scraper.pa.storage import PASQLite

    store = PASQLite(os.getenv("PA_DB"))
    try:
        store.upsert(
            apply_defaults(
                {
                    "county": "seminole",
                    "parcel_id": "SEM-0001",
                    "situs_address": "100 E SAMPLE ST",
                    "owner_names": ["DEMO OWNER"],
                    "last_sale_date": "2024-06-01",
                    "last_sale_price": 2500000,
                }
            )
        )
    finally:
        store.close()

    from fastapi.testclient import TestClient

    client = TestClient(app)
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-81.371, 28.649],
                [-81.367, 28.649],
                [-81.367, 28.653],
                [-81.371, 28.653],
                [-81.371, 28.649],
            ]
        ],
    }
    payload = {
        "county": "seminole",
        "geometry": geometry,
        # A trigger that will NOT match (price threshold too high).
        "triggers": [
            {
                "code": "TOO_EXPENSIVE",
                "all": [{"field": "last_sale_price", "op": ">", "value": 9_999_999}],
            }
        ],
        "limit": 10,
    }

    # Triggers enabled: requires a match -> empty.
    _reset_flags(monkeypatch, FPS_FEATURE_TRIGGERS="1")
    r = client.post("/api/parcels/search", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 0

    # Triggers disabled: triggers ignored -> returns results.
    _reset_flags(monkeypatch, FPS_FEATURE_TRIGGERS="0")
    r = client.post("/api/parcels/search", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["count"] >= 1
    assert data["results"][0]["reason_codes"] == []


def test_flag_sale_filtering_toggles_filter_eval(monkeypatch, tmp_path):
    from florida_property_scraper.api.app import app

    if app is None:
        pytest.skip("fastapi not installed")

    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)
    monkeypatch.setenv("PA_DB", str(tmp_path / "leads.sqlite"))

    from florida_property_scraper.pa.normalize import apply_defaults
    from florida_property_scraper.pa.storage import PASQLite

    store = PASQLite(os.getenv("PA_DB"))
    try:
        store.upsert(
            apply_defaults(
                {
                    "county": "seminole",
                    "parcel_id": "SEM-0001",
                    "situs_address": "100 E SAMPLE ST",
                    "owner_names": ["DEMO OWNER"],
                    "last_sale_date": "2024-06-01",
                    "last_sale_price": 2500000,
                }
            )
        )
    finally:
        store.close()

    from fastapi.testclient import TestClient

    client = TestClient(app)
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-81.371, 28.649],
                [-81.367, 28.649],
                [-81.367, 28.653],
                [-81.371, 28.653],
                [-81.371, 28.649],
            ]
        ],
    }
    payload = {
        "county": "seminole",
        "geometry": geometry,
        "filters": [{"field": "last_sale_price", "op": ">", "value": 1000}],
        "limit": 10,
    }

    _reset_flags(monkeypatch, FPS_FEATURE_SALE_FILTERING="1")
    r = client.post("/api/parcels/search", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["count"] >= 1

    _reset_flags(monkeypatch, FPS_FEATURE_SALE_FILTERING="0")
    r = client.post("/api/parcels/search", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 0
