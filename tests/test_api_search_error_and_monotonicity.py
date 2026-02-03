import os

import pytest
from fastapi.testclient import TestClient

from florida_property_scraper.api.app import app


@pytest.mark.parametrize("error_message", ["boom"])
def test_search_returns_json_on_error(tmp_path, monkeypatch, error_message):
    if app is None:
        return

    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)
    monkeypatch.setenv("PA_DB", str(tmp_path / "leads.sqlite"))

    from florida_property_scraper.parcels import geometry_search as geom

    def _boom(*_args, **_kwargs):
        raise RuntimeError(error_message)

    monkeypatch.setattr(geom, "geometry_bbox", _boom)

    client = TestClient(app)
    poly = {
        "type": "Polygon",
        "coordinates": [
            [
                [-81.371, 28.6495],
                [-81.367, 28.6495],
                [-81.367, 28.6525],
                [-81.371, 28.6525],
                [-81.371, 28.6495],
            ]
        ],
    }

    resp = client.post("/api/parcels/search", json={"county": "seminole", "geometry": poly})
    assert resp.status_code >= 400
    payload = resp.json()
    err = payload.get("error") or {}
    assert err.get("message")
    assert err.get("type")


def test_filters_monotonicity(tmp_path, monkeypatch):
    if app is None:
        return

    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("PA_DB", str(db_path))

    from florida_property_scraper.pa.normalize import apply_defaults
    from florida_property_scraper.pa.storage import PASQLite

    store = PASQLite(str(db_path))
    try:
        store.upsert(
            apply_defaults(
                {
                    "county": "seminole",
                    "parcel_id": "SEM-0001",
                    "situs_address": "100 E SAMPLE ST",
                    "owner_names": ["DEMO OWNER"],
                    "living_sf": 1500,
                }
            )
        )
    finally:
        store.close()

    client = TestClient(app)
    poly = {
        "type": "Polygon",
        "coordinates": [
            [
                [-81.371, 28.6495],
                [-81.367, 28.6495],
                [-81.367, 28.6525],
                [-81.371, 28.6525],
                [-81.371, 28.6495],
            ]
        ],
    }

    r_base = client.post(
        "/api/parcels/search",
        json={"county": "seminole", "geometry": poly, "limit": 50},
    )
    assert r_base.status_code == 200
    base_count = len(r_base.json().get("records") or [])
    assert base_count > 0

    r_flt = client.post(
        "/api/parcels/search",
        json={
            "county": "seminole",
            "geometry": poly,
            "limit": 50,
            "filters": {"min_sqft": 2000},
        },
    )
    assert r_flt.status_code == 200
    flt_count = len(r_flt.json().get("records") or [])
    assert flt_count <= base_count
