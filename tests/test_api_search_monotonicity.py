import os

import pytest
from fastapi.testclient import TestClient

from florida_property_scraper.api.app import app


@pytest.mark.parametrize(
    "filters",
    [
        {"min_sqft": 1000},
        {"min_year_built": 2000},
        {"min_beds": 3},
        {"min_baths": 2},
        {"min_lot_size_sqft": 2000},
    ],
)
def test_filters_monotonicity(tmp_path, monkeypatch, filters):
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
                    "bedrooms": 3,
                    "bathrooms": 2,
                    "year_built": 2005,
                    "land_sf": 3000,
                    "just_value": 250000,
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

    base = client.post(
        "/api/parcels/search",
        json={"county": "seminole", "geometry": poly, "limit": 50, "explain": True},
    )
    assert base.status_code == 200
    base_count = len(base.json().get("records") or [])
    assert base_count > 0

    filt = client.post(
        "/api/parcels/search",
        json={
            "county": "seminole",
            "geometry": poly,
            "limit": 50,
            "filters": filters,
            "explain": True,
        },
    )
    assert filt.status_code == 200
    filt_count = len(filt.json().get("records") or [])
    assert filt_count <= base_count
