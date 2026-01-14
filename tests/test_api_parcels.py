import os

from florida_property_scraper.api.app import app


def test_api_parcels_zoom_gating(tmp_path, monkeypatch):
    if app is None:
        return

    # Point parcel geojson dir to fixtures.
    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    # Isolate PA DB for the API (list endpoint batches hover fields).
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
                    "last_sale_date": "2024-06-01",
                    "last_sale_price": 2500000,
                }
            )
        )
    finally:
        store.close()

    from fastapi.testclient import TestClient

    client = TestClient(app)

    bbox = "-81.38,28.64,-81.36,28.66"

    r = client.get(
        "/api/parcels", params={"county": "seminole", "bbox": bbox, "zoom": 14}
    )
    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert data["features"] == []

    # Use a bbox that intersects the seminole fixtures.
    bbox = "-81.38,28.64,-81.36,28.66"
    r = client.get(
        "/api/parcels", params={"county": "seminole", "bbox": bbox, "zoom": 15}
    )
    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert isinstance(data["features"], list)
    assert len(data["features"]) >= 1

    allowed = {
        "parcel_id",
        "situs_address",
        "owner_name",
        "last_sale_date",
        "last_sale_price",
        "mortgage_amount",
    }

    for feat in data["features"]:
        assert feat["type"] == "Feature"
        assert isinstance(feat.get("id"), str)
        assert str(feat["id"]).startswith("seminole:")
        assert "geometry" in feat
        assert "properties" in feat
        props = feat["properties"]
        assert set(props.keys()) == allowed

        # Confirm we hydrate from PA when present.
        if props["parcel_id"] == "SEM-0001":
            assert props["situs_address"] == "100 E SAMPLE ST"
            assert props["owner_name"] == "DEMO OWNER"
            assert props["last_sale_date"] == "2024-06-01"
            assert props["last_sale_price"] == 2500000.0
            assert props["mortgage_amount"] is None


def test_api_parcels_county_switch_and_default(tmp_path, monkeypatch):
    if app is None:
        return

    # Point parcel geojson dir to fixtures.
    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    # Isolate PA DB for API.
    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("PA_DB", str(db_path))

    from fastapi.testclient import TestClient

    client = TestClient(app)

    # Default county is seminole when omitted.
    seminole_bbox = "-81.38,28.64,-81.36,28.66"
    r = client.get("/api/parcels", params={"bbox": seminole_bbox, "zoom": 15})
    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) >= 1
    assert str(data["features"][0].get("id", "")).startswith("seminole:")

    # Switching county to orange returns orange features in orange bbox.
    orange_bbox = "-81.312,28.535,-81.301,28.543"
    r = client.get(
        "/api/parcels", params={"county": "orange", "bbox": orange_bbox, "zoom": 15}
    )
    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) >= 1
    assert str(data["features"][0].get("id", "")).startswith("orange:")


def test_api_parcel_hover_contract(tmp_path, monkeypatch):
    if app is None:
        return

    from fastapi.testclient import TestClient

    # Build a PA DB with a single record.
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
                    "last_sale_date": "2024-06-01",
                    "last_sale_price": 2500000,
                }
            )
        )
    finally:
        store.close()

    client = TestClient(app)
    r = client.get("/api/parcels/seminole/SEM-0001/hover")
    assert r.status_code == 200
    data = r.json()

    assert set(data.keys()) == {
        "parcel_id",
        "county",
        "situs_address",
        "owner_name",
        "last_sale_date",
        "last_sale_price",
        "mortgage_amount",
        "mortgage_lender",
    }

    assert data["parcel_id"] == "SEM-0001"
    assert data["county"] == "seminole"
    assert data["situs_address"] == "100 E SAMPLE ST"
    assert data["owner_name"] == "DEMO OWNER"
    assert data["last_sale_date"] == "2024-06-01"
    assert data["last_sale_price"] == 2500000.0

    # Mortgage fields must remain blank/0 unless PA provides them.
    assert data["mortgage_amount"] is None
    assert data["mortgage_lender"] == ""


def test_api_parcel_detail_includes_pa_and_user_meta(tmp_path, monkeypatch):
    if app is None:
        return

    from fastapi.testclient import TestClient

    # Isolate PA DB and user-meta DB.
    db_path = tmp_path / "leads.sqlite"
    user_db = tmp_path / "user_meta.sqlite"
    monkeypatch.setenv("PA_DB", str(db_path))
    monkeypatch.setenv("USER_META_DB", str(user_db))

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
                    "last_sale_date": "2024-06-01",
                    "last_sale_price": 2500000,
                }
            )
        )
    finally:
        store.close()

    client = TestClient(app)
    r = client.get("/api/parcels/SEM-0001", params={"county": "seminole"})
    assert r.status_code == 200
    data = r.json()

    assert set(data.keys()) == {"county", "parcel_id", "pa", "computed", "user_meta"}
    assert data["county"] == "seminole"
    assert data["parcel_id"] == "SEM-0001"
    assert isinstance(data["pa"], dict)
    assert isinstance(data["computed"], dict)
    assert data["pa"]["parcel_id"] == "SEM-0001"
    assert data["pa"]["county"] == "seminole"
    assert isinstance(data["user_meta"], dict)
    assert data["user_meta"]["parcel_id"] == "SEM-0001"
    assert data["user_meta"]["starred"] is False


def test_api_parcels_search_polygon_and_radius(tmp_path, monkeypatch):
    if app is None:
        return

    # Point parcel geojson dir to fixtures.
    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    # Isolate PA DB for search hover fields.
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
                    "last_sale_date": "2024-06-01",
                    "last_sale_price": 2500000,
                }
            )
        )
    finally:
        store.close()

    from fastapi.testclient import TestClient
    from florida_property_scraper.parcels.geometry_search import circle_polygon

    client = TestClient(app)

    # Polygon tightly around SEM-0001 fixture.
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

    r = client.post(
        "/api/parcels/search",
        json={"county": "seminole", "geometry": poly, "limit": 50},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["county"] == "seminole"
    assert isinstance(data["results"], list)
    assert any(row["parcel_id"] == "SEM-0001" for row in data["results"])

    # Radius search should match an equivalent circle polygon.
    circle = circle_polygon(center_lon=-81.369, center_lat=28.651, miles=0.25)
    r_circle = client.post(
        "/api/parcels/search",
        json={"county": "seminole", "geometry": circle, "limit": 50},
    )
    assert r_circle.status_code == 200
    poly_rows = {row["parcel_id"] for row in r_circle.json()["results"]}

    r_radius = client.post(
        "/api/parcels/search",
        json={
            "county": "seminole",
            "radius": {"center": [-81.369, 28.651], "miles": 0.25},
            "limit": 50,
        },
    )
    assert r_radius.status_code == 200
    radius_rows = {row["parcel_id"] for row in r_radius.json()["results"]}

    assert radius_rows == poly_rows


def test_api_parcels_search_trigger_unknown_field_never_matches(tmp_path, monkeypatch):
    if app is None:
        return

    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("PA_DB", str(db_path))

    from fastapi.testclient import TestClient

    client = TestClient(app)

    # Geometry that intersects SEM-0001.
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

    # Trigger references a non-existent field -> must not match.
    payload = {
        "county": "seminole",
        "geometry": poly,
        "triggers": [
            {
                "code": "SHOULD_NOT_MATCH",
                "all": [{"field": "nonexistent_pa_field", "op": "equals", "value": 1}],
            }
        ],
    }
    r = client.post("/api/parcels/search", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 0


def test_api_parcels_search_filters_object(tmp_path, monkeypatch):
    if app is None:
        return

    # Point parcel geojson dir to fixtures.
    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    # Isolate PA DB.
    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("PA_DB", str(db_path))

    from florida_property_scraper.pa.normalize import apply_defaults
    from florida_property_scraper.pa.storage import PASQLite

    store = PASQLite(str(db_path))
    try:
        # Two parcels inside the seminole fixture.
        store.upsert(
            apply_defaults(
                {
                    "county": "seminole",
                    "parcel_id": "SEM-0001",
                    "situs_address": "100 E SAMPLE ST",
                    "owner_names": ["OWNER 1"],
                    "zoning": "R-1",
                    "use_type": "Residential",
                    "year_built": 2005,
                    "living_sf": 2500,
                    "bedrooms": 4,
                    "bathrooms": 2.5,
                    "just_value": 450000,
                    "land_value": 120000,
                    "improvement_value": 330000,
                    "last_sale_date": "2020-01-15",
                }
            )
        )
        store.upsert(
            apply_defaults(
                {
                    "county": "seminole",
                    "parcel_id": "SEM-0002",
                    "situs_address": "200 E SAMPLE ST",
                    "owner_names": ["OWNER 2"],
                    "zoning": "C-2",
                    "use_type": "Commercial",
                    "year_built": 1985,
                    "living_sf": 1200,
                    "bedrooms": 2,
                    "bathrooms": 1.0,
                    "just_value": 250000,
                    "land_value": 90000,
                    "improvement_value": 160000,
                    "last_sale_date": "2010-05-01",
                }
            )
        )
    finally:
        store.close()

    from fastapi.testclient import TestClient

    client = TestClient(app)

    # Polygon covering both SEM-0001 and SEM-0002 fixtures.
    poly = {
        "type": "Polygon",
        "coordinates": [
            [
                [-81.372, 28.647],
                [-81.362, 28.647],
                [-81.362, 28.653],
                [-81.372, 28.653],
                [-81.372, 28.647],
            ]
        ],
    }

    payload = {
        "county": "seminole",
        "geometry": poly,
        "limit": 50,
        "filters": {
            "min_sqft": 2000,
            "min_beds": 3,
            "min_baths": 2,
            "property_type": "residential",
            "min_value": 400000,
            "last_sale_date_start": "2015-01-01",
        },
    }
    r = client.post("/api/parcels/search", json=payload)
    assert r.status_code == 200
    data = r.json()

    rec_ids = {row["parcel_id"] for row in data.get("records") or []}
    assert rec_ids == {"SEM-0001"}


def test_api_parcel_meta_roundtrip(tmp_path, monkeypatch):
    if app is None:
        return

    from fastapi.testclient import TestClient

    db_path = tmp_path / "leads.sqlite"
    user_db = tmp_path / "user_meta.sqlite"
    monkeypatch.setenv("PA_DB", str(db_path))
    monkeypatch.setenv("USER_META_DB", str(user_db))

    client = TestClient(app)

    payload = {
        "starred": True,
        "tags": ["warm"],
        "notes": "call next week",
        "lists": ["followup"],
    }
    r = client.put(
        "/api/parcels/SEM-0001/meta", params={"county": "seminole"}, json=payload
    )
    assert r.status_code == 200
    saved = r.json()
    assert saved["county"] == "seminole"
    assert saved["parcel_id"] == "SEM-0001"
    assert saved["starred"] is True
    assert saved["tags"] == ["warm"]
    assert saved["notes"] == "call next week"
    assert saved["lists"] == ["followup"]

    r = client.get("/api/parcels/SEM-0001/meta", params={"county": "seminole"})
    assert r.status_code == 200
    loaded = r.json()
    assert loaded["starred"] is True
    assert loaded["tags"] == ["warm"]
    assert loaded["notes"] == "call next week"
    assert loaded["lists"] == ["followup"]
