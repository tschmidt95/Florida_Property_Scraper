import json
import os
import sqlite3

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
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("PARCELS_DB_PATH", str(tmp_path / "missing-parcels.sqlite"))

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
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("PARCELS_DB_PATH", str(tmp_path / "missing-parcels.sqlite"))

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

    for key in (
        "county",
        "parcel_id",
        "pa",
        "computed",
        "user_meta",
        "merged_fields",
        "evidence_ids",
        "owner_enrichment",
        "property_profile",
        "coverage",
    ):
        assert key in data
    assert data["county"] == "seminole"
    assert data["parcel_id"] == "SEM-0001"
    assert isinstance(data["pa"], dict)
    assert isinstance(data["computed"], dict)
    assert data["pa"]["parcel_id"] == "SEM-0001"
    assert data["pa"]["county"] == "seminole"
    assert isinstance(data["user_meta"], dict)
    assert data["user_meta"]["parcel_id"] == "SEM-0001"
    assert data["user_meta"]["starred"] is False
    assert isinstance(data["property_profile"], dict)
    assert isinstance(data["property_profile"].get("canonical"), dict)
    assert isinstance(data["coverage"], dict)
    assert data["coverage"]["required_count"] >= data["coverage"]["present_count"]


def test_api_parcel_detail_builds_snapshot_fallback_and_ui_aliases(tmp_path, monkeypatch):
    if app is None:
        return

    from fastapi.testclient import TestClient
    from florida_property_scraper.pa.normalize import apply_defaults
    from florida_property_scraper.pa.storage import PASQLite
    from florida_property_scraper.storage import SQLiteStore

    pa_db = tmp_path / "pa.sqlite"
    leads_db = tmp_path / "leads.sqlite"
    user_db = tmp_path / "user_meta.sqlite"
    monkeypatch.setenv("PA_DB", str(pa_db))
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(leads_db))
    monkeypatch.setenv("USER_META_DB", str(user_db))
    monkeypatch.setenv("FPS_EVIDENCE_MIN_CONFIDENCE", "low")

    pa_store = PASQLite(str(pa_db))
    try:
        pa_store.upsert(
            apply_defaults(
                {
                    "county": "seminole",
                    "parcel_id": "SEM-ALIAS-1",
                    "situs_address": "1200 TEST ST",
                    "owner_names": ["JANE DOE", "JOHN DOE"],
                    "bedrooms": 3,
                    "bathrooms": 2.5,
                    "living_sf": 1800,
                    "land_sf": 9500,
                    "land_acres": 0.22,
                    "improvement_value": 240000,
                    "just_value": 350000,
                }
            )
        )
    finally:
        pa_store.close()

    leads_store = SQLiteStore(str(leads_db))
    try:
        leads_store.upsert_provider_evidence(
            provider_key="test_provider",
            provider_name="Test Provider",
            county="seminole",
            parcel_id="SEM-ALIAS-1",
            field="owner_email",
            value="owner@example.com",
            confidence=0.9,
            confidence_label="high",
            source_type="test",
            source_url="https://example.invalid/source",
            fetched_at="2026-03-19T00:00:00+00:00",
            retrieved_at="2026-03-19T00:00:00+00:00",
            content_hash=None,
            extract_method="unit",
            raw_reference="unit-test",
            raw_snippet="owner@example.com",
            raw_id=None,
        )
    finally:
        leads_store.close()

    client = TestClient(app)
    r = client.get("/api/parcels/SEM-ALIAS-1", params={"county": "seminole"})
    assert r.status_code == 200
    data = r.json()

    assert data["merged_fields"].get("owner_email") == "owner@example.com"
    canonical = data["property_profile"]["canonical"]
    assert canonical.get("owner_name") == "JANE DOE; JOHN DOE"
    assert canonical.get("beds") == 3
    assert canonical.get("baths") == 2.5
    assert canonical.get("living_area_sqft") == 1800
    assert canonical.get("lot_size_sqft") == 9500
    assert canonical.get("lot_size_acres") == 0.22
    assert canonical.get("building_value") == 240000
    assert canonical.get("total_value") == 350000
    assert canonical.get("owner_email") == "owner@example.com"

    coverage = data.get("coverage") or {}
    missing_fields = set(coverage.get("missing_fields") or [])
    assert "beds" not in missing_fields
    assert "baths" not in missing_fields
    assert "living_area_sqft" not in missing_fields
    assert "lot_size_sqft" not in missing_fields


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


def test_api_parcels_search_text_without_geometry(tmp_path, monkeypatch):
    if app is None:
        return

    # Isolate PA DB.
    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("PA_DB", str(db_path))
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))

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
                    "owner_names": ["OWNER ONE"],
                    "zoning": "R-1",
                    "future_land_use": "RESIDENTIAL",
                }
            )
        )
        store.upsert(
            apply_defaults(
                {
                    "county": "seminole",
                    "parcel_id": "SEM-0002",
                    "situs_address": "200 E SAMPLE ST",
                    "owner_names": ["OWNER TWO"],
                    "zoning": "C-2",
                    "future_land_use": "COMMERCIAL",
                }
            )
        )
    finally:
        store.close()

    # Build a minimal parcels DB so text-only candidate selection can run.
    parcels_db = tmp_path / "parcels.sqlite"
    monkeypatch.setenv("PARCELS_DB_PATH", str(parcels_db))

    con = sqlite3.connect(str(parcels_db))
    try:
        con.execute(
            "CREATE TABLE parcels (parcel_id TEXT, county TEXT, geom_geojson TEXT)"
        )
        con.execute(
            "CREATE VIRTUAL TABLE parcels_rtree USING rtree(rowid, minx, maxx, miny, maxy)"
        )
        con.execute(
            "CREATE TABLE parcels_pa (county TEXT, parcel_id TEXT, owner_name TEXT, situs_address TEXT, mailing_address TEXT)"
        )

        geom1 = '{"type":"Polygon","coordinates":[[[-81.371,28.649],[-81.37,28.649],[-81.37,28.65],[-81.371,28.65],[-81.371,28.649]]]}'
        geom2 = '{"type":"Polygon","coordinates":[[[-81.369,28.651],[-81.368,28.651],[-81.368,28.652],[-81.369,28.652],[-81.369,28.651]]]}'

        con.execute(
            "INSERT INTO parcels(parcel_id, county, geom_geojson) VALUES (?, ?, ?)",
            ("SEM-0001", "seminole", geom1),
        )
        con.execute(
            "INSERT INTO parcels(parcel_id, county, geom_geojson) VALUES (?, ?, ?)",
            ("SEM-0002", "seminole", geom2),
        )
        con.execute(
            "INSERT INTO parcels_rtree(rowid, minx, maxx, miny, maxy) VALUES (1, -81.371, -81.37, 28.649, 28.65)"
        )
        con.execute(
            "INSERT INTO parcels_rtree(rowid, minx, maxx, miny, maxy) VALUES (2, -81.369, -81.368, 28.651, 28.652)"
        )
        con.execute(
            "INSERT INTO parcels_pa(county, parcel_id, owner_name, situs_address, mailing_address) VALUES (?, ?, ?, ?, ?)",
            ("seminole", "SEM-0001", "OWNER ONE", "100 E SAMPLE ST", "ORLANDO, FL"),
        )
        con.execute(
            "INSERT INTO parcels_pa(county, parcel_id, owner_name, situs_address, mailing_address) VALUES (?, ?, ?, ?, ?)",
            ("seminole", "SEM-0002", "OWNER TWO", "200 E SAMPLE ST", "ORLANDO, FL"),
        )
        con.commit()
    finally:
        con.close()

    from fastapi.testclient import TestClient

    client = TestClient(app)

    # Owner text should work without geometry.
    r_owner = client.post(
        "/api/parcels/search",
        json={
            "county": "seminole",
            "limit": 50,
            "filters": {"search_text": "owner two", "missing_policy": "strict"},
        },
    )
    assert r_owner.status_code == 200
    owner_ids = {row["parcel_id"] for row in (r_owner.json().get("records") or [])}
    assert owner_ids == {"SEM-0002"}

    # Non-address parameter should also be searchable via the same bar/filter.
    r_zoning = client.post(
        "/api/parcels/search",
        json={
            "county": "seminole",
            "limit": 50,
            "filters": {"search_text": "c-2", "missing_policy": "strict"},
        },
    )
    assert r_zoning.status_code == 200
    zoning_ids = {row["parcel_id"] for row in (r_zoning.json().get("records") or [])}
    assert zoning_ids == {"SEM-0002"}


def test_api_parcels_search_completeness_gate_low_coverage(tmp_path, monkeypatch):
    if app is None:
        return

    # Minimal leads DB path (not used heavily by this test, but required by app env).
    leads_db = tmp_path / "leads.sqlite"
    monkeypatch.setenv("PA_DB", str(leads_db))
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(leads_db))

    # Build a parcels DB with enough records to trigger completeness gate evaluation.
    parcels_db = tmp_path / "parcels.sqlite"
    monkeypatch.setenv("PARCELS_DB_PATH", str(parcels_db))

    con = sqlite3.connect(str(parcels_db))
    try:
        con.execute("CREATE TABLE parcels (parcel_id TEXT, county TEXT, geom_geojson TEXT)")
        con.execute("CREATE VIRTUAL TABLE parcels_rtree USING rtree(rowid, minx, maxx, miny, maxy)")
        con.execute(
            "CREATE TABLE parcels_pa (county TEXT, parcel_id TEXT, owner_name TEXT, situs_address TEXT, mailing_address TEXT)"
        )

        for idx in range(1, 31):
            pid = f"SEM-{idx:04d}"
            minx = -81.40 + (idx * 0.001)
            maxx = minx + 0.0005
            miny = 28.60 + (idx * 0.001)
            maxy = miny + 0.0005
            geom = json.dumps(
                {
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
            )
            con.execute(
                "INSERT INTO parcels(parcel_id, county, geom_geojson) VALUES (?, ?, ?)",
                (pid, "seminole", geom),
            )
            con.execute(
                "INSERT INTO parcels_rtree(rowid, minx, maxx, miny, maxy) VALUES (?, ?, ?, ?, ?)",
                (idx, minx, maxx, miny, maxy),
            )
            # Intentionally leave mailing blank to force low mailing coverage.
            con.execute(
                "INSERT INTO parcels_pa(county, parcel_id, owner_name, situs_address, mailing_address) VALUES (?, ?, ?, ?, ?)",
                ("seminole", pid, f"OWNER {idx}", f"{idx} SAMPLE ST", ""),
            )

        con.commit()
    finally:
        con.close()

    from fastapi.testclient import TestClient

    client = TestClient(app)
    poly = {
        "type": "Polygon",
        "coordinates": [
            [
                [-81.45, 28.55],
                [-81.30, 28.55],
                [-81.30, 28.70],
                [-81.45, 28.70],
                [-81.45, 28.55],
            ]
        ],
    }
    r = client.post(
        "/api/parcels/search",
        json={
            "county": "seminole",
            "limit": 100,
            "geometry": poly,
            "filters": {"missing_policy": "strict"},
        },
    )
    assert r.status_code == 200
    data = r.json()

    gate = ((data.get("data_quality") or {}).get("completeness_gate") or {})
    assert gate.get("status") == "fail"
    failed_fields = {str(x.get("field")) for x in (gate.get("failed_checks") or [])}
    assert "owner_mailing_address" in failed_fields
    assert "total_value" in failed_fields
    warnings = data.get("warnings") or []
    assert any(str(w).startswith("completeness_low:owner_mailing_address:") for w in warnings)


def test_api_parcels_search_options_complete_for_scope(tmp_path, monkeypatch):
    if app is None:
        return

    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("PA_DB", str(db_path))
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("PARCELS_DB_PATH", str(tmp_path / "missing-parcels.sqlite"))

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
                    "owner_names": ["OWNER 1"],
                    "zoning": "R-1",
                    "future_land_use": "RESIDENTIAL",
                    "living_sf": 2200,
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
                    "future_land_use": "COMMERCIAL",
                    "living_sf": 1300,
                }
            )
        )
    finally:
        store.close()

    from fastapi.testclient import TestClient

    client = TestClient(app)
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
            "zoning_in": ["R-1"],
            "missing_policy": "strict",
        },
    }
    r = client.post("/api/parcels/search", json=payload)
    assert r.status_code == 200
    data = r.json()

    # Options should represent the full geometry/county scope, not only the filtered subset.
    zoning_options = set(data.get("zoning_options") or [])
    flu_options = set(data.get("future_land_use_options") or [])
    assert "R-1" in zoning_options
    assert "C-2" in zoning_options
    assert "RESIDENTIAL" in flu_options
    assert "COMMERCIAL" in flu_options


def test_api_parcels_search_filter_application_and_stable_counts(tmp_path, monkeypatch):
    if app is None:
        return

    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    db_path = tmp_path / "leads.sqlite"
    monkeypatch.setenv("PA_DB", str(db_path))
    monkeypatch.setenv("LEADS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("PARCELS_DB_PATH", str(tmp_path / "missing-parcels.sqlite"))

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
                    "owner_names": ["OWNER 1"],
                    "zoning": "R-1",
                    "future_land_use": "RESIDENTIAL",
                    "living_sf": 2400,
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
                    "future_land_use": "COMMERCIAL",
                    "living_sf": 1200,
                }
            )
        )
    finally:
        store.close()

    from fastapi.testclient import TestClient

    client = TestClient(app)
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

    baseline = client.post(
        "/api/parcels/search",
        json={"county": "seminole", "geometry": poly, "limit": 50},
    )
    assert baseline.status_code == 200
    baseline_data = baseline.json()
    baseline_summary = baseline_data.get("summary") or {}
    baseline_ids = {row.get("parcel_id") for row in (baseline_data.get("records") or [])}
    assert {"SEM-0001", "SEM-0002"}.issubset(baseline_ids)
    assert int(baseline_summary.get("candidate_count") or 0) >= 2
    assert int(baseline_summary.get("filtered_count") or 0) >= 2

    filtered = client.post(
        "/api/parcels/search",
        json={
            "county": "seminole",
            "geometry": poly,
            "limit": 50,
            "filters": {
                "zoning_in": ["R-1"],
                "missing_policy": "strict",
            },
        },
    )
    assert filtered.status_code == 200
    filtered_data = filtered.json()
    filtered_summary = filtered_data.get("summary") or {}
    filtered_ids = {row.get("parcel_id") for row in (filtered_data.get("records") or [])}

    assert filtered_ids == {"SEM-0001"}

    # Candidate scope should stay stable (geometry scope); filtered totals should narrow.
    assert int(filtered_summary.get("candidate_count") or 0) == int(baseline_summary.get("candidate_count") or 0)
    assert int(filtered_summary.get("filtered_count") or 0) == 1
    assert int(filtered_summary.get("total_count") or 0) == 1
    assert int(filtered_data.get("total_count") or 0) == 1


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
