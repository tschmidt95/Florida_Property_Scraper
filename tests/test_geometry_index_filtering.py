import os


from florida_property_scraper.parcels.geometry_registry import get_provider


def test_spatial_index_filters_to_bbox(monkeypatch):
    # Use the repo fixture GeoJSON as the provider data source.
    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    provider = get_provider("seminole")

    # Bbox around SEM-0001 only.
    features = provider.query((-81.371, 28.6495, -81.367, 28.6525))
    parcel_ids = {f.parcel_id for f in features}
    assert "SEM-0001" in parcel_ids
    assert "SEM-0002" not in parcel_ids

    # Bbox around both.
    features = provider.query((-81.372, 28.647, -81.362, 28.653))
    parcel_ids = {f.parcel_id for f in features}
    assert {"SEM-0001", "SEM-0002"}.issubset(parcel_ids)


def test_spatial_index_filters_to_bbox_orange(monkeypatch):
    # Use the repo fixture GeoJSON as the provider data source.
    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    provider = get_provider("orange")

    # Bbox around ORA-0001 only.
    features = provider.query((-81.311, 28.5395, -81.307, 28.5425))
    parcel_ids = {f.parcel_id for f in features}
    assert "ORA-0001" in parcel_ids
    assert "ORA-0002" not in parcel_ids

    # Bbox around both.
    features = provider.query((-81.312, 28.535, -81.301, 28.543))
    parcel_ids = {f.parcel_id for f in features}
    assert {"ORA-0001", "ORA-0002"}.issubset(parcel_ids)
