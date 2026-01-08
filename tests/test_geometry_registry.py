from florida_property_scraper.parcels.geometry_registry import get_provider


def test_geometry_registry_returns_provider_for_seminole():
    provider = get_provider("seminole")
    assert provider is not None
    assert getattr(provider, "county", "") == "seminole"


def test_geometry_registry_returns_provider_for_orange():
    provider = get_provider("orange")
    assert provider is not None
    assert getattr(provider, "county", "") == "orange"


def test_geometry_registry_caches_provider_instances():
    p1 = get_provider("seminole")
    p2 = get_provider("seminole")
    assert p1 is p2

    o1 = get_provider("orange")
    o2 = get_provider("orange")
    assert o1 is o2


def test_geometry_registry_unknown_county_does_not_crash():
    provider = get_provider("does_not_exist")
    assert provider is not None


def test_geometry_provider_index_builds_once_per_process(monkeypatch):
    import os

    # Ensure a clean cache so we can observe build counts.
    get_provider.cache_clear()

    repo_root = os.path.dirname(os.path.dirname(__file__))
    fixtures_dir = os.path.join(repo_root, "tests", "fixtures", "parcels")
    monkeypatch.setenv("PARCEL_GEOJSON_DIR", fixtures_dir)

    p1 = get_provider("seminole")
    assert getattr(p1, "_builds", 0) == 1

    # Second call should return the same provider and should not rebuild its index.
    p2 = get_provider("seminole")
    assert p1 is p2
    assert getattr(p2, "_builds", 0) == 1
