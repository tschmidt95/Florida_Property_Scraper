from florida_property_scraper.api.app import app, counties, health


def test_api_exports_exist():
    assert callable(health)
    assert callable(counties)


def test_api_behavior_without_fastapi():
    assert health().get("status") == "ok"
    payload = counties()
    assert isinstance(payload, dict)
    assert "counties" in payload
    assert isinstance(payload["counties"], list)


def test_api_routes_when_fastapi_present():
    if app is None:
        return
    paths = {route.path for route in app.router.routes}
    assert "/health" in paths
    assert "/counties" in paths
    assert "/api/triggers/by_parcel" in paths
