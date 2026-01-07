from florida_property_scraper.api.app import app, counties, health


def test_api_counties_and_health_routes_exist():
    paths = {route.path for route in app.routes}
    assert "/counties" in paths
    assert "/health" in paths


def test_api_counties_and_health_functions():
    assert health() == {"status": "ok"}
    result = counties(state="fl")
    assert isinstance(result, list)
