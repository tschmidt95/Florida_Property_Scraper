from florida_property_scraper.api.app import app


def test_api_routes_exist():
    paths = {route.path for route in app.routes}
    assert "/parcels" in paths
    assert "/parcels/{parcel_id}" in paths
