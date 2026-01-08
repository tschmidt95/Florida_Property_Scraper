from florida_property_scraper.api.app import counties, health


def test_health_ok():
    assert health()["status"] == "ok"


def test_counties_payload():
    payload = counties()
    assert "counties" in payload
    assert isinstance(payload["counties"], list)
    assert len(payload["counties"]) >= 1
