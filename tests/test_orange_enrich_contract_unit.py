import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("FPS_USE_FDOR_CENTROIDS", "1")
    monkeypatch.setenv("PA_DB", str(tmp_path / "pa.sqlite"))

    from florida_property_scraper.api.app import app

    return TestClient(app)


def test_orange_enrich_returns_structured_errors(client, monkeypatch):
    # Keep this test non-flaky: do not hit OCPA.
    from florida_property_scraper.pa.providers import orange_ocpa

    def fake_enrich(pid: str):
        return {
            "error_reason": "blocked",
            "http_status": 403,
            "hint": "unit_test",
            "source_url": "https://ocpaservices.ocpafl.org/",
        }

    monkeypatch.setattr(orange_ocpa, "enrich_parcel", fake_enrich)

    r = client.post(
        "/api/parcels/enrich",
        json={"county": "orange", "parcel_ids": ["362229000000025"], "limit": 1},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["county"] == "orange"
    assert "errors" in data
    assert "362229000000025" in data["errors"]
    err = data["errors"]["362229000000025"]
    assert isinstance(err, dict)
    assert err.get("error_reason") == "blocked"
