from florida_property_scraper.api.app import app


def test_runtime_audit_contract():
    if app is None:
        return

    from fastapi.testclient import TestClient

    client = TestClient(app)
    resp = client.get('/api/debug/runtime_audit', params={'county': 'seminole'})
    assert resp.status_code == 200
    data = resp.json()

    assert data.get('ok') is True
    assert isinstance(data.get('ready'), bool)
    assert isinstance(data.get('checked_at'), str)
    assert isinstance(data.get('hourly_policy'), dict)
    assert isinstance(data.get('schedulers'), dict)
    assert isinstance(data.get('preload'), dict)
    assert isinstance(data.get('warnings'), list)

    schedulers = data.get('schedulers') or {}
    assert isinstance(schedulers.get('watchlists'), dict)
    assert isinstance(schedulers.get('statewide_refresh'), dict)
