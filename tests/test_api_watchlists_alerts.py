import os
import tempfile

from fastapi.testclient import TestClient

from florida_property_scraper.api.app import app
from florida_property_scraper.storage import SQLiteStore


def test_watchlists_alerts_inbox_sync_and_dedupe(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        monkeypatch.setenv("LEADS_SQLITE_PATH", db)

        client = TestClient(app)

        # Create saved search
        resp = client.post(
            "/api/saved-searches",
            json={
                "name": "SS",
                "county": "orange",
                "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
                "filters": {},
                "enrich": False,
            },
        )
        assert resp.status_code == 200, resp.text
        sid = resp.json()["saved_search"]["id"]

        # Add parcel member to the saved search
        resp = client.post(
            f"/api/saved-searches/{sid}/members",
            json={"county": "orange", "parcel_id": "P-1", "source": "manual"},
        )
        assert resp.status_code == 200, resp.text

        # Create a trigger alert for the parcel
        store = SQLiteStore(db)
        try:
            ok = store.upsert_trigger_alert(
                county="orange",
                parcel_id="P-1",
                alert_key="seller_intent_critical",
                severity=5,
                first_seen_at="2026-01-01T00:00:00+00:00",
                last_seen_at="2026-01-01T00:00:00+00:00",
                status="open",
                trigger_event_ids=[1, 2, 3],
                details={
                    "rule": "critical>=1",
                    "seller_score": 100,
                    "trigger_keys": ["test_a", "test_b"],
                },
            )
            assert ok is True

            # Sync inbox for the saved search (deterministic, no background scheduler required)
            res = store.sync_saved_search_inbox_from_trigger_alerts(
                saved_search_id=sid,
                now_iso="2026-01-01T00:00:01+00:00",
            )
            assert res.get("ok") is True
            assert int(res.get("inserted") or 0) == 1

            # Sync again should not re-insert (since_iso tracked via watchlist_runs)
            res2 = store.sync_saved_search_inbox_from_trigger_alerts(
                saved_search_id=sid,
                now_iso="2026-01-01T00:00:02+00:00",
            )
            assert res2.get("ok") is True
            assert int(res2.get("inserted") or 0) == 0
            assert int(res2.get("updated") or 0) == 0
        finally:
            store.close()

        # List alerts
        resp = client.get("/api/alerts", params={"saved_search_id": sid})
        assert resp.status_code == 200, resp.text
        alerts = resp.json()["alerts"]
        assert len(alerts) == 1
        assert alerts[0]["alert_key"] == "seller_intent_critical"
        assert alerts[0]["parcel_id"] == "P-1"
        assert alerts[0]["status"] == "new"

        # Mark read
        alert_id = int(alerts[0]["id"])
        resp = client.post(f"/api/alerts/{alert_id}/read")
        assert resp.status_code == 200, resp.text

        resp = client.get("/api/alerts", params={"saved_search_id": sid, "status": "read"})
        assert resp.status_code == 200, resp.text
        assert len(resp.json()["alerts"]) == 1
