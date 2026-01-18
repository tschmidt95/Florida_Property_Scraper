import os
import tempfile

from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.connectors.base import get_connector
from florida_property_scraper.triggers.engine import run_connector_once
from florida_property_scraper.triggers.taxonomy import TriggerKey


def test_liens_stub_connector_emits_strong_and_inbox_syncs() -> None:
    # Ensure connectors are registered
    import florida_property_scraper.triggers.connectors as _c  # noqa: F401

    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        os.environ["LEADS_SQLITE_PATH"] = db

        store = SQLiteStore(db)
        try:
            ss = store.create_saved_search(
                name="SS",
                county="seminole",
                polygon_geojson={"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
                filters={},
                enrich=False,
                now_iso="2026-01-01T00:00:00+00:00",
            )
            sid = ss["id"]

            assert store.add_member_to_saved_search(
                saved_search_id=sid,
                county="seminole",
                parcel_id="XYZ789",
                source="manual",
                now_iso="2026-01-01T00:00:00+00:00",
            )

            connector = get_connector("liens_stub")
            out = run_connector_once(
                store=store,
                connector=connector,
                county="seminole",
                now_iso="2026-01-18T00:00:00+00:00",
                limit=50,
            )
            assert out["trigger_events"] >= 1

            events = store.list_trigger_events_for_parcel(county="seminole", parcel_id="XYZ789", limit=50)
            keys = {e.get("trigger_key") for e in events}
            assert str(TriggerKey.MECHANICS_LIEN) in keys
            assert str(TriggerKey.HOA_LIEN) in keys
            assert str(TriggerKey.IRS_TAX_LIEN) in keys
            assert str(TriggerKey.JUDGMENT_LIEN) in keys
            assert str(TriggerKey.LIEN_RELEASE) in keys

            rebuilt = store.rebuild_parcel_trigger_rollups(county="seminole", rebuilt_at="2026-01-18T00:00:00+00:00")
            assert rebuilt["ok"] is True

            rollup = store.get_rollup_for_parcel(county="seminole", parcel_id="XYZ789")
            assert rollup is not None
            assert int(rollup.get("count_strong") or 0) >= 5
            assert int(rollup.get("count_critical") or 0) == 0

            alerts = store.list_trigger_alerts_for_parcel(county="seminole", parcel_id="XYZ789", status="open")
            alert_keys = {a.get("alert_key") for a in alerts}
            assert "seller_intent_strong_stack" in alert_keys

            sync = store.sync_saved_search_inbox_from_trigger_alerts(
                saved_search_id=sid,
                now_iso="2026-01-18T00:00:01+00:00",
            )
            assert sync.get("ok") is True

            inbox = store.list_alerts(saved_search_id=sid, limit=20)
            assert len(inbox) >= 1
            assert inbox[0]["saved_search_id"] == sid
            assert inbox[0]["parcel_id"] == "XYZ789"
        finally:
            store.close()
