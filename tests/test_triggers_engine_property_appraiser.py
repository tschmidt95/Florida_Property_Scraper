import os
import tempfile

from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.connectors.base import get_connector
from florida_property_scraper.triggers.engine import run_connector_once
from florida_property_scraper.triggers.taxonomy import TriggerKey


def test_property_appraiser_stub_emits_owner_changes_and_alerts() -> None:
    # Ensure connectors are registered
    import florida_property_scraper.triggers.connectors as _c  # noqa: F401

    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        os.environ["LEADS_SQLITE_PATH"] = db

        store = SQLiteStore(db)
        try:
            connector = get_connector("property_appraiser_stub")
            out = run_connector_once(
                store=store,
                connector=connector,
                county="seminole",
                now_iso="2026-01-16T00:00:00+00:00",
                limit=50,
            )
            assert out["trigger_events"] >= 2

            events = store.list_trigger_events_for_parcel(county="seminole", parcel_id="XYZ789", limit=50)
            keys = {e.get("trigger_key") for e in events}
            assert str(TriggerKey.OWNER_MAILING_CHANGED) in keys
            assert str(TriggerKey.OWNER_NAME_CHANGED) in keys
            assert str(TriggerKey.DEED_LAST_SALE_UPDATED) in keys

            alerts = store.list_trigger_alerts_for_parcel(county="seminole", parcel_id="XYZ789", status="open", limit=50)
            alert_keys = {a.get("alert_key") for a in alerts}
            assert "owner_moved" in alert_keys

            rebuilt = store.rebuild_parcel_trigger_rollups(county="seminole", rebuilt_at="2026-01-16T00:00:00+00:00")
            assert rebuilt["ok"] is True

            rollup = store.get_rollup_for_parcel(county="seminole", parcel_id="XYZ789")
            assert rollup is not None
            assert int(rollup.get("count_support") or 0) >= 2
        finally:
            store.close()
