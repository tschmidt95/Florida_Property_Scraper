import os
import tempfile

from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.connectors.base import get_connector
from florida_property_scraper.triggers.engine import run_connector_once
from florida_property_scraper.triggers.taxonomy import TriggerKey


def test_tax_collector_stub_connector_emits_critical_and_rollups() -> None:
    # Ensure connectors are registered
    import florida_property_scraper.triggers.connectors as _c  # noqa: F401

    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        os.environ["LEADS_SQLITE_PATH"] = db

        store = SQLiteStore(db)
        try:
            store.upsert_many_tax_collector_events(
                [
                    {
                        "county": "orange",
                        "parcel_id": "PARCEL-TAX-1",
                        "observed_at": "2026-01-05T00:00:00+00:00",
                        "event_type": "tax_collector.delinquent_tax",
                        "event_date": "2026-01-01",
                        "amount_due": 1234.56,
                        "status": "delinquent",
                        "description": "Delinquent tax notice",
                        "source": "test",
                    },
                    {
                        "county": "orange",
                        "parcel_id": "PARCEL-TAX-1",
                        "observed_at": "2026-01-06T00:00:00+00:00",
                        "event_type": "tax_collector.tax_certificate_issued",
                        "event_date": "2026-01-02",
                        "amount_due": 900.00,
                        "status": "issued",
                        "description": "Tax certificate issued",
                        "source": "test",
                    },
                ]
            )

            connector = get_connector("tax_collector_stub")
            out = run_connector_once(
                store=store,
                connector=connector,
                county="orange",
                now_iso="2026-01-16T00:00:00+00:00",
                limit=50,
            )
            assert out["trigger_events"] >= 1

            events = store.list_trigger_events_for_parcel(county="orange", parcel_id="PARCEL-TAX-1", limit=50)
            keys = {e.get("trigger_key") for e in events}
            assert str(TriggerKey.DELINQUENT_TAX) in keys
            assert str(TriggerKey.TAX_CERTIFICATE_ISSUED) in keys

            rebuilt = store.rebuild_parcel_trigger_rollups(county="orange", rebuilt_at="2026-01-16T00:00:00+00:00")
            assert rebuilt["ok"] is True

            rollup = store.get_rollup_for_parcel(county="orange", parcel_id="PARCEL-TAX-1")
            assert rollup is not None
            assert int(rollup.get("has_tax") or 0) == 1
            assert int(rollup.get("count_critical") or 0) >= 1
            assert int(rollup.get("seller_score") or 0) == 100
        finally:
            store.close()
