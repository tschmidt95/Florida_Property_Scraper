import os
import tempfile

from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.connectors.base import get_connector
from florida_property_scraper.triggers.engine import run_connector_once
from florida_property_scraper.triggers.taxonomy import TriggerKey


def test_code_enforcement_stub_connector_emits_strong_and_rollups() -> None:
    # Ensure connectors are registered
    import florida_property_scraper.triggers.connectors as _c  # noqa: F401

    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        os.environ["LEADS_SQLITE_PATH"] = db

        store = SQLiteStore(db)
        try:
            store.upsert_many_code_enforcement_events(
                [
                    {
                        "county": "orange",
                        "parcel_id": "PARCEL-CODE-1",
                        "observed_at": "2026-01-05T00:00:00+00:00",
                        "event_type": "code_enforcement.code_case_opened",
                        "event_date": "2026-01-01",
                        "case_number": "CE-123",
                        "status": "open",
                        "description": "Code case opened",
                        "fine_amount": None,
                        "lien_amount": None,
                        "source": "test",
                    },
                    {
                        "county": "orange",
                        "parcel_id": "PARCEL-CODE-1",
                        "observed_at": "2026-01-06T00:00:00+00:00",
                        "event_type": "code_enforcement.fines_imposed",
                        "event_date": "2026-01-02",
                        "case_number": "CE-123",
                        "status": "active",
                        "description": "Fines imposed",
                        "fine_amount": 250.0,
                        "lien_amount": None,
                        "source": "test",
                    },
                ]
            )

            connector = get_connector("code_enforcement_stub")
            out = run_connector_once(
                store=store,
                connector=connector,
                county="orange",
                now_iso="2026-01-16T00:00:00+00:00",
                limit=50,
            )
            assert out["trigger_events"] >= 1

            events = store.list_trigger_events_for_parcel(county="orange", parcel_id="PARCEL-CODE-1", limit=50)
            keys = {e.get("trigger_key") for e in events}
            assert str(TriggerKey.CODE_CASE_OPENED) in keys
            assert str(TriggerKey.FINES_IMPOSED) in keys

            rebuilt = store.rebuild_parcel_trigger_rollups(county="orange", rebuilt_at="2026-01-16T00:00:00+00:00")
            assert rebuilt["ok"] is True

            rollup = store.get_rollup_for_parcel(county="orange", parcel_id="PARCEL-CODE-1")
            assert rollup is not None
            assert int(rollup.get("has_code_enforcement") or 0) == 1
            assert int(rollup.get("count_strong") or 0) >= 2
            assert int(rollup.get("seller_score") or 0) == 85
        finally:
            store.close()
