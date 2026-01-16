import tempfile

from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.permits.models import PermitRecord
from florida_property_scraper.triggers.engine import evaluate_and_upsert_alerts
from florida_property_scraper.triggers.models import TriggerEvent
from florida_property_scraper.triggers.taxonomy import TriggerKey
from florida_property_scraper.triggers.connectors.base import get_connector
from florida_property_scraper.triggers.engine import run_connector_once


def test_trigger_engine_stacks_alerts() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        store = SQLiteStore(db)
        try:
            now = "2026-01-01T00:00:00+00:00"
            county = "orange"
            parcel_id = "P-123"
            run_id = "test-run"

            trigger_events = [
                TriggerEvent(
                    county=county,
                    parcel_id=parcel_id,
                    trigger_key=str(TriggerKey.PERMIT_ISSUED),
                    trigger_at=now,
                    severity=2,
                    source_connector_key="test",
                    source_event_type="permit",
                    source_event_id=None,
                    details={"k": "v"},
                ),
                TriggerEvent(
                    county=county,
                    parcel_id=parcel_id,
                    trigger_key=str(TriggerKey.OWNER_MAILING_CHANGED),
                    trigger_at=now,
                    severity=3,
                    source_connector_key="test",
                    source_event_type="mailing",
                    source_event_id=None,
                    details={"k": "v"},
                ),
            ]

            ids = store.insert_trigger_events(trigger_events=trigger_events, run_id=run_id)
            new_rows = list(zip(trigger_events, ids))
            wrote = evaluate_and_upsert_alerts(
                store=store,
                county=county,
                now_iso=now,
                new_trigger_rows=new_rows,
                window_days=30,
            )
            assert wrote >= 1

            alerts = store.list_trigger_alerts_for_parcel(county=county, parcel_id=parcel_id, status="open")
            keys = {a.get("alert_key") for a in alerts}
            assert "permit_activity" in keys
            assert "owner_moved" in keys
            assert "redevelopment_signal" in keys
        finally:
            store.close()


def test_permits_db_connector_emits_permit_alert() -> None:
    # Ensure connectors are registered
    import florida_property_scraper.triggers.connectors as _c  # noqa: F401

    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        store = SQLiteStore(db)
        try:
            store.upsert_many_permits(
                [
                    PermitRecord(
                        county="seminole",
                        parcel_id="PARCEL-1",
                        address="123 MAIN ST",
                        permit_number="P-0001",
                        permit_type="BUILDING",
                        status="ISSUED",
                        issue_date="2026-01-01",
                        final_date=None,
                        description="Test permit",
                        source="test",
                        raw=None,
                    )
                ]
            )

            import os

            os.environ["LEADS_SQLITE_PATH"] = db
            connector = get_connector("permits_db")
            out = run_connector_once(
                store=store,
                connector=connector,
                county="seminole",
                now_iso="2026-01-16T00:00:00+00:00",
                limit=50,
            )
            assert out["trigger_events"] >= 1

            alerts = store.list_trigger_alerts_for_parcel(
                county="seminole", parcel_id="PARCEL-1", status="open"
            )
            keys = {a.get("alert_key") for a in alerts}
            assert "permit_activity" in keys
        finally:
            store.close()
