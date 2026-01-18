import os
import tempfile

from florida_property_scraper.permits.models import PermitRecord
from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.connectors.base import get_connector
from florida_property_scraper.triggers.engine import run_connector_once
from florida_property_scraper.triggers.taxonomy import TriggerKey


def test_permits_db_emits_expanded_categories_and_rollups() -> None:
    # Ensure connectors are registered
    import florida_property_scraper.triggers.connectors as _c  # noqa: F401

    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        os.environ["LEADS_SQLITE_PATH"] = db

        store = SQLiteStore(db)
        try:
            store.upsert_many_permits(
                [
                    PermitRecord(
                        county="seminole",
                        parcel_id="XYZ789",
                        address="123 TEST ST",
                        permit_number="P-POOL-1",
                        permit_type="POOL",
                        status="ISSUED",
                        issue_date="2026-01-10",
                        final_date=None,
                        description="Pool permit",
                        source="test",
                    ),
                    PermitRecord(
                        county="seminole",
                        parcel_id="XYZ789",
                        address="123 TEST ST",
                        permit_number="P-FIRE-1",
                        permit_type="FIRE ALARM",
                        status="ISSUED",
                        issue_date="2026-01-11",
                        final_date=None,
                        description="Fire alarm system",
                        source="test",
                    ),
                    PermitRecord(
                        county="seminole",
                        parcel_id="XYZ789",
                        address="123 TEST ST",
                        permit_number="P-SITE-1",
                        permit_type="SITEWORK",
                        status="ISSUED",
                        issue_date="2026-01-12",
                        final_date=None,
                        description="Site work grading",
                        source="test",
                    ),
                    PermitRecord(
                        county="seminole",
                        parcel_id="XYZ789",
                        address="123 TEST ST",
                        permit_number="P-SIGN-1",
                        permit_type="SIGN",
                        status="ISSUED",
                        issue_date="2026-01-13",
                        final_date=None,
                        description="Sign permit",
                        source="test",
                    ),
                    PermitRecord(
                        county="seminole",
                        parcel_id="XYZ789",
                        address="123 TEST ST",
                        permit_number="P-TI-1",
                        permit_type="TENANT IMPROVEMENT",
                        status="ISSUED",
                        issue_date="2026-01-14",
                        final_date=None,
                        description="Tenant improvement",
                        source="test",
                    ),
                    PermitRecord(
                        county="seminole",
                        parcel_id="XYZ789",
                        address="123 TEST ST",
                        permit_number="P-REMOD-1",
                        permit_type="REMODEL",
                        status="ISSUED",
                        issue_date="2026-01-15",
                        final_date=None,
                        description="Interior remodel",
                        source="test",
                    ),
                    PermitRecord(
                        county="seminole",
                        parcel_id="XYZ789",
                        address="123 TEST ST",
                        permit_number="P-FENCE-1",
                        permit_type="FENCE",
                        status="ISSUED",
                        issue_date="2026-01-16",
                        final_date=None,
                        description="Fence",
                        source="test",
                    ),
                    PermitRecord(
                        county="seminole",
                        parcel_id="XYZ789",
                        address="123 TEST ST",
                        permit_number="P-GEN-1",
                        permit_type="GENERATOR",
                        status="ISSUED",
                        issue_date="2026-01-17",
                        final_date=None,
                        description="Standby generator",
                        source="test",
                    ),
                ]
            )

            connector = get_connector("permits_db")
            out = run_connector_once(
                store=store,
                connector=connector,
                county="seminole",
                now_iso="2026-01-18T00:00:00+00:00",
                limit=200,
            )
            assert out["trigger_events"] >= 8

            events = store.list_trigger_events_for_parcel(county="seminole", parcel_id="XYZ789", limit=200)
            keys = {e.get("trigger_key") for e in events}
            assert str(TriggerKey.PERMIT_POOL) in keys
            assert str(TriggerKey.PERMIT_FIRE) in keys
            assert str(TriggerKey.PERMIT_SITEWORK) in keys
            assert str(TriggerKey.PERMIT_SIGN) in keys
            assert str(TriggerKey.PERMIT_TENANT_IMPROVEMENT) in keys
            assert str(TriggerKey.PERMIT_REMODEL) in keys
            assert str(TriggerKey.PERMIT_FENCE) in keys
            assert str(TriggerKey.PERMIT_GENERATOR) in keys

            rebuilt = store.rebuild_parcel_trigger_rollups(county="seminole", rebuilt_at="2026-01-18T00:00:00+00:00")
            assert rebuilt["ok"] is True

            rollup = store.get_rollup_for_parcel(county="seminole", parcel_id="XYZ789")
            assert rollup is not None
            assert int(rollup.get("has_permits") or 0) == 1
            assert int(rollup.get("count_strong") or 0) >= 2
        finally:
            store.close()
