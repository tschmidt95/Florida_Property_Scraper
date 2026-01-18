import os
import tempfile

from florida_property_scraper.storage import SQLiteStore


def test_rollups_search_filters_by_trigger_key_and_group() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        os.environ["LEADS_SQLITE_PATH"] = db
        store = SQLiteStore(db)
        try:
            # Parcel A: permit signal
            store.insert_trigger_events(
                trigger_events=[
                    {
                        "county": "seminole",
                        "parcel_id": "XYZ789",
                        "trigger_key": "permit_pool",
                        "trigger_at": "2026-01-10T00:00:00+00:00",
                        "severity": 4,
                        "source_connector_key": "permits_db",
                        "source_event_type": "permits_db.permit",
                        "source_event_id": None,
                        "details": {"permit": {"permit_number": "P-1"}},
                    }
                ],
                run_id="t1",
            )

            # Parcel B: property appraiser signal (group has no dedicated column)
            store.insert_trigger_events(
                trigger_events=[
                    {
                        "county": "seminole",
                        "parcel_id": "ABC123",
                        "trigger_key": "owner_mailing_changed",
                        "trigger_at": "2026-01-11T00:00:00+00:00",
                        "severity": 4,
                        "source_connector_key": "property_appraiser_stub",
                        "source_event_type": "property_appraiser.owner_mailing_changed",
                        "source_event_id": None,
                        "details": {"property_appraiser": {"source": "stub"}},
                    }
                ],
                run_id="t2",
            )

            store.rebuild_parcel_trigger_rollups(county="seminole", rebuilt_at="2026-01-18T00:00:00+00:00")

            # Filter by trigger key
            by_key = store.search_rollups(
                county="seminole",
                require_trigger_keys=["permit_pool"],
                limit=50,
                offset=0,
            )
            assert [r["parcel_id"] for r in by_key] == ["XYZ789"]

            # Filter by group (property_appraiser is matched via details_json LIKE)
            by_group = store.search_rollups(
                county="seminole",
                require_any_groups=["property_appraiser"],
                limit=50,
                offset=0,
            )
            assert [r["parcel_id"] for r in by_group] == ["ABC123"]
        finally:
            store.close()
