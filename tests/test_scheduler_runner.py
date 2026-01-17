import os
import tempfile

from florida_property_scraper.scheduler.runner import run_scheduler_tick
from florida_property_scraper.storage import SQLiteStore


def test_scheduler_tick_syncs_saved_search_inbox_deterministically():
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        os.environ["LEADS_SQLITE_PATH"] = db

        store = SQLiteStore(db)
        try:
            ss = store.create_saved_search(
                name="SS",
                county="orange",
                polygon_geojson={"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
                filters={},
                enrich=False,
                now_iso="2026-01-01T00:00:00+00:00",
            )
            sid = ss["id"]

            assert store.add_member_to_saved_search(
                saved_search_id=sid,
                county="orange",
                parcel_id="P-1",
                source="manual",
                now_iso="2026-01-01T00:00:00+00:00",
            )

            assert store.upsert_trigger_alert(
                county="orange",
                parcel_id="P-1",
                alert_key="seller_intent_critical",
                severity=5,
                first_seen_at="2026-01-01T00:00:00+00:00",
                last_seen_at="2026-01-01T00:00:00+00:00",
                status="open",
                trigger_event_ids=[1],
                details={"rule": "critical>=1", "seller_score": 100, "trigger_keys": ["t"]},
            )
        finally:
            store.close()

        r1 = run_scheduler_tick(
            db_path=db,
            now_iso="2026-01-01T00:00:01+00:00",
            run_saved_searches=False,
            run_connectors=False,
            run_rollups=False,
        )
        assert r1["ok"] is True
        assert int(r1["alerts_inserted"]) == 1

        r2 = run_scheduler_tick(
            db_path=db,
            now_iso="2026-01-01T00:00:02+00:00",
            run_saved_searches=False,
            run_connectors=False,
            run_rollups=False,
        )
        assert r2["ok"] is True
        assert int(r2["alerts_inserted"]) == 0
        assert int(r2["alerts_updated"]) == 0

        store2 = SQLiteStore(db)
        try:
            # Move trigger forward; scheduler should mark inbox as new again via UPDATE.
            assert store2.upsert_trigger_alert(
                county="orange",
                parcel_id="P-1",
                alert_key="seller_intent_critical",
                severity=5,
                first_seen_at="2026-01-01T00:00:00+00:00",
                last_seen_at="2026-01-01T00:00:03+00:00",
                status="open",
                trigger_event_ids=[1, 2],
                details={"rule": "critical>=1", "seller_score": 100, "trigger_keys": ["t"]},
            )
        finally:
            store2.close()

        r3 = run_scheduler_tick(
            db_path=db,
            now_iso="2026-01-01T00:00:04+00:00",
            run_saved_searches=False,
            run_connectors=False,
            run_rollups=False,
        )
        assert r3["ok"] is True
        assert int(r3["alerts_inserted"]) == 0
        assert int(r3["alerts_updated"]) == 1
