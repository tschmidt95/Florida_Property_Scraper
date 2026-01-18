import os
import tempfile

from florida_property_scraper.scheduler.runner import run_scheduler_tick
from florida_property_scraper.storage import SQLiteStore


def _seed_permit(store: SQLiteStore, *, county: str, parcel_id: str, issue_date: str) -> None:
    store.conn.execute(
        """
        INSERT INTO permits(
            county, parcel_id, address, permit_number, permit_type, status,
            issue_date, final_date, description, source, raw
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            county,
            parcel_id,
            "123 TEST ST",
            f"PERMIT-{parcel_id}",
            "ROOF",
            "ISSUED",
            issue_date,
            None,
            "REROOF",
            "test",
            "{}",
        ),
    )
    store.conn.commit()


def test_scheduler_runs_stub_connectors_and_creates_saved_search_alerts_inbox():
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

            _seed_permit(store, county="orange", parcel_id="P-1", issue_date="2026-01-01")
        finally:
            store.close()

        res = run_scheduler_tick(
            db_path=db,
            now_iso="2026-01-02T00:00:00+00:00",
            connector_keys=["permits_db"],
            counties=["orange"],
            run_saved_searches=False,
            run_connectors=True,
            run_rollups=True,
            run_delivery=True,
        )
        assert res["ok"] is True
        assert int(res.get("alerts_inserted") or 0) >= 1
        assert res.get("delivery", {}).get("ok") is True

        store2 = SQLiteStore(db)
        try:
            alerts = store2.list_alerts(saved_search_id=sid, limit=10)
            assert len(alerts) >= 1
            assert alerts[0]["saved_search_id"] == sid
            assert alerts[0]["parcel_id"] == "P-1"
        finally:
            store2.close()


def test_delivery_ledger_prevents_duplicates_across_two_ticks():
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

            # Seed an already-open trigger alert with a stable last_seen_at.
            assert store.upsert_trigger_alert(
                county="orange",
                parcel_id="P-1",
                alert_key="seller_intent_critical",
                severity=5,
                first_seen_at="2026-01-01T00:00:00+00:00",
                last_seen_at="2026-01-01T00:00:00+00:00",
                status="open",
                trigger_event_ids=[1],
                details={"rule": "critical>=1", "seller_score": 100, "trigger_keys": ["tax_delinquent"]},
            )
        finally:
            store.close()

        r1 = run_scheduler_tick(
            db_path=db,
            now_iso="2026-01-01T00:00:01+00:00",
            run_saved_searches=False,
            run_connectors=False,
            run_rollups=False,
            run_delivery=True,
        )
        assert r1["ok"] is True
        assert int(r1.get("delivery", {}).get("delivered") or 0) >= 1

        r2 = run_scheduler_tick(
            db_path=db,
            now_iso="2026-01-01T00:00:02+00:00",
            run_saved_searches=False,
            run_connectors=False,
            run_rollups=False,
            run_delivery=True,
        )
        assert r2["ok"] is True
        assert int(r2.get("delivery", {}).get("delivered") or 0) == 0
