import json
import tempfile

from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.engine import evaluate_and_upsert_alerts
from florida_property_scraper.triggers.models import TriggerEvent


def _insert_events(store: SQLiteStore, *, county: str, parcel_id: str, now: str, severities: list[int]) -> None:
    trigger_events: list[TriggerEvent] = []
    for i, sev in enumerate(severities):
        trigger_events.append(
            TriggerEvent(
                county=county,
                parcel_id=parcel_id,
                trigger_key=f"test_key_{i}",
                trigger_at=now,
                severity=int(sev),
                source_connector_key="test",
                source_event_type="unit",
                source_event_id=None,
                details={"i": i},
            )
        )
    ids = store.insert_trigger_events(trigger_events=trigger_events, run_id="test")
    evaluate_and_upsert_alerts(
        store=store,
        county=county,
        now_iso=now,
        new_trigger_rows=list(zip(trigger_events, ids)),
        window_days=30,
    )


def test_seller_intent_alert_fires_on_critical() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        store = SQLiteStore(db)
        try:
            now = "2026-01-01T00:00:00+00:00"
            _insert_events(store, county="orange", parcel_id="P-CRIT", now=now, severities=[5])

            alerts = store.list_trigger_alerts_for_parcel(county="orange", parcel_id="P-CRIT", status="open")
            seller = [a for a in alerts if a.get("alert_key") == "seller_intent"]
            assert seller, "expected seller_intent alert"
            details = json.loads(seller[0].get("details_json") or "{}")
            assert details.get("rule") == "critical>=1"
            assert int(details.get("seller_score") or 0) == 100
        finally:
            store.close()


def test_seller_intent_alert_fires_on_two_strong() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        store = SQLiteStore(db)
        try:
            now = "2026-01-01T00:00:00+00:00"
            _insert_events(store, county="orange", parcel_id="P-STRONG", now=now, severities=[4, 4])

            alerts = store.list_trigger_alerts_for_parcel(county="orange", parcel_id="P-STRONG", status="open")
            seller = [a for a in alerts if a.get("alert_key") == "seller_intent"]
            assert seller, "expected seller_intent alert"
            details = json.loads(seller[0].get("details_json") or "{}")
            assert details.get("rule") == "strong>=2"
            assert int(details.get("seller_score") or 0) == 85
        finally:
            store.close()


def test_seller_intent_alert_fires_on_mixed_four() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        store = SQLiteStore(db)
        try:
            now = "2026-01-01T00:00:00+00:00"
            _insert_events(store, county="orange", parcel_id="P-MIX", now=now, severities=[4, 2, 2, 2])

            alerts = store.list_trigger_alerts_for_parcel(county="orange", parcel_id="P-MIX", status="open")
            seller = [a for a in alerts if a.get("alert_key") == "seller_intent"]
            assert seller, "expected seller_intent alert"
            details = json.loads(seller[0].get("details_json") or "{}")
            assert details.get("rule") == "mixed>=4"
            assert int(details.get("seller_score") or 0) == 70
        finally:
            store.close()


def test_rollups_persist_seller_intent_details() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        store = SQLiteStore(db)
        try:
            now = "2026-01-01T00:00:00+00:00"
            # 1 critical + 2 strong + 1 support
            _insert_events(store, county="orange", parcel_id="P-R", now=now, severities=[5, 4, 4, 2])
            rebuilt = store.rebuild_parcel_trigger_rollups(county="orange", rebuilt_at=now)
            assert rebuilt.get("ok") is True
            rollup = store.get_rollup_for_parcel(county="orange", parcel_id="P-R")
            assert rollup is not None
            assert int(rollup.get("seller_score") or 0) == 100
            details = json.loads(rollup.get("details_json") or "{}")
            seller = details.get("seller_intent") or {}
            assert seller.get("rule") == "critical>=1"
            counts = seller.get("counts") or {}
            assert int(counts.get("critical") or 0) == 1
        finally:
            store.close()
