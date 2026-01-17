import os
import tempfile

from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.connectors.base import get_connector
from florida_property_scraper.triggers.engine import run_connector_once
from florida_property_scraper.triggers.taxonomy import TriggerKey


def test_official_records_stub_connector_emits_critical_and_rollups() -> None:
    # Ensure connectors are registered
    import florida_property_scraper.triggers.connectors as _c  # noqa: F401

    with tempfile.TemporaryDirectory() as td:
        db = f"{td}/leads.sqlite"
        os.environ["LEADS_SQLITE_PATH"] = db

        store = SQLiteStore(db)
        try:
            store.upsert_many_official_records(
                [
                    {
                        "county": "orange",
                        "parcel_id": "PARCEL-9",
                        "join_key": "OWNERKEY-9",
                        "doc_type": "WARRANTY DEED",
                        "rec_date": "2026-01-01",
                        "parties": "GRANTOR -> GRANTEE",
                        "book_page_or_instrument": "INST-100",
                        "consideration": "$350,000",
                        "raw_text": "WARRANTY DEED",
                        "owner_name": "DOE, JOHN",
                        "address": "123 MAIN ST",
                        "source": "test",
                        "raw": None,
                    },
                    {
                        "county": "orange",
                        "parcel_id": "PARCEL-9",
                        "join_key": "OWNERKEY-9",
                        "doc_type": "MORTGAGE",
                        "rec_date": "2026-01-02",
                        "parties": "BORROWER / LENDER",
                        "book_page_or_instrument": "INST-101",
                        "consideration": None,
                        "raw_text": "MORTGAGE RECORDED",
                        "owner_name": "DOE, JOHN",
                        "address": "123 MAIN ST",
                        "source": "test",
                        "raw": None,
                    },
                    {
                        "county": "orange",
                        "parcel_id": "PARCEL-9",
                        "join_key": "OWNERKEY-9",
                        "doc_type": "SATISFACTION OF MORTGAGE",
                        "rec_date": "2026-01-03",
                        "parties": "LENDER / BORROWER",
                        "book_page_or_instrument": "INST-102",
                        "consideration": None,
                        "raw_text": "SATISFACTION",
                        "owner_name": "DOE, JOHN",
                        "address": "123 MAIN ST",
                        "source": "test",
                        "raw": None,
                    },
                    {
                        "county": "orange",
                        "parcel_id": "PARCEL-9",
                        "join_key": "OWNERKEY-9",
                        "doc_type": "CLAIM OF LIEN",
                        "rec_date": "2026-01-04",
                        "parties": "CONTRACTOR / OWNER",
                        "book_page_or_instrument": "INST-103",
                        "consideration": None,
                        "raw_text": "MECHANIC'S LIEN",
                        "owner_name": "DOE, JOHN",
                        "address": "123 MAIN ST",
                        "source": "test",
                        "raw": None,
                    },
                    {
                        "county": "orange",
                        "parcel_id": "PARCEL-9",
                        "join_key": "OWNERKEY-9",
                        "doc_type": "LIS PENDENS",
                        "rec_date": "2026-01-05",
                        "parties": "PLAINTIFF v DEFENDANT",
                        "book_page_or_instrument": "INST-104",
                        "consideration": None,
                        "raw_text": "LIS PENDENS NOTICE",
                        "owner_name": "DOE, JOHN",
                        "address": "123 MAIN ST",
                        "source": "test",
                        "raw": None,
                    }
                ]
            )

            connector = get_connector("official_records_stub")
            out = run_connector_once(
                store=store,
                connector=connector,
                county="orange",
                now_iso="2026-01-16T00:00:00+00:00",
                limit=50,
            )
            assert out["trigger_events"] >= 1

            events = store.list_trigger_events_for_parcel(county="orange", parcel_id="PARCEL-9", limit=50)
            keys = {e.get("trigger_key") for e in events}
            assert str(TriggerKey.DEED_WARRANTY) in keys
            assert str(TriggerKey.MORTGAGE_RECORDED) in keys
            assert str(TriggerKey.MORTGAGE_SATISFACTION) in keys
            assert str(TriggerKey.MECHANICS_LIEN) in keys
            assert str(TriggerKey.LIS_PENDENS) in keys

            rebuilt = store.rebuild_parcel_trigger_rollups(county="orange", rebuilt_at="2026-01-16T00:00:00+00:00")
            assert rebuilt["ok"] is True

            rollup = store.get_rollup_for_parcel(county="orange", parcel_id="PARCEL-9")
            assert rollup is not None
            assert int(rollup.get("has_official_records") or 0) == 1
            assert int(rollup.get("count_critical") or 0) >= 1
            assert int(rollup.get("seller_score") or 0) == 100
        finally:
            store.close()
