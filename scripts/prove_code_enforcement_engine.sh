#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"

NOW_ISO="2026-01-16T00:00:00+00:00"

echo "== seed code enforcement events =="
python - <<'PY'
import os
from florida_property_scraper.storage import SQLiteStore

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    store.upsert_many_code_enforcement_events(
        [
            {
                "county": "seminole",
                "parcel_id": "XYZ789",
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
                "county": "seminole",
                "parcel_id": "XYZ789",
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
            {
                "county": "seminole",
                "parcel_id": "XYZ789",
                "observed_at": "2026-01-07T00:00:00+00:00",
                "event_type": "code_enforcement.demolition_order",
                "event_date": "2026-01-03",
                "case_number": "CE-123",
                "status": "ordered",
                "description": "Demolition order issued",
                "fine_amount": None,
                "lien_amount": None,
                "source": "test",
            },
            {
                "county": "seminole",
                "parcel_id": "XYZ789",
                "observed_at": "2026-01-08T00:00:00+00:00",
                "event_type": "code_enforcement.board_hearing_set",
                "event_date": "2026-01-04",
                "case_number": "CE-123",
                "status": "set",
                "description": "Board hearing set",
                "fine_amount": None,
                "lien_amount": None,
                "source": "test",
            },
            {
                "county": "seminole",
                "parcel_id": "XYZ789",
                "observed_at": "2026-01-09T00:00:00+00:00",
                "event_type": "code_enforcement.reinspection_failed",
                "event_date": "2026-01-05",
                "case_number": "CE-123",
                "status": "failed",
                "description": "Reinspection failed",
                "fine_amount": None,
                "lien_amount": None,
                "source": "test",
            },
            {
                "county": "seminole",
                "parcel_id": "XYZ789",
                "observed_at": "2026-01-10T00:00:00+00:00",
                "event_type": "code_enforcement.abatement_order",
                "event_date": "2026-01-06",
                "case_number": "CE-123",
                "status": "ordered",
                "description": "Abatement order",
                "fine_amount": None,
                "lien_amount": None,
                "source": "test",
            },
            {
                "county": "seminole",
                "parcel_id": "XYZ789",
                "observed_at": "2026-01-11T00:00:00+00:00",
                "event_type": "code_enforcement.lien_recorded",
                "event_date": "2026-01-07",
                "case_number": "CE-123",
                "status": "recorded",
                "description": "Lien recorded",
                "fine_amount": None,
                "lien_amount": 1250.0,
                "source": "test",
            },
        ]
    )
finally:
    store.close()
PY

echo

echo "$ python -m florida_property_scraper triggers run --county seminole --connector code_enforcement_stub --limit 50 --now $NOW_ISO"
python -m florida_property_scraper triggers run --county seminole --connector code_enforcement_stub --limit 50 --now "$NOW_ISO"

echo

echo "$ python -m florida_property_scraper triggers rollups --county seminole --rebuilt_at $NOW_ISO"
python -m florida_property_scraper triggers rollups --county seminole --rebuilt_at "$NOW_ISO"

echo

echo "$ python - <<'PY' (verify rollup + trigger key)"
python - <<'PY'
import os
import json
from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.taxonomy import TriggerKey

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    events = store.list_trigger_events_for_parcel(county="seminole", parcel_id="XYZ789", limit=50)
    keys = {e.get("trigger_key") for e in events}
    print({"trigger_keys": sorted(k for k in keys if k)})
    assert str(TriggerKey.CODE_CASE_OPENED) in keys
    assert str(TriggerKey.FINES_IMPOSED) in keys
    assert str(TriggerKey.DEMOLITION_ORDER) in keys
    assert str(TriggerKey.BOARD_HEARING_SET) in keys
    assert str(TriggerKey.REINSPECTION_FAILED) in keys
    assert str(TriggerKey.ABATEMENT_ORDER) in keys
    assert str(TriggerKey.LIEN_RECORDED) in keys

    rollup = store.get_rollup_for_parcel(county="seminole", parcel_id="XYZ789")
    assert rollup is not None
    details = {}
    try:
        details = json.loads(str(rollup.get("details_json") or "{}"))
    except Exception:
        details = {}
    print(
        {
            "has_code_enforcement": int(rollup.get("has_code_enforcement") or 0),
            "count_critical": int(rollup.get("count_critical") or 0),
            "count_strong": int(rollup.get("count_strong") or 0),
            "count_support": int(rollup.get("count_support") or 0),
            "seller_score": int(rollup.get("seller_score") or 0),
            "seller_intent_rule": ((details.get("seller_intent") or {}).get("rule")),
        }
    )
    assert int(rollup.get("has_code_enforcement") or 0) == 1
    assert int(rollup.get("count_critical") or 0) >= 1
    assert int(rollup.get("count_strong") or 0) >= 2
    assert int(rollup.get("seller_score") or 0) == 100
finally:
    store.close()
PY

echo

echo "PASS code_enforcement_stub"
