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
                "county": "orange",
                "parcel_id": "PARCEL-OR-CODE-1",
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
                "parcel_id": "PARCEL-OR-CODE-1",
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
finally:
    store.close()
PY

echo

echo "$ python -m florida_property_scraper triggers run --county orange --connector code_enforcement_stub --limit 50 --now $NOW_ISO"
python -m florida_property_scraper triggers run --county orange --connector code_enforcement_stub --limit 50 --now "$NOW_ISO"

echo

echo "$ python -m florida_property_scraper triggers rollups --county orange --rebuilt_at $NOW_ISO"
python -m florida_property_scraper triggers rollups --county orange --rebuilt_at "$NOW_ISO"

echo

echo "$ python - <<'PY' (verify rollup + trigger key)"
python - <<'PY'
import os
import json
from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.taxonomy import TriggerKey

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    events = store.list_trigger_events_for_parcel(county="orange", parcel_id="PARCEL-OR-CODE-1", limit=50)
    keys = {e.get("trigger_key") for e in events}
    print({"trigger_keys": sorted(k for k in keys if k)})
    assert str(TriggerKey.CODE_CASE_OPENED) in keys
    assert str(TriggerKey.FINES_IMPOSED) in keys

    rollup = store.get_rollup_for_parcel(county="orange", parcel_id="PARCEL-OR-CODE-1")
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
    assert int(rollup.get("count_strong") or 0) >= 2
    assert int(rollup.get("seller_score") or 0) == 85
finally:
    store.close()
PY

echo

echo "PASS code_enforcement_stub"
