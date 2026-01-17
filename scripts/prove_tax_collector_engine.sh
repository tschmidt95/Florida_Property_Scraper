#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"

NOW_ISO="2026-01-16T00:00:00+00:00"

echo "== seed tax collector events =="
python - <<'PY'
import os
from florida_property_scraper.storage import SQLiteStore

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    store.upsert_many_tax_collector_events(
        [
            {
                "county": "orange",
                "parcel_id": "PARCEL-OR-TAX-1",
                "observed_at": "2026-01-05T00:00:00+00:00",
                "event_type": "tax_collector.delinquent_tax",
                "event_date": "2026-01-01",
                "amount_due": 1234.56,
                "status": "delinquent",
                "description": "Delinquent tax notice",
                "source": "test",
            },
            {
                "county": "orange",
                "parcel_id": "PARCEL-OR-TAX-1",
                "observed_at": "2026-01-06T00:00:00+00:00",
                "event_type": "tax_collector.tax_certificate_issued",
                "event_date": "2026-01-02",
                "amount_due": 900.00,
                "status": "issued",
                "description": "Tax certificate issued",
                "source": "test",
            },
        ]
    )
finally:
    store.close()
PY

echo

echo "$ python -m florida_property_scraper triggers run --county orange --connector tax_collector_stub --limit 50 --now $NOW_ISO"
python -m florida_property_scraper triggers run --county orange --connector tax_collector_stub --limit 50 --now "$NOW_ISO"

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
    events = store.list_trigger_events_for_parcel(county="orange", parcel_id="PARCEL-OR-TAX-1", limit=50)
    keys = {e.get("trigger_key") for e in events}
    print({"trigger_keys": sorted(k for k in keys if k)})
    assert str(TriggerKey.DELINQUENT_TAX) in keys
    assert str(TriggerKey.TAX_CERTIFICATE_ISSUED) in keys

    rollup = store.get_rollup_for_parcel(county="orange", parcel_id="PARCEL-OR-TAX-1")
    assert rollup is not None
    details = {}
    try:
        details = json.loads(str(rollup.get("details_json") or "{}"))
    except Exception:
        details = {}
    print(
        {
            "has_tax": int(rollup.get("has_tax") or 0),
            "count_critical": int(rollup.get("count_critical") or 0),
            "count_strong": int(rollup.get("count_strong") or 0),
            "count_support": int(rollup.get("count_support") or 0),
            "seller_score": int(rollup.get("seller_score") or 0),
            "seller_intent_rule": ((details.get("seller_intent") or {}).get("rule")),
        }
    )
    assert int(rollup.get("has_tax") or 0) == 1
    assert int(rollup.get("count_critical") or 0) >= 1
    assert int(rollup.get("seller_score") or 0) == 100
finally:
    store.close()
PY

echo

echo "PASS tax_collector_stub"
