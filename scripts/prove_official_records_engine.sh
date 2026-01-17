#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"

NOW_ISO="2026-01-01T00:00:00+00:00"

echo "== seed official record (lis pendens) =="
python - <<'PY'
import os
from florida_property_scraper.storage import SQLiteStore

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    store.upsert_many_official_records([
        {
            "county": "orange",
            "parcel_id": "PARCEL-OR-1",
            "join_key": None,
            "doc_type": "LIS PENDENS",
            "rec_date": "2026-01-01",
            "parties": "PLAINTIFF v DEFENDANT",
            "book_page_or_instrument": "INST-123",
            "consideration": None,
            "raw_text": "NOTICE OF LIS PENDENS",
            "owner_name": "DOE, JOHN",
            "address": "123 MAIN ST",
            "source": "test",
            "raw": None,
        }
    ])
finally:
    store.close()
PY

echo

echo "$ python -m florida_property_scraper triggers run --county orange --connector official_records_stub --limit 50 --now $NOW_ISO"
python -m florida_property_scraper triggers run --county orange --connector official_records_stub --limit 50 --now "$NOW_ISO"

echo

echo "$ python -m florida_property_scraper triggers rollups --county orange --rebuilt_at $NOW_ISO"
python -m florida_property_scraper triggers rollups --county orange --rebuilt_at "$NOW_ISO"

echo

echo "$ python - <<'PY' (verify rollup + trigger key)"
python - <<'PY'
import os
from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.taxonomy import TriggerKey

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    events = store.list_trigger_events_for_parcel(county="orange", parcel_id="PARCEL-OR-1", limit=50)
    keys = {e.get("trigger_key") for e in events}
    print({"trigger_keys": sorted(k for k in keys if k)})
    assert str(TriggerKey.LIS_PENDENS) in keys

    rollup = store.get_rollup_for_parcel(county="orange", parcel_id="PARCEL-OR-1")
    assert rollup is not None
    print({
        "has_official_records": int(rollup.get("has_official_records") or 0),
        "count_critical": int(rollup.get("count_critical") or 0),
        "seller_score": int(rollup.get("seller_score") or 0),
    })
    assert int(rollup.get("has_official_records") or 0) == 1
    assert int(rollup.get("count_critical") or 0) >= 1
    assert int(rollup.get("seller_score") or 0) == 100
finally:
    store.close()
PY

echo

echo "PASS official_records_stub"
