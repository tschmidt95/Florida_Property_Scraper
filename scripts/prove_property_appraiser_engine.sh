#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"

NOW_ISO="2026-01-16T00:00:00+00:00"

echo "$ python -m florida_property_scraper triggers run --county seminole --connector property_appraiser_stub --limit 50 --now $NOW_ISO"
python -m florida_property_scraper triggers run --county seminole --connector property_appraiser_stub --limit 50 --now "$NOW_ISO"

echo

echo "$ python -m florida_property_scraper triggers rollups --county seminole --rebuilt_at $NOW_ISO"
python -m florida_property_scraper triggers rollups --county seminole --rebuilt_at "$NOW_ISO"

echo

echo "$ python - <<'PY' (verify keys + alert)"
python - <<'PY'
import os
from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.taxonomy import TriggerKey

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    events = store.list_trigger_events_for_parcel(county="seminole", parcel_id="XYZ789", limit=50)
    keys = {e.get("trigger_key") for e in events}
    print({"trigger_keys": sorted(k for k in keys if k)})
    assert str(TriggerKey.OWNER_MAILING_CHANGED) in keys
    assert str(TriggerKey.OWNER_NAME_CHANGED) in keys
    assert str(TriggerKey.DEED_LAST_SALE_UPDATED) in keys

    alerts = store.list_trigger_alerts_for_parcel(county="seminole", parcel_id="XYZ789", status="open", limit=50)
    akeys = {a.get("alert_key") for a in alerts}
    print({"alert_keys": sorted(k for k in akeys if k)})
    assert "owner_moved" in akeys
finally:
    store.close()
PY

echo

echo "PASS property_appraiser_stub"
