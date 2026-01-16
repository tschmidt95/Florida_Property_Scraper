#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"
export TRIGGER_FAKE_PARCELS="DEMO-001,DEMO-001"

NOW_ISO="2026-01-01T00:00:00+00:00"

echo "$ python -m florida_property_scraper triggers run --county orange --connector fake --limit 2 --now $NOW_ISO"
python -m florida_property_scraper triggers run --county orange --connector fake --limit 2 --now "$NOW_ISO"

echo

echo "$ python - <<'PY' (verify alerts)"
python - <<'PY'
import os
from florida_property_scraper.storage import SQLiteStore

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    alerts = store.list_trigger_alerts_for_parcel(county="orange", parcel_id="DEMO-001", status="open")
    keys = {a.get("alert_key") for a in alerts}
    print({"alerts": sorted(keys)})
    assert "permit_activity" in keys
    assert "owner_moved" in keys
    assert "redevelopment_signal" in keys
finally:
    store.close()
PY

echo

echo "PASS"

echo
echo "== permits_db connector (offline, reads permits table) =="

echo "$ python - <<'PY' (seed permit)"
python - <<'PY'
import os

from florida_property_scraper.permits.models import PermitRecord
from florida_property_scraper.storage import SQLiteStore

db = os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite")
store = SQLiteStore(db)
try:
    store.upsert_many_permits([
        PermitRecord(
            county="seminole",
            parcel_id="PARCEL-1",
            address="123 MAIN ST",
            permit_number="P-0001",
            permit_type="BUILDING",
            status="ISSUED",
            issue_date="2026-01-01",
            final_date=None,
            description="Test permit",
            source="test",
            raw=None,
        )
    ])
finally:
    store.close()
PY

echo "$ python -m florida_property_scraper triggers run --county seminole --connector permits_db --limit 50 --now $NOW_ISO"
python -m florida_property_scraper triggers run --county seminole --connector permits_db --limit 50 --now "$NOW_ISO"

echo
echo "$ python - <<'PY' (verify permit_activity)"
python - <<'PY'
import os

from florida_property_scraper.storage import SQLiteStore

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    alerts = store.list_trigger_alerts_for_parcel(county="seminole", parcel_id="PARCEL-1", status="open")
    keys = {a.get("alert_key") for a in alerts}
    print({"alerts": sorted(keys)})
    assert "permit_activity" in keys
finally:
    store.close()
PY

echo
echo "PASS permits_db"
