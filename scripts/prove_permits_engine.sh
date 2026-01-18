#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"

NOW_ISO="2026-01-18T00:00:00+00:00"

echo "== seed permits =="
python - <<'PY'
import os

from florida_property_scraper.permits.models import PermitRecord
from florida_property_scraper.storage import SQLiteStore

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    store.upsert_many_permits(
        [
            PermitRecord(
                county="seminole",
                parcel_id="XYZ789",
                address="123 TEST ST",
                permit_number="P-POOL-1",
                permit_type="POOL",
                status="ISSUED",
                issue_date="2026-01-10",
                final_date=None,
                description="Pool permit",
                source="test",
            ),
            PermitRecord(
                county="seminole",
                parcel_id="XYZ789",
                address="123 TEST ST",
                permit_number="P-FIRE-1",
                permit_type="FIRE ALARM",
                status="ISSUED",
                issue_date="2026-01-11",
                final_date=None,
                description="Fire alarm system",
                source="test",
            ),
            PermitRecord(
                county="seminole",
                parcel_id="XYZ789",
                address="123 TEST ST",
                permit_number="P-SITE-1",
                permit_type="SITEWORK",
                status="ISSUED",
                issue_date="2026-01-12",
                final_date=None,
                description="Site work grading",
                source="test",
            ),
            PermitRecord(
                county="seminole",
                parcel_id="XYZ789",
                address="123 TEST ST",
                permit_number="P-SIGN-1",
                permit_type="SIGN",
                status="ISSUED",
                issue_date="2026-01-13",
                final_date=None,
                description="Sign permit",
                source="test",
            ),
            PermitRecord(
                county="seminole",
                parcel_id="XYZ789",
                address="123 TEST ST",
                permit_number="P-TI-1",
                permit_type="TENANT IMPROVEMENT",
                status="ISSUED",
                issue_date="2026-01-14",
                final_date=None,
                description="Tenant improvement",
                source="test",
            ),
            PermitRecord(
                county="seminole",
                parcel_id="XYZ789",
                address="123 TEST ST",
                permit_number="P-REMOD-1",
                permit_type="REMODEL",
                status="ISSUED",
                issue_date="2026-01-15",
                final_date=None,
                description="Interior remodel",
                source="test",
            ),
            PermitRecord(
                county="seminole",
                parcel_id="XYZ789",
                address="123 TEST ST",
                permit_number="P-FENCE-1",
                permit_type="FENCE",
                status="ISSUED",
                issue_date="2026-01-16",
                final_date=None,
                description="Fence",
                source="test",
            ),
            PermitRecord(
                county="seminole",
                parcel_id="XYZ789",
                address="123 TEST ST",
                permit_number="P-GEN-1",
                permit_type="GENERATOR",
                status="ISSUED",
                issue_date="2026-01-17",
                final_date=None,
                description="Standby generator",
                source="test",
            ),
        ]
    )
finally:
    store.close()
PY

echo

echo "$ python -m florida_property_scraper triggers run --county seminole --connector permits_db --limit 50 --now $NOW_ISO"
python -m florida_property_scraper triggers run --county seminole --connector permits_db --limit 50 --now "$NOW_ISO"

echo

echo "$ python -m florida_property_scraper triggers rollups --county seminole --rebuilt_at $NOW_ISO"
python -m florida_property_scraper triggers rollups --county seminole --rebuilt_at "$NOW_ISO"

echo

echo "$ python - <<'PY' (verify trigger keys + rollup)"
python - <<'PY'
import os
import json

from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.taxonomy import TriggerKey

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    events = store.list_trigger_events_for_parcel(county="seminole", parcel_id="XYZ789", limit=200)
    keys = {e.get("trigger_key") for e in events}
    print({"trigger_keys": sorted(k for k in keys if k)})

    assert str(TriggerKey.PERMIT_POOL) in keys
    assert str(TriggerKey.PERMIT_FIRE) in keys
    assert str(TriggerKey.PERMIT_SITEWORK) in keys
    assert str(TriggerKey.PERMIT_SIGN) in keys
    assert str(TriggerKey.PERMIT_TENANT_IMPROVEMENT) in keys
    assert str(TriggerKey.PERMIT_REMODEL) in keys
    assert str(TriggerKey.PERMIT_FENCE) in keys
    assert str(TriggerKey.PERMIT_GENERATOR) in keys

    rollup = store.get_rollup_for_parcel(county="seminole", parcel_id="XYZ789")
    assert rollup is not None
    details = {}
    try:
        details = json.loads(str(rollup.get("details_json") or "{}"))
    except Exception:
        details = {}
    print(
        {
            "has_permits": int(rollup.get("has_permits") or 0),
            "count_critical": int(rollup.get("count_critical") or 0),
            "count_strong": int(rollup.get("count_strong") or 0),
            "count_support": int(rollup.get("count_support") or 0),
            "seller_score": int(rollup.get("seller_score") or 0),
            "groups": (details.get("groups") or []),
            "trigger_keys": (details.get("trigger_keys") or []),
        }
    )
    assert int(rollup.get("has_permits") or 0) == 1
    assert int(rollup.get("count_strong") or 0) >= 1
finally:
    store.close()
PY

echo

echo "PASS permits_db"
