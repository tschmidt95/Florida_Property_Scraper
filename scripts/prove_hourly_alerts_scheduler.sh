#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"

NOW_ISO="2026-01-02T00:00:00+00:00"

echo "Starting hourly alerts scheduler proof using DB: $LEADS_SQLITE_PATH"
echo

echo "$ python - <<'PY' (seed saved search + member + permit)"
python - <<'PY'
import os

from florida_property_scraper.storage import SQLiteStore

db = os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite")
store = SQLiteStore(db)
try:
    ss = store.create_saved_search(
        name="Hourly Alerts SS",
        county="orange",
        polygon_geojson={"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
        filters={},
        enrich=False,
        now_iso="2026-01-01T00:00:00+00:00",
    )
    sid = ss["id"]

    ok = store.add_member_to_saved_search(
        saved_search_id=sid,
        county="orange",
        parcel_id="P-HOURLY-1",
        source="manual",
        now_iso="2026-01-01T00:00:00+00:00",
    )
    assert ok is True

    # Seed a permit so permits_db emits trigger events.
    store.conn.execute(
        """
        INSERT INTO permits(
            county, parcel_id, address, permit_number, permit_type, status,
            issue_date, final_date, description, source, raw
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "orange",
            "P-HOURLY-1",
            "123 TEST ST",
            "PERMIT-P-HOURLY-1",
            "ROOF",
            "ISSUED",
            "2026-01-01",
            None,
            "REROOF",
            "test",
            "{}",
        ),
    )
    store.conn.commit()

    print({"ok": True, "saved_search_id": sid})
finally:
    store.close()
PY

echo

echo "$ python -m florida_property_scraper scheduler run --once (tick #1)"
OUT1="$(python -m florida_property_scraper scheduler run \
  --db "$LEADS_SQLITE_PATH" \
  --once \
  --now "$NOW_ISO" \
  --counties orange \
  --connectors permits_db \
  --no-saved-searches \
  --connector-limit 50)"

echo "$OUT1"
echo

echo "$ python -m florida_property_scraper scheduler run --once (tick #2, same now -> dedupe delivery)"
OUT2="$(python -m florida_property_scraper scheduler run \
  --db "$LEADS_SQLITE_PATH" \
  --once \
  --now "$NOW_ISO" \
  --counties orange \
  --connectors permits_db \
  --no-saved-searches \
  --connector-limit 50)"

echo "$OUT2"
echo

echo "$ python - <<'PY' (assert inbox > 0 and delivery dedupe)"
python - <<'PY'
import json
import os
import subprocess

from florida_property_scraper.storage import SQLiteStore

db = os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite")
store = SQLiteStore(db)
try:
    row = store.conn.execute("SELECT COUNT(*) AS c FROM alerts_inbox").fetchone()
    assert int(row["c"] or 0) > 0

    # Ensure delivery ledger contains exactly one console delivery for the alert fingerprint.
    row2 = store.conn.execute(
        "SELECT COUNT(*) AS c FROM alert_deliveries WHERE channel='console' AND status='sent'"
    ).fetchone()
    assert int(row2["c"] or 0) == 1
finally:
    store.close()

print({"ok": True, "hourly_alerts_scheduler": "PASS"})
PY

echo

echo "PASS hourly_alerts_scheduler"
