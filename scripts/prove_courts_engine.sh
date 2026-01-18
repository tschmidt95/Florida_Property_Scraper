#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"

NOW_ISO="2026-01-18T00:00:00+00:00"

echo "== seed saved search + member =="
python - <<'PY'
import os
from florida_property_scraper.storage import SQLiteStore

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    ss = store.create_saved_search(
        name="SS",
        county="seminole",
        polygon_geojson={"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
        filters={},
        enrich=False,
        now_iso="2026-01-01T00:00:00+00:00",
    )
    sid = ss["id"]
    assert store.add_member_to_saved_search(
        saved_search_id=sid,
        county="seminole",
        parcel_id="XYZ789",
        source="manual",
        now_iso="2026-01-01T00:00:00+00:00",
    )
    print({"saved_search_id": sid})
finally:
    store.close()
PY

echo

echo "$ python -m florida_property_scraper triggers run --county seminole --connector courts_stub --limit 50 --now $NOW_ISO"
python -m florida_property_scraper triggers run --county seminole --connector courts_stub --limit 50 --now "$NOW_ISO"

echo

echo "$ python -m florida_property_scraper triggers rollups --county seminole --rebuilt_at $NOW_ISO"
python -m florida_property_scraper triggers rollups --county seminole --rebuilt_at "$NOW_ISO"

echo

echo "$ python - <<'PY' (verify courts triggers + inbox)"
python - <<'PY'
import os
from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.taxonomy import TriggerKey

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    ss = store.list_saved_searches(county="seminole")[0]
    sid = ss["id"]

    events = store.list_trigger_events_for_parcel(county="seminole", parcel_id="XYZ789", limit=50)
    keys = {e.get("trigger_key") for e in events}
    print({"trigger_keys": sorted(k for k in keys if k)})
    assert str(TriggerKey.DIVORCE_FILED) in keys
    assert str(TriggerKey.PROBATE_OPENED) in keys
    assert str(TriggerKey.EVICTION_FILING) in keys
    assert str(TriggerKey.FORECLOSURE_FILING) in keys

    alerts = store.list_trigger_alerts_for_parcel(county="seminole", parcel_id="XYZ789", status="open")
    alert_keys = {a.get("alert_key") for a in alerts}
    print({"trigger_alerts": sorted(k for k in alert_keys if k)})
    assert "seller_intent_critical" in alert_keys

    sync = store.sync_saved_search_inbox_from_trigger_alerts(saved_search_id=sid, now_iso="2026-01-18T00:00:01+00:00")
    assert sync.get("ok") is True
    inbox = store.list_alerts(saved_search_id=sid, limit=10)
    print({"inbox": [a.get("alert_key") for a in inbox]})
    assert len(inbox) >= 1
finally:
    store.close()
PY

echo

echo "PASS courts_stub"
