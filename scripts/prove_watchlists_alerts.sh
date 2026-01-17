#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

export LEADS_SQLITE_PATH="$TMP_DIR/leads.sqlite"

NOW_ISO="2026-01-01T00:00:00+00:00"

echo "$ python - <<'PY' (seed watchlist + trigger_alert + inbox sync)"
python - <<'PY'
import os

from florida_property_scraper.storage import SQLiteStore

store = SQLiteStore(os.environ.get("LEADS_SQLITE_PATH", "./leads.sqlite"))
try:
    ss = store.create_saved_search(
        name="Demo Saved Search",
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
        parcel_id="P-DEMO-1",
        source="manual",
        now_iso="2026-01-01T00:00:00+00:00",
    )
    assert ok is True

    store.upsert_trigger_alert(
        county="orange",
        parcel_id="P-DEMO-1",
        alert_key="seller_intent_critical",
        severity=5,
        first_seen_at="2026-01-01T00:00:00+00:00",
        last_seen_at="2026-01-01T00:00:00+00:00",
        status="open",
        trigger_event_ids=[101, 102],
        details={"rule": "critical>=1", "seller_score": 100, "trigger_keys": ["tax_delinquent"]},
    )

    r1 = store.sync_saved_search_inbox_from_trigger_alerts(saved_search_id=sid, now_iso="2026-01-01T00:00:01+00:00")
    assert r1.get("ok") is True
    assert int(r1.get("inserted") or 0) == 1

    # Dedupe should prevent duplicates for the same last_seen_at.
    r2 = store.sync_saved_search_inbox_from_trigger_alerts(saved_search_id=sid, now_iso="2026-01-01T00:00:02+00:00")
    assert r2.get("ok") is True
    assert int(r2.get("inserted") or 0) == 0

    alerts = store.list_alerts(saved_search_id=sid, limit=10)
    assert len(alerts) == 1
    a = alerts[0]
    assert a.get("alert_key") == "seller_intent_critical"
    assert a.get("parcel_id") == "P-DEMO-1"
    assert a.get("status") == "new"

    assert store.mark_alert_read(alert_id=int(a.get("id") or 0)) is True
    alerts2 = store.list_alerts(saved_search_id=sid, status="read", limit=10)
    assert len(alerts2) == 1
finally:
    store.close()

print({"ok": True, "watchlist_alerts": "PASS"})
PY

echo

echo "PASS"
