from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.engine import run_connector_once, utc_now_iso
from florida_property_scraper.triggers.connectors.base import get_connector, list_connectors


def run_scheduler_tick(
    *,
    db_path: str,
    now_iso: Optional[str] = None,
    connector_limit: int = 50,
    connector_keys: Optional[List[str]] = None,
    run_saved_searches: bool = True,
    run_connectors: bool = True,
    run_rollups: bool = True,
    max_saved_searches: int = 50,
    max_parcels: int = 500,
) -> Dict[str, Any]:
    """Run one deterministic scheduler tick.

    This is intended for:
    - CLI usage (hourly cron)
    - Proof scripts
    - Unit/integration tests
    """

    # Ensure builtin connectors are registered.
    import florida_property_scraper.triggers.connectors  # noqa: F401

    now = (now_iso or "").strip() or utc_now_iso()
    connector_limit = max(1, min(int(connector_limit or 50), 500))
    max_saved_searches = max(1, min(int(max_saved_searches or 50), 200))
    max_parcels = max(1, min(int(max_parcels or 500), 5000))

    store = SQLiteStore(db_path)
    try:
        ss_rows = store.conn.execute(
            "SELECT id, county FROM saved_searches WHERE is_enabled=1 ORDER BY updated_at DESC"
        ).fetchall()
        enabled_ids = [str(r["id"] or "").strip() for r in ss_rows]
        enabled_ids = [sid for sid in enabled_ids if sid][:max_saved_searches]

        if run_saved_searches:
            for sid in enabled_ids:
                store.run_saved_search(saved_search_id=sid, now_iso=now, limit=2000)

        counties = [str(r["county"] or "").strip().lower() for r in ss_rows]
        counties = [c for c in counties if c]
        counties = sorted(set(counties))

        connector_results: List[Dict[str, Any]] = []
        if run_connectors and counties:
            available = [k for k in list_connectors() if k != "fake"]
            requested = [str(k or "").strip().lower() for k in (connector_keys or [])]
            requested = [k for k in requested if k]
            keys = [k for k in (requested or available) if k in available]

            for county in counties:
                for ck in keys:
                    try:
                        connector_results.append(
                            run_connector_once(
                                store=store,
                                connector=get_connector(ck),
                                county=county,
                                now_iso=now,
                                limit=connector_limit,
                            )
                        )
                    except Exception as e:
                        connector_results.append(
                            {
                                "ok": False,
                                "county": county,
                                "connector": ck,
                                "error": str(e),
                            }
                        )

        rollups_results: List[Dict[str, Any]] = []
        if run_rollups and counties:
            for county in counties:
                try:
                    rollups_results.append(
                        {
                            "ok": True,
                            "county": county,
                            "result": store.rebuild_parcel_trigger_rollups(county=county, rebuilt_at=now),
                        }
                    )
                except Exception as e:
                    rollups_results.append({"ok": False, "county": county, "error": str(e)})

        inbox_results: List[Dict[str, Any]] = []
        inserted_total = 0
        updated_total = 0
        for sid in enabled_ids:
            r = store.sync_saved_search_inbox_from_trigger_alerts(
                saved_search_id=sid,
                now_iso=now,
                max_parcels=max_parcels,
            )
            inbox_results.append(r)
            inserted_total += int(r.get("inserted") or 0)
            updated_total += int(r.get("updated") or 0)

        return {
            "ok": True,
            "now": now,
            "db": db_path,
            "saved_searches": len(enabled_ids),
            "counties": counties,
            "connectors": connector_results,
            "rollups": rollups_results,
            "inbox": inbox_results,
            "alerts_inserted": inserted_total,
            "alerts_updated": updated_total,
        }
    finally:
        store.close()


def dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, indent=2, default=str) + "\n"
