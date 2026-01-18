from __future__ import annotations

import json
import os
import time
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
    counties: Optional[List[str]] = None,
    run_saved_searches: bool = True,
    run_connectors: bool = True,
    run_rollups: bool = True,
    run_delivery: bool = True,
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
        county_filter = [str(c or "").strip().lower() for c in (counties or [])]
        county_filter = [c for c in county_filter if c]

        ss_rows = store.list_active_saved_searches(counties=county_filter or None, limit=max_saved_searches)
        enabled_ids = [str(r.get("id") or "").strip() for r in ss_rows]
        enabled_ids = [sid for sid in enabled_ids if sid]

        if run_saved_searches:
            for sid in enabled_ids:
                store.run_saved_search(saved_search_id=sid, now_iso=now, limit=2000)

        county_set = [str(r.get("county") or "").strip().lower() for r in ss_rows]
        county_set = [c for c in county_set if c]
        county_set = sorted(set(county_set))

        connector_results: List[Dict[str, Any]] = []
        if run_connectors and county_set:
            available = [k for k in list_connectors() if k != "fake"]
            requested = [str(k or "").strip().lower() for k in (connector_keys or [])]
            requested = [k for k in requested if k]
            keys = [k for k in (requested or available) if k in available]

            for county in county_set:
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
        if run_rollups and county_set:
            for county in county_set:
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

        delivery: Dict[str, Any] | None = None
        if run_delivery and enabled_ids:
            # Delivery scans all 'new' alerts and dedupes via alert_deliveries.
            delivery = store.deliver_new_alerts(saved_search_ids=enabled_ids, now_iso=now, limit=500)

        return {
            "ok": True,
            "now": now,
            "db": db_path,
            "saved_searches": len(enabled_ids),
            "counties": county_set,
            "connectors": connector_results,
            "rollups": rollups_results,
            "inbox": inbox_results,
            "alerts_inserted": inserted_total,
            "alerts_updated": updated_total,
            "delivery": delivery or {"ok": True, "attempted": 0, "delivered": 0, "by_channel": {}},
        }
    finally:
        store.close()


def run_scheduler(
    *,
    db_path: str,
    now_iso: Optional[str] = None,
    interval_seconds: int = 3600,
    loop: bool = False,
    counties: Optional[List[str]] = None,
    connector_limit: int = 50,
    connector_keys: Optional[List[str]] = None,
    run_saved_searches: bool = True,
    run_connectors: bool = True,
    run_rollups: bool = True,
    run_delivery: bool = True,
    max_saved_searches: int = 50,
    max_parcels: int = 500,
    lock_name: str = "scheduler:hourly",
    lock_ttl_seconds: int = 7200,
) -> Dict[str, Any]:
    """Lock-aware scheduler entrypoint used by the CLI.

    - In once mode (loop=False), runs exactly one tick and exits.
    - In loop mode (loop=True), runs forever with interval sleep.
    """

    interval = max(5, int(interval_seconds or 0))
    lock_name = (lock_name or "").strip() or "scheduler:hourly"
    lock_ttl = max(interval * 2, int(lock_ttl_seconds or 0) or (interval * 2))

    lock_store = SQLiteStore(db_path)
    try:
        lock_now = (now_iso or "").strip() or utc_now_iso()
        acquired = lock_store.acquire_scheduler_lock(
            lock_name=lock_name,
            now_iso=lock_now,
            ttl_seconds=lock_ttl,
            pid=os.getpid(),
        )
        if not acquired.get("ok"):
            return {"ok": False, "error": acquired.get("error") or "lock_error"}
        if not acquired.get("acquired"):
            return {
                "ok": False,
                "error": "lock_not_acquired",
                "held_by_pid": acquired.get("held_by_pid"),
                "heartbeat_at": acquired.get("heartbeat_at"),
            }

        if not loop:
            res = run_scheduler_tick(
                db_path=db_path,
                now_iso=now_iso,
                connector_limit=connector_limit,
                connector_keys=connector_keys,
                counties=counties,
                run_saved_searches=run_saved_searches,
                run_connectors=run_connectors,
                run_rollups=run_rollups,
                run_delivery=run_delivery,
                max_saved_searches=max_saved_searches,
                max_parcels=max_parcels,
            )
            # Keep legacy shape (tick fields at top-level) for proofs/tests.
            res = dict(res)
            res["mode"] = "once"
            res["lock"] = acquired
            res["ok"] = bool(res.get("ok"))
            return res

        # Loop mode.
        ticks: list[dict[str, Any]] = []
        while True:
            t_now = utc_now_iso() if not (now_iso or "").strip() else str(now_iso)
            lock_store.refresh_scheduler_lock(lock_name=lock_name, now_iso=t_now, pid=os.getpid())

            res = run_scheduler_tick(
                db_path=db_path,
                now_iso=t_now,
                connector_limit=connector_limit,
                connector_keys=connector_keys,
                counties=counties,
                run_saved_searches=run_saved_searches,
                run_connectors=run_connectors,
                run_rollups=run_rollups,
                run_delivery=run_delivery,
                max_saved_searches=max_saved_searches,
                max_parcels=max_parcels,
            )
            ticks.append(res)
            if len(ticks) > 3:
                ticks = ticks[-3:]

            lock_store.refresh_scheduler_lock(lock_name=lock_name, now_iso=utc_now_iso(), pid=os.getpid())
            time.sleep(interval)
    finally:
        try:
            lock_store.release_scheduler_lock(lock_name=lock_name, pid=os.getpid())
        finally:
            lock_store.close()


def dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, indent=2, default=str) + "\n"
