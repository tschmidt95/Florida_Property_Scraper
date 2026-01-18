from __future__ import annotations

import argparse
from typing import List, Optional

from florida_property_scraper.scheduler.runner import dumps, run_scheduler


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="florida_property_scraper scheduler")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run scheduler once or in a loop")
    p_run.add_argument("--db", default="./leads.sqlite", help="SQLite DB path")
    p_run.add_argument("--now", default=None, help="Override current time (ISO8601); recommended only with --once")
    p_run.add_argument("--interval-seconds", type=int, default=3600, help="Loop interval in seconds")

    mode = p_run.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run one tick and exit (default)")
    mode.add_argument("--loop", action="store_true", help="Run forever with sleep interval")

    p_run.add_argument("--lock-name", default="scheduler:hourly", help="Scheduler lock name")
    p_run.add_argument("--lock-ttl-seconds", type=int, default=7200, help="Lock stale timeout; allows takeover")

    p_run.add_argument("--connector-limit", type=int, default=50)
    p_run.add_argument(
        "--connectors",
        default=None,
        help="Comma-separated connector keys (default: all builtin stubs)",
    )
    p_run.add_argument(
        "--counties",
        default=None,
        help="Comma-separated counties to run (filters enabled saved searches)",
    )

    p_run.add_argument("--no-saved-searches", action="store_true")
    p_run.add_argument("--no-connectors", action="store_true")
    p_run.add_argument("--no-rollups", action="store_true")
    p_run.add_argument("--no-delivery", action="store_true")
    p_run.add_argument("--max-saved-searches", type=int, default=50)
    p_run.add_argument("--max-parcels", type=int, default=500)

    args = parser.parse_args(argv)

    if args.cmd == "run":
        connector_keys = None
        if args.connectors:
            connector_keys = [p.strip() for p in str(args.connectors).split(",") if p.strip()]

        county_keys = None
        if args.counties:
            county_keys = [p.strip().lower() for p in str(args.counties).split(",") if p.strip()]

        loop = bool(args.loop)
        # Default is once unless --loop was explicitly requested.
        if not args.once and not args.loop:
            loop = False

        res = run_scheduler(
            db_path=str(args.db),
            now_iso=args.now,
            interval_seconds=int(args.interval_seconds or 3600),
            loop=loop,
            connector_limit=int(args.connector_limit or 50),
            connector_keys=connector_keys,
            counties=county_keys,
            run_saved_searches=not bool(args.no_saved_searches),
            run_connectors=not bool(args.no_connectors),
            run_rollups=not bool(args.no_rollups),
            run_delivery=not bool(args.no_delivery),
            max_saved_searches=int(args.max_saved_searches or 50),
            max_parcels=int(args.max_parcels or 500),
            lock_name=str(args.lock_name or "scheduler:hourly"),
            lock_ttl_seconds=int(args.lock_ttl_seconds or 7200),
        )
        print(dumps(res), end="")
        return 0 if res.get("ok") else 2

    return 2
