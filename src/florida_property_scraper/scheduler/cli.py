from __future__ import annotations

import argparse
from typing import List, Optional

from florida_property_scraper.scheduler.runner import dumps, run_scheduler_tick


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="florida_property_scraper scheduler")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run one scheduler tick")
    p_run.add_argument("--db", default="./leads.sqlite", help="SQLite DB path")
    p_run.add_argument("--now", default=None, help="Override current time (ISO8601)")
    p_run.add_argument("--connector-limit", type=int, default=50)
    p_run.add_argument(
        "--connectors",
        default=None,
        help="Comma-separated connector keys (default: all builtin stubs)",
    )
    p_run.add_argument("--no-saved-searches", action="store_true")
    p_run.add_argument("--no-connectors", action="store_true")
    p_run.add_argument("--no-rollups", action="store_true")
    p_run.add_argument("--max-saved-searches", type=int, default=50)
    p_run.add_argument("--max-parcels", type=int, default=500)

    args = parser.parse_args(argv)

    if args.cmd == "run":
        connector_keys = None
        if args.connectors:
            connector_keys = [p.strip() for p in str(args.connectors).split(",") if p.strip()]

        res = run_scheduler_tick(
            db_path=str(args.db),
            now_iso=args.now,
            connector_limit=int(args.connector_limit or 50),
            connector_keys=connector_keys,
            run_saved_searches=not bool(args.no_saved_searches),
            run_connectors=not bool(args.no_connectors),
            run_rollups=not bool(args.no_rollups),
            max_saved_searches=int(args.max_saved_searches or 50),
            max_parcels=int(args.max_parcels or 500),
        )
        print(dumps(res), end="")
        return 0 if res.get("ok") else 2

    return 2
