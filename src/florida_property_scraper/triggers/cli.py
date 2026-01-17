from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from florida_property_scraper.storage import SQLiteStore

from . import connectors as _connectors  # noqa: F401  (registers connectors)
from .connectors.base import get_connector, list_connectors
from .engine import run_connector_once, utc_now_iso


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="florida-property-scraper triggers")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="List available trigger connectors")

    p_run = sub.add_parser("run", help="Poll a connector and write triggers/alerts")
    p_run.add_argument("--db", default=os.getenv("LEADS_SQLITE_PATH", "./leads.sqlite"))
    p_run.add_argument("--county", required=True)
    p_run.add_argument("--connector", default="fake")
    p_run.add_argument("--limit", type=int, default=50)
    p_run.add_argument("--now", default=None, help="Override now (ISO8601)")

    p_rollups = sub.add_parser("rollups", help="Rebuild parcel trigger rollups (offline)")
    p_rollups.add_argument("--db", default=os.getenv("LEADS_SQLITE_PATH", "./leads.sqlite"))
    p_rollups.add_argument("--county", required=True)
    p_rollups.add_argument("--rebuilt_at", default=None, help="Override rebuilt_at timestamp (ISO8601)")

    args = parser.parse_args(argv)

    if args.cmd == "list":
        print(json.dumps({"connectors": list_connectors()}))
        return 0

    if args.cmd == "run":
        db_path = Path(str(args.db))
        db_path.parent.mkdir(parents=True, exist_ok=True)

        connector = get_connector(str(args.connector))
        store = SQLiteStore(str(db_path))
        try:
            out = run_connector_once(
                store=store,
                connector=connector,
                county=str(args.county),
                now_iso=str(args.now).strip() if args.now else utc_now_iso(),
                limit=int(args.limit),
            )
        finally:
            store.close()

        print(json.dumps(out))
        return 0

    if args.cmd == "rollups":
        db_path = Path(str(args.db))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SQLiteStore(str(db_path))
        try:
            out = store.rebuild_parcel_trigger_rollups(
                county=str(args.county),
                rebuilt_at=str(args.rebuilt_at).strip() if args.rebuilt_at else utc_now_iso(),
            )
        finally:
            store.close()

        print(json.dumps(out))
        return 0

    parser.error("Unknown command")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
