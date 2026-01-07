import argparse
import csv
import json
from pathlib import Path

from .county_router import (
    build_start_urls,
    canonicalize_county_name,
    enabled_counties,
    get_county_entry,
)
from .scraper import FloridaPropertyScraper
from .schema import REQUIRED_FIELDS, normalize_item
from .security import neutralize_csv_field, sanitize_path
from .storage import SQLiteStorage


def main():
    parser = argparse.ArgumentParser(
        description="Florida property scraper CLI",
    )

    parser.add_argument(
        "--query",
        help="Owner name or address to search",
        required=False,
    )

    parser.add_argument(
        "--name",
        help="Owner name (paired with --address)",
        required=False,
    )

    parser.add_argument(
        "--address",
        help="Owner address (paired with --name)",
        required=False,
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Request timeout in seconds",
    )

    parser.add_argument(
        "--no-stop",
        dest="stop_after_first",
        action="store_false",
        help=(
            "Search all counties instead of stopping after first result"
        ),
    )

    parser.add_argument(
        "--log-level",
        default=None,
        help="Logging level (DEBUG, INFO, etc.)",
    )

    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run in demo mode with canned responses (no network)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run live requests against county sites (network required)",
    )
    parser.add_argument(
        "--debug-html",
        action="store_true",
        help="Emit raw HTML capture when live parsing yields no rows",
    )
    parser.add_argument(
        "--county",
        default=None,
        help="Single county slug to search (e.g., broward)",
    )
    parser.add_argument(
        "--input-csv",
        default=None,
        help="CSV file with a 'query' column for batch runs",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned counties, URLs, and spider names without scraping",
    )
    parser.add_argument(
        "--per-county-limit",
        type=int,
        default=None,
        help="Maximum number of items per county",
    )
    parser.add_argument(
        "--log-json",
        action="store_true",
        help="Emit one JSON log line per county",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=None,
        help="Delay between counties in milliseconds",
    )
    parser.add_argument(
        "--counties",
        help="Comma-separated list of counties to search",
        default=None,
    )

    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Maximum number of items to scrape before stopping",
    )

    parser.add_argument(
        "--store",
        default="./leads.sqlite",
        help="SQLite path for storing scraped results",
    )

    parser.add_argument(
        "--no-store",
        action="store_true",
        help="Disable storing results to SQLite",
    )

    parser.add_argument(
        "--output",
        default=None,
        help="Write results to a file (path)",
    )

    parser.add_argument(
        "--format",
        choices=["jsonl", "json", "csv"],
        default="jsonl",
        help="Output format for --output",
    )

    parser.add_argument(
        "--no-output",
        action="store_true",
        help="Disable writing results to --output",
    )

    parser.add_argument(
        "--append-output",
        dest="append_output",
        action="store_true",
        default=True,
        help="Append to output file if it exists",
    )

    parser.add_argument(
        "--no-append-output",
        dest="append_output",
        action="store_false",
        help="Overwrite output file instead of appending",
    )

    args = parser.parse_args()

    queries = []
    if args.input_csv:
        csv_path = Path(args.input_csv)
        if not csv_path.exists():
            parser.error("--input-csv file does not exist")
        with csv_path.open(encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                value = (row.get("query") or "").strip()
                if value:
                    queries.append(value)
        if not queries:
            parser.error("--input-csv must contain at least one query")
    else:
        # Determine query from either --query or --name + --address
        if args.name:
            if not args.address:
                parser.error("--address is required when --name is provided")
            query = f"{args.name} {args.address}"
        elif args.query:
            query = args.query
        else:
            # Interactive prompts - prefer name+address pair
            name = input(
                "Enter owner name (leave blank to enter a single query): "
            ).strip()
            if name:
                address = input(
                    "Enter owner address (required when name is provided): "
                ).strip()
                if not address:
                    parser.error("Address is required when name is provided")
                query = f"{name} {address}"
            else:
                query = input("Enter owner name or address to search: ")
        queries = [query]

    counties = []
    if args.county:
        counties.append(canonicalize_county_name(args.county))
    if args.counties:
        counties.extend(
            canonicalize_county_name(c)
            for c in args.counties.split(",")
            if c.strip()
        )
    counties = [c for c in counties if c]
    if not counties:
        counties = None
    if args.live and not counties:
        parser.error("--live requires at least one county via --county or --counties")

    if counties:
        slugs = counties
    else:
        slugs = enabled_counties()

    query_for_filter = queries[0] if queries else ""
    is_address_query = False
    if args.name:
        is_address_query = False
    elif any(ch.isdigit() for ch in query_for_filter):
        is_address_query = True
    filtered = []
    for slug in slugs:
        entry = get_county_entry(slug)
        if is_address_query and not entry.get("supports_address_search", True):
            continue
        if not is_address_query and not entry.get("supports_owner_search", True):
            continue
        filtered.append(slug)
    slugs = filtered
    if args.dry_run:
        print(f"Counties: {', '.join(slugs)}")
        for slug in slugs:
            entry = get_county_entry(slug)
            urls = build_start_urls(slug, queries[0] if queries else "")
            print(f"{slug}: {entry.get('spider_key')} -> {urls}")
            if args.log_json:
                print(
                    json.dumps(
                        {
                            "county": slug,
                            "spider": entry.get("spider_key"),
                            "start_urls": urls,
                            "items_found": 0,
                            "status": "skipped",
                        }
                    )
                )
        summary = {
            "total_counties": len(slugs),
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": len(slugs),
            "total_items": 0,
        }
        print(json.dumps(summary))
        return

    scraper = FloridaPropertyScraper(
        timeout=args.timeout,
        stop_after_first=args.stop_after_first,
        log_level=args.log_level,
        demo=args.demo,
        counties=counties,
        max_items=args.max_items,
        live=args.live,
        debug_html=args.debug_html,
        per_county_limit=args.per_county_limit,
        delay_ms=args.delay_ms,
    )

    all_results = []
    project_root = Path(__file__).resolve().parents[2]
    if not args.no_store:
        store_path = sanitize_path(args.store, project_root)
        storage = SQLiteStorage(str(store_path))
    else:
        storage = None
    all_log_entries = []
    for query in queries:
        results = scraper.search_all_counties(
            query,
            max_items=args.max_items,
            per_county_limit=args.per_county_limit,
            counties=slugs,
        )
        results = [normalize_item(item) for item in results]
        if args.max_items:
            results = results[: args.max_items]
        all_results.extend(results)
        if storage:
            storage.save_items(results)
        all_log_entries.extend(getattr(scraper, "last_log_entries", []))
    if storage:
        storage.close()

    if args.output and not args.no_output:
        output_path = sanitize_path(args.output, project_root)
        if args.format == "jsonl":
            mode = "a" if args.append_output else "w"
            with output_path.open(mode, encoding="utf-8") as handle:
                for item in all_results:
                    handle.write(json.dumps(item) + "\n")
        elif args.format == "json":
            existing = []
            if args.append_output and output_path.exists():
                existing = json.loads(output_path.read_text(encoding="utf-8"))
                if not isinstance(existing, list):
                    existing = []
            payload = existing + all_results if args.append_output else all_results
            output_path.write_text(json.dumps(payload), encoding="utf-8")
        elif args.format == "csv":
            write_header = True
            if args.append_output and output_path.exists():
                write_header = False
            mode = "a" if args.append_output else "w"
            with output_path.open(mode, newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=REQUIRED_FIELDS)
                if write_header:
                    writer.writeheader()
                for item in all_results:
                    row = {
                        k: neutralize_csv_field(item.get(k, ""))
                        for k in REQUIRED_FIELDS
                    }
                    writer.writerow(row)
    print(f"Found {len(all_results)} properties:")
    for i, result in enumerate(all_results):
        print(
            f"{i+1}. {result.get('county', 'Unknown')}: "
            f"{result.get('owner', 'N/A')} - {result.get('address', 'N/A')}"
        )
    if args.log_json:
        for entry in all_log_entries:
            print(json.dumps(entry))
    if scraper.failures:
        print("Failures:")
        for failure in scraper.failures:
            print(
                f"{failure.get('county')}: {failure.get('error')} "
                f"(query={failure.get('query')})"
            )
    summary = {
        "total_counties": len(slugs),
        "attempted": sum(1 for e in all_log_entries if e["status"] != "skipped"),
        "succeeded": sum(1 for e in all_log_entries if e["status"] == "success"),
        "failed": sum(1 for e in all_log_entries if e["status"] == "failed"),
        "skipped": sum(1 for e in all_log_entries if e["status"] == "skipped"),
        "total_items": len(all_results),
    }
    print(json.dumps(summary))


def _safe_main():
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        raise SystemExit(1)


if __name__ == "__main__":
    _safe_main()
