import argparse
import os
import csv
import json
import sys
from pathlib import Path

from .routers.fl import canonicalize_jurisdiction_name as canonicalize_county_name
from .routers.registry import (
    build_start_urls,
    enabled_jurisdictions,
    get_entry,
)
from .scraper import FloridaPropertyScraper
from .schema import REQUIRED_FIELDS, normalize_item
from .security import neutralize_csv_field, sanitize_path
from .storage import SQLiteStorage
from .backend.native.extract import split_result_blocks


def _find_fixture(county):
    fixtures = [
        Path("tests/fixtures") / f"{county}_sample.html",
        Path("tests/fixtures") / f"{county}_realistic.html",
    ]
    for path in fixtures:
        if path.exists():
            return path
    return None


def run_human_command(argv):
    parser = argparse.ArgumentParser(description="Run a single county scrape")
    parser.add_argument("--backend", choices=["scrapy", "native"], default="native")
    parser.add_argument("--county", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--mode", choices=["fixture", "live"], default="fixture")
    parser.add_argument("--max-items", type=int, default=5)
    args = parser.parse_args(argv)

    if args.backend == "native":
        from florida_property_scraper.backend.native_adapter import NativeAdapter

        adapter = NativeAdapter()
    else:
        from florida_property_scraper.backend.scrapy_adapter import ScrapyAdapter

        adapter = ScrapyAdapter(live=(args.mode == "live"))

    blocks = []
    parsed_count = 0
    if args.mode == "fixture":
        fixture = _find_fixture(args.county)
        if not fixture:
            raise FileNotFoundError(f"No fixture found for {args.county}")
        html = fixture.read_text(encoding="utf-8")
        blocks = split_result_blocks(html)
        parsed_count = len(blocks)
        start_urls = [f"file://{fixture.resolve()}"]
        items = adapter.search(
            query=args.query,
            start_urls=start_urls,
            spider_name=f"{args.county}_spider",
            max_items=args.max_items,
            live=False,
            county_slug=args.county,
        )
    else:
        items = adapter.search(
            query=args.query,
            spider_name=f"{args.county}_spider",
            max_items=args.max_items,
            live=True,
            county_slug=args.county,
        )
        parsed_count = len(items)

    print("Owner | Address")
    for item in items[:5]:
        print(f"{item.get('owner','')} | {item.get('address','')}")
    print(f"Blocks found: {len(blocks)}")
    print(f"Records parsed: {parsed_count}")
    print(f"Records validated: {len(items)}")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        run_human_command(sys.argv[2:])
        return
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
        "--max-blocks-per-response",
        type=int,
        default=None,
        help="Max result blocks parsed per response (native backend)",
    )
    parser.add_argument(
        "--global-concurrency",
        type=int,
        default=None,
        help="Global async concurrency (native backend)",
    )
    parser.add_argument(
        "--per-host-concurrency",
        type=int,
        default=None,
        help="Per-host async concurrency (native backend)",
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
        "--state",
        default="fl",
        help="State identifier (default: fl)",
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

    parser.add_argument(
        "--no-robots",
        dest="obey_robots",
        action="store_false",
        help="Ignore robots.txt",
    )
    parser.add_argument(
        "--no-forms",
        dest="allow_forms",
        action="store_false",
        help="Skip form discovery for counties without templates",
    )
    parser.add_argument(
        "--webhook-url",
        help="Webhook URL for pushing normalized leads",
        required=False,
    )
    parser.add_argument(
        "--zoho-sync",
        action="store_true",
        help="Send leads to Zoho CRM (requires ZOHO_ACCESS_TOKEN)",
    )

    parser.add_argument("--backend", choices=["scrapy", "native"], default="scrapy")
    args = parser.parse_args()
    os.environ["FL_SCRAPER_BACKEND"] = args.backend
    if args.max_blocks_per_response is not None:
        os.environ["MAX_BLOCKS_PER_RESPONSE"] = str(args.max_blocks_per_response)
    if args.global_concurrency is not None:
        os.environ["GLOBAL_CONCURRENCY"] = str(args.global_concurrency)
    if args.per_host_concurrency is not None:
        os.environ["PER_HOST_CONCURRENCY"] = str(args.per_host_concurrency)
    if args.backend == "native":
        from florida_property_scraper.backend.native_adapter import NativeAdapter
        import florida_property_scraper.backend.scrapy_adapter as scrapy_adapter

        scrapy_adapter.ScrapyAdapter = NativeAdapter

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
        if args.name:
            if not args.address:
                parser.error("--address is required when --name is provided")
            query = f"{args.name} {args.address}"
        elif args.query:
            query = args.query
        else:
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
        slugs = enabled_jurisdictions(args.state)

    query_for_filter = queries[0] if queries else ""
    is_address_query = False
    if args.name:
        is_address_query = False
    elif any(ch.isdigit() for ch in query_for_filter):
        is_address_query = True
    filtered = []
    for slug in slugs:
        entry = get_entry(args.state, slug)
        if is_address_query and not entry.get("supports_address_search", True):
            continue
        if not is_address_query and not entry.get("supports_owner_search", True):
            continue
        filtered.append(slug)
    slugs = filtered
    if args.dry_run:
        print(f"Counties: {', '.join(slugs)}")
        for slug in slugs:
            entry = get_entry(args.state, slug)
            urls = build_start_urls(args.state, slug, queries[0] if queries else "")
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
        state=args.state,
        backend=args.backend,
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
