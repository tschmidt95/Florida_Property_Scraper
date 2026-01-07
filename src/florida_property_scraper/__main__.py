import argparse
import csv
import json
from pathlib import Path

from .scraper import FloridaPropertyScraper
from .schema import REQUIRED_FIELDS, normalize_item
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

    counties = None
    if args.counties:
        counties = [c.strip() for c in args.counties.split(",") if c.strip()]

    scraper = FloridaPropertyScraper(
        timeout=args.timeout,
        stop_after_first=args.stop_after_first,
        log_level=args.log_level,
        demo=args.demo,
        counties=counties,
        max_items=args.max_items,
    )

    results = scraper.search_all_counties(query, max_items=args.max_items)
    results = [normalize_item(item) for item in results]
    if args.max_items:
        results = results[: args.max_items]

    if not args.no_store:
        storage = SQLiteStorage(args.store)
        storage.save_items(results)
        storage.close()

    if args.output and not args.no_output:
        output_path = Path(args.output)
        if args.format == "jsonl":
            mode = "a" if args.append_output else "w"
            with output_path.open(mode, encoding="utf-8") as handle:
                for item in results:
                    handle.write(json.dumps(item) + "\n")
        elif args.format == "json":
            existing = []
            if args.append_output and output_path.exists():
                existing = json.loads(output_path.read_text(encoding="utf-8"))
                if not isinstance(existing, list):
                    existing = []
            payload = existing + results if args.append_output else results
            output_path.write_text(json.dumps(payload), encoding="utf-8")
        elif args.format == "csv":
            mode = "a" if args.append_output and output_path.exists() else "w"
            with output_path.open(mode, newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=REQUIRED_FIELDS)
                if mode == "w":
                    writer.writeheader()
                for item in results:
                    writer.writerow({k: item.get(k, "") for k in REQUIRED_FIELDS})
    print(f"Found {len(results)} properties:")
    for i, result in enumerate(results):
        print(
            f"{i+1}. {result.get('county', 'Unknown')}: "
            f"{result.get('owner', 'N/A')} - {result.get('address', 'N/A')}"
        )


if __name__ == "__main__":
    main()
