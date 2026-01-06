import argparse

from florida_property_scraper.scraper import FloridaPropertyScraper


def main():
    parser = argparse.ArgumentParser(description="Florida property scraper CLI (Scrapy)")
    parser.add_argument("--query", help="Owner name or address to search", required=False)
    parser.add_argument("--counties", help="Comma-separated county names to search", required=False)
    parser.add_argument(
        "--output",
        default="./results.jsonl",
        help="Output path for JSON/CSV export (default: ./results.jsonl)",
    )
    parser.add_argument("--format", default="jsonl", help="Output format (jsonl, json, csv)")
    parser.add_argument(
        "--no-output",
        action="store_true",
        help="Disable file output even if --output is set",
    )
    parser.add_argument(
        "--append-output",
        dest="append_output",
        action="store_true",
        default=True,
        help="Append JSONL output as items are scraped (default: enabled)",
    )
    parser.add_argument(
        "--no-append-output",
        dest="append_output",
        action="store_false",
        help="Disable JSONL append mode and use Scrapy feed export instead",
    )
    parser.add_argument("--max-items", type=int, default=None, help="Stop after collecting N items")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, etc.)")
    parser.add_argument("--no-robots", dest="obey_robots", action="store_false", help="Ignore robots.txt")
    parser.add_argument("--no-forms", dest="allow_forms", action="store_false", help="Skip form discovery for counties without templates")
    parser.add_argument(
        "--store",
        default="./leads.sqlite",
        help="SQLite path for storing normalized leads (default: ./leads.sqlite)",
    )
    parser.add_argument(
        "--no-store",
        action="store_true",
        help="Disable SQLite storage even if --store is set",
    )
    parser.add_argument("--webhook-url", help="Webhook URL for pushing normalized leads", required=False)
    parser.add_argument("--zoho-sync", action="store_true", help="Send leads to Zoho CRM (requires ZOHO_ACCESS_TOKEN)")
    args = parser.parse_args()

    if args.query:
        query = args.query
    else:
        query = input("Enter owner name or address to search: ")

    scraper = FloridaPropertyScraper(log_level=args.log_level, obey_robots=args.obey_robots)
    results = scraper.search(
        query=query,
        counties=args.counties,
        output_path=None if args.no_output else args.output,
        output_format=args.format,
        append_output=args.append_output,
        max_items=args.max_items,
        allow_forms=args.allow_forms,
        storage_path=None if args.no_store else args.store,
        webhook_url=args.webhook_url,
        zoho_sync=args.zoho_sync,
    )
    print(f"Found {len(results)} properties")


if __name__ == "__main__":
    main()
