import argparse
import os
from .scraper import FloridaPropertyScraper


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
        "--api-key",
        help=(
            "ScrapingBee API key (overrides SCRAPINGBEE_API_KEY env var)"
        ),
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
        "--backend",
        choices=["scrapy", "scrapingbee"],
        default="scrapy",
        help="Which scraping backend to use (default: scrapy)",
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

    api_key = (
        args.api_key
        or (
            os.environ.get("SCRAPINGBEE_API_KEY")
            if args.backend == "scrapingbee"
            else None
        )
    )

    scraper = FloridaPropertyScraper(
        scrapingbee_api_key=api_key,
        timeout=args.timeout,
        stop_after_first=args.stop_after_first,
        log_level=args.log_level,
        demo=args.demo,
        backend=args.backend,
    )

    results = scraper.search_all_counties(query)
    print(f"Found {len(results)} properties:")
    for i, result in enumerate(results):
        print(
            f"{i+1}. {result.get('county', 'Unknown')}: "
            f"{result.get('owner', 'N/A')} - {result.get('address', 'N/A')} - "
            f"Value: {result.get('value', 'N/A')}"
        )
        if "property_id" in result:
            details = scraper.get_detailed_info(
                result.get("county", ""),
                result["property_id"],
            )
            if details:
                print(
                    f"   Details: Phone: {details.get('phone', 'N/A')}, "
                    f"Mobile: {details.get('mobile', 'N/A')}, "
                    f"Email: {details.get('email', 'N/A')}"
                )


if __name__ == "__main__":
    main()
