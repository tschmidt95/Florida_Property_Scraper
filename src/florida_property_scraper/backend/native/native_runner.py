import argparse
import json
import os
import sys
import urllib.parse

from .engine import NativeEngine
from .parsers import get_parser
from ..scrapy_runner import resolve_spider_name
from ...routers.registry import get_entry


def resolve_parser(parser_name):
    slug = resolve_spider_name(parser_name)
    return get_parser(slug), slug


def run_on_fixture(county_slug, fixture_path, max_items=None, per_county_limit=None):
    parser = get_parser(county_slug)
    engine = NativeEngine(max_items=max_items, per_county_limit=per_county_limit)
    start_url = f"file://{os.path.abspath(fixture_path)}"
    return engine.run([start_url], parser, county_slug, dry_run=True)


def _allowed_hosts(entry):
    if entry and entry.get("url_template"):
        parsed = urllib.parse.urlparse(entry["url_template"])
        if parsed.hostname:
            return {parsed.hostname}
    return None


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--county", required=True)
    parser.add_argument("--start-url", action="append", default=[])
    parser.add_argument("--max-items", type=int, default=None)
    parser.add_argument("--per-county-limit", type=int, default=None)
    args = parser.parse_args(argv)
    entry = get_entry("fl", args.county)
    parser_fn = get_parser(args.county)
    engine = NativeEngine(max_items=args.max_items, per_county_limit=args.per_county_limit)
    allowed_hosts = _allowed_hosts(entry)

    def log_fn(payload):
        sys.stderr.write(json.dumps(payload) + "\n")

    items = engine.run(args.start_url, parser_fn, args.county, allowed_hosts=allowed_hosts, log_fn=log_fn)
    sys.stdout.write(json.dumps(items))


if __name__ == "__main__":
    main()
