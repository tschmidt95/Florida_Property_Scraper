"""Helper CLI to run a Scrapy spider in a subprocess and emit JSON results to stdout.

This isolates Twisted reactor usage to the subprocess so the main test process can run
multiple spiders sequentially without reactor conflicts.
"""
import argparse
import json
import sys


def resolve_spider_class(spider_name, spiders_registry=None):
    if spiders_registry is None:
        from .spiders import SPIDERS

        spiders_registry = SPIDERS

    raw_name = spider_name
    normalized_name = (
        raw_name[: -len("_spider")] if raw_name.endswith("_spider") else raw_name
    )

    SpiderCls = spiders_registry.get(raw_name) or spiders_registry.get(normalized_name)
    if not SpiderCls:
        raise KeyError(f"Unknown spider: {spider_name}")
    return SpiderCls


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--spider-name', required=True)
    parser.add_argument('--start-urls', required=True, help='JSON array of start URLs')
    parser.add_argument('--max-items', type=int, default=None)
    parser.add_argument('--debug-html', action='store_true')
    parser.add_argument('--query', default='')
    parser.add_argument('--pagination', default='none')
    parser.add_argument('--page-param', default='')
    parser.add_argument('--form-url', default='')
    parser.add_argument('--form-fields', default='')
    args = parser.parse_args()

    start_urls = json.loads(args.start_urls)

    # Resolve file:// local paths that may be relative-like when passed from tests
    from urllib.request import pathname2url
    from urllib.parse import urlparse
    from pathlib import Path
    resolved = []
    for u in start_urls:
        if isinstance(u, str) and u.startswith('file://'):
            path = urlparse(u).path
            if not Path(path).exists():
                # try relative to cwd
                candidate = Path.cwd() / path.lstrip('/')
                if candidate.exists():
                    resolved.append('file://' + pathname2url(str(candidate)))
                    continue
                # fallback: search for matching filename
                matches = list(Path.cwd().glob('**/' + Path(path).name))
                if matches:
                    resolved.append('file://' + pathname2url(str(matches[0])))
                    continue
        resolved.append(u)
    start_urls = resolved

    # Debug: surface resolved start_urls on stderr to help diagnose intermittent test failures
    import sys as _sys
    print(f"RUNNER start_urls={start_urls}", file=_sys.stderr, flush=True)

    try:
        from scrapy.crawler import CrawlerProcess
        from .scrapy_adapter import InMemoryPipeline
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(1)

    InMemoryPipeline.items_list = []
    InMemoryPipeline.max_items = args.max_items
    pipeline_key = (
        "florida_property_scraper.backend.scrapy_adapter.InMemoryPipeline"
    )

    settings = {
        "ITEM_PIPELINES": {
            pipeline_key: 100,
        },
    }
    if args.max_items:
        settings["CLOSESPIDER_ITEMCOUNT"] = args.max_items

    try:
        SpiderCls = resolve_spider_class(args.spider_name)
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(1)

    form_fields = {}
    if args.form_fields:
        try:
            form_fields = json.loads(args.form_fields)
        except Exception:
            form_fields = {}

    process = CrawlerProcess(settings=settings)
    process.crawl(
        SpiderCls,
        start_urls=start_urls,
        debug_html=args.debug_html,
        query=args.query,
        pagination=args.pagination,
        page_param=args.page_param,
        form_url=args.form_url,
        form_fields_template=form_fields,
    )
    process.start()

    # Always print the (possibly empty) items array and flush to avoid buffered stdout issues
    print(json.dumps(InMemoryPipeline.items_list), flush=True)


if __name__ == '__main__':
    main()
def resolve_spider_name(raw_name: str) -> str:
    name = (raw_name or "").lower()
    if name.endswith("_spider"):
        name = name[: -len("_spider")]
    return name


def resolve_spider_cls(spider_name: str):
    normalized = resolve_spider_name(spider_name)
    return SPIDERS.get(normalized)
def resolve_spider_name(raw_name: str) -> str:
    name = (raw_name or "").lower()
    if name.endswith("_spider"):
        name = name[: -len("_spider")]
    return name


def resolve_spider_cls(spider_name: str):
    normalized = resolve_spider_name(spider_name)
    return SPIDERS.get(normalized)
