"""Helper CLI to run a Scrapy spider in a subprocess and emit JSON results to stdout.

This isolates Twisted reactor usage to the subprocess so the main test process can run
multiple spiders sequentially without reactor conflicts.
"""
import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--spider-name', required=True)
    parser.add_argument('--start-urls', required=True, help='JSON array of start URLs')
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
    _sys.stderr.write(f"RUNNER start_urls={start_urls}\n")

    try:
        from scrapy.crawler import CrawlerProcess
        from .scrapy_adapter import InMemoryPipeline
        from .spiders import SPIDERS
    except Exception as exc:
        print(json.dumps({'error': str(exc)}))
        sys.exit(1)

    InMemoryPipeline.items_list = []
    settings = {
        'ITEM_PIPELINES': {
            'florida_property_scraper.backend.scrapy_adapter.InMemoryPipeline': 100,
        }
    }

    SpiderCls = SPIDERS.get(args.spider_name)
    if not SpiderCls:
        try:
            module_name = f"florida_property_scraper.backend.spiders.{args.spider_name}_spider"
            module = __import__(module_name, fromlist=['*'])
            class_name = ''.join(p.capitalize() for p in args.spider_name.split('_')) + 'Spider'
            SpiderCls = getattr(module, class_name)
        except Exception as exc:
            print(json.dumps({'error': str(exc)}))
            sys.exit(1)

    process = CrawlerProcess(settings=settings)
    process.crawl(SpiderCls, start_urls=start_urls)
    process.start()

    print(json.dumps(InMemoryPipeline.items_list))


if __name__ == '__main__':
    main()
