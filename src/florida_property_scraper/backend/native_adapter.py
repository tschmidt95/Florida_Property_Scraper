import urllib.parse

from florida_property_scraper.schema import normalize_item
from florida_property_scraper.routers.registry import build_start_urls, get_entry

from .native.engine import NativeEngine
from .native.http_client import HttpClient
from .native.parsers import get_parser


class NativeAdapter:
    def __init__(self):
        self.engine = NativeEngine()
        self.http = HttpClient()

    @staticmethod
    def _allowed_hosts(state, county_slug):
        entry = get_entry(state, county_slug)
        url_template = entry.get("url_template", "") if entry else ""
        if not url_template:
            return None
        parsed = urllib.parse.urlparse(url_template)
        if parsed.hostname:
            return {parsed.hostname}
        return None

    def _build_start_requests(self, state, county_slug, query):
        entry = get_entry(state, county_slug)
        if not entry:
            return []
        if entry.get("query_param_style") == "form":
            form_url = entry.get("form_url", "")
            form_fields = entry.get("form_fields_template", {})
            payload = {k: (v.format(query=query) if isinstance(v, str) else v) for k, v in form_fields.items()}
            return [self.http.build_form_request(form_url, payload)]
        start_urls = build_start_urls(state, county_slug, query)
        return [{"url": url, "method": "GET"} for url in start_urls]

    def search(
        self,
        query,
        start_urls=None,
        spider_name=None,
        max_items=None,
        per_county_limit=None,
        live=False,
        county_slug=None,
        state="fl",
        dry_run=False,
    ):
        county = county_slug or (spider_name or "").replace("_spider", "")
        if live and not dry_run:
            start_requests = self._build_start_requests(state, county, query)
        else:
            start_requests = []
            if start_urls:
                start_requests = [{"url": url, "method": "GET"} for url in start_urls]
        parser = get_parser(county)
        self.engine.max_items = max_items
        self.engine.per_county_limit = per_county_limit
        allowed_hosts = self._allowed_hosts(state, county) if live else None
        run_dry = dry_run or not live
        items = self.engine.run(
            start_requests,
            parser,
            county,
            allowed_hosts=allowed_hosts,
            dry_run=run_dry,
            debug_context={"query": query, "county": county},
        )
        normalized = [normalize_item(item) for item in items]
        if max_items:
            normalized = normalized[:max_items]
        return normalized

    def iter_records(
        self,
        query,
        start_urls=None,
        spider_name=None,
        max_items=None,
        per_county_limit=None,
        live=False,
        county_slug=None,
        state="fl",
        dry_run=False,
    ):
        county = county_slug or (spider_name or "").replace("_spider", "")
        if live and not dry_run:
            start_requests = self._build_start_requests(state, county, query)
        else:
            start_requests = []
            if start_urls:
                start_requests = [{"url": url, "method": "GET"} for url in start_urls]
        parser = get_parser(county)
        self.engine.max_items = max_items
        self.engine.per_county_limit = per_county_limit
        allowed_hosts = self._allowed_hosts(state, county) if live else None
        run_dry = dry_run or not live
        return self.engine.iter_records(
            start_requests,
            parser,
            county,
            allowed_hosts=allowed_hosts,
            dry_run=run_dry,
            debug_context={"query": query, "county": county},
        )
