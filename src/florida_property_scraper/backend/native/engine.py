from .http_client import HttpClient
from .extract import ensure_fields


class NativeEngine:
    def __init__(self, max_items=None, per_county_limit=None, retry_config=None, max_pages=50):
        self.max_items = max_items
        self.per_county_limit = per_county_limit
        self.http = HttpClient(retry_config=retry_config)
        self.max_pages = max_pages

    def run(self, start_requests, parser, county_slug, allowed_hosts=None, log_fn=None, dry_run=False, fixture_map=None):
        items = []
        visited = set()
        queue = list(start_requests or [])
        pages_seen = 0
        while queue:
            if pages_seen >= self.max_pages:
                break
            req = queue.pop(0)
            req_url = req["url"] if isinstance(req, dict) else req
            if req_url in visited:
                continue
            visited.add(req_url)
            response = self.http.request(req, allowed_hosts=allowed_hosts, dry_run=dry_run, fixture_map=fixture_map)
            pages_seen += 1
            parsed = parser(response["text"], response["final_url"], county_slug)
            normalized = [ensure_fields(item, county_slug, item.get("raw_html", "")) for item in parsed]
            items.extend(normalized)
            if log_fn:
                log_fn(
                    {
                        "county": county_slug,
                        "url": response["final_url"],
                        "items_found": len(parsed),
                        "status": "success",
                    }
                )
            if self.per_county_limit and len(items) >= self.per_county_limit:
                return items[: self.per_county_limit]
            if self.max_items and len(items) >= self.max_items:
                return items[: self.max_items]
            next_urls = getattr(parser, "get_next_urls", None)
            if callable(next_urls):
                for next_url in next_urls(response["text"], response["final_url"]):
                    if next_url not in visited:
                        queue.append({"url": next_url, "method": "GET"})
        return items
